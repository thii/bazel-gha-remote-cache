import assert from 'node:assert/strict'
import {createHash, randomBytes, randomUUID} from 'node:crypto'
import {
  createServer,
  type IncomingMessage,
  type ServerResponse
} from 'node:http'
import {mkdtemp, rm, writeFile} from 'node:fs/promises'
import type {AddressInfo} from 'node:net'
import {tmpdir} from 'node:os'
import path from 'node:path'
import {spawn, type ChildProcess} from 'node:child_process'
import {Writable} from 'node:stream'
import test from 'node:test'
import {CONTROL_FILES, pathExists, readJsonFile} from '../src/control.js'
import {sleep, validateDaemonReady, validateMetrics} from '../src/lifecycle.js'
import type {DaemonConfig} from '../src/model.js'
import {
  PACK_CACHE_VERSION,
  findPackIndexEntry,
  parsePack
} from '../src/pack-format.js'

interface CapturedRequest {
  method: string
  path: string
  authorization?: string
  body: Buffer
}

async function bodyOf(request: IncomingMessage): Promise<Buffer> {
  const chunks: Buffer[] = []
  for await (const value of request) {
    chunks.push(Buffer.isBuffer(value) ? value : Buffer.from(value))
  }
  return Buffer.concat(chunks)
}

function json(response: ServerResponse, value: unknown): void {
  const body = Buffer.from(JSON.stringify(value))
  response.writeHead(200, {
    'Content-Type': 'application/json',
    'Content-Length': String(body.length)
  })
  response.end(body)
}

async function waitForReady(
  child: ChildProcess,
  readyPath: string,
  standardError: () => string = () => ''
): Promise<ReturnType<typeof validateDaemonReady>> {
  const deadline = Date.now() + 10_000
  while (Date.now() < deadline) {
    if (child.exitCode !== null) {
      throw new Error(
        `daemon exited before ready: ${child.exitCode}; ${standardError()}`
      )
    }
    if (await pathExists(readyPath)) {
      return validateDaemonReady(await readJsonFile<unknown>(readyPath))
    }
    await sleep(25)
  }
  throw new Error('daemon readiness timeout')
}

async function waitForExit(child: ChildProcess): Promise<number | null> {
  if (child.exitCode !== null) return child.exitCode
  return new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error('daemon exit timeout')),
      10_000
    )
    child.once('exit', code => {
      clearTimeout(timer)
      resolve(code)
    })
  })
}

test('daemon finalizes one mixed CAS/AC pack and preserves both payloads', async t => {
  const runtimeToken = `runtime-${randomBytes(16).toString('hex')}`
  const sasSignature = `sas-${randomBytes(16).toString('hex')}`
  const requests: CapturedRequest[] = []
  const staged = new Map<string, Buffer>()
  const finalized = new Map<string, Buffer>()
  let serviceOrigin = ''

  const service = createServer(async (request, response) => {
    const body = await bodyOf(request)
    const requestUrl = new URL(request.url ?? '/', serviceOrigin)
    requests.push({
      method: request.method ?? '',
      path: requestUrl.pathname,
      ...(request.headers.authorization === undefined
        ? {}
        : {authorization: request.headers.authorization}),
      body
    })

    if (requestUrl.pathname.startsWith('/blob/')) {
      const key = decodeURIComponent(requestUrl.pathname.slice('/blob/'.length))
      assert.equal(request.headers.authorization, undefined)
      assert.equal(requestUrl.searchParams.get('sig'), sasSignature)
      if (request.method === 'PUT') {
        staged.set(key, body)
        response.writeHead(201, {
          ETag: '"test-etag"',
          'Last-Modified': new Date().toUTCString(),
          'x-ms-request-id': randomUUID(),
          'x-ms-version': '2023-11-03'
        })
        response.end()
        return
      }
      if (request.method === 'GET') {
        const value = finalized.get(key)
        if (!value) {
          response.writeHead(404).end()
          return
        }
        response.writeHead(200, {
          'Content-Type': 'application/octet-stream',
          'Content-Length': String(value.length)
        })
        response.end(value)
        return
      }
    }

    assert.equal(request.headers.authorization, `Bearer ${runtimeToken}`)
    const input = JSON.parse(body.toString('utf8')) as Record<string, unknown>
    const method = requestUrl.pathname.split('/').at(-1)
    const key = String(input['key'])
    const signedUrl = `${serviceOrigin}/blob/${encodeURIComponent(key)}?sig=${sasSignature}`

    if (method === 'CreateCacheEntry') {
      json(response, {ok: true, signed_upload_url: signedUrl})
      return
    }
    if (method === 'FinalizeCacheEntryUpload') {
      const value = staged.get(key)
      assert.ok(value)
      assert.equal(input['size_bytes'], String(value.length))
      finalized.set(key, value)
      json(response, {ok: true, entry_id: '9223372036854775807'})
      return
    }
    if (method === 'GetCacheEntryDownloadURL') {
      assert.equal(Object.hasOwn(input, 'restore_keys'), false)
      if (!finalized.has(key)) {
        json(response, {ok: false})
        return
      }
      json(response, {
        ok: true,
        signed_download_url: signedUrl,
        matched_key: key
      })
      return
    }
    response.writeHead(404).end()
  })
  await new Promise<void>((resolve, reject) => {
    service.once('error', reject)
    service.listen(0, '127.0.0.1', resolve)
  })
  const serviceAddress = service.address() as AddressInfo
  serviceOrigin = `http://127.0.0.1:${serviceAddress.port}`
  t.after(() => new Promise<void>(resolve => service.close(() => resolve())))

  const controlDirectory = await mkdtemp(
    path.join(tmpdir(), 'brc-daemon-test-')
  )
  t.after(() => rm(controlDirectory, {recursive: true, force: true}))
  const config: DaemonConfig = {
    namespace: 'integration-v1',
    storageMode: 'pack',
    port: 0,
    readable: true,
    writable: true,
    maxObjectSize: 1024 * 1024,
    maxInflightBytes: 1024 * 1024,
    maxPendingBytes: 1024 * 1024,
    uploadConcurrency: 1,
    downloadConcurrency: 2,
    repositoryUploadBudget: 120,
    expectedWriters: 1,
    uploadBurst: 2,
    writeBack: true,
    flushTimeoutSeconds: 5,
    packTargetBytes: 1024 * 1024,
    packMaxObjects: 2,
    packMaxAgeSeconds: 300,
    catalogRefreshSeconds: 15,
    remoteTimeoutSeconds: 5,
    failJobOnCacheError: true,
    githubApiUrl: 'https://api.github.com',
    githubRepository: 'test/integration',
    currentRef: 'refs/heads/main',
    defaultRef: 'refs/heads/main',
    runId: '1',
    jobHash: '0'.repeat(16),
    controlDirectory,
    shutdownToken: randomBytes(32).toString('base64url'),
    instanceId: randomUUID()
  }
  const configPath = path.join(controlDirectory, CONTROL_FILES.config)
  await writeFile(configPath, `${JSON.stringify(config)}\n`, {mode: 0o600})

  let standardOutput = ''
  let standardError = ''
  const daemonEntry = process.env['BRC_TEST_DAEMON_ENTRY']
  const daemonEnvironment = {...process.env}
  delete daemonEnvironment['ACTIONS_CACHE_SERVICE_V2']
  delete daemonEnvironment['ACTIONS_RESULTS_URL']
  delete daemonEnvironment['ACTIONS_RUNTIME_TOKEN']
  const child = spawn(
    process.execPath,
    daemonEntry
      ? [daemonEntry, configPath]
      : ['--import', 'tsx', path.resolve('src/daemon.ts'), configPath],
    {
      cwd: path.resolve('.'),
      env: {
        ...daemonEnvironment,
        GITHUB_SERVER_URL: 'http://localhost'
      },
      stdio: ['ignore', 'pipe', 'pipe', 'pipe']
    }
  )
  const credentialPipe = child.stdio[3]
  assert.ok(credentialPipe instanceof Writable)
  credentialPipe.end(
    `${JSON.stringify({
      resultsUrl: `${serviceOrigin}/ignored/base/`,
      runtimeToken,
      githubToken: 'github-catalog-token'
    })}\n`
  )
  child.stdout?.on('data', value => {
    standardOutput += String(value)
  })
  child.stderr?.on('data', value => {
    standardError += String(value)
  })
  t.after(() => {
    if (child.exitCode === null) child.kill('SIGKILL')
  })

  const ready = await waitForReady(
    child,
    path.join(controlDirectory, CONTROL_FILES.ready),
    () => standardError
  )
  assert.equal(ready.instanceId, config.instanceId)

  const casPayload = Buffer.from([0, 10, 0, 255, 1, 2, 3])
  const casDigest = createHash('sha256').update(casPayload).digest('hex')
  // build.bazel.remote.execution.v2.ActionResult { exit_code: 1 }
  const actionPayload = Buffer.from([0x20, 0x01])
  const actionDigest = createHash('sha256')
    .update('integration action key')
    .digest('hex')
  assert.notEqual(
    actionDigest,
    createHash('sha256').update(actionPayload).digest('hex')
  )

  const casPut = await fetch(`${ready.url}/cache/cas/${casDigest}`, {
    method: 'PUT',
    body: casPayload
  })
  assert.equal(casPut.status, 204)
  const actionPut = await fetch(`${ready.url}/cache/ac/${actionDigest}`, {
    method: 'PUT',
    body: actionPayload
  })
  assert.equal(actionPut.status, 204)

  const actionGet = await fetch(`${ready.url}/cache/ac/${actionDigest}`)
  assert.equal(actionGet.status, 200)
  assert.deepEqual(Buffer.from(await actionGet.arrayBuffer()), actionPayload)
  const casGet = await fetch(`${ready.url}/cache/cas/${casDigest}`)
  assert.equal(casGet.status, 200)
  assert.deepEqual(Buffer.from(await casGet.arrayBuffer()), casPayload)

  const shutdown = await fetch(`${ready.url}/shutdown`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${config.shutdownToken}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({drain: true, deadlineMs: 5000})
  })
  assert.equal(shutdown.status, 202)
  assert.equal(await waitForExit(child), 0)

  const stats = validateMetrics(
    await readJsonFile<unknown>(
      path.join(controlDirectory, CONTROL_FILES.stats)
    )
  )
  assert.equal(stats.writes.cas.successes, 1)
  assert.equal(stats.writes.ac.successes, 1)
  assert.equal(stats.reads.cas.hits, 1)
  assert.equal(stats.reads.ac.hits, 1)
  assert.equal(stats.backend.errors, 0)
  assert.equal(stats.writeBack.acceptedObjects, 2)
  assert.equal(stats.writeBack.packedObjects, 2)
  assert.equal(stats.writeBack.packsFinalized, 1)
  assert.equal(stats.writeBack.remainingObjects, 0)

  assert.equal(finalized.size, 1)
  const [packKey, packBytes] = [...finalized.entries()][0] ?? []
  assert.ok(packKey?.startsWith('brc2-'))
  assert.ok(packBytes)
  const parsedPack = parsePack(packBytes)
  assert.equal(parsedPack.entries.length, 2)
  const casEntry = findPackIndexEntry(parsedPack.entries, 'cas', casDigest)
  assert.ok(casEntry)
  assert.deepEqual(
    packBytes.subarray(
      Number(casEntry.offset),
      Number(casEntry.offset + casEntry.length)
    ),
    casPayload
  )
  const actionEntry = findPackIndexEntry(parsedPack.entries, 'ac', actionDigest)
  assert.ok(actionEntry)
  assert.deepEqual(
    packBytes.subarray(
      Number(actionEntry.offset),
      Number(actionEntry.offset + actionEntry.length)
    ),
    actionPayload
  )

  const finalizedRequest = requests.find(request =>
    request.path.endsWith('/FinalizeCacheEntryUpload')
  )
  assert.ok(finalizedRequest)
  const finalizedInput = JSON.parse(
    finalizedRequest.body.toString('utf8')
  ) as Record<string, unknown>
  assert.equal(finalizedInput['version'], PACK_CACHE_VERSION)

  const controlRequests = requests.filter(request =>
    request.path.startsWith('/twirp/')
  )
  assert.deepEqual(
    controlRequests.map(request => request.path.split('/').at(-1)),
    ['CreateCacheEntry', 'FinalizeCacheEntryUpload']
  )
  for (const request of controlRequests) {
    assert.equal(request.authorization, `Bearer ${runtimeToken}`)
    assert.equal(
      JSON.parse(request.body.toString()).version,
      PACK_CACHE_VERSION
    )
  }
  for (const request of requests.filter(request =>
    request.path.startsWith('/blob/')
  )) {
    assert.equal(request.authorization, undefined)
  }
  assert.equal(standardOutput.includes(runtimeToken), false)
  assert.equal(standardError.includes(runtimeToken), false)
  assert.equal(standardOutput.includes(sasSignature), false)
  assert.equal(standardError.includes(sasSignature), false)
})
