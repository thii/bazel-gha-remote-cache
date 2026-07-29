import assert from 'node:assert/strict'
import {createHash, randomUUID} from 'node:crypto'
import {createReadStream} from 'node:fs'
import {mkdtemp, readFile, readdir, rm, writeFile} from 'node:fs/promises'
import {request as httpRequest, type ClientRequest} from 'node:http'
import {tmpdir} from 'node:os'
import path from 'node:path'
import {Readable} from 'node:stream'
import test from 'node:test'
import {
  BackendError,
  type CacheBackend,
  type CacheLookup,
  type CacheReservation
} from '../src/backend.js'
import {DiagnosticJournal} from '../src/diagnostics.js'
import {Metrics} from '../src/metrics.js'
import {CACHE_VERSION, type DaemonConfig} from '../src/model.js'
import type {PackReader} from '../src/pack-reader.js'
import {CacheHttpServer, objectCacheKey} from '../src/server.js'
import type {WriteBackQueue} from '../src/writeback.js'

class Deferred<T> {
  readonly promise: Promise<T>
  resolve!: (value: T) => void
  reject!: (error: unknown) => void

  constructor() {
    this.promise = new Promise<T>((resolve, reject) => {
      this.resolve = resolve
      this.reject = reject
    })
  }
}

class MemoryBackend implements CacheBackend {
  readonly objects = new Map<string, Buffer>()
  readonly staged = new Map<string, Buffer>()
  readonly lookupCalls: Array<{key: string; version: string}> = []
  readonly reserveCalls: Array<{key: string; version: string}> = []
  readonly uploadCalls: Array<{key: string; size: number}> = []
  readonly finalizeCalls: Array<{key: string; version: string; size: number}> =
    []

  lookupHook?: (
    key: string,
    version: string,
    signal?: AbortSignal
  ) => Promise<CacheLookup>
  reserveHook?: (
    key: string,
    version: string,
    signal?: AbortSignal
  ) => Promise<CacheReservation>
  uploadHook?: (
    key: string,
    filePath: string,
    size: number,
    signal?: AbortSignal
  ) => Promise<void>
  finalizeHook?: (
    key: string,
    version: string,
    size: number,
    signal?: AbortSignal
  ) => Promise<void>
  downloadHook?: (signedUrl: string, signal?: AbortSignal) => Promise<Response>

  async lookup(
    key: string,
    version: string,
    signal?: AbortSignal
  ): Promise<CacheLookup> {
    this.lookupCalls.push({key, version})
    if (this.lookupHook) return this.lookupHook(key, version, signal)
    return this.objects.has(key)
      ? {kind: 'hit', downloadUrl: this.urlFor(key)}
      : {kind: 'miss'}
  }

  async reserve(
    key: string,
    version: string,
    signal?: AbortSignal
  ): Promise<CacheReservation> {
    if (signal?.aborted) throw new Error('cache reservation aborted')
    this.reserveCalls.push({key, version})
    if (this.reserveHook) return this.reserveHook(key, version, signal)
    return this.objects.has(key)
      ? {kind: 'conflict'}
      : {kind: 'reserved', uploadUrl: this.urlFor(key)}
  }

  async uploadFile(
    signedUrl: string,
    filePath: string,
    size: number,
    signal?: AbortSignal
  ): Promise<void> {
    if (signal?.aborted) throw new Error('cache upload aborted')
    const key = this.keyFromUrl(signedUrl)
    this.uploadCalls.push({key, size})
    if (this.uploadHook) return this.uploadHook(key, filePath, size, signal)
    this.staged.set(key, await readFile(filePath))
  }

  async finalize(
    key: string,
    version: string,
    size: number,
    signal?: AbortSignal
  ): Promise<void> {
    if (signal?.aborted) throw new Error('cache finalization aborted')
    this.finalizeCalls.push({key, version, size})
    if (this.finalizeHook) {
      return this.finalizeHook(key, version, size, signal)
    }
    const value = this.staged.get(key)
    if (!value) throw new BackendError('no staged object')
    this.objects.set(key, value)
    this.staged.delete(key)
  }

  async openDownload(
    signedUrl: string,
    signal?: AbortSignal
  ): Promise<Response> {
    if (this.downloadHook) return this.downloadHook(signedUrl, signal)
    const value = this.objects.get(this.keyFromUrl(signedUrl))
    if (!value) throw new BackendError('missing signed object')
    return new Response(Uint8Array.from(value), {
      status: 200,
      headers: {
        'Content-Length': String(value.length),
        'Content-Type': 'application/octet-stream'
      }
    })
  }

  async openDownloadRange(
    signedUrl: string,
    offset: number,
    length: number,
    signal?: AbortSignal
  ): Promise<Response> {
    if (signal?.aborted) throw new Error('range download aborted')
    const value = this.objects.get(this.keyFromUrl(signedUrl))
    if (!value) throw new BackendError('missing signed object')
    if (
      !Number.isSafeInteger(offset) ||
      offset < 0 ||
      !Number.isSafeInteger(length) ||
      length < 1 ||
      offset + length > value.length
    ) {
      throw new BackendError('invalid signed object range')
    }
    const body = value.subarray(offset, offset + length)
    return new Response(Uint8Array.from(body), {
      status: 206,
      headers: {
        'Content-Length': String(body.length),
        'Content-Range': `bytes ${offset}-${offset + length - 1}/${value.length}`,
        'Content-Type': 'application/octet-stream'
      }
    })
  }

  async commitFile(
    key: string,
    version: string,
    filePath: string,
    size: number,
    signal?: AbortSignal
  ): Promise<'created' | 'already-exists'> {
    if (signal?.aborted) throw new Error('cache commit aborted')
    const reservation = await this.reserve(key, version, signal)
    if (reservation.kind === 'conflict') return 'already-exists'
    if (!reservation.uploadUrl) {
      throw new BackendError('cache reservation omitted an upload URL')
    }
    await this.uploadFile(reservation.uploadUrl, filePath, size, signal)
    await this.finalize(key, version, size, signal)
    return 'created'
  }

  private urlFor(key: string): string {
    return `https://blob.invalid/${encodeURIComponent(key)}`
  }

  private keyFromUrl(value: string): string {
    return decodeURIComponent(new URL(value).pathname.slice(1))
  }
}

interface RunningServer {
  backend: MemoryBackend
  baseUrl: string
  config: DaemonConfig
  diagnostics: DiagnosticJournal
  metrics: Metrics
  server: CacheHttpServer
  stop: () => Promise<void>
}

interface ServerComponents {
  now?: () => number
  packReader?: PackReader
  writeBack?: WriteBackQueue
}

async function startServer(
  backend = new MemoryBackend(),
  overrides: Partial<DaemonConfig> = {},
  onShutdown: () => void = () => {},
  shutdownDrainMs?: number,
  components: ServerComponents = {}
): Promise<RunningServer> {
  const controlDirectory = await mkdtemp(
    path.join(tmpdir(), 'brc-server-test-')
  )
  const config: DaemonConfig = {
    namespace: 'test-v1',
    storageMode: 'object',
    port: 0,
    readable: true,
    writable: true,
    maxObjectSize: 1024 * 1024,
    maxInflightBytes: 2 * 1024 * 1024,
    maxPendingBytes: 2 * 1024 * 1024,
    uploadConcurrency: 2,
    downloadConcurrency: 2,
    repositoryUploadBudget: 120,
    expectedWriters: 1,
    uploadBurst: 2,
    writeBack: false,
    flushTimeoutSeconds: 120,
    packTargetBytes: 1024 * 1024,
    packMaxObjects: 256,
    packMaxAgeSeconds: 8,
    catalogRefreshSeconds: 15,
    remoteTimeoutSeconds: 30,
    failJobOnCacheError: false,
    githubApiUrl: 'https://api.github.com',
    githubRepository: 'test/server',
    currentRef: 'refs/heads/main',
    defaultRef: 'refs/heads/main',
    runId: '1',
    jobHash: '0'.repeat(16),
    controlDirectory,
    shutdownToken: 'a'.repeat(43),
    instanceId: randomUUID(),
    ...overrides
  }
  const metrics = new Metrics(config.readable, config.writable)
  const diagnostics = new DiagnosticJournal(
    path.join(controlDirectory, 'errors.ndjson')
  )
  const server = new CacheHttpServer({
    config,
    backend,
    metrics,
    diagnostics,
    onShutdown,
    ...components,
    ...(shutdownDrainMs === undefined ? {} : {shutdownDrainMs})
  })
  const address = await server.start()
  return {
    backend,
    baseUrl: `http://127.0.0.1:${address.port}`,
    config,
    diagnostics,
    metrics,
    server,
    stop: async () => {
      await server.shutdown()
      await diagnostics.flush()
      await rm(controlDirectory, {recursive: true, force: true})
    }
  }
}

async function cancelAfterFirstResponseChunk(url: string): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    let settled = false
    const settle = (error?: Error): void => {
      if (settled) return
      settled = true
      if (error === undefined) resolve()
      else reject(error)
    }
    const request = httpRequest(url, response => {
      response.once('data', () => response.destroy())
      response.once('close', () => settle())
      response.once('error', error => {
        const code = (error as NodeJS.ErrnoException).code
        if (code === 'ECONNRESET') settle()
        else settle(error)
      })
      response.resume()
    })
    request.once('error', error => {
      const code = (error as NodeJS.ErrnoException).code
      if (code === 'ECONNRESET') settle()
      else settle(error)
    })
    request.end()
  })
}

async function assertNoDiagnostics(running: RunningServer): Promise<void> {
  assert.equal(await running.diagnostics.flush(), undefined)
  assert.equal(await readFile(running.diagnostics.filePath, 'utf8'), '')
}

function digest(value: Buffer | string): string {
  return createHash('sha256').update(value).digest('hex')
}

async function waitFor(
  condition: () => boolean,
  failureMessage: string
): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (condition()) return
    await new Promise(resolve => setTimeout(resolve, 5))
  }
  throw new Error(failureMessage)
}

test('PUT stores exact raw CAS bytes and GET returns them', async t => {
  const running = await startServer()
  t.after(running.stop)
  const payload = Buffer.from([0, 1, 2, 0, 255, 10])
  const objectDigest = digest(payload)

  const put = await fetch(`${running.baseUrl}/cache/cas/${objectDigest}`, {
    method: 'PUT',
    body: payload
  })
  assert.equal(put.status, 204)
  assert.equal(await put.text(), '')

  const key = objectCacheKey('test-v1', 'cas', objectDigest)
  assert.deepEqual(running.backend.objects.get(key), payload)
  assert.deepEqual(running.backend.reserveCalls, [
    {key, version: CACHE_VERSION}
  ])
  assert.deepEqual(running.backend.finalizeCalls, [
    {key, version: CACHE_VERSION, size: payload.length}
  ])
  assert.deepEqual(
    await readdir(path.join(running.config.controlDirectory, 'spool')),
    []
  )

  const get = await fetch(`${running.baseUrl}/cache/cas/${objectDigest}`)
  assert.equal(get.status, 200)
  assert.equal(get.headers.get('content-type'), 'application/octet-stream')
  assert.deepEqual(Buffer.from(await get.arrayBuffer()), payload)
})

test('AC bodies are opaque and use a distinct immutable key', async t => {
  const running = await startServer()
  t.after(running.stop)
  const payload = Buffer.from('opaque action result')
  const actionDigest = '1'.repeat(64)

  const response = await fetch(`${running.baseUrl}/cache/ac/${actionDigest}`, {
    method: 'PUT',
    body: payload
  })
  assert.equal(response.status, 204)
  const key = objectCacheKey('test-v1', 'ac', actionDigest)
  assert.deepEqual(running.backend.objects.get(key), payload)
  assert.notEqual(key, objectCacheKey('test-v1', 'cas', actionDigest))
})

test('CAS mismatch never reaches the backend and permanently suppresses AC writes', async t => {
  const running = await startServer()
  t.after(running.stop)

  const badCas = await fetch(`${running.baseUrl}/cache/cas/${'0'.repeat(64)}`, {
    method: 'PUT',
    body: Buffer.from('not that digest')
  })
  assert.equal(badCas.status, 400)
  assert.equal(running.backend.reserveCalls.length, 0)

  const ac = await fetch(`${running.baseUrl}/cache/ac/${'2'.repeat(64)}`, {
    method: 'PUT',
    body: Buffer.from('action result')
  })
  assert.equal(ac.status, 503)
  assert.equal(running.backend.reserveCalls.length, 0)
  assert.equal(running.metrics.snapshot().casWriteFailed, true)
  assert.equal(await running.diagnostics.flush(), undefined)
  const diagnostics = await readFile(running.diagnostics.filePath, 'utf8')
  const errors = diagnostics
    .trim()
    .split('\n')
    .map(line => JSON.parse(line) as Record<string, unknown>)
  const matchingErrors = errors.filter(
    error =>
      error.area === 'http' &&
      error.operation === 'put' &&
      error.kind === 'cas' &&
      error.message === 'CAS digest did not match request bytes' &&
      error.statusCode === 400
  )
  assert.equal(matchingErrors.length, 1)
})

test('oversize and encoded uploads are rejected before backend reservation', async t => {
  const oversize = await startServer(new MemoryBackend(), {
    maxObjectSize: 4,
    maxInflightBytes: 4
  })
  t.after(oversize.stop)
  const tooLarge = await fetch(
    `${oversize.baseUrl}/cache/ac/${'3'.repeat(64)}`,
    {method: 'PUT', body: Buffer.from('12345')}
  )
  assert.equal(tooLarge.status, 413)
  assert.equal(oversize.backend.reserveCalls.length, 0)

  const encoded = await startServer()
  t.after(encoded.stop)
  const response = await fetch(
    `${encoded.baseUrl}/cache/ac/${'4'.repeat(64)}`,
    {
      method: 'PUT',
      headers: {'Content-Encoding': 'gzip'},
      body: Buffer.from('bytes')
    }
  )
  assert.equal(response.status, 415)
  assert.equal(encoded.backend.reserveCalls.length, 0)
})

test('an early Expect rejection of CAS permanently suppresses AC writes', async t => {
  const backend = new MemoryBackend()
  const running = await startServer(backend, {
    maxObjectSize: 4,
    maxInflightBytes: 4
  })
  t.after(running.stop)

  const payload = Buffer.from('large')
  let continued = false
  const status = await new Promise<number>((resolve, reject) => {
    const request = httpRequest(
      `${running.baseUrl}/cache/cas/${digest(payload)}`,
      {
        method: 'PUT',
        headers: {
          Expect: '100-continue',
          'Content-Length': String(payload.length)
        }
      },
      response => {
        response.resume()
        response.once('end', () => {
          request.destroy()
          resolve(response.statusCode ?? 0)
        })
      }
    )
    request.once('error', reject)
    request.once('continue', () => {
      continued = true
      request.end(payload)
    })
    request.flushHeaders()
  })

  assert.equal(status, 413)
  assert.equal(continued, false)
  assert.equal(backend.reserveCalls.length, 0)
  assert.equal(running.metrics.snapshot().casWriteFailed, true)

  const action = await fetch(`${running.baseUrl}/cache/ac/${'9'.repeat(64)}`, {
    method: 'PUT',
    body: Buffer.from('ac')
  })
  assert.equal(action.status, 503)
  assert.equal(
    backend.finalizeCalls.some(call => call.key.includes('-ac-sha256-')),
    false
  )
  assert.equal(await running.diagnostics.flush(), undefined)
  const diagnostics = (await readFile(running.diagnostics.filePath, 'utf8'))
    .trim()
    .split('\n')
    .map(line => JSON.parse(line) as Record<string, unknown>)
  const earlyRejections = diagnostics.filter(
    error =>
      error.area === 'http' &&
      error.operation === 'expect' &&
      error.kind === 'cas' &&
      error.message === 'cache object exceeds max-object-size' &&
      error.statusCode === 413
  )
  assert.equal(earlyRejections.length, 1)
})

test('strict routes reject malformed digests, extra path data, and queries', async t => {
  const running = await startServer()
  t.after(running.stop)
  const urls = [
    `/cache/cas/${'A'.repeat(64)}`,
    `/cache/cas/${'a'.repeat(63)}`,
    `/cache/cas/${'a'.repeat(64)}/`,
    `/cache/cas/${'a'.repeat(64)}?x=1`,
    `/cache/cas/%2f${'a'.repeat(62)}`,
    `/cache//cas/${'a'.repeat(64)}`
  ]
  for (const url of urls) {
    const response = await fetch(`${running.baseUrl}${url}`)
    assert.equal(response.status, 404, url)
  }
  assert.equal(running.backend.lookupCalls.length, 0)
})

test('read-only mode returns misses and rejects writes without backend calls', async t => {
  const running = await startServer(new MemoryBackend(), {writable: false})
  t.after(running.stop)
  const objectDigest = digest('value')
  const put = await fetch(`${running.baseUrl}/cache/cas/${objectDigest}`, {
    method: 'PUT',
    body: Buffer.from('value')
  })
  assert.equal(put.status, 403)
  assert.equal(running.backend.reserveCalls.length, 0)

  const unreadable = await startServer(new MemoryBackend(), {readable: false})
  t.after(unreadable.stop)
  const get = await fetch(`${unreadable.baseUrl}/cache/cas/${objectDigest}`)
  assert.equal(get.status, 404)
  assert.equal(unreadable.backend.lookupCalls.length, 0)
})

test('AC backend errors degrade to misses while CAS errors remain retryable', async t => {
  const backend = new MemoryBackend()
  backend.lookupHook = async () => {
    throw new BackendError('lookup unavailable', {retryable: true})
  }
  const running = await startServer(backend)
  t.after(running.stop)

  const ac = await fetch(`${running.baseUrl}/cache/ac/${'5'.repeat(64)}`)
  const cas = await fetch(`${running.baseUrl}/cache/cas/${'5'.repeat(64)}`)
  assert.equal(ac.status, 404)
  assert.equal(cas.status, 503)
  assert.equal(running.metrics.snapshot().backend.errors, 2)
  assert.equal(await running.diagnostics.flush(), undefined)
  const diagnostics = (await readFile(running.diagnostics.filePath, 'utf8'))
    .trim()
    .split('\n')
    .map(line => JSON.parse(line) as Record<string, unknown>)
  const httpErrors = diagnostics.filter(
    error => error.area === 'http' && error.operation === 'get'
  )
  assert.equal(httpErrors.length, 2)
  assert.deepEqual(httpErrors.map(error => error.kind).sort(), ['ac', 'cas'])
})

test('invalid signed-download headers cancel the upstream response body', async t => {
  const backend = new MemoryBackend()
  backend.lookupHook = async key => ({
    kind: 'hit',
    downloadUrl: `https://blob.invalid/${encodeURIComponent(key)}`
  })
  const invalidHeaders = [
    {'Content-Encoding': 'gzip'},
    {'Content-Length': 'not-a-number'}
  ]
  let downloadIndex = 0
  let cancellations = 0
  backend.downloadHook = async () => {
    const headers = invalidHeaders[downloadIndex]
    downloadIndex += 1
    assert.ok(headers)
    return new Response(
      new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(Uint8Array.from([1, 2, 3]))
        },
        cancel() {
          cancellations += 1
        }
      }),
      {status: 200, headers}
    )
  }
  const running = await startServer(backend)
  t.after(running.stop)

  for (let index = 0; index < invalidHeaders.length; index += 1) {
    const response = await fetch(
      `${running.baseUrl}/cache/cas/${String(index + 7).repeat(64)}`
    )
    assert.equal(response.status, 503)
    assert.equal(cancellations, index + 1)
  }
})

test('an aborted GET queued by the download limiter never opens a signed download', async t => {
  const backend = new MemoryBackend()
  backend.lookupHook = async key => ({
    kind: 'hit',
    downloadUrl: `https://blob.invalid/${encodeURIComponent(key)}`
  })
  let downloadOpens = 0
  let upstreamController:
    | ReadableStreamDefaultController<Uint8Array>
    | undefined
  backend.downloadHook = async () => {
    downloadOpens += 1
    return new Response(
      new ReadableStream<Uint8Array>({
        start(controller) {
          upstreamController = controller
          controller.enqueue(Uint8Array.from([1, 2, 3]))
        }
      })
    )
  }
  const running = await startServer(backend, {downloadConcurrency: 1})
  t.after(async () => {
    try {
      upstreamController?.close()
    } catch {}
    await running.stop()
  })

  const active = await fetch(`${running.baseUrl}/cache/cas/${'a'.repeat(64)}`)
  assert.equal(active.status, 200)
  assert.equal(downloadOpens, 1)

  const controller = new AbortController()
  const queued = fetch(`${running.baseUrl}/cache/cas/${'b'.repeat(64)}`, {
    signal: controller.signal
  })
  const limiter = (
    running.server as unknown as {
      downloadLimiter: {queued: number}
    }
  ).downloadLimiter
  await waitFor(
    () => limiter.queued === 1,
    'second GET did not queue behind the active signed download'
  )

  controller.abort()
  await assert.rejects(queued, {name: 'AbortError'})
  await waitFor(() => limiter.queued === 0, 'aborted GET remained queued')
  assert.equal(backend.lookupCalls.length, 1)
  assert.equal(downloadOpens, 1)

  upstreamController?.close()
  await active.arrayBuffer()
  await waitFor(
    () => running.metrics.snapshot().requests.aborted === 1,
    'queued client cancellation was not recorded'
  )
  const stats = running.metrics.snapshot()
  assert.equal(stats.reads.cas.errors, 0)
  assert.equal(stats.backend.errors, 0)
  await assertNoDiagnostics(running)
})

test('a same-key control lookup is canceled after its final GET waiter aborts', async t => {
  const backend = new MemoryBackend()
  const lookupStarted = new Deferred<void>()
  const lookupCanceled = new Deferred<void>()
  let downloadOpens = 0
  backend.lookupHook = async (_key, _version, signal) => {
    assert.ok(signal)
    lookupStarted.resolve()
    return new Promise<CacheLookup>((_resolve, reject) => {
      const onAbort = (): void => {
        lookupCanceled.resolve()
        reject(new Error('control lookup canceled'))
      }
      if (signal.aborted) onAbort()
      else signal.addEventListener('abort', onAbort, {once: true})
    })
  }
  backend.downloadHook = async () => {
    downloadOpens += 1
    return new Response(Uint8Array.from([1]))
  }
  const running = await startServer(backend, {downloadConcurrency: 1})
  t.after(running.stop)
  const url = `${running.baseUrl}/cache/cas/${'c'.repeat(64)}`
  const firstController = new AbortController()
  const secondController = new AbortController()

  const first = fetch(url, {signal: firstController.signal})
  await lookupStarted.promise
  const second = fetch(url, {signal: secondController.signal})
  const flights = (
    running.server as unknown as {
      lookupFlights: Map<string, {waiters: number}>
    }
  ).lookupFlights
  await waitFor(
    () => [...flights.values()].some(flight => flight.waiters === 2),
    'same-key requests did not join one lookup flight'
  )
  assert.equal(backend.lookupCalls.length, 1)

  firstController.abort()
  secondController.abort()
  await Promise.all([
    assert.rejects(first, {name: 'AbortError'}),
    assert.rejects(second, {name: 'AbortError'}),
    lookupCanceled.promise
  ])
  assert.equal(downloadOpens, 0)
  await waitFor(
    () => running.metrics.snapshot().requests.aborted === 2,
    'lookup client cancellations were not recorded'
  )
  const stats = running.metrics.snapshot()
  assert.equal(stats.reads.cas.errors, 0)
  assert.equal(stats.backend.errors, 0)
  await assertNoDiagnostics(running)
})

test('a 429 racing with the final lookup cancellation is still recorded', async t => {
  const backend = new MemoryBackend()
  const lookupStarted = new Deferred<void>()
  backend.lookupHook = async (_key, _version, signal) => {
    assert.ok(signal)
    lookupStarted.resolve()
    return new Promise<CacheLookup>((_resolve, reject) => {
      const onAbort = (): void =>
        reject(
          new BackendError('lookup rate limited after cancellation', {
            statusCode: 429,
            rateLimited: true,
            retryable: true,
            retryAfterMs: 1000
          })
        )
      if (signal.aborted) onAbort()
      else signal.addEventListener('abort', onAbort, {once: true})
    })
  }
  const running = await startServer(backend)
  t.after(running.stop)
  const controller = new AbortController()
  const response = fetch(`${running.baseUrl}/cache/cas/${'6'.repeat(64)}`, {
    signal: controller.signal
  })
  await lookupStarted.promise

  controller.abort()
  await assert.rejects(response, {name: 'AbortError'})
  await waitFor(
    () => running.metrics.snapshot().backend.rateLimited === 1,
    'racing 429 was not recorded'
  )

  const stats = running.metrics.snapshot()
  assert.equal(stats.requests.aborted, 1)
  assert.equal(stats.backend.errors, 1)
  await running.diagnostics.flush()
  const events = (await readFile(running.diagnostics.filePath, 'utf8'))
    .trim()
    .split('\n')
    .map(line => JSON.parse(line) as Record<string, unknown>)
  assert.equal(events.length, 1)
  assert.equal(events[0]?.area, 'backend')
  assert.equal(events[0]?.operation, 'lookup')
  assert.equal(events[0]?.statusCode, 429)
})

test('a cancelled half-open read probe does not close the circuit', async t => {
  let now = 0
  let materializations = 0
  const firstMaterializationStarted = new Deferred<void>()
  const packReader = {
    async materialize(_kind: string, _digest: string, signal?: AbortSignal) {
      materializations += 1
      if (materializations > 1) return undefined
      assert.ok(signal)
      firstMaterializationStarted.resolve()
      return new Promise<undefined>((_resolve, reject) => {
        const onAbort = (): void => reject(new Error('pack read aborted'))
        if (signal.aborted) onAbort()
        else signal.addEventListener('abort', onAbort, {once: true})
      })
    }
  } as unknown as PackReader
  const running = await startServer(
    new MemoryBackend(),
    {},
    undefined,
    undefined,
    {now: () => now, packReader}
  )
  t.after(running.stop)
  const circuit = (
    running.server as unknown as {
      readCircuit: {trip(milliseconds: number): void}
    }
  ).readCircuit
  circuit.trip(1000)
  now = 1000

  const controller = new AbortController()
  const first = fetch(`${running.baseUrl}/cache/cas/${'4'.repeat(64)}`, {
    signal: controller.signal
  })
  await firstMaterializationStarted.promise
  controller.abort()
  await assert.rejects(first, {name: 'AbortError'})
  await waitFor(
    () => running.metrics.snapshot().requests.aborted === 1,
    'half-open probe cancellation was not observed'
  )
  assert.equal(running.metrics.snapshot().readCircuitOpen, true)

  const second = await fetch(`${running.baseUrl}/cache/cas/${'5'.repeat(64)}`)
  assert.equal(second.status, 404)
  assert.equal(materializations, 2)
  assert.equal(running.metrics.snapshot().readCircuitOpen, false)
  await assertNoDiagnostics(running)
})

test('simultaneous reads single-flight the exact Cache v2 lookup', async t => {
  const backend = new MemoryBackend()
  const objectDigest = digest('shared')
  const key = objectCacheKey('test-v1', 'cas', objectDigest)
  backend.objects.set(key, Buffer.from('shared'))
  const lookupStarted = new Deferred<void>()
  const releaseLookup = new Deferred<void>()
  backend.lookupHook = async lookupKey => {
    lookupStarted.resolve()
    await releaseLookup.promise
    return {
      kind: 'hit',
      downloadUrl: `https://blob.invalid/${encodeURIComponent(lookupKey)}`
    }
  }
  const running = await startServer(backend)
  t.after(running.stop)

  const first = fetch(`${running.baseUrl}/cache/cas/${objectDigest}`)
  await lookupStarted.promise
  const second = fetch(`${running.baseUrl}/cache/cas/${objectDigest}`)
  await new Promise(resolve => setTimeout(resolve, 25))
  assert.equal(backend.lookupCalls.length, 1)
  releaseLookup.resolve()

  const responses = await Promise.all([first, second])
  assert.deepEqual(
    await Promise.all(responses.map(async response => response.text())),
    ['shared', 'shared']
  )
  assert.equal(backend.lookupCalls.length, 1)
})

test('a cancelled packed response is not reported as a cache error', async t => {
  const directory = await mkdtemp(path.join(tmpdir(), 'brc-pack-cancel-'))
  const payloadPath = path.join(directory, 'payload')
  const payloadSize = 8 * 1024 * 1024
  await writeFile(payloadPath, Buffer.alloc(payloadSize, 0x61))
  let disposed = 0
  const packReader = {
    async materialize() {
      return {
        path: payloadPath,
        size: payloadSize,
        async dispose() {
          disposed += 1
        }
      }
    }
  } as unknown as PackReader
  const running = await startServer(
    new MemoryBackend(),
    {maxObjectSize: payloadSize},
    undefined,
    undefined,
    {packReader}
  )
  t.after(async () => {
    await running.stop()
    await rm(directory, {recursive: true, force: true})
  })

  await cancelAfterFirstResponseChunk(
    `${running.baseUrl}/cache/ac/${'a'.repeat(64)}`
  )
  await waitFor(
    () => disposed === 1 && running.metrics.snapshot().requests.aborted === 1,
    'cancelled pack response did not finish cleanup'
  )

  const stats = running.metrics.snapshot()
  assert.equal(stats.reads.ac.hits, 0)
  assert.equal(stats.reads.ac.errors, 0)
  assert.equal(stats.backend.errors, 0)
  assert.equal(stats.readCircuitOpen, false)
  await assertNoDiagnostics(running)
})

test('a genuine packed response source error is still diagnosed', async t => {
  const directory = await mkdtemp(path.join(tmpdir(), 'brc-pack-error-'))
  const missingPath = path.join(directory, 'missing')
  let disposed = 0
  const packReader = {
    async materialize() {
      return {
        path: missingPath,
        size: 1,
        async dispose() {
          disposed += 1
        }
      }
    }
  } as unknown as PackReader
  const running = await startServer(
    new MemoryBackend(),
    {},
    undefined,
    undefined,
    {packReader}
  )
  t.after(async () => {
    await running.stop()
    await rm(directory, {recursive: true, force: true})
  })

  await assert.rejects(async () => {
    const response = await fetch(
      `${running.baseUrl}/cache/ac/${'b'.repeat(64)}`
    )
    await response.arrayBuffer()
  })
  await waitFor(
    () => disposed === 1 && running.metrics.snapshot().reads.ac.errors === 1,
    'pack source error was not recorded'
  )

  const stats = running.metrics.snapshot()
  assert.equal(stats.requests.aborted, 0)
  assert.equal(stats.reads.ac.hits, 0)
  assert.equal(stats.readCircuitOpen, false)
  assert.equal(await running.diagnostics.flush(), undefined)
  const events = (await readFile(running.diagnostics.filePath, 'utf8'))
    .trim()
    .split('\n')
    .map(line => JSON.parse(line) as Record<string, unknown>)
  assert.equal(events.length, 1)
  assert.equal(events[0]?.area, 'local-read')
  assert.equal(events[0]?.operation, 'pack')
  assert.equal(events[0]?.kind, 'ac')
})

test('a source-side premature close is not mistaken for a client cancellation', async t => {
  let opened = false
  const writeBack = {
    async start() {},
    async openLocal() {
      if (opened) return undefined
      opened = true
      let emitted = false
      const stream = new Readable({
        read() {
          if (emitted) return
          emitted = true
          this.push(Buffer.alloc(64, 0x64))
          setImmediate(() => {
            const error = Object.assign(new Error('Premature close'), {
              code: 'ERR_STREAM_PREMATURE_CLOSE'
            })
            this.destroy(error)
          })
        }
      })
      return {size: 1024, stream}
    },
    async drain() {
      return {}
    }
  } as unknown as WriteBackQueue
  const running = await startServer(
    new MemoryBackend(),
    {},
    undefined,
    undefined,
    {writeBack}
  )
  t.after(running.stop)

  await assert.rejects(async () => {
    const response = await fetch(
      `${running.baseUrl}/cache/cas/${'e'.repeat(64)}`
    )
    await response.arrayBuffer()
  })
  await waitFor(
    () => running.metrics.snapshot().reads.cas.errors === 1,
    'source-side premature close was not recorded'
  )

  const stats = running.metrics.snapshot()
  assert.equal(stats.requests.aborted, 0)
  assert.equal(stats.reads.cas.hits, 0)
  await running.diagnostics.flush()
  const events = (await readFile(running.diagnostics.filePath, 'utf8'))
    .trim()
    .split('\n')
    .map(line => JSON.parse(line) as Record<string, unknown>)
  assert.equal(events.length, 1)
  assert.equal(events[0]?.area, 'local-read')
  assert.equal(events[0]?.operation, 'pending')
  assert.equal(events[0]?.message, 'Premature close')
})

test('a cancelled legacy response is not reported as a cache error', async t => {
  const backend = new MemoryBackend()
  const objectDigest = 'c'.repeat(64)
  const key = objectCacheKey('test-v1', 'cas', objectDigest)
  backend.objects.set(key, Buffer.from('present'))
  const payloadSize = 8 * 1024 * 1024
  const chunk = new Uint8Array(64 * 1024).fill(0x62)
  let emitted = 0
  backend.downloadHook = async () =>
    new Response(
      new ReadableStream<Uint8Array>({
        pull(controller) {
          if (emitted * chunk.byteLength >= payloadSize) {
            controller.close()
            return
          }
          emitted += 1
          controller.enqueue(chunk)
        }
      }),
      {headers: {'Content-Length': String(payloadSize)}}
    )
  const running = await startServer(backend, {maxObjectSize: payloadSize})
  t.after(running.stop)

  await cancelAfterFirstResponseChunk(
    `${running.baseUrl}/cache/cas/${objectDigest}`
  )
  await waitFor(
    () => running.metrics.snapshot().requests.aborted === 1,
    'cancelled legacy response was not observed'
  )

  const stats = running.metrics.snapshot()
  assert.equal(stats.reads.cas.hits, 0)
  assert.equal(stats.reads.cas.errors, 0)
  assert.equal(stats.backend.errors, 0)
  assert.equal(stats.readCircuitOpen, false)
  await assertNoDiagnostics(running)
})

test('a client abort while opening a signed download is not diagnosed', async t => {
  const backend = new MemoryBackend()
  const objectDigest = '7'.repeat(64)
  const key = objectCacheKey('test-v1', 'cas', objectDigest)
  backend.objects.set(key, Buffer.from('present'))
  const downloadStarted = new Deferred<void>()
  backend.downloadHook = async (_url, signal) => {
    assert.ok(signal)
    downloadStarted.resolve()
    return new Promise<Response>((_resolve, reject) => {
      const onAbort = (): void =>
        reject(
          new BackendError('signed cache download failed', {
            retryable: true,
            cause: signal.reason
          })
        )
      if (signal.aborted) onAbort()
      else signal.addEventListener('abort', onAbort, {once: true})
    })
  }
  const running = await startServer(backend)
  t.after(running.stop)
  const controller = new AbortController()
  const response = fetch(`${running.baseUrl}/cache/cas/${objectDigest}`, {
    signal: controller.signal
  })
  await downloadStarted.promise

  controller.abort()
  await assert.rejects(response, {name: 'AbortError'})
  await waitFor(
    () => running.metrics.snapshot().requests.aborted === 1,
    'signed-download cancellation was not observed'
  )

  const stats = running.metrics.snapshot()
  assert.equal(stats.reads.cas.errors, 0)
  assert.equal(stats.backend.errors, 0)
  await assertNoDiagnostics(running)
})

test('an upstream legacy body abort is still diagnosed', async t => {
  const backend = new MemoryBackend()
  const objectDigest = 'f'.repeat(64)
  const key = objectCacheKey('test-v1', 'cas', objectDigest)
  backend.objects.set(key, Buffer.from('present'))
  backend.downloadHook = async () =>
    new Response(
      new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(Uint8Array.from([1, 2, 3]))
          setImmediate(() => {
            const error = Object.assign(new Error('remote body timed out'), {
              code: 'ABORT_ERR'
            })
            controller.error(error)
          })
        }
      }),
      {headers: {'Content-Length': '1024'}}
    )
  const running = await startServer(backend)
  t.after(running.stop)

  await assert.rejects(async () => {
    const response = await fetch(`${running.baseUrl}/cache/cas/${objectDigest}`)
    await response.arrayBuffer()
  })
  await waitFor(
    () => running.metrics.snapshot().reads.cas.errors === 1,
    'upstream body abort was not recorded'
  )

  const stats = running.metrics.snapshot()
  assert.equal(stats.requests.aborted, 0)
  assert.equal(stats.reads.cas.hits, 0)
  await running.diagnostics.flush()
  const events = (await readFile(running.diagnostics.filePath, 'utf8'))
    .trim()
    .split('\n')
    .map(line => JSON.parse(line) as Record<string, unknown>)
  assert.equal(events.length, 1)
  assert.equal(events[0]?.area, 'http')
  assert.equal(events[0]?.operation, 'get')
  assert.equal(events[0]?.message, 'remote body timed out')
})

test('a cancelled pending response is not reported as a cache error', async t => {
  const directory = await mkdtemp(path.join(tmpdir(), 'brc-pending-cancel-'))
  const payloadPath = path.join(directory, 'payload')
  const payloadSize = 8 * 1024 * 1024
  await writeFile(payloadPath, Buffer.alloc(payloadSize, 0x63))
  const writeBack = {
    async start() {},
    async openLocal() {
      return {size: payloadSize, stream: createReadStream(payloadPath)}
    },
    async drain() {
      return {}
    }
  } as unknown as WriteBackQueue
  const running = await startServer(
    new MemoryBackend(),
    {maxObjectSize: payloadSize},
    undefined,
    undefined,
    {writeBack}
  )
  t.after(async () => {
    await running.stop()
    await rm(directory, {recursive: true, force: true})
  })

  await cancelAfterFirstResponseChunk(
    `${running.baseUrl}/cache/cas/${'d'.repeat(64)}`
  )
  await waitFor(
    () => running.metrics.snapshot().requests.aborted === 1,
    'cancelled pending response was not observed'
  )

  const stats = running.metrics.snapshot()
  assert.equal(stats.reads.cas.hits, 0)
  assert.equal(stats.reads.cas.errors, 0)
  assert.equal(stats.backend.errors, 0)
  assert.equal(stats.readCircuitOpen, false)
  await assertNoDiagnostics(running)
})

test('confirmed first-writer conflict is an idempotent PUT success', async t => {
  const backend = new MemoryBackend()
  let polls = 0
  backend.reserveHook = async () => ({kind: 'conflict'})
  backend.lookupHook = async () => {
    polls += 1
    return polls < 3
      ? {kind: 'miss'}
      : {kind: 'hit', downloadUrl: 'https://blob.invalid/existing'}
  }
  const running = await startServer(backend)
  t.after(running.stop)
  const payload = Buffer.from('conflict')

  const response = await fetch(
    `${running.baseUrl}/cache/cas/${digest(payload)}`,
    {method: 'PUT', body: payload}
  )
  assert.equal(response.status, 204)
  assert.equal(polls, 3)
  assert.equal(backend.uploadCalls.length, 0)
  assert.equal(backend.finalizeCalls.length, 0)
  assert.equal(running.metrics.snapshot().casWriteFailed, false)
})

test('conflict visibility polling obeys the download limiter', async t => {
  const backend = new MemoryBackend()
  backend.reserveHook = async () => ({kind: 'conflict'})
  backend.lookupHook = async key => ({
    kind: 'hit',
    downloadUrl: `https://blob.invalid/${encodeURIComponent(key)}`
  })
  let upstreamController:
    | ReadableStreamDefaultController<Uint8Array>
    | undefined
  backend.downloadHook = async () =>
    new Response(
      new ReadableStream<Uint8Array>({
        start(controller) {
          upstreamController = controller
          controller.enqueue(Uint8Array.from([1]))
        }
      })
    )

  const running = await startServer(backend, {
    uploadConcurrency: 1,
    downloadConcurrency: 1
  })
  t.after(async () => {
    try {
      upstreamController?.close()
    } catch {}
    await running.stop()
  })

  const activeRead = await fetch(
    `${running.baseUrl}/cache/cas/${'d'.repeat(64)}`
  )
  assert.equal(activeRead.status, 200)
  assert.equal(backend.lookupCalls.length, 1)

  const payload = Buffer.from('conflict-poll')
  const conflictWrite = fetch(
    `${running.baseUrl}/cache/cas/${digest(payload)}`,
    {method: 'PUT', body: payload}
  )
  const limiters = running.server as unknown as {
    downloadLimiter: {queued: number}
    uploadLimiter: {used: number}
    byteLimiter: {used: number}
  }
  await waitFor(
    () => limiters.downloadLimiter.queued === 1,
    'conflict visibility lookup did not queue behind the active download'
  )

  assert.equal(backend.lookupCalls.length, 1)
  assert.equal(limiters.uploadLimiter.used, 0)
  assert.equal(limiters.byteLimiter.used, 0)

  upstreamController?.close()
  await activeRead.arrayBuffer()
  const response = await conflictWrite
  assert.equal(response.status, 204)
  assert.equal(backend.lookupCalls.length, 2)
  assert.equal(backend.uploadCalls.length, 0)
  assert.equal(backend.finalizeCalls.length, 0)
})

test('429 opens a write circuit and suppresses repeated backend calls', async t => {
  const backend = new MemoryBackend()
  backend.reserveHook = async () => {
    throw new BackendError('rate limited', {
      statusCode: 429,
      rateLimited: true,
      retryAfterMs: 5000
    })
  }
  const running = await startServer(backend)
  t.after(running.stop)
  const url = `${running.baseUrl}/cache/ac/${'6'.repeat(64)}`

  const first = await fetch(url, {method: 'PUT', body: Buffer.from('one')})
  const second = await fetch(url, {method: 'PUT', body: Buffer.from('two')})
  assert.equal(first.status, 429)
  assert.equal(first.headers.get('retry-after'), '5')
  assert.equal(second.status, 429)
  assert.equal(backend.reserveCalls.length, 1)
  assert.equal(running.metrics.snapshot().writeCircuitOpen, true)
})

test('a concurrent CAS failure prevents an already-uploaded AC from finalizing', async t => {
  const backend = new MemoryBackend()
  const acUploadStarted = new Deferred<void>()
  const releaseAcUpload = new Deferred<void>()
  backend.uploadHook = async (key, filePath) => {
    if (key.includes('-ac-sha256-')) {
      acUploadStarted.resolve()
      await releaseAcUpload.promise
      backend.staged.set(key, await readFile(filePath))
      return
    }
    throw new BackendError('CAS blob upload failed', {retryable: true})
  }
  const running = await startServer(backend)
  t.after(running.stop)

  const acRequest = fetch(`${running.baseUrl}/cache/ac/${'7'.repeat(64)}`, {
    method: 'PUT',
    body: Buffer.from('action')
  })
  await acUploadStarted.promise
  const casPayload = Buffer.from('output blob')
  const casResponse = await fetch(
    `${running.baseUrl}/cache/cas/${digest(casPayload)}`,
    {method: 'PUT', body: casPayload}
  )
  assert.equal(casResponse.status, 503)

  releaseAcUpload.resolve()
  const acResponse = await acRequest
  assert.equal(acResponse.status, 503)
  assert.equal(
    backend.finalizeCalls.some(call => call.key.includes('-ac-sha256-')),
    false
  )
})

test('AC finalization releases upload capacity needed by an active CAS write', async t => {
  const backend = new MemoryBackend()
  const acBlobUploaded = new Deferred<void>()
  backend.uploadHook = async (key, filePath) => {
    backend.staged.set(key, await readFile(filePath))
    if (key.includes('-ac-sha256-')) acBlobUploaded.resolve()
  }
  const running = await startServer(backend, {uploadConcurrency: 1})
  t.after(running.stop)

  // Start a CAS PUT and deliberately leave its request body incomplete. The
  // server has accepted it into the integrity gate, but it has not reached the
  // sole upload slot yet.
  const casPayload = Buffer.from('cas')
  let casRequest!: ClientRequest
  const casResponse = new Promise<number>((resolve, reject) => {
    casRequest = httpRequest(
      `${running.baseUrl}/cache/cas/${digest(casPayload)}`,
      {
        method: 'PUT',
        headers: {'Content-Length': String(casPayload.length)}
      },
      response => {
        response.resume()
        response.once('end', () => resolve(response.statusCode ?? 0))
      }
    )
    casRequest.once('error', reject)
    casRequest.write(casPayload.subarray(0, 1))
  })

  const integrity = (
    running.server as unknown as {
      integrity: {activeCasWrites: number}
    }
  ).integrity
  for (
    let attempt = 0;
    attempt < 100 && integrity.activeCasWrites !== 1;
    attempt += 1
  ) {
    await new Promise(resolve => setTimeout(resolve, 5))
  }
  assert.equal(integrity.activeCasWrites, 1)

  const acController = new AbortController()
  const acResponse = fetch(`${running.baseUrl}/cache/ac/${'8'.repeat(64)}`, {
    method: 'PUT',
    body: Buffer.from('action'),
    signal: acController.signal
  })
  await acBlobUploaded.promise

  // Before the fix, the AC request waited for activeCasWrites to reach zero
  // while retaining the only upload slot. Completing this CAS body then left
  // the CAS waiting for that same slot and deadlocked both requests.
  casRequest.end(casPayload.subarray(1))
  let timeout: NodeJS.Timeout | undefined
  try {
    const statuses = await Promise.race([
      Promise.all([casResponse, acResponse.then(response => response.status)]),
      new Promise<never>((_resolve, reject) => {
        timeout = setTimeout(() => {
          acController.abort()
          casRequest.destroy()
          reject(
            new Error('CAS/AC upload lock inversion did not make progress')
          )
        }, 1000)
      })
    ])
    assert.deepEqual(statuses, [204, 204])
  } finally {
    if (timeout !== undefined) clearTimeout(timeout)
  }

  assert.deepEqual(
    backend.uploadCalls.map(call =>
      call.key.includes('-ac-sha256-') ? 'ac' : 'cas'
    ),
    ['ac', 'cas']
  )
  assert.deepEqual(
    backend.finalizeCalls.map(call =>
      call.key.includes('-ac-sha256-') ? 'ac' : 'cas'
    ),
    ['cas', 'ac']
  )
})

test('health is loopback-only and shutdown requires the private bearer token', async t => {
  let shutdownCalls = 0
  const running = await startServer(new MemoryBackend(), {}, () => {
    shutdownCalls += 1
  })
  t.after(running.stop)

  const health = await fetch(`${running.baseUrl}/healthz`)
  assert.equal(health.status, 200)
  const healthBody = (await health.json()) as {
    status: string
    clientAbortedRequests: number
  }
  assert.equal(healthBody.status, 'ok')
  assert.equal(healthBody.clientAbortedRequests, 0)

  const denied = await fetch(`${running.baseUrl}/shutdown`, {method: 'POST'})
  assert.equal(denied.status, 401)
  assert.equal(shutdownCalls, 0)

  const accepted = await fetch(`${running.baseUrl}/shutdown`, {
    method: 'POST',
    headers: {Authorization: `Bearer ${running.config.shutdownToken}`}
  })
  assert.equal(accepted.status, 202)
  await new Promise(resolve => setImmediate(resolve))
  assert.equal(shutdownCalls, 1)
})

test(
  'shutdown aborts an active request after the drain deadline',
  {timeout: 1000},
  async t => {
    const backend = new MemoryBackend()
    let backendAborted = false
    backend.lookupHook = async (_key, _version, signal) => {
      assert.ok(signal)
      return new Promise<CacheLookup>((_resolve, reject) => {
        signal.addEventListener(
          'abort',
          () => {
            backendAborted = true
            reject(new Error('lookup aborted by shutdown'))
          },
          {once: true}
        )
      })
    }
    const running = await startServer(backend, {}, () => {}, 20)
    const clientController = new AbortController()
    t.after(() => clientController.abort())
    t.after(running.stop)

    const requestFailed = fetch(
      `${running.baseUrl}/cache/cas/${'9'.repeat(64)}`,
      {signal: clientController.signal}
    ).then(
      () => false,
      () => true
    )
    await waitFor(
      () => backend.lookupCalls.length === 1,
      'active lookup did not start'
    )

    await running.server.shutdown()

    assert.equal(backendAborted, true)
    assert.equal(await requestFailed, true)
    const stats = running.metrics.snapshot()
    assert.equal(stats.requests.aborted, 0)
    assert.equal(stats.reads.cas.errors, 1)
    await running.diagnostics.flush()
    const events = (await readFile(running.diagnostics.filePath, 'utf8'))
      .trim()
      .split('\n')
      .map(line => JSON.parse(line) as Record<string, unknown>)
    assert.equal(
      events.some(event => event.area === 'http'),
      true
    )
  }
)
