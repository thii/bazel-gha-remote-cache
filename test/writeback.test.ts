import assert from 'node:assert/strict'
import {createHash, randomUUID} from 'node:crypto'
import {mkdtemp, readFile, readdir, rm, stat, writeFile} from 'node:fs/promises'
import {tmpdir} from 'node:os'
import path from 'node:path'
import test, {type TestContext} from 'node:test'
import {
  BackendError,
  type CacheBackend,
  type CacheLookup,
  type CacheReservation
} from '../src/backend.js'
import {Metrics} from '../src/metrics.js'
import type {CacheKind, DaemonConfig} from '../src/model.js'
import {
  entriesInPayloadOrder,
  parsePack,
  readPackPayload
} from '../src/pack-format.js'
import {EntryPacer, type EntryPacerSleep} from '../src/pacer.js'
import {WriteBackQueue} from '../src/writeback.js'

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

interface BackendCall {
  key: string
  version?: string
  size?: number
}

type LookupHook = (
  key: string,
  version: string,
  signal?: AbortSignal
) => Promise<CacheLookup>
type ReserveHook = (
  key: string,
  version: string,
  signal?: AbortSignal
) => Promise<CacheReservation>
type UploadHook = (
  key: string,
  filePath: string,
  size: number,
  signal?: AbortSignal
) => Promise<void>
type FinalizeHook = (
  key: string,
  version: string,
  size: number,
  signal?: AbortSignal
) => Promise<void>

class FakeBackend implements CacheBackend {
  readonly objects = new Map<string, Buffer>()
  readonly staged = new Map<string, Buffer>()
  readonly events: string[] = []
  readonly lookupCalls: BackendCall[] = []
  readonly reserveCalls: BackendCall[] = []
  readonly uploadCalls: BackendCall[] = []
  readonly finalizeCalls: BackendCall[] = []
  lookupHook?: LookupHook
  reserveHook?: ReserveHook
  uploadHook?: UploadHook
  finalizeHook?: FinalizeHook

  async lookup(
    key: string,
    version: string,
    signal?: AbortSignal
  ): Promise<CacheLookup> {
    this.throwIfAborted(signal, 'lookup')
    this.lookupCalls.push({key, version})
    this.events.push(`lookup:${key}`)
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
    this.throwIfAborted(signal, 'reservation')
    this.reserveCalls.push({key, version})
    this.events.push(`reserve:${key}`)
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
    this.throwIfAborted(signal, 'upload')
    const key = this.keyFromUrl(signedUrl)
    this.uploadCalls.push({key, size})
    this.events.push(`upload:${key}`)
    if (this.uploadHook) return this.uploadHook(key, filePath, size, signal)
    const value = await readFile(filePath)
    assert.equal(value.length, size)
    this.staged.set(key, value)
  }

  async finalize(
    key: string,
    version: string,
    size: number,
    signal?: AbortSignal
  ): Promise<void> {
    this.throwIfAborted(signal, 'finalization')
    this.finalizeCalls.push({key, version, size})
    this.events.push(`finalize:${key}`)
    if (this.finalizeHook) {
      return this.finalizeHook(key, version, size, signal)
    }
    const value = this.staged.get(key)
    if (!value || value.length !== size) {
      throw new BackendError('no matching staged object')
    }
    this.objects.set(key, value)
    this.staged.delete(key)
  }

  async openDownload(
    signedUrl: string,
    signal?: AbortSignal
  ): Promise<Response> {
    this.throwIfAborted(signal, 'download')
    const value = this.objects.get(this.keyFromUrl(signedUrl))
    if (!value) throw new BackendError('missing object')
    return new Response(Uint8Array.from(value), {
      headers: {'Content-Length': String(value.length)}
    })
  }

  async openDownloadRange(
    signedUrl: string,
    offset: number,
    length: number,
    signal?: AbortSignal
  ): Promise<Response> {
    this.throwIfAborted(signal, 'range download')
    const value = this.objects.get(this.keyFromUrl(signedUrl))
    if (!value || offset < 0 || length < 1 || offset + length > value.length) {
      throw new BackendError('invalid object range')
    }
    const body = value.subarray(offset, offset + length)
    return new Response(Uint8Array.from(body), {
      status: 206,
      headers: {
        'Content-Length': String(body.length),
        'Content-Range': `bytes ${offset}-${offset + length - 1}/${value.length}`
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
    const reservation = await this.reserve(key, version, signal)
    if (reservation.kind === 'conflict') return 'already-exists'
    if (!reservation.uploadUrl) {
      throw new BackendError('reservation omitted an upload URL')
    }
    await this.uploadFile(reservation.uploadUrl, filePath, size, signal)
    await this.finalize(key, version, size, signal)
    return 'created'
  }

  private urlFor(key: string): string {
    return `https://blob.invalid/${encodeURIComponent(key)}`
  }

  private keyFromUrl(url: string): string {
    return decodeURIComponent(new URL(url).pathname.slice(1))
  }

  private throwIfAborted(
    signal: AbortSignal | undefined,
    operation: string
  ): void {
    if (signal?.aborted) throw new Error(`${operation} aborted`)
  }
}

interface QueueDependencies {
  now?: () => number
  queueSleep?: (milliseconds: number, signal?: AbortSignal) => Promise<void>
  pacerSleep?: EntryPacerSleep
}

interface QueueHarness {
  backend: FakeBackend
  config: DaemonConfig
  metrics: Metrics
  pacer: EntryPacer
  queue: WriteBackQueue
}

interface TestWorkspace {
  directory: string
  createQueue(
    overrides?: Partial<DaemonConfig>,
    backend?: FakeBackend,
    dependencies?: QueueDependencies
  ): Promise<QueueHarness>
}

async function workspace(t: TestContext): Promise<TestWorkspace> {
  const directory = await mkdtemp(path.join(tmpdir(), 'brc-writeback-test-'))
  const queues: QueueHarness[] = []
  t.after(async () => {
    for (const harness of queues.toReversed()) {
      if (!harness.pacer.closed) {
        await harness.queue.drain(0).catch(() => {})
      }
    }
    await rm(directory, {recursive: true, force: true})
  })

  return {
    directory,
    createQueue: async (
      overrides = {},
      backend = new FakeBackend(),
      dependencies = {}
    ) => {
      const config: DaemonConfig = {
        namespace: 'writeback-test',
        storageMode: 'pack',
        port: 0,
        readable: true,
        writable: true,
        maxObjectSize: 1024 * 1024,
        maxInflightBytes: 1024 * 1024,
        maxPendingBytes: 1024 * 1024,
        uploadConcurrency: 2,
        downloadConcurrency: 2,
        repositoryUploadBudget: 120,
        expectedWriters: 1,
        uploadBurst: 20,
        writeBack: true,
        flushTimeoutSeconds: 5,
        packTargetBytes: 64 * 1024,
        packMaxObjects: 256,
        packMaxAgeSeconds: 300,
        catalogRefreshSeconds: 15,
        remoteTimeoutSeconds: 5,
        failJobOnCacheError: false,
        githubApiUrl: 'https://api.github.com',
        githubRepository: 'test/writeback',
        currentRef: 'refs/heads/main',
        defaultRef: 'refs/heads/main',
        runId: '1',
        jobHash: '0'.repeat(16),
        controlDirectory: directory,
        shutdownToken: 'a'.repeat(43),
        instanceId: randomUUID(),
        ...overrides
      }
      const metrics = new Metrics(true, true)
      const pacer = new EntryPacer({
        repositoryUploadBudget: config.repositoryUploadBudget,
        expectedWriters: config.expectedWriters,
        uploadBurst: config.uploadBurst,
        ...(dependencies.now === undefined ? {} : {now: dependencies.now}),
        ...(dependencies.pacerSleep === undefined
          ? {}
          : {sleep: dependencies.pacerSleep}),
        random: () => 0
      })
      const queue = new WriteBackQueue({
        config,
        backend,
        metrics,
        pacer,
        ...(dependencies.now === undefined ? {} : {now: dependencies.now}),
        ...(dependencies.queueSleep === undefined
          ? {}
          : {sleep: dependencies.queueSleep})
      })
      const harness = {backend, config, metrics, pacer, queue}
      queues.push(harness)
      await queue.start()
      return harness
    }
  }
}

function sha256(value: Uint8Array | string): string {
  return createHash('sha256').update(value).digest('hex')
}

let spoolSequence = 0

async function createSpool(
  directory: string,
  value: Buffer | string
): Promise<{body: Buffer; bodySha256: string; path: string}> {
  const body = Buffer.isBuffer(value) ? value : Buffer.from(value)
  const spoolPath = path.join(directory, `input-${spoolSequence++}.spool`)
  await writeFile(spoolPath, body, {mode: 0o600})
  return {body, bodySha256: sha256(body), path: spoolPath}
}

async function acceptValue(
  queue: WriteBackQueue,
  directory: string,
  kind: CacheKind,
  value: Buffer | string,
  digest?: string
): Promise<{
  acceptance: Awaited<ReturnType<WriteBackQueue['accept']>>
  body: Buffer
  bodySha256: string
  digest: string
  spoolPath: string
}> {
  const spool = await createSpool(directory, value)
  const objectDigest = digest ?? spool.bodySha256
  const acceptance = await queue.accept(
    kind,
    objectDigest,
    spool.path,
    spool.body.length,
    spool.bodySha256
  )
  return {
    acceptance,
    body: spool.body,
    bodySha256: spool.bodySha256,
    digest: objectDigest,
    spoolPath: spool.path
  }
}

async function readLocal(
  queue: WriteBackQueue,
  kind: CacheKind,
  digest: string
): Promise<Buffer | undefined> {
  const local = await queue.openLocal(kind, digest)
  if (local === undefined) return undefined
  const chunks: Buffer[] = []
  for await (const chunk of local.stream) chunks.push(Buffer.from(chunk))
  const value = Buffer.concat(chunks)
  assert.equal(value.length, local.size)
  return value
}

async function waitFor(
  condition: () => boolean,
  failureMessage: string
): Promise<void> {
  for (let attempt = 0; attempt < 500; attempt += 1) {
    if (condition()) return
    await new Promise(resolve => setTimeout(resolve, 2))
  }
  throw new Error(failureMessage)
}

function withAbort<T>(promise: Promise<T>, signal?: AbortSignal): Promise<T> {
  if (signal === undefined) return promise
  if (signal.aborted) return Promise.reject(new Error('operation aborted'))
  return new Promise<T>((resolve, reject) => {
    const onAbort = (): void => reject(new Error('operation aborted'))
    signal.addEventListener('abort', onAbort, {once: true})
    promise.then(
      value => {
        signal.removeEventListener('abort', onAbort)
        resolve(value)
      },
      error => {
        signal.removeEventListener('abort', onAbort)
        reject(error)
      }
    )
  })
}

function waitForWorkerSignal(
  milliseconds: number,
  signal?: AbortSignal
): Promise<void> {
  if (signal === undefined) {
    return new Promise(resolve =>
      setTimeout(resolve, Math.min(2, milliseconds))
    )
  }
  if (signal.aborted) return Promise.reject(new Error('sleep aborted'))
  return new Promise((_resolve, reject) => {
    signal.addEventListener('abort', () => reject(new Error('sleep aborted')), {
      once: true
    })
  })
}

test('accept durably spools an object before acknowledgement and serves it locally', async t => {
  const testWorkspace = await workspace(t)
  const backend = new FakeBackend()
  const reserveStarted = new Deferred<void>()
  const reservation = new Deferred<CacheReservation>()
  backend.reserveHook = async (_key, _version, signal) => {
    reserveStarted.resolve()
    return withAbort(reservation.promise, signal)
  }
  const {queue, metrics} = await testWorkspace.createQueue(
    {storageMode: 'object'},
    backend
  )
  t.after(() =>
    reservation.resolve({
      kind: 'reserved',
      uploadUrl: 'https://blob.invalid/released'
    })
  )

  const accepted = await acceptValue(
    queue,
    testWorkspace.directory,
    'cas',
    'locally durable bytes'
  )
  assert.deepEqual(accepted.acceptance, {kind: 'accepted'})
  await reserveStarted.promise

  const pendingNames = await readdir(
    path.join(testWorkspace.directory, 'writeback', 'pending')
  )
  assert.equal(pendingNames.filter(name => name.endsWith('.json')).length, 1)
  assert.equal(pendingNames.filter(name => name.endsWith('.data')).length, 1)
  assert.deepEqual(
    await readLocal(queue, 'cas', accepted.digest),
    accepted.body
  )
  assert.equal(metrics.snapshot().writeBack.acceptedObjects, 1)

  reservation.resolve({
    kind: 'reserved',
    uploadUrl: `https://blob.invalid/${encodeURIComponent(backend.reserveCalls[0]!.key)}`
  })
  const drained = await queue.drain(2000)
  assert.equal(drained.pendingObjects, 0)
})

test('mixed packs preserve acceptance order and CAS-before-AC barriers', async t => {
  const testWorkspace = await workspace(t)
  const {backend, queue, metrics} = await testWorkspace.createQueue(
    {packMaxObjects: 3},
    undefined,
    {queueSleep: waitForWorkerSignal}
  )
  const firstCas = await acceptValue(
    queue,
    testWorkspace.directory,
    'cas',
    'first CAS'
  )
  const secondCas = await acceptValue(
    queue,
    testWorkspace.directory,
    'cas',
    'second CAS'
  )
  const action = await acceptValue(
    queue,
    testWorkspace.directory,
    'ac',
    'action result',
    sha256('action key')
  )

  const pendingDirectory = path.join(
    testWorkspace.directory,
    'writeback',
    'pending'
  )
  const manifests = await Promise.all(
    (await readdir(pendingDirectory))
      .filter(name => name.endsWith('.json'))
      .sort()
      .map(
        async name =>
          JSON.parse(
            await readFile(path.join(pendingDirectory, name), 'utf8')
          ) as Record<string, unknown>
      )
  )
  assert.deepEqual(
    manifests.map(manifest => ({
      kind: manifest['kind'],
      casSequence: manifest['casSequence'],
      casBarrier: manifest['casBarrier']
    })),
    [
      {kind: 'cas', casSequence: '1', casBarrier: undefined},
      {kind: 'cas', casSequence: '2', casBarrier: undefined},
      {kind: 'ac', casSequence: undefined, casBarrier: '2'}
    ]
  )

  const drained = await queue.drain(2000)
  assert.equal(drained.pendingObjects, 0)
  assert.equal(drained.durableCasHighWatermark, '2')
  assert.equal(backend.finalizeCalls.length, 1)
  assert.equal(backend.objects.size, 1)

  const packBytes = [...backend.objects.values()][0]
  assert.ok(packBytes)
  const parsed = parsePack(packBytes)
  const payloadOrder = entriesInPayloadOrder(parsed.entries)
  assert.deepEqual(
    payloadOrder.map(entry => ({
      kind: entry.kind,
      digest: Buffer.from(entry.digest).toString('hex')
    })),
    [
      {kind: 'cas', digest: firstCas.digest},
      {kind: 'cas', digest: secondCas.digest},
      {kind: 'ac', digest: action.digest}
    ]
  )
  assert.deepEqual(
    payloadOrder.map(entry => Buffer.from(readPackPayload(packBytes, entry))),
    [firstCas.body, secondCas.body, action.body]
  )
  assert.equal(metrics.snapshot().writeBack.packedObjects, 3)
  assert.equal(metrics.snapshot().writeBack.packsFinalized, 1)
  assert.equal(metrics.snapshot().writeBack.acBlockedByBarrier, 0)
})

test('pack-age waits cancel superseded timers during staggered acceptance', async t => {
  const testWorkspace = await workspace(t)
  let activeSleeps = 0
  let peakSleeps = 0
  let abortedSleeps = 0
  const cancellableSleep = (
    milliseconds: number,
    signal?: AbortSignal
  ): Promise<void> => {
    if (signal === undefined) {
      return new Promise(resolve =>
        setTimeout(resolve, Math.min(milliseconds, 2))
      )
    }
    if (signal.aborted) return Promise.reject(new Error('sleep aborted'))
    activeSleeps += 1
    peakSleeps = Math.max(peakSleeps, activeSleeps)
    return new Promise((_resolve, reject) => {
      const onAbort = (): void => {
        activeSleeps -= 1
        abortedSleeps += 1
        reject(new Error('sleep aborted'))
      }
      signal.addEventListener('abort', onAbort, {once: true})
    })
  }
  const {queue} = await testWorkspace.createQueue(
    {packMaxObjects: 256, packMaxAgeSeconds: 300},
    undefined,
    {queueSleep: cancellableSleep}
  )

  await acceptValue(queue, testWorkspace.directory, 'cas', 'object 0')
  await waitFor(() => activeSleeps === 1, 'first pack-age wait did not start')
  for (let index = 1; index <= 12; index += 1) {
    await acceptValue(queue, testWorkspace.directory, 'cas', `object ${index}`)
    await waitFor(
      () => activeSleeps === 1 && abortedSleeps === index,
      'superseded pack-age wait was not canceled'
    )
  }

  assert.equal(peakSleeps, 1)
  const drained = await queue.drain(2000)
  assert.equal(drained.pendingObjects, 0)
  assert.equal(activeSleeps, 0)
  assert.equal(abortedSleeps, 13)
})

test('pack-age timeout removes its superseded change waiter', async t => {
  const testWorkspace = await workspace(t)
  const backend = new FakeBackend()
  const reserveStarted = new Deferred<void>()
  const reservation = new Deferred<CacheReservation>()
  backend.reserveHook = async (_key, _version, signal) => {
    reserveStarted.resolve()
    return withAbort(reservation.promise, signal)
  }
  let now = 0
  let finishAgeSleep: (() => void) | undefined
  const controlledSleep = (
    milliseconds: number,
    signal?: AbortSignal
  ): Promise<void> => {
    if (signal === undefined) {
      return new Promise(resolve =>
        setTimeout(resolve, Math.min(milliseconds, 2))
      )
    }
    if (signal.aborted) return Promise.reject(new Error('sleep aborted'))
    return new Promise((resolve, reject) => {
      const onAbort = (): void => reject(new Error('sleep aborted'))
      signal.addEventListener('abort', onAbort, {once: true})
      finishAgeSleep = () => {
        signal.removeEventListener('abort', onAbort)
        now += milliseconds
        resolve()
      }
    })
  }
  const {queue} = await testWorkspace.createQueue(
    {packMaxObjects: 256, packMaxAgeSeconds: 300},
    backend,
    {now: () => now, queueSleep: controlledSleep}
  )

  await acceptValue(queue, testWorkspace.directory, 'cas', 'age timeout')
  await waitFor(
    () => finishAgeSleep !== undefined,
    'pack-age timeout did not start'
  )
  finishAgeSleep?.()
  await reserveStarted.promise
  assert.equal(
    (
      queue as unknown as {
        changeWaiters: unknown[]
      }
    ).changeWaiters.length,
    0
  )

  reservation.resolve({
    kind: 'reserved',
    uploadUrl: `https://blob.invalid/${encodeURIComponent(backend.reserveCalls[0]!.key)}`
  })
  const drained = await queue.drain(2000)
  assert.equal(drained.pendingObjects, 0)
})

test('deduplication and body conflicts remain job-wide after remote commit', async t => {
  const testWorkspace = await workspace(t)
  const {backend, queue, metrics} = await testWorkspace.createQueue({
    storageMode: 'object'
  })
  const digest = sha256('stable action key')
  const original = await acceptValue(
    queue,
    testWorkspace.directory,
    'ac',
    'same body',
    digest
  )
  assert.deepEqual(original.acceptance, {kind: 'accepted'})
  await waitFor(
    () => queue.snapshot().pendingObjects === 0,
    'original object did not become remotely durable'
  )

  const duplicate = await acceptValue(
    queue,
    testWorkspace.directory,
    'ac',
    'same body',
    digest
  )
  const conflict = await acceptValue(
    queue,
    testWorkspace.directory,
    'ac',
    'different body',
    digest
  )
  assert.deepEqual(duplicate.acceptance, {kind: 'duplicate'})
  assert.deepEqual(conflict.acceptance, {kind: 'conflict'})
  await assert.rejects(stat(duplicate.spoolPath), {code: 'ENOENT'})
  await assert.rejects(stat(conflict.spoolPath), {code: 'ENOENT'})
  assert.equal(backend.finalizeCalls.length, 1)
  const snapshot = metrics.snapshot()
  assert.equal(snapshot.writeBack.acceptedObjects, 1)
  assert.equal(snapshot.writeBack.deduplicatedObjects, 1)
  await queue.drain(1000)
})

test('max-pending-bytes rejects new data without disturbing queued objects', async t => {
  const testWorkspace = await workspace(t)
  const backend = new FakeBackend()
  const reservation = new Deferred<CacheReservation>()
  backend.reserveHook = (_key, _version, signal) =>
    withAbort(reservation.promise, signal)
  const {queue} = await testWorkspace.createQueue(
    {
      storageMode: 'object',
      maxObjectSize: 4,
      maxPendingBytes: 4,
      packTargetBytes: 4
    },
    backend
  )

  const accepted = await acceptValue(
    queue,
    testWorkspace.directory,
    'cas',
    '1234'
  )
  const full = await acceptValue(
    queue,
    testWorkspace.directory,
    'ac',
    'x',
    sha256('full action')
  )
  assert.deepEqual(accepted.acceptance, {kind: 'accepted'})
  assert.deepEqual(full.acceptance, {kind: 'full'})
  assert.equal((await stat(full.spoolPath)).size, 1)
  assert.equal(queue.snapshot().pendingObjects, 1)
  assert.equal(queue.snapshot().pendingBytes, 4)

  reservation.resolve({
    kind: 'reserved',
    uploadUrl: `https://blob.invalid/${encodeURIComponent(backend.reserveCalls[0]!.key)}`
  })
  await queue.drain(2000)
})

test('admission pressure evicts remotely durable local sources first', async t => {
  const testWorkspace = await workspace(t)
  const {queue} = await testWorkspace.createQueue({
    storageMode: 'object',
    maxObjectSize: 4,
    maxPendingBytes: 4,
    packTargetBytes: 4
  })

  const accepted = await acceptValue(
    queue,
    testWorkspace.directory,
    'cas',
    '1234'
  )
  assert.deepEqual(accepted.acceptance, {kind: 'accepted'})
  await waitFor(
    () => queue.snapshot().pendingObjects === 0,
    'object did not become remotely durable'
  )
  assert.equal(queue.snapshot().pendingBytes, 0)
  assert.deepEqual(
    await readLocal(queue, 'cas', accepted.digest),
    accepted.body
  )
  const staleManifest = path.join(
    testWorkspace.directory,
    'writeback',
    'pending',
    `000000000000-cas-${accepted.digest}.json`
  )
  await writeFile(staleManifest, '{}\n', 'utf8')

  const replacement = await acceptValue(
    queue,
    testWorkspace.directory,
    'cas',
    '5678'
  )
  assert.deepEqual(replacement.acceptance, {kind: 'accepted'})
  assert.equal(await queue.openLocal('cas', accepted.digest), undefined)
  await assert.rejects(stat(staleManifest), {code: 'ENOENT'})

  await queue.drain(2000)
  assert.deepEqual(
    await readdir(path.join(testWorkspace.directory, 'writeback', 'pending')),
    []
  )
})

test('drain seals and flushes an underfilled pack', async t => {
  const testWorkspace = await workspace(t)
  const {backend, queue, metrics} = await testWorkspace.createQueue(
    {packMaxObjects: 256, packMaxAgeSeconds: 300},
    undefined,
    {queueSleep: waitForWorkerSignal}
  )
  await acceptValue(queue, testWorkspace.directory, 'cas', 'one')
  await acceptValue(queue, testWorkspace.directory, 'cas', 'two')
  await new Promise(resolve => setImmediate(resolve))
  assert.equal(backend.finalizeCalls.length, 0)

  const drained = await queue.drain(2000)
  assert.equal(drained.pendingObjects, 0)
  assert.equal(drained.accepting, false)
  assert.equal(backend.finalizeCalls.length, 1)
  assert.equal(metrics.snapshot().writeBack.remainingObjects, 0)
  assert.deepEqual(metrics.snapshot().writeBack.remainingObjectIds, [])

  const afterDrain = await acceptValue(
    queue,
    testWorkspace.directory,
    'ac',
    'late',
    sha256('late action')
  )
  assert.deepEqual(afterDrain.acceptance, {kind: 'failed'})
})

test('drain waits for an acceptance already ordered ahead of its cutoff', async t => {
  const testWorkspace = await workspace(t)
  const {backend, queue, metrics} = await testWorkspace.createQueue({
    storageMode: 'object'
  })
  const spool = await createSpool(
    testWorkspace.directory,
    'accepted before drain'
  )

  const acceptance = queue.accept(
    'cas',
    spool.bodySha256,
    spool.path,
    spool.body.length,
    spool.bodySha256
  )
  const draining = queue.drain(2000)
  const [accepted, drained] = await Promise.all([acceptance, draining])

  assert.deepEqual(accepted, {kind: 'accepted'})
  assert.equal(drained.pendingObjects, 0)
  assert.equal(drained.accepting, false)
  assert.equal(backend.finalizeCalls.length, 1)
  assert.equal(metrics.snapshot().writeBack.acceptedObjects, 1)
  assert.equal(metrics.snapshot().writeBack.remainingObjects, 0)
})

test('drain aborts a stalled remote write at its deadline and reports remaining data', async t => {
  const testWorkspace = await workspace(t)
  const backend = new FakeBackend()
  const uploadStarted = new Deferred<void>()
  let uploadAborted = false
  backend.uploadHook = async (_key, _path, _size, signal) => {
    uploadStarted.resolve()
    assert.ok(signal)
    await new Promise<void>((_resolve, reject) => {
      const onAbort = (): void => {
        uploadAborted = true
        reject(new Error('upload aborted'))
      }
      if (signal.aborted) onAbort()
      else signal.addEventListener('abort', onAbort, {once: true})
    })
  }
  const {queue, metrics} = await testWorkspace.createQueue(
    {storageMode: 'object'},
    backend
  )
  const stalled = await acceptValue(
    queue,
    testWorkspace.directory,
    'cas',
    'stalled upload'
  )
  await uploadStarted.promise

  const startedAt = Date.now()
  const drained = await queue.drain(25)
  const elapsed = Date.now() - startedAt
  assert.equal(uploadAborted, true)
  assert.equal(drained.pendingObjects, 1)
  assert.equal(drained.accepting, false)
  assert.equal(metrics.snapshot().writeBack.remainingObjects, 1)
  assert.deepEqual(metrics.snapshot().writeBack.remainingObjectIds, [
    `cas:${stalled.digest}`
  ])
  assert.ok(elapsed < 1000, `drain took ${elapsed} ms`)
})

test('reserve 429 honors Retry-After, checks visibility first, and recovers additively', async t => {
  const testWorkspace = await workspace(t)
  const backend = new FakeBackend()
  let now = 0
  let reserveAttempts = 0
  const advancingSleep = async (
    milliseconds: number,
    signal?: AbortSignal
  ): Promise<void> => {
    if (signal?.aborted) throw new Error('sleep aborted')
    now += milliseconds
  }
  backend.reserveHook = async key => {
    reserveAttempts += 1
    if (reserveAttempts === 1) {
      throw new BackendError('repository upload budget exhausted', {
        statusCode: 429,
        rateLimited: true,
        retryAfterMs: 5000
      })
    }
    return {
      kind: 'reserved',
      uploadUrl: `https://blob.invalid/${encodeURIComponent(key)}`
    }
  }
  backend.lookupHook = async key => ({
    kind: 'hit',
    downloadUrl: `https://blob.invalid/${encodeURIComponent(key)}`
  })
  const {queue, metrics, pacer} = await testWorkspace.createQueue(
    {storageMode: 'object'},
    backend,
    {
      now: () => now,
      queueSleep: advancingSleep,
      pacerSleep: advancingSleep
    }
  )

  await acceptValue(queue, testWorkspace.directory, 'cas', 'rate limited')
  await waitFor(
    () => queue.snapshot().pendingObjects === 0,
    'visibility recovery did not complete'
  )
  assert.equal(now, 5000)
  assert.equal(backend.reserveCalls.length, 1)
  assert.equal(backend.lookupCalls.length, 1)
  assert.equal(backend.uploadCalls.length, 0)
  assert.equal(backend.finalizeCalls.length, 0)
  assert.equal(pacer.currentEntriesPerMinute, 60)
  assert.equal(pacer.snapshot.totalSleepMs, 5000)
  let snapshot = metrics.snapshot()
  assert.equal(snapshot.backend.rateLimited, 1)
  assert.equal(snapshot.rateLimits.reserve, 1)
  assert.equal(snapshot.writeBack.reservationSleepMs, 5000)

  now += 60_000
  delete backend.lookupHook
  await acceptValue(queue, testWorkspace.directory, 'cas', 'clean minute')
  await waitFor(
    () => queue.snapshot().pendingObjects === 0,
    'clean post-limit commit did not complete'
  )
  assert.equal(pacer.currentEntriesPerMinute, 72)
  snapshot = metrics.snapshot()
  assert.equal(snapshot.writeBack.currentEntriesPerMinute, 72)
  assert.equal(backend.uploadCalls.length, 1)
  assert.equal(backend.finalizeCalls.length, 1)
  await queue.drain(1000)
})

test('start recovers durable manifests with CAS sequence and AC barrier state', async t => {
  const testWorkspace = await workspace(t)
  const first = await testWorkspace.createQueue(
    {packMaxObjects: 256, packMaxAgeSeconds: 300},
    undefined,
    {queueSleep: waitForWorkerSignal}
  )
  const cas = await acceptValue(
    first.queue,
    testWorkspace.directory,
    'cas',
    'recoverable CAS'
  )
  const action = await acceptValue(
    first.queue,
    testWorkspace.directory,
    'ac',
    'recoverable action',
    sha256('recoverable action key')
  )
  const interrupted = await first.queue.drain(0)
  assert.equal(interrupted.pendingObjects, 2)

  const secondBackend = new FakeBackend()
  const second = await testWorkspace.createQueue(
    {packMaxObjects: 256, packMaxAgeSeconds: 300},
    secondBackend,
    {queueSleep: waitForWorkerSignal}
  )
  assert.deepEqual(second.queue.snapshot(), {
    pendingObjects: 2,
    pendingBytes: cas.body.length + action.body.length,
    openPackObjects: 2,
    openPackBytes: cas.body.length + action.body.length,
    durableCasHighWatermark: '0',
    acceptedCasSequence: '1',
    rateLimitPauseUntil: null,
    accepting: true,
    failed: false
  })
  assert.deepEqual(await readLocal(second.queue, 'cas', cas.digest), cas.body)
  assert.deepEqual(
    await readLocal(second.queue, 'ac', action.digest),
    action.body
  )
  assert.equal(second.metrics.snapshot().writeBack.acceptedObjects, 0)
  assert.equal(second.metrics.snapshot().writeBack.pendingObjects, 2)

  const drained = await second.queue.drain(2000)
  assert.equal(drained.pendingObjects, 0)
  assert.equal(drained.durableCasHighWatermark, '1')
  assert.equal(secondBackend.finalizeCalls.length, 1)
  assert.deepEqual(
    await readdir(path.join(testWorkspace.directory, 'writeback', 'pending')),
    []
  )
})
