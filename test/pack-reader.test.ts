import assert from 'node:assert/strict'
import {createHash} from 'node:crypto'
import {mkdtemp, readFile, readdir, rm} from 'node:fs/promises'
import {tmpdir} from 'node:os'
import path from 'node:path'
import {setImmediate as nextTurn} from 'node:timers/promises'
import {test, type TestContext} from 'node:test'
import {
  BackendError,
  type CacheBackend,
  type CacheLookup,
  type CacheReservation
} from '../src/backend.js'
import {
  PackCatalog,
  type CatalogFetch,
  type PackKeyCodec
} from '../src/catalog.js'
import {DiagnosticJournal} from '../src/diagnostics.js'
import {metricsHaveCacheErrors} from '../src/lifecycle.js'
import {Metrics} from '../src/metrics.js'
import {
  PACK_BLOOM_BYTES,
  PACK_CACHE_VERSION,
  PACK_TRAILER_SIZE,
  buildPack,
  createPackCacheKey,
  findPackIndexEntry,
  packBloomMightContain,
  packCacheKeyPrefix,
  packIndexRange,
  packPayloadRange,
  packTrailerRange,
  tryParsePackCacheKey,
  type BuiltPack,
  type ParsedPackCacheKey
} from '../src/pack-format.js'
import {PackReader} from '../src/pack-reader.js'

const NAMESPACE_HASH = 'a81f7e9d'
const PACK_PREFIX = packCacheKeyPrefix(NAMESPACE_HASH)
const REF = 'refs/heads/main'

interface ListedPack {
  key: string
  size: number
  createdAt: string
  version?: string
}

interface LookupCall {
  key: string
  version: string
  signal: AbortSignal | undefined
}

interface RangeCall {
  url: string
  offset: number
  length: number
  signal: AbortSignal | undefined
}

type LookupImplementation = (
  key: string,
  version: string,
  signal?: AbortSignal
) => CacheLookup | Promise<CacheLookup>

type RangeImplementation = (
  url: string,
  offset: number,
  length: number,
  signal?: AbortSignal
) => Response | Promise<Response>

class FakeBackend implements CacheBackend {
  readonly lookupCalls: LookupCall[] = []
  readonly rangeCalls: RangeCall[] = []

  constructor(
    private readonly lookupImplementation: LookupImplementation,
    private readonly rangeImplementation: RangeImplementation
  ) {}

  async lookup(
    key: string,
    version: string,
    signal?: AbortSignal
  ): Promise<CacheLookup> {
    this.lookupCalls.push({key, version, signal})
    return this.lookupImplementation(key, version, signal)
  }

  async openDownloadRange(
    url: string,
    offset: number,
    length: number,
    signal?: AbortSignal
  ): Promise<Response> {
    this.rangeCalls.push({url, offset, length, signal})
    return this.rangeImplementation(url, offset, length, signal)
  }

  async reserve(
    _key: string,
    _version: string,
    _signal?: AbortSignal
  ): Promise<CacheReservation> {
    throw new Error('unexpected reserve')
  }

  async uploadFile(
    _signedUrl: string,
    _filePath: string,
    _size: number,
    _signal?: AbortSignal
  ): Promise<void> {
    throw new Error('unexpected upload')
  }

  async finalize(
    _key: string,
    _version: string,
    _size: number,
    _signal?: AbortSignal
  ): Promise<void> {
    throw new Error('unexpected finalize')
  }

  async openDownload(
    _signedUrl: string,
    _signal?: AbortSignal
  ): Promise<Response> {
    throw new Error('unexpected full download')
  }

  async commitFile(
    _key: string,
    _version: string,
    _filePath: string,
    _size: number,
    _signal?: AbortSignal
  ): Promise<'created' | 'already-exists'> {
    throw new Error('unexpected commit')
  }
}

const packCodec: PackKeyCodec<ParsedPackCacheKey> = {
  parse: tryParsePackCacheKey,
  mightContain(metadata, kind, digest) {
    return packBloomMightContain(metadata.bloom, kind, digest)
  }
}

function sha256(value: Uint8Array | string): Buffer {
  return createHash('sha256').update(value).digest()
}

function digest(value: Uint8Array | string): string {
  return sha256(value).toString('hex')
}

function packKey(
  built: BuiltPack,
  sequence: number,
  bloom: Uint8Array = built.bloom
): string {
  const packId = createHash('sha256')
    .update(built.bytes)
    .update(String(sequence))
    .digest('hex')
    .slice(0, 16)
  return createPackCacheKey({
    namespaceHash: NAMESPACE_HASH,
    runId: 73400291,
    jobHash: '18ca7318ca7318ca',
    sequence,
    bloom,
    packId
  })
}

function catalogFor(
  entries: readonly ListedPack[],
  fetchOverride?: CatalogFetch,
  overrides: Partial<{
    clock: () => number
    refreshIntervalMs: number
  }> = {}
): {catalog: PackCatalog<ParsedPackCacheKey>; calls: URL[]} {
  const calls: URL[] = []
  const fetchImplementation: CatalogFetch = async (input, init) => {
    calls.push(new URL(String(input)))
    if (fetchOverride !== undefined) return fetchOverride(input, init)
    return new Response(
      JSON.stringify({
        total_count: entries.length,
        actions_caches: entries.map((entry, index) => ({
          id: index + 1,
          key: entry.key,
          version: entry.version ?? PACK_CACHE_VERSION,
          ref: REF,
          size_in_bytes: entry.size,
          created_at: entry.createdAt,
          last_accessed_at: entry.createdAt
        }))
      }),
      {headers: {'Content-Type': 'application/json'}}
    )
  }
  return {
    calls,
    catalog: new PackCatalog({
      owner: 'octo-org',
      repository: 'cache-repo',
      token: 'github-token',
      keyPrefix: PACK_PREFIX,
      currentRef: REF,
      codec: packCodec,
      fetchImplementation,
      apiBaseUrl: 'https://github-api.example.test/',
      clock: overrides.clock ?? (() => 1_000),
      refreshIntervalMs: overrides.refreshIntervalMs ?? 60_000,
      requestTimeoutMs: 1_000
    })
  }
}

function rangeResponse(
  bytes: Uint8Array,
  offset: number,
  length: number
): Response {
  assert.ok(offset >= 0)
  assert.ok(length > 0)
  assert.ok(offset + length <= bytes.byteLength)
  return new Response(Buffer.from(bytes.subarray(offset, offset + length)), {
    status: 206,
    headers: {'Content-Length': String(length)}
  })
}

async function temporaryDirectory(t: TestContext): Promise<string> {
  const directory = await mkdtemp(path.join(tmpdir(), 'brc-pack-reader-'))
  t.after(() => rm(directory, {recursive: true, force: true}))
  return directory
}

async function waitUntil(predicate: () => boolean): Promise<void> {
  for (let attempt = 0; attempt < 50; attempt += 1) {
    if (predicate()) return
    await nextTurn()
  }
  assert.fail('condition did not become true')
}

test('catalog candidate is exactly looked up and materialized with three verified ranges', async t => {
  const casPayload = Buffer.from('supporting CAS payload')
  const actionPayload = Buffer.from('opaque ActionResult payload')
  const actionDigest = 'a'.repeat(64)
  const built = buildPack([
    {kind: 'cas', digest: digest(casPayload), payload: casPayload},
    {kind: 'ac', digest: actionDigest, payload: actionPayload}
  ])
  const key = packKey(built, 1)
  const {catalog, calls: catalogCalls} = catalogFor([
    {
      key,
      size: built.bytes.byteLength,
      createdAt: '2026-07-26T12:00:00.000Z'
    }
  ])
  const signedUrl = 'https://blob.example.test/pack?sig=one'
  const backend = new FakeBackend(
    async lookupKey => {
      assert.equal(lookupKey, key)
      return {kind: 'hit', downloadUrl: signedUrl}
    },
    async (url, offset, length) => {
      assert.equal(url, signedUrl)
      return rangeResponse(built.bytes, offset, length)
    }
  )
  const metrics = new Metrics(true, false)
  const reader = new PackReader({
    backend,
    catalog,
    metrics,
    directory: await temporaryDirectory(t),
    maxObjectSize: 1024
  })

  const object = await reader.materialize('ac', actionDigest)
  assert.ok(object)
  assert.equal(object.size, actionPayload.byteLength)
  assert.deepEqual(await readFile(object.path), actionPayload)

  assert.equal(catalogCalls.length, 1)
  assert.deepEqual(
    backend.lookupCalls.map(call => [call.key, call.version]),
    [[key, PACK_CACHE_VERSION]]
  )
  const trailer = packTrailerRange(built.bytes.byteLength)
  const index = packIndexRange(built.trailer)
  const actionEntry = findPackIndexEntry(built.entries, 'ac', actionDigest)
  assert.ok(actionEntry)
  const payload = packPayloadRange(actionEntry)
  assert.deepEqual(
    backend.rangeCalls.map(call => ({
      offset: call.offset,
      length: call.length
    })),
    [
      {offset: Number(trailer.offset), length: PACK_TRAILER_SIZE},
      {offset: Number(index.offset), length: Number(index.length)},
      {offset: Number(payload.offset), length: Number(payload.length)}
    ]
  )

  const snapshot = metrics.snapshot()
  assert.equal(snapshot.backend.lookups, 1)
  assert.equal(snapshot.backend.downloads, 3)
  assert.equal(snapshot.backend.errors, 0)
  assert.equal(snapshot.catalog.refreshes, 1)
  assert.equal(snapshot.catalog.bloomCandidates, 1)
  assert.equal(
    snapshot.catalog.rangeBytesDownloaded,
    PACK_TRAILER_SIZE + Number(index.length) + actionPayload.byteLength
  )
  await object.dispose()
})

test('cold concurrent reads single-flight the index and later reads reuse it', async t => {
  const payload = Buffer.from('shared packed CAS')
  const objectDigest = digest(payload)
  const built = buildPack([{kind: 'cas', digest: objectDigest, payload}])
  const key = packKey(built, 2)
  const {catalog, calls: catalogCalls} = catalogFor([
    {
      key,
      size: built.bytes.byteLength,
      createdAt: '2026-07-26T12:00:00.000Z'
    }
  ])
  let releaseLookup: (() => void) | undefined
  const lookupGate = new Promise<void>(resolve => {
    releaseLookup = resolve
  })
  const signedUrl = 'https://blob.example.test/single-flight'
  const backend = new FakeBackend(
    async () => {
      await lookupGate
      return {kind: 'hit', downloadUrl: signedUrl}
    },
    async (_url, offset, length) => rangeResponse(built.bytes, offset, length)
  )
  const metrics = new Metrics(true, false)
  const reader = new PackReader({
    backend,
    catalog,
    metrics,
    directory: await temporaryDirectory(t),
    maxObjectSize: 1024
  })

  const firstPromise = reader.materialize('cas', objectDigest)
  const secondPromise = reader.materialize('cas', objectDigest)
  await waitUntil(() => backend.lookupCalls.length === 1)
  assert.equal(catalogCalls.length, 1)
  assert.ok(releaseLookup)
  releaseLookup()
  const [first, second] = await Promise.all([firstPromise, secondPromise])
  assert.ok(first)
  assert.ok(second)
  assert.deepEqual(await readFile(first.path), payload)
  assert.deepEqual(await readFile(second.path), payload)

  const warm = await reader.materialize('cas', objectDigest)
  assert.ok(warm)
  assert.deepEqual(await readFile(warm.path), payload)
  assert.equal(backend.lookupCalls.length, 1)
  const objectEntry = findPackIndexEntry(built.entries, 'cas', objectDigest)
  assert.ok(objectEntry)
  const payloadOffset = Number(objectEntry.offset)
  assert.equal(
    backend.rangeCalls.filter(call => call.offset === payloadOffset).length,
    3
  )
  assert.equal(backend.rangeCalls.length, 5)

  await Promise.all([first.dispose(), second.dispose(), warm.dispose()])
})

test('an abandoned final index waiter cannot poison the next reader', async t => {
  const payload = Buffer.from('fresh flight after cancellation')
  const objectDigest = digest(payload)
  const built = buildPack([{kind: 'cas', digest: objectDigest, payload}])
  const key = packKey(built, 21)
  const {catalog} = catalogFor([
    {
      key,
      size: built.bytes.byteLength,
      createdAt: '2026-07-26T12:00:00.000Z'
    }
  ])
  let firstTransportAborted = false
  let settleFirstTransport: (() => void) | undefined
  let lookup = 0
  const signedUrl = 'https://blob.example.test/recovered-flight'
  const backend = new FakeBackend(
    async (_key, _version, signal) => {
      lookup += 1
      if (lookup > 1) return {kind: 'hit', downloadUrl: signedUrl}
      return new Promise<CacheLookup>((_resolve, reject) => {
        signal?.addEventListener(
          'abort',
          () => {
            firstTransportAborted = true
            settleFirstTransport = () => reject(new Error('transport aborted'))
          },
          {once: true}
        )
      })
    },
    async (_url, offset, length) => rangeResponse(built.bytes, offset, length)
  )
  const metrics = new Metrics(true, false)
  const reader = new PackReader({
    backend,
    catalog,
    metrics,
    directory: await temporaryDirectory(t),
    maxObjectSize: 1024
  })
  const controller = new AbortController()

  const abandoned = reader.materialize('cas', objectDigest, controller.signal)
  await waitUntil(() => backend.lookupCalls.length === 1)
  controller.abort()
  await assert.rejects(abandoned, /aborted/)
  assert.equal(firstTransportAborted, true)

  const recovered = await reader.materialize('cas', objectDigest)
  assert.ok(recovered)
  assert.equal(backend.lookupCalls.length, 2)
  assert.deepEqual(await readFile(recovered.path), payload)
  assert.ok(settleFirstTransport)
  settleFirstTransport()
  await nextTurn()
  assert.equal(metrics.snapshot().backend.errors, 0)
  await recovered.dispose()
})

test('a rate limit racing with an abandoned index waiter is still counted', async t => {
  const payload = Buffer.from('rate-limited abandoned flight')
  const objectDigest = digest(payload)
  const built = buildPack([{kind: 'cas', digest: objectDigest, payload}])
  const key = packKey(built, 22)
  const {catalog} = catalogFor([
    {
      key,
      size: built.bytes.byteLength,
      createdAt: '2026-07-26T12:00:00.000Z'
    }
  ])
  let rejectLookup: ((error: Error) => void) | undefined
  const backend = new FakeBackend(
    async () =>
      new Promise<CacheLookup>((_resolve, reject) => {
        rejectLookup = reject
      }),
    async () => assert.fail('a failed lookup must not download')
  )
  const metrics = new Metrics(true, false)
  const reader = new PackReader({
    backend,
    catalog,
    metrics,
    directory: await temporaryDirectory(t),
    maxObjectSize: 1024
  })
  const controller = new AbortController()

  const abandoned = reader.materialize('cas', objectDigest, controller.signal)
  await waitUntil(() => backend.lookupCalls.length === 1)
  controller.abort()
  await assert.rejects(abandoned, /aborted/)
  assert.ok(rejectLookup)
  rejectLookup(
    new BackendError('pack lookup rate limited', {
      statusCode: 429,
      rateLimited: true,
      retryable: true,
      retryAfterMs: 1000
    })
  )
  await waitUntil(() => metrics.snapshot().backend.rateLimited === 1)
  assert.equal(metrics.snapshot().backend.errors, 1)
})

test('a cold empty catalog allows one follow-up refresh, then applies the miss cooldown', async t => {
  let now = 1_000
  const {catalog, calls} = catalogFor([], undefined, {
    clock: () => now,
    refreshIntervalMs: 300_000
  })
  const backend = new FakeBackend(
    async () => assert.fail('an empty catalog must not perform exact lookup'),
    async () => assert.fail('an empty catalog must not download')
  )
  const reader = new PackReader({
    backend,
    catalog,
    metrics: new Metrics(true, false),
    directory: await temporaryDirectory(t),
    maxObjectSize: 1024
  })

  assert.equal(await reader.materialize('cas', 'd'.repeat(64)), undefined)
  assert.equal(await reader.materialize('cas', 'e'.repeat(64)), undefined)
  assert.equal(calls.length, 2)
  assert.equal(await reader.materialize('cas', 'f'.repeat(64)), undefined)
  assert.equal(calls.length, 2)

  now += 299_999
  assert.equal(await reader.materialize('cas', '1'.repeat(64)), undefined)
  assert.equal(calls.length, 2)
  now += 1
  assert.equal(await reader.materialize('cas', '2'.repeat(64)), undefined)
  assert.equal(calls.length, 3)

  assert.equal(backend.lookupCalls.length, 0)
  assert.equal(backend.rangeCalls.length, 0)
  assert.equal(catalog.metricsSnapshot().refreshAttempts, 3)
  assert.equal(catalog.metricsSnapshot().apparentMissRefreshes, 2)
})

test('a rate-limited catalog refresh served stale is counted exactly once', async t => {
  let now = 1_000
  let request = 0
  const {catalog, calls} = catalogFor(
    [],
    async () => {
      request += 1
      if (request === 1) {
        return new Response(
          JSON.stringify({total_count: 0, actions_caches: []}),
          {headers: {'Content-Type': 'application/json'}}
        )
      }
      return new Response('limited', {
        status: 403,
        headers: {'Retry-After': '7'}
      })
    },
    {clock: () => now, refreshIntervalMs: 100}
  )
  const backend = new FakeBackend(
    async () => assert.fail('an empty catalog must not perform exact lookup'),
    async () => assert.fail('an empty catalog must not download')
  )
  const metrics = new Metrics(true, false)
  const reader = new PackReader({
    backend,
    catalog,
    metrics,
    directory: await temporaryDirectory(t),
    maxObjectSize: 1024
  })

  assert.equal(await reader.materialize('cas', '3'.repeat(64)), undefined)
  now += 101
  assert.equal(await reader.materialize('cas', '4'.repeat(64)), undefined)
  assert.equal(calls.length, 2)

  let snapshot = metrics.snapshot()
  assert.equal(catalog.metricsSnapshot().rateLimitedResponses, 1)
  assert.equal(snapshot.backend.rateLimited, 1)
  assert.equal(snapshot.rateLimits.lookup, 1)
  assert.equal(snapshot.backend.errors, 0)

  assert.equal(await reader.materialize('cas', '5'.repeat(64)), undefined)
  assert.equal(calls.length, 2)
  snapshot = metrics.snapshot()
  assert.equal(snapshot.backend.rateLimited, 1)
  assert.equal(snapshot.rateLimits.lookup, 1)
})

test('Bloom false positives inspect the index without downloading a payload', async t => {
  const payload = Buffer.from('pack contains a different object')
  const built = buildPack([{kind: 'cas', digest: digest(payload), payload}])
  const missingDigest = 'f'.repeat(64)
  const key = packKey(built, 3, Buffer.alloc(PACK_BLOOM_BYTES, 0xff))
  const {catalog, calls: catalogCalls} = catalogFor([
    {
      key,
      size: built.bytes.byteLength,
      createdAt: '2026-07-26T12:00:00.000Z'
    }
  ])
  const backend = new FakeBackend(
    async () => ({
      kind: 'hit',
      downloadUrl: 'https://blob.example.test/false-positive'
    }),
    async (_url, offset, length) => rangeResponse(built.bytes, offset, length)
  )
  const metrics = new Metrics(true, false)
  const reader = new PackReader({
    backend,
    catalog,
    metrics,
    directory: await temporaryDirectory(t),
    maxObjectSize: 1024
  })

  assert.equal(await reader.materialize('cas', missingDigest), undefined)
  assert.equal(catalogCalls.length, 2)
  assert.equal(backend.lookupCalls.length, 1)
  assert.equal(backend.rangeCalls.length, 2)
  const snapshot = metrics.snapshot()
  assert.equal(snapshot.catalog.refreshes, 2)
  assert.equal(snapshot.catalog.bloomCandidates, 2)
  assert.equal(snapshot.catalog.bloomFalsePositives, 1)
  assert.equal(catalog.metricsSnapshot().bloomFalsePositives, 1)
  assert.equal(snapshot.backend.downloads, 2)
})

test('a corrupt candidate payload is rejected, removed, and falls back to an older pack', async t => {
  const payload = Buffer.from('verified fallback payload')
  const objectDigest = digest(payload)
  const built = buildPack([{kind: 'cas', digest: objectDigest, payload}])
  const corruptBytes = Buffer.from(built.bytes)
  corruptBytes[0]! ^= 1
  const corruptKey = packKey(built, 4)
  const validKey = packKey(built, 5)
  const {catalog} = catalogFor([
    {
      key: corruptKey,
      size: corruptBytes.byteLength,
      createdAt: '2026-07-26T12:00:02.000Z'
    },
    {
      key: validKey,
      size: built.bytes.byteLength,
      createdAt: '2026-07-26T12:00:01.000Z'
    }
  ])
  const bytesByUrl = new Map<string, Uint8Array>([
    ['https://blob.example.test/corrupt', corruptBytes],
    ['https://blob.example.test/valid', built.bytes]
  ])
  const backend = new FakeBackend(
    async key => ({
      kind: 'hit',
      downloadUrl:
        key === corruptKey
          ? 'https://blob.example.test/corrupt'
          : 'https://blob.example.test/valid'
    }),
    async (url, offset, length) => {
      const bytes = bytesByUrl.get(url)
      assert.ok(bytes)
      return rangeResponse(bytes, offset, length)
    }
  )
  const metrics = new Metrics(true, false)
  const directory = await temporaryDirectory(t)
  const reader = new PackReader({
    backend,
    catalog,
    metrics,
    directory,
    maxObjectSize: 1024
  })

  const object = await reader.materialize('cas', objectDigest)
  assert.ok(object)
  assert.deepEqual(await readFile(object.path), payload)
  assert.deepEqual(
    backend.lookupCalls.map(call => call.key),
    [corruptKey, validKey]
  )
  assert.equal(backend.rangeCalls.length, 6)
  assert.equal(metrics.snapshot().backend.errors, 1)
  assert.deepEqual(await readdir(directory), [path.basename(object.path)])
  await object.dispose()
  assert.deepEqual(await readdir(directory), [])
})

test('an expired signed URL refreshes the exact lookup, index, and payload once', async t => {
  const supportingPayload = Buffer.from('supporting object')
  const payload = Buffer.from('payload behind refreshed URL')
  const actionDigest = 'b'.repeat(64)
  const built = buildPack([
    {
      kind: 'cas',
      digest: digest(supportingPayload),
      payload: supportingPayload
    },
    {kind: 'ac', digest: actionDigest, payload}
  ])
  const key = packKey(built, 6)
  const {catalog} = catalogFor([
    {
      key,
      size: built.bytes.byteLength,
      createdAt: '2026-07-26T12:00:00.000Z'
    }
  ])
  const staleUrl = 'https://blob.example.test/stale'
  const freshUrl = 'https://blob.example.test/fresh'
  const entry = findPackIndexEntry(built.entries, 'ac', actionDigest)
  assert.ok(entry)
  const payloadRange = packPayloadRange(entry)
  let lookup = 0
  const backend = new FakeBackend(
    async () => {
      lookup += 1
      return {
        kind: 'hit',
        downloadUrl: lookup === 1 ? staleUrl : freshUrl
      }
    },
    async (url, offset, length) => {
      if (
        url === staleUrl &&
        offset === Number(payloadRange.offset) &&
        length === Number(payloadRange.length)
      ) {
        throw new BackendError('signed URL expired', {
          statusCode: 403,
          retryable: false
        })
      }
      return rangeResponse(built.bytes, offset, length)
    }
  )
  const metrics = new Metrics(true, false)
  const directory = await temporaryDirectory(t)
  const diagnostics = new DiagnosticJournal(
    path.join(directory, 'errors.ndjson')
  )
  const reader = new PackReader({
    backend,
    catalog,
    metrics,
    diagnostics,
    directory,
    maxObjectSize: 1024
  })

  const object = await reader.materialize('ac', actionDigest)
  assert.ok(object)
  assert.deepEqual(await readFile(object.path), payload)
  assert.equal(backend.lookupCalls.length, 2)
  assert.deepEqual(
    backend.rangeCalls.map(call => call.url),
    [staleUrl, staleUrl, staleUrl, freshUrl, freshUrl, freshUrl]
  )
  const snapshot = metrics.snapshot()
  assert.equal(snapshot.backend.lookups, 2)
  assert.equal(snapshot.backend.downloads, 6)
  assert.equal(snapshot.backend.errors, 0)
  assert.equal(snapshot.backend.rateLimited, 0)
  assert.equal(snapshot.catalog.signedUrlRefreshes, 1)
  await diagnostics.flush()
  assert.equal(await readFile(diagnostics.filePath, 'utf8'), '')
  metrics.stop()
  assert.equal(metricsHaveCacheErrors(metrics.snapshot()), false)
  await object.dispose()
})

test('a signed URL refresh that also gets 403 remains a cache error', async t => {
  const payload = Buffer.from('payload behind repeatedly denied URLs')
  const objectDigest = digest(payload)
  const built = buildPack([{kind: 'cas', digest: objectDigest, payload}])
  const key = packKey(built, 61)
  const {catalog} = catalogFor([
    {
      key,
      size: built.bytes.byteLength,
      createdAt: '2026-07-26T12:00:00.000Z'
    }
  ])
  const staleUrl = 'https://blob.example.test/stale-terminal'
  const freshUrl = 'https://blob.example.test/fresh-terminal'
  const entry = findPackIndexEntry(built.entries, 'cas', objectDigest)
  assert.ok(entry)
  const payloadRange = packPayloadRange(entry)
  let lookup = 0
  const backend = new FakeBackend(
    async () => ({
      kind: 'hit',
      downloadUrl: lookup++ === 0 ? staleUrl : freshUrl
    }),
    async (url, offset, length) => {
      if (
        offset === Number(payloadRange.offset) &&
        length === Number(payloadRange.length)
      ) {
        throw new BackendError(`signed URL denied: ${url}`, {
          statusCode: 403,
          retryable: false
        })
      }
      return rangeResponse(built.bytes, offset, length)
    }
  )
  const metrics = new Metrics(true, false)
  const directory = await temporaryDirectory(t)
  const diagnostics = new DiagnosticJournal(
    path.join(directory, 'errors.ndjson')
  )
  const reader = new PackReader({
    backend,
    catalog,
    metrics,
    diagnostics,
    directory,
    maxObjectSize: 1024
  })

  assert.equal(await reader.materialize('cas', objectDigest), undefined)
  assert.equal(backend.lookupCalls.length, 2)
  const snapshot = metrics.snapshot()
  assert.equal(snapshot.backend.downloads, 6)
  assert.equal(snapshot.backend.errors, 1)
  assert.equal(snapshot.catalog.signedUrlRefreshes, 1)
  await diagnostics.flush()
  const events = (await readFile(diagnostics.filePath, 'utf8'))
    .trim()
    .split('\n')
    .map(line => JSON.parse(line) as Record<string, unknown>)
  assert.equal(events.length, 1)
  assert.equal(events[0]?.area, 'pack-reader')
  assert.equal(events[0]?.operation, 'candidate')
  assert.equal(events[0]?.statusCode, 403)
  metrics.stop()
  assert.equal(metricsHaveCacheErrors(metrics.snapshot()), true)
})

test('cold readers share one refresh after an expired index URL', async t => {
  const payload = Buffer.from('payload behind an initially expired index URL')
  const objectDigest = digest(payload)
  const built = buildPack([{kind: 'cas', digest: objectDigest, payload}])
  const key = packKey(built, 611)
  const {catalog} = catalogFor([
    {
      key,
      size: built.bytes.byteLength,
      createdAt: '2026-07-26T12:00:00.000Z'
    }
  ])
  const staleUrl = 'https://blob.example.test/stale-index'
  const freshUrl = 'https://blob.example.test/fresh-index'
  const parallelReaders = 6
  let lookup = 0
  let staleRangeStarted!: () => void
  let releaseStaleRange!: () => void
  const staleStarted = new Promise<void>(resolve => {
    staleRangeStarted = resolve
  })
  const staleGate = new Promise<void>(resolve => {
    releaseStaleRange = resolve
  })
  const sharedExpiration = new BackendError('index URL expired', {
    statusCode: 403,
    retryable: false
  })
  const backend = new FakeBackend(
    async () => ({
      kind: 'hit',
      downloadUrl: lookup++ === 0 ? staleUrl : freshUrl
    }),
    async (url, offset, length) => {
      if (url === staleUrl) {
        staleRangeStarted()
        await staleGate
        throw sharedExpiration
      }
      return rangeResponse(built.bytes, offset, length)
    }
  )
  const metrics = new Metrics(true, false)
  const directory = await temporaryDirectory(t)
  const diagnostics = new DiagnosticJournal(
    path.join(directory, 'errors.ndjson')
  )
  const reader = new PackReader({
    backend,
    catalog,
    metrics,
    diagnostics,
    directory,
    maxObjectSize: 1024
  })

  const reads = Array.from({length: parallelReaders}, () =>
    reader.materialize('cas', objectDigest)
  )
  await staleStarted
  const flights = (
    reader as unknown as {
      indexFlights: Map<string, {waiters: number}>
    }
  ).indexFlights
  await waitUntil(() => flights.get(key)?.waiters === parallelReaders)
  releaseStaleRange()
  const objects = await Promise.all(reads)
  for (const object of objects) {
    assert.ok(object)
    assert.deepEqual(await readFile(object.path), payload)
    await object.dispose()
  }

  assert.equal(backend.lookupCalls.length, 2)
  const snapshot = metrics.snapshot()
  assert.equal(snapshot.backend.downloads, parallelReaders + 3)
  assert.equal(snapshot.backend.errors, 0)
  assert.equal(snapshot.catalog.signedUrlRefreshes, 1)
  await diagnostics.flush()
  assert.equal(await readFile(diagnostics.filePath, 'utf8'), '')
})

test('parallel readers share one refresh of an expired cached URL', async t => {
  const payload = Buffer.from('parallel payload behind an expired URL')
  const objectDigest = digest(payload)
  const built = buildPack([{kind: 'cas', digest: objectDigest, payload}])
  const key = packKey(built, 62)
  const {catalog} = catalogFor([
    {
      key,
      size: built.bytes.byteLength,
      createdAt: '2026-07-26T12:00:00.000Z'
    }
  ])
  const staleUrl = 'https://blob.example.test/stale-parallel'
  const freshUrl = 'https://blob.example.test/fresh-parallel'
  const entry = findPackIndexEntry(built.entries, 'cas', objectDigest)
  assert.ok(entry)
  const payloadRange = packPayloadRange(entry)
  const parallelReaders = 8
  let lookup = 0
  let expired = false
  let staleRequests = 0
  let releaseStaleRequests!: () => void
  let allStaleRequestsStarted!: () => void
  const staleGate = new Promise<void>(resolve => {
    releaseStaleRequests = resolve
  })
  const allStarted = new Promise<void>(resolve => {
    allStaleRequestsStarted = resolve
  })
  const backend = new FakeBackend(
    async () => ({
      kind: 'hit',
      downloadUrl: lookup++ === 0 ? staleUrl : freshUrl
    }),
    async (url, offset, length) => {
      if (
        expired &&
        url === staleUrl &&
        offset === Number(payloadRange.offset) &&
        length === Number(payloadRange.length)
      ) {
        staleRequests += 1
        if (staleRequests === parallelReaders) allStaleRequestsStarted()
        await staleGate
        throw new BackendError('cached signed URL expired', {
          statusCode: 403,
          retryable: false
        })
      }
      return rangeResponse(built.bytes, offset, length)
    }
  )
  const metrics = new Metrics(true, false)
  const directory = await temporaryDirectory(t)
  const diagnostics = new DiagnosticJournal(
    path.join(directory, 'errors.ndjson')
  )
  const reader = new PackReader({
    backend,
    catalog,
    metrics,
    diagnostics,
    directory,
    maxObjectSize: 1024
  })
  const warm = await reader.materialize('cas', objectDigest)
  assert.ok(warm)
  await warm.dispose()
  expired = true

  const reads = Array.from({length: parallelReaders}, () =>
    reader.materialize('cas', objectDigest)
  )
  await allStarted
  releaseStaleRequests()
  const objects = await Promise.all(reads)
  for (const object of objects) {
    assert.ok(object)
    assert.deepEqual(await readFile(object.path), payload)
    await object.dispose()
  }

  assert.equal(backend.lookupCalls.length, 2)
  const snapshot = metrics.snapshot()
  assert.equal(snapshot.backend.downloads, 21)
  assert.equal(snapshot.backend.errors, 0)
  assert.equal(snapshot.catalog.signedUrlRefreshes, 1)
  await diagnostics.flush()
  assert.equal(await readFile(diagnostics.filePath, 'utf8'), '')
})

test('download 429 and catalog REST 403 propagate Retry-After and operation metrics', async t => {
  const payload = Buffer.from('rate-limited payload')
  const objectDigest = digest(payload)
  const built = buildPack([{kind: 'cas', digest: objectDigest, payload}])
  const key = packKey(built, 7)
  const listed = {
    key,
    size: built.bytes.byteLength,
    createdAt: '2026-07-26T12:00:00.000Z'
  }

  const {catalog: downloadCatalog} = catalogFor([listed])
  const downloadLimit = new BackendError('range limited', {
    statusCode: 429,
    rateLimited: true,
    retryAfterMs: 9000
  })
  const downloadBackend = new FakeBackend(
    async () => ({
      kind: 'hit',
      downloadUrl: 'https://blob.example.test/limited'
    }),
    async () => {
      throw downloadLimit
    }
  )
  const downloadMetrics = new Metrics(true, false)
  const downloadReader = new PackReader({
    backend: downloadBackend,
    catalog: downloadCatalog,
    metrics: downloadMetrics,
    directory: await temporaryDirectory(t),
    maxObjectSize: 1024
  })
  await assert.rejects(
    downloadReader.materialize('cas', objectDigest),
    error => {
      assert.equal(error, downloadLimit)
      return true
    }
  )
  const downloadSnapshot = downloadMetrics.snapshot()
  assert.equal(downloadSnapshot.backend.rateLimited, 1)
  assert.equal(downloadSnapshot.backend.errors, 1)
  assert.equal(downloadSnapshot.rateLimits.download, 1)
  assert.equal(downloadSnapshot.rateLimits.lookup, 0)

  const {catalog: limitedCatalog} = catalogFor(
    [],
    async () =>
      new Response('limited', {
        status: 403,
        headers: {'Retry-After': '7'}
      })
  )
  const unusedBackend = new FakeBackend(
    async () => ({kind: 'miss'}),
    async () => assert.fail('catalog failure must not download')
  )
  const catalogMetrics = new Metrics(true, false)
  const catalogReader = new PackReader({
    backend: unusedBackend,
    catalog: limitedCatalog,
    metrics: catalogMetrics,
    directory: await temporaryDirectory(t),
    maxObjectSize: 1024
  })
  await assert.rejects(
    catalogReader.materialize('cas', objectDigest),
    error => {
      assert.ok(error instanceof BackendError)
      assert.equal(error.statusCode, 403)
      assert.equal(error.rateLimited, true)
      assert.equal(error.retryable, true)
      assert.equal(error.retryAfterMs, 7000)
      return true
    }
  )
  assert.equal(unusedBackend.lookupCalls.length, 0)
  const catalogSnapshot = catalogMetrics.snapshot()
  assert.equal(catalogSnapshot.backend.rateLimited, 1)
  assert.equal(catalogSnapshot.backend.errors, 1)
  assert.equal(catalogSnapshot.rateLimits.lookup, 1)
  assert.equal(catalogSnapshot.rateLimits.download, 0)
})

test('non-rate-limited catalog failures degrade to a cache miss', async t => {
  const {catalog} = catalogFor([], async () => {
    throw new Error('GitHub API unavailable')
  })
  const backend = new FakeBackend(
    async () => ({kind: 'miss'}),
    async () => assert.fail('catalog failure must not download')
  )
  const metrics = new Metrics(true, false)
  const reader = new PackReader({
    backend,
    catalog,
    metrics,
    directory: await temporaryDirectory(t),
    maxObjectSize: 1024
  })

  assert.equal(await reader.materialize('cas', 'c'.repeat(64)), undefined)
  assert.equal(backend.lookupCalls.length, 0)
  const snapshot = metrics.snapshot()
  assert.equal(snapshot.backend.errors, 1)
  assert.equal(snapshot.backend.rateLimited, 0)
  assert.equal(snapshot.rateLimits.lookup, 0)
  assert.equal(catalog.metricsSnapshot().refreshErrors, 1)
})
