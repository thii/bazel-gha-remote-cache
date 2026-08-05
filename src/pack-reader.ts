import {createHash, randomBytes} from 'node:crypto'
import {mkdir, open, rm} from 'node:fs/promises'
import path from 'node:path'
import {BackendError, type CacheBackend} from './backend.js'
import {
  ServerShutdownError,
  isSuppressibleAbortError,
  signalReason
} from './cancellation.js'
import {
  PackCatalog,
  PackCatalogError,
  type PackCatalogEntry,
  type PackCatalogMetrics
} from './catalog.js'
import type {DiagnosticJournal} from './diagnostics.js'
import {Metrics} from './metrics.js'
import type {CacheKind} from './model.js'
import {
  PACK_CACHE_VERSION,
  PACK_INDEX_ENTRY_SIZE,
  PACK_INDEX_HEADER_SIZE,
  PACK_TRAILER_SIZE,
  decodePackTrailer,
  findPackIndexEntry,
  packIndexRange,
  packPayloadRange,
  packTrailerRange,
  parsePackIndex,
  validatePackLayout,
  verifyPackPayloadSha256,
  type PackIndexEntry,
  type ParsedPackCacheKey
} from './pack-format.js'

export interface MaterializedPackObject {
  path: string
  size: number
  dispose(): Promise<void>
}

interface CachedPack {
  key: string
  size: number
  entries: readonly PackIndexEntry[]
  downloadUrl: string
}

interface IndexFlight {
  promise: Promise<CachedPack | undefined>
  controller: AbortController
  waiters: number
  settled: boolean
}

export interface PackReaderOptions {
  backend: CacheBackend
  catalog: PackCatalog<ParsedPackCacheKey>
  metrics: Metrics
  diagnostics?: DiagnosticJournal
  directory: string
  maxObjectSize: number
  indexCacheSize?: number
}

const MAX_PACK_OBJECTS = 256
const MAX_INDEX_BYTES =
  PACK_INDEX_HEADER_SIZE + PACK_INDEX_ENTRY_SIZE * MAX_PACK_OBJECTS

function safeRange(value: bigint, name: string): number {
  if (value > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new Error(`${name} exceeds the supported range`)
  }
  return Number(value)
}

function waitWithAbort<T>(
  promise: Promise<T>,
  signal?: AbortSignal
): Promise<T> {
  if (signal === undefined) return promise
  if (signal.aborted)
    return Promise.reject(signalReason(signal, 'pack read aborted'))
  return new Promise<T>((resolve, reject) => {
    const onAbort = (): void =>
      reject(signalReason(signal, 'pack read aborted'))
    signal.addEventListener('abort', onAbort, {once: true})
    void promise.then(
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

export class PackReader {
  private readonly indexCache = new Map<string, CachedPack>()
  private readonly indexFlights = new Map<string, IndexFlight>()
  private readonly deferredAuthenticationFailures = new WeakSet<BackendError>()
  private readonly indexCacheSize: number
  private lastCatalogMetrics: PackCatalogMetrics

  constructor(private readonly options: PackReaderOptions) {
    this.indexCacheSize = options.indexCacheSize ?? 128
    if (!Number.isSafeInteger(this.indexCacheSize) || this.indexCacheSize < 1) {
      throw new Error('pack index cache size must be positive')
    }
    this.lastCatalogMetrics = options.catalog.metricsSnapshot()
  }

  async materialize(
    kind: CacheKind,
    digest: string,
    signal?: AbortSignal
  ): Promise<MaterializedPackObject | undefined> {
    let candidates
    try {
      candidates = await this.options.catalog.candidates(kind, digest, signal)
    } catch (error) {
      this.syncCatalogMetrics()
      return this.catalogFailure(error, signal)
    }
    this.syncCatalogMetrics()
    const attemptedCandidates = new Set<string>()
    for (let refresh = 0; refresh < 2; refresh += 1) {
      for (const candidate of candidates.entries) {
        if (candidate.version !== PACK_CACHE_VERSION) continue
        const identity = `${candidate.key}\0${candidate.version}`
        if (attemptedCandidates.has(identity)) continue
        attemptedCandidates.add(identity)
        try {
          const materialized = await this.fromCandidate(
            candidate,
            kind,
            digest,
            signal
          )
          if (materialized !== undefined) return materialized
        } catch (error) {
          const deferredAuthenticationFailure =
            this.isDeferredAuthenticationFailure(error)
          if (error instanceof BackendError && error.rateLimited) {
            throw error
          }
          if (
            isSuppressibleAbortError(error, signal) ||
            signal?.reason instanceof ServerShutdownError
          ) {
            throw error
          }
          this.options.diagnostics?.record(
            {
              area: 'pack-reader',
              operation: 'candidate',
              kind,
              digest,
              fallback: 'legacy-object'
            },
            error
          )
          if (
            deferredAuthenticationFailure ||
            !(error instanceof BackendError)
          ) {
            this.options.metrics.backend('errors')
          }
        }
      }
      if (refresh === 1 || candidates.entries.length === 0) break
      try {
        const previousGeneration = candidates.generation
        const refreshed = await this.options.catalog.refreshAfterMiss(
          candidates.generation,
          kind,
          digest,
          signal
        )
        this.syncCatalogMetrics()
        if (refreshed.generation <= previousGeneration) break
        candidates = refreshed
      } catch (error) {
        this.syncCatalogMetrics()
        return this.catalogFailure(error, signal)
      }
    }
    return undefined
  }

  private async fromCandidate(
    candidate: PackCatalogEntry<ParsedPackCacheKey>,
    kind: CacheKind,
    digest: string,
    signal?: AbortSignal
  ): Promise<MaterializedPackObject | undefined> {
    let cached
    try {
      cached = await this.loadPack(candidate, signal)
    } catch (error) {
      if (!this.isDeferredAuthenticationFailure(error)) throw error
      cached = await this.loadPack(candidate, signal, true)
    }
    if (cached === undefined) return undefined
    const entry = findPackIndexEntry(cached.entries, kind, digest)
    if (entry === undefined) {
      this.options.catalog.recordBloomFalsePositive()
      this.options.metrics.bloomFalsePositive()
      return undefined
    }
    try {
      return await this.downloadEntry(cached, entry, signal)
    } catch (error) {
      if (this.isDeferredAuthenticationFailure(error)) {
        if (this.indexCache.get(candidate.key) === cached) {
          this.indexCache.delete(candidate.key)
        }
        cached = await this.loadPack(candidate, signal, true)
        if (cached === undefined) return undefined
        const refreshedEntry = findPackIndexEntry(cached.entries, kind, digest)
        if (refreshedEntry === undefined) return undefined
        return this.downloadEntry(cached, refreshedEntry, signal)
      }
      throw error
    }
  }

  private async loadPack(
    candidate: PackCatalogEntry<ParsedPackCacheKey>,
    signal?: AbortSignal,
    signedUrlRefresh = false
  ): Promise<CachedPack | undefined> {
    const existing = this.indexCache.get(candidate.key)
    if (existing !== undefined) {
      this.indexCache.delete(candidate.key)
      this.indexCache.set(candidate.key, existing)
      return existing
    }

    let flight = this.indexFlights.get(candidate.key)
    if (flight === undefined) {
      if (signedUrlRefresh) this.options.metrics.signedUrlRefresh()
      const controller = new AbortController()
      const promise = this.loadPackUncached(candidate, controller.signal)
      flight = {promise, controller, waiters: 0, settled: false}
      this.indexFlights.set(candidate.key, flight)
      const created = flight
      void promise.then(
        () => this.finishIndexFlight(candidate.key, created),
        () => this.finishIndexFlight(candidate.key, created)
      )
    }

    flight.waiters += 1
    try {
      return await waitWithAbort(flight.promise, signal)
    } finally {
      flight.waiters -= 1
      if (flight.waiters === 0 && !flight.settled) {
        if (this.indexFlights.get(candidate.key) === flight) {
          this.indexFlights.delete(candidate.key)
        }
        flight.controller.abort()
      }
    }
  }

  private async loadPackUncached(
    candidate: PackCatalogEntry<ParsedPackCacheKey>,
    signal: AbortSignal
  ): Promise<CachedPack | undefined> {
    const lookup = await this.observedBackend(
      'lookups',
      'lookup',
      () =>
        this.options.backend.lookup(candidate.key, PACK_CACHE_VERSION, signal),
      signal
    )
    if (lookup.kind === 'miss') return undefined

    const trailerRange = packTrailerRange(candidate.sizeBytes)
    const trailerBytes = await this.readSmallRange(
      lookup.downloadUrl,
      safeRange(trailerRange.offset, 'pack trailer offset'),
      PACK_TRAILER_SIZE,
      signal
    )
    const trailer = decodePackTrailer(trailerBytes)
    validatePackLayout(trailer, candidate.sizeBytes)
    const indexRange = packIndexRange(trailer)
    const indexLength = safeRange(indexRange.length, 'pack index length')
    if (indexLength > MAX_INDEX_BYTES)
      throw new Error('pack index is too large')
    const indexBytes = await this.readSmallRange(
      lookup.downloadUrl,
      safeRange(indexRange.offset, 'pack index offset'),
      indexLength,
      signal
    )
    const cached: CachedPack = {
      key: candidate.key,
      size: candidate.sizeBytes,
      entries: parsePackIndex(indexBytes, trailer),
      downloadUrl: lookup.downloadUrl
    }
    this.indexCache.set(candidate.key, cached)
    while (this.indexCache.size > this.indexCacheSize) {
      const oldest = this.indexCache.keys().next().value as string | undefined
      if (oldest === undefined) break
      this.indexCache.delete(oldest)
    }
    return cached
  }

  private finishIndexFlight(key: string, flight: IndexFlight): void {
    flight.settled = true
    if (this.indexFlights.get(key) === flight) this.indexFlights.delete(key)
  }

  private isRefreshableAuthenticationFailure(
    error: unknown
  ): error is BackendError {
    return (
      error instanceof BackendError &&
      !error.rateLimited &&
      (error.statusCode === 401 || error.statusCode === 403)
    )
  }

  private isDeferredAuthenticationFailure(error: unknown): boolean {
    return (
      error instanceof BackendError &&
      this.deferredAuthenticationFailures.has(error)
    )
  }

  private async readSmallRange(
    signedUrl: string,
    offset: number,
    length: number,
    signal?: AbortSignal
  ): Promise<Uint8Array> {
    const response = await this.observedRangeDownload(
      () =>
        this.options.backend.openDownloadRange(
          signedUrl,
          offset,
          length,
          signal
        ),
      signal
    )
    const bytes = new Uint8Array(await response.arrayBuffer())
    if (bytes.byteLength !== length) throw new Error('pack range was truncated')
    this.options.metrics.rangeBytesDownloaded(bytes.byteLength)
    return bytes
  }

  private async downloadEntry(
    pack: CachedPack,
    entry: PackIndexEntry,
    signal?: AbortSignal
  ): Promise<MaterializedPackObject> {
    const range = packPayloadRange(entry)
    const offset = safeRange(range.offset, 'pack object offset')
    const length = safeRange(range.length, 'pack object length')
    if (length > this.options.maxObjectSize) {
      throw new Error('pack object exceeds max-object-size')
    }
    await mkdir(this.options.directory, {recursive: true, mode: 0o700})
    const outputPath = path.join(
      this.options.directory,
      `${randomBytes(16).toString('hex')}.range`
    )
    const file = await open(outputPath, 'wx', 0o600)
    const hash = createHash('sha256')
    let bytes = 0
    try {
      if (length > 0) {
        const response = await this.observedRangeDownload(
          () =>
            this.options.backend.openDownloadRange(
              pack.downloadUrl,
              offset,
              length,
              signal
            ),
          signal
        )
        const reader = response.body?.getReader()
        if (reader === undefined) throw new Error('pack range returned no body')
        while (true) {
          const result = await reader.read()
          if (result.done) break
          const chunk = Buffer.from(result.value)
          hash.update(chunk)
          let written = 0
          while (written < chunk.length) {
            const output = await file.write(
              chunk,
              written,
              chunk.length - written,
              null
            )
            if (output.bytesWritten === 0) {
              throw new Error('pack range write made no progress')
            }
            written += output.bytesWritten
          }
          bytes += chunk.length
          if (bytes > length) throw new Error('pack range exceeded its length')
        }
      }
      if (bytes !== length) throw new Error('pack range was truncated')
      verifyPackPayloadSha256(entry, hash.digest())
      await file.sync()
      await file.close()
      this.options.metrics.rangeBytesDownloaded(bytes)
      return {
        path: outputPath,
        size: length,
        dispose: () => rm(outputPath, {force: true})
      }
    } catch (error) {
      await file.close().catch(() => {})
      await rm(outputPath, {force: true}).catch(() => {})
      throw error
    }
  }

  private syncCatalogMetrics(): void {
    const current = this.options.catalog.metricsSnapshot()
    const refreshes = current.refreshes - this.lastCatalogMetrics.refreshes
    const candidates =
      current.bloomCandidates - this.lastCatalogMetrics.bloomCandidates
    const rateLimitedResponses =
      current.rateLimitedResponses -
      this.lastCatalogMetrics.rateLimitedResponses
    for (let index = 0; index < refreshes; index += 1) {
      this.options.metrics.catalogRefresh()
    }
    if (candidates > 0) this.options.metrics.bloomCandidates(candidates)
    for (let index = 0; index < rateLimitedResponses; index += 1) {
      this.options.metrics.backend('rateLimited')
      this.options.metrics.rateLimit('lookup')
    }
    this.lastCatalogMetrics = current
  }

  private catalogFailure(error: unknown, signal?: AbortSignal): undefined {
    if (
      isSuppressibleAbortError(error, signal) ||
      signal?.reason instanceof ServerShutdownError
    ) {
      throw error
    }
    this.options.metrics.backend('errors')
    this.options.diagnostics?.record(
      {area: 'catalog', operation: 'refresh'},
      error
    )
    if (error instanceof PackCatalogError && error.rateLimited) {
      throw new BackendError('GitHub cache catalog is rate limited', {
        statusCode: error.statusCode ?? 429,
        rateLimited: true,
        retryable: true,
        ...(error.retryAfterMs === undefined
          ? {}
          : {retryAfterMs: error.retryAfterMs})
      })
    }
    return undefined
  }

  private async observedBackend<T>(
    counter: 'lookups' | 'downloads',
    operation: 'lookup' | 'download',
    call: () => Promise<T>,
    signal?: AbortSignal
  ): Promise<T> {
    this.options.metrics.backend(counter)
    try {
      return await call()
    } catch (error) {
      if (isSuppressibleAbortError(error, signal)) throw error
      this.options.metrics.backend('errors')
      this.options.diagnostics?.record({area: 'backend', operation}, error)
      if (error instanceof BackendError && error.rateLimited) {
        this.options.metrics.backend('rateLimited')
        this.options.metrics.rateLimit(operation)
      }
      throw error
    }
  }

  private async observedRangeDownload<T>(
    call: () => Promise<T>,
    signal?: AbortSignal
  ): Promise<T> {
    this.options.metrics.backend('downloads')
    try {
      return await call()
    } catch (error) {
      if (isSuppressibleAbortError(error, signal)) throw error
      if (this.isRefreshableAuthenticationFailure(error)) {
        this.deferredAuthenticationFailures.add(error)
        throw error
      }
      this.options.metrics.backend('errors')
      this.options.diagnostics?.record(
        {area: 'backend', operation: 'download'},
        error
      )
      if (error instanceof BackendError && error.rateLimited) {
        this.options.metrics.backend('rateLimited')
        this.options.metrics.rateLimit('download')
      }
      throw error
    }
  }
}
