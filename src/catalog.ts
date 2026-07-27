export type PackCatalogObjectKind = 'ac' | 'cas'

export interface PackKeyCodec<Metadata> {
  /**
   * Parses and validates all pack-key metadata, including its Bloom filter.
   * Invalid keys must return undefined rather than partially parsed metadata.
   */
  parse(key: string): Metadata | undefined

  /** Returns false only when the requested object is definitely absent. */
  mightContain(
    metadata: Metadata,
    kind: PackCatalogObjectKind,
    digest: string
  ): boolean
}

export type CatalogFetch = (
  input: string | URL,
  init?: RequestInit
) => Promise<Response>

export type CatalogClock = () => number

export interface PackCatalogOptions<Metadata> {
  owner: string
  repository: string
  token: string
  keyPrefix: string
  currentRef: string
  baseRef?: string
  defaultRef?: string
  codec: PackKeyCodec<Metadata>
  fetchImplementation?: CatalogFetch
  clock?: CatalogClock
  apiBaseUrl?: string
  refreshIntervalMs?: number
  requestTimeoutMs?: number
  maxPagesPerRef?: number
}

export interface PackCatalogEntry<Metadata> {
  readonly id: number
  readonly key: string
  readonly version: string
  readonly ref: string
  readonly sizeBytes: number
  readonly createdAt: string
  readonly metadata: Metadata
}

export interface PackCatalogSnapshot<Metadata> {
  readonly generation: number
  readonly refreshedAt: number
  readonly entries: readonly PackCatalogEntry<Metadata>[]
}

export interface PackCatalogCandidates<Metadata> {
  readonly generation: number
  readonly entries: readonly PackCatalogEntry<Metadata>[]
}

export interface PackCatalogMetrics {
  refreshAttempts: number
  refreshes: number
  refreshErrors: number
  rateLimitedResponses: number
  coalescedRefreshes: number
  pagesFetched: number
  entriesSeen: number
  entriesAccepted: number
  invalidPackKeys: number
  candidateQueries: number
  bloomCandidates: number
  apparentMissRefreshes: number
  bloomFalsePositives: number
}

export class PackCatalogError extends Error {
  readonly statusCode?: number
  readonly retryAfterMs?: number
  readonly rateLimited: boolean

  constructor(
    message: string,
    statusCode?: number,
    retryAfterMs?: number,
    rateLimited = false
  ) {
    super(message)
    this.name = 'PackCatalogError'
    if (statusCode !== undefined) this.statusCode = statusCode
    if (retryAfterMs !== undefined) this.retryAfterMs = retryAfterMs
    this.rateLimited = rateLimited
  }
}

interface GitHubCacheRecord {
  id: number
  key: string
  version: string
  ref: string
  sizeBytes: number
  createdAt: string
  createdAtMs: number
}

interface GitHubCachePage {
  totalCount: number
  records: GitHubCacheRecord[]
  seen: number
}

interface RefreshOperation<Metadata> {
  promise: Promise<PackCatalogSnapshot<Metadata>>
  controller: AbortController
  waiters: number
  settled: boolean
}

interface RateLimitPause {
  until: number
  statusCode: number
}

interface RefreshFailurePause {
  until: number
  error: PackCatalogError
}

const DEFAULT_API_BASE_URL = 'https://api.github.com/'
const DEFAULT_REFRESH_INTERVAL_MS = 300_000
const DEFAULT_REQUEST_TIMEOUT_MS = 30_000
const DEFAULT_MAX_PAGES_PER_REF = 100
const PAGE_SIZE = 100
const MAX_RESPONSE_BYTES = 4 * 1024 * 1024
const API_VERSION = '2022-11-28'
const DEFAULT_RATE_LIMIT_PAUSE_MS = 60_000

function retryAfterMilliseconds(
  value: string | null,
  now: number
): number | undefined {
  if (value === null) return undefined
  if (/^[0-9]+$/.test(value)) {
    const seconds = Number(value)
    const milliseconds = seconds * 1000
    return Number.isSafeInteger(milliseconds) ? milliseconds : undefined
  }
  if (!value.includes(',')) return undefined
  const timestamp = Date.parse(value)
  return Number.isFinite(timestamp) ? Math.max(0, timestamp - now) : undefined
}

function rateLimitResetMilliseconds(
  value: string | null,
  now: number
): number | undefined {
  if (value === null || !/^[0-9]+$/.test(value)) return undefined
  const reset = Number(value) * 1000
  return Number.isSafeInteger(reset) ? Math.max(0, reset - now) : undefined
}

function githubRateLimit(
  response: Response,
  now: number
): {retryAfterMs: number} | undefined {
  const retryAfterMs = retryAfterMilliseconds(
    response.headers.get('retry-after'),
    now
  )
  const remaining = response.headers.get('x-ratelimit-remaining')?.trim()
  const rateLimited =
    response.status === 429 ||
    (response.status === 403 &&
      (retryAfterMs !== undefined || remaining === '0'))
  if (!rateLimited) return undefined

  return {
    retryAfterMs:
      retryAfterMs ??
      rateLimitResetMilliseconds(
        response.headers.get('x-ratelimit-reset'),
        now
      ) ??
      DEFAULT_RATE_LIMIT_PAUSE_MS
  }
}

function positiveSafeInteger(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive safe integer`)
  }
  return value
}

function nonEmpty(value: string, name: string): string {
  if (value.trim().length === 0) throw new Error(`${name} must not be empty`)
  return value
}

function safeRepositoryPart(value: string, name: string): string {
  nonEmpty(value, name)
  if (!/^[A-Za-z0-9_.-]+$/.test(value)) {
    throw new Error(`${name} contains unsupported characters`)
  }
  return value
}

function apiBaseUrl(value: string): URL {
  const parsed = new URL(value)
  if (parsed.protocol !== 'https:') {
    throw new Error('apiBaseUrl must use HTTPS')
  }
  if (parsed.username.length > 0 || parsed.password.length > 0) {
    throw new Error('apiBaseUrl must not contain credentials')
  }
  if (!parsed.pathname.endsWith('/')) parsed.pathname += '/'
  parsed.search = ''
  parsed.hash = ''
  return parsed
}

function stringCompare(left: string, right: string): number {
  if (left < right) return -1
  if (left > right) return 1
  return 0
}

function newestFirst(
  left: GitHubCacheRecord,
  right: GitHubCacheRecord
): number {
  if (left.createdAtMs !== right.createdAtMs) {
    return left.createdAtMs > right.createdAtMs ? -1 : 1
  }
  if (left.id !== right.id) return left.id > right.id ? -1 : 1
  const keyOrder = stringCompare(left.key, right.key)
  if (keyOrder !== 0) return keyOrder
  const refOrder = stringCompare(left.ref, right.ref)
  if (refOrder !== 0) return refOrder
  return stringCompare(left.version, right.version)
}

function objectRecord(value: unknown): Record<string, unknown> | undefined {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    return undefined
  }
  return value as Record<string, unknown>
}

function decodeCacheRecord(value: unknown): GitHubCacheRecord | undefined {
  const record = objectRecord(value)
  if (record === undefined) return undefined

  const id = record['id']
  const key = record['key']
  const version = record['version']
  const ref = record['ref']
  const sizeBytes = record['size_in_bytes']
  const createdAt = record['created_at']
  if (
    typeof id !== 'number' ||
    !Number.isSafeInteger(id) ||
    id < 0 ||
    typeof key !== 'string' ||
    typeof version !== 'string' ||
    typeof ref !== 'string' ||
    typeof sizeBytes !== 'number' ||
    !Number.isSafeInteger(sizeBytes) ||
    sizeBytes < 0 ||
    typeof createdAt !== 'string'
  ) {
    return undefined
  }

  const createdAtMs = Date.parse(createdAt)
  if (!Number.isFinite(createdAtMs)) return undefined
  return {id, key, version, ref, sizeBytes, createdAt, createdAtMs}
}

function decodeCachePage(value: unknown): GitHubCachePage {
  const record = objectRecord(value)
  const totalCount = record?.['total_count']
  const actionsCaches = record?.['actions_caches']
  if (
    typeof totalCount !== 'number' ||
    !Number.isSafeInteger(totalCount) ||
    totalCount < 0 ||
    !Array.isArray(actionsCaches) ||
    actionsCaches.length > PAGE_SIZE
  ) {
    throw new PackCatalogError(
      'GitHub cache catalog returned an invalid response'
    )
  }

  const records: GitHubCacheRecord[] = []
  for (const cache of actionsCaches) {
    const decoded = decodeCacheRecord(cache)
    if (decoded !== undefined) records.push(decoded)
  }
  return {totalCount, records, seen: actionsCaches.length}
}

function abortedError(): PackCatalogError {
  return new PackCatalogError('GitHub cache catalog request aborted')
}

/**
 * A lazy, miss-refreshed catalog of immutable pack cache entries.
 *
 * REST results are an optimization only. Callers must still use an exact
 * Cache v2 lookup to establish visibility before reading a candidate pack.
 */
export class PackCatalog<Metadata> {
  private readonly owner: string
  private readonly repository: string
  private readonly token: string
  private readonly keyPrefix: string
  private readonly relevantRefs: readonly string[]
  private readonly codec: PackKeyCodec<Metadata>
  private readonly fetchImplementation: CatalogFetch
  private readonly clock: CatalogClock
  private readonly baseUrl: URL
  private readonly refreshIntervalMs: number
  private readonly requestTimeoutMs: number
  private readonly maxPagesPerRef: number
  private readonly metrics: PackCatalogMetrics = {
    refreshAttempts: 0,
    refreshes: 0,
    refreshErrors: 0,
    rateLimitedResponses: 0,
    coalescedRefreshes: 0,
    pagesFetched: 0,
    entriesSeen: 0,
    entriesAccepted: 0,
    invalidPackKeys: 0,
    candidateQueries: 0,
    bloomCandidates: 0,
    apparentMissRefreshes: 0,
    bloomFalsePositives: 0
  }

  private cachedSnapshot: PackCatalogSnapshot<Metadata> | undefined
  private refreshOperation: RefreshOperation<Metadata> | undefined
  private lastApparentMissAttemptAt: number | undefined
  private rateLimitPause: RateLimitPause | undefined
  private refreshFailurePause: RefreshFailurePause | undefined
  private nextGeneration = 1

  constructor(options: PackCatalogOptions<Metadata>) {
    this.owner = safeRepositoryPart(options.owner, 'owner')
    this.repository = safeRepositoryPart(options.repository, 'repository')
    this.token = nonEmpty(options.token, 'token')
    this.keyPrefix = nonEmpty(options.keyPrefix, 'keyPrefix')
    if (this.keyPrefix.length > 512) {
      throw new Error('keyPrefix must not exceed 512 characters')
    }
    this.codec = options.codec
    this.fetchImplementation = options.fetchImplementation ?? globalThis.fetch
    this.clock = options.clock ?? Date.now
    this.baseUrl = apiBaseUrl(options.apiBaseUrl ?? DEFAULT_API_BASE_URL)
    this.refreshIntervalMs = positiveSafeInteger(
      options.refreshIntervalMs ?? DEFAULT_REFRESH_INTERVAL_MS,
      'refreshIntervalMs'
    )
    this.requestTimeoutMs = positiveSafeInteger(
      options.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS,
      'requestTimeoutMs'
    )
    this.maxPagesPerRef = positiveSafeInteger(
      options.maxPagesPerRef ?? DEFAULT_MAX_PAGES_PER_REF,
      'maxPagesPerRef'
    )

    const refs = [
      nonEmpty(options.currentRef, 'currentRef'),
      ...(options.baseRef === undefined
        ? []
        : [nonEmpty(options.baseRef, 'baseRef')]),
      ...(options.defaultRef === undefined
        ? []
        : [nonEmpty(options.defaultRef, 'defaultRef')])
    ]
    this.relevantRefs = Object.freeze([...new Set(refs)])
  }

  metricsSnapshot(): PackCatalogMetrics {
    return {...this.metrics}
  }

  recordBloomFalsePositive(count = 1): void {
    if (!Number.isSafeInteger(count) || count <= 0) {
      throw new Error('false-positive count must be a positive safe integer')
    }
    this.metrics.bloomFalsePositives += count
  }

  /** Returns the cached snapshot, refreshing it when its TTL has elapsed. */
  async snapshot(signal?: AbortSignal): Promise<PackCatalogSnapshot<Metadata>> {
    if (signal?.aborted) throw abortedError()
    const cached = this.cachedSnapshot
    const now = this.now()
    if (
      cached !== undefined &&
      now >= cached.refreshedAt &&
      now - cached.refreshedAt < this.refreshIntervalMs
    ) {
      return cached
    }
    return this.sharedRefresh(signal)
  }

  /** Forces a refresh, coalescing it with any refresh already in progress. */
  async refresh(signal?: AbortSignal): Promise<PackCatalogSnapshot<Metadata>> {
    if (signal?.aborted) throw abortedError()
    return this.sharedRefresh(signal)
  }

  /**
   * Returns Bloom-filter candidates. Positive matches remain valid because
   * packs are immutable and exact Cache v2 lookup is the final authority, so
   * steady hits do not periodically re-list the repository. Definite or
   * apparent misses refresh under a shared repository-friendly cooldown.
   */
  async candidates(
    kind: PackCatalogObjectKind,
    digest: string,
    signal?: AbortSignal
  ): Promise<PackCatalogCandidates<Metadata>> {
    if (signal?.aborted) throw abortedError()
    this.metrics.candidateQueries += 1
    let snapshot = this.cachedSnapshot
    const hadSnapshot = snapshot !== undefined
    if (snapshot === undefined) snapshot = await this.sharedRefresh(signal)
    let entries = this.filterCandidates(snapshot, kind, digest)

    if (entries.length === 0 && hadSnapshot) {
      snapshot = await this.refreshForApparentMiss(snapshot, signal)
      entries = this.filterCandidates(snapshot, kind, digest)
    }

    this.metrics.bloomCandidates += entries.length
    return Object.freeze({
      generation: snapshot.generation,
      entries: Object.freeze(entries)
    })
  }

  /**
   * Refreshes once after every candidate from a generation proved to be a
   * miss, subject to the global miss-refresh cooldown. If another caller
   * already advanced the generation, its result is reused instead.
   */
  async refreshAfterMiss(
    previousGeneration: number,
    kind: PackCatalogObjectKind,
    digest: string,
    signal?: AbortSignal
  ): Promise<PackCatalogCandidates<Metadata>> {
    if (!Number.isSafeInteger(previousGeneration) || previousGeneration < 0) {
      throw new Error('previousGeneration must be a non-negative safe integer')
    }
    if (signal?.aborted) throw abortedError()
    this.metrics.candidateQueries += 1

    let snapshot = this.cachedSnapshot
    if (snapshot === undefined || snapshot.generation <= previousGeneration) {
      snapshot = await this.refreshForApparentMiss(snapshot, signal)
    }
    const entries = this.filterCandidates(snapshot, kind, digest)
    this.metrics.bloomCandidates += entries.length
    return Object.freeze({
      generation: snapshot.generation,
      entries: Object.freeze(entries)
    })
  }

  private filterCandidates(
    snapshot: PackCatalogSnapshot<Metadata>,
    kind: PackCatalogObjectKind,
    digest: string
  ): PackCatalogEntry<Metadata>[] {
    const result: PackCatalogEntry<Metadata>[] = []
    for (const entry of snapshot.entries) {
      try {
        if (this.codec.mightContain(entry.metadata, kind, digest)) {
          result.push(entry)
        }
      } catch {
        // A codec failure cannot turn a definite negative into a candidate.
        // Treat it as corrupt metadata and let the next catalog refresh retry.
      }
    }
    return result
  }

  private sharedRefresh(
    signal?: AbortSignal
  ): Promise<PackCatalogSnapshot<Metadata>> {
    if (signal?.aborted) return Promise.reject(abortedError())

    const paused = this.activeRateLimitError()
    if (paused !== undefined) {
      const cached = this.cachedSnapshot
      return cached === undefined
        ? Promise.reject(paused)
        : Promise.resolve(cached)
    }

    const failed = this.activeRefreshFailure()
    if (failed !== undefined) {
      const cached = this.cachedSnapshot
      return cached === undefined
        ? Promise.reject(failed)
        : Promise.resolve(cached)
    }

    let operation = this.refreshOperation
    if (operation === undefined) {
      const controller = new AbortController()
      const promise = this.performRefresh(controller.signal)
      operation = {
        promise,
        controller,
        waiters: 0,
        settled: false
      }
      this.refreshOperation = operation
      const activeOperation = operation
      void operation.promise.then(
        () => this.finishRefresh(activeOperation),
        () => this.finishRefresh(activeOperation)
      )
    } else {
      this.metrics.coalescedRefreshes += 1
    }

    return this.waitForRefresh(operation, signal)
  }

  private finishRefresh(operation: RefreshOperation<Metadata>): void {
    operation.settled = true
    if (this.refreshOperation === operation) this.refreshOperation = undefined
  }

  private async refreshForApparentMiss(
    fallback: PackCatalogSnapshot<Metadata> | undefined,
    signal?: AbortSignal
  ): Promise<PackCatalogSnapshot<Metadata>> {
    // Join an already-started miss refresh instead of letting its cooldown
    // make concurrent readers return the older generation early.
    if (this.refreshOperation !== undefined) return this.sharedRefresh(signal)

    const now = this.now()
    if (
      fallback !== undefined &&
      this.lastApparentMissAttemptAt !== undefined &&
      now >= this.lastApparentMissAttemptAt &&
      now - this.lastApparentMissAttemptAt < this.refreshIntervalMs
    ) {
      return this.cachedSnapshot ?? fallback
    }

    this.metrics.apparentMissRefreshes += 1
    this.lastApparentMissAttemptAt = now
    return this.sharedRefresh(signal)
  }

  private now(): number {
    const value = this.clock()
    if (!Number.isFinite(value)) {
      throw new Error('clock must return finite time')
    }
    return value
  }

  private waitForRefresh(
    operation: RefreshOperation<Metadata>,
    signal?: AbortSignal
  ): Promise<PackCatalogSnapshot<Metadata>> {
    operation.waiters += 1
    return new Promise((resolve, reject) => {
      let finished = false
      const finish = (): boolean => {
        if (finished) return false
        finished = true
        signal?.removeEventListener('abort', onAbort)
        operation.waiters -= 1
        return true
      }
      const onAbort = (): void => {
        if (!finish()) return
        if (operation.waiters === 0 && !operation.settled) {
          if (this.refreshOperation === operation) {
            this.refreshOperation = undefined
          }
          operation.controller.abort()
        }
        reject(abortedError())
      }

      signal?.addEventListener('abort', onAbort, {once: true})
      if (signal?.aborted) {
        onAbort()
        return
      }
      void operation.promise.then(
        value => {
          if (finish()) resolve(value)
        },
        error => {
          if (finish()) reject(error)
        }
      )
    })
  }

  private async performRefresh(
    signal: AbortSignal
  ): Promise<PackCatalogSnapshot<Metadata>> {
    this.metrics.refreshAttempts += 1
    try {
      const records: GitHubCacheRecord[] = []
      for (const ref of this.relevantRefs) {
        records.push(...(await this.listRef(ref, signal)))
      }

      records.sort(newestFirst)
      const seenIds = new Set<number>()
      const entries: PackCatalogEntry<Metadata>[] = []
      for (const record of records) {
        if (seenIds.has(record.id)) continue
        seenIds.add(record.id)
        if (!record.key.startsWith(this.keyPrefix)) continue

        let metadata: Metadata | undefined
        try {
          metadata = this.codec.parse(record.key)
        } catch {
          metadata = undefined
        }
        if (metadata === undefined) {
          this.metrics.invalidPackKeys += 1
          continue
        }

        entries.push(
          Object.freeze({
            id: record.id,
            key: record.key,
            version: record.version,
            ref: record.ref,
            sizeBytes: record.sizeBytes,
            createdAt: record.createdAt,
            metadata
          })
        )
      }

      const snapshot: PackCatalogSnapshot<Metadata> = Object.freeze({
        generation: this.nextGeneration,
        refreshedAt: this.now(),
        entries: Object.freeze(entries)
      })
      this.nextGeneration += 1
      this.cachedSnapshot = snapshot
      this.refreshFailurePause = undefined
      this.metrics.refreshes += 1
      this.metrics.entriesAccepted += entries.length
      return snapshot
    } catch (error) {
      this.metrics.refreshErrors += 1
      const catalogError =
        error instanceof PackCatalogError
          ? error
          : new PackCatalogError('GitHub cache catalog refresh failed')
      if (!signal.aborted && !catalogError.rateLimited) {
        this.pauseAfterRefreshFailure(catalogError)
      }
      if (!signal.aborted && this.cachedSnapshot !== undefined) {
        return this.cachedSnapshot
      }
      throw catalogError
    }
  }

  private async listRef(
    ref: string,
    signal: AbortSignal
  ): Promise<GitHubCacheRecord[]> {
    const result: GitHubCacheRecord[] = []
    for (let page = 1; page <= this.maxPagesPerRef; page += 1) {
      const response = await this.fetchPage(ref, page, signal)
      this.metrics.pagesFetched += 1
      this.metrics.entriesSeen += response.seen

      for (const record of response.records) {
        // The server-side `ref` and `key` parameters are not treated as a
        // security boundary. Recheck both fields before accepting a result.
        if (record.ref === ref && record.key.startsWith(this.keyPrefix)) {
          result.push(record)
        }
      }

      const pageWasFull = response.seen === PAGE_SIZE
      const reportedMore = page * PAGE_SIZE < response.totalCount
      if (!pageWasFull || !reportedMore) return result
      if (page === this.maxPagesPerRef) {
        throw new PackCatalogError(
          'GitHub cache catalog pagination limit exceeded'
        )
      }
    }
    return result
  }

  private async fetchPage(
    ref: string,
    page: number,
    parentSignal: AbortSignal
  ): Promise<GitHubCachePage> {
    if (parentSignal.aborted) throw abortedError()
    const endpoint = new URL(
      `repos/${encodeURIComponent(this.owner)}/${encodeURIComponent(
        this.repository
      )}/actions/caches`,
      this.baseUrl
    )
    endpoint.searchParams.set('key', this.keyPrefix)
    endpoint.searchParams.set('ref', ref)
    endpoint.searchParams.set('sort', 'created_at')
    endpoint.searchParams.set('direction', 'desc')
    endpoint.searchParams.set('per_page', String(PAGE_SIZE))
    endpoint.searchParams.set('page', String(page))

    const controller = new AbortController()
    let timedOut = false
    const onParentAbort = (): void => controller.abort()
    parentSignal.addEventListener('abort', onParentAbort, {once: true})
    const timeout = setTimeout(() => {
      timedOut = true
      controller.abort()
    }, this.requestTimeoutMs)

    try {
      let response: Response
      try {
        response = await this.fetchImplementation(endpoint, {
          method: 'GET',
          headers: {
            Accept: 'application/vnd.github+json',
            Authorization: `Bearer ${this.token}`,
            'User-Agent': 'bazel-gha-remote-cache/pack-catalog',
            'X-GitHub-Api-Version': API_VERSION
          },
          redirect: 'error',
          signal: controller.signal
        })
      } catch {
        if (timedOut) {
          throw new PackCatalogError('GitHub cache catalog request timed out')
        }
        if (parentSignal.aborted || controller.signal.aborted) {
          throw abortedError()
        }
        throw new PackCatalogError('GitHub cache catalog request failed')
      }
      if (timedOut) {
        throw new PackCatalogError('GitHub cache catalog request timed out')
      }
      if (parentSignal.aborted) throw abortedError()

      if (!response.ok) {
        const rateLimit = githubRateLimit(response, this.now())
        if (rateLimit !== undefined) {
          this.metrics.rateLimitedResponses += 1
          this.pauseForRateLimit(response.status, rateLimit.retryAfterMs)
          throw new PackCatalogError(
            'GitHub cache catalog is rate limited',
            response.status,
            rateLimit.retryAfterMs,
            true
          )
        }
        const permissionHint =
          response.status === 403
            ? '; github-token requires Actions: read permission'
            : ''
        throw new PackCatalogError(
          `GitHub cache catalog request failed with HTTP ${response.status}${permissionHint}`,
          response.status,
          retryAfterMilliseconds(
            response.headers.get('retry-after'),
            this.now()
          )
        )
      }

      const contentLength = response.headers.get('content-length')
      if (
        contentLength !== null &&
        /^[0-9]+$/.test(contentLength) &&
        Number(contentLength) > MAX_RESPONSE_BYTES
      ) {
        throw new PackCatalogError(
          'GitHub cache catalog response exceeded the size limit'
        )
      }

      let text: string
      try {
        text = await response.text()
      } catch {
        if (timedOut) {
          throw new PackCatalogError('GitHub cache catalog request timed out')
        }
        if (parentSignal.aborted || controller.signal.aborted) {
          throw abortedError()
        }
        throw new PackCatalogError('GitHub cache catalog response failed')
      }
      if (timedOut) {
        throw new PackCatalogError('GitHub cache catalog request timed out')
      }
      if (parentSignal.aborted) throw abortedError()
      if (text.length > MAX_RESPONSE_BYTES) {
        throw new PackCatalogError(
          'GitHub cache catalog response exceeded the size limit'
        )
      }

      let value: unknown
      try {
        value = JSON.parse(text) as unknown
      } catch {
        throw new PackCatalogError(
          'GitHub cache catalog returned an invalid response'
        )
      }
      return decodeCachePage(value)
    } finally {
      clearTimeout(timeout)
      parentSignal.removeEventListener('abort', onParentAbort)
    }
  }

  private pauseForRateLimit(statusCode: number, retryAfterMs: number): void {
    const until = this.now() + retryAfterMs
    const current = this.rateLimitPause
    if (current === undefined || until > current.until) {
      this.rateLimitPause = {until, statusCode}
    }
  }

  private pauseAfterRefreshFailure(error: PackCatalogError): void {
    const delay = Math.max(
      this.refreshIntervalMs,
      error.retryAfterMs ?? this.refreshIntervalMs
    )
    const until = this.now() + delay
    const current = this.refreshFailurePause
    if (current === undefined || until > current.until) {
      this.refreshFailurePause = {until, error}
    }
  }

  private activeRefreshFailure(): PackCatalogError | undefined {
    const pause = this.refreshFailurePause
    if (pause === undefined) return undefined
    if (pause.until - this.now() <= 0) {
      this.refreshFailurePause = undefined
      return undefined
    }
    return pause.error
  }

  private activeRateLimitError(): PackCatalogError | undefined {
    const pause = this.rateLimitPause
    if (pause === undefined) return undefined
    const remaining = pause.until - this.now()
    if (remaining <= 0) {
      this.rateLimitPause = undefined
      return undefined
    }
    return new PackCatalogError(
      'GitHub cache catalog is rate limited',
      pause.statusCode,
      remaining,
      true
    )
  }
}
