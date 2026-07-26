import {BlockBlobClient} from '@azure/storage-blob'
import {AsyncLocalStorage} from 'node:async_hooks'
import {CacheServiceClientJSON} from './vendor/actions-toolkit/generated/results/api/v1/cache.twirp-client.js'
import type {
  CreateCacheEntryResponse,
  FinalizeCacheEntryUploadResponse,
  GetCacheEntryDownloadURLResponse
} from './vendor/actions-toolkit/generated/results/api/v1/cache.js'

export interface CacheLookupHit {
  kind: 'hit'
  downloadUrl: string
}

export interface CacheLookupMiss {
  kind: 'miss'
}

export type CacheLookup = CacheLookupHit | CacheLookupMiss

export interface CacheReservation {
  kind: 'reserved' | 'conflict'
  uploadUrl?: string
}

export interface CacheBackend {
  lookup(
    key: string,
    version: string,
    signal?: AbortSignal
  ): Promise<CacheLookup>
  reserve(
    key: string,
    version: string,
    signal?: AbortSignal
  ): Promise<CacheReservation>
  uploadFile(
    signedUrl: string,
    filePath: string,
    size: number,
    signal?: AbortSignal
  ): Promise<void>
  finalize(
    key: string,
    version: string,
    size: number,
    signal?: AbortSignal
  ): Promise<void>
  openDownload(signedUrl: string, signal?: AbortSignal): Promise<Response>
}

export class BackendError extends Error {
  readonly statusCode?: number
  readonly retryable: boolean
  readonly rateLimited: boolean
  readonly retryAfterMs?: number
  readonly conflict: boolean

  constructor(
    message: string,
    options: {
      statusCode?: number
      retryable?: boolean
      rateLimited?: boolean
      retryAfterMs?: number
      conflict?: boolean
      cause?: unknown
    } = {}
  ) {
    super(
      message,
      options.cause === undefined ? undefined : {cause: options.cause}
    )
    this.name = 'BackendError'
    if (options.statusCode !== undefined) this.statusCode = options.statusCode
    this.retryable = options.retryable ?? false
    this.rateLimited = options.rateLimited ?? false
    if (options.retryAfterMs !== undefined) {
      this.retryAfterMs = options.retryAfterMs
    }
    this.conflict = options.conflict ?? false
  }
}

type FetchFunction = (
  input: string | URL,
  init?: RequestInit
) => Promise<Response>

const RETRYABLE_STATUS = new Set([408, 500, 502, 503, 504])
const CONFLICT_CODES = new Set(['already_exists', 'aborted'])
const MAX_RETRY_DELAY_MS = 60_000

export function parseRetryAfter(
  header: string | null,
  now = Date.now()
): number | undefined {
  if (!header) return undefined
  if (/^[0-9]+$/.test(header)) {
    const seconds = Number(header)
    if (Number.isSafeInteger(seconds) && seconds >= 0) return seconds * 1000
    return undefined
  }
  // HTTP-date is normally IMF-fixdate. Requiring the comma prevents values
  // such as "1.5" from being accepted by Date.parse as an implementation-
  // specific calendar date.
  if (!header.includes(',')) return undefined
  const timestamp = Date.parse(header)
  if (!Number.isFinite(timestamp)) return undefined
  return Math.max(0, timestamp - now)
}

function safeTwirpMessage(value: unknown): string | undefined {
  if (typeof value !== 'string') return undefined
  return value
    .replace(/https?:\/\/[^\s"'<>]+/giu, '<redacted-url>')
    .replace(/bearer\s+[^\s]+/giu, 'Bearer <redacted>')
    .slice(0, 300)
}

function twirpBody(value: unknown): {code?: string; message?: string} {
  if (typeof value !== 'object' || value === null) return {}
  const record = value as Record<string, unknown>
  const code = typeof record['code'] === 'string' ? record['code'] : undefined
  const message = safeTwirpMessage(record['msg'])
  return {
    ...(code === undefined ? {} : {code}),
    ...(message === undefined ? {} : {message})
  }
}

async function responseJson(response: Response): Promise<unknown> {
  const text = await response.text()
  if (text.length === 0) return {}
  try {
    return JSON.parse(text) as unknown
  } catch {
    // Twirp specifies JSON errors, but retain the HTTP status classification
    // (especially 429) even if an intermediary returned a non-JSON body.
    if (!response.ok) return {}
    throw new BackendError('cache service returned malformed JSON', {
      statusCode: response.status,
      retryable: response.status >= 500
    })
  }
}

function combinedTimeoutSignal(
  timeoutMs: number,
  signal?: AbortSignal
): AbortSignal {
  const timeout = AbortSignal.timeout(timeoutMs)
  return signal === undefined ? timeout : AbortSignal.any([signal, timeout])
}

function retryDelay(attempt: number, retryAfterMs?: number): number {
  const backoff = Math.min(2000, 250 * 2 ** attempt)
  return Math.min(MAX_RETRY_DELAY_MS, Math.max(backoff, retryAfterMs ?? 0))
}

function delay(milliseconds: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted) return Promise.reject(new Error('request aborted'))
  return new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort)
      resolve()
    }, milliseconds)
    const onAbort = (): void => {
      clearTimeout(timer)
      reject(new Error('request aborted'))
    }
    signal?.addEventListener('abort', onAbort, {once: true})
  })
}

class TwirpJsonTransport {
  private readonly baseUrl: URL

  constructor(
    resultsUrl: string,
    private readonly token: string,
    private readonly timeoutMs: number,
    private readonly fetchImplementation: FetchFunction,
    private readonly maxAttempts: number,
    private readonly requestSignals: AsyncLocalStorage<AbortSignal>
  ) {
    this.baseUrl = new URL(resultsUrl)
  }

  async request(
    service: string,
    method: string,
    contentType: 'application/json' | 'application/protobuf',
    data: object | Uint8Array
  ): Promise<object | Uint8Array> {
    if (contentType !== 'application/json' || data instanceof Uint8Array) {
      throw new BackendError('only the Cache v2 JSON transport is supported')
    }

    const endpoint = new URL(`/twirp/${service}/${method}`, this.baseUrl)
    let lastError: BackendError | undefined
    const externalSignal = this.requestSignals.getStore()

    for (let attempt = 0; attempt < this.maxAttempts; attempt += 1) {
      try {
        const response = await this.fetchImplementation(endpoint, {
          method: 'POST',
          headers: {
            Accept: 'application/json',
            Authorization: `Bearer ${this.token}`,
            'Content-Type': 'application/json',
            'User-Agent': 'bazel-gha-remote-cache/0.0.1'
          },
          body: JSON.stringify(data),
          redirect: 'error',
          signal: combinedTimeoutSignal(this.timeoutMs, externalSignal)
        })
        const body = await responseJson(response)
        if (response.ok) return body as object

        const errorBody = twirpBody(body)
        const statusCode = response.status
        const rateLimited = statusCode === 429
        const retryable = RETRYABLE_STATUS.has(statusCode)
        const conflict =
          statusCode === 409 ||
          (errorBody.code !== undefined && CONFLICT_CODES.has(errorBody.code))
        const suffix = errorBody.message ? `: ${errorBody.message}` : ''
        const retryAfterMs = parseRetryAfter(
          response.headers.get('retry-after')
        )
        const error = new BackendError(
          `cache service ${method} failed with HTTP ${statusCode}${suffix}`,
          {
            statusCode,
            retryable,
            rateLimited,
            conflict,
            ...(retryAfterMs === undefined ? {} : {retryAfterMs})
          }
        )

        if (rateLimited || conflict || !retryable) throw error
        lastError = error
      } catch (error) {
        if (externalSignal?.aborted) {
          throw new BackendError(`cache service ${method} request aborted`, {
            cause: error
          })
        }
        if (error instanceof BackendError) {
          if (error.rateLimited || error.conflict || !error.retryable)
            throw error
          lastError = error
        } else {
          lastError = new BackendError(
            `cache service ${method} network failure`,
            {
              retryable: true,
              cause: error
            }
          )
        }
      }

      if (attempt + 1 < this.maxAttempts) {
        await delay(
          retryDelay(attempt, lastError?.retryAfterMs),
          externalSignal
        )
      }
    }

    throw (
      lastError ??
      new BackendError(`cache service ${method} failed`, {retryable: true})
    )
  }
}

function assertSignedUrl(value: string, purpose: string): string {
  if (!value) throw new BackendError(`cache service omitted the ${purpose} URL`)
  let parsed: URL
  try {
    parsed = new URL(value)
  } catch {
    throw new BackendError(`cache service returned an invalid ${purpose} URL`)
  }
  if (parsed.protocol !== 'https:' && parsed.protocol !== 'http:') {
    throw new BackendError(`cache service returned an unsafe ${purpose} URL`)
  }
  if (parsed.username || parsed.password) {
    throw new BackendError(`cache service returned an unsafe ${purpose} URL`)
  }
  if (
    parsed.protocol === 'http:' &&
    parsed.hostname !== '127.0.0.1' &&
    parsed.hostname !== '[::1]' &&
    parsed.hostname !== 'localhost'
  ) {
    throw new BackendError(`cache service returned an insecure ${purpose} URL`)
  }
  return value
}

function policyDenied(message: string): boolean {
  return message.toLowerCase().startsWith('cache write denied:')
}

function blobError(error: unknown, operation: string): BackendError {
  const record =
    typeof error === 'object' && error !== null
      ? (error as Record<string, unknown>)
      : undefined
  const statusCode =
    typeof record?.['statusCode'] === 'number'
      ? record['statusCode']
      : undefined
  const rateLimited = statusCode === 429
  let retryAfterMs: number | undefined
  const response = record?.['response']
  if (
    typeof response === 'object' &&
    response !== null &&
    'headers' in response
  ) {
    const headers = (
      response as {headers?: {get?: (name: string) => string | null}}
    ).headers
    const milliseconds = headers?.get?.('x-ms-retry-after-ms')
    if (milliseconds && /^[0-9]+$/.test(milliseconds)) {
      const value = Number(milliseconds)
      if (Number.isSafeInteger(value)) retryAfterMs = value
    } else {
      retryAfterMs = parseRetryAfter(headers?.get?.('retry-after') ?? null)
    }
  }
  return new BackendError(`${operation} failed`, {
    ...(statusCode === undefined ? {} : {statusCode}),
    retryable: statusCode === undefined || RETRYABLE_STATUS.has(statusCode),
    rateLimited,
    ...(retryAfterMs === undefined ? {} : {retryAfterMs}),
    cause: error
  })
}

export class ActionsCacheBackend implements CacheBackend {
  private readonly requestSignals = new AsyncLocalStorage<AbortSignal>()
  private readonly client: CacheServiceClientJSON
  private readonly timeoutMs: number

  constructor(
    resultsUrl: string,
    runtimeToken: string,
    timeoutSeconds: number,
    private readonly fetchImplementation: FetchFunction = fetch,
    maxControlPlaneAttempts = 4
  ) {
    this.timeoutMs = timeoutSeconds * 1000
    const transport = new TwirpJsonTransport(
      resultsUrl,
      runtimeToken,
      this.timeoutMs,
      fetchImplementation,
      maxControlPlaneAttempts,
      this.requestSignals
    )
    this.client = new CacheServiceClientJSON(transport)
  }

  async lookup(
    key: string,
    version: string,
    signal?: AbortSignal
  ): Promise<CacheLookup> {
    return this.withSignal(signal, async () => {
      const response: GetCacheEntryDownloadURLResponse =
        await this.client.GetCacheEntryDownloadURL({
          key,
          restoreKeys: [],
          version
        })
      if (!response.ok) return {kind: 'miss'}
      if (response.matchedKey !== key) {
        throw new BackendError('cache service returned a non-exact key match')
      }
      return {
        kind: 'hit',
        downloadUrl: assertSignedUrl(response.signedDownloadUrl, 'download')
      }
    })
  }

  async reserve(
    key: string,
    version: string,
    signal?: AbortSignal
  ): Promise<CacheReservation> {
    return this.withSignal(signal, async () => {
      let response: CreateCacheEntryResponse
      try {
        response = await this.client.CreateCacheEntry({key, version})
      } catch (error) {
        if (error instanceof BackendError && error.conflict) {
          return {kind: 'conflict'}
        }
        throw error
      }

      if (!response.ok) {
        if (policyDenied(response.message)) {
          throw new BackendError('cache service denied cache writes', {
            statusCode: 403
          })
        }
        return {kind: 'conflict'}
      }

      return {
        kind: 'reserved',
        uploadUrl: assertSignedUrl(response.signedUploadUrl, 'upload')
      }
    })
  }

  async uploadFile(
    signedUrl: string,
    filePath: string,
    _size: number,
    signal?: AbortSignal
  ): Promise<void> {
    const client = new BlockBlobClient(
      assertSignedUrl(signedUrl, 'upload'),
      undefined,
      {retryOptions: {maxTries: 1}}
    )
    let lastError: BackendError | undefined
    for (let attempt = 0; attempt < 3; attempt += 1) {
      const attemptSignal = combinedTimeoutSignal(this.timeoutMs, signal)
      const options = {
        blockSize: 64 * 1024 * 1024,
        concurrency: 1,
        maxSingleShotSize: 128 * 1024 * 1024,
        abortSignal: attemptSignal
      }
      try {
        await client.uploadFile(filePath, options)
        return
      } catch (error) {
        const mapped =
          error instanceof BackendError
            ? error
            : blobError(error, 'signed cache upload')
        if (
          signal?.aborted ||
          mapped.rateLimited ||
          !mapped.retryable ||
          attempt === 2
        ) {
          throw mapped
        }
        lastError = mapped
        await delay(retryDelay(attempt, mapped.retryAfterMs), signal)
      }
    }
    throw lastError ?? new BackendError('signed cache upload failed')
  }

  async finalize(
    key: string,
    version: string,
    size: number,
    signal?: AbortSignal
  ): Promise<void> {
    return this.withSignal(signal, async () => {
      let response: FinalizeCacheEntryUploadResponse
      try {
        response = await this.client.FinalizeCacheEntryUpload({
          key,
          version,
          sizeBytes: String(size)
        })
      } catch (error) {
        if (error instanceof BackendError && error.conflict) {
          throw new BackendError('cache finalization outcome is ambiguous', {
            retryable: true,
            conflict: true,
            cause: error
          })
        }
        throw error
      }
      if (!response.ok) {
        // A successful response can be lost before an idempotent retry returns
        // ok=false. The server confirms exact visibility before treating this
        // ambiguous finalization as a failure.
        throw new BackendError(
          'cache service did not finalize the cache entry',
          {
            retryable: true,
            conflict: true
          }
        )
      }
    })
  }

  async openDownload(
    signedUrl: string,
    signal?: AbortSignal
  ): Promise<Response> {
    try {
      const response = await this.fetchImplementation(
        assertSignedUrl(signedUrl, 'download'),
        {
          method: 'GET',
          redirect: 'error',
          signal: combinedTimeoutSignal(this.timeoutMs, signal)
        }
      )
      if (response.status !== 200) {
        const retryAfterMs = parseRetryAfter(
          response.headers.get('retry-after')
        )
        await response.body?.cancel().catch(() => {})
        throw new BackendError(
          `signed cache download failed with HTTP ${response.status}`,
          {
            statusCode: response.status,
            retryable: response.status >= 500,
            rateLimited: response.status === 429,
            ...(retryAfterMs === undefined ? {} : {retryAfterMs})
          }
        )
      }
      if (response.body === null) {
        throw new BackendError('signed cache download returned no body')
      }
      return response
    } catch (error) {
      if (error instanceof BackendError) throw error
      throw new BackendError('signed cache download failed', {
        retryable: true,
        cause: error
      })
    }
  }

  private withSignal<T>(
    signal: AbortSignal | undefined,
    operation: () => Promise<T>
  ): Promise<T> {
    if (signal === undefined) return operation()
    return this.requestSignals.run(signal, operation)
  }
}
