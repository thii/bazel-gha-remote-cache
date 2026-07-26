import {createHash, randomBytes, timingSafeEqual} from 'node:crypto'
import {
  createServer,
  type IncomingMessage,
  type ServerResponse
} from 'node:http'
import {mkdir, open, rm} from 'node:fs/promises'
import path from 'node:path'
import {Readable, Transform} from 'node:stream'
import {pipeline} from 'node:stream/promises'
import type {ReadableStream as NodeReadableStream} from 'node:stream/web'
import type {AddressInfo} from 'node:net'
import {BackendError, type CacheBackend, type CacheLookup} from './backend.js'
import {CapacityLimiter} from './concurrency.js'
import {Metrics} from './metrics.js'
import {
  CACHE_KEY_PREFIX,
  CACHE_VERSION,
  type CacheKind,
  type DaemonConfig
} from './model.js'

const OBJECT_PATH = /^\/cache\/(ac|cas)\/([0-9a-f]{64})$/
const CONTENT_LENGTH = /^(0|[1-9][0-9]*)$/
const DEFAULT_RATE_LIMIT_MS = 60_000
const SHUTDOWN_DRAIN_MS = 10_000

export class RequestError extends Error {
  constructor(
    readonly statusCode: number,
    message: string,
    readonly headers: Record<string, string> = {}
  ) {
    super(message)
    this.name = 'RequestError'
  }
}

export class CasIntegrityError extends Error {
  constructor() {
    super('action-cache uploads are disabled after a CAS write failure')
    this.name = 'CasIntegrityError'
  }
}

interface IntegrityWaiter {
  resolve: () => void
  signal?: AbortSignal
  onAbort?: () => void
}

/**
 * CAS writes run concurrently, while AC finalization is exclusive. This makes
 * the CAS-failure latch atomic with the final publication of an action result.
 */
export class CasIntegrityGate {
  private activeCasWrites = 0
  private acFinalizing = false
  private failed = false
  private readonly waiters: IntegrityWaiter[] = []

  get healthy(): boolean {
    return !this.failed
  }

  async beginCas(signal?: AbortSignal): Promise<(success: boolean) => void> {
    try {
      while (this.acFinalizing) await this.wait(signal)
      if (signal?.aborted) throw new Error('request aborted')
    } catch (error) {
      // The request has already been accepted as a CAS write. Even if it
      // aborts while waiting for an AC publication to finish, it is a failed
      // CAS attempt and must permanently suppress later AC publications.
      this.failCas()
      throw error
    }
    this.activeCasWrites += 1
    let finished = false
    return success => {
      if (finished) return
      finished = true
      if (!success) this.failCas()
      this.activeCasWrites -= 1
      this.notify()
    }
  }

  failCas(): void {
    this.failed = true
    this.notify()
  }

  assertHealthy(): void {
    if (this.failed) throw new CasIntegrityError()
  }

  async finalizeAction<T>(
    operation: () => Promise<T>,
    signal?: AbortSignal
  ): Promise<T> {
    while (this.acFinalizing || this.activeCasWrites > 0)
      await this.wait(signal)
    if (signal?.aborted) throw new Error('request aborted')
    this.assertHealthy()
    this.acFinalizing = true
    try {
      this.assertHealthy()
      return await operation()
    } finally {
      this.acFinalizing = false
      this.notify()
    }
  }

  private wait(signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) return Promise.reject(new Error('request aborted'))
    return new Promise<void>((resolve, reject) => {
      const waiter: IntegrityWaiter = {resolve}
      if (signal !== undefined) {
        waiter.signal = signal
        waiter.onAbort = () => {
          const index = this.waiters.indexOf(waiter)
          if (index !== -1) this.waiters.splice(index, 1)
          reject(new Error('request aborted'))
        }
        signal.addEventListener('abort', waiter.onAbort, {once: true})
      }
      this.waiters.push(waiter)
    })
  }

  private notify(): void {
    const waiters = this.waiters.splice(0)
    for (const waiter of waiters) {
      if (waiter.signal !== undefined && waiter.onAbort !== undefined) {
        waiter.signal.removeEventListener('abort', waiter.onAbort)
      }
      waiter.resolve()
    }
  }
}

interface CircuitLease {
  probeId?: symbol
}

class CircuitOpenError extends Error {
  constructor(readonly retryAfterSeconds: number) {
    super('cache request circuit is temporarily open')
    this.name = 'CircuitOpenError'
  }
}

class RateCircuit {
  private openUntil = 0
  private probeId: symbol | undefined

  constructor(
    private readonly now: () => number,
    private readonly onChange: (open: boolean) => void
  ) {}

  enter(): CircuitLease {
    const current = this.now()
    if (this.openUntil === 0) return {}
    if (current < this.openUntil) {
      throw new CircuitOpenError(
        Math.max(1, Math.ceil((this.openUntil - current) / 1000))
      )
    }
    if (this.probeId !== undefined) throw new CircuitOpenError(1)
    this.probeId = Symbol('rate-limit-probe')
    return {probeId: this.probeId}
  }

  assertAllowed(lease: CircuitLease): void {
    if (this.openUntil === 0) return
    if (lease.probeId !== undefined && lease.probeId === this.probeId) return
    const retryAfter = Math.max(
      1,
      Math.ceil((this.openUntil - this.now()) / 1000)
    )
    throw new CircuitOpenError(retryAfter)
  }

  trip(retryAfterMs?: number): void {
    const duration = Math.max(1000, retryAfterMs ?? DEFAULT_RATE_LIMIT_MS)
    this.openUntil = Math.max(this.openUntil, this.now() + duration)
    this.probeId = undefined
    this.onChange(true)
  }

  complete(lease: CircuitLease): void {
    if (lease.probeId !== undefined && lease.probeId === this.probeId) {
      this.openUntil = 0
      this.probeId = undefined
      this.onChange(false)
    }
  }
}

interface ObjectPath {
  kind: CacheKind
  digest: string
}

interface SpoolResult {
  size: number
  digest: string
}

interface LookupFlight {
  promise: Promise<CacheLookup>
  controller: AbortController
  waiters: number
  settled: boolean
}

export interface CacheServerOptions {
  config: DaemonConfig
  backend: CacheBackend
  metrics: Metrics
  onShutdown: () => void
  now?: () => number
  sleep?: (milliseconds: number) => Promise<void>
  shutdownDrainMs?: number
}

function parseObjectPath(rawUrl: string | undefined): ObjectPath | undefined {
  if (rawUrl === undefined || rawUrl.includes('?') || rawUrl.includes('#')) {
    return undefined
  }
  const match = OBJECT_PATH.exec(rawUrl)
  if (match === null) return undefined
  const kind = match[1]
  const digest = match[2]
  if ((kind !== 'ac' && kind !== 'cas') || digest === undefined)
    return undefined
  return {kind, digest}
}

export function objectCacheKey(
  namespace: string,
  kind: CacheKind,
  digest: string
): string {
  const key = `${CACHE_KEY_PREFIX}-${namespace}-${kind}-sha256-${digest}`
  if (key.length > 512 || key.includes(',')) {
    throw new Error('generated cache key is invalid')
  }
  return key
}

function requestContentLength(request: IncomingMessage): number | undefined {
  const raw = request.headers['content-length']
  if (raw === undefined) return undefined
  if (!CONTENT_LENGTH.test(raw)) {
    throw new RequestError(400, 'invalid Content-Length')
  }
  const value = Number(raw)
  if (!Number.isSafeInteger(value)) {
    throw new RequestError(400, 'invalid Content-Length')
  }
  return value
}

function writeJson(
  response: ServerResponse,
  status: number,
  value: unknown
): void {
  const body = Buffer.from(`${JSON.stringify(value)}\n`)
  response.writeHead(status, {
    'Cache-Control': 'no-store',
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': String(body.length)
  })
  response.end(body)
}

function writeEmpty(
  response: ServerResponse,
  status: number,
  headers: Record<string, string> = {}
): void {
  response.writeHead(status, {...headers, 'Content-Length': '0'})
  response.end()
}

function authorizedShutdown(
  request: IncomingMessage,
  expected: string
): boolean {
  const prefix = 'Bearer '
  const header = request.headers.authorization
  if (header === undefined || !header.startsWith(prefix)) return false
  const supplied = Buffer.from(header.slice(prefix.length))
  const wanted = Buffer.from(expected)
  return supplied.length === wanted.length && timingSafeEqual(supplied, wanted)
}

async function spoolRequest(
  request: IncomingMessage,
  filePath: string,
  maximumSize: number,
  expectedSize: number | undefined
): Promise<SpoolResult> {
  const file = await open(filePath, 'wx', 0o600)
  const hash = createHash('sha256')
  let size = 0
  try {
    for await (const value of request) {
      const chunk = Buffer.isBuffer(value) ? value : Buffer.from(value)
      if (size + chunk.length > maximumSize) {
        throw new RequestError(413, 'cache object exceeds max-object-size')
      }
      hash.update(chunk)
      let offset = 0
      while (offset < chunk.length) {
        const result = await file.write(
          chunk,
          offset,
          chunk.length - offset,
          null
        )
        if (result.bytesWritten === 0)
          throw new Error('spool write made no progress')
        offset += result.bytesWritten
      }
      size += chunk.length
    }
    if (expectedSize !== undefined && size !== expectedSize) {
      throw new RequestError(400, 'request size did not match Content-Length')
    }
    await file.sync()
    return {size, digest: hash.digest('hex')}
  } finally {
    await file.close().catch(() => {})
  }
}

function abortForRequest(
  request: IncomingMessage,
  response: ServerResponse
): AbortController {
  const controller = new AbortController()
  if (request.aborted || response.destroyed) controller.abort()
  request.once('aborted', () => controller.abort())
  response.once('close', () => {
    if (!response.writableEnded) controller.abort()
  })
  return controller
}

function awaitWithAbort<T>(
  operation: Promise<T>,
  signal: AbortSignal
): Promise<T> {
  if (signal.aborted) return Promise.reject(new Error('request aborted'))
  return new Promise<T>((resolve, reject) => {
    const onAbort = (): void => reject(new Error('request aborted'))
    signal.addEventListener('abort', onAbort, {once: true})
    void operation.then(
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

export class CacheHttpServer {
  private readonly uploadLimiter: CapacityLimiter
  private readonly downloadLimiter: CapacityLimiter
  private readonly byteLimiter: CapacityLimiter
  private readonly integrity = new CasIntegrityGate()
  private readonly lookupFlights = new Map<string, LookupFlight>()
  private readonly writeCircuit: RateCircuit
  private readonly readCircuit: RateCircuit
  private readonly sleep: (milliseconds: number) => Promise<void>
  private readonly shutdownDrainMs: number
  private readonly spoolDirectory: string
  private readonly activeRequestControllers = new Set<AbortController>()
  private shuttingDown = false
  private started = false

  private readonly server = createServer((request, response) => {
    void this.dispatch(request, response)
  })

  constructor(private readonly options: CacheServerOptions) {
    this.uploadLimiter = new CapacityLimiter(options.config.uploadConcurrency)
    this.downloadLimiter = new CapacityLimiter(
      options.config.downloadConcurrency
    )
    this.byteLimiter = new CapacityLimiter(options.config.maxInflightBytes)
    this.spoolDirectory = path.join(options.config.controlDirectory, 'spool')
    const now = options.now ?? Date.now
    this.writeCircuit = new RateCircuit(now, open =>
      options.metrics.setWriteCircuitOpen(open)
    )
    this.readCircuit = new RateCircuit(now, open =>
      options.metrics.setReadCircuitOpen(open)
    )
    this.sleep =
      options.sleep ??
      (milliseconds =>
        new Promise(resolve => setTimeout(resolve, milliseconds)))
    this.shutdownDrainMs = options.shutdownDrainMs ?? SHUTDOWN_DRAIN_MS

    this.server.on('checkContinue', (request, response) => {
      try {
        const parsed = parseObjectPath(request.url)
        if (parsed === undefined) {
          writeEmpty(response, 404)
          return
        }
        if (request.method !== 'PUT') {
          writeEmpty(response, 405, {Allow: 'PUT'})
          return
        }
        if (!this.options.config.writable) {
          writeEmpty(response, 403)
          return
        }
        const rejectUpload = (status: number): void => {
          this.options.metrics.request('put')
          this.options.metrics.request('rejected')
          this.options.metrics.write(parsed.kind, 'error', 0, 0)
          if (parsed.kind === 'cas') {
            this.integrity.failCas()
            this.options.metrics.setCasWriteFailed()
          }
          writeEmpty(response, status)
        }
        const encoding = request.headers['content-encoding']?.toLowerCase()
        if (encoding !== undefined && encoding !== 'identity') {
          rejectUpload(415)
          return
        }
        const length = requestContentLength(request)
        if (length === undefined) {
          rejectUpload(400)
          return
        }
        if (length > this.options.config.maxObjectSize) {
          rejectUpload(413)
          return
        }
        response.writeContinue()
        void this.dispatch(request, response)
      } catch (error) {
        const status = error instanceof RequestError ? error.statusCode : 400
        const parsed = parseObjectPath(request.url)
        if (
          parsed?.kind === 'cas' &&
          request.method === 'PUT' &&
          this.options.config.writable
        ) {
          this.integrity.failCas()
          this.options.metrics.setCasWriteFailed()
        }
        writeEmpty(response, status)
      }
    })
    this.server.on('clientError', (_error, socket) => socket.destroy())
    this.server.headersTimeout = 30_000
    this.server.requestTimeout = 30 * 60_000
    this.server.keepAliveTimeout = 5_000
    this.server.maxRequestsPerSocket = 100
  }

  async start(): Promise<AddressInfo> {
    if (this.started) throw new Error('cache server already started')
    await mkdir(this.spoolDirectory, {recursive: true, mode: 0o700})
    await new Promise<void>((resolve, reject) => {
      const onError = (error: Error): void => reject(error)
      this.server.once('error', onError)
      this.server.listen(this.options.config.port, '127.0.0.1', () => {
        this.server.off('error', onError)
        resolve()
      })
    })
    const address = this.server.address()
    if (address === null || typeof address === 'string') {
      throw new Error('cache server did not bind a TCP address')
    }
    if (address.address !== '127.0.0.1') {
      throw new Error('cache server did not bind to loopback')
    }
    this.started = true
    return address
  }

  async shutdown(): Promise<void> {
    if (!this.started) return
    this.shuttingDown = true
    await new Promise<void>((resolve, reject) => {
      const forceTimer = setTimeout(() => {
        for (const controller of this.activeRequestControllers) {
          controller.abort()
        }
        this.server.closeAllConnections()
      }, this.shutdownDrainMs)
      this.server.close(error => {
        clearTimeout(forceTimer)
        if (error) reject(error)
        else resolve()
      })
      this.server.closeIdleConnections()
    })
    this.started = false
    this.activeRequestControllers.clear()
    await rm(this.spoolDirectory, {recursive: true, force: true}).catch(
      () => {}
    )
  }

  private async dispatch(
    request: IncomingMessage,
    response: ServerResponse
  ): Promise<void> {
    try {
      if (request.url === '/healthz' && request.method === 'GET') {
        this.options.metrics.request('health')
        writeJson(response, 200, {
          status: this.shuttingDown ? 'stopping' : 'ok',
          pid: process.pid,
          instanceId: this.options.config.instanceId,
          readable: this.options.config.readable,
          writable: this.options.config.writable,
          casHealthy: this.integrity.healthy
        })
        return
      }

      if (request.url === '/shutdown') {
        if (request.method !== 'POST') {
          writeEmpty(response, 405, {Allow: 'POST'})
          return
        }
        this.options.metrics.request('shutdown')
        if (!authorizedShutdown(request, this.options.config.shutdownToken)) {
          writeEmpty(response, 401)
          return
        }
        writeEmpty(response, 202)
        setImmediate(this.options.onShutdown)
        return
      }

      const object = parseObjectPath(request.url)
      if (object === undefined) {
        writeEmpty(response, 404)
        return
      }
      if (this.shuttingDown) throw new RequestError(503, 'server is stopping')

      if (request.method === 'GET') {
        this.options.metrics.request('get')
        await this.handleGet(request, response, object)
        return
      }

      if (request.method === 'PUT') {
        this.options.metrics.request('put')
        if (!this.options.config.writable) {
          this.options.metrics.request('rejected')
          writeEmpty(response, 403)
          return
        }
        const controller = this.requestController(request, response)
        let finishCas: ((success: boolean) => void) | undefined
        try {
          if (object.kind === 'cas') {
            finishCas = await this.integrity.beginCas(controller.signal)
          }
          await this.handlePut(request, response, object, controller.signal)
          finishCas?.(true)
        } catch (error) {
          finishCas?.(false)
          if (object.kind === 'cas') {
            this.options.metrics.setCasWriteFailed()
          }
          throw error
        }
        return
      }

      writeEmpty(response, 405, {Allow: 'GET, PUT'})
    } catch (error) {
      this.respondError(response, error)
    }
  }

  private async handleGet(
    request: IncomingMessage,
    response: ServerResponse,
    object: ObjectPath
  ): Promise<void> {
    const startedAt = Date.now()
    if (!this.options.config.readable) {
      this.options.metrics.read(object.kind, 'miss', 0, Date.now() - startedAt)
      writeEmpty(response, 404)
      return
    }
    const controller = this.requestController(request, response)

    let lease: CircuitLease
    try {
      lease = this.readCircuit.enter()
    } catch (error) {
      this.options.metrics.read(object.kind, 'error', 0, Date.now() - startedAt)
      if (object.kind === 'ac') {
        writeEmpty(response, 404)
        return
      }
      throw this.circuitRequestError(error)
    }

    const key = objectCacheKey(
      this.options.config.namespace,
      object.kind,
      object.digest
    )
    let lookup: CacheLookup
    try {
      lookup = await this.lookupSingleFlight(key, lease, controller.signal)
    } catch (error) {
      if (!(error instanceof BackendError && error.rateLimited)) {
        this.readCircuit.complete(lease)
      }
      this.handleRateLimit(error, this.readCircuit)
      this.options.metrics.read(object.kind, 'error', 0, Date.now() - startedAt)
      if (controller.signal.aborted) return
      if (object.kind === 'ac') {
        writeEmpty(response, 404)
        return
      }
      throw this.backendRequestError(error)
    }

    if (controller.signal.aborted) {
      this.readCircuit.complete(lease)
      this.options.metrics.read(object.kind, 'error', 0, Date.now() - startedAt)
      return
    }
    if (lookup.kind === 'miss') {
      this.readCircuit.complete(lease)
      this.options.metrics.read(object.kind, 'miss', 0, Date.now() - startedAt)
      writeEmpty(response, 404)
      return
    }

    let release: (() => void) | undefined
    let bytes = 0
    try {
      release = await this.downloadLimiter.acquire(1, controller.signal)
      this.readCircuit.assertAllowed(lease)
      const download = await this.observedBackend('downloads', () =>
        this.options.backend.openDownload(lookup.downloadUrl, controller.signal)
      )
      const encoding = download.headers.get('content-encoding')
      if (encoding !== null && encoding.toLowerCase() !== 'identity') {
        await download.body?.cancel().catch(() => {})
        throw new BackendError('signed cache download was content-encoded')
      }
      const rawLength = download.headers.get('content-length')
      if (rawLength !== null) {
        if (!CONTENT_LENGTH.test(rawLength)) {
          await download.body?.cancel().catch(() => {})
          throw new BackendError('signed cache download had invalid length')
        }
        const length = Number(rawLength)
        if (
          !Number.isSafeInteger(length) ||
          length > this.options.config.maxObjectSize
        ) {
          await download.body?.cancel().catch(() => {})
          throw new BackendError(
            'signed cache download exceeded max-object-size'
          )
        }
      }

      const counter = new Transform({
        transform: (chunk: Buffer, _encoding, callback) => {
          bytes += chunk.length
          if (bytes > this.options.config.maxObjectSize) {
            callback(
              new Error('signed cache download exceeded max-object-size')
            )
            return
          }
          callback(null, chunk)
        }
      })
      response.writeHead(200, {
        'Content-Type': 'application/octet-stream',
        ...(rawLength === null ? {} : {'Content-Length': rawLength})
      })
      await pipeline(
        Readable.fromWeb(
          download.body as unknown as NodeReadableStream<Uint8Array>
        ),
        counter,
        response
      )
      this.readCircuit.complete(lease)
      this.options.metrics.read(
        object.kind,
        'hit',
        bytes,
        Date.now() - startedAt
      )
    } catch (error) {
      if (response.headersSent) {
        response.destroy(error instanceof Error ? error : undefined)
        this.readCircuit.complete(lease)
        this.options.metrics.read(
          object.kind,
          'error',
          bytes,
          Date.now() - startedAt
        )
        return
      }
      if (!(error instanceof BackendError && error.rateLimited)) {
        this.readCircuit.complete(lease)
      }
      this.handleRateLimit(error, this.readCircuit)
      this.options.metrics.read(
        object.kind,
        'error',
        bytes,
        Date.now() - startedAt
      )
      if (object.kind === 'ac') {
        writeEmpty(response, 404)
        return
      }
      throw this.backendRequestError(error)
    } finally {
      release?.()
    }
  }

  private async handlePut(
    request: IncomingMessage,
    response: ServerResponse,
    object: ObjectPath,
    signal: AbortSignal
  ): Promise<void> {
    const startedAt = Date.now()
    let size = 0
    let releaseBytes: (() => void) | undefined
    let releaseUpload: (() => void) | undefined
    let spoolPath: string | undefined
    let lease: CircuitLease | undefined
    const releaseResources = async (): Promise<void> => {
      releaseUpload?.()
      releaseUpload = undefined
      if (spoolPath !== undefined) {
        await rm(spoolPath, {force: true}).catch(() => {})
        spoolPath = undefined
      }
      releaseBytes?.()
      releaseBytes = undefined
      this.options.metrics.setInflightBytes(this.byteLimiter.used)
    }

    try {
      if (object.kind === 'ac') this.integrity.assertHealthy()
      const contentEncoding = request.headers['content-encoding']?.toLowerCase()
      if (contentEncoding !== undefined && contentEncoding !== 'identity') {
        throw new RequestError(415, 'content encoding is not supported')
      }
      const expectedSize = requestContentLength(request)
      if (
        expectedSize !== undefined &&
        expectedSize > this.options.config.maxObjectSize
      ) {
        throw new RequestError(413, 'cache object exceeds max-object-size')
      }

      lease = this.writeCircuit.enter()
      const reservationSize = expectedSize ?? this.options.config.maxObjectSize
      releaseBytes = await this.byteLimiter.acquire(reservationSize, signal)
      this.options.metrics.setInflightBytes(this.byteLimiter.used)

      spoolPath = path.join(
        this.spoolDirectory,
        `${randomBytes(16).toString('hex')}.spool`
      )
      const spooled = await spoolRequest(
        request,
        spoolPath,
        this.options.config.maxObjectSize,
        expectedSize
      )
      size = spooled.size
      if (object.kind === 'cas' && spooled.digest !== object.digest) {
        this.options.metrics.integrityFailure()
        throw new RequestError(400, 'CAS digest did not match request bytes')
      }

      releaseUpload = await this.uploadLimiter.acquire(1, signal)
      this.writeCircuit.assertAllowed(lease)
      if (object.kind === 'ac') this.integrity.assertHealthy()

      const key = objectCacheKey(
        this.options.config.namespace,
        object.kind,
        object.digest
      )
      const reservation = await this.observedBackend('reservations', () =>
        this.options.backend.reserve(key, CACHE_VERSION, signal)
      )
      if (reservation.kind === 'conflict') {
        await releaseResources()
        const visible = await this.pollForVisibility(key, lease, signal)
        if (!visible) {
          throw new BackendError(
            'conflicting cache entry did not become visible',
            {
              retryable: true
            }
          )
        }
        this.writeCircuit.complete(lease)
        this.options.metrics.write(
          object.kind,
          'conflict',
          size,
          Date.now() - startedAt
        )
        writeEmpty(response, 204)
        return
      }

      if (!reservation.uploadUrl) {
        throw new BackendError('cache reservation omitted an upload URL')
      }
      this.writeCircuit.assertAllowed(lease)
      await this.observedBackend('uploads', () =>
        this.options.backend.uploadFile(
          reservation.uploadUrl as string,
          spoolPath as string,
          size,
          signal
        )
      )

      // The staged blob no longer needs the spool file or an upload slot.
      // Releasing both before the integrity gate prevents AC finalization
      // from holding capacity needed by active CAS writes it must await.
      await releaseResources()

      const finalize = async (): Promise<void> => {
        this.writeCircuit.assertAllowed(lease as CircuitLease)
        try {
          await this.observedBackend('finalizations', () =>
            this.options.backend.finalize(key, CACHE_VERSION, size, signal)
          )
        } catch (error) {
          if (
            error instanceof BackendError &&
            error.retryable &&
            !error.rateLimited
          ) {
            const visible = await this.pollForVisibility(
              key,
              lease as CircuitLease,
              signal
            )
            if (visible) return
          }
          throw error
        }
      }

      if (object.kind === 'ac') {
        await this.integrity.finalizeAction(finalize, signal)
      } else {
        await finalize()
      }

      this.writeCircuit.complete(lease)
      this.options.metrics.write(
        object.kind,
        'success',
        size,
        Date.now() - startedAt
      )
      await releaseResources()
      writeEmpty(response, 204)
    } catch (error) {
      if (
        lease !== undefined &&
        !(error instanceof BackendError && error.rateLimited)
      ) {
        this.writeCircuit.complete(lease)
      }
      this.handleRateLimit(error, this.writeCircuit)
      this.options.metrics.write(
        object.kind,
        'error',
        size,
        Date.now() - startedAt
      )
      if (error instanceof RequestError) throw error
      if (error instanceof CasIntegrityError) {
        throw new RequestError(503, error.message)
      }
      if (error instanceof CircuitOpenError)
        throw this.circuitRequestError(error)
      if (error instanceof BackendError) throw this.backendRequestError(error)
      if (signal.aborted) throw new RequestError(499, 'request aborted')
      throw new RequestError(500, 'failed to process cache upload')
    } finally {
      await releaseResources()
    }
  }

  private async lookupSingleFlight(
    key: string,
    lease: CircuitLease,
    signal: AbortSignal
  ): Promise<CacheLookup> {
    let flight = this.lookupFlights.get(key)
    if (flight === undefined) {
      const controller = new AbortController()
      const promise = (async (): Promise<CacheLookup> => {
        const release = await this.downloadLimiter.acquire(1, controller.signal)
        try {
          this.readCircuit.assertAllowed(lease)
          return await this.observedBackend('lookups', () =>
            this.options.backend.lookup(key, CACHE_VERSION, controller.signal)
          )
        } finally {
          release()
        }
      })()
      flight = {promise, controller, waiters: 0, settled: false}
      this.lookupFlights.set(key, flight)
      const createdFlight = flight
      void promise.then(
        () => this.finishLookupFlight(key, createdFlight),
        () => this.finishLookupFlight(key, createdFlight)
      )
    }

    flight.waiters += 1
    try {
      return await awaitWithAbort(flight.promise, signal)
    } finally {
      flight.waiters -= 1
      if (flight.waiters === 0 && !flight.settled) flight.controller.abort()
    }
  }

  private finishLookupFlight(key: string, flight: LookupFlight): void {
    flight.settled = true
    if (this.lookupFlights.get(key) === flight) this.lookupFlights.delete(key)
  }

  private requestController(
    request: IncomingMessage,
    response: ServerResponse
  ): AbortController {
    const controller = abortForRequest(request, response)
    this.activeRequestControllers.add(controller)
    response.once('close', () => {
      this.activeRequestControllers.delete(controller)
    })
    return controller
  }

  private async pollForVisibility(
    key: string,
    lease: CircuitLease,
    signal: AbortSignal
  ): Promise<boolean> {
    for (let attempt = 0; attempt < 5; attempt += 1) {
      if (signal.aborted) throw new Error('request aborted')
      this.writeCircuit.assertAllowed(lease)
      const release = await this.downloadLimiter.acquire(1, signal)
      let lookup: CacheLookup
      try {
        lookup = await this.observedBackend('lookups', () =>
          this.options.backend.lookup(key, CACHE_VERSION, signal)
        )
      } finally {
        release()
      }
      if (lookup.kind === 'hit') return true
      if (attempt < 4) await this.sleep(100 * 2 ** attempt)
    }
    return false
  }

  private async observedBackend<T>(
    counter:
      | 'lookups'
      | 'reservations'
      | 'uploads'
      | 'finalizations'
      | 'downloads',
    operation: () => Promise<T>
  ): Promise<T> {
    this.options.metrics.backend(counter)
    try {
      return await operation()
    } catch (error) {
      this.options.metrics.backend('errors')
      if (error instanceof BackendError && error.rateLimited) {
        this.options.metrics.backend('rateLimited')
      }
      throw error
    }
  }

  private handleRateLimit(error: unknown, circuit: RateCircuit): void {
    if (error instanceof BackendError && error.rateLimited) {
      circuit.trip(error.retryAfterMs)
    }
  }

  private circuitRequestError(error: unknown): RequestError {
    const seconds =
      error instanceof CircuitOpenError ? error.retryAfterSeconds : 1
    return new RequestError(429, 'cache service is rate limited', {
      'Retry-After': String(seconds)
    })
  }

  private backendRequestError(error: unknown): RequestError {
    if (error instanceof BackendError && error.rateLimited) {
      const seconds = Math.max(
        1,
        Math.ceil((error.retryAfterMs ?? DEFAULT_RATE_LIMIT_MS) / 1000)
      )
      return new RequestError(429, 'cache service is rate limited', {
        'Retry-After': String(seconds)
      })
    }
    return new RequestError(503, 'cache backend is temporarily unavailable')
  }

  private respondError(response: ServerResponse, error: unknown): void {
    if (response.headersSent) {
      response.destroy(error instanceof Error ? error : undefined)
      return
    }
    if (error instanceof RequestError) {
      writeEmpty(response, error.statusCode, error.headers)
      return
    }
    this.options.metrics.request('rejected')
    writeEmpty(response, 500)
  }
}
