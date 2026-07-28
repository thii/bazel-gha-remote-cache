import {createReadStream} from 'node:fs'
import {
  mkdir,
  open,
  readdir,
  readFile,
  rename,
  rm,
  stat
} from 'node:fs/promises'
import path from 'node:path'
import {
  BackendError,
  type CacheBackend,
  type CacheReservation
} from './backend.js'
import type {DiagnosticJournal} from './diagnostics.js'
import {Metrics} from './metrics.js'
import {
  CACHE_KEY_PREFIX,
  CACHE_VERSION,
  type CacheKind,
  type DaemonConfig
} from './model.js'
import {EntryPacer} from './pacer.js'
import {
  PackWriter,
  type PackSourceRecord,
  type SealedPack
} from './pack-writer.js'

export interface PendingObject {
  kind: CacheKind
  digest: string
  path: string
  manifestPath: string
  size: number
  bodySha256: string
  acceptedAt: number
  order: number
  casSequence?: bigint
  casBarrier?: bigint
}

export interface LocalPendingObject {
  size: number
  stream: ReturnType<typeof createReadStream>
}

export interface WriteBackSnapshot {
  pendingObjects: number
  pendingBytes: number
  openPackObjects: number
  openPackBytes: number
  durableCasHighWatermark: string
  acceptedCasSequence: string
  rateLimitPauseUntil: number | null
  accepting: boolean
  failed: boolean
}

export type WriteBackAcceptance =
  | {kind: 'accepted'}
  | {kind: 'duplicate'}
  | {kind: 'full'}
  | {kind: 'conflict'}
  | {kind: 'failed'}

interface PreparedEntry {
  key: string
  version: string
  path: string
  size: number
  isPack: boolean
  dispose(): Promise<void>
}

interface SeenObject {
  size: number
  bodySha256: string
}

export interface WriteBackQueueOptions {
  config: DaemonConfig
  backend: CacheBackend
  metrics: Metrics
  pacer: EntryPacer
  diagnostics?: DiagnosticJournal
  now?: () => number
  sleep?: (milliseconds: number, signal?: AbortSignal) => Promise<void>
}

const RETRY_AFTER_FALLBACK_MS = 60_000
const MAX_RETRY_DELAY_MS = 10_000
const COMPLETED_LOCAL_RETRY_MS = 1000
const MANIFEST_NAME = /^(\d{12})-(ac|cas)-([0-9a-f]{64})\.json$/
const MAX_MANIFEST_BYTES = 8192

function objectKey(namespace: string, kind: CacheKind, digest: string): string {
  const key = `${CACHE_KEY_PREFIX}-${namespace}-${kind}-sha256-${digest}`
  if (key.length > 512 || key.includes(',')) {
    throw new Error('generated cache key is invalid')
  }
  return key
}

function identity(kind: CacheKind, digest: string): string {
  return `${kind}:${digest}`
}

function manifestBigInt(
  value: unknown,
  name: string,
  allowZero: boolean
): bigint {
  if (typeof value !== 'string' || !/^(0|[1-9][0-9]*)$/.test(value)) {
    throw new Error(`write-back manifest has an invalid ${name}`)
  }
  const parsed = BigInt(value)
  if ((!allowZero && parsed === 0n) || parsed > 0xffff_ffff_ffff_ffffn) {
    throw new Error(`write-back manifest has an invalid ${name}`)
  }
  return parsed
}

function manifestNumber(
  value: unknown,
  name: string,
  minimum: number,
  maximum: number
): number {
  if (
    typeof value !== 'number' ||
    !Number.isSafeInteger(value) ||
    value < minimum ||
    value > maximum
  ) {
    throw new Error(`write-back manifest has an invalid ${name}`)
  }
  return value
}

function defaultSleep(
  milliseconds: number,
  signal?: AbortSignal
): Promise<void> {
  if (signal?.aborted) return Promise.reject(new Error('sleep aborted'))
  return new Promise<void>((resolve, reject) => {
    const timer = setTimeout(
      () => {
        signal?.removeEventListener('abort', onAbort)
        resolve()
      },
      Math.max(0, milliseconds)
    )
    const onAbort = (): void => {
      clearTimeout(timer)
      reject(new Error('sleep aborted'))
    }
    signal?.addEventListener('abort', onAbort, {once: true})
  })
}

async function syncDirectory(directory: string): Promise<void> {
  if (process.platform === 'win32') return
  const handle = await open(directory, 'r')
  try {
    await handle.sync()
  } finally {
    await handle.close().catch(() => {})
  }
}

class EntryCommitter {
  private lastRecoveryAt: number
  private lastRateLimitAt = 0

  constructor(
    private readonly backend: CacheBackend,
    private readonly pacer: EntryPacer,
    private readonly metrics: Metrics,
    private readonly diagnostics: DiagnosticJournal | undefined,
    private readonly now: () => number,
    private readonly sleep: (
      milliseconds: number,
      signal?: AbortSignal
    ) => Promise<void>
  ) {
    this.lastRecoveryAt = now()
    this.syncPacerMetrics()
  }

  async commit(
    entry: PreparedEntry,
    signal: AbortSignal
  ): Promise<'created' | 'already-exists'> {
    let reservation: CacheReservation
    let uploadAttempt = 0
    while (true) {
      await this.pacer.acquire(signal)
      this.syncPacerMetrics()
      try {
        reservation = await this.observed('reservations', 'reserve', () =>
          this.backend.reserve(entry.key, entry.version, signal)
        )
        break
      } catch (error) {
        if (!(error instanceof BackendError && error.rateLimited)) throw error
        await this.rateLimited(error, signal)
        if (await this.waitForVisibility(entry.key, entry.version, signal)) {
          this.maybeRecover()
          return 'already-exists'
        }
      }
    }

    if (reservation.kind === 'conflict') {
      if (await this.waitForVisibility(entry.key, entry.version, signal)) {
        this.maybeRecover()
        return 'already-exists'
      }
      throw new BackendError('conflicting cache entry did not become visible', {
        retryable: true
      })
    }
    if (!reservation.uploadUrl) {
      throw new BackendError('cache reservation omitted an upload URL')
    }

    while (true) {
      try {
        await this.observed('uploads', 'upload', () =>
          this.backend.uploadFile(
            reservation.uploadUrl as string,
            entry.path,
            entry.size,
            signal
          )
        )
        break
      } catch (error) {
        if (error instanceof BackendError && error.rateLimited) {
          await this.rateLimited(error, signal)
          if (await this.waitForVisibility(entry.key, entry.version, signal)) {
            this.maybeRecover()
            return 'already-exists'
          }
          continue
        }
        if (error instanceof BackendError && error.retryable) {
          await this.sleep(
            Math.min(MAX_RETRY_DELAY_MS, 250 * 2 ** uploadAttempt++),
            signal
          )
          continue
        }
        throw error
      }
    }

    let finalizeAttempt = 0
    while (true) {
      try {
        await this.observed('finalizations', 'finalize', () =>
          this.backend.finalize(entry.key, entry.version, entry.size, signal)
        )
        this.maybeRecover()
        return 'created'
      } catch (error) {
        if (error instanceof BackendError && error.rateLimited) {
          await this.rateLimited(error, signal)
          if (await this.waitForVisibility(entry.key, entry.version, signal)) {
            this.maybeRecover()
            return 'already-exists'
          }
          continue
        }
        if (
          error instanceof BackendError &&
          (error.retryable || error.conflict) &&
          (await this.waitForVisibility(entry.key, entry.version, signal))
        ) {
          this.maybeRecover()
          return 'already-exists'
        }
        if (error instanceof BackendError && error.retryable) {
          await this.sleep(
            Math.min(MAX_RETRY_DELAY_MS, 250 * 2 ** finalizeAttempt++),
            signal
          )
          continue
        }
        throw error
      }
    }
  }

  private async waitForVisibility(
    key: string,
    version: string,
    signal: AbortSignal
  ): Promise<boolean> {
    for (let attempt = 0; attempt < 5; attempt += 1) {
      try {
        const lookup = await this.observed('lookups', 'lookup', () =>
          this.backend.lookup(key, version, signal)
        )
        if (lookup.kind === 'hit') return true
      } catch (error) {
        if (!(error instanceof BackendError && error.rateLimited)) throw error
        await this.rateLimited(error, signal)
        continue
      }
      if (attempt < 4) await this.sleep(100 * 2 ** attempt, signal)
    }
    return false
  }

  private async observed<T>(
    counter: 'lookups' | 'reservations' | 'uploads' | 'finalizations',
    operation: 'lookup' | 'reserve' | 'upload' | 'finalize',
    call: () => Promise<T>
  ): Promise<T> {
    this.metrics.backend(counter)
    try {
      return await call()
    } catch (error) {
      this.metrics.backend('errors')
      this.diagnostics?.record({area: 'backend', operation}, error)
      if (error instanceof BackendError && error.rateLimited) {
        this.metrics.backend('rateLimited')
        this.metrics.rateLimit(operation)
      }
      throw error
    }
  }

  private async rateLimited(
    error: BackendError,
    signal: AbortSignal
  ): Promise<void> {
    this.lastRateLimitAt = this.now()
    this.lastRecoveryAt = this.lastRateLimitAt
    this.pacer.recordRateLimit(
      error.retryAfterMs ?? RETRY_AFTER_FALLBACK_MS,
      1000
    )
    this.syncPacerMetrics()
    await this.pacer.waitForResume(signal)
    this.syncPacerMetrics()
  }

  private maybeRecover(): void {
    const now = this.now()
    if (
      now - this.lastRecoveryAt >= 60_000 &&
      this.lastRateLimitAt <= this.lastRecoveryAt
    ) {
      this.pacer.recordCleanMinute()
      this.lastRecoveryAt = now
      this.syncPacerMetrics()
    }
  }

  private syncPacerMetrics(): void {
    const snapshot = this.pacer.snapshot
    this.metrics.setPacer(
      snapshot.configuredEntriesPerMinute,
      snapshot.currentEntriesPerMinute,
      Math.round(snapshot.totalSleepMs)
    )
  }
}

export class WriteBackQueue {
  private readonly directory: string
  private readonly pendingDirectory: string
  private readonly packWriter: PackWriter
  private readonly committer: EntryCommitter
  private readonly now: () => number
  private readonly sleep: (
    milliseconds: number,
    signal?: AbortSignal
  ) => Promise<void>
  private readonly queue: PendingObject[] = []
  private readonly local = new Map<string, PendingObject>()
  private readonly completedLocal = new Map<string, PendingObject>()
  private readonly seen = new Map<string, SeenObject>()
  private readonly expiryTimers = new Set<NodeJS.Timeout>()
  private readonly completedExpiryTimers = new Map<
    PendingObject,
    NodeJS.Timeout
  >()
  private readonly completedCas = new Set<bigint>()
  private acceptedCasSequence = 0n
  private durableCasHighWatermark = 0n
  private pendingBytes = 0
  private residentSourceBytes = 0
  private nextOrder = 0
  private packSequence = 0n
  private accepting = true
  private draining = false
  private stopped = false
  private failed: unknown
  private workerPromise: Promise<void> | undefined
  private workerController = new AbortController()
  private changeGeneration = 0
  private changeWaiters: Array<() => void> = []
  private lock: Promise<void> = Promise.resolve()

  constructor(private readonly options: WriteBackQueueOptions) {
    this.directory = path.join(options.config.controlDirectory, 'writeback')
    this.pendingDirectory = path.join(this.directory, 'pending')
    this.now = options.now ?? Date.now
    this.sleep = options.sleep ?? defaultSleep
    this.packWriter = new PackWriter({
      directory: path.join(this.directory, 'packs'),
      namespace: options.config.namespace,
      runId: options.config.runId,
      jobHash: options.config.jobHash
    })
    this.committer = new EntryCommitter(
      options.backend,
      options.pacer,
      options.metrics,
      options.diagnostics,
      this.now,
      this.sleep
    )
  }

  async start(): Promise<void> {
    if (this.workerPromise !== undefined) {
      throw new Error('write-back queue already started')
    }
    await mkdir(this.pendingDirectory, {recursive: true, mode: 0o700})
    await rm(path.join(this.directory, 'packs'), {recursive: true, force: true})
    await this.recoverPending()
    this.workerPromise = this.worker()
  }

  async accept(
    kind: CacheKind,
    digest: string,
    spoolPath: string,
    size: number,
    bodySha256: string
  ): Promise<WriteBackAcceptance> {
    return this.exclusive(async () => {
      if (!this.accepting || this.failed !== undefined) return {kind: 'failed'}
      const objectIdentity = identity(kind, digest)
      const existing = this.seen.get(objectIdentity)
      if (existing !== undefined) {
        await rm(spoolPath, {force: true}).catch(() => {})
        if (existing.size !== size || existing.bodySha256 !== bodySha256) {
          return {kind: 'conflict'}
        }
        this.options.metrics.deduplicatedObject()
        return {kind: 'duplicate'}
      }
      await this.reclaimResidentCapacity(size)
      if (
        size >
        this.options.config.maxPendingBytes - this.residentSourceBytes
      ) {
        return {kind: 'full'}
      }

      const order = this.nextOrder++
      const casSequence =
        kind === 'cas' ? this.acceptedCasSequence + 1n : undefined
      const casBarrier = kind === 'ac' ? this.acceptedCasSequence : undefined
      const stem = `${order.toString().padStart(12, '0')}-${kind}-${digest}`
      const dataPath = path.join(this.pendingDirectory, `${stem}.data`)
      const manifestPath = path.join(this.pendingDirectory, `${stem}.json`)
      const temporaryManifestPath = `${manifestPath}.tmp`
      await rename(spoolPath, dataPath)
      const record: PendingObject = {
        kind,
        digest,
        path: dataPath,
        manifestPath,
        size,
        bodySha256,
        acceptedAt: this.now(),
        order,
        ...(casSequence === undefined ? {} : {casSequence}),
        ...(casBarrier === undefined ? {} : {casBarrier})
      }
      try {
        const manifest = await open(temporaryManifestPath, 'wx', 0o600)
        try {
          await manifest.writeFile(
            `${JSON.stringify({
              ...record,
              ...(casSequence === undefined
                ? {}
                : {casSequence: casSequence.toString()}),
              ...(casBarrier === undefined
                ? {}
                : {casBarrier: casBarrier.toString()})
            })}\n`,
            {encoding: 'utf8'}
          )
          await manifest.sync()
        } finally {
          await manifest.close().catch(() => {})
        }
        await rename(temporaryManifestPath, manifestPath)
        await syncDirectory(this.pendingDirectory)
      } catch (error) {
        await rm(dataPath, {force: true}).catch(() => {})
        await rm(manifestPath, {force: true}).catch(() => {})
        await rm(temporaryManifestPath, {force: true}).catch(() => {})
        throw error
      }
      if (casSequence !== undefined) this.acceptedCasSequence = casSequence
      this.queue.push(record)
      this.local.set(objectIdentity, record)
      this.seen.set(objectIdentity, {size, bodySha256})
      this.pendingBytes += size
      this.residentSourceBytes += size
      this.options.metrics.acceptedObject()
      this.updatePendingMetrics()
      this.notifyChange()
      return {kind: 'accepted'}
    })
  }

  async openLocal(
    kind: CacheKind,
    digest: string
  ): Promise<LocalPendingObject | undefined> {
    return this.exclusive(async () => {
      const record = this.local.get(identity(kind, digest))
      if (record === undefined) return undefined
      const file = await open(record.path, 'r')
      return {
        size: record.size,
        stream: file.createReadStream({autoClose: true})
      }
    })
  }

  snapshot(): WriteBackSnapshot {
    const pacer = this.options.pacer.snapshot
    return {
      pendingObjects: this.queue.length,
      pendingBytes: this.pendingBytes,
      openPackObjects: this.currentBatchSize(),
      openPackBytes: this.currentBatchBytes(),
      durableCasHighWatermark: this.durableCasHighWatermark.toString(),
      acceptedCasSequence: this.acceptedCasSequence.toString(),
      rateLimitPauseUntil: pacer.pauseUntil,
      accepting: this.accepting,
      failed: this.failed !== undefined
    }
  }

  async drain(deadlineMs: number): Promise<WriteBackSnapshot> {
    const deadline = this.now() + Math.max(0, deadlineMs)
    await this.exclusive(async () => {
      this.accepting = false
      this.draining = true
      this.notifyChange()
    })
    while (
      this.queue.length > 0 &&
      this.failed === undefined &&
      this.now() < deadline
    ) {
      const remaining = Math.max(1, Math.min(250, deadline - this.now()))
      await Promise.race([
        this.waitForChange(this.changeGeneration),
        this.sleep(remaining)
      ]).catch(() => {})
    }
    if (this.queue.length > 0) this.workerController.abort()
    this.stopped = true
    this.notifyChange()
    await this.workerPromise?.catch(() => {})
    this.options.pacer.shutdown()
    for (const timer of this.expiryTimers) clearTimeout(timer)
    this.expiryTimers.clear()
    this.completedExpiryTimers.clear()
    await this.exclusive(async () => {
      for (const record of [...this.completedLocal.values()]) {
        await this.removeCompletedLocal(record)
      }
    })
    this.recordRemainingObjects()
    this.updatePendingMetrics()
    return this.snapshot()
  }

  private async worker(): Promise<void> {
    while (!this.stopped) {
      if (this.queue.length === 0 || this.failed !== undefined) {
        const generation = this.changeGeneration
        await this.waitForChange(generation)
        continue
      }
      const oldest = this.queue[0]
      if (oldest === undefined) continue
      const batch = this.selectBatch()
      const targetReached =
        batch.length >= this.options.config.packMaxObjects ||
        batch.reduce((total, record) => total + record.size, 0) >=
          this.options.config.packTargetBytes
      const remainingAge =
        this.options.config.packMaxAgeSeconds * 1000 -
        (this.now() - oldest.acceptedAt)
      if (
        this.options.config.storageMode === 'pack' &&
        !this.draining &&
        !targetReached &&
        remainingAge > 0
      ) {
        const generation = this.changeGeneration
        await this.waitForChangeOrTimeout(
          generation,
          remainingAge,
          this.workerController.signal
        ).catch(() => {})
        continue
      }

      try {
        await this.commitBatch(batch, this.workerController.signal)
      } catch (error) {
        if (this.workerController.signal.aborted) break
        if (error instanceof BackendError && error.retryable) {
          await this.sleep(1000, this.workerController.signal).catch(() => {})
          continue
        }
        this.options.diagnostics?.record(
          {area: 'write-back', operation: 'worker'},
          error
        )
        this.failed = error
        this.recordRemainingObjects()
        this.notifyChange()
      }
    }
  }

  private selectBatch(): PendingObject[] {
    if (this.options.config.storageMode === 'object') {
      const first = this.queue[0]
      return first === undefined ? [] : [first]
    }
    const records: PendingObject[] = []
    let bytes = 0
    for (const record of this.queue) {
      if (records.length >= this.options.config.packMaxObjects) break
      if (
        records.length > 0 &&
        bytes + record.size > this.options.config.packTargetBytes
      ) {
        break
      }
      records.push(record)
      bytes += record.size
    }
    return records
  }

  private async commitBatch(
    records: readonly PendingObject[],
    signal: AbortSignal
  ): Promise<void> {
    if (records.length === 0) return
    this.assertAcBarriers(records)
    const prepared = await this.prepare(records, signal)
    let attempt = 0
    try {
      while (true) {
        try {
          await this.committer.commit(prepared, signal)
          break
        } catch (error) {
          if (signal.aborted) throw error
          if (!(error instanceof BackendError && error.retryable)) throw error
          const delay = Math.min(MAX_RETRY_DELAY_MS, 250 * 2 ** attempt)
          attempt += 1
          await this.sleep(delay, signal)
        }
      }
      if (prepared.isPack) {
        this.options.metrics.packFinalized(prepared.size)
      }
      await this.completeRecords(records)
    } finally {
      await prepared.dispose().catch(() => {})
    }
  }

  private async prepare(
    records: readonly PendingObject[],
    signal: AbortSignal
  ): Promise<PreparedEntry> {
    if (this.options.config.storageMode === 'object') {
      const record = records[0]
      if (record === undefined) throw new Error('object entry is missing')
      return {
        key: objectKey(
          this.options.config.namespace,
          record.kind,
          record.digest
        ),
        version: CACHE_VERSION,
        path: record.path,
        size: record.size,
        isPack: false,
        dispose: async () => {}
      }
    }

    const sequence = this.packSequence++
    const pack: SealedPack = await this.packWriter.seal(
      records.map<PackSourceRecord>(record => ({
        kind: record.kind,
        digest: record.digest,
        path: record.path,
        size: record.size,
        bodySha256: record.bodySha256
      })),
      sequence,
      signal
    )
    this.options.metrics.packedObjects(records.length)
    return {
      key: pack.key,
      version: pack.version,
      path: pack.path,
      size: pack.size,
      isPack: true,
      dispose: pack.dispose
    }
  }

  private assertAcBarriers(records: readonly PendingObject[]): void {
    let covered = this.durableCasHighWatermark
    for (const record of records) {
      if (record.casSequence !== undefined) {
        if (record.casSequence !== covered + 1n) {
          throw new Error('pack crossed a non-contiguous CAS sequence')
        }
        covered = record.casSequence
      }
      if (record.casBarrier !== undefined && record.casBarrier > covered) {
        this.options.metrics.setAcBlockedByBarrier(1)
        throw new Error('action-cache pack crossed an undurable CAS barrier')
      }
    }
    this.options.metrics.setAcBlockedByBarrier(0)
  }

  private async completeRecords(
    records: readonly PendingObject[]
  ): Promise<void> {
    await this.exclusive(async () => {
      for (const record of records) {
        if (this.queue[0] !== record) {
          throw new Error('write-back completion order was violated')
        }
        this.queue.shift()
        this.pendingBytes -= record.size
        if (record.casSequence !== undefined) {
          this.completedCas.add(record.casSequence)
        }
        // The remote object is already durable. A local cleanup failure must not
        // fail the worker and strand later records behind an obsolete manifest.
        await rm(record.manifestPath, {force: true}).catch(() => {})
        this.retainCompletedLocal(record)
      }
      while (this.completedCas.has(this.durableCasHighWatermark + 1n)) {
        this.completedCas.delete(this.durableCasHighWatermark + 1n)
        this.durableCasHighWatermark += 1n
      }
      this.updatePendingMetrics()
      this.notifyChange()
    })
  }

  private updatePendingMetrics(): void {
    this.options.metrics.setPending(this.queue.length, this.pendingBytes)
  }

  private recordRemainingObjects(): void {
    this.options.metrics.setRemainingObjects(
      this.queue.length,
      this.queue
        .slice(0, 100)
        .map(record => identity(record.kind, record.digest))
    )
  }

  private async recoverPending(): Promise<void> {
    const names = await readdir(this.pendingDirectory)
    const manifests: PendingObject[] = []
    const referencedData = new Set<string>()
    for (const name of names.sort()) {
      if (!name.endsWith('.json')) continue
      const match = MANIFEST_NAME.exec(name)
      if (match === null) throw new Error('write-back manifest name is invalid')
      const [, rawOrder, rawKind, digest] = match
      if (
        rawOrder === undefined ||
        (rawKind !== 'ac' && rawKind !== 'cas') ||
        digest === undefined
      ) {
        throw new Error('write-back manifest name is invalid')
      }
      const order = Number(rawOrder)
      const manifestPath = path.join(this.pendingDirectory, name)
      const manifestStats = await stat(manifestPath)
      if (!manifestStats.isFile() || manifestStats.size > MAX_MANIFEST_BYTES) {
        throw new Error('write-back manifest is invalid')
      }
      let value: unknown
      try {
        value = JSON.parse(await readFile(manifestPath, 'utf8')) as unknown
      } catch {
        throw new Error('write-back manifest is invalid')
      }
      if (typeof value !== 'object' || value === null || Array.isArray(value)) {
        throw new Error('write-back manifest is invalid')
      }
      const data = value as Record<string, unknown>
      const dataName = name.replace(/\.json$/, '.data')
      const dataPath = path.join(this.pendingDirectory, dataName)
      const size = manifestNumber(
        data['size'],
        'size',
        0,
        this.options.config.maxObjectSize
      )
      const acceptedAt = manifestNumber(
        data['acceptedAt'],
        'acceptance time',
        0,
        Number.MAX_SAFE_INTEGER
      )
      if (
        data['kind'] !== rawKind ||
        data['digest'] !== digest ||
        data['order'] !== order ||
        data['path'] !== dataPath ||
        data['manifestPath'] !== manifestPath ||
        typeof data['bodySha256'] !== 'string' ||
        !/^[0-9a-f]{64}$/.test(data['bodySha256'])
      ) {
        throw new Error('write-back manifest is inconsistent')
      }
      const bodySha256 = data['bodySha256']
      if (rawKind === 'cas' && bodySha256 !== digest) {
        throw new Error('write-back CAS manifest is inconsistent')
      }
      const dataStats = await stat(dataPath)
      if (!dataStats.isFile() || dataStats.size !== size) {
        throw new Error('write-back data does not match its manifest')
      }
      const record: PendingObject = {
        kind: rawKind,
        digest,
        path: dataPath,
        manifestPath,
        size,
        bodySha256,
        acceptedAt,
        order,
        ...(rawKind === 'cas'
          ? {
              casSequence: manifestBigInt(
                data['casSequence'],
                'CAS sequence',
                false
              )
            }
          : {
              casBarrier: manifestBigInt(
                data['casBarrier'],
                'CAS barrier',
                true
              )
            })
      }
      if (
        (rawKind === 'cas' && data['casBarrier'] !== undefined) ||
        (rawKind === 'ac' && data['casSequence'] !== undefined)
      ) {
        throw new Error('write-back manifest has invalid CAS ordering fields')
      }
      manifests.push(record)
      referencedData.add(dataName)
    }

    for (const name of names) {
      if (name.endsWith('.data') && !referencedData.has(name)) {
        await rm(path.join(this.pendingDirectory, name), {force: true})
      }
      if (name.endsWith('.json.tmp')) {
        await rm(path.join(this.pendingDirectory, name), {force: true})
      }
    }
    manifests.sort((left, right) => left.order - right.order)
    const firstPendingCas = manifests.find(
      record => record.casSequence !== undefined
    )?.casSequence
    let durable = firstPendingCas === undefined ? 0n : firstPendingCas - 1n
    if (firstPendingCas === undefined) {
      for (const record of manifests) {
        if (record.casBarrier !== undefined && record.casBarrier > durable) {
          durable = record.casBarrier
        }
      }
    }
    let accepted = durable
    let pendingBytes = 0
    let previousOrder = -1
    for (const record of manifests) {
      if (record.order <= previousOrder) {
        throw new Error('write-back manifest order is not unique')
      }
      previousOrder = record.order
      if (record.casSequence !== undefined) {
        if (record.casSequence !== accepted + 1n) {
          throw new Error('write-back CAS sequence is not contiguous')
        }
        accepted = record.casSequence
      }
      if (record.casBarrier !== undefined && record.casBarrier > accepted) {
        throw new Error('write-back AC barrier is not covered')
      }
      const objectIdentity = identity(record.kind, record.digest)
      if (this.seen.has(objectIdentity)) {
        throw new Error('write-back manifests contain a duplicate object')
      }
      pendingBytes += record.size
      if (pendingBytes > this.options.config.maxPendingBytes) {
        throw new Error('recovered write-back data exceeds max-pending-bytes')
      }
      this.queue.push(record)
      this.local.set(objectIdentity, record)
      this.seen.set(objectIdentity, {
        size: record.size,
        bodySha256: record.bodySha256
      })
    }
    this.pendingBytes = pendingBytes
    this.residentSourceBytes = pendingBytes
    this.durableCasHighWatermark = durable
    this.acceptedCasSequence = accepted
    this.nextOrder = previousOrder + 1
    this.updatePendingMetrics()
  }

  private retainCompletedLocal(record: PendingObject): void {
    const objectIdentity = identity(record.kind, record.digest)
    this.completedLocal.set(objectIdentity, record)
    const retentionMs = Math.max(
      30_000,
      this.options.config.catalogRefreshSeconds * 2000
    )
    this.scheduleCompletedExpiry(record, retentionMs)
  }

  private scheduleCompletedExpiry(
    record: PendingObject,
    delayMs: number
  ): void {
    const timer = setTimeout(() => {
      this.expiryTimers.delete(timer)
      this.completedExpiryTimers.delete(record)
      void this.exclusive(async () => {
        const removed = await this.removeCompletedLocal(record)
        if (
          !removed &&
          !this.stopped &&
          this.completedLocal.get(identity(record.kind, record.digest)) ===
            record
        ) {
          this.scheduleCompletedExpiry(record, COMPLETED_LOCAL_RETRY_MS)
        }
      }).catch(() => {})
    }, delayMs)
    timer.unref()
    this.expiryTimers.add(timer)
    this.completedExpiryTimers.set(record, timer)
  }

  private async reclaimResidentCapacity(size: number): Promise<void> {
    for (const record of [...this.completedLocal.values()]) {
      if (
        size <=
        this.options.config.maxPendingBytes - this.residentSourceBytes
      ) {
        return
      }
      await this.removeCompletedLocal(record)
    }
  }

  private async removeCompletedLocal(record: PendingObject): Promise<boolean> {
    const objectIdentity = identity(record.kind, record.digest)
    if (this.completedLocal.get(objectIdentity) !== record) return true
    try {
      // A manifest left behind by a previously failed unlink must never point
      // at source data that local retention has removed. If it remains, a
      // restart can safely replay the remotely durable record idempotently.
      await rm(record.manifestPath, {force: true})
      await rm(record.path, {force: true})
    } catch {
      return false
    }
    this.completedLocal.delete(objectIdentity)
    if (this.local.get(objectIdentity) === record) {
      this.local.delete(objectIdentity)
    }
    const timer = this.completedExpiryTimers.get(record)
    if (timer !== undefined) {
      clearTimeout(timer)
      this.expiryTimers.delete(timer)
      this.completedExpiryTimers.delete(record)
    }
    this.residentSourceBytes -= record.size
    return true
  }

  private currentBatchSize(): number {
    return this.selectBatch().length
  }

  private currentBatchBytes(): number {
    return this.selectBatch().reduce((total, record) => total + record.size, 0)
  }

  private notifyChange(): void {
    this.changeGeneration += 1
    for (const resolve of this.changeWaiters.splice(0)) resolve()
  }

  private waitForChange(
    generation: number,
    signal?: AbortSignal
  ): Promise<void> {
    if (generation !== this.changeGeneration) return Promise.resolve()
    if (signal?.aborted) return Promise.reject(new Error('change wait aborted'))
    return new Promise((resolve, reject) => {
      let settled = false
      const finish = (): boolean => {
        if (settled) return false
        settled = true
        signal?.removeEventListener('abort', onAbort)
        return true
      }
      const onChange = (): void => {
        if (finish()) resolve()
      }
      const onAbort = (): void => {
        if (!finish()) return
        const index = this.changeWaiters.indexOf(onChange)
        if (index !== -1) this.changeWaiters.splice(index, 1)
        reject(new Error('change wait aborted'))
      }
      this.changeWaiters.push(onChange)
      signal?.addEventListener('abort', onAbort, {once: true})
      if (signal?.aborted) onAbort()
    })
  }

  private async waitForChangeOrTimeout(
    generation: number,
    milliseconds: number,
    signal?: AbortSignal
  ): Promise<void> {
    const controller = new AbortController()
    const onAbort = (): void => controller.abort()
    signal?.addEventListener('abort', onAbort, {once: true})
    if (signal?.aborted) controller.abort()
    try {
      await Promise.race([
        this.waitForChange(generation, controller.signal),
        this.sleep(milliseconds, controller.signal)
      ])
    } finally {
      controller.abort()
      signal?.removeEventListener('abort', onAbort)
    }
  }

  private async exclusive<T>(operation: () => Promise<T>): Promise<T> {
    let release!: () => void
    const previous = this.lock
    this.lock = new Promise<void>(resolve => {
      release = resolve
    })
    await previous
    try {
      return await operation()
    } finally {
      release()
    }
  }
}
