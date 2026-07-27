export type EntryPacerClock = () => number
export type EntryPacerRandom = () => number
export type EntryPacerSleep = (
  milliseconds: number,
  signal: AbortSignal
) => Promise<void>

export interface EntryPacerOptions {
  repositoryUploadBudget: number
  expectedWriters: number
  uploadBurst: number
  now?: EntryPacerClock
  sleep?: EntryPacerSleep
  random?: EntryPacerRandom
}

export interface EntryPacerSnapshot {
  repositoryUploadBudget: number
  expectedWriters: number
  uploadBurst: number
  configuredEntriesPerMinute: number
  currentEntriesPerMinute: number
  pauseUntil: number | null
  totalSleepMs: number
  sleepCount: number
  rateLimitCount: number
  entriesGranted: number
  queued: number
  closed: boolean
}

interface Waiter {
  resolve: () => void
  reject: (error: Error) => void
  signal?: AbortSignal
  onAbort?: () => void
}

const MILLISECONDS_PER_MINUTE = 60_000
const TOKEN_EPSILON = 1e-9
const MAX_TIMER_MS = 2_147_483_647

function abortError(message: string): Error {
  const error = new Error(message)
  error.name = 'AbortError'
  return error
}

function defaultSleep(
  milliseconds: number,
  signal: AbortSignal
): Promise<void> {
  if (signal.aborted) return Promise.reject(abortError('pacer sleep aborted'))

  return new Promise<void>((resolve, reject) => {
    const timer = setTimeout(
      () => {
        signal.removeEventListener('abort', onAbort)
        resolve()
      },
      Math.min(MAX_TIMER_MS, Math.max(0, Math.ceil(milliseconds)))
    )
    const onAbort = (): void => {
      clearTimeout(timer)
      reject(abortError('pacer sleep aborted'))
    }
    signal.addEventListener('abort', onAbort, {once: true})
  })
}

function positiveFinite(value: number, name: string): void {
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error(`${name} must be a positive finite number`)
  }
}

function nonNegativeFinite(value: number, name: string): void {
  if (!Number.isFinite(value) || value < 0) {
    throw new Error(`${name} must be a non-negative finite number`)
  }
}

/**
 * A FIFO token bucket for Cache v2 entry creation.
 *
 * Call acquire immediately before reserving an entry. Uploading blocks and
 * finalizing the entry do not consume additional tokens.
 */
export class EntryPacer {
  readonly repositoryUploadBudget: number
  readonly expectedWriters: number
  readonly uploadBurst: number
  readonly configuredEntriesPerMinute: number

  private readonly now: EntryPacerClock
  private readonly sleep: EntryPacerSleep
  private readonly random: EntryPacerRandom
  private readonly waiters: Waiter[] = []
  private currentEntriesPerMinuteValue: number
  private tokens: number
  private lastRefillAt: number
  private pauseUntilValue = 0
  private totalSleepMsValue = 0
  private sleepCountValue = 0
  private rateLimitCountValue = 0
  private entriesGrantedValue = 0
  private draining = false
  private closedValue = false
  private sleepController: AbortController | undefined
  private readonly resumeSleepControllers = new Set<AbortController>()

  constructor(options: EntryPacerOptions) {
    positiveFinite(options.repositoryUploadBudget, 'repository upload budget')
    if (
      !Number.isSafeInteger(options.expectedWriters) ||
      options.expectedWriters < 1
    ) {
      throw new Error('expected writers must be a positive safe integer')
    }
    if (!Number.isSafeInteger(options.uploadBurst) || options.uploadBurst < 1) {
      throw new Error('upload burst must be a positive safe integer')
    }

    this.repositoryUploadBudget = options.repositoryUploadBudget
    this.expectedWriters = options.expectedWriters
    this.uploadBurst = options.uploadBurst
    this.configuredEntriesPerMinute =
      options.repositoryUploadBudget / options.expectedWriters
    this.currentEntriesPerMinuteValue = this.configuredEntriesPerMinute
    this.tokens = options.uploadBurst
    this.now = options.now ?? Date.now
    this.sleep = options.sleep ?? defaultSleep
    this.random = options.random ?? Math.random
    this.lastRefillAt = this.readClock()
  }

  get currentEntriesPerMinute(): number {
    return this.currentEntriesPerMinuteValue
  }

  get queued(): number {
    return this.waiters.length
  }

  get closed(): boolean {
    return this.closedValue
  }

  get snapshot(): EntryPacerSnapshot {
    const now = this.readClock()
    return {
      repositoryUploadBudget: this.repositoryUploadBudget,
      expectedWriters: this.expectedWriters,
      uploadBurst: this.uploadBurst,
      configuredEntriesPerMinute: this.configuredEntriesPerMinute,
      currentEntriesPerMinute: this.currentEntriesPerMinuteValue,
      pauseUntil: this.pauseUntilValue > now ? this.pauseUntilValue : null,
      totalSleepMs: this.totalSleepMsValue,
      sleepCount: this.sleepCountValue,
      rateLimitCount: this.rateLimitCountValue,
      entriesGranted: this.entriesGrantedValue,
      queued: this.waiters.length,
      closed: this.closedValue
    }
  }

  acquire(signal?: AbortSignal): Promise<void> {
    if (this.closedValue) {
      return Promise.reject(new Error('entry pacer is shut down'))
    }
    if (signal?.aborted) {
      return Promise.reject(abortError('entry pacer wait aborted'))
    }

    const promise = new Promise<void>((resolve, reject) => {
      const waiter: Waiter = {resolve, reject}
      if (signal !== undefined) {
        waiter.signal = signal
        waiter.onAbort = () => {
          const index = this.waiters.indexOf(waiter)
          if (index === -1) return
          this.waiters.splice(index, 1)
          reject(abortError('entry pacer wait aborted'))
          if (index === 0) this.wakeScheduler()
        }
        signal.addEventListener('abort', waiter.onAbort, {once: true})
      }
      this.waiters.push(waiter)
    })

    this.startDrain()
    return promise
  }

  /** Wait for the active Retry-After pause without consuming a bucket token. */
  async waitForResume(signal?: AbortSignal): Promise<void> {
    if (this.closedValue) throw new Error('entry pacer is shut down')
    if (signal?.aborted) {
      throw abortError('entry pacer resume wait aborted')
    }

    while (!this.closedValue) {
      if (signal?.aborted) {
        throw abortError('entry pacer resume wait aborted')
      }
      const delay = this.pauseUntilValue - this.readClock()
      if (delay <= 0) return

      const controller = new AbortController()
      const onAbort = (): void => controller.abort()
      if (signal !== undefined) {
        signal.addEventListener('abort', onAbort, {once: true})
      }
      this.resumeSleepControllers.add(controller)
      try {
        await this.measuredSleep(
          Math.min(MAX_TIMER_MS, Math.max(1, Math.ceil(delay))),
          controller.signal
        )
      } catch (error) {
        if (signal?.aborted) {
          throw abortError('entry pacer resume wait aborted')
        }
        if (this.closedValue) throw new Error('entry pacer is shut down')
        if (!controller.signal.aborted) throw error
      } finally {
        this.resumeSleepControllers.delete(controller)
        if (signal !== undefined) {
          signal.removeEventListener('abort', onAbort)
        }
      }
    }

    throw new Error('entry pacer is shut down')
  }

  /** Extend the global reservation pause by a duration plus caller-sized jitter. */
  pauseFor(milliseconds: number, jitterMaxMs = 0): number {
    nonNegativeFinite(milliseconds, 'pause duration')
    nonNegativeFinite(jitterMaxMs, 'pause jitter')

    let jitter = 0
    if (jitterMaxMs > 0) {
      const random = this.random()
      if (!Number.isFinite(random) || random < 0 || random >= 1) {
        throw new Error('random source must return a value from 0 up to 1')
      }
      jitter = Math.floor(random * jitterMaxMs)
    }

    const pauseUntil = this.readClock() + milliseconds + jitter
    if (pauseUntil > this.pauseUntilValue) {
      this.pauseUntilValue = pauseUntil
      this.wakeScheduler()
    }
    return this.pauseUntilValue
  }

  /** Apply 429 handling: halve the rate and honor Retry-After plus jitter. */
  recordRateLimit(
    retryAfterMs = MILLISECONDS_PER_MINUTE,
    jitterMaxMs = 1000
  ): void {
    this.refill(this.readClock())
    this.pauseFor(retryAfterMs, jitterMaxMs)
    this.currentEntriesPerMinuteValue = Math.max(
      Number.MIN_VALUE,
      this.currentEntriesPerMinuteValue * 0.5
    )
    this.rateLimitCountValue += 1
    this.wakeScheduler()
  }

  /** Recover 10% after one clean minute, capped at the configured rate. */
  recordCleanMinute(): void {
    this.refill(this.readClock())
    this.currentEntriesPerMinuteValue = Math.min(
      this.configuredEntriesPerMinute,
      this.currentEntriesPerMinuteValue + this.configuredEntriesPerMinute * 0.1
    )
    this.wakeScheduler()
  }

  shutdown(): void {
    if (this.closedValue) return
    this.closedValue = true
    this.wakeScheduler()
    for (const controller of this.resumeSleepControllers) controller.abort()

    const error = new Error('entry pacer is shut down')
    for (const waiter of this.waiters.splice(0)) {
      this.removeAbortListener(waiter)
      waiter.reject(error)
    }
  }

  private readClock(): number {
    const now = this.now()
    if (!Number.isFinite(now)) {
      throw new Error('clock must return a finite millisecond timestamp')
    }
    return now
  }

  private refill(now: number): void {
    if (now <= this.lastRefillAt) return
    const elapsed = now - this.lastRefillAt
    this.tokens = Math.min(
      this.uploadBurst,
      this.tokens +
        (elapsed * this.currentEntriesPerMinuteValue) / MILLISECONDS_PER_MINUTE
    )
    this.lastRefillAt = now
  }

  private startDrain(): void {
    if (this.draining || this.closedValue) return
    this.draining = true
    void this.drain()
      .catch(error => {
        const failure =
          error instanceof Error ? error : new Error('entry pacer failed')
        for (const waiter of this.waiters.splice(0)) {
          this.removeAbortListener(waiter)
          waiter.reject(failure)
        }
      })
      .finally(() => {
        this.draining = false
        if (!this.closedValue && this.waiters.length > 0) this.startDrain()
      })
  }

  private async drain(): Promise<void> {
    while (!this.closedValue && this.waiters.length > 0) {
      const now = this.readClock()
      this.refill(now)

      if (now >= this.pauseUntilValue && this.tokens + TOKEN_EPSILON >= 1) {
        this.tokens = Math.max(0, this.tokens - 1)
        const waiter = this.waiters.shift()
        if (waiter === undefined) continue
        this.removeAbortListener(waiter)
        this.entriesGrantedValue += 1
        waiter.resolve()
        continue
      }

      const tokenDelay =
        this.tokens + TOKEN_EPSILON >= 1
          ? 0
          : ((1 - this.tokens) * MILLISECONDS_PER_MINUTE) /
            this.currentEntriesPerMinuteValue
      const delay = Math.max(this.pauseUntilValue - now, tokenDelay)
      await this.wait(Math.min(MAX_TIMER_MS, Math.max(1, Math.ceil(delay))))
    }
  }

  private async wait(milliseconds: number): Promise<void> {
    const controller = new AbortController()
    this.sleepController = controller
    try {
      await this.measuredSleep(milliseconds, controller.signal)
    } catch (error) {
      if (!controller.signal.aborted) throw error
    } finally {
      if (this.sleepController === controller) {
        this.sleepController = undefined
      }
    }
  }

  private async measuredSleep(
    milliseconds: number,
    signal: AbortSignal
  ): Promise<void> {
    const startedAt = this.readClock()
    this.sleepCountValue += 1
    try {
      await this.sleep(milliseconds, signal)
    } finally {
      this.totalSleepMsValue += Math.max(0, this.readClock() - startedAt)
    }
  }

  private wakeScheduler(): void {
    this.sleepController?.abort()
  }

  private removeAbortListener(waiter: Waiter): void {
    if (waiter.signal !== undefined && waiter.onAbort !== undefined) {
      waiter.signal.removeEventListener('abort', waiter.onAbort)
    }
  }
}
