import type {CacheKind, KindCounters, MetricsSnapshot} from './model.js'

function counters(): KindCounters {
  return {
    hits: 0,
    misses: 0,
    successes: 0,
    conflicts: 0,
    errors: 0,
    bytes: 0,
    latencyMs: 0
  }
}

export class Metrics {
  private readonly data: MetricsSnapshot
  private persistChain: Promise<void> = Promise.resolve()
  private persistenceError: unknown

  constructor(
    readable: boolean,
    writable: boolean,
    private readonly persist?: (snapshot: MetricsSnapshot) => Promise<void>
  ) {
    this.data = {
      schemaVersion: 1,
      startedAt: new Date().toISOString(),
      readable,
      writable,
      requests: {
        health: 0,
        shutdown: 0,
        get: 0,
        put: 0,
        rejected: 0
      },
      reads: {ac: counters(), cas: counters()},
      writes: {ac: counters(), cas: counters()},
      backend: {
        lookups: 0,
        reservations: 0,
        uploads: 0,
        finalizations: 0,
        downloads: 0,
        errors: 0,
        rateLimited: 0
      },
      integrityFailures: 0,
      casWriteFailed: false,
      writeCircuitOpen: false,
      readCircuitOpen: false,
      inflightBytes: 0,
      peakInflightBytes: 0
    }
  }

  snapshot(): MetricsSnapshot {
    return structuredClone(this.data)
  }

  stop(): void {
    this.data.stoppedAt = new Date().toISOString()
    this.schedulePersist()
  }

  request(name: keyof MetricsSnapshot['requests']): void {
    this.data.requests[name] += 1
  }

  backend(name: keyof MetricsSnapshot['backend']): void {
    this.data.backend[name] += 1
  }

  read(
    kind: CacheKind,
    outcome: 'hit' | 'miss' | 'error',
    bytes: number,
    latencyMs: number
  ): void {
    const target = this.data.reads[kind]
    if (outcome === 'hit') target.hits += 1
    if (outcome === 'miss') target.misses += 1
    if (outcome === 'error') target.errors += 1
    target.bytes += bytes
    target.latencyMs += latencyMs
    this.schedulePersist()
  }

  write(
    kind: CacheKind,
    outcome: 'success' | 'conflict' | 'error',
    bytes: number,
    latencyMs: number
  ): void {
    const target = this.data.writes[kind]
    if (outcome === 'success') target.successes += 1
    if (outcome === 'conflict') target.conflicts += 1
    if (outcome === 'error') target.errors += 1
    target.bytes += bytes
    target.latencyMs += latencyMs
    this.schedulePersist()
  }

  integrityFailure(): void {
    this.data.integrityFailures += 1
    this.schedulePersist()
  }

  setCasWriteFailed(): void {
    this.data.casWriteFailed = true
    this.schedulePersist()
  }

  setWriteCircuitOpen(open: boolean): void {
    this.data.writeCircuitOpen = open
    this.schedulePersist()
  }

  setReadCircuitOpen(open: boolean): void {
    this.data.readCircuitOpen = open
    this.schedulePersist()
  }

  setInflightBytes(bytes: number): void {
    this.data.inflightBytes = bytes
    this.data.peakInflightBytes = Math.max(this.data.peakInflightBytes, bytes)
  }

  async flush(): Promise<void> {
    this.schedulePersist()
    await this.persistChain
    if (this.persistenceError !== undefined) throw this.persistenceError
  }

  private schedulePersist(): void {
    if (this.persist === undefined) return
    const snapshot = this.snapshot()
    // Keep the background chain fulfilled at all times so a transient control
    // file failure cannot become an unhandled rejection while traffic is idle.
    // flush() still surfaces the recorded failure during lifecycle shutdown.
    this.persistChain = this.persistChain.then(async () => {
      try {
        await this.persist?.(snapshot)
      } catch (error) {
        this.persistenceError = error
      }
    })
  }
}
