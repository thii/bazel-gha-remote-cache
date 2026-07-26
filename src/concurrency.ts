interface Waiter {
  amount: number
  resolve: (release: () => void) => void
  reject: (error: Error) => void
  signal?: AbortSignal
  onAbort?: () => void
}

export class CapacityLimiter {
  readonly capacity: number
  private usedValue = 0
  private readonly waiters: Waiter[] = []

  constructor(capacity: number) {
    if (!Number.isSafeInteger(capacity) || capacity < 1) {
      throw new Error('capacity must be a positive safe integer')
    }
    this.capacity = capacity
  }

  get used(): number {
    return this.usedValue
  }

  get queued(): number {
    return this.waiters.length
  }

  acquire(amount = 1, signal?: AbortSignal): Promise<() => void> {
    if (!Number.isSafeInteger(amount) || amount < 0 || amount > this.capacity) {
      return Promise.reject(new Error('requested capacity is out of range'))
    }
    if (signal?.aborted) {
      return Promise.reject(new Error('capacity wait aborted'))
    }
    if (amount === 0) return Promise.resolve(() => {})

    if (this.waiters.length === 0 && this.usedValue + amount <= this.capacity) {
      this.usedValue += amount
      return Promise.resolve(this.releaseFunction(amount))
    }

    return new Promise<() => void>((resolve, reject) => {
      const waiter: Waiter = {amount, resolve, reject}
      if (signal !== undefined) {
        waiter.signal = signal
        waiter.onAbort = () => {
          const index = this.waiters.indexOf(waiter)
          if (index !== -1) this.waiters.splice(index, 1)
          reject(new Error('capacity wait aborted'))
          this.drain()
        }
        signal.addEventListener('abort', waiter.onAbort, {once: true})
      }
      this.waiters.push(waiter)
      this.drain()
    })
  }

  private releaseFunction(amount: number): () => void {
    let released = false
    return () => {
      if (released) return
      released = true
      this.usedValue -= amount
      this.drain()
    }
  }

  private drain(): void {
    while (this.waiters.length > 0) {
      const waiter = this.waiters[0]
      if (waiter === undefined) return
      if (this.usedValue + waiter.amount > this.capacity) return
      this.waiters.shift()
      if (waiter.signal !== undefined && waiter.onAbort !== undefined) {
        waiter.signal.removeEventListener('abort', waiter.onAbort)
      }
      this.usedValue += waiter.amount
      waiter.resolve(this.releaseFunction(waiter.amount))
    }
  }
}
