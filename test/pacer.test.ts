import assert from 'node:assert/strict'
import {test} from 'node:test'
import {EntryPacer, type EntryPacerSleep} from '../src/pacer.js'

interface Sleeper {
  deadline: number
  resolve: () => void
  reject: (error: Error) => void
  signal: AbortSignal
  onAbort: () => void
}

class ManualClock {
  nowValue = 0
  readonly sleepDurations: number[] = []
  private readonly sleepers: Sleeper[] = []

  readonly now = (): number => this.nowValue

  readonly sleep: EntryPacerSleep = (milliseconds, signal) => {
    this.sleepDurations.push(milliseconds)
    return new Promise<void>((resolve, reject) => {
      const sleeper: Sleeper = {
        deadline: this.nowValue + milliseconds,
        resolve,
        reject,
        signal,
        onAbort: () => {
          const index = this.sleepers.indexOf(sleeper)
          if (index !== -1) this.sleepers.splice(index, 1)
          const error = new Error('manual sleep aborted')
          error.name = 'AbortError'
          reject(error)
        }
      }
      if (signal.aborted) {
        sleeper.onAbort()
        return
      }
      signal.addEventListener('abort', sleeper.onAbort, {once: true})
      this.sleepers.push(sleeper)
    })
  }

  advance(milliseconds: number): void {
    this.nowValue += milliseconds
    const ready = this.sleepers.filter(
      sleeper => sleeper.deadline <= this.nowValue
    )
    for (const sleeper of ready) {
      const index = this.sleepers.indexOf(sleeper)
      if (index !== -1) this.sleepers.splice(index, 1)
      sleeper.signal.removeEventListener('abort', sleeper.onAbort)
      sleeper.resolve()
    }
  }
}

async function settle(): Promise<void> {
  for (let turn = 0; turn < 5; turn += 1) await Promise.resolve()
}

function createPacer(
  clock: ManualClock,
  overrides: Partial<{
    repositoryUploadBudget: number
    expectedWriters: number
    uploadBurst: number
    random: () => number
  }> = {}
): EntryPacer {
  return new EntryPacer({
    repositoryUploadBudget: 120,
    expectedWriters: 4,
    uploadBurst: 2,
    now: clock.now,
    sleep: clock.sleep,
    random: () => 0,
    ...overrides
  })
}

test('EntryPacer validates configuration and derives a per-writer rate', () => {
  assert.throws(
    () =>
      new EntryPacer({
        repositoryUploadBudget: 0,
        expectedWriters: 1,
        uploadBurst: 1
      }),
    /repository upload budget/
  )
  assert.throws(
    () =>
      new EntryPacer({
        repositoryUploadBudget: 120,
        expectedWriters: 1.5,
        uploadBurst: 1
      }),
    /expected writers/
  )
  assert.throws(
    () =>
      new EntryPacer({
        repositoryUploadBudget: 120,
        expectedWriters: 1,
        uploadBurst: 0
      }),
    /upload burst/
  )

  const clock = new ManualClock()
  const pacer = createPacer(clock)
  assert.deepEqual(pacer.snapshot, {
    repositoryUploadBudget: 120,
    expectedWriters: 4,
    uploadBurst: 2,
    configuredEntriesPerMinute: 30,
    currentEntriesPerMinute: 30,
    pauseUntil: null,
    totalSleepMs: 0,
    sleepCount: 0,
    rateLimitCount: 0,
    entriesGranted: 0,
    queued: 0,
    closed: false
  })
})

test('EntryPacer grants only the configured burst then paces FIFO entries evenly', async () => {
  const clock = new ManualClock()
  const pacer = createPacer(clock)
  const order: number[] = []

  await Promise.all([pacer.acquire(), pacer.acquire()])
  const third = pacer.acquire().then(() => order.push(3))
  const fourth = pacer.acquire().then(() => order.push(4))
  await settle()

  assert.equal(pacer.queued, 2)
  assert.deepEqual(clock.sleepDurations, [2000])
  clock.advance(1999)
  await settle()
  assert.deepEqual(order, [])

  clock.advance(1)
  await settle()
  assert.deepEqual(order, [3])
  assert.deepEqual(clock.sleepDurations, [2000, 2000])

  clock.advance(2000)
  await Promise.all([third, fourth])
  assert.deepEqual(order, [3, 4])
  assert.equal(pacer.snapshot.totalSleepMs, 4000)
})

test('idle time never accumulates more tokens than upload-burst', async () => {
  const clock = new ManualClock()
  const pacer = createPacer(clock)

  await Promise.all([pacer.acquire(), pacer.acquire()])
  clock.advance(60 * 60 * 1000)
  await Promise.all([pacer.acquire(), pacer.acquire()])

  let extraGranted = false
  const extra = pacer.acquire().then(() => {
    extraGranted = true
  })
  await settle()
  assert.equal(extraGranted, false)
  assert.deepEqual(clock.sleepDurations, [2000])

  clock.advance(2000)
  await extra
  assert.equal(extraGranted, true)
})

test('rate limits pause admission with deterministic jitter and adapt the rate', async () => {
  const clock = new ManualClock()
  const pacer = createPacer(clock, {
    repositoryUploadBudget: 120,
    expectedWriters: 2,
    uploadBurst: 1,
    random: () => 0.25
  })
  await pacer.acquire()

  const waiting = pacer.acquire()
  await settle()
  assert.deepEqual(clock.sleepDurations, [1000])

  pacer.recordRateLimit(5000, 1000)
  await settle()
  assert.deepEqual(clock.sleepDurations, [1000, 5250])
  assert.equal(pacer.currentEntriesPerMinute, 30)
  assert.equal(pacer.snapshot.pauseUntil, 5250)
  assert.equal(pacer.snapshot.rateLimitCount, 1)

  let granted = false
  void waiting.then(() => {
    granted = true
  })
  clock.advance(5249)
  await settle()
  assert.equal(granted, false)
  clock.advance(1)
  await waiting
  assert.equal(granted, true)
  assert.equal(pacer.snapshot.pauseUntil, null)
  assert.equal(pacer.snapshot.totalSleepMs, 5250)

  pacer.recordCleanMinute()
  assert.equal(pacer.currentEntriesPerMinute, 36)
  for (let minute = 0; minute < 20; minute += 1) {
    pacer.recordCleanMinute()
  }
  assert.equal(pacer.currentEntriesPerMinute, 60)
})

test('pauseFor only extends a pause and validates caller-supplied jitter', () => {
  const clock = new ManualClock()
  const pacer = createPacer(clock, {random: () => 0.75})

  assert.equal(pacer.pauseFor(5000, 1000), 5750)
  assert.equal(pacer.pauseFor(1000), 5750)
  assert.equal(pacer.snapshot.pauseUntil, 5750)
  assert.throws(() => pacer.pauseFor(-1), /pause duration/)

  const invalidRandom = createPacer(clock, {random: () => 1})
  assert.throws(() => invalidRandom.pauseFor(1, 1), /random source/)
})

test('waitForResume waits only for the active pause and consumes no token', async () => {
  const clock = new ManualClock()
  const pacer = createPacer(clock, {uploadBurst: 1})

  await pacer.waitForResume()
  assert.deepEqual(clock.sleepDurations, [])
  pacer.pauseFor(5000)

  let resumed = false
  const resume = pacer.waitForResume().then(() => {
    resumed = true
  })
  await settle()
  assert.equal(pacer.queued, 0)
  assert.deepEqual(clock.sleepDurations, [5000])

  clock.advance(4999)
  await settle()
  assert.equal(resumed, false)
  clock.advance(1)
  await resume
  assert.equal(resumed, true)
  assert.equal(pacer.snapshot.totalSleepMs, 5000)
  assert.equal(pacer.snapshot.sleepCount, 1)

  await pacer.acquire()
  assert.equal(pacer.snapshot.entriesGranted, 1)
})

test('waitForResume follows a pause extension and accounts both sleeps', async () => {
  const clock = new ManualClock()
  const pacer = createPacer(clock)
  pacer.pauseFor(5000)
  const resume = pacer.waitForResume()
  await settle()

  clock.advance(2000)
  pacer.pauseFor(5000)
  clock.advance(3000)
  await settle()
  assert.deepEqual(clock.sleepDurations, [5000, 2000])

  let resumed = false
  void resume.then(() => {
    resumed = true
  })
  clock.advance(1999)
  await settle()
  assert.equal(resumed, false)
  clock.advance(1)
  await resume
  assert.equal(resumed, true)
  assert.equal(pacer.snapshot.totalSleepMs, 7000)
  assert.equal(pacer.snapshot.sleepCount, 2)
})

test('waitForResume is abort-aware and records partial sleep time', async () => {
  const clock = new ManualClock()
  const pacer = createPacer(clock)
  const controller = new AbortController()
  pacer.pauseFor(5000)
  const resume = pacer.waitForResume(controller.signal)
  const rejected = assert.rejects(resume, {
    name: 'AbortError',
    message: 'entry pacer resume wait aborted'
  })

  clock.advance(1200)
  controller.abort()
  await rejected
  assert.equal(pacer.snapshot.totalSleepMs, 1200)
  assert.equal(pacer.snapshot.sleepCount, 1)
})

test('shutdown rejects a waitForResume sleep', async () => {
  const clock = new ManualClock()
  const pacer = createPacer(clock)
  pacer.pauseFor(5000)
  const resume = pacer.waitForResume()
  const rejected = assert.rejects(resume, /entry pacer is shut down/)

  clock.advance(400)
  pacer.shutdown()
  await rejected
  assert.equal(pacer.snapshot.totalSleepMs, 400)
  assert.equal(pacer.closed, true)
})

test('aborting a FIFO waiter cannot strand its follower', async () => {
  const clock = new ManualClock()
  const pacer = createPacer(clock, {uploadBurst: 1})
  await pacer.acquire()

  const controller = new AbortController()
  const aborted = pacer.acquire(controller.signal)
  const follower = pacer.acquire()
  await settle()
  assert.equal(pacer.queued, 2)

  const rejected = assert.rejects(aborted, {
    name: 'AbortError',
    message: 'entry pacer wait aborted'
  })
  controller.abort()
  await rejected
  await settle()
  assert.equal(pacer.queued, 1)

  clock.advance(2000)
  await follower
  assert.equal(pacer.queued, 0)
})

test('shutdown rejects queued and future waiters without leaving a sleep active', async () => {
  const clock = new ManualClock()
  const pacer = createPacer(clock, {uploadBurst: 1})
  await pacer.acquire()

  const queued = pacer.acquire()
  await settle()
  const queuedRejection = assert.rejects(queued, /entry pacer is shut down/)
  pacer.shutdown()
  await queuedRejection
  await assert.rejects(pacer.acquire(), /entry pacer is shut down/)
  assert.equal(pacer.queued, 0)
  assert.equal(pacer.closed, true)
  pacer.shutdown()
})

test('an already-aborted acquisition is rejected without queueing', async () => {
  const clock = new ManualClock()
  const pacer = createPacer(clock)
  const controller = new AbortController()
  controller.abort()

  await assert.rejects(pacer.acquire(controller.signal), {
    name: 'AbortError',
    message: 'entry pacer wait aborted'
  })
  assert.equal(pacer.queued, 0)
})
