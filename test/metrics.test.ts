import assert from 'node:assert/strict'
import {test} from 'node:test'
import {Metrics} from '../src/metrics.js'

class Deferred<T> {
  readonly promise: Promise<T>
  resolve!: (value: T) => void

  constructor() {
    this.promise = new Promise(resolve => {
      this.resolve = resolve
    })
  }
}

test('a background persist rejection is handled and flush reports it deterministically', async t => {
  const persistenceError = new Error('control file persistence failed')
  const firstAttempt = new Deferred<void>()
  const unhandled: unknown[] = []
  let attempts = 0
  const onUnhandled = (reason: unknown): void => {
    unhandled.push(reason)
  }
  process.on('unhandledRejection', onUnhandled)
  t.after(() => process.off('unhandledRejection', onUnhandled))

  const metrics = new Metrics(true, true, async () => {
    attempts += 1
    if (attempts === 1) {
      firstAttempt.resolve()
      throw persistenceError
    }
  })

  metrics.read('cas', 'hit', 12, 3)
  await firstAttempt.promise
  await new Promise(resolve => setImmediate(resolve))
  assert.deepEqual(unhandled, [])
  assert.equal(attempts, 1)

  for (let attempt = 2; attempt <= 3; attempt += 1) {
    await assert.rejects(
      metrics.flush(),
      error => error === persistenceError,
      `flush attempt ${attempt - 1}`
    )
    assert.equal(attempts, attempt)
  }

  await new Promise(resolve => setImmediate(resolve))
  assert.deepEqual(unhandled, [])
})
