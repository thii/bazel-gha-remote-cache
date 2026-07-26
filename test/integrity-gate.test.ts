import assert from 'node:assert/strict'
import {test} from 'node:test'
import {CasIntegrityError, CasIntegrityGate} from '../src/server.js'

class Deferred<T> {
  readonly promise: Promise<T>
  resolve!: (value: T) => void

  constructor() {
    this.promise = new Promise(resolve => {
      this.resolve = resolve
    })
  }
}

test('a CAS abort queued behind AC finalization permanently fails the integrity gate', async () => {
  const gate = new CasIntegrityGate()
  const acStarted = new Deferred<void>()
  const releaseAc = new Deferred<void>()
  const firstFinalization = gate.finalizeAction(async () => {
    acStarted.resolve()
    await releaseAc.promise
    return 'finalized'
  })
  await acStarted.promise

  const controller = new AbortController()
  const queuedCas = gate.beginCas(controller.signal)
  controller.abort()
  await assert.rejects(queuedCas, /request aborted/)
  assert.equal(gate.healthy, false)

  releaseAc.resolve()
  assert.equal(await firstFinalization, 'finalized')

  let laterFinalizationCalled = false
  await assert.rejects(
    gate.finalizeAction(async () => {
      laterFinalizationCalled = true
    }),
    CasIntegrityError
  )
  assert.equal(laterFinalizationCalled, false)
  assert.throws(() => gate.assertHealthy(), CasIntegrityError)
})
