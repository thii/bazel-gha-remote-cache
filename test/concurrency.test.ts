import assert from 'node:assert/strict'
import {test} from 'node:test'
import {CapacityLimiter} from '../src/concurrency.js'

test('CapacityLimiter validates capacities and requested amounts', async () => {
  assert.throws(() => new CapacityLimiter(0), /positive safe integer/)
  assert.throws(() => new CapacityLimiter(1.5), /positive safe integer/)

  const limiter = new CapacityLimiter(3)
  await assert.rejects(
    limiter.acquire(-1),
    /requested capacity is out of range/
  )
  await assert.rejects(limiter.acquire(4), /requested capacity is out of range/)
  const release = await limiter.acquire(0)
  release()
  assert.equal(limiter.used, 0)
})

test('CapacityLimiter queues in FIFO order and releases capacity idempotently', async () => {
  const limiter = new CapacityLimiter(3)
  const releaseHeld = await limiter.acquire(2)
  const firstQueued = limiter.acquire(2)
  const secondQueued = limiter.acquire(1)
  let firstGranted = false
  let secondGranted = false
  void firstQueued.then(() => {
    firstGranted = true
  })
  void secondQueued.then(() => {
    secondGranted = true
  })
  await Promise.resolve()

  assert.equal(limiter.used, 2)
  assert.equal(limiter.queued, 2)
  assert.equal(firstGranted, false)
  assert.equal(secondGranted, false)

  releaseHeld()
  const [releaseFirst, releaseSecond] = await Promise.all([
    firstQueued,
    secondQueued
  ])
  assert.equal(limiter.used, 3)
  assert.equal(limiter.queued, 0)

  releaseFirst()
  releaseFirst()
  assert.equal(limiter.used, 1)
  releaseSecond()
  releaseSecond()
  assert.equal(limiter.used, 0)
})

test('CapacityLimiter rejects an already-aborted acquisition without queueing it', async () => {
  const limiter = new CapacityLimiter(1)
  const controller = new AbortController()
  controller.abort()

  await assert.rejects(
    limiter.acquire(1, controller.signal),
    /capacity wait aborted/
  )
  assert.equal(limiter.used, 0)
  assert.equal(limiter.queued, 0)
})

test('aborting a queued head waiter immediately grants an eligible follower', async () => {
  const limiter = new CapacityLimiter(3)
  const releaseHeld = await limiter.acquire(2)
  const controller = new AbortController()
  const blockedHead = limiter.acquire(2, controller.signal)
  const eligibleFollower = limiter.acquire(1)
  let followerRelease: (() => void) | undefined
  void eligibleFollower.then(release => {
    followerRelease = release
  })
  assert.equal(limiter.queued, 2)

  controller.abort()
  await assert.rejects(blockedHead, /capacity wait aborted/)
  await Promise.resolve()
  const grantedBeforeUnrelatedRelease = followerRelease !== undefined

  releaseHeld()
  const releaseFollower = await eligibleFollower
  releaseFollower()

  assert.equal(grantedBeforeUnrelatedRelease, true)
  assert.equal(limiter.used, 0)
  assert.equal(limiter.queued, 0)
})
