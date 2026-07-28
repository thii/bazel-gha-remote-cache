import assert from 'node:assert/strict'
import path from 'node:path'
import {test} from 'node:test'
import {isSafeControlDirectory} from '../src/control.js'
import {Metrics} from '../src/metrics.js'
import {
  metricsHaveCacheErrors,
  safeErrorMessage,
  validateDaemonConfig,
  validateDaemonReady,
  validateMetrics
} from '../src/lifecycle.js'

const validConfig = {
  namespace: 'bazel-v1',
  storageMode: 'pack',
  port: 0,
  readable: true,
  writable: false,
  maxObjectSize: 100,
  maxInflightBytes: 200,
  maxPendingBytes: 1000,
  uploadConcurrency: 4,
  downloadConcurrency: 16,
  repositoryUploadBudget: 120,
  expectedWriters: 1,
  uploadBurst: 2,
  writeBack: true,
  flushTimeoutSeconds: 120,
  packTargetBytes: 500,
  packMaxObjects: 256,
  packMaxAgeSeconds: 8,
  catalogRefreshSeconds: 15,
  remoteTimeoutSeconds: 30,
  failJobOnCacheError: false,
  githubApiUrl: 'https://api.github.com',
  githubRepository: 'owner/repository',
  currentRef: 'refs/heads/topic',
  baseRef: 'refs/heads/release/v1',
  defaultRef: 'refs/heads/main',
  runId: '123456789',
  jobHash: '1'.repeat(16),
  controlDirectory: path.resolve('/runner/temp/bazel-gha-cache-abc'),
  shutdownToken: 'a'.repeat(32),
  instanceId: '12345678-1234-1234-1234-123456789abc'
}

test('validateDaemonConfig accepts and returns a complete valid control object', () => {
  assert.deepEqual(validateDaemonConfig(validConfig), validConfig)
})

test('validateDaemonConfig rejects malformed security and resource fields', () => {
  const cases: Array<{value: unknown; error: RegExp}> = [
    {value: null, error: /control data must be a JSON object/},
    {value: [], error: /control data must be a JSON object/},
    {
      value: {...validConfig, namespace: '../shared'},
      error: /invalid namespace/
    },
    {
      value: {...validConfig, readable: 'true'},
      error: /invalid readable/
    },
    {
      value: {...validConfig, maxInflightBytes: 99},
      error: /invalid maxInflightBytes/
    },
    {
      value: {...validConfig, storageMode: 'archive'},
      error: /invalid storageMode/
    },
    {
      value: {...validConfig, writeBack: false},
      error: /packed storage requires write-back/
    },
    {
      value: {...validConfig, githubRepository: 'owner/repo/extra'},
      error: /invalid GitHub repository/
    },
    {
      value: {...validConfig, baseRef: 'refs/tags/release-v1'},
      error: /invalid Git ref/
    },
    {
      value: {...validConfig, runId: 'local'},
      error: /invalid run ID/
    },
    {
      value: {...validConfig, uploadConcurrency: 0},
      error: /invalid uploadConcurrency/
    },
    {
      value: {...validConfig, packMaxObjects: 257},
      error: /invalid packMaxObjects/
    },
    {
      value: {...validConfig, controlDirectory: 'relative/path'},
      error: /control directory must be absolute/
    },
    {
      value: {...validConfig, shutdownToken: 'too-short'},
      error: /invalid shutdown token/
    },
    {
      value: {...validConfig, instanceId: 'not-a-uuid'},
      error: /invalid instance ID/
    }
  ]

  for (const {value, error} of cases) {
    assert.throws(() => validateDaemonConfig(value), error)
  }
})

test('validateDaemonReady accepts only a loopback HTTP root URL', () => {
  const ready = {
    pid: 123,
    port: 4567,
    url: 'http://127.0.0.1:4567/',
    readable: true,
    writable: false,
    instanceId: validConfig.instanceId,
    startedAt: '2026-07-26T12:00:00.000Z'
  }
  assert.deepEqual(validateDaemonReady(ready), {
    ...ready,
    url: 'http://127.0.0.1:4567'
  })

  for (const url of [
    'https://127.0.0.1:4567/',
    'http://localhost:4567/',
    'http://127.0.0.1:4567/cache',
    'http://127.0.0.1:4567/?token=secret',
    'http://127.0.0.1:4567/#fragment'
  ]) {
    assert.throws(
      () => validateDaemonReady({...ready, url}),
      /daemon reported a non-loopback URL/
    )
  }
  assert.throws(() => validateDaemonReady({...ready, pid: 0}), /invalid pid/)
  assert.throws(
    () => validateDaemonReady({...ready, port: 65536}),
    /invalid port/
  )
})

test('validateMetrics accepts a real Metrics snapshot', () => {
  const metrics = new Metrics(true, true)
  metrics.request('get')
  metrics.backend('lookups')
  metrics.read('cas', 'hit', 17, 3)
  metrics.write('ac', 'conflict', 9, 2)
  metrics.integrityFailure()
  metrics.setCasWriteFailed()
  metrics.setWriteCircuitOpen(true)
  metrics.setReadCircuitOpen(true)
  metrics.setInflightBytes(23)
  metrics.stop()
  const snapshot = metrics.snapshot()

  assert.deepEqual(validateMetrics(snapshot), snapshot)
})

test('validateMetrics rejects missing or malformed nested control data', () => {
  const snapshot = new Metrics(true, false).snapshot()
  const missingRequest = structuredClone(snapshot.requests) as Partial<
    typeof snapshot.requests
  >
  delete missingRequest.get
  const missingReadKind = structuredClone(snapshot.reads) as Partial<
    typeof snapshot.reads
  >
  delete missingReadKind.cas
  const missingBackendCounter = structuredClone(snapshot.backend) as Partial<
    typeof snapshot.backend
  >
  delete missingBackendCounter.downloads

  const cases: Array<{label: string; value: unknown}> = [
    {label: 'missing top-level fields', value: {schemaVersion: 1}},
    {
      label: 'invalid start timestamp',
      value: {...snapshot, startedAt: 'never'}
    },
    {
      label: 'missing request counter',
      value: {...snapshot, requests: missingRequest}
    },
    {label: 'missing read kind', value: {...snapshot, reads: missingReadKind}},
    {
      label: 'negative nested counter',
      value: {
        ...snapshot,
        writes: {
          ...snapshot.writes,
          ac: {...snapshot.writes.ac, errors: -1}
        }
      }
    },
    {
      label: 'fractional nested counter',
      value: {
        ...snapshot,
        reads: {
          ...snapshot.reads,
          cas: {...snapshot.reads.cas, latencyMs: 1.5}
        }
      }
    },
    {
      label: 'missing backend counter',
      value: {...snapshot, backend: missingBackendCounter}
    },
    {
      label: 'invalid top-level boolean',
      value: {...snapshot, casWriteFailed: 'false'}
    },
    {
      label: 'invalid diagnostic journal status',
      value: {...snapshot, diagnosticJournalFailed: 'false'}
    },
    {
      label: 'invalid optional stop timestamp',
      value: {...snapshot, stoppedAt: 123}
    }
  ]

  for (const {label, value} of cases) {
    assert.throws(() => validateMetrics(value), label)
  }

  assert.throws(
    () => validateMetrics({schemaVersion: 2}),
    /control data must be a JSON object/
  )
  assert.throws(() => validateMetrics([]), /control data must be a JSON object/)
})

test('metricsHaveCacheErrors enforces strict post-step health', () => {
  const clean = new Metrics(true, true).snapshot()
  clean.stoppedAt = new Date().toISOString()
  assert.equal(metricsHaveCacheErrors(clean), false)

  for (const mutate of [
    (stats: typeof clean) => {
      delete stats.stoppedAt
    },
    (stats: typeof clean) => {
      stats.backend.errors = 1
    },
    (stats: typeof clean) => {
      stats.backend.rateLimited = 1
    },
    (stats: typeof clean) => {
      stats.diagnosticJournalFailed = true
    },
    (stats: typeof clean) => {
      stats.reads.ac.errors = 1
    },
    (stats: typeof clean) => {
      stats.writes.cas.errors = 1
    },
    (stats: typeof clean) => {
      stats.casWriteFailed = true
    },
    (stats: typeof clean) => {
      stats.readCircuitOpen = true
    }
  ]) {
    const stats = structuredClone(clean)
    mutate(stats)
    assert.equal(metricsHaveCacheErrors(stats), true)
  }
})

test('isSafeControlDirectory permits only prefixed descendants of RUNNER_TEMP', () => {
  const runnerTemp = path.resolve('/runner/temp')
  assert.equal(
    isSafeControlDirectory(
      path.join(runnerTemp, 'bazel-gha-cache-random'),
      runnerTemp
    ),
    true
  )
  assert.equal(isSafeControlDirectory(runnerTemp, runnerTemp), false)
  assert.equal(
    isSafeControlDirectory(path.join(runnerTemp, 'unrelated'), runnerTemp),
    false
  )
  assert.equal(
    isSafeControlDirectory(
      path.resolve(runnerTemp, '..', 'bazel-gha-cache-random'),
      runnerTemp
    ),
    false
  )
  assert.equal(isSafeControlDirectory('', runnerTemp), false)
  assert.equal(isSafeControlDirectory('/bazel-gha-cache-random', ''), false)
})

test('safeErrorMessage redacts signed URLs and authorization material', () => {
  assert.equal(
    safeErrorMessage(
      new Error(
        'GET https://blob.example.test/object?sig=url-secret failed; Bearer token-secret; signature=body-secret'
      )
    ),
    'GET <redacted-url> failed; Bearer <redacted> sig=<redacted>'
  )
  assert.equal(safeErrorMessage('plain failure'), 'plain failure')
  assert.equal(safeErrorMessage('x'.repeat(600)).length, 500)
})
