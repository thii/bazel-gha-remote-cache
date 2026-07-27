import assert from 'node:assert/strict'
import {mkdtemp, rm, writeFile} from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import {test} from 'node:test'
import {
  loadEventContext,
  parseInputs,
  resolvePermissions,
  validateCacheEnvironment,
  type InputReader
} from '../src/config.js'

function inputReader(values: Record<string, string> = {}): InputReader {
  return name => values[name] ?? ''
}

test('parseInputs applies the documented defaults', () => {
  assert.deepEqual(parseInputs(inputReader()), {
    namespace: 'bazel-v1',
    mode: 'auto',
    storageMode: 'pack',
    githubToken: '',
    port: 0,
    maxObjectSize: 2_147_483_648,
    maxInflightBytes: 4_294_967_296,
    maxPendingBytes: 4_294_967_296,
    uploadConcurrency: 4,
    downloadConcurrency: 16,
    repositoryUploadBudget: 120,
    expectedWriters: 1,
    uploadBurst: 2,
    writeBack: true,
    flushTimeoutSeconds: 120,
    packTargetBytes: 67_108_864,
    packMaxObjects: 256,
    packMaxAgeSeconds: 8,
    catalogRefreshSeconds: 300,
    remoteTimeoutSeconds: 30,
    failJobOnCacheError: false
  })
})

test('parseInputs accepts valid explicit values and normalizes enum inputs', () => {
  assert.deepEqual(
    parseInputs(
      inputReader({
        namespace: 'linux_x86-64.v2',
        mode: 'READ-WRITE',
        'storage-mode': 'OBJECT',
        'github-token': 'catalog-token',
        port: '65535',
        'max-object-size': '1024',
        'max-inflight-bytes': '2048',
        'max-pending-bytes': '4096',
        'upload-concurrency': '128',
        'download-concurrency': '256',
        'repository-upload-budget': '180',
        'expected-writers': '6',
        'upload-burst': '3',
        'write-back': 'FALSE',
        'flush-timeout-seconds': '600',
        'pack-target-bytes': '3072',
        'pack-max-objects': '256',
        'pack-max-age-seconds': '10',
        'catalog-refresh-seconds': '20',
        'remote-timeout-seconds': '3600',
        'fail-job-on-cache-error': 'TRUE'
      })
    ),
    {
      namespace: 'linux_x86-64.v2',
      mode: 'read-write',
      storageMode: 'object',
      githubToken: 'catalog-token',
      port: 65535,
      maxObjectSize: 1024,
      maxInflightBytes: 2048,
      maxPendingBytes: 4096,
      uploadConcurrency: 128,
      downloadConcurrency: 256,
      repositoryUploadBudget: 180,
      expectedWriters: 6,
      uploadBurst: 3,
      writeBack: false,
      flushTimeoutSeconds: 600,
      packTargetBytes: 3072,
      packMaxObjects: 256,
      packMaxAgeSeconds: 10,
      catalogRefreshSeconds: 20,
      remoteTimeoutSeconds: 3600,
      failJobOnCacheError: true
    }
  )
})

test('parseInputs rejects malformed and out-of-policy inputs', () => {
  const cases: Array<{
    label: string
    values: Record<string, string>
    error: RegExp
  }> = [
    {
      label: 'namespace syntax',
      values: {namespace: '.private'},
      error: /namespace must be 1-128 characters/
    },
    {
      label: 'mode',
      values: {mode: 'write'},
      error: /mode must be auto, read-only, or read-write/
    },
    {
      label: 'storage mode',
      values: {'storage-mode': 'archive'},
      error: /storage-mode must be object or pack/
    },
    {
      label: 'pack requires write-back',
      values: {'write-back': 'false'},
      error: /write-back must be true when storage-mode is pack/
    },
    {
      label: 'non-canonical integer',
      values: {port: '01'},
      error: /port must be an unsigned base-10 integer/
    },
    {
      label: 'port range',
      values: {port: '65536'},
      error: /port must be between 0 and 65535/
    },
    {
      label: 'object size minimum',
      values: {'max-object-size': '0'},
      error: /max-object-size must be between 1/
    },
    {
      label: 'aggregate inflight bound',
      values: {
        'max-object-size': '10',
        'max-inflight-bytes': '9'
      },
      error: /max-inflight-bytes must be at least max-object-size/
    },
    {
      label: 'pending bound',
      values: {
        'max-object-size': '10',
        'max-pending-bytes': '9'
      },
      error: /max-pending-bytes must be at least max-object-size/
    },
    {
      label: 'pack target bound',
      values: {
        'max-object-size': '1',
        'max-pending-bytes': '10',
        'pack-target-bytes': '11'
      },
      error: /pack-target-bytes must not exceed max-pending-bytes/
    },
    {
      label: 'upload concurrency',
      values: {'upload-concurrency': '129'},
      error: /upload-concurrency must be between 1 and 128/
    },
    {
      label: 'pack object bound',
      values: {'pack-max-objects': '257'},
      error: /pack-max-objects must be between 1 and 256/
    },
    {
      label: 'boolean syntax',
      values: {'fail-job-on-cache-error': 'yes'},
      error: /fail-job-on-cache-error must be true or false/
    }
  ]

  for (const {label, values, error} of cases) {
    assert.throws(() => parseInputs(inputReader(values)), error, label)
  }
})

test('resolvePermissions writes only when both event and runtime policy allow it', () => {
  const defaultBranchPush = {
    eventName: 'push',
    ref: 'refs/heads/main',
    defaultBranch: 'main',
    refProtected: true
  }

  assert.deepEqual(resolvePermissions('auto', defaultBranchPush, 'write'), {
    readable: true,
    writable: true,
    reason: 'requested mode and runner permissions'
  })
  assert.deepEqual(
    resolvePermissions('auto', {...defaultBranchPush, ref: 'refs/heads/topic'}),
    {
      readable: true,
      writable: false,
      reason:
        'auto mode writes only on a protected repository default-branch push'
    }
  )
  assert.deepEqual(
    resolvePermissions('read-write', defaultBranchPush, 'read'),
    {
      readable: true,
      writable: false,
      reason: 'runner cache permissions do not allow writes'
    }
  )
  assert.deepEqual(
    resolvePermissions('read-write', defaultBranchPush, 'write-only'),
    {
      readable: false,
      writable: true,
      reason: 'requested mode and runner permissions'
    }
  )
  assert.equal(
    resolvePermissions('read-only', defaultBranchPush).writable,
    false
  )
})

test('resolvePermissions forcibly disables writes for both pull request event types', () => {
  for (const eventName of ['pull_request', 'pull_request_target']) {
    assert.deepEqual(
      resolvePermissions(
        'read-write',
        {
          eventName,
          ref: 'refs/pull/7/merge',
          defaultBranch: 'main',
          refProtected: false
        },
        'write'
      ),
      {
        readable: true,
        writable: false,
        reason: 'pull request events are forcibly read-only'
      }
    )
  }
})

test('loadEventContext reads default and PR base branches and fails closed on malformed event data', async t => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'brc-config-test-'))
  t.after(() => rm(directory, {recursive: true, force: true}))
  const eventPath = path.join(directory, 'event.json')
  await writeFile(
    eventPath,
    JSON.stringify({
      repository: {default_branch: 'trunk'},
      pull_request: {base: {ref: 'release/v1'}}
    }),
    'utf8'
  )

  assert.deepEqual(
    await loadEventContext({
      GITHUB_EVENT_PATH: eventPath,
      GITHUB_EVENT_NAME: 'pull_request',
      GITHUB_REF: 'refs/pull/7/merge',
      GITHUB_BASE_REF: 'environment-fallback',
      GITHUB_REF_PROTECTED: 'true'
    }),
    {
      eventName: 'pull_request',
      ref: 'refs/pull/7/merge',
      refProtected: true,
      baseBranch: 'release/v1',
      defaultBranch: 'trunk'
    }
  )

  await writeFile(eventPath, '{not json', 'utf8')
  assert.deepEqual(
    await loadEventContext({
      GITHUB_EVENT_PATH: eventPath,
      GITHUB_EVENT_NAME: 'pull_request',
      GITHUB_REF: 'refs/pull/7/merge',
      GITHUB_BASE_REF: 'release/fallback'
    }),
    {
      eventName: 'pull_request',
      ref: 'refs/pull/7/merge',
      baseBranch: 'release/fallback',
      refProtected: false
    }
  )
})

test('validateCacheEnvironment accepts Cache v2 on supported hosts', () => {
  const base = {
    ACTIONS_CACHE_SERVICE_V2: 'true',
    ACTIONS_RESULTS_URL: 'https://results.example.test/base',
    ACTIONS_RUNTIME_TOKEN: 'runtime-token'
  }

  assert.deepEqual(validateCacheEnvironment(base), {
    resultsUrl: base.ACTIONS_RESULTS_URL,
    runtimeToken: base.ACTIONS_RUNTIME_TOKEN
  })
  assert.doesNotThrow(() =>
    validateCacheEnvironment({
      ...base,
      GITHUB_SERVER_URL: 'https://acme.ghe.com'
    })
  )
  assert.doesNotThrow(() =>
    validateCacheEnvironment({...base, GITHUB_SERVER_URL: 'http://localhost'})
  )
})

test('validateCacheEnvironment rejects unavailable or unsupported Cache v2 endpoints', () => {
  const base = {
    ACTIONS_CACHE_SERVICE_V2: 'true',
    ACTIONS_RESULTS_URL: 'https://results.example.test/base',
    ACTIONS_RUNTIME_TOKEN: 'runtime-token'
  }

  assert.throws(
    () => validateCacheEnvironment({...base, ACTIONS_CACHE_SERVICE_V2: '0'}),
    /ACTIONS_CACHE_SERVICE_V2 is not enabled/
  )
  assert.throws(
    () =>
      validateCacheEnvironment({
        ...base,
        ACTIONS_RESULTS_URL: 'file:///tmp/cache'
      }),
    /ACTIONS_RESULTS_URL must use HTTP or HTTPS/
  )
  assert.throws(
    () =>
      validateCacheEnvironment({
        ...base,
        GITHUB_SERVER_URL: 'https://github.enterprise.example'
      }),
    /not supported on GitHub Enterprise Server/
  )
})
