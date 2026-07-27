import {readFile} from 'node:fs/promises'
import {URL} from 'node:url'
import type {EventContext, RequestedMode, StorageMode} from './model.js'

export interface ParsedInputs {
  namespace: string
  mode: RequestedMode
  storageMode: StorageMode
  githubToken: string
  port: number
  maxObjectSize: number
  maxInflightBytes: number
  maxPendingBytes: number
  uploadConcurrency: number
  downloadConcurrency: number
  repositoryUploadBudget: number
  expectedWriters: number
  uploadBurst: number
  writeBack: boolean
  flushTimeoutSeconds: number
  packTargetBytes: number
  packMaxObjects: number
  packMaxAgeSeconds: number
  catalogRefreshSeconds: number
  remoteTimeoutSeconds: number
  failJobOnCacheError: boolean
}

export interface EffectivePermissions {
  readable: boolean
  writable: boolean
  reason: string
}

export type InputReader = (name: string) => string

const NAMESPACE_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/
const UNSIGNED_INTEGER_PATTERN = /^(0|[1-9][0-9]*)$/

function integerInput(
  reader: InputReader,
  name: string,
  fallback: string,
  minimum: number,
  maximum: number
): number {
  const raw = reader(name) || fallback
  if (!UNSIGNED_INTEGER_PATTERN.test(raw)) {
    throw new Error(`${name} must be an unsigned base-10 integer`)
  }

  const value = Number(raw)
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    throw new Error(`${name} must be between ${minimum} and ${maximum}`)
  }
  return value
}

function booleanInput(
  reader: InputReader,
  name: string,
  fallback: string
): boolean {
  const raw = (reader(name) || fallback).toLowerCase()
  if (raw === 'true') return true
  if (raw === 'false') return false
  throw new Error(`${name} must be true or false`)
}

export function parseInputs(reader: InputReader): ParsedInputs {
  const namespace = reader('namespace') || 'bazel-v1'
  if (!NAMESPACE_PATTERN.test(namespace)) {
    throw new Error(
      'namespace must be 1-128 characters using letters, digits, dot, underscore, or hyphen'
    )
  }

  const mode = (reader('mode') || 'auto').toLowerCase()
  if (mode !== 'auto' && mode !== 'read-only' && mode !== 'read-write') {
    throw new Error('mode must be auto, read-only, or read-write')
  }

  const storageMode = (reader('storage-mode') || 'pack').toLowerCase()
  if (storageMode !== 'object' && storageMode !== 'pack') {
    throw new Error('storage-mode must be object or pack')
  }
  const writeBack = booleanInput(reader, 'write-back', 'true')
  if (storageMode === 'pack' && !writeBack) {
    throw new Error('write-back must be true when storage-mode is pack')
  }

  const maxObjectSize = integerInput(
    reader,
    'max-object-size',
    '2147483648',
    1,
    Number.MAX_SAFE_INTEGER
  )
  const maxInflightBytes = integerInput(
    reader,
    'max-inflight-bytes',
    '4294967296',
    1,
    Number.MAX_SAFE_INTEGER
  )
  if (maxInflightBytes < maxObjectSize) {
    throw new Error('max-inflight-bytes must be at least max-object-size')
  }
  const maxPendingBytes = integerInput(
    reader,
    'max-pending-bytes',
    '4294967296',
    1,
    Number.MAX_SAFE_INTEGER
  )
  if (maxPendingBytes < maxObjectSize) {
    throw new Error('max-pending-bytes must be at least max-object-size')
  }
  const packTargetBytes = integerInput(
    reader,
    'pack-target-bytes',
    '67108864',
    1,
    Number.MAX_SAFE_INTEGER
  )
  if (packTargetBytes > maxPendingBytes) {
    throw new Error('pack-target-bytes must not exceed max-pending-bytes')
  }

  return {
    namespace,
    mode,
    storageMode,
    githubToken: reader('github-token'),
    port: integerInput(reader, 'port', '0', 0, 65535),
    maxObjectSize,
    maxInflightBytes,
    maxPendingBytes,
    uploadConcurrency: integerInput(reader, 'upload-concurrency', '4', 1, 128),
    downloadConcurrency: integerInput(
      reader,
      'download-concurrency',
      '16',
      1,
      256
    ),
    repositoryUploadBudget: integerInput(
      reader,
      'repository-upload-budget',
      '120',
      1,
      200
    ),
    expectedWriters: integerInput(reader, 'expected-writers', '1', 1, 256),
    uploadBurst: integerInput(reader, 'upload-burst', '2', 1, 200),
    writeBack,
    flushTimeoutSeconds: integerInput(
      reader,
      'flush-timeout-seconds',
      '120',
      1,
      3600
    ),
    packTargetBytes,
    packMaxObjects: integerInput(reader, 'pack-max-objects', '256', 1, 256),
    packMaxAgeSeconds: integerInput(
      reader,
      'pack-max-age-seconds',
      '8',
      1,
      300
    ),
    catalogRefreshSeconds: integerInput(
      reader,
      'catalog-refresh-seconds',
      '300',
      1,
      3600
    ),
    remoteTimeoutSeconds: integerInput(
      reader,
      'remote-timeout-seconds',
      '30',
      1,
      3600
    ),
    failJobOnCacheError: booleanInput(
      reader,
      'fail-job-on-cache-error',
      'false'
    )
  }
}

function runtimePermissions(mode: string | undefined): {
  readable: boolean
  writable: boolean
} {
  switch ((mode ?? '').trim().toLowerCase()) {
    case 'none':
      return {readable: false, writable: false}
    case 'read':
      return {readable: true, writable: false}
    case 'write':
      return {readable: true, writable: true}
    case 'write-only':
      return {readable: false, writable: true}
    default:
      return {readable: true, writable: true}
  }
}

function isPullRequestLike(eventName: string): boolean {
  return eventName === 'pull_request' || eventName === 'pull_request_target'
}

function isTrustedDefaultBranchPush(context: EventContext): boolean {
  return (
    context.eventName === 'push' &&
    context.refProtected &&
    context.defaultBranch !== undefined &&
    context.ref === `refs/heads/${context.defaultBranch}`
  )
}

export function resolvePermissions(
  requestedMode: RequestedMode,
  context: EventContext,
  actionsCacheMode?: string
): EffectivePermissions {
  const runtime = runtimePermissions(actionsCacheMode)
  const pullRequest = isPullRequestLike(context.eventName)
  const requestedWritable =
    requestedMode === 'read-write' ||
    (requestedMode === 'auto' && isTrustedDefaultBranchPush(context))

  const writable = runtime.writable && requestedWritable && !pullRequest
  const readable = runtime.readable

  let reason = 'requested mode and runner permissions'
  if (pullRequest && requestedWritable) {
    reason = 'pull request events are forcibly read-only'
  } else if (requestedMode === 'auto' && !requestedWritable) {
    reason =
      'auto mode writes only on a protected repository default-branch push'
  } else if (!runtime.writable && requestedWritable) {
    reason = 'runner cache permissions do not allow writes'
  }

  return {readable, writable, reason}
}

export async function loadEventContext(
  environment: NodeJS.ProcessEnv = process.env
): Promise<EventContext> {
  let defaultBranch: string | undefined
  const environmentBaseRef = environment['GITHUB_BASE_REF']
  let baseBranch =
    environmentBaseRef === undefined || environmentBaseRef.length === 0
      ? undefined
      : environmentBaseRef
  const eventPath = environment['GITHUB_EVENT_PATH']
  if (eventPath) {
    try {
      const value: unknown = JSON.parse(await readFile(eventPath, 'utf8'))
      if (
        typeof value === 'object' &&
        value !== null &&
        'repository' in value &&
        typeof value.repository === 'object' &&
        value.repository !== null &&
        'default_branch' in value.repository &&
        typeof value.repository.default_branch === 'string'
      ) {
        defaultBranch = value.repository.default_branch
      }
      if (
        typeof value === 'object' &&
        value !== null &&
        'pull_request' in value &&
        typeof value.pull_request === 'object' &&
        value.pull_request !== null &&
        'base' in value.pull_request &&
        typeof value.pull_request.base === 'object' &&
        value.pull_request.base !== null &&
        'ref' in value.pull_request.base &&
        typeof value.pull_request.base.ref === 'string' &&
        value.pull_request.base.ref.length > 0
      ) {
        baseBranch = value.pull_request.base.ref
      }
    } catch {
      // Auto mode deliberately fails closed to read-only when context is absent.
    }
  }

  return {
    eventName: environment['GITHUB_EVENT_NAME'] ?? '',
    ref: environment['GITHUB_REF'] ?? '',
    refProtected: environment['GITHUB_REF_PROTECTED'] === 'true',
    ...(baseBranch === undefined ? {} : {baseBranch}),
    ...(defaultBranch === undefined ? {} : {defaultBranch})
  }
}

export function validateCacheEnvironment(
  environment: NodeJS.ProcessEnv = process.env
): {resultsUrl: string; runtimeToken: string} {
  const resultsUrl = environment['ACTIONS_RESULTS_URL']
  const runtimeToken = environment['ACTIONS_RUNTIME_TOKEN']
  const v2Flag = environment['ACTIONS_CACHE_SERVICE_V2']?.toLowerCase()
  if (!v2Flag || v2Flag === 'false' || v2Flag === '0') {
    throw new Error('ACTIONS_CACHE_SERVICE_V2 is not enabled')
  }
  if (!resultsUrl) throw new Error('ACTIONS_RESULTS_URL is not available')
  if (!runtimeToken) throw new Error('ACTIONS_RUNTIME_TOKEN is not available')

  let parsed: URL
  try {
    parsed = new URL(resultsUrl)
  } catch {
    throw new Error('ACTIONS_RESULTS_URL is not a valid URL')
  }
  if (parsed.protocol !== 'https:' && parsed.protocol !== 'http:') {
    throw new Error('ACTIONS_RESULTS_URL must use HTTP or HTTPS')
  }
  if (parsed.username || parsed.password) {
    throw new Error('ACTIONS_RESULTS_URL must not contain credentials')
  }
  if (
    parsed.protocol === 'http:' &&
    parsed.hostname !== '127.0.0.1' &&
    parsed.hostname !== '[::1]' &&
    parsed.hostname !== 'localhost' &&
    !parsed.hostname.endsWith('.localhost')
  ) {
    throw new Error('ACTIONS_RESULTS_URL must use HTTPS unless it is loopback')
  }

  const serverUrl = new URL(
    environment['GITHUB_SERVER_URL'] ?? 'https://github.com'
  )
  const hostname = serverUrl.hostname.toLowerCase()
  if (
    hostname !== 'github.com' &&
    !hostname.endsWith('.ghe.com') &&
    hostname !== 'localhost' &&
    !hostname.endsWith('.localhost')
  ) {
    throw new Error(
      'Actions Cache v2 is not supported on GitHub Enterprise Server'
    )
  }

  return {resultsUrl, runtimeToken}
}
