import path from 'node:path'
import type {
  DaemonConfig,
  DaemonReady,
  KindCounters,
  MetricsSnapshot
} from './model.js'

function record(value: unknown): Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new Error('control data must be a JSON object')
  }
  return value as Record<string, unknown>
}

function stringField(value: Record<string, unknown>, name: string): string {
  const field = value[name]
  if (typeof field !== 'string' || field.length === 0) {
    throw new Error(`control data has an invalid ${name}`)
  }
  return field
}

function optionalStringField(
  value: Record<string, unknown>,
  name: string
): string | undefined {
  if (value[name] === undefined) return undefined
  return stringField(value, name)
}

function booleanField(value: Record<string, unknown>, name: string): boolean {
  const field = value[name]
  if (typeof field !== 'boolean') {
    throw new Error(`control data has an invalid ${name}`)
  }
  return field
}

function integerField(
  value: Record<string, unknown>,
  name: string,
  minimum: number,
  maximum: number
): number {
  const field = value[name]
  if (
    typeof field !== 'number' ||
    !Number.isSafeInteger(field) ||
    field < minimum ||
    field > maximum
  ) {
    throw new Error(`control data has an invalid ${name}`)
  }
  return field
}

export function validateDaemonConfig(value: unknown): DaemonConfig {
  const data = record(value)
  const namespace = stringField(data, 'namespace')
  if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(namespace)) {
    throw new Error('control data has an invalid namespace')
  }
  const maxObjectSize = integerField(
    data,
    'maxObjectSize',
    1,
    Number.MAX_SAFE_INTEGER
  )
  const maxInflightBytes = integerField(
    data,
    'maxInflightBytes',
    maxObjectSize,
    Number.MAX_SAFE_INTEGER
  )
  const maxPendingBytes = integerField(
    data,
    'maxPendingBytes',
    maxObjectSize,
    Number.MAX_SAFE_INTEGER
  )
  const storageMode = stringField(data, 'storageMode')
  if (storageMode !== 'object' && storageMode !== 'pack') {
    throw new Error('control data has an invalid storageMode')
  }
  const writeBack = booleanField(data, 'writeBack')
  if (storageMode === 'pack' && !writeBack) {
    throw new Error('packed storage requires write-back')
  }
  const packTargetBytes = integerField(
    data,
    'packTargetBytes',
    1,
    maxPendingBytes
  )
  const controlDirectory = stringField(data, 'controlDirectory')
  if (!path.isAbsolute(controlDirectory)) {
    throw new Error('control directory must be absolute')
  }
  const shutdownToken = stringField(data, 'shutdownToken')
  if (!/^[A-Za-z0-9_-]{32,}$/.test(shutdownToken)) {
    throw new Error('control data has an invalid shutdown token')
  }
  const instanceId = stringField(data, 'instanceId')
  if (!/^[0-9a-f-]{36}$/.test(instanceId)) {
    throw new Error('control data has an invalid instance ID')
  }
  const githubApiUrl = stringField(data, 'githubApiUrl')
  let parsedApiUrl: URL
  try {
    parsedApiUrl = new URL(githubApiUrl)
  } catch {
    throw new Error('control data has an invalid GitHub API URL')
  }
  if (
    parsedApiUrl.protocol !== 'https:' ||
    parsedApiUrl.username !== '' ||
    parsedApiUrl.password !== ''
  ) {
    throw new Error('control data has an invalid GitHub API URL')
  }
  const githubRepository = stringField(data, 'githubRepository')
  if (!/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(githubRepository)) {
    throw new Error('control data has an invalid GitHub repository')
  }
  const currentRef = stringField(data, 'currentRef')
  const baseRef = optionalStringField(data, 'baseRef')
  const defaultRef = stringField(data, 'defaultRef')
  if (
    !currentRef.startsWith('refs/') ||
    (baseRef !== undefined && !baseRef.startsWith('refs/heads/')) ||
    !defaultRef.startsWith('refs/')
  ) {
    throw new Error('control data has an invalid Git ref')
  }
  const runId = stringField(data, 'runId')
  if (!/^(0|[1-9][0-9]{0,19})$/.test(runId)) {
    throw new Error('control data has an invalid run ID')
  }
  const jobHash = stringField(data, 'jobHash')
  if (!/^[0-9a-f]{16,64}$/.test(jobHash)) {
    throw new Error('control data has an invalid job hash')
  }

  return {
    namespace,
    storageMode,
    port: integerField(data, 'port', 0, 65535),
    readable: booleanField(data, 'readable'),
    writable: booleanField(data, 'writable'),
    maxObjectSize,
    maxInflightBytes,
    maxPendingBytes,
    uploadConcurrency: integerField(data, 'uploadConcurrency', 1, 128),
    downloadConcurrency: integerField(data, 'downloadConcurrency', 1, 256),
    repositoryUploadBudget: integerField(
      data,
      'repositoryUploadBudget',
      1,
      200
    ),
    expectedWriters: integerField(data, 'expectedWriters', 1, 256),
    uploadBurst: integerField(data, 'uploadBurst', 1, 200),
    writeBack,
    flushTimeoutSeconds: integerField(data, 'flushTimeoutSeconds', 1, 3600),
    packTargetBytes,
    packMaxObjects: integerField(data, 'packMaxObjects', 1, 256),
    packMaxAgeSeconds: integerField(data, 'packMaxAgeSeconds', 1, 300),
    catalogRefreshSeconds: integerField(data, 'catalogRefreshSeconds', 1, 3600),
    remoteTimeoutSeconds: integerField(data, 'remoteTimeoutSeconds', 1, 3600),
    failJobOnCacheError: booleanField(data, 'failJobOnCacheError'),
    githubApiUrl,
    githubRepository,
    currentRef,
    ...(baseRef === undefined ? {} : {baseRef}),
    defaultRef,
    runId,
    jobHash,
    controlDirectory,
    shutdownToken,
    instanceId
  }
}

export function validateDaemonReady(value: unknown): DaemonReady {
  const data = record(value)
  const port = integerField(data, 'port', 1, 65535)
  const url = stringField(data, 'url')
  const parsed = new URL(url)
  if (
    parsed.protocol !== 'http:' ||
    parsed.hostname !== '127.0.0.1' ||
    parsed.pathname !== '/' ||
    parsed.search !== '' ||
    parsed.hash !== '' ||
    Number(parsed.port) !== port
  ) {
    throw new Error('daemon reported a non-loopback URL')
  }
  return {
    pid: integerField(data, 'pid', 1, 2_147_483_647),
    port,
    url: url.replace(/\/$/, ''),
    readable: booleanField(data, 'readable'),
    writable: booleanField(data, 'writable'),
    instanceId: stringField(data, 'instanceId'),
    startedAt: stringField(data, 'startedAt')
  }
}

export function validateMetrics(value: unknown): MetricsSnapshot {
  const data = record(value)
  if (data['schemaVersion'] !== 2) throw new Error('unknown metrics schema')

  const timestamp = (source: Record<string, unknown>, name: string): string => {
    const result = stringField(source, name)
    if (!Number.isFinite(Date.parse(result))) {
      throw new Error(`control data has an invalid ${name}`)
    }
    return result
  }
  const counter = (source: Record<string, unknown>, name: string): number =>
    integerField(source, name, 0, Number.MAX_SAFE_INTEGER)
  const decimal = (source: Record<string, unknown>, name: string): number => {
    const value = source[name]
    if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) {
      throw new Error(`control data has an invalid ${name}`)
    }
    return value
  }
  const objectIdentities = (source: unknown): string[] => {
    if (
      !Array.isArray(source) ||
      source.length > 100 ||
      source.some(
        value =>
          typeof value !== 'string' || !/^(ac|cas):[0-9a-f]{64}$/.test(value)
      )
    ) {
      throw new Error('control data has invalid remaining object identities')
    }
    const identities = [...source]
    if (new Set(identities).size !== identities.length) {
      throw new Error('control data has duplicate remaining object identities')
    }
    return identities
  }
  const kindCounters = (source: unknown): KindCounters => {
    const counters = record(source)
    return {
      hits: counter(counters, 'hits'),
      misses: counter(counters, 'misses'),
      successes: counter(counters, 'successes'),
      conflicts: counter(counters, 'conflicts'),
      errors: counter(counters, 'errors'),
      bytes: counter(counters, 'bytes'),
      latencyMs: counter(counters, 'latencyMs')
    }
  }

  const requests = record(data['requests'])
  const reads = record(data['reads'])
  const writes = record(data['writes'])
  const backend = record(data['backend'])
  const rateLimits = record(data['rateLimits'])
  const writeBack = record(data['writeBack'])
  const catalog = record(data['catalog'])
  const stoppedAt = data['stoppedAt']
  const remainingObjects = counter(writeBack, 'remainingObjects')
  const remainingObjectIds = objectIdentities(writeBack['remainingObjectIds'])
  if (remainingObjectIds.length > remainingObjects) {
    throw new Error('control data has too many remaining object identities')
  }
  if (
    stoppedAt !== undefined &&
    (typeof stoppedAt !== 'string' || !Number.isFinite(Date.parse(stoppedAt)))
  ) {
    throw new Error('control data has an invalid stoppedAt')
  }

  return {
    schemaVersion: 2,
    startedAt: timestamp(data, 'startedAt'),
    ...(stoppedAt === undefined ? {} : {stoppedAt}),
    readable: booleanField(data, 'readable'),
    writable: booleanField(data, 'writable'),
    requests: {
      health: counter(requests, 'health'),
      shutdown: counter(requests, 'shutdown'),
      get: counter(requests, 'get'),
      put: counter(requests, 'put'),
      rejected: counter(requests, 'rejected')
    },
    reads: {
      ac: kindCounters(reads['ac']),
      cas: kindCounters(reads['cas'])
    },
    writes: {
      ac: kindCounters(writes['ac']),
      cas: kindCounters(writes['cas'])
    },
    backend: {
      lookups: counter(backend, 'lookups'),
      reservations: counter(backend, 'reservations'),
      uploads: counter(backend, 'uploads'),
      finalizations: counter(backend, 'finalizations'),
      downloads: counter(backend, 'downloads'),
      errors: counter(backend, 'errors'),
      rateLimited: counter(backend, 'rateLimited')
    },
    rateLimits: {
      reserve: counter(rateLimits, 'reserve'),
      upload: counter(rateLimits, 'upload'),
      finalize: counter(rateLimits, 'finalize'),
      lookup: counter(rateLimits, 'lookup'),
      download: counter(rateLimits, 'download')
    },
    writeBack: {
      acceptedObjects: counter(writeBack, 'acceptedObjects'),
      deduplicatedObjects: counter(writeBack, 'deduplicatedObjects'),
      packedObjects: counter(writeBack, 'packedObjects'),
      packsFinalized: counter(writeBack, 'packsFinalized'),
      packBytes: counter(writeBack, 'packBytes'),
      pendingObjects: counter(writeBack, 'pendingObjects'),
      pendingBytes: counter(writeBack, 'pendingBytes'),
      peakPendingBytes: counter(writeBack, 'peakPendingBytes'),
      remainingObjects,
      remainingObjectIds,
      acBlockedByBarrier: counter(writeBack, 'acBlockedByBarrier'),
      reservationSleepMs: counter(writeBack, 'reservationSleepMs'),
      configuredEntriesPerMinute: decimal(
        writeBack,
        'configuredEntriesPerMinute'
      ),
      currentEntriesPerMinute: decimal(writeBack, 'currentEntriesPerMinute')
    },
    catalog: {
      refreshes: counter(catalog, 'refreshes'),
      bloomCandidates: counter(catalog, 'bloomCandidates'),
      bloomFalsePositives: counter(catalog, 'bloomFalsePositives'),
      rangeBytesDownloaded: counter(catalog, 'rangeBytesDownloaded')
    },
    integrityFailures: counter(data, 'integrityFailures'),
    casWriteFailed: booleanField(data, 'casWriteFailed'),
    writeCircuitOpen: booleanField(data, 'writeCircuitOpen'),
    readCircuitOpen: booleanField(data, 'readCircuitOpen'),
    inflightBytes: counter(data, 'inflightBytes'),
    peakInflightBytes: counter(data, 'peakInflightBytes')
  }
}

export function metricsHaveCacheErrors(stats: MetricsSnapshot): boolean {
  return (
    stats.stoppedAt === undefined ||
    stats.backend.errors > 0 ||
    stats.backend.rateLimited > 0 ||
    stats.integrityFailures > 0 ||
    stats.casWriteFailed ||
    stats.readCircuitOpen ||
    stats.writeCircuitOpen ||
    stats.reads.ac.errors > 0 ||
    stats.reads.cas.errors > 0 ||
    stats.writes.ac.errors > 0 ||
    stats.writes.cas.errors > 0 ||
    stats.writeBack.remainingObjects > 0
  )
}

export function safeErrorMessage(error: unknown): string {
  const raw = error instanceof Error ? error.message : String(error)
  return raw
    .replace(/https?:\/\/[^\s"'<>]+/giu, '<redacted-url>')
    .replace(/bearer\s+[^\s]+/giu, 'Bearer <redacted>')
    .replace(/sig(?:nature)?=[^&\s]+/giu, 'sig=<redacted>')
    .slice(0, 500)
}

export function sleep(milliseconds: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, milliseconds))
}

export function processIsRunning(pid: number): boolean {
  try {
    process.kill(pid, 0)
    return true
  } catch (error) {
    return (error as NodeJS.ErrnoException).code === 'EPERM'
  }
}
