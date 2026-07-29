export const CACHE_KEY_PREFIX = 'brc-v1'
// SHA-256("brc-raw-object-v1"). Cache v2 identity is scope + key + version.
export const CACHE_VERSION =
  '11acdfc3e9b90ded551d471f1478526b0bc5196d2407af7be873fb20f89da139'
export const CONTROL_DIRECTORY_PREFIX = 'bazel-gha-cache-'

export type CacheKind = 'ac' | 'cas'
export type RequestedMode = 'auto' | 'read-only' | 'read-write'
export type StorageMode = 'object' | 'pack'

export interface DaemonConfig {
  namespace: string
  storageMode: StorageMode
  port: number
  readable: boolean
  writable: boolean
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
  githubApiUrl: string
  githubRepository: string
  currentRef: string
  baseRef?: string
  defaultRef: string
  runId: string
  jobHash: string
  controlDirectory: string
  shutdownToken: string
  instanceId: string
}

export interface DaemonReady {
  pid: number
  port: number
  url: string
  readable: boolean
  writable: boolean
  instanceId: string
  startedAt: string
}

export interface KindCounters {
  hits: number
  misses: number
  successes: number
  conflicts: number
  errors: number
  bytes: number
  latencyMs: number
}

export interface MetricsSnapshot {
  schemaVersion: 3
  startedAt: string
  stoppedAt?: string
  readable: boolean
  writable: boolean
  requests: {
    health: number
    shutdown: number
    get: number
    put: number
    rejected: number
    aborted: number
  }
  reads: Record<CacheKind, KindCounters>
  writes: Record<CacheKind, KindCounters>
  backend: {
    lookups: number
    reservations: number
    uploads: number
    finalizations: number
    downloads: number
    errors: number
    rateLimited: number
  }
  rateLimits: {
    reserve: number
    upload: number
    finalize: number
    lookup: number
    download: number
  }
  writeBack: {
    acceptedObjects: number
    deduplicatedObjects: number
    packedObjects: number
    packsFinalized: number
    packBytes: number
    pendingObjects: number
    pendingBytes: number
    peakPendingBytes: number
    remainingObjects: number
    remainingObjectIds: string[]
    acBlockedByBarrier: number
    reservationSleepMs: number
    configuredEntriesPerMinute: number
    currentEntriesPerMinute: number
  }
  catalog: {
    refreshes: number
    bloomCandidates: number
    bloomFalsePositives: number
    rangeBytesDownloaded: number
  }
  integrityFailures: number
  diagnosticJournalFailed: boolean
  casWriteFailed: boolean
  writeCircuitOpen: boolean
  readCircuitOpen: boolean
  inflightBytes: number
  peakInflightBytes: number
}

export interface EventContext {
  eventName: string
  ref: string
  baseBranch?: string
  defaultBranch?: string
  refProtected: boolean
}
