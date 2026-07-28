import type {ArtifactClient, UploadArtifactResponse} from '@actions/artifact'
import {constants as fsConstants} from 'node:fs'
import {
  chmod,
  type FileHandle,
  lstat,
  mkdir,
  mkdtemp,
  open,
  rm,
  writeFile
} from 'node:fs/promises'
import path from 'node:path'
import {CONTROL_FILES} from './control.js'
import type {CacheKind, MetricsSnapshot} from './model.js'
import {ACTION_VERSION} from './version.js'

export type DiagnosticUploadMode = 'on-error' | 'always' | 'never'

export interface DiagnosticContext {
  area: string
  operation: string
  kind?: CacheKind
  digest?: string
  fallback?: string
}

export interface DiagnosticEvent {
  timestamp: string
  area: string
  operation: string
  kind?: CacheKind
  digestPrefix?: string
  fallback?: string
  name: string
  message: string
  statusCode?: number
  retryable?: boolean
  rateLimited?: boolean
  retryAfterMs?: number
  conflict?: boolean
  aborted: boolean
}

export interface DiagnosticsArtifactDocument {
  schemaVersion: 1
  actionVersion: string
  createdAt: string
  reason: 'cache-errors' | 'always' | 'startup-failure'
  lifecycle: {
    phase: 'startup' | 'post'
    stopped?: boolean
    errors: string[]
  }
  metrics?: MetricsSnapshot
  errors: DiagnosticEvent[]
  daemonMessages: string[]
}

export interface DiagnosticsArtifactOptions {
  runnerTemp: string
  controlDirectory: string
  artifactName: string
  retentionDays: number
  reason: DiagnosticsArtifactDocument['reason']
  phase: DiagnosticsArtifactDocument['lifecycle']['phase']
  stopped?: boolean
  lifecycleErrors?: readonly string[]
  stats?: MetricsSnapshot
  now?: () => Date
}

export interface DiagnosticsArtifactUploadOptions {
  runnerTemp: string
  artifactName: string
  retentionDays: number
}

type ArtifactUploader = Pick<ArtifactClient, 'uploadArtifact'>

const MAX_JOURNAL_EVENTS = 1000
const MAX_JOURNAL_BYTES = 1024 * 1024
const MAX_DAEMON_LOG_BYTES = 256 * 1024
const FIELD_PATTERN = /^[a-z][a-z0-9-]{0,47}$/
const ARTIFACT_NAME_PATTERN =
  /^bazel-gha-remote-cache-diagnostics-[0-9]+-[0-9a-f]{16}$/

function record(value: unknown): Record<string, unknown> | undefined {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    return undefined
  }
  return value as Record<string, unknown>
}

function numberField(
  value: Record<string, unknown> | undefined,
  name: string,
  minimum: number,
  maximum: number
): number | undefined {
  const candidate = value?.[name]
  return typeof candidate === 'number' &&
    Number.isSafeInteger(candidate) &&
    candidate >= minimum &&
    candidate <= maximum
    ? candidate
    : undefined
}

function booleanField(
  value: Record<string, unknown> | undefined,
  name: string
): boolean | undefined {
  const candidate = value?.[name]
  return typeof candidate === 'boolean' ? candidate : undefined
}

function safeField(value: string, fallback: string): string {
  return FIELD_PATTERN.test(value) ? value : fallback
}

export function redactDiagnosticText(value: string): string {
  return value
    .replace(/https?:\/\/[^\s"'<>]+/giu, '<redacted-url>')
    .replace(/bearer\s+[^\s]+/giu, 'Bearer <redacted>')
    .replace(/\bgithub_pat_[A-Za-z0-9_]{20,}\b/gu, '<redacted-token>')
    .replace(/\bgh[pousr]_[A-Za-z0-9_]{20,}\b/gu, '<redacted-token>')
    .replace(/\b[0-9a-f]{64}\b/giu, '<redacted-digest>')
    .replace(
      /\b[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\b/gu,
      '<redacted-token>'
    )
    .replace(/sig(?:nature)?=[^&\s]+/giu, 'sig=<redacted>')
    .replace(
      /\b(authorization|github[-_]?token|runtime[-_]?token|shutdown[-_]?token|password|secret)\b\s*[:=]\s*["']?[^,\s"'}]+/giu,
      '$1=<redacted>'
    )
    .replace(
      /(["'])(?:(?:[A-Za-z]:\\)|\/|\\\\)[^"'<>\r\n]*\1/gu,
      '$1<redacted-path>$1'
    )
    .replace(/\\\\[^\\\s"'<>]+\\[^\s"'<>]+/gu, '<redacted-path>')
    .replace(/(?:[A-Za-z]:\\|\/)[^\s"'<>]+/gu, '<redacted-path>')
    .replace(
      /(?<![A-Za-z0-9_.-])(?:\.{1,2}[\\/])?(?:[A-Za-z0-9_.@+-]+[\\/])+[A-Za-z0-9_.@+-]+/gu,
      '<redacted-path>'
    )
}

function diagnosticEvent(
  context: DiagnosticContext,
  error: unknown,
  now: Date
): DiagnosticEvent {
  const value = record(error)
  const statusCode = numberField(value, 'statusCode', 100, 599)
  const retryAfterMs = numberField(
    value,
    'retryAfterMs',
    0,
    Number.MAX_SAFE_INTEGER
  )
  const rawName = typeof value?.['name'] === 'string' ? value['name'] : 'Error'
  const rawMessage =
    error instanceof Error
      ? error.message
      : typeof error === 'string'
        ? error
        : 'cache operation failed'
  const message = redactDiagnosticText(rawMessage).slice(0, 500)
  const aborted =
    rawName === 'AbortError' ||
    statusCode === 499 ||
    /\babort(?:ed)?\b/iu.test(message)
  const digestPrefix =
    context.digest !== undefined && /^[0-9a-f]{64}$/.test(context.digest)
      ? context.digest.slice(0, 12)
      : undefined
  const retryable = booleanField(value, 'retryable')
  const rateLimited = booleanField(value, 'rateLimited')
  const conflict = booleanField(value, 'conflict')

  return {
    timestamp: now.toISOString(),
    area: safeField(context.area, 'unknown'),
    operation: safeField(context.operation, 'unknown'),
    ...(context.kind === undefined ? {} : {kind: context.kind}),
    ...(digestPrefix === undefined ? {} : {digestPrefix}),
    ...(context.fallback === undefined
      ? {}
      : {fallback: safeField(context.fallback, 'unknown')}),
    name: safeField(rawName.toLowerCase(), 'error'),
    message,
    ...(statusCode === undefined ? {} : {statusCode}),
    ...(retryable === undefined ? {} : {retryable}),
    ...(rateLimited === undefined ? {} : {rateLimited}),
    ...(retryAfterMs === undefined ? {} : {retryAfterMs}),
    ...(conflict === undefined ? {} : {conflict}),
    aborted
  }
}

export class DiagnosticJournal {
  private readonly handle: Promise<FileHandle | undefined>
  private chain: Promise<void> = Promise.resolve()
  private closePromise: Promise<void> | undefined
  private events = 0
  private limitRecorded = false
  private persistenceError: unknown

  constructor(
    readonly filePath: string,
    private readonly now: () => Date = () => new Date(),
    private readonly maxEvents = MAX_JOURNAL_EVENTS
  ) {
    if (!Number.isSafeInteger(maxEvents) || maxEvents < 1) {
      throw new Error('diagnostic event limit must be positive')
    }
    const flags =
      fsConstants.O_WRONLY |
      fsConstants.O_APPEND |
      fsConstants.O_CREAT |
      fsConstants.O_EXCL |
      (process.platform === 'win32' ? 0 : fsConstants.O_NOFOLLOW)
    this.handle = open(filePath, flags, 0o600).catch(error => {
      this.persistenceError ??= error
      return undefined
    })
  }

  record(context: DiagnosticContext, error: unknown): void {
    if (this.closePromise !== undefined) {
      this.persistenceError ??= new Error(
        'diagnostic event arrived after the journal was closed'
      )
      return
    }
    if (this.events >= this.maxEvents) {
      if (!this.limitRecorded) {
        this.limitRecorded = true
        this.enqueue(
          diagnosticEvent(
            {area: 'diagnostics', operation: 'limit'},
            new Error('additional diagnostic events were omitted'),
            this.now()
          )
        )
      }
      return
    }
    this.events += 1
    this.enqueue(diagnosticEvent(context, error, this.now()))
  }

  async flush(): Promise<unknown | undefined> {
    this.closePromise ??= this.close()
    await this.closePromise
    return this.persistenceError
  }

  private enqueue(event: DiagnosticEvent): void {
    const line = `${JSON.stringify(event)}\n`
    this.chain = this.chain.then(async () => {
      try {
        const handle = await this.handle
        if (handle !== undefined) {
          await handle.appendFile(line, {encoding: 'utf8'})
        }
      } catch (error) {
        this.persistenceError ??= error
      }
    })
  }

  private async close(): Promise<void> {
    await this.chain
    const handle = await this.handle
    if (handle === undefined) return
    try {
      await handle.sync()
    } catch (error) {
      this.persistenceError ??= error
    }
    try {
      await handle.close()
    } catch (error) {
      this.persistenceError ??= error
    }
  }
}

async function readRegularFile(
  filePath: string,
  maximumBytes: number,
  fromEnd: boolean
): Promise<string | undefined> {
  let before
  try {
    before = await lstat(filePath)
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return undefined
    throw error
  }
  if (!before.isFile() || before.isSymbolicLink()) return undefined

  const flags =
    fsConstants.O_RDONLY |
    (process.platform === 'win32' ? 0 : fsConstants.O_NOFOLLOW)
  const handle = await open(filePath, flags)
  try {
    const current = await handle.stat()
    if (
      !current.isFile() ||
      current.dev !== before.dev ||
      current.ino !== before.ino
    ) {
      return undefined
    }
    const length = Math.min(current.size, maximumBytes)
    const offset = fromEnd ? Math.max(0, current.size - length) : 0
    const buffer = Buffer.alloc(length)
    let bytesRead = 0
    while (bytesRead < length) {
      const result = await handle.read(
        buffer,
        bytesRead,
        length - bytesRead,
        offset + bytesRead
      )
      if (result.bytesRead === 0) break
      bytesRead += result.bytesRead
    }
    let text = buffer.subarray(0, bytesRead).toString('utf8')
    if (fromEnd && offset > 0) {
      const newline = text.indexOf('\n')
      text = newline === -1 ? '' : text.slice(newline + 1)
    }
    return text
  } finally {
    await handle.close()
  }
}

function parseJournal(raw: string | undefined): DiagnosticEvent[] {
  if (raw === undefined) return []
  const events: DiagnosticEvent[] = []
  for (const line of raw.split('\n')) {
    if (line.length === 0 || events.length > MAX_JOURNAL_EVENTS) continue
    let value: unknown
    try {
      value = JSON.parse(line)
    } catch {
      continue
    }
    const data = record(value)
    if (data === undefined) continue
    const timestamp = data['timestamp']
    const area = data['area']
    const operation = data['operation']
    const name = data['name']
    const message = data['message']
    const aborted = data['aborted']
    if (
      typeof timestamp !== 'string' ||
      !Number.isFinite(Date.parse(timestamp)) ||
      typeof area !== 'string' ||
      !FIELD_PATTERN.test(area) ||
      typeof operation !== 'string' ||
      !FIELD_PATTERN.test(operation) ||
      typeof name !== 'string' ||
      !FIELD_PATTERN.test(name) ||
      typeof message !== 'string' ||
      typeof aborted !== 'boolean'
    ) {
      continue
    }
    const kind = data['kind']
    const digestPrefix = data['digestPrefix']
    const fallback = data['fallback']
    const statusCode = numberField(data, 'statusCode', 100, 599)
    const retryAfterMs = numberField(
      data,
      'retryAfterMs',
      0,
      Number.MAX_SAFE_INTEGER
    )
    const retryable = booleanField(data, 'retryable')
    const rateLimited = booleanField(data, 'rateLimited')
    const conflict = booleanField(data, 'conflict')
    events.push({
      timestamp,
      area,
      operation,
      ...(kind === 'ac' || kind === 'cas' ? {kind} : {}),
      ...(typeof digestPrefix === 'string' &&
      /^[0-9a-f]{12}$/.test(digestPrefix)
        ? {digestPrefix}
        : {}),
      ...(typeof fallback === 'string' && FIELD_PATTERN.test(fallback)
        ? {fallback}
        : {}),
      name,
      message: redactDiagnosticText(message).slice(0, 500),
      ...(statusCode === undefined ? {} : {statusCode}),
      ...(retryable === undefined ? {} : {retryable}),
      ...(rateLimited === undefined ? {} : {rateLimited}),
      ...(retryAfterMs === undefined ? {} : {retryAfterMs}),
      ...(conflict === undefined ? {} : {conflict}),
      aborted
    })
  }
  return events
}

function daemonMessages(raw: string | undefined): string[] {
  if (raw === undefined) return []
  return raw
    .split('\n')
    .filter(
      line =>
        line.startsWith('Cache daemon failed:') ||
        line.startsWith('Cache daemon shutdown failed:') ||
        line.startsWith('Cache diagnostic journal failed:')
    )
    .slice(-100)
    .map(line => redactDiagnosticText(line).slice(0, 500))
}

function sanitizedMetrics(stats: MetricsSnapshot): MetricsSnapshot {
  const result = structuredClone(stats)
  result.writeBack.remainingObjectIds = []
  return result
}

export function diagnosticsArtifactName(
  runId: string,
  jobHash: string
): string {
  if (!/^(0|[1-9][0-9]*)$/.test(runId) || !/^[0-9a-f]{16}$/.test(jobHash)) {
    throw new Error('diagnostic artifact identity is invalid')
  }
  return `bazel-gha-remote-cache-diagnostics-${runId}-${jobHash}`
}

export function shouldUploadDiagnostics(
  mode: DiagnosticUploadMode,
  hasCacheErrors: boolean,
  hasLifecycleErrors: boolean
): boolean {
  return (
    mode === 'always' ||
    (mode === 'on-error' && (hasCacheErrors || hasLifecycleErrors))
  )
}

export async function buildDiagnosticsDocument(
  options: DiagnosticsArtifactOptions
): Promise<DiagnosticsArtifactDocument> {
  const journal = await readRegularFile(
    path.join(options.controlDirectory, CONTROL_FILES.diagnostics),
    MAX_JOURNAL_BYTES,
    false
  )
  const log = await readRegularFile(
    path.join(options.controlDirectory, CONTROL_FILES.log),
    MAX_DAEMON_LOG_BYTES,
    true
  )
  const now = options.now?.() ?? new Date()
  return {
    schemaVersion: 1,
    actionVersion: ACTION_VERSION,
    createdAt: now.toISOString(),
    reason: options.reason,
    lifecycle: {
      phase: options.phase,
      ...(options.stopped === undefined ? {} : {stopped: options.stopped}),
      errors: (options.lifecycleErrors ?? []).map(error =>
        redactDiagnosticText(error).slice(0, 500)
      )
    },
    ...(options.stats === undefined
      ? {}
      : {metrics: sanitizedMetrics(options.stats)}),
    errors: parseJournal(journal),
    daemonMessages: daemonMessages(log)
  }
}

export async function uploadDiagnosticsArtifact(
  options: DiagnosticsArtifactOptions,
  uploader?: ArtifactUploader
): Promise<UploadArtifactResponse> {
  if (!options.runnerTemp) throw new Error('RUNNER_TEMP is not available')
  const document = await buildDiagnosticsDocument(options)
  return uploadDiagnosticsDocument(options, document, uploader)
}

export async function uploadDiagnosticsDocument(
  options: DiagnosticsArtifactUploadOptions,
  document: DiagnosticsArtifactDocument,
  uploader?: ArtifactUploader
): Promise<UploadArtifactResponse> {
  if (!options.runnerTemp) throw new Error('RUNNER_TEMP is not available')
  if (!ARTIFACT_NAME_PATTERN.test(options.artifactName)) {
    throw new Error('diagnostic artifact name is invalid')
  }
  if (
    !Number.isSafeInteger(options.retentionDays) ||
    options.retentionDays < 1 ||
    options.retentionDays > 90
  ) {
    throw new Error('diagnostic artifact retention must be between 1 and 90')
  }
  await mkdir(options.runnerTemp, {recursive: true})
  const stagingDirectory = await mkdtemp(
    path.join(path.resolve(options.runnerTemp), 'bazel-gha-diagnostics-')
  )
  await chmod(stagingDirectory, 0o700).catch(() => {})
  try {
    const documentPath = path.join(stagingDirectory, 'diagnostics.json')
    await writeFile(documentPath, `${JSON.stringify(document, null, 2)}\n`, {
      encoding: 'utf8',
      mode: 0o600,
      flag: 'wx'
    })
    await chmod(documentPath, 0o600).catch(() => {})
    const resolvedUploader =
      uploader ?? (await import('@actions/artifact')).default
    return await resolvedUploader.uploadArtifact(
      options.artifactName,
      [documentPath],
      stagingDirectory,
      {retentionDays: options.retentionDays, compressionLevel: 9}
    )
  } finally {
    await rm(stagingDirectory, {recursive: true, force: true}).catch(() => {})
  }
}
