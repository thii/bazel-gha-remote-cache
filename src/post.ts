import * as core from '@actions/core'
import {createHash} from 'node:crypto'
import path from 'node:path'
import {
  CONTROL_FILES,
  isSafeControlDirectory,
  pathExists,
  readJsonFile,
  removeControlDirectory
} from './control.js'
import {
  buildDiagnosticsDocument,
  diagnosticsArtifactName,
  redactDiagnosticText,
  shouldUploadDiagnostics,
  uploadDiagnosticsDocument,
  type DiagnosticsArtifactDocument,
  type DiagnosticUploadMode
} from './diagnostics.js'
import {
  metricsHaveCacheErrors,
  processIsRunning,
  safeErrorMessage,
  sleep,
  validateDaemonConfig,
  validateDaemonReady,
  validateMetrics
} from './lifecycle.js'
import type {DaemonConfig, DaemonReady, MetricsSnapshot} from './model.js'

const SHUTDOWN_BUFFER_MS = 15_000

function statePid(raw: string): number | undefined {
  if (!/^[1-9][0-9]*$/.test(raw)) return undefined
  const value = Number(raw)
  return Number.isSafeInteger(value) && value <= 2_147_483_647
    ? value
    : undefined
}

async function waitForExit(
  pid: number,
  milliseconds: number
): Promise<boolean> {
  const deadline = Date.now() + milliseconds
  while (Date.now() < deadline) {
    if (!processIsRunning(pid)) return true
    await sleep(100)
  }
  return !processIsRunning(pid)
}

async function terminateKnownProcess(pid: number): Promise<boolean> {
  if (!processIsRunning(pid)) return true
  try {
    process.kill(pid, 'SIGTERM')
  } catch {
    return true
  }
  if (await waitForExit(pid, 3000)) return true
  try {
    process.kill(pid, 'SIGKILL')
  } catch {
    return true
  }
  return waitForExit(pid, 2000)
}

async function daemonIdentityMatches(ready: DaemonReady): Promise<boolean> {
  try {
    const response = await fetch(`${ready.url}/healthz`, {
      signal: AbortSignal.timeout(1000)
    })
    if (!response.ok) return false
    const value = (await response.json()) as {
      instanceId?: unknown
      pid?: unknown
    }
    return value.instanceId === ready.instanceId && value.pid === ready.pid
  } catch {
    return false
  }
}

async function stopDaemon(
  pid: number,
  ready: DaemonReady,
  config: DaemonConfig
): Promise<boolean> {
  if (!processIsRunning(pid)) return true
  if (!(await daemonIdentityMatches(ready))) return false
  const drainDeadlineMs = config.flushTimeoutSeconds * 1000
  const gracefulTimeoutMs = drainDeadlineMs + SHUTDOWN_BUFFER_MS
  const gracefulDeadline = Date.now() + gracefulTimeoutMs
  try {
    const response = await fetch(`${ready.url}/shutdown`, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${config.shutdownToken}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({drain: true, deadlineMs: drainDeadlineMs}),
      signal: AbortSignal.timeout(Math.min(10_000, gracefulTimeoutMs))
    })
    if (response.status !== 202 && response.status !== 503) {
      core.warning(
        `Cache daemon rejected graceful shutdown (${response.status}).`
      )
    }
  } catch {
    core.warning('Cache daemon did not respond to graceful shutdown.')
  }

  // The daemon closes its listener before draining the write-back queue. Do
  // not send SIGTERM while that bounded drain can still be making progress.
  if (await waitForExit(pid, Math.max(0, gracefulDeadline - Date.now()))) {
    return true
  }
  // Identity was verified immediately before the authenticated shutdown
  // request, and waitForExit polls often enough to observe an intervening exit.
  return terminateKnownProcess(pid)
}

async function addSummary(stats: MetricsSnapshot): Promise<void> {
  const readRows = (['ac', 'cas'] as const).map(kind => [
    kind.toUpperCase(),
    String(stats.reads[kind].hits),
    String(stats.reads[kind].misses),
    String(stats.reads[kind].errors),
    String(stats.reads[kind].bytes)
  ])
  const writeRows = (['ac', 'cas'] as const).map(kind => [
    kind.toUpperCase(),
    String(stats.writes[kind].successes),
    String(stats.writes[kind].conflicts),
    String(stats.writes[kind].errors),
    String(stats.writes[kind].bytes)
  ])

  const elapsedMinutes = Math.max(
    1 / 60,
    ((stats.stoppedAt ? Date.parse(stats.stoppedAt) : Date.now()) -
      Date.parse(stats.startedAt)) /
      60_000
  )
  const reservationRate = stats.backend.reservations / elapsedMinutes
  const lookupRate = stats.backend.lookups / elapsedMinutes
  const rateLimitOperations =
    stats.rateLimits.reserve +
    stats.rateLimits.upload +
    stats.rateLimits.finalize +
    stats.rateLimits.lookup +
    stats.rateLimits.download
  const rateLimitCount = Math.max(
    stats.backend.rateLimited,
    rateLimitOperations
  )
  const sawRateLimit = rateLimitCount > 0
  const averageObjectsPerPack =
    stats.writeBack.packsFinalized === 0
      ? 0
      : stats.writeBack.packedObjects / stats.writeBack.packsFinalized
  const averageBytesPerPack =
    stats.writeBack.packsFinalized === 0
      ? 0
      : stats.writeBack.packBytes / stats.writeBack.packsFinalized

  core.summary
    .addHeading('Bazel Actions Cache adapter', 3)
    .addTable([
      [
        {data: 'Read', header: true},
        {data: 'Hits', header: true},
        {data: 'Misses', header: true},
        {data: 'Errors', header: true},
        {data: 'Bytes', header: true}
      ],
      ...readRows
    ])
    .addTable([
      [
        {data: 'Write', header: true},
        {data: 'Saved', header: true},
        {data: 'Conflicts', header: true},
        {data: 'Errors', header: true},
        {data: 'Bytes', header: true}
      ],
      ...writeRows
    ])
    .addTable([
      [
        {data: 'Write-back and packs', header: true},
        {data: 'Value', header: true}
      ],
      ['Bazel objects accepted', String(stats.writeBack.acceptedObjects)],
      [
        'Objects deduplicated locally',
        String(stats.writeBack.deduplicatedObjects)
      ],
      ['Objects written into packs', String(stats.writeBack.packedObjects)],
      ['Packs finalized', String(stats.writeBack.packsFinalized)],
      ['Average objects per pack', averageObjectsPerPack.toFixed(1)],
      ['Average bytes per pack', averageBytesPerPack.toFixed(1)],
      ['Pending objects', String(stats.writeBack.pendingObjects)],
      ['Pending bytes', String(stats.writeBack.pendingBytes)],
      ['Peak pending bytes', String(stats.writeBack.peakPendingBytes)],
      [
        'Objects remaining at shutdown',
        String(stats.writeBack.remainingObjects)
      ],
      [
        'AC records blocked by CAS barriers',
        String(stats.writeBack.acBlockedByBarrier)
      ]
    ])
    .addTable([
      [
        {data: 'Rate limiting', header: true},
        {data: 'Value', header: true}
      ],
      [
        'Configured entry rate',
        `${stats.writeBack.configuredEntriesPerMinute.toFixed(1)}/min`
      ],
      [
        'Current adaptive entry rate',
        `${stats.writeBack.currentEntriesPerMinute.toFixed(1)}/min`
      ],
      ['Observed reservations', `${reservationRate.toFixed(1)}/min`],
      [
        'Reservation pacing sleep',
        `${(stats.writeBack.reservationSleepMs / 1000).toFixed(1)} s`
      ],
      ['Rate-limit responses', sawRateLimit ? `Yes (${rateLimitCount})` : 'No'],
      ['Rate limited: reserve', String(stats.rateLimits.reserve)],
      ['Rate limited: upload', String(stats.rateLimits.upload)],
      ['Rate limited: finalize', String(stats.rateLimits.finalize)],
      ['Rate limited: lookup', String(stats.rateLimits.lookup)],
      ['Rate limited: download', String(stats.rateLimits.download)]
    ])
    .addTable([
      [
        {data: 'Pack catalog and reads', header: true},
        {data: 'Value', header: true}
      ],
      ['Catalog refreshes', String(stats.catalog.refreshes)],
      ['Bloom candidates', String(stats.catalog.bloomCandidates)],
      ['Bloom false positives', String(stats.catalog.bloomFalsePositives)],
      ['Range bytes downloaded', String(stats.catalog.rangeBytesDownloaded)]
    ])
    .addRaw(
      `Backend calls: ${stats.backend.lookups} lookups, ${stats.backend.reservations} reservations, ${stats.backend.uploads} uploads, ${stats.backend.finalizations} finalizations. ` +
        `Peak spool usage: ${stats.peakInflightBytes} bytes.\n\n`
    )

  if (reservationRate >= 160 || lookupRate >= 1200) {
    core.summary.addRaw(
      `⚠️ Projected request pressure is ${reservationRate.toFixed(1)} reservations/min and ${lookupRate.toFixed(1)} lookups/min. Object-per-entry mode is intended for near-term, moderate-volume sharing.\n`
    )
  }
  if (sawRateLimit) {
    core.summary.addRaw(
      `⚠️ GitHub rate-limited ${rateLimitCount} cache operation${rateLimitCount === 1 ? '' : 's'}. Our observed reservation rate was ${reservationRate.toFixed(1)}/min; other jobs and cache consumers may also be using the repository-wide budget.\n`
    )
  }
  if (stats.writeBack.remainingObjects > 0) {
    core.summary.addRaw(
      `⚠️ ${stats.writeBack.remainingObjects} cache object${stats.writeBack.remainingObjects === 1 ? '' : 's'} remained unflushed at shutdown.\n`
    )
    if (stats.writeBack.remainingObjectIds.length > 0) {
      const listed = stats.writeBack.remainingObjectIds
        .map(value => `- \`${value}\``)
        .join('\n')
      const omitted =
        stats.writeBack.remainingObjects -
        stats.writeBack.remainingObjectIds.length
      core.summary.addDetails(
        'Unflushed cache objects',
        `${listed}${omitted > 0 ? `\n- …and ${omitted} more` : ''}`
      )
    }
  }
  if (stats.casWriteFailed) {
    core.summary.addRaw(
      '⚠️ A CAS write failed, so later action-cache uploads were suppressed for integrity.\n'
    )
  }
  if (stats.diagnosticJournalFailed) {
    core.summary.addRaw(
      '⚠️ The structured diagnostic journal could not be written completely.\n'
    )
  }
  await core.summary.write()
}

async function run(failJobOnCacheError: boolean): Promise<void> {
  const controlDirectory = core.getState('control_directory')
  const pid = statePid(core.getState('pid'))
  const expectedInstanceId = core.getState('instance_id')
  if (!controlDirectory || pid === undefined) return

  const runnerTemp = process.env['RUNNER_TEMP'] ?? ''
  if (!isSafeControlDirectory(controlDirectory, runnerTemp)) {
    core.warning('Refusing to use an invalid cache daemon control directory.')
    if (failJobOnCacheError) {
      core.setFailed('The Bazel cache adapter post-step state was invalid.')
    }
    return
  }

  let config: DaemonConfig | undefined
  let ready: DaemonReady | undefined
  let stopped = !processIsRunning(pid)
  let stats: MetricsSnapshot | undefined
  let cleanupError = false
  const lifecycleErrors: string[] = []

  const uploadPreparedDiagnostics = async (
    retentionDays: number,
    document: DiagnosticsArtifactDocument
  ): Promise<void> => {
    const environmentRunId = process.env['GITHUB_RUN_ID'] ?? '0'
    const runId = /^(0|[1-9][0-9]*)$/.test(environmentRunId)
      ? environmentRunId
      : '0'
    const jobHash =
      config?.jobHash ??
      createHash('sha256')
        .update(expectedInstanceId || controlDirectory)
        .digest('hex')
        .slice(0, 16)
    const artifactName = diagnosticsArtifactName(runId, jobHash)
    try {
      const uploaded = await uploadDiagnosticsDocument(
        {
          runnerTemp,
          artifactName,
          retentionDays
        },
        document
      )
      const runUrl =
        uploaded.id === undefined ||
        !process.env['GITHUB_SERVER_URL'] ||
        !process.env['GITHUB_REPOSITORY'] ||
        !process.env['GITHUB_RUN_ID']
          ? undefined
          : `${process.env['GITHUB_SERVER_URL']}/${process.env['GITHUB_REPOSITORY']}/actions/runs/${process.env['GITHUB_RUN_ID']}/artifacts/${uploaded.id}`
      core.notice(
        runUrl === undefined
          ? `Uploaded cache diagnostics artifact ${artifactName}.`
          : `Uploaded cache diagnostics artifact ${artifactName}: ${runUrl}`
      )
      core.summary.addRaw(
        runUrl === undefined
          ? `Cache diagnostics artifact: \`${artifactName}\`\n`
          : `Cache diagnostics artifact: [\`${artifactName}\`](${runUrl})\n`
      )
      await core.summary.write()
    } catch (error) {
      core.warning(
        `Could not upload cache diagnostics: ${safeErrorMessage(error)}`
      )
    }
  }

  const uploadMode = diagnosticUploadMode(core.getState('upload_diagnostics'))
  const diagnosticsRetentionDays = diagnosticRetentionDays(
    core.getState('diagnostics_retention_days')
  )
  try {
    config = validateDaemonConfig(
      await readJsonFile<unknown>(
        path.join(controlDirectory, CONTROL_FILES.config)
      )
    )
    if (
      path.resolve(config.controlDirectory) !== path.resolve(controlDirectory)
    ) {
      throw new Error(
        'cache daemon config control directory did not match state'
      )
    }
    ready = validateDaemonReady(
      await readJsonFile<unknown>(
        path.join(controlDirectory, CONTROL_FILES.ready)
      )
    )
    if (
      ready.pid !== pid ||
      ready.instanceId !== expectedInstanceId ||
      config.instanceId !== expectedInstanceId
    ) {
      throw new Error('cache daemon identity did not match saved state')
    }
    stopped = await stopDaemon(pid, ready, config)
    if (!stopped) core.warning('Cache daemon could not be terminated cleanly.')

    const statsPath = path.join(controlDirectory, CONTROL_FILES.stats)
    if (await pathExists(statsPath)) {
      stats = validateMetrics(await readJsonFile<unknown>(statsPath))
      await addSummary(stats)
    }
  } catch (error) {
    cleanupError = true
    const message = safeErrorMessage(error)
    lifecycleErrors.push(message)
    core.warning(`Cache daemon cleanup warning: ${message}`)
  }

  let diagnosticsDocument: DiagnosticsArtifactDocument | undefined
  if (uploadMode !== 'never') {
    try {
      diagnosticsDocument = await buildDiagnosticsDocument({
        runnerTemp,
        controlDirectory,
        artifactName: diagnosticsArtifactName(
          /^(0|[1-9][0-9]*)$/.test(process.env['GITHUB_RUN_ID'] ?? '')
            ? (process.env['GITHUB_RUN_ID'] as string)
            : '0',
          config?.jobHash ??
            createHash('sha256')
              .update(expectedInstanceId || controlDirectory)
              .digest('hex')
              .slice(0, 16)
        ),
        retentionDays: diagnosticsRetentionDays,
        reason: 'cache-errors',
        phase: 'post',
        stopped,
        lifecycleErrors,
        ...(stats === undefined ? {} : {stats})
      })
    } catch (error) {
      core.warning(
        `Could not prepare cache diagnostics: ${safeErrorMessage(error)}`
      )
    }
  }

  if (stopped) {
    try {
      await removeControlDirectory(controlDirectory, runnerTemp)
    } catch (error) {
      cleanupError = true
      const message = safeErrorMessage(error)
      lifecycleErrors.push(message)
      core.warning(`Could not remove cache control files: ${message}`)
    }
  } else {
    const message =
      'Cache daemon identity could not be safely terminated; private control files were retained.'
    lifecycleErrors.push(message)
    core.warning(message)
  }

  const hasCacheErrors = stats === undefined || metricsHaveCacheErrors(stats)
  const hasRecordedErrors =
    (diagnosticsDocument?.errors.length ?? 0) > 0 ||
    (diagnosticsDocument?.daemonMessages.length ?? 0) > 0
  if (
    diagnosticsDocument !== undefined &&
    shouldUploadDiagnostics(
      uploadMode,
      hasCacheErrors || hasRecordedErrors,
      lifecycleErrors.length > 0
    )
  ) {
    diagnosticsDocument.reason =
      uploadMode === 'always' &&
      !hasCacheErrors &&
      !hasRecordedErrors &&
      lifecycleErrors.length === 0
        ? 'always'
        : 'cache-errors'
    diagnosticsDocument.lifecycle.stopped = stopped
    diagnosticsDocument.lifecycle.errors = lifecycleErrors.map(error =>
      redactDiagnosticText(error).slice(0, 500)
    )
    await uploadPreparedDiagnostics(
      diagnosticsRetentionDays,
      diagnosticsDocument
    )
  }

  if (
    failJobOnCacheError &&
    (cleanupError ||
      !stopped ||
      stats === undefined ||
      metricsHaveCacheErrors(stats))
  ) {
    core.setFailed('The Bazel cache adapter observed one or more cache errors.')
  }
}

function diagnosticUploadMode(raw: string): DiagnosticUploadMode {
  return raw === 'always' || raw === 'never' || raw === 'on-error'
    ? raw
    : 'on-error'
}

function diagnosticRetentionDays(raw: string): number {
  if (!/^[1-9][0-9]*$/.test(raw)) return 7
  const value = Number(raw)
  return Number.isSafeInteger(value) && value <= 90 ? value : 7
}

const failJobOnCacheError = core.getState('fail_job_on_cache_error') === 'true'
try {
  await run(failJobOnCacheError)
} catch (error) {
  core.warning(`Cache post-step warning: ${safeErrorMessage(error)}`)
  if (failJobOnCacheError) {
    core.setFailed('The Bazel cache adapter post-step failed.')
  }
}
