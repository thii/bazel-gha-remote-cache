import * as core from '@actions/core'
import path from 'node:path'
import {
  CONTROL_FILES,
  isSafeControlDirectory,
  pathExists,
  readJsonFile,
  removeControlDirectory
} from './control.js'
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
  try {
    const response = await fetch(`${ready.url}/shutdown`, {
      method: 'POST',
      headers: {Authorization: `Bearer ${config.shutdownToken}`},
      signal: AbortSignal.timeout(5000)
    })
    if (response.status !== 202 && response.status !== 503) {
      core.warning(
        `Cache daemon rejected graceful shutdown (${response.status}).`
      )
    }
  } catch {
    core.warning('Cache daemon did not respond to graceful shutdown.')
  }

  if (await waitForExit(pid, 15_000)) return true
  if (!(await daemonIdentityMatches(ready))) return false
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
    .addRaw(
      `Backend calls: ${stats.backend.lookups} lookups, ${stats.backend.reservations} reservations, ${stats.backend.uploads} uploads, ${stats.backend.finalizations} finalizations. ` +
        `Peak spool usage: ${stats.peakInflightBytes} bytes.\n\n`
    )

  const elapsedMinutes = Math.max(
    1 / 60,
    ((stats.stoppedAt ? Date.parse(stats.stoppedAt) : Date.now()) -
      Date.parse(stats.startedAt)) /
      60_000
  )
  const uploadRate = stats.backend.reservations / elapsedMinutes
  const lookupRate = stats.backend.lookups / elapsedMinutes
  if (uploadRate >= 160 || lookupRate >= 1200) {
    core.summary.addRaw(
      `⚠️ Projected request pressure is ${uploadRate.toFixed(1)} uploads/min and ${lookupRate.toFixed(1)} lookups/min. Object-per-entry mode is intended for near-term, moderate-volume sharing.\n`
    )
  }
  if (stats.casWriteFailed) {
    core.summary.addRaw(
      '⚠️ A CAS write failed, so later action-cache uploads were suppressed for integrity.\n'
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
    core.warning(`Cache daemon cleanup warning: ${safeErrorMessage(error)}`)
  } finally {
    if (stopped) {
      await removeControlDirectory(controlDirectory, runnerTemp).catch(
        error => {
          cleanupError = true
          core.warning(
            `Could not remove cache control files: ${safeErrorMessage(error)}`
          )
        }
      )
    } else {
      core.warning(
        `Cache daemon identity could not be safely terminated; control files remain at ${controlDirectory}.`
      )
    }
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

const failJobOnCacheError = core.getState('fail_job_on_cache_error') === 'true'
try {
  await run(failJobOnCacheError)
} catch (error) {
  core.warning(`Cache post-step warning: ${safeErrorMessage(error)}`)
  if (failJobOnCacheError) {
    core.setFailed('The Bazel cache adapter post-step failed.')
  }
}
