import * as core from '@actions/core'
import {createHash, randomBytes, randomUUID} from 'node:crypto'
import {open} from 'node:fs/promises'
import path from 'node:path'
import {fileURLToPath} from 'node:url'
import {spawn, type ChildProcess} from 'node:child_process'
import {Writable} from 'node:stream'
import {
  loadEventContext,
  parseInputs,
  resolvePermissions,
  validateCacheEnvironment
} from './config.js'
import {
  CONTROL_FILES,
  createControlDirectory,
  pathExists,
  readJsonFile,
  removeControlDirectory,
  writePrivateFile
} from './control.js'
import {safeErrorMessage, sleep, validateDaemonReady} from './lifecycle.js'
import type {DaemonConfig, DaemonReady} from './model.js'

const STARTUP_TIMEOUT_MS = 20_000

function daemonEntryPoint(): string {
  return fileURLToPath(new URL('daemon.js', import.meta.url))
}

function daemonEnvironment(): NodeJS.ProcessEnv {
  const allowed = [
    'PATH',
    'SystemRoot',
    'WINDIR',
    'COMSPEC',
    'PATHEXT',
    'TEMP',
    'TMP',
    'TMPDIR',
    'USERPROFILE',
    'LANG',
    'LC_ALL',
    'TZ',
    'HTTP_PROXY',
    'HTTPS_PROXY',
    'NO_PROXY',
    'http_proxy',
    'https_proxy',
    'no_proxy',
    'SSL_CERT_FILE',
    'SSL_CERT_DIR',
    'NODE_EXTRA_CA_CERTS',
    'NODE_USE_ENV_PROXY',
    'GITHUB_SERVER_URL'
  ]
  const environment: NodeJS.ProcessEnv = {}
  for (const name of allowed) {
    const value = process.env[name]
    if (value !== undefined) environment[name] = value
  }
  return environment
}

async function sendDaemonCredentials(
  child: ChildProcess,
  credentials: {
    resultsUrl: string
    runtimeToken: string
    githubToken?: string
  }
): Promise<void> {
  const pipe = child.stdio[3]
  if (!(pipe instanceof Writable)) {
    throw new Error('cache daemon credential pipe was not created')
  }
  await new Promise<void>((resolve, reject) => {
    pipe.once('error', reject)
    pipe.end(`${JSON.stringify(credentials)}\n`, resolve)
  })
}

async function terminateChild(child: ChildProcess): Promise<void> {
  if (
    child.pid === undefined ||
    child.exitCode !== null ||
    child.signalCode !== null
  )
    return
  child.kill('SIGTERM')
  for (
    let attempt = 0;
    attempt < 20 && child.exitCode === null && child.signalCode === null;
    attempt += 1
  ) {
    await sleep(100)
  }
  if (child.exitCode === null && child.signalCode === null)
    child.kill('SIGKILL')
}

async function waitForReady(
  child: ChildProcess,
  readyPath: string,
  expectedInstanceId: string,
  spawnError: () => Error | undefined
): Promise<DaemonReady> {
  const deadline = Date.now() + STARTUP_TIMEOUT_MS
  while (Date.now() < deadline) {
    const childError = spawnError()
    if (childError !== undefined) throw childError
    if (child.exitCode !== null || child.signalCode !== null) {
      throw new Error(
        `cache daemon exited during startup (${child.exitCode ?? child.signalCode})`
      )
    }
    if (await pathExists(readyPath)) {
      const ready = validateDaemonReady(await readJsonFile<unknown>(readyPath))
      if (ready.pid !== child.pid || ready.instanceId !== expectedInstanceId) {
        throw new Error('cache daemon readiness identity did not match')
      }
      const health = await fetch(`${ready.url}/healthz`, {
        signal: AbortSignal.timeout(1000)
      }).catch(() => undefined)
      if (health?.ok) {
        try {
          const body = (await health.json()) as {instanceId?: unknown}
          if (body.instanceId === expectedInstanceId) return ready
        } catch {
          // Continue until the complete health contract is visible.
        }
      }
    }
    await sleep(100)
  }
  throw new Error(
    'cache daemon did not become healthy before the startup timeout'
  )
}

async function run(): Promise<void> {
  const inputs = parseInputs(name => core.getInput(name))
  if (inputs.githubToken) core.setSecret(inputs.githubToken)
  // Preserve this policy outside the daemon-owned control files so the post
  // step can still honor it when those files are missing or malformed.
  core.saveState('fail_job_on_cache_error', String(inputs.failJobOnCacheError))
  const credentials = validateCacheEnvironment()
  const {runtimeToken} = credentials
  core.setSecret(runtimeToken)

  const context = await loadEventContext()
  const permissions = resolvePermissions(
    inputs.mode,
    context,
    process.env['ACTIONS_CACHE_MODE']
  )
  if (
    inputs.storageMode === 'pack' &&
    permissions.readable &&
    !inputs.githubToken
  ) {
    throw new Error(
      'github-token is required for readable packed storage; grant Actions read permission or select storage-mode: object'
    )
  }
  const runnerTemp = process.env['RUNNER_TEMP'] ?? ''
  const controlDirectory = await createControlDirectory(runnerTemp)
  let child: ChildProcess | undefined
  let childSpawnError: Error | undefined

  try {
    const shutdownToken = randomBytes(32).toString('base64url')
    const instanceId = randomUUID()
    const githubRepository =
      process.env['GITHUB_REPOSITORY'] ?? 'local/repository'
    const config: DaemonConfig = {
      namespace: inputs.namespace,
      storageMode: inputs.storageMode,
      port: inputs.port,
      readable: permissions.readable,
      writable: permissions.writable,
      maxObjectSize: inputs.maxObjectSize,
      maxInflightBytes: inputs.maxInflightBytes,
      maxPendingBytes: inputs.maxPendingBytes,
      uploadConcurrency: inputs.uploadConcurrency,
      downloadConcurrency: inputs.downloadConcurrency,
      repositoryUploadBudget: inputs.repositoryUploadBudget,
      expectedWriters: inputs.expectedWriters,
      uploadBurst: inputs.uploadBurst,
      writeBack: inputs.writeBack,
      flushTimeoutSeconds: inputs.flushTimeoutSeconds,
      packTargetBytes: inputs.packTargetBytes,
      packMaxObjects: inputs.packMaxObjects,
      packMaxAgeSeconds: inputs.packMaxAgeSeconds,
      catalogRefreshSeconds: inputs.catalogRefreshSeconds,
      remoteTimeoutSeconds: inputs.remoteTimeoutSeconds,
      failJobOnCacheError: inputs.failJobOnCacheError,
      githubApiUrl: process.env['GITHUB_API_URL'] ?? 'https://api.github.com',
      githubRepository,
      currentRef: context.ref,
      ...(context.baseBranch === undefined
        ? {}
        : {baseRef: `refs/heads/${context.baseBranch}`}),
      defaultRef: context.defaultBranch
        ? `refs/heads/${context.defaultBranch}`
        : context.ref,
      runId: process.env['GITHUB_RUN_ID'] ?? '0',
      jobHash: createHash('sha256')
        .update(instanceId)
        .digest('hex')
        .slice(0, 16),
      controlDirectory,
      shutdownToken,
      instanceId
    }
    const configPath = path.join(controlDirectory, CONTROL_FILES.config)
    const readyPath = path.join(controlDirectory, CONTROL_FILES.ready)
    const logPath = path.join(controlDirectory, CONTROL_FILES.log)
    await writePrivateFile(configPath, `${JSON.stringify(config)}\n`)

    const log = await open(logPath, 'wx', 0o600)
    try {
      child = spawn(process.execPath, [daemonEntryPoint(), configPath], {
        detached: true,
        env: daemonEnvironment(),
        shell: false,
        windowsHide: true,
        stdio: ['ignore', log.fd, log.fd, 'pipe']
      })
      child.once('error', error => {
        childSpawnError = error
      })
      await sendDaemonCredentials(child, {
        ...credentials,
        ...(inputs.githubToken ? {githubToken: inputs.githubToken} : {})
      })
    } finally {
      await log.close()
    }
    if (child.pid === undefined)
      throw new Error('cache daemon did not receive a PID')
    child.unref()

    core.saveState('pid', String(child.pid))
    core.saveState('control_directory', controlDirectory)
    core.saveState('instance_id', instanceId)

    const ready = await waitForReady(
      child,
      readyPath,
      instanceId,
      () => childSpawnError
    )
    const cacheUrl = `${ready.url}/cache`
    const bazelrcPath = path.join(controlDirectory, CONTROL_FILES.bazelrc)
    const bazelrc = [
      `build --remote_cache=${cacheUrl}`,
      `build --remote_timeout=${inputs.remoteTimeoutSeconds}`,
      'build --noremote_cache_compression',
      ...(ready.writable ? [] : ['build --remote_upload_local_results=false'])
    ].join('\n')
    await writePrivateFile(bazelrcPath, `${bazelrc}\n`)

    core.setOutput('url', cacheUrl)
    core.setOutput('writable', String(ready.writable))
    core.setOutput('readable', String(ready.readable))
    core.setOutput('bazelrc', bazelrcPath)
    core.info(
      `Bazel cache adapter is ready (${ready.readable ? 'read' : 'no-read'}, ${
        ready.writable ? 'write' : 'no-write'
      }); ${permissions.reason}.`
    )
  } catch (error) {
    if (child !== undefined) await terminateChild(child)
    await removeControlDirectory(controlDirectory, runnerTemp).catch(() => {})
    throw error
  }
}

try {
  await run()
} catch (error) {
  core.setFailed(safeErrorMessage(error))
}
