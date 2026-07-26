import path from 'node:path'
import {createReadStream} from 'node:fs'
import {ActionsCacheBackend} from './backend.js'
import {validateCacheEnvironment} from './config.js'
import {CONTROL_FILES, readJsonFile, writeJsonAtomic} from './control.js'
import {safeErrorMessage, validateDaemonConfig} from './lifecycle.js'
import {Metrics} from './metrics.js'
import type {DaemonConfig, DaemonReady} from './model.js'
import {CacheHttpServer} from './server.js'

interface DaemonCredentials {
  resultsUrl: string
  runtimeToken: string
}

async function readCredentialPipe(): Promise<DaemonCredentials> {
  const stream = createReadStream('', {
    fd: 3,
    autoClose: true,
    encoding: 'utf8'
  })
  let input = ''
  for await (const chunk of stream) {
    input += chunk
    if (input.length > 64 * 1024) {
      throw new Error('daemon credential payload was too large')
    }
  }
  let value: unknown
  try {
    value = JSON.parse(input)
  } catch {
    throw new Error('daemon credential payload was invalid')
  }
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new Error('daemon credential payload was invalid')
  }
  const data = value as Record<string, unknown>
  if (
    typeof data['resultsUrl'] !== 'string' ||
    data['resultsUrl'].length === 0 ||
    typeof data['runtimeToken'] !== 'string' ||
    data['runtimeToken'].length === 0
  ) {
    throw new Error('daemon credential payload was invalid')
  }
  return {
    resultsUrl: data['resultsUrl'],
    runtimeToken: data['runtimeToken']
  }
}

async function run(): Promise<void> {
  const configPath = process.argv[2]
  if (!configPath) throw new Error('daemon config path is required')
  const config: DaemonConfig = validateDaemonConfig(
    await readJsonFile<unknown>(configPath)
  )
  if (
    path.resolve(configPath) !==
    path.join(config.controlDirectory, CONTROL_FILES.config)
  ) {
    throw new Error('daemon config is outside its control directory')
  }

  const suppliedCredentials = await readCredentialPipe()
  const {resultsUrl, runtimeToken} = validateCacheEnvironment({
    ACTIONS_CACHE_SERVICE_V2: 'true',
    ACTIONS_RESULTS_URL: suppliedCredentials.resultsUrl,
    ACTIONS_RUNTIME_TOKEN: suppliedCredentials.runtimeToken,
    ...(process.env['GITHUB_SERVER_URL'] === undefined
      ? {}
      : {GITHUB_SERVER_URL: process.env['GITHUB_SERVER_URL']})
  })
  const statsPath = path.join(config.controlDirectory, CONTROL_FILES.stats)
  const metrics = new Metrics(config.readable, config.writable, snapshot =>
    writeJsonAtomic(statsPath, snapshot)
  )
  const backend = new ActionsCacheBackend(
    resultsUrl,
    runtimeToken,
    config.remoteTimeoutSeconds
  )

  let shutdownPromise: Promise<void> | undefined
  const cacheServer = new CacheHttpServer({
    config,
    backend,
    metrics,
    onShutdown: () => {
      requestShutdown()
    }
  })

  const shutdown = (): Promise<void> => {
    shutdownPromise ??= (async () => {
      await cacheServer.shutdown()
      metrics.stop()
      await metrics.flush()
    })()
    return shutdownPromise
  }

  const requestShutdown = (): void => {
    void shutdown().catch(error => {
      process.stderr.write(
        `Cache daemon shutdown failed: ${safeErrorMessage(error)}\n`
      )
      process.exitCode = 1
    })
  }

  let serverStarted = false
  let shutdownRequested = false
  const handleSignal = (): void => {
    shutdownRequested = true
    if (serverStarted) requestShutdown()
  }
  process.once('SIGTERM', handleSignal)
  process.once('SIGINT', handleSignal)

  try {
    const address = await cacheServer.start()
    serverStarted = true
    if (shutdownRequested) {
      requestShutdown()
      return
    }
    const ready: DaemonReady = {
      pid: process.pid,
      port: address.port,
      url: `http://127.0.0.1:${address.port}`,
      readable: config.readable,
      writable: config.writable,
      instanceId: config.instanceId,
      startedAt: new Date().toISOString()
    }
    await writeJsonAtomic(
      path.join(config.controlDirectory, CONTROL_FILES.ready),
      ready
    )
    await metrics.flush()
  } catch (error) {
    if (serverStarted) {
      try {
        await shutdown()
      } catch (shutdownError) {
        throw new AggregateError(
          [error, shutdownError],
          'daemon initialization and cleanup failed'
        )
      }
    }
    throw error
  }
}

try {
  await run()
} catch (error) {
  process.stderr.write(`Cache daemon failed: ${safeErrorMessage(error)}\n`)
  process.exitCode = 1
}
