import path from 'node:path'
import {createReadStream} from 'node:fs'
import {ActionsCacheBackend} from './backend.js'
import {PackCatalog} from './catalog.js'
import {validateCacheEnvironment} from './config.js'
import {CONTROL_FILES, readJsonFile, writeJsonAtomic} from './control.js'
import {safeErrorMessage, validateDaemonConfig} from './lifecycle.js'
import {Metrics} from './metrics.js'
import type {DaemonConfig, DaemonReady} from './model.js'
import {EntryPacer} from './pacer.js'
import {
  packBloomMightContain,
  packCacheKeyPrefix,
  tryParsePackCacheKey,
  type ParsedPackCacheKey
} from './pack-format.js'
import {PackReader} from './pack-reader.js'
import {namespaceHash} from './pack-writer.js'
import {CacheHttpServer} from './server.js'
import {WriteBackQueue} from './writeback.js'

interface DaemonCredentials {
  resultsUrl: string
  runtimeToken: string
  githubToken?: string
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
    runtimeToken: data['runtimeToken'],
    ...(typeof data['githubToken'] === 'string' &&
    data['githubToken'].length > 0
      ? {githubToken: data['githubToken']}
      : {})
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

  const pacer =
    config.writable && config.writeBack
      ? new EntryPacer({
          repositoryUploadBudget: config.repositoryUploadBudget,
          expectedWriters: config.expectedWriters,
          uploadBurst: config.uploadBurst
        })
      : undefined
  const writeBack =
    pacer === undefined
      ? undefined
      : new WriteBackQueue({config, backend, metrics, pacer})

  let packReader: PackReader | undefined
  if (config.readable && config.storageMode === 'pack') {
    if (!suppliedCredentials.githubToken) {
      throw new Error('packed storage requires a GitHub catalog token')
    }
    const repositoryParts = config.githubRepository.split('/')
    const owner = repositoryParts[0]
    const repository = repositoryParts[1]
    if (owner === undefined || repository === undefined) {
      throw new Error('packed storage repository is invalid')
    }
    const expectedNamespaceHash = namespaceHash(config.namespace)
    const catalog = new PackCatalog<ParsedPackCacheKey>({
      owner,
      repository,
      token: suppliedCredentials.githubToken,
      keyPrefix: packCacheKeyPrefix(expectedNamespaceHash),
      currentRef: config.currentRef,
      ...(config.baseRef === undefined ? {} : {baseRef: config.baseRef}),
      defaultRef: config.defaultRef,
      codec: {
        parse: key => {
          const parsed = tryParsePackCacheKey(key)
          return parsed?.namespaceHash === expectedNamespaceHash
            ? parsed
            : undefined
        },
        mightContain: (metadata, kind, digest) =>
          packBloomMightContain(metadata.bloom, kind, digest)
      },
      apiBaseUrl: config.githubApiUrl,
      refreshIntervalMs: config.catalogRefreshSeconds * 1000,
      requestTimeoutMs: config.remoteTimeoutSeconds * 1000
    })
    packReader = new PackReader({
      backend,
      catalog,
      metrics,
      directory: path.join(config.controlDirectory, 'pack-downloads'),
      maxObjectSize: config.maxObjectSize
    })
  }

  let shutdownPromise: Promise<void> | undefined
  const cacheServer = new CacheHttpServer({
    config,
    backend,
    metrics,
    ...(writeBack === undefined ? {} : {writeBack}),
    ...(packReader === undefined ? {} : {packReader}),
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
