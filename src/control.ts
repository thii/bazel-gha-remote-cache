import {constants as fsConstants} from 'node:fs'
import {
  access,
  chmod,
  mkdir,
  mkdtemp,
  readFile,
  rename,
  rm,
  writeFile
} from 'node:fs/promises'
import path from 'node:path'
import {randomBytes} from 'node:crypto'
import {CONTROL_DIRECTORY_PREFIX} from './model.js'

export const CONTROL_FILES = {
  config: 'daemon-config.json',
  ready: 'ready.json',
  stats: 'stats.json',
  log: 'daemon.log',
  bazelrc: 'bazelrc'
} as const

export async function createControlDirectory(
  runnerTemp: string
): Promise<string> {
  if (!runnerTemp) throw new Error('RUNNER_TEMP is not available')
  await mkdir(runnerTemp, {recursive: true})
  const directory = await mkdtemp(
    path.join(path.resolve(runnerTemp), CONTROL_DIRECTORY_PREFIX)
  )
  await chmod(directory, 0o700).catch(() => {})
  return directory
}

export async function writePrivateFile(
  filePath: string,
  content: string
): Promise<void> {
  await writeFile(filePath, content, {
    encoding: 'utf8',
    mode: 0o600,
    flag: 'wx'
  })
  await chmod(filePath, 0o600).catch(() => {})
}

export async function writeJsonAtomic(
  filePath: string,
  value: unknown
): Promise<void> {
  const temporaryPath = `${filePath}.${randomBytes(8).toString('hex')}.tmp`
  await writeFile(temporaryPath, `${JSON.stringify(value)}\n`, {
    encoding: 'utf8',
    mode: 0o600,
    flag: 'wx'
  })
  try {
    await rename(temporaryPath, filePath)
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code
    if (code !== 'EPERM' && code !== 'EACCES' && code !== 'EEXIST') throw error
    await rm(filePath, {force: true})
    await rename(temporaryPath, filePath)
  } finally {
    await rm(temporaryPath, {force: true}).catch(() => {})
  }
}

export async function readJsonFile<T>(filePath: string): Promise<T> {
  return JSON.parse(await readFile(filePath, 'utf8')) as T
}

export async function pathExists(filePath: string): Promise<boolean> {
  try {
    await access(filePath, fsConstants.F_OK)
    return true
  } catch {
    return false
  }
}

export function isSafeControlDirectory(
  controlDirectory: string,
  runnerTemp: string
): boolean {
  if (!controlDirectory || !runnerTemp) return false
  const resolvedDirectory = path.resolve(controlDirectory)
  const resolvedTemp = path.resolve(runnerTemp)
  const relative = path.relative(resolvedTemp, resolvedDirectory)
  return (
    relative !== '' &&
    !relative.startsWith('..') &&
    !path.isAbsolute(relative) &&
    path.basename(resolvedDirectory).startsWith(CONTROL_DIRECTORY_PREFIX)
  )
}

export async function removeControlDirectory(
  controlDirectory: string,
  runnerTemp: string
): Promise<void> {
  if (!isSafeControlDirectory(controlDirectory, runnerTemp)) {
    throw new Error('refusing to remove an invalid control directory')
  }

  let lastError: unknown
  for (let attempt = 0; attempt < 4; attempt += 1) {
    try {
      await rm(controlDirectory, {recursive: true, force: true})
      return
    } catch (error) {
      lastError = error
      await new Promise(resolve => setTimeout(resolve, 100 * 2 ** attempt))
    }
  }
  throw lastError
}
