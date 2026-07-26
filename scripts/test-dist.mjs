import {spawn, spawnSync} from 'node:child_process'
import path from 'node:path'

const root = path.resolve(import.meta.dirname, '..')
const child = spawn(
  process.execPath,
  ['--import', 'tsx', '--test', 'test/daemon.integration.test.ts'],
  {
    cwd: root,
    env: {
      ...process.env,
      BRC_TEST_DAEMON_ENTRY: path.join(root, 'dist', 'daemon.js')
    },
    stdio: 'inherit'
  }
)

const result = await new Promise((resolve, reject) => {
  child.once('error', reject)
  child.once('exit', (code, signal) => resolve({code, signal}))
})
if (result.signal !== null) {
  throw new Error(`dist daemon test exited from signal ${result.signal}`)
}
if (result.code !== 0) {
  process.exitCode = result.code ?? 1
} else {
  for (const entry of ['main', 'daemon', 'post']) {
    const check = spawnSync(
      process.execPath,
      ['--check', path.join(root, 'dist', `${entry}.js`)],
      {stdio: 'inherit'}
    )
    if (check.status !== 0 || check.signal !== null) {
      throw new Error(`dist/${entry}.js failed Node syntax validation`)
    }
  }

  const smokeEnvironment = {...process.env}
  for (const name of Object.keys(smokeEnvironment)) {
    if (
      name.startsWith('INPUT_') ||
      name.startsWith('STATE_') ||
      name === 'ACTIONS_CACHE_SERVICE_V2' ||
      name === 'ACTIONS_RESULTS_URL' ||
      name === 'ACTIONS_RUNTIME_TOKEN' ||
      name === 'GITHUB_STATE'
    ) {
      delete smokeEnvironment[name]
    }
  }
  const main = spawnSync(
    process.execPath,
    [path.join(root, 'dist', 'main.js')],
    {
      env: smokeEnvironment,
      encoding: 'utf8'
    }
  )
  if (main.status !== 1 || main.signal !== null) {
    throw new Error('dist/main.js did not fail closed without Cache v2 state')
  }
  const post = spawnSync(
    process.execPath,
    [path.join(root, 'dist', 'post.js')],
    {
      env: smokeEnvironment,
      stdio: 'inherit'
    }
  )
  if (post.status !== 0 || post.signal !== null) {
    throw new Error('dist/post.js no-state smoke test failed')
  }
}
