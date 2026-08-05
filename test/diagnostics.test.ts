import assert from 'node:assert/strict'
import {
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  rename,
  rm,
  symlink,
  writeFile
} from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import {test} from 'node:test'
import {BackendError} from '../src/backend.js'
import {
  DiagnosticJournal,
  buildDiagnosticsDocument,
  diagnosticsArtifactName,
  redactDiagnosticText,
  shouldUploadDiagnostics,
  uploadDiagnosticsArtifact
} from '../src/diagnostics.js'
import {Metrics} from '../src/metrics.js'

test('DiagnosticJournal emits bounded structured errors without credentials or URLs', async t => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'brc-diagnostics-'))
  t.after(() => rm(directory, {recursive: true, force: true}))
  const journal = new DiagnosticJournal(
    path.join(directory, 'errors.ndjson'),
    () => new Date('2026-07-28T12:00:00.000Z'),
    1
  )
  const error = new BackendError(
    'upload https://blob.invalid/object?sig=secret with Bearer runtime-secret',
    {
      statusCode: 429,
      retryable: true,
      rateLimited: true,
      retryAfterMs: 60_000
    }
  )

  journal.record(
    {
      area: 'backend',
      operation: 'upload',
      kind: 'cas',
      digest: 'a'.repeat(64)
    },
    error
  )
  journal.record({area: 'backend', operation: 'upload'}, error)
  assert.equal(await journal.flush(), undefined)

  const lines = (await readFile(journal.filePath, 'utf8')).trim().split('\n')
  assert.equal(lines.length, 2)
  const first = JSON.parse(lines[0] as string) as Record<string, unknown>
  assert.deepEqual(first, {
    timestamp: '2026-07-28T12:00:00.000Z',
    area: 'backend',
    operation: 'upload',
    kind: 'cas',
    digestPrefix: 'aaaaaaaaaaaa',
    name: 'backenderror',
    message: 'upload <redacted-url> with Bearer <redacted>',
    statusCode: 429,
    retryable: true,
    rateLimited: true,
    retryAfterMs: 60_000,
    conflict: false,
    aborted: false
  })
  const omitted = JSON.parse(lines[1] as string) as Record<string, unknown>
  assert.equal(omitted.area, 'diagnostics')
  assert.equal(omitted.operation, 'limit')

  const raw = lines.join('\n')
  assert.doesNotMatch(raw, /blob\.invalid|runtime-secret|a{64}/)
})

test('DiagnosticJournal refuses a precreated symbolic link', async t => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'brc-diagnostics-'))
  const external = await mkdtemp(path.join(os.tmpdir(), 'brc-secret-'))
  t.after(() => rm(directory, {recursive: true, force: true}))
  t.after(() => rm(external, {recursive: true, force: true}))
  const externalPath = path.join(external, 'secret')
  const journalPath = path.join(directory, 'errors.ndjson')
  await writeFile(externalPath, 'unchanged\n')
  await symlink(externalPath, journalPath)

  const journal = new DiagnosticJournal(journalPath)
  journal.record({area: 'http', operation: 'put'}, new Error('failure'))
  assert.ok(await journal.flush())
  assert.equal(await readFile(externalPath, 'utf8'), 'unchanged\n')
})

test('DiagnosticJournal keeps writing through its pinned handle after a path swap', async t => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'brc-diagnostics-'))
  const external = await mkdtemp(path.join(os.tmpdir(), 'brc-secret-'))
  t.after(() => rm(directory, {recursive: true, force: true}))
  t.after(() => rm(external, {recursive: true, force: true}))
  const journalPath = path.join(directory, 'errors.ndjson')
  const pinnedPath = path.join(directory, 'pinned.ndjson')
  const externalPath = path.join(external, 'secret')
  await writeFile(externalPath, 'unchanged\n')
  const journal = new DiagnosticJournal(journalPath)

  for (let attempt = 0; attempt < 100; attempt += 1) {
    try {
      await rename(journalPath, pinnedPath)
      break
    } catch (error) {
      if (
        (error as NodeJS.ErrnoException).code !== 'ENOENT' ||
        attempt === 99
      ) {
        throw error
      }
      await new Promise(resolve => setTimeout(resolve, 1))
    }
  }
  await symlink(externalPath, journalPath)
  journal.record({area: 'http', operation: 'put'}, new Error('pinned write'))
  assert.equal(await journal.flush(), undefined)

  assert.equal(await readFile(externalPath, 'utf8'), 'unchanged\n')
  assert.match(await readFile(pinnedPath, 'utf8'), /pinned write/)
})

test('diagnostics document admits only validated metrics and sanitized daemon messages', async t => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'brc-diagnostics-'))
  t.after(() => rm(directory, {recursive: true, force: true}))
  const journal = new DiagnosticJournal(path.join(directory, 'errors.ndjson'))
  journal.record(
    {area: 'http', operation: 'put', kind: 'cas', digest: 'b'.repeat(64)},
    Object.assign(new Error('CAS digest did not match request bytes'), {
      statusCode: 400
    })
  )
  await journal.flush()
  await writeFile(
    path.join(directory, 'daemon.log'),
    [
      'untrusted dependency output secret-value',
      'Cache daemon failed: request to https://cache.invalid/?sig=secret',
      'Cache daemon shutdown failed: Authorization=runtime-secret'
    ].join('\n'),
    {mode: 0o600}
  )
  const metrics = new Metrics(true, true)
  metrics.integrityFailure()
  metrics.setRemainingObjects(1, [`cas:${'c'.repeat(64)}`])
  metrics.stop()

  const document = await buildDiagnosticsDocument({
    runnerTemp: directory,
    controlDirectory: directory,
    artifactName: diagnosticsArtifactName('123', 'd'.repeat(16)),
    retentionDays: 7,
    reason: 'cache-errors',
    phase: 'post',
    stopped: true,
    lifecycleErrors: [
      'github_token=ghp_abcdefghijklmnopqrstuvwxyz123456 and https://api.invalid/'
    ],
    stats: metrics.snapshot(),
    now: () => new Date('2026-07-28T12:01:00.000Z')
  })

  assert.equal(document.schemaVersion, 1)
  assert.equal(document.actionVersion, '0.0.7')
  assert.equal(document.metrics?.integrityFailures, 1)
  assert.deepEqual(document.metrics?.writeBack.remainingObjectIds, [])
  assert.equal(document.errors.length, 1)
  assert.equal(
    document.errors[0]?.message,
    'CAS digest did not match request bytes'
  )
  assert.deepEqual(document.daemonMessages, [
    'Cache daemon failed: request to <redacted-url>',
    'Cache daemon shutdown failed: Authorization=<redacted>'
  ])

  const serialized = JSON.stringify(document)
  assert.doesNotMatch(
    serialized,
    /secret-value|runtime-secret|abcdefghijklmnopqrstuvwxyz|api\.invalid|cache\.invalid|c{64}/
  )
})

test('diagnostics document ignores a substituted symbolic-link journal', async t => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'brc-diagnostics-'))
  const external = await mkdtemp(path.join(os.tmpdir(), 'brc-secret-'))
  t.after(() => rm(directory, {recursive: true, force: true}))
  t.after(() => rm(external, {recursive: true, force: true}))
  const secretPath = path.join(external, 'secret')
  await writeFile(secretPath, 'runtime-token=do-not-upload\n')
  await symlink(secretPath, path.join(directory, 'errors.ndjson'))

  const document = await buildDiagnosticsDocument({
    runnerTemp: directory,
    controlDirectory: directory,
    artifactName: diagnosticsArtifactName('1', '0'.repeat(16)),
    retentionDays: 1,
    reason: 'cache-errors',
    phase: 'post'
  })

  assert.deepEqual(document.errors, [])
  assert.doesNotMatch(JSON.stringify(document), /do-not-upload/)
})

test('uploadDiagnosticsArtifact stages exactly one private JSON document', async t => {
  const runnerTemp = await mkdtemp(path.join(os.tmpdir(), 'brc-runner-'))
  const controlDirectory = path.join(runnerTemp, 'bazel-gha-cache-control')
  await mkdir(controlDirectory, {mode: 0o700})
  t.after(() => rm(runnerTemp, {recursive: true, force: true}))
  const artifactName = diagnosticsArtifactName('456', 'e'.repeat(16))
  let captured:
    | {
        name: string
        files: string[]
        rootDirectory: string
        retentionDays: number | undefined
        content: string
      }
    | undefined
  const uploader = {
    uploadArtifact: async (
      name: string,
      files: string[],
      rootDirectory: string,
      options?: {retentionDays?: number}
    ) => {
      captured = {
        name,
        files,
        rootDirectory,
        retentionDays: options?.retentionDays,
        content: await readFile(files[0] as string, 'utf8')
      }
      return {id: 42, size: 123, digest: 'f'.repeat(64)}
    }
  }

  const result = await uploadDiagnosticsArtifact(
    {
      runnerTemp,
      controlDirectory,
      artifactName,
      retentionDays: 3,
      reason: 'always',
      phase: 'post',
      stopped: true
    },
    uploader
  )

  assert.equal(result.id, 42)
  assert.equal(captured?.name, artifactName)
  assert.equal(captured?.files.length, 1)
  assert.equal(path.basename(captured?.files[0] ?? ''), 'diagnostics.json')
  assert.equal(captured?.rootDirectory, path.dirname(captured?.files[0] ?? ''))
  assert.equal(captured?.retentionDays, 3)
  assert.equal(JSON.parse(captured?.content ?? '').schemaVersion, 1)
  assert.deepEqual(
    (await readdir(runnerTemp)).filter(name =>
      name.startsWith('bazel-gha-diagnostics-')
    ),
    []
  )
})

test('diagnostics policy, identity, and redaction fail closed', () => {
  assert.equal(shouldUploadDiagnostics('on-error', false, false), false)
  assert.equal(shouldUploadDiagnostics('on-error', true, false), true)
  assert.equal(shouldUploadDiagnostics('on-error', false, true), true)
  assert.equal(shouldUploadDiagnostics('always', false, false), true)
  assert.equal(shouldUploadDiagnostics('never', true, true), false)
  assert.equal(
    diagnosticsArtifactName('123', 'a'.repeat(16)),
    `bazel-gha-remote-cache-diagnostics-123-${'a'.repeat(16)}`
  )
  assert.throws(() => diagnosticsArtifactName('run-1', 'a'.repeat(16)))
  assert.throws(() => diagnosticsArtifactName('1', '../not-a-job-id'))

  const redacted = redactDiagnosticText(
    `Bearer abc secret=shh https://example.invalid/?signature=value github_pat_${'x'.repeat(82)} ${'d'.repeat(64)} ../private/file \\\\server\\share\\secret '/Users/admin/My Secret File.txt' "C:\\Users\\admin\\Another Secret.txt"`
  )
  assert.doesNotMatch(
    redacted,
    /abc|shh|example\.invalid|value|github_pat_|x{20}|d{64}|private|server|share|Users|My Secret|Another Secret/
  )
})
