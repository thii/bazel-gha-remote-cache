import assert from 'node:assert/strict'
import {createHash} from 'node:crypto'
import {
  mkdtemp,
  mkdir,
  open,
  readFile,
  readdir,
  rm,
  stat,
  writeFile
} from 'node:fs/promises'
import {tmpdir} from 'node:os'
import path from 'node:path'
import {test, type TestContext} from 'node:test'
import {
  PACK_CACHE_KEY_MAX_LENGTH,
  PACK_CACHE_VERSION,
  entriesInPayloadOrder,
  findPackIndexEntry,
  packBloomMightContain,
  parsePack,
  parsePackCacheKey,
  readPackPayload
} from '../src/pack-format.js'
import {
  PackWriter,
  namespaceHash,
  type PackSourceRecord
} from '../src/pack-writer.js'

const MEBIBYTE = 1024 * 1024

function sha256(value: Uint8Array | string): string {
  return createHash('sha256').update(value).digest('hex')
}

async function temporaryDirectory(t: TestContext): Promise<string> {
  const directory = await mkdtemp(path.join(tmpdir(), 'brc-pack-writer-test-'))
  t.after(() => rm(directory, {recursive: true, force: true}))
  return directory
}

async function writeSource(
  directory: string,
  name: string,
  kind: 'ac' | 'cas',
  digest: string,
  payload: Uint8Array
): Promise<PackSourceRecord> {
  const sourcePath = path.join(directory, name)
  await writeFile(sourcePath, payload, {mode: 0o600})
  return {
    kind,
    digest,
    path: sourcePath,
    size: payload.byteLength,
    bodySha256: sha256(payload)
  }
}

test('PackWriter streams ordered mixed objects into a valid immutable pack', async t => {
  const root = await temporaryDirectory(t)
  const sources = path.join(root, 'sources')
  const packs = path.join(root, 'packs')
  await mkdir(sources, {recursive: true, mode: 0o700})

  const largeCas = Buffer.alloc(MEBIBYTE + 137)
  for (let index = 0; index < largeCas.length; index += 1) {
    largeCas[index] = index % 251
  }
  const action = Buffer.from([0, 255, 1, 2, 0, 10, 42])
  const emptyCas = Buffer.alloc(0)
  const records = [
    await writeSource(sources, 'large.cas', 'cas', sha256(largeCas), largeCas),
    await writeSource(sources, 'result.ac', 'ac', 'f'.repeat(64), action),
    await writeSource(sources, 'empty.cas', 'cas', sha256(emptyCas), emptyCas)
  ]

  const options = {
    directory: packs,
    namespace: 'linux-x86_64-v2',
    runId: '73400291',
    jobHash: '18ca73ff18ca73ff'
  }
  const sealed = await new PackWriter(options).seal(records, 3n)
  t.after(() => sealed.dispose())
  const bytes = await readFile(sealed.path)
  const fileStats = await stat(sealed.path)

  assert.equal(sealed.version, PACK_CACHE_VERSION)
  assert.equal(sealed.sequence, 3n)
  assert.equal(sealed.objectCount, records.length)
  assert.equal(
    sealed.payloadBytes,
    largeCas.byteLength + action.byteLength + emptyCas.byteLength
  )
  assert.equal(sealed.size, bytes.byteLength)
  assert.equal(fileStats.size, sealed.size)
  assert.match(path.basename(sealed.path), /^000003-[0-9a-f]{16}\.pack$/)
  assert.deepEqual(await readdir(packs), [path.basename(sealed.path)])
  assert.deepEqual(
    bytes.subarray(0, sealed.payloadBytes),
    Buffer.concat([largeCas, action, emptyCas])
  )

  const parsed = parsePack(bytes)
  assert.equal(parsed.entries.length, records.length)
  assert.equal(parsed.trailer.indexOffset, BigInt(sealed.payloadBytes))
  const payloadOrder = entriesInPayloadOrder(parsed.entries)
  assert.deepEqual(
    payloadOrder.map(entry => ({
      kind: entry.kind,
      digest: Buffer.from(entry.digest).toString('hex'),
      offset: entry.offset,
      length: entry.length
    })),
    [
      {
        kind: 'cas',
        digest: records[0]!.digest,
        offset: 0n,
        length: BigInt(largeCas.byteLength)
      },
      {
        kind: 'ac',
        digest: records[1]!.digest,
        offset: BigInt(largeCas.byteLength),
        length: BigInt(action.byteLength)
      },
      {
        kind: 'cas',
        digest: records[2]!.digest,
        offset: BigInt(largeCas.byteLength + action.byteLength),
        length: 0n
      }
    ]
  )

  const payloads = [largeCas, action, emptyCas]
  for (let index = 0; index < records.length; index += 1) {
    const record = records[index]
    const payload = payloads[index]
    assert.ok(record)
    assert.ok(payload)
    const entry = findPackIndexEntry(parsed.entries, record.kind, record.digest)
    assert.ok(entry)
    assert.deepEqual(readPackPayload(bytes, entry), payload)
  }

  const key = parsePackCacheKey(sealed.key)
  assert.ok(sealed.key.length <= PACK_CACHE_KEY_MAX_LENGTH)
  assert.equal(
    namespaceHash(options.namespace),
    sha256(options.namespace).slice(0, 16)
  )
  assert.equal(key.namespaceHash.length, 16)
  assert.equal(key.namespaceHash, namespaceHash(options.namespace))
  assert.equal(key.runId, options.runId)
  assert.equal(key.jobHash, options.jobHash)
  assert.equal(key.sequence, 3n)
  assert.equal(key.packId, sha256(bytes).slice(0, 16))
  for (const record of records) {
    assert.equal(
      packBloomMightContain(key.bloom, record.kind, record.digest),
      true
    )
  }

  await sealed.dispose()
  assert.deepEqual(await readdir(packs), [])
})

test('PackWriter rejects source size and content changes without leaving packs', async t => {
  const root = await temporaryDirectory(t)
  const sourcePath = path.join(root, 'source')
  const packs = path.join(root, 'packs')
  const writer = new PackWriter({
    directory: packs,
    namespace: 'mutation-test',
    runId: '1',
    jobHash: '1234567890abcdef'
  })
  const accepted = Buffer.from('accepted')
  const base: PackSourceRecord = {
    kind: 'ac',
    digest: 'a'.repeat(64),
    path: sourcePath,
    size: accepted.byteLength,
    bodySha256: sha256(accepted)
  }

  const cases: Array<{
    name: string
    value: Buffer
    message: RegExp
  }> = [
    {
      name: 'grew',
      value: Buffer.from('accepted!'),
      message: /pack source exceeded its size/
    },
    {
      name: 'shrunk',
      value: Buffer.from('accept'),
      message: /pack source ended early/
    },
    {
      name: 'same-size content mutation',
      value: Buffer.from('reject!!'),
      message: /pack source SHA-256 changed after acceptance/
    }
  ]

  for (let index = 0; index < cases.length; index += 1) {
    const scenario = cases[index]
    assert.ok(scenario)
    await writeFile(sourcePath, scenario.value, {mode: 0o600})
    await assert.rejects(
      writer.seal([base], BigInt(index)),
      scenario.message,
      scenario.name
    )
    assert.deepEqual(await readdir(packs), [])
  }
})

test('PackWriter removes a renamed pack when cache-key construction fails', async t => {
  const root = await temporaryDirectory(t)
  const source = Buffer.from('valid source')
  const sourcePath = path.join(root, 'source')
  const packs = path.join(root, 'packs')
  await writeFile(sourcePath, source, {mode: 0o600})
  const writer = new PackWriter({
    directory: packs,
    namespace: 'late-failure-test',
    runId: 'not-an-integer',
    jobHash: '1234567890abcdef'
  })

  await assert.rejects(
    writer.seal(
      [
        {
          kind: 'cas',
          digest: sha256(source),
          path: sourcePath,
          size: source.byteLength,
          bodySha256: sha256(source)
        }
      ],
      1n
    ),
    /run ID must be an unsigned integer/
  )
  assert.deepEqual(await readdir(packs), [])
})

test('PackWriter aborts an active seal and removes its building file', async t => {
  const root = await temporaryDirectory(t)
  const sourcePath = path.join(root, 'large-source')
  const packs = path.join(root, 'packs')
  const sourceSize = 64 * MEBIBYTE
  const chunk = Buffer.alloc(MEBIBYTE)
  const bodyHash = createHash('sha256')
  for (let offset = 0; offset < sourceSize; offset += chunk.length) {
    bodyHash.update(chunk)
  }
  const largeDigest = bodyHash.digest('hex')
  const source = await open(sourcePath, 'wx', 0o600)
  try {
    await source.truncate(sourceSize)
  } finally {
    await source.close()
  }

  const writer = new PackWriter({
    directory: packs,
    namespace: 'abort-test',
    runId: '2',
    jobHash: 'fedcba0987654321'
  })
  const controller = new AbortController()
  const sealing = writer.seal(
    [
      {
        kind: 'cas',
        digest: largeDigest,
        path: sourcePath,
        size: sourceSize,
        bodySha256: largeDigest
      }
    ],
    9n,
    controller.signal
  )
  const buildingPath = path.join(packs, '000009.building')
  const observationDeadline = Date.now() + 5000
  let observedBytes = 0
  while (Date.now() < observationDeadline) {
    observedBytes = await stat(buildingPath)
      .then(value => value.size)
      .catch(() => 0)
    if (observedBytes > 0) break
    await new Promise(resolve => setImmediate(resolve))
  }
  assert.ok(
    observedBytes > 0,
    'the seal should begin copying before cancellation'
  )
  controller.abort()

  await assert.rejects(sealing, /pack creation aborted/)
  assert.deepEqual(await readdir(packs), [])
  assert.equal((await stat(sourcePath)).size, sourceSize)
})
