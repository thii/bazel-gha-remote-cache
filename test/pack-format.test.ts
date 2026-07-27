import assert from 'node:assert/strict'
import {createHash} from 'node:crypto'
import {test} from 'node:test'
import {
  PACK_BLOOM_BYTES,
  PACK_BLOOM_HASHES,
  PACK_CACHE_KEY_MAX_LENGTH,
  PACK_CACHE_VERSION,
  PACK_FORMAT_VERSION,
  PACK_INDEX_ENTRY_SIZE,
  PACK_INDEX_HEADER_SIZE,
  PACK_MAGIC,
  PACK_TRAILER_SIZE,
  PackFormatError,
  buildPack,
  createPackBloom,
  createPackCacheKey,
  createPackIndexEntry,
  createPackTrailer,
  decodePackBloom,
  decodePackIndex,
  decodePackTrailer,
  encodePackBloom,
  encodePackIndex,
  encodePackTrailer,
  entriesInPayloadOrder,
  findPackIndexEntry,
  packBloomMightContain,
  packCacheKeyPrefix,
  packIndexRange,
  packPayloadRange,
  packTrailerRange,
  parsePack,
  parsePackCacheKey,
  parsePackIndex,
  readPackPayload,
  tryParsePackCacheKey,
  validatePackLayout,
  verifyPackPayload,
  verifyPackPayloadSha256,
  type PackIndexEntry
} from '../src/pack-format.js'

function sha256(value: Uint8Array | string): Buffer {
  return createHash('sha256').update(value).digest()
}

function hex(value: Uint8Array): string {
  return Buffer.from(value).toString('hex')
}

function streamedEntries(): PackIndexEntry[] {
  const casPayload = Buffer.from('streamed CAS')
  const acPayload = Buffer.from('streamed AC')
  return [
    createPackIndexEntry({
      kind: 'cas',
      ordinal: 0,
      digest: sha256(casPayload),
      offset: 0,
      length: casPayload.length,
      bodySha256: sha256(casPayload)
    }),
    createPackIndexEntry({
      kind: 'ac',
      ordinal: 1,
      digest: 'f'.repeat(64),
      offset: casPayload.length,
      length: acPayload.length,
      bodySha256: sha256(acPayload)
    })
  ]
}

test('pack constants describe pack-v1 without compression', () => {
  assert.equal(PACK_MAGIC, 'BRC2PACK')
  assert.equal(PACK_FORMAT_VERSION, 1)
  assert.equal(PACK_CACHE_VERSION, 'bazel-remote-pack-v1-raw-sha256')
  assert.equal(PACK_TRAILER_SIZE, 64)
  assert.equal(PACK_INDEX_HEADER_SIZE, 16)
  assert.equal(PACK_INDEX_ENTRY_SIZE, 96)
  assert.equal(PACK_BLOOM_BYTES, 256)
  assert.equal(PACK_BLOOM_HASHES, 5)
})

test('buildPack preserves mixed payload order and emits a deterministic sorted index', () => {
  const firstCas = Buffer.from('first CAS')
  const action = Buffer.from('opaque ActionResult')
  const emptyCas = Buffer.alloc(0)
  const records = [
    {kind: 'cas' as const, digest: hex(sha256(firstCas)), payload: firstCas},
    {kind: 'ac' as const, digest: 'f'.repeat(64), payload: action},
    {kind: 'cas' as const, digest: hex(sha256(emptyCas)), payload: emptyCas}
  ]
  const verified: number[] = []
  const built = buildPack(records, {
    onCasDigestVerified: ({entry}) => verified.push(entry.ordinal)
  })
  const second = buildPack(records)

  assert.deepEqual(built.bytes, second.bytes)
  assert.deepEqual(verified, [0, 2])
  assert.deepEqual(
    built.entries.map(entry => `${entry.kind}:${hex(entry.digest)}`),
    [...built.entries]
      .sort((left, right) => {
        const leftKey = `${left.kind}:${hex(left.digest)}`
        const rightKey = `${right.kind}:${hex(right.digest)}`
        return leftKey.localeCompare(rightKey)
      })
      .map(entry => `${entry.kind}:${hex(entry.digest)}`)
  )
  assert.deepEqual(
    entriesInPayloadOrder(built.entries).map(entry => entry.ordinal),
    [0, 1, 2]
  )

  const payloadLength = firstCas.length + action.length
  assert.deepEqual(
    Buffer.from(built.bytes.subarray(0, payloadLength)),
    Buffer.concat([firstCas, action, emptyCas])
  )
  assert.equal(built.trailer.indexOffset, BigInt(payloadLength))
  assert.equal(
    built.bytes.byteLength,
    payloadLength +
      PACK_INDEX_HEADER_SIZE +
      records.length * PACK_INDEX_ENTRY_SIZE +
      PACK_TRAILER_SIZE
  )

  const parsed = parsePack(built.bytes)
  assert.deepEqual(parsed.entries, built.entries)
  for (const record of records) {
    const entry = findPackIndexEntry(parsed.entries, record.kind, record.digest)
    assert.ok(entry)
    assert.deepEqual(readPackPayload(built.bytes, entry), record.payload)
    assert.equal(
      packBloomMightContain(built.bloom, record.kind, record.digest),
      true
    )
  }
})

test('index and trailer primitives support a streaming writer and range reader', () => {
  const entries = streamedEntries()
  const payloadLength = entries.reduce(
    (total, entry) => total + entry.length,
    0n
  )
  const index = encodePackIndex(entries)
  const trailer = createPackTrailer(payloadLength, index)
  const trailerBytes = encodePackTrailer(trailer)

  assert.equal(index.byteLength, PACK_INDEX_HEADER_SIZE + 2 * 96)
  assert.equal(trailerBytes.byteLength, PACK_TRAILER_SIZE)
  assert.deepEqual(decodePackTrailer(trailerBytes), trailer)
  assert.deepEqual(parsePackIndex(index, trailer), decodePackIndex(index))
  assert.deepEqual(packIndexRange(trailer), {
    offset: payloadLength,
    length: BigInt(index.byteLength)
  })
  assert.deepEqual(
    packTrailerRange(payloadLength + BigInt(index.length) + 64n),
    {
      offset: payloadLength + BigInt(index.length),
      length: 64n
    }
  )
  assert.deepEqual(packPayloadRange(entries[1]!), {
    offset: entries[1]!.offset,
    length: entries[1]!.length
  })
  validatePackLayout(trailer, payloadLength + BigInt(index.length) + 64n)
  assert.throws(
    () => validatePackLayout(trailer, payloadLength + BigInt(index.length)),
    /pack size does not match/
  )
})

test('body hashes and CAS digests are verified independently', () => {
  const casPayload = Buffer.from('verified CAS')
  const cas = createPackIndexEntry({
    kind: 'cas',
    ordinal: 0,
    digest: sha256(casPayload),
    offset: 0,
    length: casPayload.length,
    bodySha256: sha256(casPayload)
  })
  let hookCalls = 0
  verifyPackPayload(cas, casPayload, {
    onCasDigestVerified: verification => {
      hookCalls += 1
      assert.deepEqual(verification.expectedDigest, sha256(casPayload))
      assert.deepEqual(verification.actualBodySha256, sha256(casPayload))
    }
  })
  verifyPackPayloadSha256(cas, sha256(casPayload))
  assert.equal(hookCalls, 1)

  assert.throws(
    () => verifyPackPayload(cas, Buffer.from('same length!')),
    /body SHA-256 verification failed/
  )
  assert.throws(
    () => verifyPackPayload(cas, Buffer.alloc(1)),
    /payload length is invalid/
  )
  assert.throws(
    () =>
      createPackIndexEntry({
        kind: 'cas',
        ordinal: 0,
        digest: '0'.repeat(64),
        offset: 0,
        length: casPayload.length,
        bodySha256: sha256(casPayload)
      }),
    /CAS digest does not match/
  )
})

test('pack parsing rejects trailer, index, ordering, and payload corruption', () => {
  const casPayload = Buffer.from('cas payload')
  const action = Buffer.from('action payload')
  const built = buildPack([
    {kind: 'cas', digest: sha256(casPayload), payload: casPayload},
    {kind: 'ac', digest: '1'.repeat(64), payload: action}
  ])

  const badMagic = Buffer.from(built.bytes)
  badMagic[badMagic.length - PACK_TRAILER_SIZE] ^= 1
  assert.throws(() => parsePack(badMagic), /trailer magic is invalid/)

  const badBody = Buffer.from(built.bytes)
  badBody[0] ^= 1
  const casEntry = findPackIndexEntry(built.entries, 'cas', sha256(casPayload))
  assert.ok(casEntry)
  assert.throws(
    () => readPackPayload(badBody, casEntry),
    /body SHA-256 verification failed/
  )

  const badIndex = Buffer.from(built.bytes)
  const indexByte = Number(built.trailer.indexOffset) + PACK_INDEX_HEADER_SIZE
  badIndex[indexByte] ^= 1
  assert.throws(() => parsePack(badIndex), /index SHA-256 verification failed/)

  const index = Buffer.from(
    built.bytes.subarray(
      Number(built.trailer.indexOffset),
      built.bytes.length - PACK_TRAILER_SIZE
    )
  )
  const firstRecord = Buffer.from(
    index.subarray(
      PACK_INDEX_HEADER_SIZE,
      PACK_INDEX_HEADER_SIZE + PACK_INDEX_ENTRY_SIZE
    )
  )
  const secondRecord = Buffer.from(
    index.subarray(
      PACK_INDEX_HEADER_SIZE + PACK_INDEX_ENTRY_SIZE,
      PACK_INDEX_HEADER_SIZE + 2 * PACK_INDEX_ENTRY_SIZE
    )
  )
  firstRecord.copy(index, PACK_INDEX_HEADER_SIZE + PACK_INDEX_ENTRY_SIZE)
  secondRecord.copy(index, PACK_INDEX_HEADER_SIZE)
  assert.throws(() => decodePackIndex(index), /not strictly sorted/)
})

test('index encoding rejects duplicates and non-contiguous payload ranges', () => {
  const entries = streamedEntries()
  assert.throws(
    () => encodePackIndex([entries[0]!, entries[0]!]),
    /ordinals must be unique/
  )
  const body = sha256('body')
  const gap = createPackIndexEntry({
    kind: 'ac',
    ordinal: 0,
    digest: 'a'.repeat(64),
    offset: 1,
    length: 4,
    bodySha256: body
  })
  assert.throws(() => encodePackIndex([gap]), /ranges must be contiguous/)
  assert.throws(
    () =>
      createPackIndexEntry({
        kind: 'ac',
        ordinal: 0,
        digest: 'A'.repeat(64),
        offset: 0,
        length: 0,
        bodySha256: sha256('')
      }),
    /lowercase SHA-256/
  )
})

test('the fixed Bloom filter is deterministic, canonical, and has no false negatives', () => {
  const identities = [
    {kind: 'cas' as const, digest: sha256('cas')},
    {kind: 'ac' as const, digest: '2'.repeat(64)},
    {kind: 'cas' as const, digest: sha256('another cas')}
  ]
  const bloom = createPackBloom(identities)
  const encoded = encodePackBloom(bloom)

  assert.equal(bloom.byteLength, 256)
  assert.deepEqual(createPackBloom(identities), bloom)
  assert.equal(encoded.length, 342)
  assert.equal(encoded.includes('='), false)
  assert.deepEqual(decodePackBloom(encoded), bloom)
  for (const identity of identities) {
    assert.equal(
      packBloomMightContain(bloom, identity.kind, identity.digest),
      true
    )
  }

  const negatives = Array.from({length: 50}, (_unused, index) =>
    sha256(`definite negative ${index}`)
  )
  assert.ok(
    negatives.some(digest => !packBloomMightContain(bloom, 'cas', digest))
  )
  assert.throws(() => decodePackBloom(`${encoded.slice(0, -1)}=`), /invalid/)
  assert.throws(() => createPackBloom([{kind: 'cas', digest: 'bad'}]), /digest/)
})

test('cache keys round-trip their 2048-bit Bloom filter within 512 characters', () => {
  const bloom = createPackBloom([
    {kind: 'cas', digest: sha256('cache key object')}
  ])
  const key = createPackCacheKey({
    namespaceHash: 'a81f7e9d',
    runId: 73_400_291,
    jobHash: '18ca73ff',
    sequence: 3,
    bloom,
    packId: 'd33f9c1e8a7b0000'
  })

  assert.equal(key.startsWith(packCacheKeyPrefix('a81f7e9d')), true)
  assert.ok(key.length <= PACK_CACHE_KEY_MAX_LENGTH)
  const parsed = parsePackCacheKey(key)
  assert.equal(parsed.namespaceHash, 'a81f7e9d')
  assert.equal(parsed.runId, '73400291')
  assert.equal(parsed.jobHash, '18ca73ff')
  assert.equal(parsed.sequence, 3n)
  assert.equal(parsed.packId, 'd33f9c1e8a7b0000')
  assert.deepEqual(parsed.bloom, bloom)
  assert.equal(
    packBloomMightContain(parsed.bloom, 'cas', sha256('cache key object')),
    true
  )
  assert.deepEqual(tryParsePackCacheKey(key), parsed)
  assert.equal(tryParsePackCacheKey('brc2-not-a-pack'), undefined)

  assert.throws(
    () =>
      createPackCacheKey({
        namespaceHash: 'a'.repeat(64),
        runId: '9'.repeat(20),
        jobHash: 'b'.repeat(64),
        sequence: '8'.repeat(20),
        bloom,
        packId: 'c'.repeat(64)
      }),
    /generated pack cache key is invalid/
  )
  assert.throws(
    () =>
      createPackCacheKey({
        namespaceHash: 'not-a-hash',
        runId: 1,
        jobHash: '18ca73ff',
        sequence: 0,
        bloom,
        packId: 'd33f9c1e8a7b0000'
      }),
    /namespace hash/
  )
})

test('fixed trailer fields detect unsupported versions and flags', () => {
  const index = encodePackIndex(streamedEntries())
  const trailer = Buffer.from(encodePackTrailer(createPackTrailer(22, index)))

  trailer.writeUInt32BE(2, 8)
  assert.throws(() => decodePackTrailer(trailer), /version is unsupported/)

  trailer.writeUInt32BE(PACK_FORMAT_VERSION, 8)
  trailer[12] = 1
  assert.throws(() => decodePackTrailer(trailer), /unsupported flags/)
  assert.throws(
    () => decodePackTrailer(trailer.subarray(1)),
    /exactly 64 bytes/
  )
  assert.ok(new PackFormatError('format').name === 'PackFormatError')
})
