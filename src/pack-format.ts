import {createHash, timingSafeEqual} from 'node:crypto'
import type {CacheKind} from './model.js'

/**
 * Pack v1 is entirely big-endian and has no compression or per-record framing:
 *
 *   payload bytes in ordinal order
 *   16-byte index header + 96-byte records sorted by (kind, digest)
 *   64-byte trailer containing the index range and its SHA-256
 *
 * Ordinals preserve acceptance order even for zero-length objects. Reserved
 * bytes must remain zero so future formats cannot be misread as pack-v1.
 */
export const PACK_MAGIC = 'BRC2PACK'
export const PACK_FORMAT_VERSION = 1
export const PACK_CACHE_VERSION = 'bazel-remote-pack-v1-raw-sha256'
export const PACK_TRAILER_SIZE = 64
export const PACK_INDEX_HEADER_SIZE = 16
export const PACK_INDEX_ENTRY_SIZE = 96
export const PACK_BLOOM_BITS = 2048
export const PACK_BLOOM_BYTES = PACK_BLOOM_BITS / 8
export const PACK_BLOOM_HASHES = 5
export const PACK_CACHE_KEY_MAX_LENGTH = 512

const PACK_INDEX_MAGIC = 'BRC2IDX1'
const SHA256_BYTES = 32
const MAX_UINT32 = 0xffff_ffff
const MAX_UINT64 = 0xffff_ffff_ffff_ffffn
const MAX_KEY_INTEGER_DIGITS = 20
const PACK_BLOOM_ENCODED_LENGTH = 342
const PACK_MAGIC_BYTES = Buffer.from(PACK_MAGIC, 'ascii')
const PACK_INDEX_MAGIC_BYTES = Buffer.from(PACK_INDEX_MAGIC, 'ascii')
const HEX_DIGEST = /^[0-9a-f]{64}$/
const HASHED_KEY_COMPONENT = /^[0-9a-f]{8,64}$/
const PACK_ID = /^[0-9a-f]{16,64}$/
const BASE64URL = /^[A-Za-z0-9_-]+$/

export class PackFormatError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'PackFormatError'
  }
}

export interface PackRecord {
  readonly kind: CacheKind
  readonly digest: string | Uint8Array
  readonly payload: Uint8Array
}

export interface PackObjectIdentity {
  readonly kind: CacheKind
  readonly digest: string | Uint8Array
}

export interface PackIndexEntry {
  readonly kind: CacheKind
  readonly ordinal: number
  readonly digest: Uint8Array
  readonly offset: bigint
  readonly length: bigint
  readonly bodySha256: Uint8Array
}

export interface PackIndexEntryInput {
  readonly kind: CacheKind
  readonly ordinal: number
  readonly digest: string | Uint8Array
  readonly offset: number | bigint
  readonly length: number | bigint
  readonly bodySha256: string | Uint8Array
}

export interface PackTrailer {
  readonly formatVersion: number
  readonly indexOffset: bigint
  readonly indexLength: bigint
  readonly indexSha256: Uint8Array
}

export interface PackByteRange {
  readonly offset: bigint
  readonly length: bigint
}

export interface BuiltPack {
  /** Payloads in input order, followed by the sorted index and trailer. */
  readonly bytes: Uint8Array
  /** Entries in index sort order. Use entriesInPayloadOrder to recover input order. */
  readonly entries: readonly PackIndexEntry[]
  readonly trailer: PackTrailer
  readonly bloom: Uint8Array
}

export interface ParsedPack {
  readonly entries: readonly PackIndexEntry[]
  readonly trailer: PackTrailer
}

export interface CasDigestVerification {
  readonly entry: PackIndexEntry
  readonly expectedDigest: Uint8Array
  readonly actualBodySha256: Uint8Array
}

export type CasDigestVerificationHook = (
  verification: CasDigestVerification
) => void

export interface PackVerificationOptions {
  /** Called only after the built-in SHA-256 CAS check succeeds. */
  readonly onCasDigestVerified?: CasDigestVerificationHook
}

export interface PackCacheKeyInput {
  readonly namespaceHash: string
  readonly runId: string | number | bigint
  readonly jobHash: string
  readonly sequence: string | number | bigint
  readonly bloom: Uint8Array
  readonly packId: string
}

export interface ParsedPackCacheKey {
  readonly namespaceHash: string
  readonly runId: string
  readonly jobHash: string
  readonly sequence: bigint
  readonly bloom: Uint8Array
  readonly packId: string
}

function sha256(value: Uint8Array): Buffer {
  return createHash('sha256').update(value).digest()
}

function validateKind(kind: string): asserts kind is CacheKind {
  if (kind !== 'ac' && kind !== 'cas') {
    throw new PackFormatError(`unsupported pack object kind: ${kind}`)
  }
}

function kindCode(kind: CacheKind): number {
  return kind === 'ac' ? 0 : 1
}

function kindFromCode(code: number): CacheKind {
  if (code === 0) return 'ac'
  if (code === 1) return 'cas'
  throw new PackFormatError(`unsupported pack object kind code: ${code}`)
}

function bytesEqual(left: Uint8Array, right: Uint8Array): boolean {
  return (
    left.byteLength === right.byteLength &&
    timingSafeEqual(Buffer.from(left), Buffer.from(right))
  )
}

function digestBytes(value: string | Uint8Array, field: string): Buffer {
  if (typeof value === 'string') {
    if (!HEX_DIGEST.test(value)) {
      throw new PackFormatError(`${field} must be a lowercase SHA-256 digest`)
    }
    return Buffer.from(value, 'hex')
  }
  if (value.byteLength !== SHA256_BYTES) {
    throw new PackFormatError(`${field} must contain 32 bytes`)
  }
  return Buffer.from(value)
}

function unsignedBigInt(
  value: number | bigint,
  field: string,
  maximum = MAX_UINT64
): bigint {
  if (typeof value === 'number') {
    if (!Number.isSafeInteger(value) || value < 0) {
      throw new PackFormatError(`${field} must be a non-negative safe integer`)
    }
    value = BigInt(value)
  }
  if (value < 0n || value > maximum) {
    throw new PackFormatError(`${field} is outside the supported range`)
  }
  return value
}

function safeNumber(value: bigint, field: string): number {
  if (value > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new PackFormatError(`${field} exceeds the JavaScript safe range`)
  }
  return Number(value)
}

function assertZero(bytes: Uint8Array, field: string): void {
  if (bytes.some(value => value !== 0)) {
    throw new PackFormatError(`${field} contains unsupported flags`)
  }
}

function compareEntries(left: PackIndexEntry, right: PackIndexEntry): number {
  const kindDifference = kindCode(left.kind) - kindCode(right.kind)
  if (kindDifference !== 0) return kindDifference
  return Buffer.compare(Buffer.from(left.digest), Buffer.from(right.digest))
}

function cloneEntry(entry: PackIndexEntry): PackIndexEntry {
  return Object.freeze({
    kind: entry.kind,
    ordinal: entry.ordinal,
    digest: Buffer.from(entry.digest),
    offset: entry.offset,
    length: entry.length,
    bodySha256: Buffer.from(entry.bodySha256)
  })
}

function validateEntries(entries: readonly PackIndexEntry[]): void {
  if (entries.length === 0) {
    throw new PackFormatError('a pack must contain at least one object')
  }
  if (entries.length > MAX_UINT32) {
    throw new PackFormatError('pack index contains too many objects')
  }

  const identities = new Set<string>()
  const byOrdinal: Array<PackIndexEntry | undefined> = new Array(entries.length)
  for (const entry of entries) {
    validateKind(entry.kind)
    if (
      !Number.isSafeInteger(entry.ordinal) ||
      entry.ordinal < 0 ||
      entry.ordinal >= entries.length
    ) {
      throw new PackFormatError('pack ordinals must be contiguous from zero')
    }
    if (byOrdinal[entry.ordinal] !== undefined) {
      throw new PackFormatError('pack ordinals must be unique')
    }
    if (entry.digest.byteLength !== SHA256_BYTES) {
      throw new PackFormatError('pack digest must contain 32 bytes')
    }
    if (entry.bodySha256.byteLength !== SHA256_BYTES) {
      throw new PackFormatError('pack body SHA-256 must contain 32 bytes')
    }
    unsignedBigInt(entry.offset, 'pack object offset')
    unsignedBigInt(entry.length, 'pack object length')
    if (entry.offset + entry.length > MAX_UINT64) {
      throw new PackFormatError('pack object range overflows uint64')
    }
    if (entry.kind === 'cas' && !bytesEqual(entry.digest, entry.bodySha256)) {
      throw new PackFormatError('CAS digest does not match body SHA-256')
    }
    const identity = `${entry.kind}:${Buffer.from(entry.digest).toString('hex')}`
    if (identities.has(identity)) {
      throw new PackFormatError('pack contains a duplicate object identity')
    }
    identities.add(identity)
    byOrdinal[entry.ordinal] = entry
  }

  let expectedOffset = 0n
  for (const entry of byOrdinal) {
    if (entry === undefined) {
      throw new PackFormatError('pack ordinals must be contiguous from zero')
    }
    if (entry.offset !== expectedOffset) {
      throw new PackFormatError(
        'pack payload ranges must be contiguous in ordinal order'
      )
    }
    expectedOffset += entry.length
  }
}

export function createPackIndexEntry(
  input: PackIndexEntryInput
): PackIndexEntry {
  validateKind(input.kind)
  if (!Number.isSafeInteger(input.ordinal) || input.ordinal < 0) {
    throw new PackFormatError('pack ordinal must be a non-negative integer')
  }
  const entry: PackIndexEntry = {
    kind: input.kind,
    ordinal: input.ordinal,
    digest: digestBytes(input.digest, 'pack digest'),
    offset: unsignedBigInt(input.offset, 'pack object offset'),
    length: unsignedBigInt(input.length, 'pack object length'),
    bodySha256: digestBytes(input.bodySha256, 'pack body SHA-256')
  }
  if (entry.offset + entry.length > MAX_UINT64) {
    throw new PackFormatError('pack object range overflows uint64')
  }
  if (entry.kind === 'cas' && !bytesEqual(entry.digest, entry.bodySha256)) {
    throw new PackFormatError('CAS digest does not match body SHA-256')
  }
  return cloneEntry(entry)
}

export function sortPackIndexEntries(
  entries: readonly PackIndexEntry[]
): readonly PackIndexEntry[] {
  validateEntries(entries)
  return Object.freeze(entries.map(cloneEntry).sort(compareEntries))
}

export function entriesInPayloadOrder(
  entries: readonly PackIndexEntry[]
): readonly PackIndexEntry[] {
  validateEntries(entries)
  return Object.freeze(
    entries.map(cloneEntry).sort((left, right) => left.ordinal - right.ordinal)
  )
}

/**
 * Encodes a deterministic index sorted by (kind, digest). The supplied entries
 * may be in payload order or index order.
 */
export function encodePackIndex(
  entries: readonly PackIndexEntry[]
): Uint8Array {
  const sorted = sortPackIndexEntries(entries)
  const size = PACK_INDEX_HEADER_SIZE + sorted.length * PACK_INDEX_ENTRY_SIZE
  const output = Buffer.alloc(size)
  PACK_INDEX_MAGIC_BYTES.copy(output, 0)
  output.writeUInt32BE(sorted.length, 8)
  output.writeUInt16BE(PACK_INDEX_ENTRY_SIZE, 12)

  for (let index = 0; index < sorted.length; index += 1) {
    const entry = sorted[index]
    if (entry === undefined) throw new PackFormatError('missing pack entry')
    const start = PACK_INDEX_HEADER_SIZE + index * PACK_INDEX_ENTRY_SIZE
    output.writeUInt8(kindCode(entry.kind), start)
    output.writeUInt32BE(entry.ordinal, start + 4)
    Buffer.from(entry.digest).copy(output, start + 8)
    output.writeBigUInt64BE(entry.offset, start + 40)
    output.writeBigUInt64BE(entry.length, start + 48)
    Buffer.from(entry.bodySha256).copy(output, start + 56)
  }
  return output
}

export function decodePackIndex(
  indexBytes: Uint8Array
): readonly PackIndexEntry[] {
  const input = Buffer.from(indexBytes)
  if (input.byteLength < PACK_INDEX_HEADER_SIZE) {
    throw new PackFormatError('pack index is truncated')
  }
  if (!bytesEqual(input.subarray(0, 8), PACK_INDEX_MAGIC_BYTES)) {
    throw new PackFormatError('pack index magic is invalid')
  }
  const count = input.readUInt32BE(8)
  const recordSize = input.readUInt16BE(12)
  if (recordSize !== PACK_INDEX_ENTRY_SIZE) {
    throw new PackFormatError('pack index record size is unsupported')
  }
  assertZero(input.subarray(14, 16), 'pack index header')
  const expectedLength = PACK_INDEX_HEADER_SIZE + count * PACK_INDEX_ENTRY_SIZE
  if (input.byteLength !== expectedLength) {
    throw new PackFormatError('pack index length does not match its header')
  }

  const entries: PackIndexEntry[] = []
  for (let index = 0; index < count; index += 1) {
    const start = PACK_INDEX_HEADER_SIZE + index * PACK_INDEX_ENTRY_SIZE
    assertZero(input.subarray(start + 1, start + 4), 'pack index record')
    assertZero(input.subarray(start + 88, start + 96), 'pack index record')
    entries.push(
      createPackIndexEntry({
        kind: kindFromCode(input.readUInt8(start)),
        ordinal: input.readUInt32BE(start + 4),
        digest: input.subarray(start + 8, start + 40),
        offset: input.readBigUInt64BE(start + 40),
        length: input.readBigUInt64BE(start + 48),
        bodySha256: input.subarray(start + 56, start + 88)
      })
    )
  }
  validateEntries(entries)
  for (let index = 1; index < entries.length; index += 1) {
    const previous = entries[index - 1]
    const current = entries[index]
    if (
      previous === undefined ||
      current === undefined ||
      compareEntries(previous, current) >= 0
    ) {
      throw new PackFormatError('pack index entries are not strictly sorted')
    }
  }
  return Object.freeze(entries)
}

export function createPackTrailer(
  indexOffset: number | bigint,
  indexBytes: Uint8Array
): PackTrailer {
  const offset = unsignedBigInt(indexOffset, 'pack index offset')
  const length = unsignedBigInt(indexBytes.byteLength, 'pack index length')
  if (offset + length > MAX_UINT64) {
    throw new PackFormatError('pack index range overflows uint64')
  }
  return Object.freeze({
    formatVersion: PACK_FORMAT_VERSION,
    indexOffset: offset,
    indexLength: length,
    indexSha256: sha256(indexBytes)
  })
}

export function encodePackTrailer(trailer: PackTrailer): Uint8Array {
  if (trailer.formatVersion !== PACK_FORMAT_VERSION) {
    throw new PackFormatError('pack format version is unsupported')
  }
  const indexOffset = unsignedBigInt(trailer.indexOffset, 'pack index offset')
  const indexLength = unsignedBigInt(trailer.indexLength, 'pack index length')
  if (indexOffset + indexLength > MAX_UINT64) {
    throw new PackFormatError('pack index range overflows uint64')
  }
  const indexSha256 = digestBytes(trailer.indexSha256, 'pack index SHA-256')
  const output = Buffer.alloc(PACK_TRAILER_SIZE)
  PACK_MAGIC_BYTES.copy(output, 0)
  output.writeUInt32BE(PACK_FORMAT_VERSION, 8)
  output.writeBigUInt64BE(indexOffset, 16)
  output.writeBigUInt64BE(indexLength, 24)
  indexSha256.copy(output, 32)
  return output
}

export function decodePackTrailer(trailerBytes: Uint8Array): PackTrailer {
  const input = Buffer.from(trailerBytes)
  if (input.byteLength !== PACK_TRAILER_SIZE) {
    throw new PackFormatError('pack trailer must contain exactly 64 bytes')
  }
  if (!bytesEqual(input.subarray(0, 8), PACK_MAGIC_BYTES)) {
    throw new PackFormatError('pack trailer magic is invalid')
  }
  const formatVersion = input.readUInt32BE(8)
  if (formatVersion !== PACK_FORMAT_VERSION) {
    throw new PackFormatError('pack format version is unsupported')
  }
  assertZero(input.subarray(12, 16), 'pack trailer')
  const indexOffset = input.readBigUInt64BE(16)
  const indexLength = input.readBigUInt64BE(24)
  if (indexOffset + indexLength > MAX_UINT64) {
    throw new PackFormatError('pack index range overflows uint64')
  }
  return Object.freeze({
    formatVersion,
    indexOffset,
    indexLength,
    indexSha256: Buffer.from(input.subarray(32, 64))
  })
}

export function packTrailerRange(packSize: number | bigint): PackByteRange {
  const size = unsignedBigInt(packSize, 'pack size')
  if (size < BigInt(PACK_TRAILER_SIZE)) {
    throw new PackFormatError('pack is too small to contain a trailer')
  }
  return Object.freeze({
    offset: size - BigInt(PACK_TRAILER_SIZE),
    length: BigInt(PACK_TRAILER_SIZE)
  })
}

export function packIndexRange(trailer: PackTrailer): PackByteRange {
  return Object.freeze({
    offset: trailer.indexOffset,
    length: trailer.indexLength
  })
}

export function packPayloadRange(entry: PackIndexEntry): PackByteRange {
  return Object.freeze({offset: entry.offset, length: entry.length})
}

export function validatePackLayout(
  trailer: PackTrailer,
  packSize: number | bigint
): void {
  const size = unsignedBigInt(packSize, 'pack size')
  const expectedSize =
    trailer.indexOffset + trailer.indexLength + BigInt(PACK_TRAILER_SIZE)
  if (expectedSize !== size) {
    throw new PackFormatError('pack size does not match its trailer')
  }
}

/** Verifies the trailer hash before decoding a separately range-read index. */
export function parsePackIndex(
  indexBytes: Uint8Array,
  trailer: PackTrailer
): readonly PackIndexEntry[] {
  if (BigInt(indexBytes.byteLength) !== trailer.indexLength) {
    throw new PackFormatError('range-read pack index has the wrong length')
  }
  const actualSha256 = sha256(indexBytes)
  if (!bytesEqual(actualSha256, trailer.indexSha256)) {
    throw new PackFormatError('pack index SHA-256 verification failed')
  }
  const entries = decodePackIndex(indexBytes)
  const payloadEntries = entriesInPayloadOrder(entries)
  const last = payloadEntries.at(-1)
  const payloadLength = last === undefined ? 0n : last.offset + last.length
  if (payloadLength !== trailer.indexOffset) {
    throw new PackFormatError('pack payload length does not match index offset')
  }
  return entries
}

export function parsePack(packBytes: Uint8Array): ParsedPack {
  const pack = Buffer.from(packBytes)
  const trailerRange = packTrailerRange(pack.byteLength)
  const trailerOffset = safeNumber(trailerRange.offset, 'pack trailer offset')
  const trailer = decodePackTrailer(pack.subarray(trailerOffset))
  validatePackLayout(trailer, pack.byteLength)
  const indexOffset = safeNumber(trailer.indexOffset, 'pack index offset')
  const indexLength = safeNumber(trailer.indexLength, 'pack index length')
  const entries = parsePackIndex(
    pack.subarray(indexOffset, indexOffset + indexLength),
    trailer
  )
  return Object.freeze({entries, trailer})
}

export function findPackIndexEntry(
  entries: readonly PackIndexEntry[],
  kind: CacheKind,
  digest: string | Uint8Array
): PackIndexEntry | undefined {
  validateKind(kind)
  const wanted = digestBytes(digest, 'requested digest')
  const target: PackIndexEntry = {
    kind,
    ordinal: 0,
    digest: wanted,
    offset: 0n,
    length: 0n,
    bodySha256: wanted
  }
  let low = 0
  let high = entries.length - 1
  while (low <= high) {
    const middle = low + Math.floor((high - low) / 2)
    const entry = entries[middle]
    if (entry === undefined) return undefined
    const comparison = compareEntries(entry, target)
    if (comparison === 0) return entry
    if (comparison < 0) low = middle + 1
    else high = middle - 1
  }
  return undefined
}

/**
 * Verifies a streaming hash against the index. CAS records receive the extra
 * requested-digest check before the optional observation hook is invoked.
 */
export function verifyPackPayloadSha256(
  entry: PackIndexEntry,
  actualBodySha256: string | Uint8Array,
  options: PackVerificationOptions = {}
): void {
  const actual = digestBytes(actualBodySha256, 'actual body SHA-256')
  if (!bytesEqual(entry.bodySha256, actual)) {
    throw new PackFormatError('pack object body SHA-256 verification failed')
  }
  if (entry.kind === 'cas') {
    if (!bytesEqual(entry.digest, actual)) {
      throw new PackFormatError('CAS digest verification failed')
    }
    options.onCasDigestVerified?.({
      entry,
      expectedDigest: Buffer.from(entry.digest),
      actualBodySha256: Buffer.from(actual)
    })
  }
}

export function verifyPackPayload(
  entry: PackIndexEntry,
  payload: Uint8Array,
  options: PackVerificationOptions = {}
): void {
  if (BigInt(payload.byteLength) !== entry.length) {
    throw new PackFormatError('pack object payload length is invalid')
  }
  verifyPackPayloadSha256(entry, sha256(payload), options)
}

export function readPackPayload(
  packBytes: Uint8Array,
  entry: PackIndexEntry,
  options: PackVerificationOptions = {}
): Uint8Array {
  const pack = Buffer.from(packBytes)
  const offset = safeNumber(entry.offset, 'pack object offset')
  const length = safeNumber(entry.length, 'pack object length')
  if (offset + length > pack.byteLength) {
    throw new PackFormatError('pack object range is outside the pack')
  }
  const payload = Buffer.from(pack.subarray(offset, offset + length))
  verifyPackPayload(entry, payload, options)
  return payload
}

function bloomHashPositions(
  kind: CacheKind,
  digest: Uint8Array
): readonly number[] {
  const positions: number[] = []
  const identity = Buffer.concat([
    Buffer.from([kindCode(kind)]),
    Buffer.from(digest)
  ])
  for (let index = 0; index < PACK_BLOOM_HASHES; index += 1) {
    const hash = createHash('sha256')
      .update('brc2-bloom-v1\0')
      .update(Buffer.from([index]))
      .update(identity)
      .digest()
    positions.push(hash.readUInt16BE(0) % PACK_BLOOM_BITS)
  }
  return positions
}

function validateBloom(bloom: Uint8Array): Buffer {
  if (bloom.byteLength !== PACK_BLOOM_BYTES) {
    throw new PackFormatError(
      `pack Bloom filter must contain ${PACK_BLOOM_BYTES} bytes`
    )
  }
  return Buffer.from(bloom)
}

export function createPackBloom(
  identities: readonly PackObjectIdentity[]
): Uint8Array {
  const bloom = Buffer.alloc(PACK_BLOOM_BYTES)
  for (const identity of identities) {
    validateKind(identity.kind)
    const digest = digestBytes(identity.digest, 'Bloom filter digest')
    for (const position of bloomHashPositions(identity.kind, digest)) {
      bloom[Math.floor(position / 8)]! |= 1 << position % 8
    }
  }
  return bloom
}

export function packBloomMightContain(
  bloom: Uint8Array,
  kind: CacheKind,
  digest: string | Uint8Array
): boolean {
  const filter = validateBloom(bloom)
  validateKind(kind)
  const digestValue = digestBytes(digest, 'Bloom filter digest')
  return bloomHashPositions(kind, digestValue).every(position => {
    const byte = filter[Math.floor(position / 8)]
    return byte !== undefined && (byte & (1 << position % 8)) !== 0
  })
}

export function encodePackBloom(bloom: Uint8Array): string {
  return validateBloom(bloom).toString('base64url')
}

export function decodePackBloom(encoded: string): Uint8Array {
  if (
    encoded.length !== PACK_BLOOM_ENCODED_LENGTH ||
    !BASE64URL.test(encoded)
  ) {
    throw new PackFormatError('pack Bloom filter encoding is invalid')
  }
  const decoded = Buffer.from(encoded, 'base64url')
  if (
    decoded.byteLength !== PACK_BLOOM_BYTES ||
    decoded.toString('base64url') !== encoded
  ) {
    throw new PackFormatError('pack Bloom filter encoding is not canonical')
  }
  return decoded
}

function validateHashedKeyComponent(value: string, field: string): string {
  if (!HASHED_KEY_COMPONENT.test(value)) {
    throw new PackFormatError(
      `${field} must be 8 to 64 lowercase hexadecimal characters`
    )
  }
  return value
}

function keyInteger(value: string | number | bigint, field: string): string {
  let parsed: bigint
  if (typeof value === 'string') {
    if (!/^(0|[1-9][0-9]*)$/.test(value)) {
      throw new PackFormatError(`${field} must be an unsigned integer`)
    }
    parsed = BigInt(value)
  } else if (typeof value === 'number') {
    if (!Number.isSafeInteger(value) || value < 0) {
      throw new PackFormatError(`${field} must be a non-negative safe integer`)
    }
    parsed = BigInt(value)
  } else {
    parsed = value
  }
  if (parsed < 0n) {
    throw new PackFormatError(`${field} must be an unsigned integer`)
  }
  const normalized = parsed.toString()
  if (normalized.length > MAX_KEY_INTEGER_DIGITS) {
    throw new PackFormatError(`${field} is too large for a pack cache key`)
  }
  return normalized
}

export function packCacheKeyPrefix(namespaceHash: string): string {
  return `brc2-${validateHashedKeyComponent(namespaceHash, 'namespace hash')}-pack-`
}

export function createPackCacheKey(input: PackCacheKeyInput): string {
  const prefix = packCacheKeyPrefix(input.namespaceHash)
  const runId = keyInteger(input.runId, 'run ID')
  const jobHash = validateHashedKeyComponent(input.jobHash, 'job hash')
  const sequence = keyInteger(input.sequence, 'pack sequence').padStart(6, '0')
  const bloom = encodePackBloom(input.bloom)
  if (!PACK_ID.test(input.packId)) {
    throw new PackFormatError(
      'pack ID must be 16 to 64 lowercase hexadecimal characters'
    )
  }
  const key = `${prefix}${runId}-${jobHash}-${sequence}-${bloom}-${input.packId}`
  if (key.length > PACK_CACHE_KEY_MAX_LENGTH || key.includes(',')) {
    throw new PackFormatError('generated pack cache key is invalid')
  }
  return key
}

export function parsePackCacheKey(key: string): ParsedPackCacheKey {
  if (key.length > PACK_CACHE_KEY_MAX_LENGTH || key.includes(',')) {
    throw new PackFormatError('pack cache key is invalid')
  }
  const match =
    /^brc2-([0-9a-f]{8,64})-pack-([0-9]{1,20})-([0-9a-f]{8,64})-([0-9]{6,20})-([A-Za-z0-9_-]{342})-([0-9a-f]{16,64})$/.exec(
      key
    )
  if (match === null) throw new PackFormatError('pack cache key is invalid')
  const [, namespaceHash, runId, jobHash, sequence, encodedBloom, packId] =
    match
  if (
    namespaceHash === undefined ||
    runId === undefined ||
    jobHash === undefined ||
    sequence === undefined ||
    encodedBloom === undefined ||
    packId === undefined
  ) {
    throw new PackFormatError('pack cache key is invalid')
  }
  if (keyInteger(runId, 'run ID') !== runId) {
    throw new PackFormatError('pack cache key run ID is not canonical')
  }
  const parsedSequence = BigInt(sequence)
  if (parsedSequence.toString().padStart(6, '0') !== sequence) {
    throw new PackFormatError('pack cache key sequence is not canonical')
  }
  return Object.freeze({
    namespaceHash,
    runId,
    jobHash,
    sequence: parsedSequence,
    bloom: decodePackBloom(encodedBloom),
    packId
  })
}

export function tryParsePackCacheKey(
  key: string
): ParsedPackCacheKey | undefined {
  try {
    return parsePackCacheKey(key)
  } catch (error) {
    if (error instanceof PackFormatError) return undefined
    throw error
  }
}

/** Convenience helper; streaming writers should use the public index primitives. */
export function buildPack(
  records: readonly PackRecord[],
  options: PackVerificationOptions = {}
): BuiltPack {
  if (records.length === 0) {
    throw new PackFormatError('a pack must contain at least one object')
  }
  const payloads: Buffer[] = []
  const entries: PackIndexEntry[] = []
  let offset = 0n
  for (let ordinal = 0; ordinal < records.length; ordinal += 1) {
    const record = records[ordinal]
    if (record === undefined) throw new PackFormatError('missing pack record')
    validateKind(record.kind)
    const payload = Buffer.from(record.payload)
    const bodySha256 = sha256(payload)
    const entry = createPackIndexEntry({
      kind: record.kind,
      ordinal,
      digest: record.digest,
      offset,
      length: payload.byteLength,
      bodySha256
    })
    verifyPackPayloadSha256(entry, bodySha256, options)
    payloads.push(payload)
    entries.push(entry)
    offset += BigInt(payload.byteLength)
  }
  const sortedEntries = sortPackIndexEntries(entries)
  const index = Buffer.from(encodePackIndex(sortedEntries))
  const trailer = createPackTrailer(offset, index)
  const bytes = Buffer.concat([
    ...payloads,
    index,
    Buffer.from(encodePackTrailer(trailer))
  ])
  return Object.freeze({
    bytes,
    entries: sortedEntries,
    trailer,
    bloom: createPackBloom(sortedEntries)
  })
}
