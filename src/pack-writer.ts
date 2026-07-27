import {createHash} from 'node:crypto'
import {mkdir, open, rename, rm, type FileHandle} from 'node:fs/promises'
import path from 'node:path'
import type {CacheKind} from './model.js'
import {
  PACK_CACHE_VERSION,
  createPackBloom,
  createPackCacheKey,
  createPackIndexEntry,
  createPackTrailer,
  encodePackIndex,
  encodePackTrailer,
  type PackIndexEntry
} from './pack-format.js'

export interface PackSourceRecord {
  kind: CacheKind
  digest: string
  path: string
  size: number
  bodySha256: string
}

export interface SealedPack {
  key: string
  version: string
  path: string
  size: number
  sequence: bigint
  entries: readonly PackIndexEntry[]
  objectCount: number
  payloadBytes: number
  dispose(): Promise<void>
}

export interface PackWriterOptions {
  directory: string
  namespace: string
  runId: string
  jobHash: string
}

const COPY_BUFFER_BYTES = 1024 * 1024

async function writeAll(
  file: FileHandle,
  value: Uint8Array,
  hash: ReturnType<typeof createHash>,
  signal?: AbortSignal
): Promise<void> {
  let offset = 0
  while (offset < value.byteLength) {
    if (signal?.aborted) throw new Error('pack creation aborted')
    const result = await file.write(
      value,
      offset,
      value.byteLength - offset,
      null
    )
    if (result.bytesWritten === 0) {
      throw new Error('pack write made no progress')
    }
    hash.update(value.subarray(offset, offset + result.bytesWritten))
    offset += result.bytesWritten
  }
}

async function appendSource(
  destination: FileHandle,
  sourcePath: string,
  expectedSize: number,
  expectedSha256: string,
  packHash: ReturnType<typeof createHash>,
  signal?: AbortSignal
): Promise<void> {
  const source = await open(sourcePath, 'r')
  const bodyHash = createHash('sha256')
  const buffer = Buffer.allocUnsafe(
    Math.max(1, Math.min(COPY_BUFFER_BYTES, expectedSize || 1))
  )
  let position = 0
  try {
    while (position < expectedSize) {
      if (signal?.aborted) throw new Error('pack creation aborted')
      const result = await source.read(
        buffer,
        0,
        Math.min(buffer.byteLength, expectedSize - position),
        position
      )
      if (result.bytesRead === 0) throw new Error('pack source ended early')
      const chunk = buffer.subarray(0, result.bytesRead)
      await writeAll(destination, chunk, packHash, signal)
      bodyHash.update(chunk)
      position += result.bytesRead
    }
    const extra = await source.read(buffer, 0, 1, position)
    if (extra.bytesRead !== 0) throw new Error('pack source exceeded its size')
  } finally {
    await source.close().catch(() => {})
  }
  if (bodyHash.digest('hex') !== expectedSha256) {
    throw new Error('pack source SHA-256 changed after acceptance')
  }
}

export function namespaceHash(namespace: string): string {
  return createHash('sha256').update(namespace).digest('hex').slice(0, 16)
}

export class PackWriter {
  private readonly namespaceHash: string

  constructor(private readonly options: PackWriterOptions) {
    this.namespaceHash = namespaceHash(options.namespace)
  }

  async seal(
    records: readonly PackSourceRecord[],
    sequence: bigint,
    signal?: AbortSignal
  ): Promise<SealedPack> {
    if (records.length === 0) throw new Error('cannot seal an empty pack')
    if (signal?.aborted) throw new Error('pack creation aborted')
    await mkdir(this.options.directory, {recursive: true, mode: 0o700})
    const buildingPath = path.join(
      this.options.directory,
      `${sequence.toString().padStart(6, '0')}.building`
    )
    const file = await open(buildingPath, 'wx', 0o600)
    const packHash = createHash('sha256')
    const entries: PackIndexEntry[] = []
    let payloadBytes = 0
    let closed = false
    let renamedPath: string | undefined
    try {
      for (let ordinal = 0; ordinal < records.length; ordinal += 1) {
        if (signal?.aborted) throw new Error('pack creation aborted')
        const record = records[ordinal]
        if (record === undefined)
          throw new Error('pack source record is missing')
        if (!Number.isSafeInteger(record.size) || record.size < 0) {
          throw new Error('pack source size is invalid')
        }
        await appendSource(
          file,
          record.path,
          record.size,
          record.bodySha256,
          packHash,
          signal
        )
        entries.push(
          createPackIndexEntry({
            kind: record.kind,
            ordinal,
            digest: record.digest,
            offset: payloadBytes,
            length: record.size,
            bodySha256: record.bodySha256
          })
        )
        payloadBytes += record.size
      }

      const index = Buffer.from(encodePackIndex(entries))
      const trailer = Buffer.from(
        encodePackTrailer(createPackTrailer(payloadBytes, index))
      )
      await writeAll(file, index, packHash, signal)
      await writeAll(file, trailer, packHash, signal)
      if (signal?.aborted) throw new Error('pack creation aborted')
      await file.sync()
      await file.close()
      closed = true
      if (signal?.aborted) throw new Error('pack creation aborted')

      const packId = packHash.digest('hex').slice(0, 16)
      const finalPath = path.join(
        this.options.directory,
        `${sequence.toString().padStart(6, '0')}-${packId}.pack`
      )
      await rename(buildingPath, finalPath)
      renamedPath = finalPath
      const bloom = createPackBloom(
        records.map(record => ({kind: record.kind, digest: record.digest}))
      )
      const key = createPackCacheKey({
        namespaceHash: this.namespaceHash,
        runId: this.options.runId,
        jobHash: this.options.jobHash,
        sequence,
        bloom,
        packId
      })
      const size = payloadBytes + index.byteLength + trailer.byteLength
      return {
        key,
        version: PACK_CACHE_VERSION,
        path: finalPath,
        size,
        sequence,
        entries,
        objectCount: records.length,
        payloadBytes,
        dispose: () => rm(finalPath, {force: true})
      }
    } catch (error) {
      if (!closed) await file.close().catch(() => {})
      await rm(buildingPath, {force: true}).catch(() => {})
      if (renamedPath !== undefined) {
        await rm(renamedPath, {force: true}).catch(() => {})
      }
      throw error
    }
  }
}
