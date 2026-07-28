import assert from 'node:assert/strict'
import path from 'node:path'
import {test} from 'node:test'
import {createZipUploadStream} from '../node_modules/@actions/artifact/lib/internal/upload/zip.js'
import {getUploadZipSpecification} from '../node_modules/@actions/artifact/lib/internal/upload/upload-zip-specification.js'

test('the installed artifact client can create its production zip stream', async () => {
  const root = path.resolve('.')
  const file = path.join(root, 'package.json')
  const specification = getUploadZipSpecification([file], root)
  const stream = await createZipUploadStream(specification, 9)
  const chunks: Buffer[] = []
  for await (const chunk of stream) chunks.push(Buffer.from(chunk))
  const archive = Buffer.concat(chunks)

  assert.equal(archive.subarray(0, 2).toString('ascii'), 'PK')
  assert.ok(archive.length > 100)
})
