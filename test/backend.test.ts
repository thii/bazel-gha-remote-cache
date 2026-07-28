import assert from 'node:assert/strict'
import {test} from 'node:test'
import {
  ActionsCacheBackend,
  BackendError,
  parseRetryAfter
} from '../src/backend.js'

type FakeFetch = (input: string | URL, init?: RequestInit) => Promise<Response>

interface FetchCall {
  url: string
  init: RequestInit | undefined
}

function jsonResponse(
  value: unknown,
  status = 200,
  headers?: HeadersInit
): Response {
  return new Response(JSON.stringify(value), {
    status,
    ...(headers === undefined ? {} : {headers})
  })
}

function fakeFetch(responseFor: (call: FetchCall, index: number) => Response): {
  fetchImplementation: FakeFetch
  calls: FetchCall[]
} {
  const calls: FetchCall[] = []
  return {
    calls,
    fetchImplementation: async (input, init) => {
      const call = {url: String(input), init}
      calls.push(call)
      return responseFor(call, calls.length - 1)
    }
  }
}

test('ActionsCacheBackend sends exact Twirp JSON paths, authentication, and proto field names', async () => {
  const responses = [
    jsonResponse({
      ok: true,
      signed_download_url: 'https://blob.example.test/download?sig=secret',
      matched_key: 'cache-key'
    }),
    jsonResponse({
      ok: true,
      signed_upload_url: 'https://blob.example.test/upload?sig=secret'
    }),
    jsonResponse({ok: true, entry_id: '123'})
  ]
  const {fetchImplementation, calls} = fakeFetch((_call, index) => {
    const response = responses[index]
    assert.ok(response)
    return response
  })
  const backend = new ActionsCacheBackend(
    'https://results.example.test/a/base/path?ignored=true',
    'runtime-token',
    30,
    fetchImplementation,
    1
  )

  assert.deepEqual(await backend.lookup('cache-key', 'cache-version'), {
    kind: 'hit',
    downloadUrl: 'https://blob.example.test/download?sig=secret'
  })
  assert.deepEqual(await backend.reserve('cache-key', 'cache-version'), {
    kind: 'reserved',
    uploadUrl: 'https://blob.example.test/upload?sig=secret'
  })
  await backend.finalize('cache-key', 'cache-version', 42)

  const service = 'github.actions.results.api.v1.CacheService'
  assert.deepEqual(
    calls.map(call => call.url),
    [
      `https://results.example.test/twirp/${service}/GetCacheEntryDownloadURL`,
      `https://results.example.test/twirp/${service}/CreateCacheEntry`,
      `https://results.example.test/twirp/${service}/FinalizeCacheEntryUpload`
    ]
  )
  assert.deepEqual(
    calls.map(call => call.init?.body),
    [
      '{"key":"cache-key","version":"cache-version"}',
      '{"key":"cache-key","version":"cache-version"}',
      '{"key":"cache-key","size_bytes":"42","version":"cache-version"}'
    ]
  )

  for (const call of calls) {
    assert.equal(call.init?.method, 'POST')
    assert.equal(call.init?.redirect, 'error')
    const headers = new Headers(call.init?.headers)
    assert.equal(headers.get('accept'), 'application/json')
    assert.equal(headers.get('authorization'), 'Bearer runtime-token')
    assert.equal(headers.get('content-type'), 'application/json')
    assert.equal(headers.get('user-agent'), 'bazel-gha-remote-cache/0.0.4')
    assert.ok(call.init?.signal instanceof AbortSignal)
  }
})

test('lookup maps an explicit service miss without requiring signed URL fields', async () => {
  const {fetchImplementation, calls} = fakeFetch(() =>
    jsonResponse({ok: false})
  )
  const backend = new ActionsCacheBackend(
    'https://results.example.test',
    'token',
    30,
    fetchImplementation
  )

  assert.deepEqual(await backend.lookup('missing-key', 'version'), {
    kind: 'miss'
  })
  assert.equal(calls.length, 1)
})

test('lookup rejects a non-exact matched key even when the service reports a hit', async () => {
  const {fetchImplementation} = fakeFetch(() =>
    jsonResponse({
      ok: true,
      signed_download_url: 'https://blob.example.test/object',
      matched_key: 'prefix-match'
    })
  )
  const backend = new ActionsCacheBackend(
    'https://results.example.test',
    'token',
    30,
    fetchImplementation
  )

  await assert.rejects(backend.lookup('exact-key', 'version'), {
    name: 'BackendError',
    message: 'cache service returned a non-exact key match'
  })
})

test('reserve maps HTTP and Twirp conflicts without retrying', async () => {
  const conflicts = [
    {status: 409, body: {code: 'unknown', msg: 'entry exists'}},
    {status: 400, body: {code: 'already_exists', msg: 'entry exists'}},
    {status: 400, body: {code: 'aborted', msg: 'reservation raced'}}
  ]

  for (const {status, body} of conflicts) {
    const {fetchImplementation, calls} = fakeFetch(() =>
      jsonResponse(body, status)
    )
    const backend = new ActionsCacheBackend(
      'https://results.example.test',
      'token',
      30,
      fetchImplementation,
      4
    )

    assert.deepEqual(await backend.reserve('key', 'version'), {
      kind: 'conflict'
    })
    assert.equal(calls.length, 1)
  }
})

test('reserve maps a successful RPC with ok=false to a reservation conflict', async () => {
  const {fetchImplementation} = fakeFetch(() =>
    jsonResponse({ok: false, message: 'another writer won'})
  )
  const backend = new ActionsCacheBackend(
    'https://results.example.test',
    'token',
    30,
    fetchImplementation
  )

  assert.deepEqual(await backend.reserve('key', 'version'), {kind: 'conflict'})
})

test('finalize normalizes a service conflict as retryable ambiguity', async () => {
  const {fetchImplementation, calls} = fakeFetch(() =>
    jsonResponse({code: 'already_exists', msg: 'entry already finalized'}, 409)
  )
  const backend = new ActionsCacheBackend(
    'https://results.example.test',
    'token',
    30,
    fetchImplementation,
    4
  )

  let error: unknown
  try {
    await backend.finalize('key', 'version', 42)
    assert.fail('expected finalization to reject')
  } catch (caught) {
    error = caught
  }
  assert.ok(error instanceof BackendError)
  assert.equal(error.message, 'cache finalization outcome is ambiguous')
  assert.equal(error.retryable, true)
  assert.equal(error.conflict, true)
  assert.ok(error.cause instanceof BackendError)
  assert.equal(error.cause.statusCode, 409)
  assert.equal(calls.length, 1)
})

test('a 429 is rate limited, honors Retry-After, and is never retried', async () => {
  const {fetchImplementation, calls} = fakeFetch(() =>
    jsonResponse({code: 'resource_exhausted', msg: 'slow down'}, 429, {
      'Retry-After': '7'
    })
  )
  const backend = new ActionsCacheBackend(
    'https://results.example.test',
    'token',
    30,
    fetchImplementation,
    5
  )

  let error: unknown
  try {
    await backend.lookup('key', 'version')
    assert.fail('expected the lookup to reject')
  } catch (caught) {
    error = caught
  }
  assert.ok(error instanceof BackendError)
  assert.equal(error.statusCode, 429)
  assert.equal(error.rateLimited, true)
  assert.equal(error.retryable, false)
  assert.equal(error.retryAfterMs, 7000)
  assert.equal(calls.length, 1)
})

test('plain-text and malformed-JSON 429 responses preserve rate-limit classification', async () => {
  for (const body of ['upstream overloaded', '{malformed json']) {
    const {fetchImplementation, calls} = fakeFetch(
      () =>
        new Response(body, {
          status: 429,
          headers: {'Retry-After': '11'}
        })
    )
    const backend = new ActionsCacheBackend(
      'https://results.example.test',
      'token',
      30,
      fetchImplementation,
      5
    )

    let error: unknown
    try {
      await backend.lookup('key', 'version')
      assert.fail('expected the lookup to reject')
    } catch (caught) {
      error = caught
    }
    assert.ok(error instanceof BackendError)
    assert.match(error.message, /failed with HTTP 429/)
    assert.equal(error.statusCode, 429)
    assert.equal(error.rateLimited, true)
    assert.equal(error.retryable, false)
    assert.equal(error.retryAfterMs, 11_000)
    assert.equal(calls.length, 1)
  }
})

test('503 Retry-After controls the bounded retry delay', async t => {
  t.mock.timers.enable({apis: ['setTimeout']})
  const {fetchImplementation, calls} = fakeFetch((_call, index) =>
    index === 0
      ? jsonResponse(
          {code: 'unavailable', msg: 'temporarily unavailable'},
          503,
          {'Retry-After': '120'}
        )
      : jsonResponse({ok: false})
  )
  const backend = new ActionsCacheBackend(
    'https://results.example.test',
    'token',
    3600,
    fetchImplementation,
    2
  )

  const lookup = backend.lookup('key', 'version')
  for (let turn = 0; turn < 10; turn += 1) await Promise.resolve()
  assert.equal(calls.length, 1)

  t.mock.timers.tick(59_999)
  await Promise.resolve()
  assert.equal(calls.length, 1)

  t.mock.timers.tick(1)
  assert.deepEqual(await lookup, {kind: 'miss'})
  assert.equal(calls.length, 2)
})

test('parseRetryAfter supports delta seconds and HTTP dates', () => {
  const now = Date.parse('2026-07-26T12:00:00.000Z')
  assert.equal(parseRetryAfter('15', now), 15_000)
  assert.equal(parseRetryAfter('Sun, 26 Jul 2026 12:00:09 GMT', now), 9000)
  assert.equal(parseRetryAfter('Sun, 26 Jul 2026 11:59:00 GMT', now), 0)
  assert.equal(parseRetryAfter('1.5', now), undefined)
  assert.equal(parseRetryAfter('not-a-date', now), undefined)
  assert.equal(parseRetryAfter(null, now), undefined)
})

test('openDownloadRange requires an exact 206 byte range', async () => {
  const payload = Buffer.from('requested-range')
  const {fetchImplementation, calls} = fakeFetch(
    () =>
      new Response(payload, {
        status: 206,
        headers: {
          'Content-Length': String(payload.length),
          'Content-Range': `bytes 7-${7 + payload.length - 1}/100`
        }
      })
  )
  const backend = new ActionsCacheBackend(
    'https://results.example.test',
    'token',
    30,
    fetchImplementation
  )

  const response = await backend.openDownloadRange(
    'https://blob.example.test/pack?sig=secret',
    7,
    payload.length
  )
  assert.deepEqual(Buffer.from(await response.arrayBuffer()), payload)
  assert.equal(calls.length, 1)
  assert.equal(new Headers(calls[0]?.init?.headers).get('range'), 'bytes=7-21')

  const invalid = new ActionsCacheBackend(
    'https://results.example.test',
    'token',
    30,
    async () => new Response(payload, {status: 200})
  )
  await assert.rejects(
    invalid.openDownloadRange('https://blob.example.test/pack', 0, 1),
    /range download failed with HTTP 200/
  )
})

test('commitFile recovers idempotent conflicts and ambiguous finalization', async () => {
  const backend = new ActionsCacheBackend(
    'https://results.example.test',
    'token',
    30,
    async () => jsonResponse({ok: false})
  )
  const calls: string[] = []
  backend.reserve = async () => {
    calls.push('reserve')
    return {kind: 'reserved', uploadUrl: 'https://blob.example.test/upload'}
  }
  backend.uploadFile = async () => {
    calls.push('upload')
  }
  backend.finalize = async () => {
    calls.push('finalize')
    throw new BackendError('ambiguous', {retryable: true})
  }
  backend.lookup = async () => {
    calls.push('lookup')
    return {kind: 'hit', downloadUrl: 'https://blob.example.test/download'}
  }

  assert.equal(
    await backend.commitFile('key', 'version', '/unused', 12),
    'already-exists'
  )
  assert.deepEqual(calls, ['reserve', 'upload', 'finalize', 'lookup'])

  calls.length = 0
  backend.reserve = async () => {
    calls.push('reserve')
    return {kind: 'conflict'}
  }
  assert.equal(
    await backend.commitFile('key', 'version', '/unused', 12),
    'already-exists'
  )
  assert.deepEqual(calls, ['reserve', 'lookup'])
})
