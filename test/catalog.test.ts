import assert from 'node:assert/strict'
import {test} from 'node:test'
import {
  PackCatalog,
  PackCatalogError,
  type CatalogFetch,
  type PackCatalogObjectKind,
  type PackKeyCodec
} from '../src/catalog.js'

const PREFIX = 'brc2-a81f7e9d-pack-'

interface TestMetadata {
  member: string
}

const codec: PackKeyCodec<TestMetadata> = {
  parse(key) {
    if (!key.startsWith(PREFIX)) return undefined
    const member = key.slice(PREFIX.length)
    if (!/^(ac|cas):[a-z0-9]+$/.test(member)) return undefined
    return {member}
  },
  mightContain(metadata, kind, digest) {
    return metadata.member === `${kind}:${digest}`
  }
}

interface FetchCall {
  url: URL
  init: RequestInit | undefined
}

function cacheRecord(
  id: number,
  key: string,
  ref: string,
  createdAt: string
): Record<string, unknown> {
  return {
    id,
    key,
    version: `version-${id}`,
    ref,
    size_in_bytes: id * 10,
    created_at: createdAt,
    last_accessed_at: createdAt
  }
}

function jsonResponse(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: {'Content-Type': 'application/json'}
  })
}

function makeCatalog(
  fetchImplementation: CatalogFetch,
  overrides: Partial<{
    currentRef: string
    baseRef: string
    defaultRef: string
    clock: () => number
    refreshIntervalMs: number
    requestTimeoutMs: number
    maxPagesPerRef: number
    token: string
  }> = {}
): PackCatalog<TestMetadata> {
  return new PackCatalog({
    owner: 'octo-org',
    repository: 'cache-repo',
    token: overrides.token ?? 'github-token',
    keyPrefix: PREFIX,
    currentRef: overrides.currentRef ?? 'refs/pull/7/merge',
    ...(overrides.baseRef === undefined ? {} : {baseRef: overrides.baseRef}),
    defaultRef: overrides.defaultRef ?? 'refs/heads/main',
    codec,
    fetchImplementation,
    apiBaseUrl: 'https://github-api.example.test/api/v3/',
    clock: overrides.clock ?? (() => 1_000),
    refreshIntervalMs: overrides.refreshIntervalMs ?? 15_000,
    requestTimeoutMs: overrides.requestTimeoutMs ?? 1_000,
    maxPagesPerRef: overrides.maxPagesPerRef ?? 10
  })
}

test('catalog paginates current/default refs, validates results, and sorts newest first', async () => {
  const currentRef = 'refs/pull/7/merge'
  const defaultRef = 'refs/heads/main'
  const firstPage = Array.from({length: 100}, (_, index) =>
    cacheRecord(
      index + 1,
      `${PREFIX}invalid-${index}`,
      currentRef,
      '2026-01-01T00:00:00.000Z'
    )
  )
  firstPage[0] = cacheRecord(
    1,
    `${PREFIX}cas:old`,
    currentRef,
    '2026-01-01T00:00:00.000Z'
  )
  // A REST implementation returning an unexpected ref or fuzzy key must not
  // expand the catalog's visibility scope.
  firstPage[1] = cacheRecord(
    2,
    `${PREFIX}cas:wrongref`,
    'refs/heads/untrusted',
    '2026-01-05T00:00:00.000Z'
  )
  firstPage[2] = cacheRecord(
    3,
    `other-${PREFIX}cas:fuzzy`,
    currentRef,
    '2026-01-06T00:00:00.000Z'
  )

  const calls: FetchCall[] = []
  const fetchImplementation: CatalogFetch = async (input, init) => {
    const url = new URL(String(input))
    calls.push({url, init})
    const ref = url.searchParams.get('ref')
    const page = url.searchParams.get('page')
    if (ref === currentRef && page === '1') {
      return jsonResponse({total_count: 101, actions_caches: firstPage})
    }
    if (ref === currentRef && page === '2') {
      return jsonResponse({
        total_count: 101,
        actions_caches: [
          cacheRecord(
            200,
            `${PREFIX}cas:new`,
            currentRef,
            '2026-01-03T00:00:00.000Z'
          )
        ]
      })
    }
    assert.equal(ref, defaultRef)
    assert.equal(page, '1')
    return jsonResponse({
      total_count: 1,
      actions_caches: [
        cacheRecord(
          300,
          `${PREFIX}ac:middle`,
          defaultRef,
          '2026-01-02T00:00:00.000Z'
        )
      ]
    })
  }

  const catalog = makeCatalog(fetchImplementation)
  const snapshot = await catalog.snapshot()

  assert.deepEqual(
    snapshot.entries.map(entry => entry.key),
    [`${PREFIX}cas:new`, `${PREFIX}ac:middle`, `${PREFIX}cas:old`]
  )
  assert.deepEqual(
    snapshot.entries.map(entry => entry.ref),
    [currentRef, defaultRef, currentRef]
  )
  assert.equal(snapshot.generation, 1)
  assert.equal(snapshot.refreshedAt, 1_000)

  assert.equal(calls.length, 3)
  assert.deepEqual(
    calls.map(call => call.url.pathname),
    [
      '/api/v3/repos/octo-org/cache-repo/actions/caches',
      '/api/v3/repos/octo-org/cache-repo/actions/caches',
      '/api/v3/repos/octo-org/cache-repo/actions/caches'
    ]
  )
  assert.deepEqual(
    calls.map(call => call.url.searchParams.get('ref')),
    [currentRef, currentRef, defaultRef]
  )
  for (const call of calls) {
    assert.equal(call.url.searchParams.get('key'), PREFIX)
    assert.equal(call.url.searchParams.get('sort'), 'created_at')
    assert.equal(call.url.searchParams.get('direction'), 'desc')
    assert.equal(call.url.searchParams.get('per_page'), '100')
    assert.equal(call.init?.method, 'GET')
    assert.equal(call.init?.redirect, 'error')
    assert.ok(call.init?.signal instanceof AbortSignal)
    const headers = new Headers(call.init?.headers)
    assert.equal(headers.get('authorization'), 'Bearer github-token')
    assert.equal(headers.get('accept'), 'application/vnd.github+json')
    assert.equal(headers.get('x-github-api-version'), '2022-11-28')
  }

  const metrics = catalog.metricsSnapshot()
  assert.equal(metrics.refreshAttempts, 1)
  assert.equal(metrics.refreshes, 1)
  assert.equal(metrics.pagesFetched, 3)
  assert.equal(metrics.entriesSeen, 102)
  assert.equal(metrics.entriesAccepted, 3)
  assert.equal(metrics.invalidPackKeys, 97)
})

test('catalog includes a distinct PR base ref and deduplicates repeated refs', async () => {
  const currentRef = 'refs/pull/7/merge'
  const baseRef = 'refs/heads/release/v1'
  const defaultRef = 'refs/heads/main'
  const refs: Array<string | null> = []
  const catalog = makeCatalog(
    async input => {
      refs.push(new URL(String(input)).searchParams.get('ref'))
      return jsonResponse({total_count: 0, actions_caches: []})
    },
    {currentRef, baseRef, defaultRef}
  )

  await catalog.snapshot()
  assert.deepEqual(refs, [currentRef, baseRef, defaultRef])

  const deduplicatedRefs: Array<string | null> = []
  const deduplicated = makeCatalog(
    async input => {
      deduplicatedRefs.push(new URL(String(input)).searchParams.get('ref'))
      return jsonResponse({total_count: 0, actions_caches: []})
    },
    {currentRef, baseRef: defaultRef, defaultRef}
  )
  await deduplicated.snapshot()
  assert.deepEqual(deduplicatedRefs, [currentRef, defaultRef])
})

test('candidate lookup keeps positive snapshots and refreshes misses under a cooldown', async () => {
  let now = 1_000
  let request = 0
  const members = ['cas:a', 'cas:b', 'cas:c', 'cas:d']
  const fetchImplementation: CatalogFetch = async () => {
    const member = members[request]
    assert.ok(member)
    request += 1
    return jsonResponse({
      total_count: 1,
      actions_caches: [
        cacheRecord(
          request,
          `${PREFIX}${member}`,
          'refs/heads/main',
          `2026-01-0${request}T00:00:00.000Z`
        )
      ]
    })
  }
  const catalog = makeCatalog(fetchImplementation, {
    currentRef: 'refs/heads/main',
    defaultRef: 'refs/heads/main',
    clock: () => now,
    refreshIntervalMs: 100
  })

  const first = await catalog.candidates('cas', 'a')
  assert.equal(request, 1)
  assert.deepEqual(
    first.entries.map(entry => entry.metadata.member),
    ['cas:a']
  )

  const cached = await catalog.candidates('cas', 'a')
  assert.equal(request, 1)
  assert.equal(cached.generation, first.generation)

  // Immutable positive candidates do not trigger a full REST re-list merely
  // because the miss-refresh interval elapsed.
  now += 101
  const staleHit = await catalog.candidates('cas', 'a')
  assert.equal(request, 1)
  assert.equal(staleHit.generation, first.generation)

  // A definite Bloom miss gets one cooldown-controlled refresh.
  const discovered = await catalog.candidates('cas', 'b')
  assert.equal(request, 2)
  assert.equal(discovered.generation, 2)
  assert.deepEqual(
    discovered.entries.map(entry => entry.metadata.member),
    ['cas:b']
  )

  // A global miss-refresh cooldown prevents a high Bazel miss rate from
  // turning into the same high GitHub REST request rate.
  const negative = await catalog.candidates('cas', 'unavailable')
  assert.equal(request, 2)
  assert.equal(negative.generation, discovered.generation)
  assert.deepEqual(negative.entries, [])

  // PackReader can report that every candidate was a false positive. The
  // generation guard ensures another caller can reuse the resulting refresh.
  now += 101
  const afterMiss = await catalog.refreshAfterMiss(
    discovered.generation,
    'cas',
    'c'
  )
  assert.equal(request, 3)
  assert.deepEqual(
    afterMiss.entries.map(entry => entry.metadata.member),
    ['cas:c']
  )
  const reused = await catalog.refreshAfterMiss(
    discovered.generation,
    'cas',
    'c'
  )
  assert.equal(request, 3)
  assert.equal(reused.generation, afterMiss.generation)

  now += 101
  const expired = await catalog.candidates('cas', 'd')
  assert.equal(request, 4)
  assert.deepEqual(
    expired.entries.map(entry => entry.metadata.member),
    ['cas:d']
  )

  catalog.recordBloomFalsePositive(2)
  const metrics = catalog.metricsSnapshot()
  assert.equal(metrics.refreshAttempts, 4)
  assert.equal(metrics.refreshes, 4)
  assert.equal(metrics.candidateQueries, 8)
  assert.equal(metrics.bloomCandidates, 7)
  assert.equal(metrics.apparentMissRefreshes, 3)
  assert.equal(metrics.bloomFalsePositives, 2)
})

test('failed miss refreshes serve stale and retry only after the failure cooldown', async () => {
  let now = 1_000
  let requests = 0
  const catalog = makeCatalog(
    async () => {
      requests += 1
      if (requests === 1) {
        return jsonResponse({
          total_count: 1,
          actions_caches: [
            cacheRecord(
              1,
              `${PREFIX}cas:a`,
              'refs/heads/main',
              '2026-01-01T00:00:00.000Z'
            )
          ]
        })
      }
      throw new Error('GitHub API unavailable')
    },
    {
      currentRef: 'refs/heads/main',
      defaultRef: 'refs/heads/main',
      clock: () => now,
      refreshIntervalMs: 100
    }
  )

  const initial = await catalog.candidates('cas', 'a')
  assert.equal(initial.generation, 1)
  assert.equal(initial.entries.length, 1)

  now += 101
  const stale = await catalog.candidates('cas', 'b')
  assert.equal(stale.generation, initial.generation)
  assert.deepEqual(stale.entries, [])
  assert.equal(requests, 2)

  now += 99
  const cooled = await catalog.candidates('cas', 'c')
  assert.equal(cooled.generation, initial.generation)
  assert.deepEqual(cooled.entries, [])
  assert.equal(requests, 2)

  now += 2
  const retried = await catalog.candidates('cas', 'd')
  assert.equal(retried.generation, initial.generation)
  assert.deepEqual(retried.entries, [])
  assert.equal(requests, 3)

  const metrics = catalog.metricsSnapshot()
  assert.equal(metrics.refreshAttempts, 3)
  assert.equal(metrics.refreshes, 1)
  assert.equal(metrics.refreshErrors, 2)
  assert.equal(metrics.apparentMissRefreshes, 2)
})

test('failed cold refreshes do not repeat network attempts inside the cooldown', async () => {
  let now = 1_000
  let requests = 0
  const catalog = makeCatalog(
    async () => {
      requests += 1
      throw new Error('GitHub API unavailable')
    },
    {
      currentRef: 'refs/heads/main',
      defaultRef: 'refs/heads/main',
      clock: () => now,
      refreshIntervalMs: 100
    }
  )

  await assert.rejects(catalog.candidates('cas', 'a'), PackCatalogError)
  now += 99
  await assert.rejects(catalog.candidates('cas', 'b'), PackCatalogError)
  assert.equal(requests, 1)
  assert.equal(catalog.metricsSnapshot().refreshAttempts, 1)
  assert.equal(catalog.metricsSnapshot().refreshErrors, 1)

  now += 2
  await assert.rejects(catalog.candidates('cas', 'c'), PackCatalogError)
  assert.equal(requests, 2)
  const metrics = catalog.metricsSnapshot()
  assert.equal(metrics.refreshAttempts, 2)
  assert.equal(metrics.refreshes, 0)
  assert.equal(metrics.refreshErrors, 2)
})

test('concurrent refreshes coalesce and one aborted waiter does not cancel another', async () => {
  let resolveFetch: ((response: Response) => void) | undefined
  let requestSignal: AbortSignal | undefined
  let calls = 0
  const fetchImplementation: CatalogFetch = async (_input, init) => {
    calls += 1
    requestSignal = init?.signal ?? undefined
    return new Promise<Response>(resolve => {
      resolveFetch = resolve
    })
  }
  const catalog = makeCatalog(fetchImplementation, {
    currentRef: 'refs/heads/main',
    defaultRef: 'refs/heads/main'
  })
  const controller = new AbortController()

  const abandoned = catalog.refresh(controller.signal)
  const retained = catalog.refresh()
  assert.equal(calls, 1)
  controller.abort()
  await assert.rejects(abandoned, {
    name: 'PackCatalogError',
    message: 'GitHub cache catalog request aborted'
  })
  assert.equal(requestSignal?.aborted, false)

  assert.ok(resolveFetch)
  resolveFetch(jsonResponse({total_count: 0, actions_caches: []}))
  const snapshot = await retained
  assert.equal(snapshot.generation, 1)
  assert.equal(catalog.metricsSnapshot().coalescedRefreshes, 1)
})

test('aborting the only waiter cancels transport without poisoning a later refresh', async () => {
  let firstSignal: AbortSignal | undefined
  let calls = 0
  const catalog = makeCatalog(
    async (_input, init) => {
      calls += 1
      if (calls > 1) {
        return jsonResponse({total_count: 0, actions_caches: []})
      }
      firstSignal = init?.signal ?? undefined
      return new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener(
          'abort',
          () => reject(new Error('transport aborted')),
          {once: true}
        )
      })
    },
    {
      currentRef: 'refs/heads/main',
      defaultRef: 'refs/heads/main'
    }
  )
  const controller = new AbortController()

  const abandoned = catalog.refresh(controller.signal)
  controller.abort()
  await assert.rejects(abandoned, /catalog request aborted/)
  assert.equal(firstSignal?.aborted, true)

  const recovered = await catalog.refresh()
  assert.equal(recovered.generation, 1)
  assert.equal(calls, 2)
})

test('request timeout aborts transport and redacts transport failures', async () => {
  const secret = 'ghs_do-not-leak-this-token'
  let observedSignal: AbortSignal | undefined
  const timedOut = makeCatalog(
    async (_input, init) => {
      observedSignal = init?.signal ?? undefined
      return new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener(
          'abort',
          () => reject(new Error(`Bearer ${secret} https://signed.invalid`)),
          {once: true}
        )
      })
    },
    {
      currentRef: 'refs/heads/main',
      defaultRef: 'refs/heads/main',
      requestTimeoutMs: 10,
      token: secret
    }
  )

  await assert.rejects(timedOut.refresh(), error => {
    assert.ok(error instanceof PackCatalogError)
    assert.equal(error.message, 'GitHub cache catalog request timed out')
    assert.equal(error.message.includes(secret), false)
    assert.equal(error.cause, undefined)
    return true
  })
  assert.equal(observedSignal?.aborted, true)
  assert.equal(timedOut.metricsSnapshot().refreshErrors, 1)

  const transportFailure = makeCatalog(
    async () => {
      throw new Error(`Bearer ${secret} https://signed.invalid?sig=${secret}`)
    },
    {
      currentRef: 'refs/heads/main',
      defaultRef: 'refs/heads/main',
      token: secret
    }
  )
  await assert.rejects(transportFailure.refresh(), error => {
    assert.ok(error instanceof PackCatalogError)
    assert.equal(error.message, 'GitHub cache catalog request failed')
    assert.equal(error.message.includes(secret), false)
    assert.equal(error.cause, undefined)
    return true
  })
})

test('HTTP errors do not include response bodies and explain Actions read permission', async () => {
  const secret = 'body-secret-value'
  const catalog = makeCatalog(
    async () => jsonResponse({message: `Bearer ${secret}`}, 403),
    {
      currentRef: 'refs/heads/main',
      defaultRef: 'refs/heads/main'
    }
  )

  await assert.rejects(catalog.refresh(), error => {
    assert.ok(error instanceof PackCatalogError)
    assert.equal(error.statusCode, 403)
    assert.equal(error.rateLimited, false)
    assert.match(error.message, /Actions: read permission/)
    assert.equal(error.message.includes(secret), false)
    return true
  })
})

test('rate-limited REST 403 honors Retry-After and pauses without a snapshot', async () => {
  let now = 1_000
  let requests = 0
  const catalog = makeCatalog(
    async () => {
      requests += 1
      return new Response('rate limited', {
        status: 403,
        headers: {
          'Retry-After': '7',
          'X-RateLimit-Remaining': '0',
          'X-RateLimit-Reset': '2'
        }
      })
    },
    {
      currentRef: 'refs/heads/main',
      defaultRef: 'refs/heads/main',
      clock: () => now
    }
  )

  await assert.rejects(catalog.refresh(), error => {
    assert.ok(error instanceof PackCatalogError)
    assert.equal(error.statusCode, 403)
    assert.equal(error.rateLimited, true)
    assert.equal(error.retryAfterMs, 7000)
    return true
  })
  now += 1000
  await assert.rejects(catalog.refresh(), error => {
    assert.ok(error instanceof PackCatalogError)
    assert.equal(error.rateLimited, true)
    assert.equal(error.retryAfterMs, 6000)
    return true
  })
  assert.equal(requests, 1)
  assert.equal(catalog.metricsSnapshot().rateLimitedResponses, 1)
})

test('rate-limited refresh serves stale catalog until x-ratelimit-reset', async () => {
  let now = 1_000
  let requests = 0
  const catalog = makeCatalog(
    async () => {
      requests += 1
      if (requests === 1) {
        return jsonResponse({
          total_count: 1,
          actions_caches: [
            cacheRecord(
              1,
              `${PREFIX}cas:a`,
              'refs/heads/main',
              '2026-01-01T00:00:00.000Z'
            )
          ]
        })
      }
      if (requests === 2) {
        return new Response('rate limited', {
          status: 403,
          headers: {
            'X-RateLimit-Remaining': '0',
            'X-RateLimit-Reset': '5'
          }
        })
      }
      return jsonResponse({
        total_count: 1,
        actions_caches: [
          cacheRecord(
            2,
            `${PREFIX}cas:b`,
            'refs/heads/main',
            '2026-01-02T00:00:00.000Z'
          )
        ]
      })
    },
    {
      currentRef: 'refs/heads/main',
      defaultRef: 'refs/heads/main',
      clock: () => now,
      refreshIntervalMs: 100
    }
  )

  const initial = await catalog.candidates('cas', 'a')
  assert.equal(initial.entries.length, 1)
  now += 101
  const staleMiss = await catalog.candidates('cas', 'b')
  assert.deepEqual(staleMiss.entries, [])
  assert.equal(requests, 2)
  assert.equal(catalog.metricsSnapshot().refreshErrors, 1)

  now += 200
  assert.equal((await catalog.refresh()).generation, initial.generation)
  assert.deepEqual((await catalog.candidates('cas', 'b')).entries, [])
  assert.equal(requests, 2)

  now = 5_001
  const recovered = await catalog.candidates('cas', 'b')
  assert.equal(requests, 3)
  assert.deepEqual(
    recovered.entries.map(entry => entry.metadata.member),
    ['cas:b']
  )
  const metrics = catalog.metricsSnapshot()
  assert.equal(metrics.refreshAttempts, 3)
  assert.equal(metrics.refreshes, 2)
  assert.equal(metrics.refreshErrors, 1)
  assert.equal(metrics.rateLimitedResponses, 1)
})

test('pagination is bounded and malformed pages fail closed', async () => {
  const fullPage = Array.from({length: 100}, (_, index) =>
    cacheRecord(
      index,
      `${PREFIX}cas:a`,
      'refs/heads/main',
      '2026-01-01T00:00:00.000Z'
    )
  )
  const bounded = makeCatalog(
    async () => jsonResponse({total_count: 101, actions_caches: fullPage}),
    {
      currentRef: 'refs/heads/main',
      defaultRef: 'refs/heads/main',
      maxPagesPerRef: 1
    }
  )
  await assert.rejects(bounded.refresh(), {
    name: 'PackCatalogError',
    message: 'GitHub cache catalog pagination limit exceeded'
  })
  assert.equal(bounded.metricsSnapshot().pagesFetched, 1)
  assert.equal(bounded.metricsSnapshot().refreshErrors, 1)

  const malformed = makeCatalog(
    async () => jsonResponse({total_count: 'one', actions_caches: []}),
    {
      currentRef: 'refs/heads/main',
      defaultRef: 'refs/heads/main'
    }
  )
  await assert.rejects(malformed.refresh(), {
    name: 'PackCatalogError',
    message: 'GitHub cache catalog returned an invalid response'
  })
})

test('an already-aborted lookup performs no network request', async () => {
  let calls = 0
  const catalog = makeCatalog(async () => {
    calls += 1
    return jsonResponse({total_count: 0, actions_caches: []})
  })
  const controller = new AbortController()
  controller.abort(new Error('secret abort reason'))

  await assert.rejects(catalog.snapshot(controller.signal), {
    name: 'PackCatalogError',
    message: 'GitHub cache catalog request aborted'
  })
  assert.equal(calls, 0)
})

test('codec candidate matching receives the requested kind and digest', async () => {
  const seen: Array<{kind: PackCatalogObjectKind; digest: string}> = []
  const observingCodec: PackKeyCodec<TestMetadata> = {
    ...codec,
    mightContain(_metadata, kind, digest) {
      seen.push({kind, digest})
      return true
    }
  }
  const catalog = new PackCatalog({
    owner: 'octo-org',
    repository: 'cache-repo',
    token: 'token',
    keyPrefix: PREFIX,
    currentRef: 'refs/heads/main',
    codec: observingCodec,
    fetchImplementation: async () =>
      jsonResponse({
        total_count: 1,
        actions_caches: [
          cacheRecord(
            1,
            `${PREFIX}cas:a`,
            'refs/heads/main',
            '2026-01-01T00:00:00.000Z'
          )
        ]
      })
  })

  await catalog.candidates('ac', '0123abcd')
  assert.deepEqual(seen, [{kind: 'ac', digest: '0123abcd'}])
})
