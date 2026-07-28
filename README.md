# Bazel remote cache backed by GitHub Actions Cache v2

This action starts a loopback Bazel HTTP remote cache and stores its data in
GitHub Actions Cache v2. Packed storage is the default: up to 256 Bazel AC/CAS
objects are combined into one immutable cache entry, substantially reducing
repository-wide cache-entry request pressure.

The adapter is a cache, not artifact storage. GitHub's normal cache scope,
retention, capacity, and eviction rules still apply, and the action does not
provide cross-repository sharing.

## Architecture

```text
Bazel PUT /cache/{cas,ac}/digest
          │
          ▼
validate → fsync local spool → durable ordered queue → immediate 204
                               │
                               ▼
                    mixed immutable pack builder
                    (64 MiB / 256 objects / 8 s)
                               │
                               ▼
                    repository-budget entry pacer
                               │
                               ▼
                       Actions Cache v2

Bazel GET → local accepted data → pack catalog + Bloom filter → range reads
                                      │
                                      └→ legacy object-v1 fallback
```

CAS bodies are verified against the SHA-256 digest in the URL before
acknowledgement. Every accepted CAS object receives a monotonic sequence, and
an AC object records the latest accepted CAS sequence as its barrier. Packs
are finalized in order, and a pack containing AC data cannot be published
until its barriers are covered by CAS data in that pack or an earlier durable
pack.

The queue is bounded by `max-pending-bytes`. A PUT receives `204` only after
its data and manifest have been synced locally; a full or failed queue returns
`503`. Pending and recently committed objects are served directly from the
runner, including during the short interval before GitHub's REST catalog shows
a new pack. The post step drains for at most `flush-timeout-seconds` and reports
anything left unflushed.

## Requirements

- Bazel using its HTTP remote-cache protocol.
- A GitHub Actions environment exposing the Cache v2 runtime service.
- Node.js 24 action support. Self-hosted runners must be version `2.327.1` or
  later.
- For packed reads, a token with repository `Actions: read` permission. The
  `github-token` input defaults to `${{ github.token }}`.

## Usage

Pin the action to a reviewed full commit SHA. The default namespace is suitable
for normal Bazel caches: Bazel supplies the AC and CAS digests, while the
adapter's key and Cache v2 versions isolate its storage formats. Set a different
namespace only when you deliberately want a separate cache population or a
manual cold-cache boundary. A namespace is not a substitute for hermetic,
correctly declared Bazel actions and is not an authorization boundary.

```yaml
name: Bazel CI

on:
  push:
  pull_request:

permissions:
  contents: read
  actions: read

jobs:
  test:
    strategy:
      fail-fast: false
      matrix:
        shard: [0, 1, 2, 3]

    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@d23441a48e516b6c34aea4fa41551a30e30af803 # v6

      - name: Start Bazel cache adapter
        id: cache
        uses: thii/bazel-gha-remote-cache@<pinned-sha>
        with:
          storage-mode: pack
          github-token: ${{ github.token }}
          repository-upload-budget: '120'
          expected-writers: '4'
          mode: >-
            ${{ github.event_name == 'push' &&
                github.ref == format(
                  'refs/heads/{0}',
                  github.event.repository.default_branch
                ) &&
                'read-write' || 'read-only' }}

      - name: Run shard
        shell: bash
        run: |
          bazel \
            --bazelrc="${{ steps.cache.outputs.bazelrc }}" \
            test //... \
            --test_env=CI_SHARD=${{ matrix.shard }}
```

`--bazelrc` is a Bazel startup option and must appear before `build`, `test`,
or another command. The generated file configures the loopback cache URL,
remote timeout, raw transfer mode, and disables uploads whenever the resolved
mode is read-only.

The `url` output can be used directly when an existing rc file is preferred:

```yaml
- name: Run Bazel with the URL output
  shell: bash
  env:
    CACHE_URL: ${{ steps.cache.outputs.url }}
    CACHE_WRITABLE: ${{ steps.cache.outputs.writable }}
  run: |
    upload_flags=(--remote_upload_local_results=false)
    if [[ "$CACHE_WRITABLE" == "true" ]]; then upload_flags=(); fi
    bazel test //... \
      --remote_cache="$CACHE_URL" \
      --noremote_cache_compression \
      "${upload_flags[@]}"
```

## Modes and trust boundaries

| Mode         | Reads | Writes                                                   |
| ------------ | ----- | -------------------------------------------------------- |
| `auto`       | Yes   | Only on a protected default-branch push                  |
| `read-only`  | Yes   | No                                                       |
| `read-write` | Yes   | Yes on trusted non-PR events when the runtime permits it |

Pull-request and `pull_request_target` events are forcibly read-only. The
`readable` and `writable` outputs expose the resolved behavior.

The GitHub runtime token and Cache v2 URL are sent to the child daemon through
a one-shot inherited pipe. `github-token` uses the same pipe and is used only
for the supported REST cache-listing API; Cache v2 transfers continue to use
the runtime token. Credentials are not action outputs or environment exports.
The HTTP server binds only to `127.0.0.1`, and shutdown requires a private
bearer token stored in the runner's temporary control directory.

Treat restored bytes as untrusted input. Do not cache secrets or signing
material, keep untrusted workflows read-only, and use separate namespaces only
for intentional cache partitioning, never authorization.

## Storage modes

| Configuration                               | Behavior                                           | Intended use                   |
| ------------------------------------------- | -------------------------------------------------- | ------------------------------ |
| `storage-mode: pack`                        | Ordered mixed pack-v1 entries, write-back required | Default and high-volume builds |
| `storage-mode: object`, `write-back: true`  | Paced asynchronous object-per-entry writes         | Migration and diagnostics      |
| `storage-mode: object`, `write-back: false` | Legacy synchronous object-per-entry writes         | Compatibility debugging only   |

Pack keys are unique and immutable per writer. A 2,048-bit Bloom filter in
each key eliminates definite-negative packs without downloading their indexes.
For a possible match, the adapter performs an exact Cache v2 lookup, range
reads the fixed trailer and sorted index, binary-searches `(kind, digest)`, and
range reads only the payload. Indexes and signed URLs are cached locally, and
cold concurrent index loads are coalesced.

During migration, reads check local data, then pack-v1, then the legacy exact
object key. Object and pack storage revisions have separate adapter-managed key
and Cache v2 versions. Changing `namespace` simply forces a new logical cache
partition.

## Rate limiting

GitHub's cache-entry limits are repository-wide: other matrix jobs,
workflows, `actions/cache`, and BuildKit caches consume the same budget.
`repository-upload-budget` is divided by `expected-writers` to derive each
daemon's entry-creation rate. The token is acquired immediately before
`CreateCacheEntry`; Azure upload blocks do not consume additional pacer tokens.

The daemons run on isolated machines, so this is approximate coordination.
The default budget of 120 entries/minute leaves headroom below GitHub's current
200 uploads/minute limit. When a `429` is observed, the writer honors
`Retry-After`, adds jitter, checks exact visibility before retrying, and halves
its rate. Each clean minute adds 10% of the configured rate until it recovers.
Catalog reads recognize GitHub REST rate limits returned as either `403` or
`429`, honor `Retry-After` or `X-RateLimit-Reset`, and keep serving the last
complete catalog without issuing more REST requests during the pause.

Packed mode normally creates only a few entries per job, making approximate
coordination practical. In object-per-entry mode, use one writable job and
make other matrix jobs read-only; `upload-concurrency: 1` reduces bursts but
does not enforce an entries-per-minute rate by itself.

## Inputs

| Input                        | Default               | Meaning                                                |
| ---------------------------- | --------------------- | ------------------------------------------------------ |
| `namespace`                  | `bazel-v1`            | Optional logical partition; changing it forces misses  |
| `mode`                       | `auto`                | `auto`, `read-only`, or `read-write`                   |
| `storage-mode`               | `pack`                | `pack` or legacy `object` storage                      |
| `github-token`               | `${{ github.token }}` | Token used only to list pack cache entries             |
| `port`                       | `0`                   | Loopback port; `0` selects an available port           |
| `max-object-size`            | `2147483648`          | Maximum object bytes (2 GiB)                           |
| `max-inflight-bytes`         | `4294967296`          | Maximum bytes being request-spooled (4 GiB)            |
| `max-pending-bytes`          | `4294967296`          | Queued and retained local source-data budget (4 GiB)   |
| `upload-concurrency`         | `4`                   | Legacy synchronous upload concurrency                  |
| `download-concurrency`       | `16`                  | Maximum concurrent remote read operations              |
| `repository-upload-budget`   | `120`                 | Desired repository-wide entries/minute budget          |
| `expected-writers`           | `1`                   | Simultaneously writable adapter jobs                   |
| `upload-burst`               | `2`                   | Initial per-daemon entry-creation burst                |
| `write-back`                 | `true`                | Acknowledge after durable local acceptance             |
| `flush-timeout-seconds`      | `120`                 | Bounded post-step queue drain                          |
| `pack-target-bytes`          | `67108864`            | Target pack payload bytes (64 MiB)                     |
| `pack-max-objects`           | `256`                 | Maximum records per pack                               |
| `pack-max-age-seconds`       | `8`                   | Oldest-record age that seals a pack                    |
| `catalog-refresh-seconds`    | `300`                 | Minimum interval between miss-triggered REST refreshes |
| `remote-timeout-seconds`     | `30`                  | Bazel and remote request timeout                       |
| `fail-job-on-cache-error`    | `false`               | Fail post step after cache errors or unflushed data    |
| `upload-diagnostics`         | `on-error`            | Upload sanitized diagnostics: on-error, always, never  |
| `diagnostics-retention-days` | `7`                   | Diagnostic artifact retention (1–90 days)              |

`max-inflight-bytes` and `max-pending-bytes` must each be at least
`max-object-size`; `pack-target-bytes` cannot exceed `max-pending-bytes`.
Remotely durable local copies are evicted first when new writes need that
budget. Request spools and pack sealing temporarily need additional space, so
runner disk use can be higher than `max-pending-bytes`.

## Outputs

| Output                      | Meaning                                      |
| --------------------------- | -------------------------------------------- |
| `url`                       | Loopback remote-cache URL including `/cache` |
| `readable`                  | Whether remote reads are enabled             |
| `writable`                  | Whether PUTs are enabled after event policy  |
| `bazelrc`                   | Generated temporary Bazel rc path            |
| `diagnostics-artifact-name` | Exact name used for a diagnostics artifact   |

## Failure behavior and metrics

A later remote write failure does not invalidate a successful local Bazel
build, but it can lose cache population. The queue retains retryable failures
until the drain deadline. Permanent errors stop new acceptance, and objects
remaining at shutdown are listed in the job summary. Set
`fail-job-on-cache-error: true` for compatibility canaries or workflows where
cache degradation must fail the job.

The summary reports accepted/deduplicated objects, packs and their averages,
pending and remaining data, CAS-barrier blocking, configured/current/observed
reservation rates, pacing sleep, rate-limit responses by operation, catalog
refreshes, Bloom candidates and false positives, and range bytes downloaded. A
low observed reservation rate alongside a rate-limit response is evidence that
another repository job or cache consumer is using the shared quota.

When cache or lifecycle errors occur, `upload-diagnostics: on-error` uploads a
short-retention artifact named
`bazel-gha-remote-cache-diagnostics-<run>-<job>`. Its single
`diagnostics.json` file contains validated metrics and a bounded structured
error journal with timestamps, operation phases, HTTP status, retry and rate
limit classification, and client-abort classification. The action captures the
diagnostic snapshot before post-step cleanup, so drain and shutdown failures
are retained.

The private daemon control directory is never uploaded. Diagnostic artifacts
exclude credentials, signed URLs, configuration files, Bazel object payloads,
packs, manifests, local paths, and full object digests. Diagnostic upload
failure produces a warning but does not change the build result or prevent
cleanup. Set `upload-diagnostics: always` when investigating a run without
recorded cache errors, or `never` to disable artifacts entirely.

## Development

Node.js 24 and npm are required. The checked-in `dist/` bundles must be rebuilt
whenever TypeScript sources change.

```bash
npm ci
npm run check
```

`npm run check` formats/checks sources, type-checks, runs unit and integration
tests, rebuilds all action entry points, verifies third-party notices, and
exercises the bundles against a fake Cache v2 service.

## License

This project is available under the [MIT License](LICENSE). Vendored Cache v2
definitions retain their upstream license and metadata under
`src/vendor/actions-toolkit/`; bundled dependency notices are in
[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
