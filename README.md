# Bazel remote cache backed by GitHub Actions Cache v2

This JavaScript action starts a loopback HTTP remote cache for Bazel and stores
each action-cache (AC) or content-addressable storage (CAS) object as an
immutable, raw GitHub Actions Cache v2 entry. It is intended to let parallel
jobs in the same repository share Bazel outputs without operating a separate
cache service.

> [!WARNING]
> This project is an alpha. The object-per-entry design can exhaust GitHub's
> cache request limits during a large build, and independent eviction can leave
> an AC result without all of the CAS blobs it references. The supported alpha
> profile is near-term sharing between concurrent or closely spaced jobs on
> GitHub-hosted Linux runners. Do not treat it as durable artifact storage or a
> general-purpose, high-volume Bazel cache.

## How it works

The action starts a detached Node.js daemon bound to `127.0.0.1`. Bazel sends
its normal HTTP remote-cache requests to that daemon:

```text
Bazel ── GET/PUT /cache/{ac,cas}/<digest> ──> loopback adapter
                                                    │
                                                    └──> Actions Cache v2
```

The adapter maps each Bazel object to an exact, immutable cache key:

```text
/cache/ac/<digest>  -> brc-v1-<namespace>-ac-sha256-<digest>
/cache/cas/<digest> -> brc-v1-<namespace>-cas-sha256-<digest>
```

Uploads and downloads use the Cache v2 raw-byte transfer contract directly;
they are not tar archives produced by `actions/cache`. CAS request bodies are
spooled to disk and checked against the SHA-256 digest in the URL before they
are published. A post step shuts down the daemon, reports statistics, and
removes its temporary control and spool files.

## Requirements

- Bazel using its HTTP remote-cache protocol.
- A GitHub Actions environment that exposes the Cache v2 runtime service. The
  action fails at startup when Cache v2 is unavailable.
- A GitHub-hosted Ubuntu runner for the supported alpha profile. A self-hosted
  runner must be version `2.327.1` or later to run Node.js 24 actions, but
  non-Linux platforms have not yet completed the alpha compatibility rollout.

## Usage

Pin the action to a reviewed full commit SHA. Include the operating system,
architecture, Bazel/toolchain versions, and a schema generation in the
namespace so incompatible objects cannot collide.

```yaml
name: Bazel CI

on:
  push:
  pull_request:

permissions:
  contents: read

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
        uses: thii/bazel-gha-remote-cache@v0.0.1
        with:
          namespace: linux-amd64-bazel8-v1
          mode: auto

      - name: Run shard
        shell: bash
        run: |
          bazel \
            --bazelrc="${{ steps.cache.outputs.bazelrc }}" \
            test //... \
            --test_env=CI_SHARD=${{ matrix.shard }}
```

For stronger supply-chain immutability, replace `v0.0.1` with the full commit
SHA you reviewed. `--bazelrc` is a Bazel startup option, so it must appear
before `build`, `test`, or another command.

The generated rc file configures the loopback URL, the requested remote
timeout, and `--noremote_cache_compression`. It also adds
`--remote_upload_local_results=false` whenever the resolved mode is read-only.
You can use the `url` output instead if an existing rc file is more convenient:

```yaml
- name: Run Bazel with the URL output
  shell: bash
  env:
    CACHE_URL: ${{ steps.cache.outputs.url }}
    CACHE_WRITABLE: ${{ steps.cache.outputs.writable }}
  run: |
    upload_flags=(--remote_upload_local_results=false)
    if [[ "$CACHE_WRITABLE" == "true" ]]; then upload_flags=(); fi
    bazel test //... --remote_cache="$CACHE_URL" "${upload_flags[@]}"
```

When using `url` directly, retain the generated rc's timeout and compression
settings as appropriate and always disable uploads when `writable` is false.

## Modes and trust boundaries

| Mode         | Reads | Writes                                                    | Intended use                                       |
| ------------ | ----- | --------------------------------------------------------- | -------------------------------------------------- |
| `auto`       | Yes   | Only on a protected push to the repository default branch | Recommended default                                |
| `read-only`  | Yes   | No                                                        | Pull requests, forks, and other untrusted triggers |
| `read-write` | Yes   | Yes, except where the action enforces a read-only event   | Explicit trusted workflows                         |

Pull-request events are always read-only, even if `read-write` is requested.
The `readable` and `writable` outputs expose the resolved behavior, which lets a
workflow assert its expectations.

The GitHub Actions runtime token and Cache v2 service URL remain inside the
action process and its child daemon. The setup process sends them through a
one-shot inherited pipe; the daemon receives an allowlisted environment, and
the pipe closes before setup returns. The credentials are never action outputs
and are not written to `GITHUB_ENV`. The server listens only on IPv4 loopback,
and its shutdown endpoint is protected by a private token in the runner's
temporary directory.

Treat restored cache bytes as untrusted input:

- Do not place credentials, signing material, or other secrets in Bazel build
  outputs.
- Keep pull requests read-only to reduce cache-poisoning risk.
- Pin this action and other workflow actions to reviewed revisions when your
  threat model requires immutable dependencies.
- Use namespaces for compatibility separation, not as an authorization
  boundary.

GitHub applies its normal repository/ref cache scopes. Eligible jobs in one
repository can share entries; this action does not support cross-repository
sharing. A workflow that can read a cache may be able to inspect everything in
that cache entry.

## Inputs

| Input                     | Default      | Meaning                                               |
| ------------------------- | ------------ | ----------------------------------------------------- |
| `namespace`               | `bazel-v1`   | Logical compatibility namespace and schema generation |
| `mode`                    | `auto`       | `auto`, `read-only`, or `read-write`                  |
| `port`                    | `0`          | Loopback port; `0` selects an available port          |
| `max-object-size`         | `2147483648` | Maximum accepted Bazel object size in bytes (2 GiB)   |
| `max-inflight-bytes`      | `4294967296` | Maximum bytes simultaneously being spooled (4 GiB)    |
| `upload-concurrency`      | `4`          | Maximum concurrent object uploads                     |
| `download-concurrency`    | `16`         | Maximum concurrent object downloads                   |
| `remote-timeout-seconds`  | `30`         | Bazel and cache-service request timeout               |
| `fail-job-on-cache-error` | `false`      | Fail the post step if a backend error was observed    |

`namespace` must be 1–128 characters and may contain letters, digits, dots,
underscores, and hyphens. `max-inflight-bytes` must be at least
`max-object-size`; both limits are decimal byte counts.

Set `fail-job-on-cache-error: true` in a cache compatibility canary or when a
cache failure must be visible. The default keeps cache outages from masking the
result of the Bazel build.

## Outputs

| Output     | Meaning                                                        |
| ---------- | -------------------------------------------------------------- |
| `url`      | Loopback Bazel remote-cache URL, including the `/cache` prefix |
| `readable` | Whether backend reads are enabled                              |
| `writable` | Whether uploads are enabled after applying event policy        |
| `bazelrc`  | Path to the generated temporary Bazel rc file                  |

## Alpha limits and failure behavior

The direct backend creates one GitHub cache entry per Bazel object. GitHub's
current repository-level service limits are 200 cache uploads per minute and
1,500 downloads per minute. The default cache capacity is 10 GB; entries not
accessed for more than seven days can be removed, and storage-pressure eviction
uses last-access order. These service limits and policies can change
independently of this project.

The adapter bounds transfer concurrency, honors `Retry-After`, and opens a
write circuit breaker after a rate-limit response. If any CAS upload fails or
is rate-limited, subsequent AC uploads are refused for the rest of the job so
the adapter does not knowingly publish action results whose output blobs may be
missing. Backend errors are summarized by the post step.

Because AC and CAS entries are evicted independently, a retained action result
can refer to an evicted blob. A future stable, high-volume backend needs packed
objects and dependency-aware lookup; those features are deliberately outside
the alpha.

## Development

Node.js 24 and npm are required. The bundled `dist/` files are the action's
runtime and must be regenerated whenever TypeScript sources change.

```bash
npm ci
npm run check
```

`npm run check` checks formatting, type-checks, runs the source tests, builds
the three action entry points, verifies third-party notices, and exercises the
bundled daemon against a fake Cache v2 service. To rebuild only the checked-in
runtime bundle, run:

```bash
npm run build
```

The CI workflow rejects a change when rebuilding leaves `dist/` different from
the committed bundle. The scheduled Cache v2 canary separately uploads one raw
CAS object in a writer job and retrieves and verifies it in a dependent reader
job, detecting changes in the implementation-level Cache v2 contract and
reporting the cross-job finalization-to-verification latency.

## License

This project is available under the [MIT License](LICENSE). Minimal generated
Cache v2 definitions vendored from `actions/toolkit` retain their upstream
license and commit metadata under `src/vendor/actions-toolkit/`. License and
attribution texts for npm code included in the action bundles are collected in
[the third-party notices](THIRD_PARTY_NOTICES.md).
