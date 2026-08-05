# MongoDB session-store benchmark (1:1 Redis structure)

Demonstrates that an Auth0 session store currently on a 3-shard Redis cluster
runs on MongoDB Atlas **with the exact same data shape and access patterns** —
point reads of session documents under 10 ms while a write workload runs
concurrently. No application-side remodeling: every Redis key becomes one
MongoDB document keyed by the Redis key string itself.

The document envelope was confirmed against the real QA RDB dumps (Redis 7),
not assumed: the multi-brand injector namespaces, the `keyv` envelope with its
double-encoded inner JSON, the drifted per-user zsets (largest observed:
16,571 members), zset scores in epoch milliseconds, and the BullMQ
stream/hash are all reproduced. Access tokens are structurally faithful
RS256 JWTs (real base64url header and namespaced-claims payload, same
segment sizes as the export) but cryptographically invalid on purpose: the
signature is random bytes and the `kid` is prefixed `FAKE-`. No real tokens
or customer data appear anywhere; IPs come from RFC 5737 test ranges.

## Data model

One collection, one document per Redis key:

```js
// session (Redis string, ~86% of keys)
{ _id: "keyv::AUTH0_DEFAULT_INJECTOR:session:<uuid>",   // the Redis key
  shard: "node1", type: "string",
  value: { accessToken, scope, deviceInfo: {...}, decodeAccessToken: {...},
           profile: { email, emailVerified, firstName, lastName, phoneNumber },
           ... },
  expiresAt: ISODate(),   // from the keyv envelope's `expires` (epoch ms)
  ttlSeconds: 604800 }    // only when the source key had a live TTL (~99% do)

// per-user session index (Redis zset, ~14% of keys)
{ _id: "AUTH0_DEFAULT_INJECTOR:idx:user:<client_id>@clients:sessions",
  shard: "node2", type: "zset",
  value: [ { member: "<session uuid>", score: 1770218306487 }, ... ] } // epoch ms

// plus one BullMQ stream doc and one meta hash doc
```

Two intentional deviations from the export, both documented:

1. The keyv envelope's inner JSON **string** is stored parsed, as a real
   object. Same fields, but queryable.
2. `value.profile` (email + basic userinfo) is an **addition** — the export
   carries no email, but the customer's auth app will cache profile data, so
   non-anonymous sessions (~70%) include it. Emails use RFC 2606 reserved
   domains only; the JWT claim set is untouched (it matches the export).

**Zero secondary indexes on the primary access path.** `_id` *is* the Redis
key; its unique index comes for free and the point read is an `EXPRESS`/
`IDHACK` plan (proven by `verify.py`, not asserted).

Three **query indexes** serve the additional customer lookups (things Redis
cannot do without `SCAN`), created after the load as plain (non-partial)
indexes — documents without a field appear as one null entry each:

| index | keys | serves |
|---|---|---|
| `email_1` | `{value.profile.email: 1}` | all sessions for an account |
| `device_lastUsed` | `{value.deviceInfo.os: 1, value.lastUsedDate: -1}` | newest sessions per device type, sorted by the index |

(No index on `value.accessToken`: ~1 KB JWT keys across 13.7M docs make the
build and the index itself disproportionately expensive; dropped by choice.)

They never touch the point-read plan — `verify.py` proves both facts with
`explain()`. Remaining optional indexes: `{type: 1}` (off by default) and a
TTL index on `expiresAt` (`ENABLE_TTL_INDEX=false` by default — the dataset
carries past expiries on purpose and would delete itself mid-benchmark).

## Scale

`TOTAL_DOCUMENT_COUNT=16000000`: ~13.76M session docs + ~2.24M index docs,
preserving the observed 86/14 type mix. That is ~45–50 GB logical;
`verify.py` prints actual `dataSize` vs `storageSize` so the WiredTiger
compression ratio is a reported fact.

**Generate from an EC2 instance in the Atlas cluster's region. This is a
prerequisite, not an optimization** — pushing ~50 GB over an office
connection is a non-starter; in-region it is minutes. The dataset is never
written to disk: workers generate and insert in one streaming pass.

## Setup (EC2)

1. Launch the instance in the **same AWS region** as the Atlas cluster
   (cross-region RTT alone can exceed the 10 ms budget) and add it to the
   Atlas IP access list (or use VPC peering / private endpoint).
2. Clone the repo onto the host, then:

```bash
scripts/ec2_setup.sh          # python venv, deps, ulimit -n 65535
cp .env.example .env          # fill in MONGODB_URI; .env is gitignored
source .venv/bin/activate
python src/verify.py --preflight
```

The pre-flight prints raw RTT against the latency budget (so it's clear how
much of the 10 ms is network before any load runs) and the hot-set estimate
next to the cluster's actual WiredTiger cache size, warning if the tier is
undersized — catch it here, not in bad percentiles afterward.

### Cluster sizing

What must fit in cache is the **hot working set**, not the dataset:
`dataSize × HOT_SET_RATIO` plus the `_id` index — roughly 2.5 GB + index at
the defaults. Atlas gives WiredTiger about half of instance RAM; pick a tier
whose cache comfortably exceeds the pre-flight's hot-set figure. Reads that
miss cache land on NVMe and are still fast, but they are what will move p99.

## Load

```bash
python src/load.py            # resumable; ~GENERATE_WORKERS processes
```

- Workers own contiguous ID ranges; every document derives purely from
  `(RANDOM_SEED, kind, index)`, so the dataset is byte-identical regardless
  of worker count.
- Progress line: docs/s, elapsed, ETA, per-worker counts.
- Checkpoints to `LOAD_CHECKPOINT_PATH.w<n>` after every batch — an
  interrupted load **resumes**; duplicate keys from resume overlap are
  swallowed. Never restarts from zero.
- On completion: `count`, `avgObjSize`, `dataSize`, `storageSize`, index
  sizes, and any document over 1 MB (the largest zset docs approach it).
- Inspect sample documents anytime without touching the cluster:
  `python src/generate.py --dry-run --limit 3`.

Then prove the state: `python src/verify.py` (counts vs configured mix,
stored shapes, `explain()` for both read paths, storage/compression).

## Benchmark

```bash
scripts/run_headless.sh                                 # standalone
# distributed: one worker per vCPU
LOCUST_MODE=master EXPECT_WORKERS=8 scripts/run_headless.sh
LOCUST_MODE=worker MASTER_HOST=<master-ip> scripts/run_headless.sh   # x8
```

Workers must reach the master on TCP 5557. Every run starts with a discarded
warm-up phase so TLS handshakes, pool growth, and first-touch page faults
stay out of the reported percentiles.

Operations, mapped 1:1 to the Redis calls they replace:

| name | Redis op | MongoDB op |
|---|---|---|
| `get_session` | `GET` | `find_one` by `_id` — **the SLO applies here** |
| `zrange_index` | `ZRANGE` | `find_one` by `_id`, `$slice: -ZRANGE_LIMIT` projection |
| `mget_sessions` | `MGET` | `find {_id: {$in: [...]}}` |
| `index_round_trip` | — | both legs of the two-step read, end to end |
| `update_session` | `SETEX` refresh | `$set` a field inside `value` |
| `insert_session` | `SETEX` new | `insert_one` full session doc |
| `push_index` | `ZADD` | `$push` with `$slice` cap |
| `find_by_email` | — (needs `SCAN`) | `find` on `value.profile.email` (email_1) |
| `sessions_by_device` | — (needs `SCAN`) | os equality + `lastUsedDate` range, index-sorted (device_lastUsed) |

The secondary-index reads derive their lookup values client-side with
`build_session_doc` (generation is deterministic), so they always hit real
stored emails with zero memory or precomputed lists.

Notes on honesty:

- **Access distribution:** `HOT_ACCESS_SHARE` (80%) of reads hit the most
  recent `HOT_SET_RATIO` (5%) of sessions; the rest are uniform over the
  cold range. Quote latency numbers only next to this distribution — a
  latency figure without its access pattern is not meaningful.
- The `$slice` projection on index reads is deliberate: a 16k-member index
  doc is ~1.2 MB and transferring it whole would blow the budget, exactly as
  `ZRANGE 0 -1` would.
- `mget_sessions` misses are **expected**: `INDEX_DOC_DRIFT_RATIO` (40%) of
  index members reference expired/deleted sessions, the same drift the Redis
  zsets exhibit. The harness counts and reports them.
- `push_index` rewrites the whole document (inherent to keeping the zset as
  an array), so its latency profile is legitimately different from a point
  read. Shown honestly rather than hidden.
- One `MongoClient` per Locust worker process, `connect=False`, created at
  module scope. Startup asserts `MONGODB_MAX_POOL_SIZE` covers concurrent
  users so pool queuing is never reported as database latency.
- `constant_throughput` pacing makes `TARGET_READ_RPS` a target, not an
  emergent property of think time.

## Reading the results

```bash
python scripts/report.py --csv-prefix results
```

Prints **p50 / p95 / p99 / max per named operation** (never a bare mean) and
writes `results_latency.png` — latency over time with the
`READ_LATENCY_SLO_MS` line drawn on it. The measured run also writes
Locust's standard HTML report (`results_report.html`: RPS, response-time
percentiles, and user-count charts) — scp it off the EC2 to view.

Two numbers must never be conflated:

- **Client-observed latency** — what Locust times from EC2, network
  included. This is the customer-facing number; the pre-flight RTT tells you
  its floor.
- **Server-side execution time** — `verify.py`'s `explain()` output, plus
  Atlas metrics (operation execution time, cache hit ratio, queue depth)
  over the run window.

## Repository layout

```
src/config.py     .env loading + validation, fail fast
src/model.py      envelope shape, deterministic key derivation (shared by
                  generator AND harness — any index -> valid _id in O(1))
src/generate.py   synthetic generator; --dry-run / --jsonl for inspection
src/load.py       multiprocess streaming loader, checkpoint/resume
src/verify.py     pre-flight (RTT, cache sizing) + post-load proof
locustfile.py     the harness
scripts/          EC2 setup, headless runner, CSV -> table + chart
tests/            determinism, shape-vs-export equivalence, checkpoint resume
```

Run tests locally: `pytest tests/` (no cluster needed).
