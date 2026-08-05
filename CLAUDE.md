# CLAUDE.md

MongoDB session-store benchmark mirroring a Redis RDB export 1:1. Fidelity to
the export is the point of the demo — do not normalize, split, or redesign
the schema, and do not add secondary indexes to the hot read path. The
`_id` point read must stay EXPRESS/IDHACK. The email/token/device query
indexes (`ensure_indexes`, added 2026-08-05 at customer request) serve
their own named ops only and must never appear in `get_session`'s plan.

## Commands

```bash
pytest tests/                              # offline, no cluster needed
python src/generate.py --dry-run --limit 3 # inspect sample docs
python src/load.py [--drop]                # full load (run from EC2, resumable)
python src/verify.py [--preflight]         # RTT/cache check + access-path proof
scripts/run_headless.sh                    # warm-up + measured Locust run
python scripts/report.py --csv-prefix results
```

## Invariants — do not break

- **Determinism:** every document derives purely from
  `(RANDOM_SEED, kind, global index)` via `model.doc_rng`/`_digest`. Never
  introduce wall-clock time, ordering dependence, or shared RNG state into
  generation — `tests/test_generate.py::test_worker_partitioning_yields_identical_dataset`
  guards this. Timestamps anchor to `model.EPOCH_ANCHOR_MS`, not `now()`.
- **`session_key()` lives only in `src/model.py`** and is imported by both
  generator and harness. It derives the injector namespace from the hash;
  the harness relies on it to hit existing `_id`s with zero memory.
- Index-doc *live* members must reference DEFAULT-namespace sessions
  (`DEFAULT_SESSION_PREFIX`), because the reader composes
  `prefix + member`. Otherwise the measured miss rate detaches from
  `INDEX_DOC_DRIFT_RATIO`.
- The dataset is **never** written to disk at scale; generation streams into
  `insert_many`. `--jsonl` is for small samples only.
- Loader must stay resumable: checkpoint after every batch, swallow only
  duplicate-key (11000) errors, refuse resume if worker ranges changed.
- One `MongoClient` per Locust worker process, module scope, `connect=False`.
  Locust's gevent monkey-patching must happen before pymongo import (it does,
  because locust is imported first in locustfile.py).
- TTL index stays behind `ENABLE_TTL_INDEX=false`: ~10% of generated docs are
  already expired and a TTL index would delete them mid-run.
- Report percentiles per named operation; never blend ops or quote a mean.

## Facts confirmed from the real QA RDB dumps (Redis 7 / RDB v10)

- keyv envelope: `{"value": "<json string>", "expires": epoch_ms}` — inner
  payload is double-encoded JSON; we store it parsed.
- Nine injector namespaces with observed weights in `model.SESSION_NAMESPACES`;
  ~6% of session keys have no `keyv::` prefix. All idx zsets are DEFAULT.
- Zset scores are epoch **milliseconds** (ints) — the project prompt's sketch
  showed seconds; the data won.
- `accessToken` is a 3-segment RS256 JWT (header `{alg,typ,kid}` ~76 chars,
  payload of namespaced `https://liverpool.com.mx/*` claims, 342-char
  signature). The JWT payload key set DIFFERS from `decodeAccessToken`,
  which is the app's normalized decode — both are reproduced. Generated
  signatures are random bytes and kids are `FAKE-` prefixed: structurally
  faithful, cryptographically invalid, by design.
- ~99% of session strings carry a live TTL; string values mean ~3.4 KB,
  right-skewed (p50 ~2.2 KB, max ~5.6 KB); largest zset 16,571 members.
- Bull stream `bull:token-refresh:events` entries have fields
  `event`/`jobId`/`prev`; meta hash has literal dotted key
  `opts.maxLenEvents` (legal in MongoDB, kept for fidelity).
- `value.profile` (email, emailVerified, firstName, lastName, phoneNumber)
  is NOT from the export — a customer-requested addition (2026-08-05),
  present only on non-anonymous sessions (~70%). Emails use RFC 2606
  reserved domains; names come from curated ASCII lists. The JWT claim set
  stays exactly as observed — do not add email claims to the token.

## Gotchas

- `SYNTHETIC_USER_COUNT` is informational; the principal count is derived as
  `index_doc_count` (`ZSET_SHARE × TOTAL − 2`). config.py prints a note.
- `faker` from the original spec is intentionally unused: hash-seeded
  `random.Random` with curated lists is faster and version-stable for the
  determinism tests. Don't add faker calls to document builders.
- Generated IPs are RFC 5737 TEST-NET; JWTs carry random-byte signatures
  and `FAKE-` kids. Keep it that way — no real tokens, credentials, or
  customer data, and the
  `*.rdb` QA dumps in this directory are gitignored and must never be
  committed.
- The hash doc's dotted field name breaks naive dot-notation queries on it;
  that is faithful to the export, not a bug.
- `--remodeled` variants are out of scope; if ever added, they are a second
  collection, not changes to this one.
