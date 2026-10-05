# Changelog

## Unreleased

### Breaking changes

**The in-process cache (L1) is gone.** The proxy's result cache now serves
every client the same way, so the wrapper no longer carries its own. Deleted
with it: `NativeCache` (with `CacheEntry`, `SetCommand` and its
`SetCommand.CommandKind` enum, the session-settings tracker
`ConnectionGucState` and its `GucStateSnapshot` struct), `CachedConnection`
and `CachedDataReader`, aggressive post-DML verify (`AggressiveVerifyMode`), the
invalidation-socket client and its stats reporting. `gl.Connection` and the
connections you open against `gl.Url` were never wrapped and are unchanged.

**Removed options, no aliases:** `GoldLapelOptions.InvalidationPort`,
`DisableNativeCache`, `AggressiveVerify`, `DisableMatviews` and
`LicensePayload`, the `gl.InvalidationPort` property, and
`GoldLapel.ParseLicensePayload` (it only fed the cache's aggressive-verify
claim). The `GOLDLAPEL_NATIVE_CACHE`, `GOLDLAPEL_NATIVE_CACHE_SIZE` and
`GOLDLAPEL_REPORT_STATS` env vars are no longer read. The proxy now uses two
ports: proxy and dashboard (proxy + 1).

**Removed `Config` keys** for materialized views, which the proxy no longer
has: `refreshIntervalSecs`, `patternTtlSecs`, `maxTablesPerView`,
`maxColumnsPerView`, `disableConsolidation`, `disableRewrite`,
`disableShadowMode`. `enableCoalescing` is replaced by `disableCoalescing`,
matching the proxy (coalescing is on by default). Passing a removed key
throws `ArgumentException` at `StartAsync`.

**Fixed:** `StartAsync` and `gl.Url` failed with "Couldn't set
application_name" — the connection string carried the libpq spelling
`application_name`, which Npgsql rejects (see the URL conversion entry below).

**One proxy per upstream, and ports that never collide.** Several
databases in one process used to share port 7932, so their proxies split
each other's connections. Now:

- `GoldLapelOptions.ProxyPort` is `int?` and defaults to null: the wrapper
  picks the lowest port from 7932 up whose dashboard port (port + 1) is free
  too, skipping ports held by this process's other proxies or by any other
  program. Read the result from `gl.ProxyPort` / `gl.DashboardPort`.
- `StartAsync` on an upstream that is already running returns another handle
  on the same proxy (with its own internal connection); the proxy stops when
  the last handle is disposed. Concurrent starts share one spawn.
- An explicit `ProxyPort` or `DashboardPort` held by another proxy of this
  process throws `InvalidOperationException` naming the port and that
  proxy's upstream (password masked). One held by another program makes the
  proxy refuse to start; its message is in the exception.
- A start only succeeds if the proxy process is still alive when its port
  answers. If it exited, the exception gives its exit status and the tail of
  its stderr.
- Stopping a proxy clears Npgsql's pool for its connection string, so a proxy
  restarted on the same port isn't handed the old one's dead connections.

**`gl.Url` and `gl.ProxyUrl` no longer carry upstream TLS settings.**
`sslmode`, `sslrootcert`, `sslcert`, `sslkey`, `channel_binding`,
`gssencmode` and the other TLS/GSS parameters are for the proxy's hop to
your database; the proxy declines client TLS unless started with
`tlsCert`/`tlsKey`, so a `?sslmode=require` URL (every Neon, Supabase and RDS
URL) made the app's connection fail. They are kept when `tlsCert`/`tlsKey`
is configured.

**`UrlToNpgsqlConnectionString` rebuilt on `NpgsqlConnectionStringBuilder`.**
Values were concatenated unquoted, so a password containing `;` broke the
string, and the database name was not URL-decoded. libpq parameters now map
to the Npgsql keyword with the same meaning (`connect_timeout` -> `Timeout`,
`sslrootcert` -> `Root Certificate`, `sslcert`/`sslkey`/`sslpassword`,
`krbsrvname`, `options`, `client_encoding`, `passfile`,
`target_session_attrs` for multi-host URLs, `search_path`); before, they
threw "Keyword not supported" and a Neon URL failed `StartAsync`.
`sslmode=require`/`prefer`/`allow` also set `Trust Server Certificate`,
matching libpq (Npgsql 6 refuses `Require` otherwise). libpq parameters with
no Npgsql counterpart (`channel_binding`, `gssencmode`, keepalive tuning,
...) are dropped; anything else throws `ArgumentException` naming the
parameter. IPv6 (`[::1]:5432`) and multi-host URLs are parsed.

**Removed `Config` keys say why.** Passing `invalidationPort`,
`disableNativeCache`, `nativeCacheSize`, `aggressiveVerify`, a matview key or
`enableCoalescing` in `Config` throws an `ArgumentException` that says it was
removed with the in-process cache or materialized views (or points to
`disableCoalescing`). Top-level options are typed properties, so the removed
ones are compile errors.

---

**Doc-store and stream methods moved under nested namespaces.** The flat
`gl.Doc*Async` and `gl.Stream*Async` methods are gone; document and stream
operations now live under `gl.Documents.<Verb>Async` and
`gl.Streams.<Verb>Async`. No backwards-compat aliases — search and replace
once.

Migration map:

| Old (flat)                                | New (nested)                                  |
| ----------------------------------------- | --------------------------------------------- |
| `gl.DocInsertAsync(name, doc)`            | `gl.Documents.InsertAsync(name, doc)`         |
| `gl.DocInsertManyAsync(name, docs)`       | `gl.Documents.InsertManyAsync(name, docs)`    |
| `gl.DocFindAsync(name, filter)`           | `gl.Documents.FindAsync(name, filter)`        |
| `gl.DocFindOneAsync(name, filter)`        | `gl.Documents.FindOneAsync(name, filter)`    |
| `gl.DocFindCursor(name, ...)`             | `gl.Documents.FindCursorAsync(name, ...)`     |
| `gl.DocUpdateAsync(name, f, u)`           | `gl.Documents.UpdateAsync(name, f, u)`        |
| `gl.DocUpdateOneAsync(name, f, u)`        | `gl.Documents.UpdateOneAsync(name, f, u)`     |
| `gl.DocDeleteAsync(name, f)`              | `gl.Documents.DeleteAsync(name, f)`           |
| `gl.DocDeleteOneAsync(name, f)`           | `gl.Documents.DeleteOneAsync(name, f)`        |
| `gl.DocFindOneAndUpdateAsync(...)`        | `gl.Documents.FindOneAndUpdateAsync(...)`     |
| `gl.DocFindOneAndDeleteAsync(...)`        | `gl.Documents.FindOneAndDeleteAsync(...)`     |
| `gl.DocDistinctAsync(name, field, f)`     | `gl.Documents.DistinctAsync(name, field, f)`  |
| `gl.DocCountAsync(name, filter)`          | `gl.Documents.CountAsync(name, filter)`       |
| `gl.DocCreateIndexAsync(name, keys)`      | `gl.Documents.CreateIndexAsync(name, keys)`   |
| `gl.DocAggregateAsync(name, pipeline)`    | `gl.Documents.AggregateAsync(name, pipeline)` |
| `gl.DocWatchAsync(name, cb)`              | `gl.Documents.WatchAsync(name, cb)`           |
| `gl.DocUnwatchAsync(name)`                | `gl.Documents.UnwatchAsync(name)`             |
| `gl.DocCreateTtlIndexAsync(name, n)`      | `gl.Documents.CreateTtlIndexAsync(name, n)`   |
| `gl.DocRemoveTtlIndexAsync(name)`         | `gl.Documents.RemoveTtlIndexAsync(name)`      |
| `gl.DocCreateCappedAsync(name, max)`      | `gl.Documents.CreateCappedAsync(name, max)`   |
| `gl.DocRemoveCapAsync(name)`              | `gl.Documents.RemoveCapAsync(name)`           |
| `gl.DocCreateCollectionAsync(name, ...)`  | `gl.Documents.CreateCollectionAsync(name, ...)` |
| `gl.StreamAddAsync(name, payload)`        | `gl.Streams.AddAsync(name, payload)`          |
| `gl.StreamCreateGroupAsync(name, group)`  | `gl.Streams.CreateGroupAsync(name, group)`    |
| `gl.StreamReadAsync(name, g, c, count)`   | `gl.Streams.ReadAsync(name, g, c, count)`     |
| `gl.StreamAckAsync(name, group, id)`      | `gl.Streams.AckAsync(name, group, id)`        |
| `gl.StreamClaimAsync(name, g, c, ...)`    | `gl.Streams.ClaimAsync(name, g, c, ...)`      |

**Phase 5 of schema-to-core: Redis-compat helper families moved under
nested namespaces.** The flat `gl.IncrAsync` / `gl.ZaddAsync` /
`gl.HsetAsync` / `gl.EnqueueAsync` / `gl.GeoaddAsync` (and friends) are
gone; counter / zset / hash / queue / geo operations now live under
`gl.Counters` / `gl.Zsets` / `gl.Hashes` / `gl.Queues` / `gl.Geos`. No
backwards-compat aliases — search and replace once.

Phase 5 contracts (breaking, no aliases):

- **counter**: every UPSERT stamps `updated_at = NOW()` on the proxy side.
- **zset**: every method takes `zsetKey` as the first arg after the
  namespace name (one canonical table holds many sorted sets).
- **hash**: storage flipped from JSONB-blob-per-key to row-per-field.
  `gl.Hashes.GetAllAsync` rebuilds the dictionary client-side from
  per-row results.
- **queue**: at-least-once with visibility timeout. The legacy
  delete-on-fetch `DequeueAsync` is replaced by
  `gl.Queues.ClaimAsync` (returns `ClaimedMessage?`) +
  `gl.Queues.AckAsync(id)`. **There is intentionally no `DequeueAsync`
  shim** — claim/ack is explicit by design.
- **geo**: GEOGRAPHY-native (no `::geography` casts on the column
  reference because the column already IS geography). `gl.Geos.AddAsync`
  is idempotent on the member name (Redis GEOADD semantics).

Migration map:

| Old (flat)                                | New (nested)                                  |
| ----------------------------------------- | --------------------------------------------- |
| `gl.IncrAsync(t, k)`                      | `gl.Counters.IncrAsync(name, k)`              |
| `gl.GetCounterAsync(t, k)`                | `gl.Counters.GetAsync(name, k)`               |
| `gl.ZaddAsync(t, m, s)`                   | `gl.Zsets.AddAsync(name, zsetKey, m, s)`      |
| `gl.ZincrbyAsync(t, m, d)`                | `gl.Zsets.IncrByAsync(name, zsetKey, m, d)`   |
| `gl.ZrangeAsync(t, …)`                    | `gl.Zsets.RangeAsync(name, zsetKey, …)`       |
| `gl.ZrankAsync(t, m)`                     | `gl.Zsets.RankAsync(name, zsetKey, m)`        |
| `gl.ZscoreAsync(t, m)`                    | `gl.Zsets.ScoreAsync(name, zsetKey, m)`       |
| `gl.ZremAsync(t, m)`                      | `gl.Zsets.RemoveAsync(name, zsetKey, m)`      |
| `gl.HsetAsync(t, k, f, v)`                | `gl.Hashes.SetAsync(name, hashKey, f, v)`     |
| `gl.HgetAsync(t, k, f)`                   | `gl.Hashes.GetAsync(name, hashKey, f)`        |
| `gl.HgetallAsync(t, k)`                   | `gl.Hashes.GetAllAsync(name, hashKey)`        |
| `gl.HdelAsync(t, k, f)`                   | `gl.Hashes.DeleteAsync(name, hashKey, f)`     |
| `gl.EnqueueAsync(t, payload)`             | `gl.Queues.EnqueueAsync(name, payload)`       |
| `gl.DequeueAsync(t)`                      | `gl.Queues.ClaimAsync(name, ms)` + `AckAsync` |
| `gl.GeoaddAsync(t, n, g, name, lon, lat)` | `gl.Geos.AddAsync(name, member, lon, lat)`    |
| `gl.GeoradiusAsync(t, g, lon, lat, r)`    | `gl.Geos.RadiusAsync(name, lon, lat, r, …)`   |
| `gl.GeodistAsync(t, g, n, a, b)`          | `gl.Geos.DistAsync(name, a, b, unit)`         |

Pub/sub stays flat (no DDL family); search / cache / auth remain flat
and will migrate when their own schema-to-core phase fires.

**Doc-store DDL is now owned by the proxy.** The wrapper no longer emits
`CREATE TABLE _goldlapel.doc_<name>` SQL when a collection is first used.
Instead, `gl.Documents.<Verb>Async` calls `POST /api/ddl/doc_store/create`
against the proxy's dashboard port; the proxy runs the canonical DDL on its
management connection and returns the table reference + query patterns. The
wrapper caches `(tables, query_patterns)` per session — one HTTP round-trip
per `(family, name)` per session.

Canonical doc-store schema (v1) standardizes the column shape across every
Gold Lapel wrapper:

```
_id        UUID PRIMARY KEY DEFAULT gen_random_uuid()
data       JSONB NOT NULL
created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
```

Both timestamps are `NOT NULL` — kills the `created_at NOT NULL` /
`updated_at` drift surfaced in the v0.2 cross-wrapper compat audit. Any
wrapper (Python, JS, Ruby, Java, PHP, Go, .NET) writing to a doc-store
collection now produces the same table.

**Doc-store index naming changed.** Wrapper-side indexes (the GIN index from
`CreateIndexAsync()`, the TTL index from `CreateTtlIndexAsync(...)`, the
capped-collection index from `CreateCappedAsync(...)`) now use the
canonical `idx_<bare_table>_<suffix>` form — derived from the proxy table
name with the schema prefix stripped. Across wrappers this means
`idx_doc_users_data_gin`, `idx_doc_logs_ttl`, etc., instead of the
collection-named variants in earlier versions.

**Upgrade path for dev databases:** wipe and recreate. There is no
in-place migration. Pre-1.0, dev databases get rebuilt freely.

```bash
goldlapel clean   # drops _goldlapel.* tables
# ...drop/recreate your DB if needed...
```

### Internal

- `Utils.Doc*` static methods now require a `DdlEntry patterns` argument —
  the wrapper API no longer fabricates DDL itself; the proxy supplies the
  canonical table name and query patterns.
- `Ddl.FetchPatternsAsync` accepts an optional `options` map for per-family
  creation knobs (e.g. doc_store accepts `{"unlogged": true}`). Honored
  only on the first call for a given `(family, name)` — subsequent calls
  hit the cache and the proxy's idempotent `CREATE TABLE IF NOT EXISTS`.
- New `DocumentsApi` and `StreamsApi` sub-API classes in the same
  namespace, each holding a back-reference to the parent `GoldLapel`.
  This is the canonical sub-API shape for the schema-to-core wrapper
  rollout — other namespaces migrate to nested form one-at-a-time as
  their own schema-to-core phase fires.
