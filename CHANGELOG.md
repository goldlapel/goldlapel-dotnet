# Changelog

## Unreleased

### Breaking changes

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

Other namespaces (`gl.SearchAsync`, `gl.PublishAsync` / `gl.SubscribeAsync`,
`gl.IncrAsync`, `gl.ZaddAsync`, `gl.HsetAsync`, `gl.GeoaddAsync`, …) remain
flat and will migrate to nested form in subsequent releases (one namespace
per schema-to-core phase).

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
