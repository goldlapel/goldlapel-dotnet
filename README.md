# Gold Lapel

Self-optimizing Postgres proxy — automatic materialized views and indexes, with an L1 native cache that serves repeated reads in microseconds. Zero code changes required.

Gold Lapel sits between your app and Postgres, watches query patterns, and automatically creates materialized views and indexes to make your database faster. Port 7932 (79 = atomic number for gold, 32 from Postgres).

## Install

```
dotnet add package GoldLapel
```

`Npgsql` is a required dependency (installed automatically).

## Quick Start

```csharp
using GoldLapel;
using Npgsql;

// Factory — spawns the proxy, opens an internal Npgsql connection, returns an instance.
await using var gl = await GoldLapel.StartAsync(
    "postgresql://user:pass@localhost:5432/mydb",
    opts => {
        opts.Port = 7932;
        opts.LogLevel = "info";
    });

// Raw SQL through the proxy — gl.Url is ready for Npgsql.
await using var conn = new NpgsqlConnection(gl.Url);
await conn.OpenAsync();
await using var cmd = new NpgsqlCommand("SELECT * FROM users LIMIT 10", conn);
await using var reader = await cmd.ExecuteReaderAsync();

// High-level wrapper methods run against the internal connection by default.
var hits = await gl.SearchAsync("articles", "body", "postgres tuning");
await gl.DocInsertAsync("events", "{\"type\":\"signup\"}");

// await using disposes the instance: stops the proxy and closes the internal connection.
```

## Scoped connection: `gl.UsingAsync(conn, ...)`

For transactions or request-scoped pools, use `UsingAsync`. Every wrapper method called on the scoped `gl` inside the callback uses `conn` instead of the internal connection. The binding is `AsyncLocal`-based, so it correctly follows awaits and unwinds on exception.

```csharp
await using var conn = new NpgsqlConnection(gl.Url);
await conn.OpenAsync();
await using var tx = await conn.BeginTransactionAsync();

await gl.UsingAsync(conn, async gl => {
    await gl.DocInsertAsync("events", "{\"type\":\"order.created\"}");
    await gl.IncrAsync("counters", "orders_today");
    // All wrapper calls above run on `conn` — one atomic transaction.
});

await tx.CommitAsync();
```

You can also pass `connection:` explicitly to any wrapper method:

```csharp
await gl.DocInsertAsync("events", "{\"type\":\"x\"}", connection: myConn);
```

## API Reference

### `GoldLapel.StartAsync(upstream, Action<GoldLapelOptions> configure = null)`

Async factory. Spawns the proxy, waits for it to accept connections, eagerly opens an internal `NpgsqlConnection`, and returns a ready `GoldLapel` instance. `await using` disposal calls `DisposeAsync` which stops the proxy.

- `upstream` — Postgres connection string (`postgresql://user:pass@localhost:5432/mydb`)
- `opts.Port` — proxy port (default: `7932`)
- `opts.LogLevel` — proxy log level: `"trace"`, `"debug"`, `"info"`, `"warn"`, `"error"`. The binary defaults to `"warn"` (banner only); `"info"`, `"debug"`, and `"trace"` progressively add detail.
- `opts.Config` — dictionary of proxy config (see [Configuration](#configuration))
- `opts.ExtraArgs` — additional raw CLI flags

### `gl.Url`

Proxy connection string in Npgsql keyword form — pass directly to `new NpgsqlConnection(gl.Url)`.

### `gl.ProxyUrl`

Proxy connection string in URL form (`postgresql://user:pass@localhost:7932/mydb`).

### `gl.Connection`

The internal `NpgsqlConnection` opened by `StartAsync`. Wrapper methods use this by default.

### `gl.DashboardUrl`

Dashboard URL (`http://127.0.0.1:7933`), or `null` if not running. Defaults to `proxy port + 1`; configurable via `opts.Config["dashboardPort"]`, or disabled with `0`.

### `gl.UsingAsync(connection, Func<GoldLapel, Task> action)`

Scopes `action` to use `connection` for every wrapper method called on the passed handle. Scope unwinds on normal return and on exception. Nests; generic `UsingAsync<T>(conn, Func<GoldLapel, Task<T>>)` overload returns a value.

### `gl.DisposeAsync()` (or `await using`)

Stops the proxy (SIGTERM, 5s grace, then SIGKILL) and closes the internal connection. Also available as synchronous `Dispose()`.

### Wrapper methods

Every wrapper method has an `Async` suffix, returns `Task`/`Task<T>`, and accepts an optional `NpgsqlConnection connection = null` named argument. When omitted, the active connection (from `UsingAsync`, or the internal one) is used.

- **Documents** — `DocInsertAsync`, `DocInsertManyAsync`, `DocFindAsync`, `DocFindCursor`, `DocFindOneAsync`, `DocUpdateAsync`, `DocUpdateOneAsync`, `DocDeleteAsync`, `DocDeleteOneAsync`, `DocCountAsync`, `DocFindOneAndUpdateAsync`, `DocFindOneAndDeleteAsync`, `DocDistinctAsync`, `DocCreateIndexAsync`, `DocAggregateAsync`, `DocWatchAsync`, `DocUnwatchAsync`, `DocCreateTtlIndexAsync`, `DocRemoveTtlIndexAsync`, `DocCreateCollectionAsync`, `DocCreateCappedAsync`, `DocRemoveCapAsync`
- **Search** — `SearchAsync`, `SearchFuzzyAsync`, `SearchPhoneticAsync`, `SimilarAsync`, `SuggestAsync`, `FacetsAsync`, `AggregateAsync`, `CreateSearchConfigAsync`, `AnalyzeAsync`, `ExplainScoreAsync`
- **Pub/Sub** — `PublishAsync`, `SubscribeAsync`
- **Queue** — `EnqueueAsync`, `DequeueAsync`
- **Counters** — `IncrAsync`, `GetCounterAsync`
- **Hash** — `HsetAsync`, `HgetAsync`, `HgetallAsync`, `HdelAsync`
- **Sorted set** — `ZaddAsync`, `ZincrbyAsync`, `ZrangeAsync`, `ZrankAsync`, `ZscoreAsync`, `ZremAsync`
- **Geo** — `GeoaddAsync`, `GeoradiusAsync`, `GeodistAsync`
- **Streams** — `StreamAddAsync`, `StreamCreateGroupAsync`, `StreamReadAsync`, `StreamAckAsync`, `StreamClaimAsync`
- **Percolate** — `PercolateAddAsync`, `PercolateAsync`, `PercolateDeleteAsync`
- **Misc** — `CountDistinctAsync`, `ScriptAsync`

`DocFindCursor` stays synchronous (`IEnumerable<Dictionary<string, object>>`) since cursor iteration is pull-style.

## Configuration

```csharp
await using var gl = await GoldLapel.StartAsync("postgresql://user:pass@localhost/mydb",
    opts => {
        opts.Config = new Dictionary<string, object>
        {
            ["mode"] = "waiter",
            ["poolSize"] = 50,
            ["disableMatviews"] = true,
            ["replica"] = new List<string> { "postgresql://user:pass@replica1/mydb" },
        };
    });
```

Keys use `camelCase` and map to CLI flags (`poolSize` → `--pool-size`). Boolean keys are flags — `true` enables them. List keys produce repeated flags.

Unknown keys throw `ArgumentException`. To list valid keys:

```csharp
foreach (var k in GoldLapel.ConfigKeys())
    Console.WriteLine(k);
```

See the [main documentation](https://github.com/goldlapel/goldlapel#setting-reference) for the full configuration reference.

Raw CLI flags can also be passed via `opts.ExtraArgs`:

```csharp
await using var gl = await GoldLapel.StartAsync(
    "postgresql://user:pass@localhost:5432/mydb",
    opts => {
        opts.ExtraArgs = new[] { "--threshold-duration-ms", "200", "--refresh-interval-secs", "30" };
    });
```

Or set environment variables (`GOLDLAPEL_PROXY_PORT`, `GOLDLAPEL_UPSTREAM`, etc.) — the binary reads them automatically.

## Upgrading from v0.1

v0.2.0 is a hard break. Migration:

| v0.1 | v0.2.0 |
|------|--------|
| `using var gl = new GoldLapel(url); gl.StartProxy();` | `await using var gl = await GoldLapel.StartAsync(url);` |
| `GoldLapel.Start(url)` / `GoldLapel.StartConnection(url)` (singletons) | `await GoldLapel.StartAsync(url)` |
| `gl.DocInsert(collection, doc)` | `await gl.DocInsertAsync(collection, doc)` |
| `new GoldLapelOptions { Port = 7932 }` | `opts => { opts.Port = 7932; }` |
| `gl.Url` (URL form, e.g. `postgresql://…`) | `gl.ProxyUrl` (URL form) or `gl.Url` (Npgsql keyword form) |
| `GoldLapel.ProxyUrl` (static singleton accessor) | `gl.ProxyUrl` (per-instance) |
| `new NpgsqlConnection(gl.Url)` required manual conversion | `new NpgsqlConnection(gl.Url)` works directly |

Notable:
- All wrapper methods renamed with `Async` suffix and return `Task<T>` (e.g. `DocInsert` → `DocInsertAsync`).
- Instance construction is factory-only — the public constructor has been removed.
- Singleton static API (`GoldLapel.Start`, `GoldLapel.Stop`, `GoldLapel.ProxyUrl`) has been removed. Hold onto the instance returned by `StartAsync`.
- Every wrapper method accepts an optional `connection:` named argument for per-call overrides. `gl.UsingAsync(conn, ...)` scopes a connection across many calls.

## How It Works

This package bundles the Gold Lapel Rust binary for your platform. When you call `StartAsync`, it:

1. Locates the binary (bundled in NuGet package, on PATH, or via `GOLDLAPEL_BINARY` env var)
2. Spawns it as a subprocess listening on localhost
3. Waits for the port to be ready
4. Opens an internal `NpgsqlConnection` against the proxy
5. Returns the `GoldLapel` instance
6. Stops the subprocess and closes the connection on `await using` / `DisposeAsync`

## Supported Platforms

- Linux x64
- Linux ARM64
- macOS ARM64 (Apple Silicon)
- Windows x64

Targets `netstandard2.0` — compatible with .NET Framework 4.6.2+, .NET Core 2.0+, and all modern .NET.

## Links

- [Website](https://goldlapel.com)
- [Documentation](https://github.com/goldlapel/goldlapel)
