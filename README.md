# GoldLapel

[![Tests](https://github.com/goldlapel/goldlapel-dotnet/actions/workflows/test.yml/badge.svg)](https://github.com/goldlapel/goldlapel-dotnet/actions/workflows/test.yml)

The .NET wrapper for [Gold Lapel](https://goldlapel.com) — a self-optimizing Postgres proxy that caches query results, creates indexes from your query patterns, and keeps the cache correct as your data changes. Zero code changes beyond the connection string.

The wrapper runs the proxy as a managed subprocess: it finds the bundled binary, starts it with your app and stops it when the instance is disposed, translates options into proxy flags, generates the dashboard token, and hands back an Npgsql-ready connection string. It also provides Postgres-backed helpers — search and percolator, a document store, streams, counters, sorted sets, hashes, queues, geo, and pub/sub. Caching happens in the proxy, which serves every client the same way; `gl.Connection` and the connections you open against `gl.Url` are plain `NpgsqlConnection`s.

## Install

```bash
dotnet add package GoldLapel
```

`Npgsql` is installed as a transitive dependency.

## Quickstart

```csharp
using GoldLapel;
using Npgsql;

// Spawn the proxy in front of your upstream DB
await using var gl = await GoldLapel.StartAsync(
    "postgresql://user:pass@localhost:5432/mydb");

// gl.Url is an Npgsql-ready keyword connection string
await using var conn = new NpgsqlConnection(gl.Url);
await conn.OpenAsync();

await using var cmd = new NpgsqlCommand("SELECT * FROM users LIMIT 10", conn);
await using var reader = await cmd.ExecuteReaderAsync();

// `await using` disposes the instance: stops the proxy and closes the internal connection.
```

Point Npgsql at `gl.Url`. Gold Lapel sits between your app and your DB, caching results and creating indexes from your query patterns. Connections are tagged `application_name=goldlapel:dotnet:<version>` so they're recognisable in `pg_stat_activity`.

The proxy listens on two ports: the proxy itself (`opts.ProxyPort`) and the dashboard (`opts.DashboardPort`, default proxy port + 1; `0` disables it). Leave `ProxyPort` unset and the wrapper picks the lowest free pair from 7932 up, so several databases can each have a proxy in one process; `gl.ProxyPort` and `gl.DashboardPort` report what was chosen. Starting an upstream that is already running in the process returns another handle on the same proxy, which stops when the last handle is disposed. An explicit port that another of your proxies, or another program, already holds is refused with an error naming it.

TLS settings in your URL (`sslmode`, `sslrootcert`, `channel_binding`, ...) apply to the proxy's connection to your database. `gl.Url` and `gl.ProxyUrl` leave them out, because the proxy speaks plain TCP to your app on localhost unless you give it a certificate (`tlsCert`/`tlsKey` in `Config`).

Scoped transactions via `gl.UsingAsync(conn, ...)`, per-call `connection:` overrides, and the full wrapper surface (`gl.Documents.<Verb>Async`, `gl.Streams.<Verb>Async`, search, Redis replacement) are in the docs.

## Dashboard

Gold Lapel exposes a live dashboard at `gl.DashboardUrl`:

```csharp
Console.WriteLine(gl.DashboardUrl);
// -> http://127.0.0.1:7933
```

## Supported platforms

Linux x64, Linux ARM64, macOS ARM64 (Apple Silicon), Windows x64. Targets `netstandard2.0` — compatible with .NET Framework 4.6.2+, .NET Core 2.0+, and all modern .NET.

## Documentation

Full API reference, configuration, async patterns, upgrading from v0.1, and production deployment: https://goldlapel.com/docs/dotnet

## Uninstalling

Before removing the package, drop Gold Lapel's helper schema and indexes from your Postgres:

```bash
goldlapel clean
```

Then remove the package and any local state:

```bash
dotnet remove package GoldLapel
rm -rf ~/.goldlapel
rm -f goldlapel.toml     # only if you wrote one
```

Cancelling your subscription does not delete your data — only Gold Lapel's helper schema and indexes go away.

## License

MIT. See `LICENSE`.
