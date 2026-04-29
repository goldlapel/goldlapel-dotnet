# GoldLapel

[![Tests](https://github.com/goldlapel/goldlapel-dotnet/actions/workflows/test.yml/badge.svg)](https://github.com/goldlapel/goldlapel-dotnet/actions/workflows/test.yml)

The .NET wrapper for [Gold Lapel](https://goldlapel.com) — a self-optimizing Postgres proxy that watches query patterns and creates materialized views + indexes automatically. Zero code changes beyond the connection string.

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

Point Npgsql at `gl.Url`. Gold Lapel sits between your app and your DB, watching query patterns and creating materialized views + indexes automatically. Zero code changes beyond the connection string.

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

Before removing the package, drop Gold Lapel's helper schema and cached matviews from your Postgres:

```bash
goldlapel clean
```

Then remove the package and any local state:

```bash
dotnet remove package GoldLapel
rm -rf ~/.goldlapel
rm -f goldlapel.toml     # only if you wrote one
```

Cancelling your subscription does not delete your data — only Gold Lapel's helper schema and cached matviews go away.

## License

MIT. See `LICENSE`.
