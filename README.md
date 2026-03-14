# Gold Lapel

Self-optimizing Postgres proxy — automatic materialized views and indexes. Zero code changes required.

Gold Lapel sits between your app and Postgres, watches query patterns, and automatically creates materialized views and indexes to make your database faster. Port 7932 (79 = atomic number for gold, 32 from Postgres).

## Install

```
dotnet add package GoldLapel
```

## Quick Start

```csharp
using GoldLapel;

// Start the proxy — returns a connection string pointing at Gold Lapel
var url = GoldLapel.Start("postgresql://user:pass@localhost:5432/mydb");

// Use the URL with any Postgres driver
using var conn = new NpgsqlConnection(url);

// Or Entity Framework, Dapper, ADO.NET — anything that speaks Postgres
```

Gold Lapel is driver-agnostic. `Start()` returns a connection string (`postgresql://...@localhost:7932/...`) that works with any Postgres driver or ORM.

## API

### `GoldLapel.Start(upstream)`
### `GoldLapel.Start(upstream, options)`

Starts the Gold Lapel proxy and returns the proxy connection string.

- `upstream` — your Postgres connection string (e.g. `postgresql://user:pass@localhost:5432/mydb`)
- `options.Port` — proxy port (default: 7932)
- `options.ExtraArgs` — additional CLI flags passed to the binary (e.g. `"--threshold-impact", "5000"`)

### `GoldLapel.Stop()`

Stops the proxy. Also called automatically on process exit.

### `GoldLapel.ProxyUrl`

Returns the current proxy URL, or `null` if not running.

### `GoldLapel.DashboardProxyUrl`

Returns the dashboard URL (e.g. `http://127.0.0.1:7933`), or `null` if not running. Default port is 7933. Set `dashboardPort` in config to customize, or `0` to disable.

### `new GoldLapel(upstream)` / `new GoldLapel(upstream, options)`

Instance API for managing multiple proxies. Implements `IDisposable` for automatic cleanup:

```csharp
using GoldLapel;

using var proxy = new GoldLapel(
    "postgresql://user:pass@localhost:5432/mydb",
    new GoldLapelOptions { Port = 7932 });
var url = proxy.StartProxy();
// proxy.StopProxy() called automatically via Dispose
```

### `GoldLapel.ConfigKeys()`

Returns all valid config key names as an `IReadOnlyCollection<string>`.

## Configuration

Pass a config dictionary via options:

```csharp
using GoldLapel;

var url = GoldLapel.GoldLapel.Start("postgresql://user:pass@localhost/mydb",
    new GoldLapelOptions
    {
        Config = new Dictionary<string, object>
        {
            ["mode"] = "butler",
            ["poolSize"] = 50,
            ["disableMatviews"] = true,
            ["replica"] = new List<string> { "postgresql://user:pass@replica1/mydb" },
        }
    });
```

Keys use `camelCase` and map to CLI flags (`poolSize` → `--pool-size`). Boolean keys are flags — `true` enables them. List keys produce repeated flags.

Unknown keys throw `ArgumentException`. To see all valid keys:

```csharp
GoldLapel.GoldLapel.ConfigKeys()
```

For the full configuration reference, see the [main documentation](https://github.com/goldlapel/goldlapel#setting-reference).

You can also pass raw CLI flags via `ExtraArgs`:

```csharp
var url = GoldLapel.GoldLapel.Start(
    "postgresql://user:pass@localhost:5432/mydb",
    new GoldLapelOptions
    {
        ExtraArgs = new[] { "--threshold-duration-ms", "200", "--refresh-interval-secs", "30" }
    });
```

Or set environment variables (`GOLDLAPEL_PORT`, `GOLDLAPEL_UPSTREAM`, etc.) — the binary reads them automatically.

## How It Works

This package bundles the Gold Lapel Rust binary for your platform. When you call `Start()`, it:

1. Locates the binary (bundled in NuGet package, on PATH, or via `GOLDLAPEL_BINARY` env var)
2. Spawns it as a subprocess listening on localhost
3. Waits for the port to be ready
4. Returns a connection string pointing at the proxy
5. Cleans up automatically on process exit

The binary does all the work — this wrapper just manages its lifecycle.

## Supported Platforms

- Linux x64
- Linux ARM64
- macOS ARM64 (Apple Silicon)
- Windows x64

## Links

- [Website](https://goldlapel.com)
- [Documentation](https://github.com/goldlapel/goldlapel)
