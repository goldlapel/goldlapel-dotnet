# Gold Lapel

Self-optimizing Postgres proxy — automatic materialized views and indexes, with an L1 native cache that serves repeated reads in microseconds. Zero code changes required.

Gold Lapel sits between your app and Postgres, watches query patterns, and automatically creates materialized views and indexes to make your database faster. Port 7932 (79 = atomic number for gold, 32 from Postgres).

## Install

```
dotnet add package GoldLapel
```

## Quick Start

```csharp
using GoldLapel;

// Create and start the proxy — IDisposable for automatic cleanup
using var gl = new GoldLapel("postgresql://user:pass@localhost:5432/mydb");
var conn = gl.Start();

// Use the connection directly — no NpgsqlConnection needed
var cmd = conn.CreateCommand();
cmd.CommandText = "SELECT * FROM users WHERE id = @id";
cmd.Parameters.AddWithValue("id", 42);
var reader = cmd.ExecuteReader();
```

## API

### `new GoldLapel(upstream)` / `new GoldLapel(upstream, options)`

Creates a new Gold Lapel proxy instance. Implements `IDisposable` for automatic cleanup.

- `upstream` — your Postgres connection string (e.g. `postgresql://user:pass@localhost:5432/mydb`)
- `options.Port` — proxy port (default: 7932)
- `options.ExtraArgs` — additional CLI flags passed to the binary (e.g. `"--threshold-impact", "5000"`)

### `gl.Start()`

Starts the proxy and returns a database connection with L1 cache.

### `gl.Stop()`

Stops the proxy. Also called automatically via `Dispose()`.

### `gl.ProxyUrl`

Returns the current proxy URL, or `null` if not running.

### `gl.DashboardUrl`

Returns the dashboard URL (e.g. `http://127.0.0.1:7933`), or `null` if not running. Default port is 7933. Set `dashboardPort` in config to customize, or `0` to disable.

### `gl.ConfigKeys()`

Returns all valid config key names as an `IReadOnlyCollection<string>`.

## Configuration

Pass a config dictionary via options:

```csharp
using GoldLapel;

using var gl = new GoldLapel("postgresql://user:pass@localhost/mydb",
    new GoldLapelOptions
    {
        Config = new Dictionary<string, object>
        {
            ["mode"] = "waiter",
            ["poolSize"] = 50,
            ["disableMatviews"] = true,
            ["replica"] = new List<string> { "postgresql://user:pass@replica1/mydb" },
        }
    });
var conn = gl.Start();
```

Keys use `camelCase` and map to CLI flags (`poolSize` → `--pool-size`). Boolean keys are flags — `true` enables them. List keys produce repeated flags.

Unknown keys throw `ArgumentException`. To see all valid keys:

```csharp
GoldLapel.ConfigKeys()
```

For the full configuration reference, see the [main documentation](https://github.com/goldlapel/goldlapel#setting-reference).

You can also pass raw CLI flags via `ExtraArgs`:

```csharp
using var gl = new GoldLapel(
    "postgresql://user:pass@localhost:5432/mydb",
    new GoldLapelOptions
    {
        ExtraArgs = new[] { "--threshold-duration-ms", "200", "--refresh-interval-secs", "30" }
    });
var conn = gl.Start();
```

Or set environment variables (`GOLDLAPEL_PROXY_PORT`, `GOLDLAPEL_UPSTREAM`, etc.) — the binary reads them automatically.

## How It Works

This package bundles the Gold Lapel Rust binary for your platform. When you call `gl.Start()`, it:

1. Locates the binary (bundled in NuGet package, on PATH, or via `GOLDLAPEL_BINARY` env var)
2. Spawns it as a subprocess listening on localhost
3. Waits for the port to be ready
4. Returns a database connection with L1 native cache built in
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
