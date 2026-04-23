using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using Xunit;
using GL = GoldLapel.GoldLapel;
using GoldLapel;

namespace GoldLapel.Tests
{
    // ── FindBinary ────────────────────────────────────────────

    // Shares the "EnvVarTests" collection with IntegrationTests so xUnit serializes
    // them — FindBinaryTest mutates the process-global GOLDLAPEL_BINARY env var,
    // which IntegrationTests reads via FindBinary() and would otherwise see in a
    // poisoned state during parallel test runs.
    [Collection("EnvVarTests")]
    public class FindBinaryTest : IDisposable
    {
        private string? _origBinary;
        private string? _origPath;

        public FindBinaryTest()
        {
            _origBinary = Environment.GetEnvironmentVariable("GOLDLAPEL_BINARY");
            _origPath = Environment.GetEnvironmentVariable("PATH");
        }

        public void Dispose()
        {
            SetEnv("GOLDLAPEL_BINARY", _origBinary);
            SetEnv("PATH", _origPath);
        }

        [Fact]
        public void EnvVarOverride()
        {
            var tmp = Path.GetTempFileName();
            try
            {
                SetEnv("GOLDLAPEL_BINARY", tmp);
                Assert.Equal(tmp, GL.FindBinary());
            }
            finally
            {
                File.Delete(tmp);
            }
        }

        [Fact]
        public void EnvVarMissingFileThrows()
        {
            SetEnv("GOLDLAPEL_BINARY", "/nonexistent/goldlapel");
            var ex = Assert.Throws<InvalidOperationException>(() => GL.FindBinary());
            Assert.Contains("GOLDLAPEL_BINARY", ex.Message);
        }

        [Fact]
        public void NotFoundThrows()
        {
            SetEnv("GOLDLAPEL_BINARY", null);
            SetEnv("PATH", "/nonexistent-dir-for-test");
            var ex = Assert.Throws<InvalidOperationException>(() => GL.FindBinary());
            Assert.Contains("Gold Lapel binary not found", ex.Message);
        }

        private static void SetEnv(string key, string? value)
        {
            Environment.SetEnvironmentVariable(key, value);
        }
    }

    // ── MakeProxyUrl ──────────────────────────────────────────

    public class MakeProxyUrlTest
    {
        [Fact]
        public void PostgresqlUrl()
        {
            Assert.Equal(
                "postgresql://user:pass@localhost:7932/mydb",
                GL.MakeProxyUrl("postgresql://user:pass@dbhost:5432/mydb", 7932)
            );
        }

        [Fact]
        public void PostgresUrl()
        {
            Assert.Equal(
                "postgres://user:pass@localhost:7932/mydb",
                GL.MakeProxyUrl("postgres://user:pass@remote.aws.com:5432/mydb", 7932)
            );
        }

        [Fact]
        public void PgUrlWithoutPort()
        {
            Assert.Equal(
                "postgresql://user:pass@localhost:7932/mydb",
                GL.MakeProxyUrl("postgresql://user:pass@host.aws.com/mydb", 7932)
            );
        }

        [Fact]
        public void PgUrlWithoutPortOrPath()
        {
            Assert.Equal(
                "postgresql://user:pass@localhost:7932",
                GL.MakeProxyUrl("postgresql://user:pass@host.aws.com", 7932)
            );
        }

        [Fact]
        public void BareHostPort()
        {
            Assert.Equal("localhost:7932", GL.MakeProxyUrl("dbhost:5432", 7932));
        }

        [Fact]
        public void BareHost()
        {
            Assert.Equal("localhost:7932", GL.MakeProxyUrl("dbhost", 7932));
        }

        [Fact]
        public void PreservesQueryParams()
        {
            Assert.Equal(
                "postgresql://user:pass@localhost:7932/mydb?sslmode=require",
                GL.MakeProxyUrl("postgresql://user:pass@remote:5432/mydb?sslmode=require", 7932)
            );
        }

        [Fact]
        public void PreservesPercentEncodedPassword()
        {
            Assert.Equal(
                "postgresql://user:p%40ss@localhost:7932/mydb",
                GL.MakeProxyUrl("postgresql://user:p%40ss@remote:5432/mydb", 7932)
            );
        }

        [Fact]
        public void NoUserinfo()
        {
            Assert.Equal(
                "postgresql://localhost:7932/mydb",
                GL.MakeProxyUrl("postgresql://dbhost:5432/mydb", 7932)
            );
        }

        [Fact]
        public void NoUserinfoNoPort()
        {
            Assert.Equal(
                "postgresql://localhost:7932/mydb",
                GL.MakeProxyUrl("postgresql://dbhost/mydb", 7932)
            );
        }

        [Fact]
        public void LocalhostStaysLocalhost()
        {
            Assert.Equal(
                "postgresql://user:pass@localhost:7932/mydb",
                GL.MakeProxyUrl("postgresql://user:pass@localhost:5432/mydb", 7932)
            );
        }

        [Fact]
        public void AtSignInPasswordWithPort()
        {
            Assert.Equal(
                "postgresql://user:p@ss@localhost:7932/mydb",
                GL.MakeProxyUrl("postgresql://user:p@ss@host:5432/mydb", 7932)
            );
        }

        [Fact]
        public void AtSignInPasswordWithoutPort()
        {
            Assert.Equal(
                "postgresql://user:p@ss@localhost:7932/mydb",
                GL.MakeProxyUrl("postgresql://user:p@ss@host/mydb", 7932)
            );
        }

        [Fact]
        public void AtSignInPasswordWithQueryParams()
        {
            Assert.Equal(
                "postgresql://user:p@ss@localhost:7932/mydb?sslmode=require&param=val@ue",
                GL.MakeProxyUrl("postgresql://user:p@ss@host:5432/mydb?sslmode=require&param=val@ue", 7932)
            );
        }
    }

    // ── WaitForPort ───────────────────────────────────────────

    public class WaitForPortTest
    {
        [Fact]
        public void OpenPortReturnsTrue()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            var port = ((IPEndPoint)listener.LocalEndpoint).Port;
            try
            {
                Assert.True(GL.WaitForPort("127.0.0.1", port, 1000));
            }
            finally
            {
                listener.Stop();
            }
        }

        [Fact]
        public void ClosedPortTimesOut()
        {
            Assert.False(GL.WaitForPort("127.0.0.1", 19999, 200));
        }
    }

    // ── PollForPortAsync ──────────────────────────────────────
    //
    // PollForPortAsync is the startup-readiness loop extracted from SpawnAsync.
    // Regression coverage for the v0.2 double-budget bug: the previous
    // SpawnAsync wrapped a looping WaitForPortAsync inside its own outer
    // stopwatch loop, so total elapsed time could reach budget * N (each
    // outer iteration consumed another full inner budget). These tests
    // assert the single-budget contract: total elapsed <= budget (+ small
    // slack for the final per-attempt connect + thread scheduling).

    public class PollForPortAsyncTest
    {
        [Fact]
        public async Task ReachablePortSucceedsInsideBudget()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            var port = ((IPEndPoint)listener.LocalEndpoint).Port;
            try
            {
                var sw = Stopwatch.StartNew();
                var ok = await GL.PollForPortAsync("127.0.0.1", port, 2000);
                sw.Stop();

                Assert.True(ok);
                // Should return almost immediately for a listening port.
                Assert.True(sw.ElapsedMilliseconds < 2000,
                    $"expected fast success, took {sw.ElapsedMilliseconds}ms");
            }
            finally
            {
                listener.Stop();
            }
        }

        [Fact]
        public async Task UnreachablePortFailsAtApproximatelyBudget()
        {
            // Budget of 600ms. The old bug allowed total elapsed to reach
            // several multiples of the budget (each outer iteration ran a
            // full inner 500ms loop). With the single-loop fix, total time
            // is bounded by budget + one per-attempt connect timeout.
            const long budgetMs = 600;
            var sw = Stopwatch.StartNew();
            var ok = await GL.PollForPortAsync("127.0.0.1", 19999, budgetMs);
            sw.Stop();

            Assert.False(ok);
            // Upper bound: budget + one per-attempt connect timeout (capped at
            // 500ms by PollForPortAsync) + generous scheduling slack. The old
            // bug would have produced elapsed >= budget * 2 here.
            Assert.True(sw.ElapsedMilliseconds < budgetMs + 1500,
                $"expected failure near {budgetMs}ms budget, took {sw.ElapsedMilliseconds}ms");
        }

        [Fact]
        public async Task AbortCallbackShortCircuits()
        {
            // Simulate the "child process exited" abort path: the loop must
            // return false promptly without waiting out the full budget.
            var sw = Stopwatch.StartNew();
            var ok = await GL.PollForPortAsync("127.0.0.1", 19999, 5000, () => true);
            sw.Stop();

            Assert.False(ok);
            Assert.True(sw.ElapsedMilliseconds < 1000,
                $"expected fast abort, took {sw.ElapsedMilliseconds}ms");
        }
    }

    // ── Options / construction ────────────────────────────────

    public class OptionsTest
    {
        [Fact]
        public void DefaultPort()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            Assert.Equal(7932, gl.ProxyPort);
        }

        [Fact]
        public void CustomPort()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions { ProxyPort = 9000 });
            Assert.Equal(9000, gl.ProxyPort);
        }

        [Fact]
        public void NotRunningInitially()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            Assert.False(gl.IsRunning);
            Assert.Null(gl.Url);
        }

        [Fact]
        public async System.Threading.Tasks.Task StartAsyncNullUpstreamThrows()
        {
            await Assert.ThrowsAsync<ArgumentNullException>(() => GL.StartAsync(null));
        }

        [Fact]
        public void LogLevelDebugMapsToDoubleVerbose()
        {
            // The proxy binary accepts -v/-vv/-vvv (count-based), not --log-level.
            // LogLevel "debug" → -vv.
            Assert.Equal("-vv", GL.LogLevelToVerboseFlag("debug"));
        }

        [Fact]
        public void LogLevelTraceMapsToTripleVerbose()
        {
            Assert.Equal("-vvv", GL.LogLevelToVerboseFlag("trace"));
        }

        [Fact]
        public void LogLevelInfoMapsToSingleVerbose()
        {
            Assert.Equal("-v", GL.LogLevelToVerboseFlag("info"));
        }

        [Theory]
        [InlineData("warn")]
        [InlineData("warning")]
        [InlineData("error")]
        public void LogLevelWarnOrErrorEmitsNoFlag(string level)
        {
            // warn/error are the default level — no extra flag needed.
            Assert.Null(GL.LogLevelToVerboseFlag(level));
        }

        [Fact]
        public void LogLevelIsCaseInsensitive()
        {
            Assert.Equal("-vv", GL.LogLevelToVerboseFlag("DEBUG"));
        }

        [Fact]
        public void LogLevelInvalidRaises()
        {
            var ex = Assert.Throws<ArgumentException>(() => GL.LogLevelToVerboseFlag("loud"));
            Assert.Contains("logLevel must be one of", ex.Message);
        }

        [Fact]
        public void LogLevelNeverEmitsLongFlag()
        {
            // Regression guard: the proxy binary does not accept --log-level.
            foreach (var lvl in new[] { "trace", "debug", "info", "warn", "error" })
            {
                var flag = GL.LogLevelToVerboseFlag(lvl);
                Assert.True(flag == null || !flag.StartsWith("--log-level"));
            }
        }

        [Fact]
        public void LogLevelInConfigMapIsRejected()
        {
            // Regression guard: logLevel was promoted out of the Config map
            // to the top-level LogLevel option. Passing it through Config
            // must raise (no silent fallback).
            var config = new Dictionary<string, object> { { "logLevel", "info" } };
            Assert.Throws<ArgumentException>(() => GL.ConfigToArgs(config));
        }

        // ─── Mesh startup options ─────────────────────────────────────

        [Fact]
        public void MeshDefaultsToFalse()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            Assert.False(gl.IsMesh);
            Assert.Null(gl.MeshTag);
        }

        [Fact]
        public void MeshOptionStored()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions { Mesh = true, MeshTag = "prod-east" });
            Assert.True(gl.IsMesh);
            Assert.Equal("prod-east", gl.MeshTag);
        }

        [Fact]
        public void MeshTagEmptyStringNormalizedToNull()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions { Mesh = true, MeshTag = "" });
            Assert.Null(gl.MeshTag);
        }

        [Fact]
        public void MeshInConfigMapIsRejected()
        {
            // Regression guard: Mesh / MeshTag are top-level canonical-surface
            // options, never valid inside the structured config map.
            var meshCfg = new Dictionary<string, object> { { "mesh", true } };
            Assert.Throws<ArgumentException>(() => GL.ConfigToArgs(meshCfg));
            var tagCfg = new Dictionary<string, object> { { "meshTag", "prod" } };
            Assert.Throws<ArgumentException>(() => GL.ConfigToArgs(tagCfg));
        }
    }

    // ── DashboardUrl ───────────────────────────────────────

    public class DashboardUrlTest
    {
        [Fact]
        public void DefaultDashboardPort()
        {
            Assert.Equal(7933, GL.DefaultDashboardPort);
        }

        [Fact]
        public void CustomDashboardPort()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions { DashboardPort = 9090 });
            Assert.Null(gl.DashboardUrl);
        }

        [Fact]
        public void DashboardUrlNullWhenNotRunning()
        {
            // Cross-wrapper contract: DashboardUrl reports only while the proxy
            // process is live. Pre-start (and post-dispose), it is null. This
            // matches Python (dashboard_url), Go (DashboardURL), Java
            // (getDashboardUrl), and PHP (getDashboardUrl). If this assertion
            // flips, update the DashboardUrl XML doc too.
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            Assert.Null(gl.DashboardUrl);
        }

        [Fact]
        public void DashboardUrlNullPreStartEvenWithExplicitPort()
        {
            // Regression guard: a user-supplied dashboardPort must not cause
            // DashboardUrl to synthesize a URL before the proxy is running.
            // The URL only becomes observable once the process binds the port.
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions
                {
                    ProxyPort = 17932,
                    DashboardPort = 9999
                });
            Assert.False(gl.IsRunning);
            Assert.Null(gl.DashboardUrl);
        }

        [Fact]
        public void DashboardPortFromTopLevelOption()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions { DashboardPort = 8888 });
            Assert.Null(gl.DashboardUrl);
            Assert.False(gl.IsRunning);
            Assert.Equal(8888, gl.DashboardPort);
        }

        [Fact]
        public void DashboardPortDerivesFromCustomProxyPort()
        {
            // When only ProxyPort is set, dashboard defaults to proxyPort + 1
            // (not the hardcoded 7933).
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions { ProxyPort = 17932 });
            Assert.Equal(17933, gl.DashboardPort);
        }

        [Fact]
        public void ExplicitDashboardPortOverridesDerivation()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions
                {
                    ProxyPort = 17932,
                    DashboardPort = 9999
                });
            Assert.Equal(9999, gl.DashboardPort);
        }
    }

    // ── ConfigKeys ────────────────────────────────────────────

    public class ConfigKeysTest
    {
        [Fact]
        public void ReturnsNonEmptyCollection()
        {
            var keys = GL.ConfigKeys();
            Assert.NotNull(keys);
            Assert.NotEmpty(keys);
        }

        [Fact]
        public void ContainsKnownKeys()
        {
            // Tuning knobs still live in the structured Config map.
            // Top-level options (mode, logLevel, dashboardPort, etc.) do not.
            var keys = GL.ConfigKeys();
            Assert.Contains("poolSize", keys);
            Assert.Contains("disableMatviews", keys);
            Assert.Contains("replica", keys);
        }

        [Fact]
        public void DoesNotContainPromotedTopLevelKeys()
        {
            // Canonical surface: logLevel, dashboardPort, invalidationPort,
            // mode, client, config, license are top-level options on
            // GoldLapelOptions, not structured-config keys. ConfigKeys()
            // reports only the tuning knobs that remain inside the `Config`
            // map.
            var keys = GL.ConfigKeys();
            Assert.DoesNotContain("logLevel", keys);
            Assert.DoesNotContain("dashboardPort", keys);
            Assert.DoesNotContain("invalidationPort", keys);
            Assert.DoesNotContain("mode", keys);
            Assert.DoesNotContain("client", keys);
            Assert.DoesNotContain("config", keys);
            Assert.DoesNotContain("license", keys);
        }

        [Fact]
        public void DoesNotContainUnknownKeys()
        {
            var keys = GL.ConfigKeys();
            Assert.DoesNotContain("notARealKey", keys);
        }
    }

    // ── GracefulStop ──────────────────────────────────────────

    public class GracefulStopTest
    {
        // SendSignal relies on POSIX kill(2); the underlying P/Invoke is only
        // wired up for non-Windows platforms. Mark as SkippableFact so a
        // Windows run reports "Skipped" rather than a silent Fact pass that
        // never exercises any assertion.
        [SkippableFact]
        public void SendSignalToSelf()
        {
            Skip.If(
                System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(
                    System.Runtime.InteropServices.OSPlatform.Windows),
                "POSIX-only: SendSignal uses kill(2), unavailable on Windows");

            var pid = System.Diagnostics.Process.GetCurrentProcess().Id;
            Assert.True(GL.SendSignal(pid, 0)); // signal 0 = existence check
        }

        [SkippableFact]
        public void SendSignalToNonexistentPid()
        {
            Skip.If(
                System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(
                    System.Runtime.InteropServices.OSPlatform.Windows),
                "POSIX-only: SendSignal uses kill(2), unavailable on Windows");

            Assert.False(GL.SendSignal(4194304, 0));
        }

        [Fact]
        public void DisposeIsIdempotent()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            gl.Dispose();
            gl.Dispose(); // second call should not throw
        }

        // DisposeAsync is the .NET stop-idempotency equivalent: in async code,
        // `await using` calls DisposeAsync, not Dispose. Double-DisposeAsync is
        // reachable via atexit-style cleanup, AppDomain.ProcessExit handlers,
        // cancellation-token teardown, and test class teardown loops. A buggy
        // second-DisposeAsync (re-closing a null _conn, re-stopping a null
        // _process) would mask the root error or crash the test host.
        [Fact]
        public async Task StopAsync_IsIdempotent()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            await gl.DisposeAsync();
            await gl.DisposeAsync(); // second call must not throw

            // Internal state is fully torn down after first DisposeAsync; the
            // second call observes _disposed=true and returns early.
            var disposedField = typeof(GL).GetField("_disposed",
                System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
            Assert.NotNull(disposedField);
            Assert.True((bool)disposedField.GetValue(gl));

            var processField = typeof(GL).GetField("_process",
                System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
            Assert.NotNull(processField);
            Assert.Null(processField.GetValue(gl));

            var connField = typeof(GL).GetField("_conn",
                System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
            Assert.NotNull(connField);
            Assert.Null(connField.GetValue(gl));

            var proxyUrlField = typeof(GL).GetField("_proxyUrl",
                System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
            Assert.NotNull(proxyUrlField);
            Assert.Null(proxyUrlField.GetValue(gl));

            Assert.False(gl.IsRunning);
            Assert.Null(gl.ProxyUrl);
            Assert.Null(gl.Url);
        }

        // Mixed sync/async teardown is reachable when user code awaits
        // DisposeAsync then a finally-block also calls Dispose (or vice-versa).
        // The _disposed flag must cover both code paths.
        [Fact]
        public async Task DisposeAsync_ThenDispose_IsIdempotent()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            await gl.DisposeAsync();
            gl.Dispose(); // sync follow-up must not throw
        }

        [Fact]
        public async Task Dispose_ThenDisposeAsync_IsIdempotent()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            gl.Dispose();
            await gl.DisposeAsync(); // async follow-up must not throw
        }
    }

    // ── ConfigToArgs ─────────────────────────────────────────

    public class ConfigToArgsTest
    {
        [Fact]
        public void ConfigToArgs_StringValue()
        {
            var config = new Dictionary<string, object> { { "poolMode", "transaction" } };
            var args = GL.ConfigToArgs(config);
            Assert.Equal(new List<string> { "--pool-mode", "transaction" }, args);
        }

        [Fact]
        public void ConfigToArgs_NumericValue()
        {
            var config = new Dictionary<string, object> { { "poolSize", 20 } };
            var args = GL.ConfigToArgs(config);
            Assert.Equal(new List<string> { "--pool-size", "20" }, args);
        }

        [Fact]
        public void ConfigToArgs_BooleanTrue()
        {
            var config = new Dictionary<string, object> { { "disablePool", true } };
            var args = GL.ConfigToArgs(config);
            Assert.Equal(new List<string> { "--disable-pool" }, args);
        }

        [Fact]
        public void ConfigToArgs_BooleanFalse()
        {
            var config = new Dictionary<string, object> { { "disablePool", false } };
            var args = GL.ConfigToArgs(config);
            Assert.Empty(args);
        }

        [Fact]
        public void ConfigToArgs_ListValue()
        {
            var config = new Dictionary<string, object>
            {
                { "replica", new List<string> { "host1:5432", "host2:5432" } }
            };
            var args = GL.ConfigToArgs(config);
            Assert.Equal(new List<string> { "--replica", "host1:5432", "--replica", "host2:5432" }, args);
        }

        [Fact]
        public void ConfigToArgs_ExcludeTablesList()
        {
            var config = new Dictionary<string, object>
            {
                { "excludeTables", new[] { "logs", "sessions" } }
            };
            var args = GL.ConfigToArgs(config);
            Assert.Equal(
                new List<string> { "--exclude-tables", "logs", "--exclude-tables", "sessions" },
                args
            );
        }

        [Fact]
        public void ConfigToArgs_UnknownKeyThrows()
        {
            var config = new Dictionary<string, object> { { "notARealKey", "val" } };
            var ex = Assert.Throws<ArgumentException>(() => GL.ConfigToArgs(config));
            Assert.Contains("Unknown config key: notARealKey", ex.Message);
        }

        [Fact]
        public void ConfigToArgs_MultipleKeys()
        {
            var config = new Dictionary<string, object>
            {
                { "poolMode", "transaction" },
                { "poolSize", 10 },
                { "disableRewrite", true }
            };
            var args = GL.ConfigToArgs(config);
            Assert.Contains("--pool-mode", args);
            Assert.Contains("transaction", args);
            Assert.Contains("--pool-size", args);
            Assert.Contains("10", args);
            Assert.Contains("--disable-rewrite", args);
            Assert.Equal(5, args.Count);
        }

        [Fact]
        public void ConfigToArgs_EmptyConfig()
        {
            var config = new Dictionary<string, object>();
            var args = GL.ConfigToArgs(config);
            Assert.Empty(args);
        }

        [Fact]
        public void ConfigToArgs_NullConfig()
        {
            var args = GL.ConfigToArgs(null);
            Assert.Empty(args);
        }

        [Fact]
        public void ConfigToArgs_BooleanNonBoolThrows()
        {
            var config = new Dictionary<string, object> { { "disablePool", "yes" } };
            var ex = Assert.Throws<ArgumentException>(() => GL.ConfigToArgs(config));
            Assert.Contains("must be a boolean", ex.Message);
        }

        [Fact]
        public void ConfigToArgs_OptionsIntegration()
        {
            var options = new GoldLapelOptions
            {
                Mode = "waiter",
                Config = new Dictionary<string, object>
                {
                    { "disablePool", true }
                }
            };
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb", options);
            Assert.Equal(7932, gl.ProxyPort);
            Assert.Equal("waiter", options.Mode);
        }
    }
}
