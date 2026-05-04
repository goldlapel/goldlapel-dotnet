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
    //
    // The wrapper appends `application_name=goldlapel:dotnet:<version>` to
    // every rewritten URL so the proxy can classify wrapper-vs-raw traffic
    // and skip L2 result cache for wrappers (they have their own L1).
    // PGAPPNAME is cleared per test for deterministic URLs.

    [Collection("EnvVarTests")]
    public class MakeProxyUrlTest : IDisposable
    {
        private readonly string? _origPgAppName;

        public MakeProxyUrlTest()
        {
            _origPgAppName = Environment.GetEnvironmentVariable("PGAPPNAME");
            Environment.SetEnvironmentVariable("PGAPPNAME", null);
        }

        public void Dispose()
        {
            Environment.SetEnvironmentVariable("PGAPPNAME", _origPgAppName);
        }

        private static string AppNameSuffix => "application_name=" + GL.ApplicationNameMarker();

        [Fact]
        public void PostgresqlUrl()
        {
            Assert.Equal(
                "postgresql://user:pass@localhost:7932/mydb?" + AppNameSuffix,
                GL.MakeProxyUrl("postgresql://user:pass@dbhost:5432/mydb", 7932)
            );
        }

        [Fact]
        public void PostgresUrl()
        {
            Assert.Equal(
                "postgres://user:pass@localhost:7932/mydb?" + AppNameSuffix,
                GL.MakeProxyUrl("postgres://user:pass@remote.aws.com:5432/mydb", 7932)
            );
        }

        [Fact]
        public void PgUrlWithoutPort()
        {
            Assert.Equal(
                "postgresql://user:pass@localhost:7932/mydb?" + AppNameSuffix,
                GL.MakeProxyUrl("postgresql://user:pass@host.aws.com/mydb", 7932)
            );
        }

        [Fact]
        public void PgUrlWithoutPortOrPath()
        {
            Assert.Equal(
                "postgresql://user:pass@localhost:7932?" + AppNameSuffix,
                GL.MakeProxyUrl("postgresql://user:pass@host.aws.com", 7932)
            );
        }

        [Fact]
        public void BareHostPort()
        {
            // Bare-host form skips the marker — atypical caller path.
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
                "postgresql://user:pass@localhost:7932/mydb?sslmode=require&" + AppNameSuffix,
                GL.MakeProxyUrl("postgresql://user:pass@remote:5432/mydb?sslmode=require", 7932)
            );
        }

        [Fact]
        public void PreservesPercentEncodedPassword()
        {
            Assert.Equal(
                "postgresql://user:p%40ss@localhost:7932/mydb?" + AppNameSuffix,
                GL.MakeProxyUrl("postgresql://user:p%40ss@remote:5432/mydb", 7932)
            );
        }

        [Fact]
        public void NoUserinfo()
        {
            Assert.Equal(
                "postgresql://localhost:7932/mydb?" + AppNameSuffix,
                GL.MakeProxyUrl("postgresql://dbhost:5432/mydb", 7932)
            );
        }

        [Fact]
        public void NoUserinfoNoPort()
        {
            Assert.Equal(
                "postgresql://localhost:7932/mydb?" + AppNameSuffix,
                GL.MakeProxyUrl("postgresql://dbhost/mydb", 7932)
            );
        }

        [Fact]
        public void LocalhostStaysLocalhost()
        {
            Assert.Equal(
                "postgresql://user:pass@localhost:7932/mydb?" + AppNameSuffix,
                GL.MakeProxyUrl("postgresql://user:pass@localhost:5432/mydb", 7932)
            );
        }

        [Fact]
        public void AtSignInPasswordWithPort()
        {
            Assert.Equal(
                "postgresql://user:p@ss@localhost:7932/mydb?" + AppNameSuffix,
                GL.MakeProxyUrl("postgresql://user:p@ss@host:5432/mydb", 7932)
            );
        }

        [Fact]
        public void AtSignInPasswordWithoutPort()
        {
            Assert.Equal(
                "postgresql://user:p@ss@localhost:7932/mydb?" + AppNameSuffix,
                GL.MakeProxyUrl("postgresql://user:p@ss@host/mydb", 7932)
            );
        }

        [Fact]
        public void AtSignInPasswordWithQueryParams()
        {
            Assert.Equal(
                "postgresql://user:p@ss@localhost:7932/mydb?sslmode=require&param=val@ue&" + AppNameSuffix,
                GL.MakeProxyUrl("postgresql://user:p@ss@host:5432/mydb?sslmode=require&param=val@ue", 7932)
            );
        }
    }


    // ── ApplicationNameMarker ────────────────────────────────
    //
    // L2-router architecture: wrappers identify themselves to the proxy via
    // PG `application_name` so the proxy can gate L2 result cache.

    [Collection("EnvVarTests")]
    public class ApplicationNameMarkerTest : IDisposable
    {
        private readonly string? _origPgAppName;

        public ApplicationNameMarkerTest()
        {
            _origPgAppName = Environment.GetEnvironmentVariable("PGAPPNAME");
            Environment.SetEnvironmentVariable("PGAPPNAME", null);
        }

        public void Dispose()
        {
            Environment.SetEnvironmentVariable("PGAPPNAME", _origPgAppName);
        }

        [Fact]
        public void MarkerHasGoldlapelDotnetShape()
        {
            var marker = GL.ApplicationNameMarker();
            Assert.Matches(@"^goldlapel:dotnet:.+$", marker);
        }

        [Fact]
        public void AppendsMarkerWhenNoExistingQuery()
        {
            var url = GL.MakeProxyUrl("postgresql://localhost:5432/mydb", 7932);
            Assert.Contains("?application_name=goldlapel:dotnet:", url);
        }

        [Fact]
        public void AppendsMarkerWithExistingQuery()
        {
            var url = GL.MakeProxyUrl("postgresql://localhost:5432/mydb?sslmode=require", 7932);
            Assert.Contains("sslmode=require", url);
            Assert.Contains("&application_name=goldlapel:dotnet:", url);
        }

        [Fact]
        public void RespectsUserSetApplicationName()
        {
            var url = GL.MakeProxyUrl("postgresql://localhost:5432/mydb?application_name=my-app", 7932);
            Assert.Contains("application_name=my-app", url);
            Assert.DoesNotContain("goldlapel:dotnet", url);
        }

        [Fact]
        public void RespectsPgAppNameEnv()
        {
            Environment.SetEnvironmentVariable("PGAPPNAME", "my-app");
            try
            {
                var url = GL.MakeProxyUrl("postgresql://localhost:5432/mydb", 7932);
                Assert.DoesNotContain("application_name=", url);
                Assert.DoesNotContain("goldlapel:dotnet", url);
            }
            finally
            {
                Environment.SetEnvironmentVariable("PGAPPNAME", null);
            }
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

        // ─── EnableL2ForWrappers startup option ────────────────────────

        [Fact]
        public void EnableL2ForWrappersDefaultsToFalse()
        {
            // Per-connection L2 wrapper-skip is the default; opt-in is required
            // for fleet customers (multi-pod, frequent restarts, mesh).
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            Assert.False(gl.IsEnableL2ForWrappers);
        }

        [Fact]
        public void EnableL2ForWrappersOptionStored()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions { EnableL2ForWrappers = true });
            // When set, SpawnAsync emits `--enable-l2-for-wrappers` so the
            // proxy keeps the L2 result cache active for wrapper traffic.
            Assert.True(gl.IsEnableL2ForWrappers);
        }

        [Fact]
        public void EnableL2ForWrappersInConfigMapIsRejected()
        {
            // Regression guard: EnableL2ForWrappers is a top-level
            // canonical-surface option, never valid inside the structured
            // config map.
            var cfg = new Dictionary<string, object> { { "enableL2ForWrappers", true } };
            Assert.Throws<ArgumentException>(() => GL.ConfigToArgs(cfg));
        }

        // ─── DisableL1 startup option ──────────────────────────────────

        [Fact]
        public void DisableL1DefaultsToFalse()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            Assert.False(gl.IsDisableL1);
        }

        [Fact]
        public void DisableL1OptionStored()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions { DisableL1 = true });
            Assert.True(gl.IsDisableL1);
        }

        [Fact]
        public void DisableL1InConfigMapIsRejected()
        {
            // Regression guard: DisableL1 is a top-level canonical-surface
            // option, never valid inside the structured config map.
            var cfg = new Dictionary<string, object> { { "disableL1", true } };
            Assert.Throws<ArgumentException>(() => GL.ConfigToArgs(cfg));
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

    // ── BuildSpawnArgs ────────────────────────────────────────
    //
    // Argv-emission tests: assert the right CLI flag actually reaches the
    // spawned proxy binary. The field-storage tests (e.g. IsMesh,
    // IsEnableL2ForWrappers) verify the property model independently — these
    // assert the wire format. Without these, a refactor that drops a flag
    // from SpawnAsync would still pass storage tests while silently shipping
    // a broken proxy invocation.
    //
    // BuildSpawnArgs is the package-internal extraction of SpawnAsync's
    // argv-construction (mirrors Java's buildSpawnCmd). The result excludes
    // the binary path — that's set separately on ProcessStartInfo.FileName.

    public class BuildSpawnArgsTest
    {
        // Helper: index of `flag` in args, -1 if absent.
        private static int IndexOf(List<string> args, string flag) => args.IndexOf(flag);

        // Helper: the value following `flag` (for two-token --flag value emissions).
        private static string ValueAfter(List<string> args, string flag)
        {
            var i = args.IndexOf(flag);
            return (i >= 0 && i + 1 < args.Count) ? args[i + 1] : null;
        }

        [Fact]
        public void RequiredFlagsAlwaysEmitted()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            var args = gl.BuildSpawnArgs();
            // --upstream and --proxy-port are unconditional and lead the argv.
            Assert.Equal("--upstream", args[0]);
            Assert.Equal("postgresql://localhost:5432/mydb", args[1]);
            Assert.Equal("--proxy-port", args[2]);
            Assert.Equal("7932", args[3]);
        }

        [Fact]
        public void UpstreamPropagatesToFlag()
        {
            var gl = GL.CreateForTest("postgresql://user:pass@host:5432/db?sslmode=require");
            var args = gl.BuildSpawnArgs();
            Assert.Equal("postgresql://user:pass@host:5432/db?sslmode=require", ValueAfter(args, "--upstream"));
        }

        [Fact]
        public void ProxyPortPropagatesToFlag()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions { ProxyPort = 9000 });
            var args = gl.BuildSpawnArgs();
            Assert.Equal("9000", ValueAfter(args, "--proxy-port"));
        }

        // ─── Defaults: nothing optional emits when unset ────────────────

        [Fact]
        public void DefaultOptionsEmitOnlyRequiredFlags()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            var args = gl.BuildSpawnArgs();
            // With no top-level options set and no Config/ExtraArgs, the only
            // emission should be the four required tokens.
            Assert.Equal(4, args.Count);
            // Defensive — none of the optional flags should appear.
            Assert.DoesNotContain("--dashboard-port", args);
            Assert.DoesNotContain("--invalidation-port", args);
            Assert.DoesNotContain("--mode", args);
            Assert.DoesNotContain("--license", args);
            Assert.DoesNotContain("--client", args);
            Assert.DoesNotContain("--config", args);
            Assert.DoesNotContain("--mesh", args);
            Assert.DoesNotContain("--mesh-tag", args);
            Assert.DoesNotContain("--enable-l2-for-wrappers", args);
            Assert.DoesNotContain("-v", args);
            Assert.DoesNotContain("-vv", args);
            Assert.DoesNotContain("-vvv", args);
            // Silent intentionally never emits a flag (see SilentDoesNotEmitFlag).
            Assert.DoesNotContain("--silent", args);
            // DisableL1 is wrapper-only — no CLI translation (see DisableL1DoesNotEmitFlag).
            Assert.DoesNotContain("--disable-l1", args);
        }

        // ─── DashboardPort ──────────────────────────────────────────────

        [Fact]
        public void DashboardPortNotEmittedByDefault()
        {
            // Default DashboardPort=null ⇒ derived to proxy+1 by the wrapper,
            // but the flag is suppressed so the Rust binary applies its own
            // identical derivation. Keeps argv minimal in the common case.
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            Assert.DoesNotContain("--dashboard-port", gl.BuildSpawnArgs());
        }

        [Fact]
        public void DashboardPortEmittedWhenExplicit()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions { DashboardPort = 9090 });
            Assert.Equal("9090", ValueAfter(gl.BuildSpawnArgs(), "--dashboard-port"));
        }

        [Fact]
        public void DashboardPortZeroEmittedExplicitly()
        {
            // DashboardPort=0 means "disable dashboard" — the binary needs
            // the explicit 0 to suppress its default derivation.
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions { DashboardPort = 0 });
            Assert.Equal("0", ValueAfter(gl.BuildSpawnArgs(), "--dashboard-port"));
        }

        // ─── InvalidationPort ───────────────────────────────────────────

        [Fact]
        public void InvalidationPortNotEmittedByDefault()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            Assert.DoesNotContain("--invalidation-port", gl.BuildSpawnArgs());
        }

        [Fact]
        public void InvalidationPortEmittedWhenExplicit()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions { InvalidationPort = 9091 });
            Assert.Equal("9091", ValueAfter(gl.BuildSpawnArgs(), "--invalidation-port"));
        }

        // ─── LogLevel ───────────────────────────────────────────────────
        //
        // The proxy binary uses count-based -v/-vv/-vvv; the wrapper translates
        // the ergonomic LogLevel string. warn/error map to "no flag" (binary
        // default). The translator is unit-tested in OptionsTest; these tests
        // confirm the translated flag actually lands in argv.

        [Fact]
        public void LogLevelTraceEmitsTripleVerbose()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions { LogLevel = "trace" });
            Assert.Contains("-vvv", gl.BuildSpawnArgs());
        }

        [Fact]
        public void LogLevelDebugEmitsDoubleVerbose()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions { LogLevel = "debug" });
            Assert.Contains("-vv", gl.BuildSpawnArgs());
        }

        [Fact]
        public void LogLevelInfoEmitsSingleVerbose()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions { LogLevel = "info" });
            Assert.Contains("-v", gl.BuildSpawnArgs());
        }

        [Theory]
        [InlineData("warn")]
        [InlineData("warning")]
        [InlineData("error")]
        public void LogLevelWarnOrErrorEmitsNoVerboseFlag(string level)
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions { LogLevel = level });
            var args = gl.BuildSpawnArgs();
            Assert.DoesNotContain("-v", args);
            Assert.DoesNotContain("-vv", args);
            Assert.DoesNotContain("-vvv", args);
        }

        [Fact]
        public void LogLevelNullEmitsNoVerboseFlag()
        {
            // Belt-and-braces: null LogLevel must not produce a stray flag
            // (the IsNullOrEmpty guard in BuildSpawnArgs covers this).
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions { LogLevel = null });
            var args = gl.BuildSpawnArgs();
            Assert.DoesNotContain("-v", args);
            Assert.DoesNotContain("-vv", args);
            Assert.DoesNotContain("-vvv", args);
        }

        // ─── Mode ───────────────────────────────────────────────────────

        [Fact]
        public void ModeNotEmittedByDefault()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            Assert.DoesNotContain("--mode", gl.BuildSpawnArgs());
        }

        [Fact]
        public void ModeEmittedWhenSet()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions { Mode = "waiter" });
            Assert.Equal("waiter", ValueAfter(gl.BuildSpawnArgs(), "--mode"));
        }

        [Fact]
        public void ModeConsiderationEmitted()
        {
            // "consideration" is the renamed bellhop mode; verify it survives
            // round-trip into argv unchanged.
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions { Mode = "consideration" });
            Assert.Equal("consideration", ValueAfter(gl.BuildSpawnArgs(), "--mode"));
        }

        // ─── License ────────────────────────────────────────────────────

        [Fact]
        public void LicenseNotEmittedByDefault()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            Assert.DoesNotContain("--license", gl.BuildSpawnArgs());
        }

        [Fact]
        public void LicenseEmittedWhenSet()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions { License = "/etc/goldlapel/license.json" });
            Assert.Equal("/etc/goldlapel/license.json",
                ValueAfter(gl.BuildSpawnArgs(), "--license"));
        }

        // ─── Client ─────────────────────────────────────────────────────

        [Fact]
        public void ClientNotEmittedByDefault()
        {
            // When Client is unset the wrapper sets GOLDLAPEL_CLIENT=dotnet
            // via env var (in SpawnAsync) — NOT via --client. So argv stays
            // clean by default.
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            Assert.DoesNotContain("--client", gl.BuildSpawnArgs());
        }

        [Fact]
        public void ClientEmittedWhenSet()
        {
            // Explicit Client overrides the dotnet env-var default and emits
            // --client so the proxy can tag telemetry with the user's value.
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions { Client = "fleet-prod" });
            Assert.Equal("fleet-prod", ValueAfter(gl.BuildSpawnArgs(), "--client"));
        }

        // ─── ConfigFile ─────────────────────────────────────────────────

        [Fact]
        public void ConfigFileNotEmittedByDefault()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            Assert.DoesNotContain("--config", gl.BuildSpawnArgs());
        }

        [Fact]
        public void ConfigFileEmittedWhenSet()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions { ConfigFile = "/etc/goldlapel/config.toml" });
            Assert.Equal("/etc/goldlapel/config.toml",
                ValueAfter(gl.BuildSpawnArgs(), "--config"));
        }

        // ─── Mesh ───────────────────────────────────────────────────────

        [Fact]
        public void MeshNotEmittedByDefault()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            Assert.DoesNotContain("--mesh", gl.BuildSpawnArgs());
        }

        [Fact]
        public void MeshEmittedWhenTrue()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions { Mesh = true });
            Assert.Contains("--mesh", gl.BuildSpawnArgs());
        }

        [Fact]
        public void MeshTagNotEmittedByDefault()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            Assert.DoesNotContain("--mesh-tag", gl.BuildSpawnArgs());
        }

        [Fact]
        public void MeshTagEmittedWhenSet()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions { Mesh = true, MeshTag = "prod-east" });
            var args = gl.BuildSpawnArgs();
            Assert.Equal("prod-east", ValueAfter(args, "--mesh-tag"));
            // --mesh and --mesh-tag are independent flags; both should appear.
            Assert.Contains("--mesh", args);
        }

        [Fact]
        public void MeshTagEmittedWithoutMeshFlag()
        {
            // MeshTag is independent of Mesh — setting only the tag still
            // emits --mesh-tag (the proxy may interpret this as "join the
            // tagged mesh"). Matches the existing string.IsNullOrEmpty guard
            // logic in BuildSpawnArgs (no coupling to _mesh).
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions { MeshTag = "prod-east" });
            var args = gl.BuildSpawnArgs();
            Assert.Equal("prod-east", ValueAfter(args, "--mesh-tag"));
            Assert.DoesNotContain("--mesh", args);
        }

        [Fact]
        public void MeshTagEmptyStringNotEmitted()
        {
            // Empty tag is normalized to null in the constructor; ensure no
            // stray --mesh-tag emission.
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions { Mesh = true, MeshTag = "" });
            Assert.DoesNotContain("--mesh-tag", gl.BuildSpawnArgs());
        }

        // ─── EnableL2ForWrappers ────────────────────────────────────────

        [Fact]
        public void EnableL2ForWrappersNotEmittedByDefault()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            Assert.DoesNotContain("--enable-l2-for-wrappers", gl.BuildSpawnArgs());
        }

        [Fact]
        public void EnableL2ForWrappersEmittedWhenTrue()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions { EnableL2ForWrappers = true });
            Assert.Contains("--enable-l2-for-wrappers", gl.BuildSpawnArgs());
        }

        // ─── DisableL1 ──────────────────────────────────────────────────
        //
        // DisableL1 is a wrapper-only knob — there is no --disable-l1 flag
        // on the proxy binary. The flag toggles L1 (the wrapper's
        // in-process NativeCache) only; the proxy stays oblivious. Argv
        // must not gain any new tokens regardless of the option's value.

        [Fact]
        public void DisableL1DoesNotEmitFlag()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions { DisableL1 = true });
            var args = gl.BuildSpawnArgs();
            Assert.DoesNotContain("--disable-l1", args);
            // Belt-and-suspenders: with no other options set, argv stays at
            // the four required tokens (--upstream <url> --proxy-port <n>).
            Assert.Equal(4, args.Count);
        }

        [Fact]
        public void DisableL1FalseDoesNotEmitFlag()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions { DisableL1 = false });
            Assert.DoesNotContain("--disable-l1", gl.BuildSpawnArgs());
        }

        // ─── Silent ─────────────────────────────────────────────────────
        //
        // Audit finding: GoldLapelOptions.Silent is a wrapper-banner-only
        // option. It does NOT emit --silent to the binary. This matches the
        // Python and Java wrappers (verified in goldlapel-python proxy.py
        // and goldlapel-java GoldLapel.java). The binary's --silent flag
        // suppresses the first-run welcome prompt — a separate concern
        // owned by the binary, set via env var (GOLDLAPEL_SILENT) or
        // ExtraArgs by callers who actually need it.

        [Fact]
        public void SilentDoesNotEmitFlag()
        {
            // Regression guard: even when Silent=true, --silent must not
            // appear in argv. The wrapper uses _silent only to suppress its
            // own startup banner (see SpawnAsync's banner block).
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions { Silent = true });
            Assert.DoesNotContain("--silent", gl.BuildSpawnArgs());
        }

        // ─── ExtraArgs / Config integration ─────────────────────────────

        [Fact]
        public void ExtraArgsAppendedAtEnd()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions { ExtraArgs = new[] { "--silent", "--foo" } });
            var args = gl.BuildSpawnArgs();
            // ExtraArgs are the trailing tokens (config map is empty here).
            Assert.Equal("--silent", args[args.Count - 2]);
            Assert.Equal("--foo", args[args.Count - 1]);
        }

        [Fact]
        public void ConfigMapFlagsAppearBeforeExtraArgs()
        {
            // The argv-construction contract: required flags → top-level
            // options → structured config map → extra args. Order matters
            // because earlier flags can be overridden by later ones (last
            // write wins in clap), and ExtraArgs is the user's escape hatch.
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions
                {
                    Config = new Dictionary<string, object> { { "poolSize", 50 } },
                    ExtraArgs = new[] { "--custom" }
                });
            var args = gl.BuildSpawnArgs();
            var poolIdx = IndexOf(args, "--pool-size");
            var customIdx = IndexOf(args, "--custom");
            Assert.True(poolIdx >= 0 && customIdx >= 0);
            Assert.True(poolIdx < customIdx,
                $"--pool-size (index {poolIdx}) should precede --custom (index {customIdx})");
        }

        [Fact]
        public void TopLevelFlagsPrecedeConfigMap()
        {
            // Argv ordering contract: top-level (--mode) emits before the
            // structured config map (--pool-size). Tests exercising flag
            // overrides via ExtraArgs rely on this stable ordering.
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions
                {
                    Mode = "waiter",
                    Config = new Dictionary<string, object> { { "poolSize", 10 } }
                });
            var args = gl.BuildSpawnArgs();
            var modeIdx = IndexOf(args, "--mode");
            var poolIdx = IndexOf(args, "--pool-size");
            Assert.True(modeIdx >= 0 && poolIdx >= 0);
            Assert.True(modeIdx < poolIdx,
                $"--mode (index {modeIdx}) should precede --pool-size (index {poolIdx})");
        }

        // ─── Mixed: every top-level flag wired together ─────────────────

        [Fact]
        public void AllTopLevelFlagsCoexist()
        {
            // Smoke test that no two top-level flag emissions interfere with
            // each other — every option set, every flag observable in argv.
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions
                {
                    ProxyPort = 17932,
                    DashboardPort = 9090,
                    InvalidationPort = 9091,
                    LogLevel = "debug",
                    Mode = "waiter",
                    License = "/tmp/lic",
                    Client = "fleet",
                    ConfigFile = "/tmp/cfg.toml",
                    Mesh = true,
                    MeshTag = "east",
                    EnableL2ForWrappers = true,
                    DisableL1 = true,
                });
            var args = gl.BuildSpawnArgs();
            Assert.Equal("17932", ValueAfter(args, "--proxy-port"));
            Assert.Equal("9090", ValueAfter(args, "--dashboard-port"));
            Assert.Equal("9091", ValueAfter(args, "--invalidation-port"));
            Assert.Contains("-vv", args);
            Assert.Equal("waiter", ValueAfter(args, "--mode"));
            Assert.Equal("/tmp/lic", ValueAfter(args, "--license"));
            Assert.Equal("fleet", ValueAfter(args, "--client"));
            Assert.Equal("/tmp/cfg.toml", ValueAfter(args, "--config"));
            Assert.Contains("--mesh", args);
            Assert.Equal("east", ValueAfter(args, "--mesh-tag"));
            Assert.Contains("--enable-l2-for-wrappers", args);
            // DisableL1 is wrapper-only — must not leak into argv even when
            // every other option is set.
            Assert.DoesNotContain("--disable-l1", args);
        }
    }
}
