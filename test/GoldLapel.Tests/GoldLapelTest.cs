using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using Xunit;
using GL = Goldlapel.GoldLapel;
using Goldlapel;

namespace Goldlapel.Tests
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

    // ── Options / construction ────────────────────────────────

    public class OptionsTest
    {
        [Fact]
        public void DefaultPort()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            Assert.Equal(7932, gl.Port);
        }

        [Fact]
        public void CustomPort()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions { Port = 9000 });
            Assert.Equal(9000, gl.Port);
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
            var config = new Dictionary<string, object> { { "logLevel", "debug" } };
            var args = GL.ConfigToArgs(config);
            Assert.Equal(new List<string> { "-vv" }, args);
        }

        [Fact]
        public void LogLevelTraceMapsToTripleVerbose()
        {
            var args = GL.ConfigToArgs(new Dictionary<string, object> { { "logLevel", "trace" } });
            Assert.Equal(new List<string> { "-vvv" }, args);
        }

        [Fact]
        public void LogLevelInfoMapsToSingleVerbose()
        {
            var args = GL.ConfigToArgs(new Dictionary<string, object> { { "logLevel", "info" } });
            Assert.Equal(new List<string> { "-v" }, args);
        }

        [Theory]
        [InlineData("warn")]
        [InlineData("warning")]
        [InlineData("error")]
        public void LogLevelWarnOrErrorEmitsNoFlag(string level)
        {
            // warn/error are the default level — no extra flag needed.
            var args = GL.ConfigToArgs(new Dictionary<string, object> { { "logLevel", level } });
            Assert.Empty(args);
        }

        [Fact]
        public void LogLevelIsCaseInsensitive()
        {
            var args = GL.ConfigToArgs(new Dictionary<string, object> { { "logLevel", "DEBUG" } });
            Assert.Equal(new List<string> { "-vv" }, args);
        }

        [Fact]
        public void LogLevelInvalidRaises()
        {
            var ex = Assert.Throws<ArgumentException>(() =>
                GL.ConfigToArgs(new Dictionary<string, object> { { "logLevel", "loud" } }));
            Assert.Contains("logLevel must be one of", ex.Message);
        }

        [Fact]
        public void LogLevelNeverEmitsLongFlag()
        {
            // Regression guard: the proxy binary does not accept --log-level.
            foreach (var lvl in new[] { "trace", "debug", "info", "warn", "error" })
            {
                var args = GL.ConfigToArgs(new Dictionary<string, object> { { "logLevel", lvl } });
                Assert.DoesNotContain("--log-level", args);
            }
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
                new GoldLapelOptions
                {
                    Config = new Dictionary<string, object> { { "dashboardPort", 9090 } }
                });
            Assert.Null(gl.DashboardUrl);
        }

        [Fact]
        public void DashboardDisabledWithZero()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions
                {
                    Config = new Dictionary<string, object> { { "dashboardPort", 0 } }
                });
            Assert.Null(gl.DashboardUrl);
        }

        [Fact]
        public void DashboardUrlNullWhenNotRunning()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            Assert.Null(gl.DashboardUrl);
        }

        [Fact]
        public void DashboardPortExtractedFromConfig()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions
                {
                    Config = new Dictionary<string, object> { { "dashboardPort", 8888 } }
                });
            Assert.Null(gl.DashboardUrl);
            Assert.False(gl.IsRunning);
        }

        [Fact]
        public void DashboardPortDerivesFromCustomProxyPort()
        {
            // When only Port is set, dashboard defaults to port + 1 (not the
            // hardcoded 7933). The internal _dashboardPort isn't public, but
            // DashboardUrl exposes it when the process is running; since we
            // can't spawn a real process here, we reach in via reflection.
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions { Port = 17932 });
            var field = typeof(GL).GetField("_dashboardPort",
                System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
            Assert.NotNull(field);
            Assert.Equal(17933, (int)field.GetValue(gl));
        }

        [Fact]
        public void ExplicitDashboardPortOverridesDerivation()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb",
                new GoldLapelOptions
                {
                    Port = 17932,
                    Config = new Dictionary<string, object> { { "dashboardPort", 9999 } }
                });
            var field = typeof(GL).GetField("_dashboardPort",
                System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
            Assert.NotNull(field);
            Assert.Equal(9999, (int)field.GetValue(gl));
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
            var keys = GL.ConfigKeys();
            Assert.Contains("mode", keys);
            Assert.Contains("poolSize", keys);
            Assert.Contains("disableMatviews", keys);
            Assert.Contains("replica", keys);
        }

        [Fact]
        public void ContainsLogLevel()
        {
            // v0.2.0: logLevel is a first-class config key (exposed via LogLevel option).
            var keys = GL.ConfigKeys();
            Assert.Contains("logLevel", keys);
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
        [Fact]
        public void SendSignalToSelf()
        {
            if (System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(
                    System.Runtime.InteropServices.OSPlatform.Windows))
                return;

            var pid = System.Diagnostics.Process.GetCurrentProcess().Id;
            Assert.True(GL.SendSignal(pid, 0)); // signal 0 = existence check
        }

        [Fact]
        public void SendSignalToNonexistentPid()
        {
            if (System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(
                    System.Runtime.InteropServices.OSPlatform.Windows))
                return;

            Assert.False(GL.SendSignal(4194304, 0));
        }

        [Fact]
        public void DisposeIsIdempotent()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            gl.Dispose();
            gl.Dispose(); // second call should not throw
        }
    }

    // ── ConfigToArgs ─────────────────────────────────────────

    public class ConfigToArgsTest
    {
        [Fact]
        public void ConfigToArgs_StringValue()
        {
            var config = new Dictionary<string, object> { { "mode", "waiter" } };
            var args = GL.ConfigToArgs(config);
            Assert.Equal(new List<string> { "--mode", "waiter" }, args);
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
                { "mode", "waiter" },
                { "poolSize", 10 },
                { "disableRewrite", true }
            };
            var args = GL.ConfigToArgs(config);
            Assert.Contains("--mode", args);
            Assert.Contains("waiter", args);
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
                Config = new Dictionary<string, object>
                {
                    { "mode", "waiter" },
                    { "disablePool", true }
                }
            };
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb", options);
            Assert.Equal(7932, gl.Port);
        }
    }
}
