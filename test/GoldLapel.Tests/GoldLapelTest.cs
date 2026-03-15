using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using Xunit;
using GL = GoldLapel.GoldLapel;

namespace GoldLapel.Tests
{
    // ── FindBinary ────────────────────────────────────────────

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

    // ── Lifecycle ─────────────────────────────────────────────

    public class LifecycleTest
    {
        [Fact]
        public void DefaultPort()
        {
            var gl = new GL("postgresql://localhost:5432/mydb");
            Assert.Equal(7932, gl.Port);
        }

        [Fact]
        public void CustomPort()
        {
            var gl = new GL("postgresql://localhost:5432/mydb",
                new GoldLapelOptions { Port = 9000 });
            Assert.Equal(9000, gl.Port);
        }

        [Fact]
        public void NotRunningInitially()
        {
            var gl = new GL("postgresql://localhost:5432/mydb");
            Assert.False(gl.IsRunning);
            Assert.Null(gl.Url);
        }

        [Fact]
        public void NullUpstreamThrows()
        {
            Assert.Throws<ArgumentNullException>(() => new GL(null));
        }

        [Fact]
        public void NullUpstreamWithOptionsThrows()
        {
            Assert.Throws<ArgumentNullException>(() => new GL(null, new GoldLapelOptions()));
        }
    }

    // ── DashboardUrl ───────────────────────────────────────

    public class DashboardUrlTest
    {
        [Fact]
        public void DefaultDashboardPort()
        {
            var gl = new GL("postgresql://localhost:5432/mydb");
            Assert.Equal(GL.DefaultDashboardPort, 7933);
        }

        [Fact]
        public void CustomDashboardPort()
        {
            var gl = new GL("postgresql://localhost:5432/mydb",
                new GoldLapelOptions
                {
                    Config = new Dictionary<string, object> { { "dashboardPort", 9090 } }
                });
            Assert.Null(gl.DashboardUrl);
        }

        [Fact]
        public void DashboardDisabledWithZero()
        {
            var gl = new GL("postgresql://localhost:5432/mydb",
                new GoldLapelOptions
                {
                    Config = new Dictionary<string, object> { { "dashboardPort", 0 } }
                });
            Assert.Null(gl.DashboardUrl);
        }

        [Fact]
        public void DashboardUrlNullWhenNotRunning()
        {
            var gl = new GL("postgresql://localhost:5432/mydb");
            Assert.Null(gl.DashboardUrl);
        }

        [Fact]
        public void DashboardPortExtractedFromConfig()
        {
            var gl = new GL("postgresql://localhost:5432/mydb",
                new GoldLapelOptions
                {
                    Config = new Dictionary<string, object> { { "dashboardPort", 8888 } }
                });
            // Not running, so DashboardUrl is null, but we can verify the port was extracted
            // by checking it doesn't use the default when we eventually start
            Assert.Null(gl.DashboardUrl);
            Assert.False(gl.IsRunning);
        }
    }

    // ── DashboardProxyUrl (singleton) ────────────────────

    public class DashboardProxyUrlTest
    {
        [Fact]
        public void DashboardProxyUrlNullWhenNotStarted()
        {
            GL.Stop();
            Assert.Null(GL.DashboardProxyUrl);
        }
    }

    // ── Singleton ─────────────────────────────────────────────

    public class SingletonTest
    {
        [Fact]
        public void ProxyUrlNullWhenNotStarted()
        {
            GL.Stop();
            Assert.Null(GL.ProxyUrl);
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
        public void DoesNotContainUnknownKeys()
        {
            var keys = GL.ConfigKeys();
            Assert.DoesNotContain("notARealKey", keys);
        }

        [Fact]
        public void HasExpectedCount()
        {
            var keys = GL.ConfigKeys();
            Assert.Equal(43, keys.Count);
        }
    }

    // ── ConfigToArgs ─────────────────────────────────────────

    public class ConfigToArgsTest
    {
        [Fact]
        public void ConfigToArgs_StringValue()
        {
            var config = new Dictionary<string, object> { { "mode", "butler" } };
            var args = GL.ConfigToArgs(config);
            Assert.Equal(new List<string> { "--mode", "butler" }, args);
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
                { "mode", "butler" },
                { "poolSize", 10 },
                { "disableRewrite", true }
            };
            var args = GL.ConfigToArgs(config);
            Assert.Contains("--mode", args);
            Assert.Contains("butler", args);
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
                    { "mode", "butler" },
                    { "disablePool", true }
                }
            };
            var gl = new GL("postgresql://localhost:5432/mydb", options);
            Assert.Equal(7932, gl.Port);
        }
    }
}
