using System;
using System.Data.Common;
using System.Threading.Tasks;
using Npgsql;
using Xunit;
using GL = GoldLapel.GoldLapel;

namespace GoldLapel.Tests
{
    /// <summary>
    /// End-to-end streams integration — proxy-owned DDL (Phase 3).
    ///
    /// Runs only when a live proxy binary + Postgres are reachable (same gate
    /// as <see cref="IntegrationTests"/>).
    /// </summary>
    [Collection("EnvVarTests")]
    public class StreamsIntegrationTests
    {
        // Gated on the standardized Gold Lapel integration-test convention
        // (GOLDLAPEL_INTEGRATION=1 + GOLDLAPEL_TEST_UPSTREAM) — see
        // IntegrationGate.cs. Upstream is empty iff the gate is closed, which
        // makes CanRunIntegration() return false (every [Fact] exits early).
        private static readonly string Upstream = IntegrationGate.Upstream() ?? string.Empty;

        private static bool CanRunIntegration()
        {
            if (string.IsNullOrEmpty(Upstream)) return false;
            // GOLDLAPEL_BINARY is the canonical way to pin the binary; fall
            // back to FindBinary() if it isn't set (e.g. dev machine with the
            // binary on PATH).
            try { GL.FindBinary(); return true; }
            catch { return false; }
        }

        private static int NextPort() => 18000 + Random.Shared.Next(0, 1000);

        private static async Task<NpgsqlConnection> DirectAsync()
        {
            // Npgsql doesn't accept postgresql:// URLs — translate to keyword form.
            var connStr = Upstream;
            if (connStr.StartsWith("postgresql://") || connStr.StartsWith("postgres://"))
            {
                var rest = connStr.Substring(connStr.IndexOf("://") + 3);
                string user = null;
                int at = rest.IndexOf('@');
                if (at >= 0)
                {
                    user = rest.Substring(0, at);
                    rest = rest.Substring(at + 1);
                }
                int slash = rest.IndexOf('/');
                string hostPort = slash >= 0 ? rest.Substring(0, slash) : rest;
                string db = slash >= 0 ? rest.Substring(slash + 1) : "postgres";
                int colon = hostPort.IndexOf(':');
                string host = colon >= 0 ? hostPort.Substring(0, colon) : hostPort;
                string port = colon >= 0 ? hostPort.Substring(colon + 1) : "5432";
                connStr = $"Host={host};Port={port};Database={db}";
                if (user != null) connStr += $";Username={user}";
            }
            var c = new NpgsqlConnection(connStr);
            await c.OpenAsync();
            return c;
        }

        [Fact]
        public async Task StreamAdd_CreatesPrefixedTable()
        {
            if (!CanRunIntegration()) return;
            var name = "gl_dotnet_int_" + DateTime.UtcNow.Ticks;
            await using var gl = await GL.StartAsync(Upstream, opts =>
            {
                opts.ProxyPort = NextPort();
                opts.Silent = true;
            });
            await gl.StreamAddAsync(name, "{\"type\":\"click\"}");

            await using var direct = await DirectAsync();
            using var cmd = direct.CreateCommand();
            cmd.CommandText =
                "SELECT COUNT(*) FROM information_schema.tables " +
                "WHERE table_schema = '_goldlapel' AND table_name = @n";
            cmd.Parameters.AddWithValue("@n", "stream_" + name);
            var count = Convert.ToInt64(await cmd.ExecuteScalarAsync());
            Assert.Equal(1, count);

            using var cmd2 = direct.CreateCommand();
            cmd2.CommandText =
                "SELECT COUNT(*) FROM information_schema.tables " +
                "WHERE table_schema = 'public' AND table_name = @n";
            cmd2.Parameters.AddWithValue("@n", name);
            var publicCount = Convert.ToInt64(await cmd2.ExecuteScalarAsync());
            Assert.Equal(0, publicCount);
        }

        [Fact]
        public async Task SchemaMetaRowRecorded()
        {
            if (!CanRunIntegration()) return;
            var name = "gl_dotnet_int_meta_" + DateTime.UtcNow.Ticks;
            await using var gl = await GL.StartAsync(Upstream, opts =>
            {
                opts.ProxyPort = NextPort();
                opts.Silent = true;
            });
            await gl.StreamAddAsync(name, "{\"type\":\"click\"}");

            await using var direct = await DirectAsync();
            using var cmd = direct.CreateCommand();
            cmd.CommandText =
                "SELECT family, name, schema_version FROM _goldlapel.schema_meta " +
                "WHERE family = 'stream' AND name = @n";
            cmd.Parameters.AddWithValue("@n", name);
            using var reader = await cmd.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            Assert.Equal("stream", reader.GetString(0));
            Assert.Equal(name, reader.GetString(1));
            Assert.Equal("v1", reader.GetString(2));
        }

        [Fact]
        public async Task AddReadAckRoundTrip()
        {
            if (!CanRunIntegration()) return;
            var name = "gl_dotnet_int_rt_" + DateTime.UtcNow.Ticks;
            await using var gl = await GL.StartAsync(Upstream, opts =>
            {
                opts.ProxyPort = NextPort();
                opts.Silent = true;
            });
            await gl.StreamCreateGroupAsync(name, "workers");
            var id1 = await gl.StreamAddAsync(name, "{\"i\":1}");
            var id2 = await gl.StreamAddAsync(name, "{\"i\":2}");
            Assert.True(id2 > id1);

            var msgs = await gl.StreamReadAsync(name, "workers", "c", 10);
            Assert.Equal(2, msgs.Count);

            var first = await gl.StreamAckAsync(name, "workers", id1);
            Assert.True(first);
            var second = await gl.StreamAckAsync(name, "workers", id1);
            Assert.False(second);
        }
    }
}
