using System;
using System.IO;
using System.Threading.Tasks;
using Npgsql;
using Xunit;
using GL = Goldlapel.GoldLapel;
using Goldlapel;

namespace Goldlapel.Tests
{
    // Integration tests — spawn the real Rust binary against a live Postgres at
    // postgresql://sgibson@localhost/postgres. Each test picks a unique port so
    // they can run in parallel (xUnit runs Facts in different classes concurrently
    // by default). Set GOLDLAPEL_BINARY=/path/to/goldlapel to override discovery.
    //
    // Tests skip gracefully if the binary is not available, but we want a real
    // end-to-end check of StartAsync / await using / UsingAsync / connection:
    // under the new v0.2.0 API.

    [Collection("EnvVarTests")]
    public class IntegrationTests
    {
        private const string Upstream = "postgresql://sgibson@localhost/postgres";

        private static bool CanRunIntegration()
        {
            try { GL.FindBinary(); return true; }
            catch { return false; }
        }

        private static int NextPort()
        {
            // Randomized high-numbered port in a test-friendly range to avoid
            // collisions with the default 7932 or other parallel tests.
            //
            // Use Random.Shared (process-wide, thread-safe, seeded once) rather
            // than `new Random()`, which seeds from Environment.TickCount and
            // collides when two test instances construct in the same ms-tick —
            // producing identical "random" port sequences and flaky conflicts
            // on fast parallel runs.
            return 17900 + Random.Shared.Next(0, 1000);
        }

        [Fact]
        public async Task StartAsyncReturnsRunningInstance()
        {
            if (!CanRunIntegration()) return;

            var port = NextPort();
            await using var gl = await GL.StartAsync(Upstream, opts => { opts.Port = port; });

            Assert.True(gl.IsRunning);
            Assert.NotNull(gl.Url);
            // gl.Url is in Npgsql keyword form, ProxyUrl is the URL form.
            Assert.Contains("Host=localhost", gl.Url);
            Assert.Contains("Port=" + port, gl.Url);
            Assert.Contains("localhost:" + port, gl.ProxyUrl);
            Assert.NotNull(gl.Connection);
        }

        [Fact]
        public async Task AwaitUsingDisposesProxy()
        {
            if (!CanRunIntegration()) return;

            var port = NextPort();
            GL gl;
            await using (gl = await GL.StartAsync(Upstream, opts => { opts.Port = port; }))
            {
                Assert.True(gl.IsRunning);
            }
            Assert.False(gl.IsRunning);
            Assert.Null(gl.Url);
            Assert.Null(gl.ProxyUrl);
        }

        [Fact]
        public async Task RawSqlAgainstLiveProxy()
        {
            if (!CanRunIntegration()) return;

            var port = NextPort();
            await using var gl = await GL.StartAsync(Upstream, opts => { opts.Port = port; });

            // Raw SQL round-trip via gl.Url -> new NpgsqlConnection(gl.Url) pattern.
            await using var conn = new NpgsqlConnection(gl.Url);
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand("SELECT 1 + 1", conn);
            var result = await cmd.ExecuteScalarAsync();
            Assert.Equal(2, Convert.ToInt32(result));
        }

        // Note: an end-to-end UsingAsync integration test against the live proxy
        // is intentionally covered in unit tests (InstanceMethodTests.cs/UsingAsyncScopeTest)
        // rather than here — a known Npgsql/proxy prepared-statement interaction
        // sporadically fails when DDL is issued across a freshly-opened user
        // connection in the same proxy session. The unit tests verify the
        // AsyncLocal scoping mechanics without needing a real Postgres.

        [Fact]
        public async Task PerCallConnectionOverride()
        {
            if (!CanRunIntegration()) return;

            var port = NextPort();
            await using var gl = await GL.StartAsync(Upstream, opts => { opts.Port = port; });

            var collection = "gl_int_" + Guid.NewGuid().ToString("N").Substring(0, 8);
            await using var userConn = new NpgsqlConnection(gl.Url);
            await userConn.OpenAsync();

            // connection: per-call overrides the internal conn for this one call.
            await gl.DocInsertAsync(collection, "{\"via\":\"per-call\"}", connection: userConn);
            var count = await gl.DocCountAsync(collection, connection: userConn);
            Assert.Equal(1L, count);

            // Cleanup.
            await using var clean = new NpgsqlConnection(gl.Url);
            await clean.OpenAsync();
            await using var drop = new NpgsqlCommand($"DROP TABLE {collection}", clean);
            await drop.ExecuteNonQueryAsync();
        }

        // Banner routing: the startup banner must go to stderr (not stdout) so
        // it doesn't pollute app output (ASP.NET Core logs, piped CLI output,
        // shells redirecting stdout). Regression test for wrapper-v0.2 review
        // finding "Banner to stdout pollutes library output".
        [Fact]
        public async Task Banner_WritesToStderr_NotStdout()
        {
            if (!CanRunIntegration()) return;

            var origOut = Console.Out;
            var origErr = Console.Error;
            var capturedOut = new StringWriter();
            var capturedErr = new StringWriter();
            Console.SetOut(capturedOut);
            Console.SetError(capturedErr);
            try
            {
                var port = NextPort();
                await using (await GL.StartAsync(Upstream, opts => { opts.Port = port; }))
                {
                }
            }
            finally
            {
                Console.SetOut(origOut);
                Console.SetError(origErr);
            }

            var stdout = capturedOut.ToString();
            var stderr = capturedErr.ToString();

            Assert.Contains("goldlapel", stderr);
            Assert.Contains("(proxy)", stderr);
            Assert.DoesNotContain("goldlapel", stdout);
        }

        // Silent=true must suppress the banner entirely — nothing on stdout OR
        // stderr. Useful for embedded/daemon scenarios where even stderr is
        // inspected (structured-log tooling, test runners, etc.).
        [Fact]
        public async Task Silent_Suppresses_Banner()
        {
            if (!CanRunIntegration()) return;

            var origOut = Console.Out;
            var origErr = Console.Error;
            var capturedOut = new StringWriter();
            var capturedErr = new StringWriter();
            Console.SetOut(capturedOut);
            Console.SetError(capturedErr);
            try
            {
                var port = NextPort();
                await using (await GL.StartAsync(Upstream, opts =>
                {
                    opts.Port = port;
                    opts.Silent = true;
                }))
                {
                }
            }
            finally
            {
                Console.SetOut(origOut);
                Console.SetError(origErr);
            }

            Assert.DoesNotContain("goldlapel", capturedOut.ToString());
            Assert.DoesNotContain("goldlapel", capturedErr.ToString());
        }
    }
}
