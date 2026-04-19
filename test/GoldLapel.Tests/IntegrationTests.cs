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

        // End-to-end UsingAsync test against the live proxy.
        //
        // Previously blocked by a Npgsql/proxy CloseComplete framing bug
        // (wrapper-v0.2 TODO 03, fixed on the `closecomplete-framing-fix`
        // branch in the Rust repo). To run this test against a binary that
        // doesn't have the fix, there is a per-collection isolation guard
        // (unique collection name + tx rollback) so one flaky run doesn't
        // leave stale state behind.
        //
        // Verifies the scoping contract: inside UsingAsync(userConn, ...),
        // wrapper methods use `userConn` (not the internal connection). We
        // begin a tx on userConn, call DocInsertAsync via UsingAsync,
        // then roll back. If UsingAsync honored the scope, the insert is
        // inside the rolled-back tx and leaves no row. If it used the
        // internal conn (the bug), the row would persist independently.
        [Fact]
        public async Task UsingAsyncScopesConnectionAgainstLiveProxy()
        {
            if (!CanRunIntegration()) return;

            var port = NextPort();
            await using var gl = await GL.StartAsync(Upstream, opts => { opts.Port = port; });

            var collection = "gl_int_using_" + Guid.NewGuid().ToString("N").Substring(0, 8);

            // Pre-create the collection on the internal connection so the
            // DDL inside UsingAsync is a no-op. Keeps the tx small — only
            // the INSERT gets rolled back, and we avoid a DDL-in-transaction
            // path that has historically interacted poorly with the proxy's
            // prepared-statement cache.
            await gl.DocCreateCollectionAsync(collection);

            await using var userConn = new NpgsqlConnection(gl.Url);
            await userConn.OpenAsync();

            await using (var tx = await userConn.BeginTransactionAsync())
            {
                await gl.UsingAsync(userConn, async scoped =>
                {
                    await scoped.DocInsertAsync(collection, "{\"via\":\"using-tx\"}");
                });
                // Rollback — if UsingAsync used userConn, the insert is undone.
                await tx.RollbackAsync();
            }

            // Verify: collection exists (pre-created) but has zero rows. If
            // UsingAsync had used the internal connection, the insert would
            // have persisted outside the rolled-back tx and count would be 1.
            var count = await gl.DocCountAsync(collection);
            Assert.Equal(0L, count);

            // Cleanup.
            await using var clean = new NpgsqlConnection(gl.Url);
            await clean.OpenAsync();
            await using var drop = new NpgsqlCommand($"DROP TABLE {collection}", clean);
            await drop.ExecuteNonQueryAsync();
        }

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
