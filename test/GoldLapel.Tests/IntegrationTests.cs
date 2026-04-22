using System;
using System.IO;
using System.Threading.Tasks;
using Npgsql;
using Xunit;
using GL = GoldLapel.GoldLapel;
using GoldLapel;

namespace GoldLapel.Tests
{
    // Integration tests — spawn the real Rust binary against a live Postgres.
    //
    // Gated on the standardized Gold Lapel integration-test convention
    // shared across all 7 wrapper repos: GOLDLAPEL_INTEGRATION=1 +
    // GOLDLAPEL_TEST_UPSTREAM must both be set. If GOLDLAPEL_INTEGRATION=1
    // is set but the upstream is missing, IntegrationGate.Upstream() throws
    // loudly — preventing a half-configured CI from silently skipping.
    // See IntegrationGate.cs.
    //
    // Also set GOLDLAPEL_BINARY=/path/to/goldlapel to override discovery.
    // Each test picks a unique port so they can run in parallel (xUnit runs
    // Facts in different classes concurrently by default).

    [Collection("EnvVarTests")]
    public class IntegrationTests
    {
        // Upstream is non-empty iff the integration gate allows these tests
        // to run. When the gate is closed (opt-in flag not set), Upstream is
        // an empty string and CanRunIntegration() returns false, so every
        // [Fact] exits early without touching the URL.
        private static readonly string Upstream = IntegrationGate.Upstream() ?? string.Empty;

        private static bool CanRunIntegration()
        {
            if (string.IsNullOrEmpty(Upstream)) return false;
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

        // Regression test for the CloseComplete framing bug fixed by
        // goldlapel commits c74748d (eviction-CloseComplete counter) and
        // 77d7dfd (invalidate ext-cache client mapping on every Parse).
        //
        // Pre-fix symptom on Npgsql: when the Rust proxy's ext prepared-
        // statement cache evicts entries (LRU on write, or full `.clear()`
        // on DDL-detected-in-simple-query), it forwards one or more stray
        // `CloseComplete` frames to the client. Npgsql's extended-protocol
        // parser aborts with:
        //
        //     CloseComplete while expecting ParseCompleteMessage
        //
        // The bug only fires on drivers that take the extended/Parse-Bind
        // path — which is exactly what `NpgsqlCommand.Prepare()` triggers.
        // The pre-existing UsingAsyncScopesConnectionAgainstLiveProxy test
        // deliberately avoids Prepare() so it can run against any binary;
        // THIS test exists specifically to exercise Parse/Bind so future
        // regressions in the proxy's framing logic are caught.
        //
        // Gated on GOLDLAPEL_CLOSECOMPLETE_FIX=1 (opt-in) because stock
        // pre-fix binaries will deterministically fail. Default-skip is
        // safe — dev/CI sets the env var once the fix ships. Using
        // SkippableFact (not Fact) so `dotnet test` reports these as
        // "Skipped" rather than silently passing.
        private static bool CloseCompleteFixConfirmed()
        {
            var v = Environment.GetEnvironmentVariable("GOLDLAPEL_CLOSECOMPLETE_FIX");
            return !string.IsNullOrEmpty(v) && v != "0" && v != "false";
        }

        [SkippableFact]
        public async Task PreparedStatement_RoundTrips_AgainstLiveProxy()
        {
            if (!CanRunIntegration()) return;
            Skip.IfNot(
                CloseCompleteFixConfirmed(),
                "Requires a CloseComplete-fixed proxy binary. Set GOLDLAPEL_CLOSECOMPLETE_FIX=1 to run.");

            var port = NextPort();
            await using var gl = await GL.StartAsync(Upstream, opts => { opts.Port = port; });

            await using var conn = new NpgsqlConnection(gl.Url);
            await conn.OpenAsync();

            // Parameterized query + Prepare() forces Npgsql into its
            // extended-protocol Parse/Bind/Describe/Execute flow — the
            // exact path that pre-fix tripped the CloseComplete framing
            // bug. Assert the full round-trip returns the expected value.
            await using var cmd = new NpgsqlCommand("SELECT $1::int AS x", conn);
            cmd.Parameters.AddWithValue(42);
            await cmd.PrepareAsync();

            var first = await cmd.ExecuteScalarAsync();
            Assert.Equal(42, Convert.ToInt32(first));

            // Re-execute the prepared statement to cover statement reuse
            // (different param value, same prepared plan).
            cmd.Parameters[0].Value = 1337;
            var second = await cmd.ExecuteScalarAsync();
            Assert.Equal(1337, Convert.ToInt32(second));
        }

        // Second regression shape from the original .NET bug report: DDL
        // issued over a second user connection in the same proxy session,
        // while a first connection has primed the proxy's ext prepared-
        // statement cache. The DDL path (simple-query `CREATE TABLE`)
        // triggers `ext_prep_cache.clear()` on the proxy; pre-fix, the
        // resulting burst of CloseComplete frames leaked back to whichever
        // connection next used extended protocol — reliably trashing it
        // with "CloseComplete while expecting ParseCompleteMessage".
        [SkippableFact]
        public async Task PreparedStatement_Survives_DdlOnSecondConnection()
        {
            if (!CanRunIntegration()) return;
            Skip.IfNot(
                CloseCompleteFixConfirmed(),
                "Requires a CloseComplete-fixed proxy binary. Set GOLDLAPEL_CLOSECOMPLETE_FIX=1 to run.");

            var port = NextPort();
            await using var gl = await GL.StartAsync(Upstream, opts => { opts.Port = port; });

            var table = "gl_cc_" + Guid.NewGuid().ToString("N").Substring(0, 8);

            // Connection A: prime the proxy's ext prepared cache by
            // preparing + executing a parameterized statement a few times.
            await using var connA = new NpgsqlConnection(gl.Url);
            await connA.OpenAsync();
            await using (var prime = new NpgsqlCommand("SELECT $1::int AS x", connA))
            {
                prime.Parameters.AddWithValue(1);
                await prime.PrepareAsync();
                for (int i = 0; i < 3; i++)
                {
                    prime.Parameters[0].Value = i;
                    await prime.ExecuteScalarAsync();
                }
            }

            // Connection B: issue DDL. This is the CREATE TABLE path that
            // pre-fix triggered ext_prep_cache.clear() on the proxy and
            // spilled CloseCompletes back toward extended-protocol users.
            await using (var connB = new NpgsqlConnection(gl.Url))
            {
                await connB.OpenAsync();
                await using var ddl = new NpgsqlCommand(
                    $"CREATE TABLE {table} (id int)", connB);
                await ddl.ExecuteNonQueryAsync();
            }

            // Connection A again: run a fresh parameterized + Prepare()d
            // query. Pre-fix, this is where Npgsql would observe the stray
            // CloseComplete and throw. Post-fix, it should round-trip.
            await using (var follow = new NpgsqlCommand("SELECT $1::int AS y", connA))
            {
                follow.Parameters.AddWithValue(99);
                await follow.PrepareAsync();
                var result = await follow.ExecuteScalarAsync();
                Assert.Equal(99, Convert.ToInt32(result));
            }

            // Cleanup.
            await using var clean = new NpgsqlConnection(gl.Url);
            await clean.OpenAsync();
            await using var drop = new NpgsqlCommand(
                $"DROP TABLE IF EXISTS {table}", clean);
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
