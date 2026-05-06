using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using Xunit;

namespace GoldLapel.Tests
{
    // ── DetectWrite ──────────────────────────────────────────

    public class DetectWriteTest
    {
        [Fact] public void Insert() => Assert.Equal("orders", NativeCache.DetectWrite("INSERT INTO orders VALUES (1)"));
        [Fact] public void InsertSchema() => Assert.Equal("orders", NativeCache.DetectWrite("INSERT INTO public.orders VALUES (1)"));
        [Fact] public void Update() => Assert.Equal("orders", NativeCache.DetectWrite("UPDATE orders SET name = 'x'"));
        [Fact] public void Delete() => Assert.Equal("orders", NativeCache.DetectWrite("DELETE FROM orders WHERE id = 1"));
        [Fact] public void Truncate() => Assert.Equal("orders", NativeCache.DetectWrite("TRUNCATE orders"));
        [Fact] public void TruncateTable() => Assert.Equal("orders", NativeCache.DetectWrite("TRUNCATE TABLE orders"));
        [Fact] public void CreateDdl() => Assert.Equal(NativeCache.DdlSentinel, NativeCache.DetectWrite("CREATE TABLE foo (id int)"));
        [Fact] public void AlterDdl() => Assert.Equal(NativeCache.DdlSentinel, NativeCache.DetectWrite("ALTER TABLE foo ADD COLUMN bar int"));
        [Fact] public void DropDdl() => Assert.Equal(NativeCache.DdlSentinel, NativeCache.DetectWrite("DROP TABLE foo"));
        [Fact] public void SelectReturnsNull() => Assert.Null(NativeCache.DetectWrite("SELECT * FROM orders"));
        [Fact] public void CaseInsensitive() => Assert.Equal("orders", NativeCache.DetectWrite("insert INTO Orders VALUES (1)"));
        [Fact] public void CopyFrom() => Assert.Equal("orders", NativeCache.DetectWrite("COPY orders FROM '/tmp/data.csv'"));
        [Fact] public void CopyToNull() => Assert.Null(NativeCache.DetectWrite("COPY orders TO '/tmp/data.csv'"));
        [Fact] public void CopySubqueryNull() => Assert.Null(NativeCache.DetectWrite("COPY (SELECT * FROM orders) TO '/tmp/data.csv'"));
        [Fact] public void WithCteInsert() => Assert.Equal(NativeCache.DdlSentinel, NativeCache.DetectWrite("WITH x AS (SELECT 1) INSERT INTO foo SELECT * FROM x"));
        [Fact] public void WithCteSelect() => Assert.Null(NativeCache.DetectWrite("WITH x AS (SELECT 1) SELECT * FROM x"));
        [Fact] public void Empty() => Assert.Null(NativeCache.DetectWrite(""));
        [Fact] public void Whitespace() => Assert.Null(NativeCache.DetectWrite("   "));
        [Fact] public void CopyWithColumns() => Assert.Equal("orders", NativeCache.DetectWrite("COPY orders(id, name) FROM '/tmp/data.csv'"));

        // ── SELECT-INTO false-positive on string literals ──
        //
        // Pre-fix tokenizer split SELECTs on whitespace only, so a bare
        // `INTO` inside a `'...'` / `"..."` literal got classified as the
        // SELECT-INTO DDL form and returned DdlSentinel — silently
        // flushing the whole cache on plain reads. Fix: re-tokenize the
        // SELECT branch from a literal-stripped form. Mirrors
        // goldlapel-js commit `63753fe`. Spec:
        // docs/todos/wrapper-detect-write-string-literal-false-positive.md.
        [Fact] public void SelectIntoSingleQuotedLiteralIsNotDdl()
            => Assert.Null(NativeCache.DetectWrite("SELECT 'INSERT INTO orders;' FROM audit_log"));
        [Fact] public void SelectIntoDoubleQuotedIdentifierIsNotDdl()
            => Assert.Null(NativeCache.DetectWrite("SELECT * FROM \"into_table\""));
        [Fact] public void SelectIntoLikePatternLiteralIsNotDdl()
            => Assert.Null(NativeCache.DetectWrite("SELECT message FROM logs WHERE message LIKE '%INTO%'"));
        [Fact] public void SelectIntoDoubledQuoteEscapeIsNotDdl()
            => Assert.Null(NativeCache.DetectWrite("SELECT 'It''s INTO trouble' FROM notes"));
        [Fact] public void RealSelectIntoNewTableStillDdl()
            => Assert.Equal(NativeCache.DdlSentinel, NativeCache.DetectWrite("SELECT * INTO new_table FROM source"));
        [Fact] public void RealSelectIntoTempStillDdl()
            => Assert.Equal(NativeCache.DdlSentinel, NativeCache.DetectWrite("SELECT id INTO TEMP scratch FROM source"));
    }

    // ── ExtractTables ────────────────────────────────────────

    public class ExtractTablesTest
    {
        [Fact]
        public void SimpleFrom() => Assert.Contains("orders", NativeCache.ExtractTables("SELECT * FROM orders"));

        [Fact]
        public void Join()
        {
            var t = NativeCache.ExtractTables("SELECT * FROM orders o JOIN customers c ON o.cid = c.id");
            Assert.Contains("orders", t);
            Assert.Contains("customers", t);
        }

        [Fact]
        public void SchemaQualified() => Assert.Contains("orders", NativeCache.ExtractTables("SELECT * FROM public.orders"));

        [Fact]
        public void MultipleJoins() => Assert.Equal(3, NativeCache.ExtractTables("SELECT * FROM orders JOIN items ON 1=1 JOIN products ON 1=1").Count);

        [Fact]
        public void CaseInsensitive() => Assert.Contains("orders", NativeCache.ExtractTables("SELECT * FROM ORDERS"));

        [Fact]
        public void NoTables() => Assert.Empty(NativeCache.ExtractTables("SELECT 1"));

        [Fact]
        public void Subquery()
        {
            var t = NativeCache.ExtractTables("SELECT * FROM orders WHERE id IN (SELECT oid FROM users)");
            Assert.Contains("orders", t);
            Assert.Contains("users", t);
        }
    }

    // ── DetectTxTransition ───────────────────────────────────
    //
    // Multi-segment tx-flag bookkeeping. The legacy IsTxStart/IsTxEnd
    // checks only the first token, so a multi-statement body like
    // `BEGIN; INSERT...; COMMIT` flips the wrapper-side InTransaction
    // flag based on `BEGIN` alone — server ends out-of-tx, wrapper
    // thinks still in tx, cache is bypassed forever. DetectTxTransition
    // walks every segment and returns the final state change so the
    // wrapper settles to the correct InTransaction value.

    public class DetectTxTransitionTest
    {
        [Fact] public void EmptyReturnsNull()
            => Assert.Null(NativeCache.DetectTxTransition(""));

        [Fact] public void NullReturnsNull()
            => Assert.Null(NativeCache.DetectTxTransition(null));

        [Fact] public void WhitespaceReturnsNull()
            => Assert.Null(NativeCache.DetectTxTransition("   "));

        [Fact] public void PlainSelectReturnsNull()
            => Assert.Null(NativeCache.DetectTxTransition("SELECT * FROM orders"));

        [Fact] public void SingleBeginIsStart()
            => Assert.True(NativeCache.DetectTxTransition("BEGIN"));

        [Fact] public void SingleStartTransactionIsStart()
            => Assert.True(NativeCache.DetectTxTransition("START TRANSACTION"));

        [Fact] public void SingleCommitIsEnd()
            => Assert.False(NativeCache.DetectTxTransition("COMMIT"));

        [Fact] public void SingleRollbackIsEnd()
            => Assert.False(NativeCache.DetectTxTransition("ROLLBACK"));

        [Fact] public void SingleEndIsEnd()
            => Assert.False(NativeCache.DetectTxTransition("END"));

        // SAVEPOINT and RELEASE SAVEPOINT are intra-transaction markers,
        // not boundaries. SAVEPOINT errors outside a tx (no-op flip), and
        // RELEASE does NOT end the outer tx — flipping the flag to false
        // would desync wrapper from server. Both are no-change.
        [Fact] public void SingleSavepointNoChange()
            => Assert.Null(NativeCache.DetectTxTransition("SAVEPOINT s1"));

        [Fact] public void SingleReleaseNoChange()
            => Assert.Null(NativeCache.DetectTxTransition("RELEASE SAVEPOINT s1"));

        [Fact] public void CaseInsensitive()
        {
            Assert.True(NativeCache.DetectTxTransition("begin"));
            Assert.False(NativeCache.DetectTxTransition("commit"));
        }

        // ── The headline regression: multi-statement BEGIN/COMMIT ──
        //
        // Pre-fix bug: only the first token (BEGIN) was inspected, so
        // the wrapper got stuck in InTransaction=true after the body
        // completed. Fix: last-segment-wins yields the correct final
        // out-of-tx state.

        [Fact] public void BeginInsertCommitSettlesEnd()
        {
            var t = NativeCache.DetectTxTransition("BEGIN; INSERT INTO orders VALUES (1); COMMIT");
            Assert.False(t);
        }

        [Fact] public void BeginInsertRollbackSettlesEnd()
        {
            var t = NativeCache.DetectTxTransition("BEGIN; INSERT INTO orders VALUES (1); ROLLBACK");
            Assert.False(t);
        }

        [Fact] public void BeginWithoutCommitStaysStart()
        {
            // `BEGIN; SELECT 1` — only BEGIN is tx-relevant; SELECT
            // doesn't flip the flag. Final state should be true.
            var t = NativeCache.DetectTxTransition("BEGIN; SELECT 1");
            Assert.True(t);
        }

        [Fact] public void CommitWithoutBeginStaysEnd()
        {
            // `SELECT 1; COMMIT` — final segment is COMMIT, transition false.
            var t = NativeCache.DetectTxTransition("SELECT 1; COMMIT");
            Assert.False(t);
        }

        [Fact] public void NestedSavepointReleaseCommit()
        {
            // `BEGIN; SAVEPOINT a; INSERT...; RELEASE a; COMMIT` — the
            // SAVEPOINT/RELEASE markers don't change the flag; only BEGIN
            // (true) and COMMIT (false) matter, so the body settles false.
            var t = NativeCache.DetectTxTransition(
                "BEGIN; SAVEPOINT a; INSERT INTO orders VALUES (1); RELEASE SAVEPOINT a; COMMIT");
            Assert.False(t);
        }

        [Fact] public void SavepointReleaseInsideTxStaysOpen()
        {
            // Regression: `BEGIN; SAVEPOINT s; SELECT 1; RELEASE s; SELECT 2`
            // — InTransaction must stay true through the SAVEPOINT/RELEASE
            // markers. Pre-fix, RELEASE incorrectly flipped to false,
            // desyncing the wrapper from the still-open server transaction
            // and serving stale cache reads for the trailing SELECT 2.
            var t = NativeCache.DetectTxTransition(
                "BEGIN; SAVEPOINT s; SELECT 1; RELEASE SAVEPOINT s; SELECT 2");
            Assert.True(t);
        }

        [Fact] public void SavepointReleaseFullCycleClosesOnCommit()
        {
            // `BEGIN; SAVEPOINT s; SELECT 1; RELEASE s; SELECT 2; COMMIT`
            // — same as above but with the trailing COMMIT, which is the
            // only segment that flips the flag to false.
            var t = NativeCache.DetectTxTransition(
                "BEGIN; SAVEPOINT s; SELECT 1; RELEASE SAVEPOINT s; SELECT 2; COMMIT");
            Assert.False(t);
        }

        [Fact] public void SetThenSelectNoTxChange()
        {
            // `SET app.user_id = '42'; SELECT 1` — no tx-relevant tokens.
            var t = NativeCache.DetectTxTransition("SET app.user_id = '42'; SELECT 1");
            Assert.Null(t);
        }

        [Fact] public void SemicolonInsideStringLiteralNotSplit()
        {
            // The splitter respects `'...'` literals — a `;` inside a
            // literal must not be treated as a segment boundary.
            // `SELECT 'BEGIN; COMMIT'` has no real tx token outside the
            // literal.
            var t = NativeCache.DetectTxTransition("SELECT 'BEGIN; COMMIT' FROM logs");
            Assert.Null(t);
        }

        [Fact] public void TrailingSemicolonHarmless()
        {
            // Trailing `;` produces an empty final segment that the
            // splitter drops; should not interfere with the prior tx
            // segment's classification.
            Assert.False(NativeCache.DetectTxTransition("BEGIN; INSERT INTO t VALUES (1); COMMIT;"));
        }
    }

    // ── Cache operations ─────────────────────────────────────

    public class CacheOpsTest : IDisposable
    {
        public CacheOpsTest() { NativeCache.Reset(); }
        public void Dispose() { NativeCache.Reset(); }

        private NativeCache MakeCache()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            return cache;
        }

        [Fact]
        public void PutAndGet()
        {
            var cache = MakeCache();
            cache.Put("SELECT * FROM users", null,
                new[] { new object[] { "1", "alice" } },
                new[] { "id", "name" });
            var entry = cache.Get("SELECT * FROM users", null);
            Assert.NotNull(entry);
            Assert.Single(entry.Rows);
        }

        [Fact]
        public void MissReturnsNull()
        {
            var cache = MakeCache();
            Assert.Null(cache.Get("SELECT 1", null));
        }

        [Fact]
        public void ParamsDifferentiate()
        {
            var cache = MakeCache();
            cache.Put("SELECT $1", new object[] { 1 },
                new[] { new object[] { "1" } }, new[] { "id" });
            cache.Put("SELECT $1", new object[] { 2 },
                new[] { new object[] { "2" } }, new[] { "id" });
            Assert.Equal("1", cache.Get("SELECT $1", new object[] { 1 }).Rows[0][0]);
            Assert.Equal("2", cache.Get("SELECT $1", new object[] { 2 }).Rows[0][0]);
        }

        [Fact]
        public void Stats()
        {
            var cache = MakeCache();
            cache.Put("SELECT 1", null,
                new[] { new object[] { "1" } }, new[] { "x" });
            cache.Get("SELECT 1", null);
            cache.Get("SELECT 2", null);
            Assert.Equal(1, Interlocked.Read(ref cache.StatsHits));
            Assert.Equal(1, Interlocked.Read(ref cache.StatsMisses));
        }
    }

    // ── Invalidation ─────────────────────────────────────────

    public class InvalidationTest : IDisposable
    {
        public InvalidationTest() { NativeCache.Reset(); }
        public void Dispose() { NativeCache.Reset(); }

        private NativeCache MakeCache()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            return cache;
        }

        [Fact]
        public void InvalidateTable()
        {
            var cache = MakeCache();
            cache.Put("SELECT * FROM orders", null,
                new[] { new object[] { "1" } }, new[] { "id" });
            cache.Put("SELECT * FROM users", null,
                new[] { new object[] { "2" } }, new[] { "id" });
            cache.InvalidateTable("orders");
            Assert.Null(cache.Get("SELECT * FROM orders", null));
            Assert.NotNull(cache.Get("SELECT * FROM users", null));
        }

        [Fact]
        public void InvalidateAll()
        {
            var cache = MakeCache();
            cache.Put("SELECT * FROM orders", null,
                new[] { new object[] { "1" } }, new[] { "id" });
            cache.Put("SELECT * FROM users", null,
                new[] { new object[] { "2" } }, new[] { "id" });
            cache.InvalidateAll();
            Assert.Null(cache.Get("SELECT * FROM orders", null));
            Assert.Null(cache.Get("SELECT * FROM users", null));
        }

        [Fact]
        public void CrossReferenced()
        {
            var cache = MakeCache();
            cache.Put("SELECT * FROM orders JOIN users ON 1=1", null,
                new[] { new object[] { "1" } }, new[] { "id" });
            cache.InvalidateTable("orders");
            Assert.Null(cache.Get("SELECT * FROM orders JOIN users ON 1=1", null));
        }
    }

    // ── Signal processing ────────────────────────────────────

    public class SignalTest : IDisposable
    {
        public SignalTest() { NativeCache.Reset(); }
        public void Dispose() { NativeCache.Reset(); }

        private NativeCache MakeCache()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            return cache;
        }

        [Fact]
        public void TableSignal()
        {
            var cache = MakeCache();
            cache.Put("SELECT * FROM orders", null,
                new[] { new object[] { "1" } }, new[] { "id" });
            cache.ProcessSignal("I:orders");
            Assert.Null(cache.Get("SELECT * FROM orders", null));
        }

        [Fact]
        public void WildcardSignal()
        {
            var cache = MakeCache();
            cache.Put("SELECT * FROM orders", null,
                new[] { new object[] { "1" } }, new[] { "id" });
            cache.ProcessSignal("I:*");
            Assert.Null(cache.Get("SELECT * FROM orders", null));
        }

        [Fact]
        public void KeepalivePreserves()
        {
            var cache = MakeCache();
            cache.Put("SELECT * FROM orders", null,
                new[] { new object[] { "1" } }, new[] { "id" });
            cache.ProcessSignal("P:");
            Assert.NotNull(cache.Get("SELECT * FROM orders", null));
        }

        [Fact]
        public void UnknownPreserves()
        {
            var cache = MakeCache();
            cache.Put("SELECT * FROM orders", null,
                new[] { new object[] { "1" } }, new[] { "id" });
            cache.ProcessSignal("X:something");
            Assert.NotNull(cache.Get("SELECT * FROM orders", null));
        }
    }

    // ── Push invalidation ────────────────────────────────────

    public class PushInvalidationTest : IDisposable
    {
        public PushInvalidationTest() { NativeCache.Reset(); }
        public void Dispose() { NativeCache.Reset(); }

        [Fact]
        public void RemoteSignal()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            cache.Put("SELECT * FROM orders", null,
                new[] { new object[] { "1" } }, new[] { "id" });

            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            var port = ((IPEndPoint)listener.LocalEndpoint).Port;
            try
            {
                cache.SetConnected(false);
                cache.ConnectInvalidation(port);
                var conn = listener.AcceptTcpClient();
                Thread.Sleep(100);

                Assert.True(cache.IsConnected);
                var writer = new StreamWriter(conn.GetStream()) { AutoFlush = true };
                writer.WriteLine("I:orders");
                Thread.Sleep(200);

                Assert.Null(cache.Get("SELECT * FROM orders", null));

                conn.Close();
                cache.StopInvalidation();
            }
            finally
            {
                listener.Stop();
            }
        }

        [Fact]
        public void ConnectionDropClears()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            cache.Put("SELECT * FROM orders", null,
                new[] { new object[] { "1" } }, new[] { "id" });

            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            var port = ((IPEndPoint)listener.LocalEndpoint).Port;
            try
            {
                cache.SetConnected(false);
                cache.ConnectInvalidation(port);
                var conn = listener.AcceptTcpClient();
                Thread.Sleep(100);

                Assert.True(cache.IsConnected);
                conn.Close();
                Thread.Sleep(500);

                Assert.False(cache.IsConnected);
                Assert.Equal(0, cache.Size);

                cache.StopInvalidation();
            }
            finally
            {
                listener.Stop();
            }
        }
    }

    // ── Cache capacity (TOCTOU regression) ────────────────────

    [Collection("NativeCacheTelemetry")]
    public class CacheCapacityTest : IDisposable
    {
        public CacheCapacityTest() { NativeCache.Reset(); }
        public void Dispose() { NativeCache.Reset(); }

        [Fact]
        public void ConcurrentPutDoesNotExceedMax()
        {
            // Set a small cache size via env var
            var origSize = Environment.GetEnvironmentVariable("GOLDLAPEL_NATIVE_CACHE_SIZE");
            try
            {
                Environment.SetEnvironmentVariable("GOLDLAPEL_NATIVE_CACHE_SIZE", "10");
                var cache = new NativeCache();
                cache.SetConnected(true);

                // Use many threads to hammer Put concurrently
                var threads = new Thread[20];
                for (int t = 0; t < threads.Length; t++)
                {
                    var threadId = t;
                    threads[t] = new Thread(() =>
                    {
                        for (int i = 0; i < 50; i++)
                        {
                            var sql = $"SELECT * FROM t{threadId}_{i}";
                            cache.Put(sql, null,
                                new[] { new object[] { i } }, new[] { "id" });
                        }
                    });
                }

                foreach (var t in threads) t.Start();
                foreach (var t in threads) t.Join();

                // Cache size should never exceed the max (10)
                Assert.True(cache.Size <= 10,
                    $"Cache size {cache.Size} exceeded max 10");
            }
            finally
            {
                Environment.SetEnvironmentVariable("GOLDLAPEL_NATIVE_CACHE_SIZE", origSize);
            }
        }
    }

    // ── MakeKey ──────────────────────────────────────────────
    //
    // Cache key shape: `<sql>\0<state_hash_hex>\0<params>`. The
    // state-hash segment renders as lowercase hex (parity with the
    // proxy's `{:x}` formatting, eases log correlation). State hash
    // 0 means "no unsafe-GUC state set" — fresh connections still
    // hit cache slots populated by other state-0 connections.

    public class MakeKeyTest
    {
        [Fact]
        public void NullParams()
        {
            var key = NativeCache.MakeKey("SELECT 1", null);
            Assert.Equal("SELECT 1\00\0null", key);
        }

        [Fact]
        public void EmptyParams()
        {
            var key = NativeCache.MakeKey("SELECT 1", new object[0]);
            Assert.Equal("SELECT 1\00\0null", key);
        }

        [Fact]
        public void WithParams()
        {
            var key = NativeCache.MakeKey("SELECT $1", new object[] { 42 });
            Assert.Equal("SELECT $1\00\042", key);
        }

        [Fact]
        public void MultipleParams()
        {
            var key = NativeCache.MakeKey("SELECT $1, $2", new object[] { "a", "b" });
            Assert.Equal("SELECT $1, $2\00\0a,b", key);
        }

        [Fact]
        public void StateHashRendersAsLowercaseHex()
        {
            // 0xdeadbeef = 3735928559 — chosen for the obvious hex form.
            var key = NativeCache.MakeKey("SELECT 1", null, 0xdeadbeefL);
            Assert.Equal("SELECT 1\0deadbeef\0null", key);
        }

        [Fact]
        public void DifferentStateHashesYieldDifferentKeys()
        {
            // Same SQL + params, different state hashes → different keys
            // (the whole point of folding the hash into the key).
            var k0 = NativeCache.MakeKey("SELECT * FROM accounts", null, 0);
            var k1 = NativeCache.MakeKey("SELECT * FROM accounts", null, 0x42);
            Assert.NotEqual(k0, k1);
        }
    }

    // ── Native-cache telemetry: counters + snapshot ──────────
    //
    // The telemetry tests below read GOLDLAPEL_NATIVE_CACHE_SIZE and
    // GOLDLAPEL_REPORT_STATS during construction. Process-global env vars
    // race when test classes run in parallel, so we put them all in one
    // collection to serialize them. See xUnit collection-fixture docs.

    [CollectionDefinition("NativeCacheTelemetry", DisableParallelization = true)]
    public class NativeCacheTelemetryCollection { }

    [Collection("NativeCacheTelemetry")]
    public class EvictionsCounterTest : IDisposable
    {
        public EvictionsCounterTest() { NativeCache.Reset(); }
        public void Dispose() { NativeCache.Reset(); }

        private NativeCache MakeCache(int max)
        {
            Environment.SetEnvironmentVariable("GOLDLAPEL_NATIVE_CACHE_SIZE", max.ToString());
            try
            {
                var cache = new NativeCache();
                cache.SetConnected(true);
                return cache;
            }
            finally
            {
                Environment.SetEnvironmentVariable("GOLDLAPEL_NATIVE_CACHE_SIZE", null);
            }
        }

        [Fact]
        public void StartsZero()
        {
            var cache = MakeCache(4);
            Assert.Equal(0L, Interlocked.Read(ref cache.StatsEvictions));
        }

        [Fact]
        public void BumpsOnOverflow()
        {
            var cache = MakeCache(4);
            for (int i = 0; i < 8; i++)
                cache.Put($"SELECT {i}", null, new[] { new object[] { i } }, new[] { "id" });
            // 8 puts, capacity 4 → 4 evictions.
            Assert.Equal(4L, Interlocked.Read(ref cache.StatsEvictions));
        }

        [Fact]
        public void NoBumpWithinCapacity()
        {
            var cache = MakeCache(8);
            for (int i = 0; i < 4; i++)
                cache.Put($"SELECT {i}", null, new[] { new object[] { i } }, new[] { "id" });
            Assert.Equal(0L, Interlocked.Read(ref cache.StatsEvictions));
        }
    }

    [Collection("NativeCacheTelemetry")]
    public class SnapshotShapeTest : IDisposable
    {
        public SnapshotShapeTest() { NativeCache.Reset(); }
        public void Dispose() { NativeCache.Reset(); }

        private NativeCache MakeCache(int max = 64)
        {
            Environment.SetEnvironmentVariable("GOLDLAPEL_NATIVE_CACHE_SIZE", max.ToString());
            try
            {
                var cache = new NativeCache();
                cache.SetConnected(true);
                return cache;
            }
            finally
            {
                Environment.SetEnvironmentVariable("GOLDLAPEL_NATIVE_CACHE_SIZE", null);
            }
        }

        [Fact]
        public void CarriesRequiredFields()
        {
            var cache = MakeCache(64);
            cache.Put("SELECT 1", null, new[] { new object[] { 1 } }, new[] { "id" });
            cache.Get("SELECT 1", null);
            cache.Get("SELECT MISS", null);
            var snap = cache.BuildSnapshot();
            Assert.Equal(cache.WrapperId, snap["wrapper_id"]);
            Assert.Equal("dotnet", snap["lang"]);
            Assert.True(snap.ContainsKey("version"));
            Assert.Equal(1L, snap["hits"]);
            Assert.Equal(1L, snap["misses"]);
            Assert.Equal(0L, snap["evictions"]);
            Assert.Equal(1L, snap["current_size_entries"]);
            Assert.Equal(64L, snap["capacity_entries"]);
        }

        [Fact]
        public void WrapperIdIsUuidV4()
        {
            var cache = MakeCache();
            // Guid.Parse rejects malformed strings; Variant 1 + version 4
            // is what Guid.NewGuid() produces.
            var g = Guid.Parse(cache.WrapperId);
            // Version is the 4 high-order bits of the third group: peek at
            // the 13th hex char of the canonical form.
            var s = g.ToString();
            Assert.Equal('4', s[14]);
        }

        [Fact]
        public void WrapperIdStableAcrossCalls()
        {
            var cache = MakeCache();
            var a = (string)cache.BuildSnapshot()["wrapper_id"];
            var b = (string)cache.BuildSnapshot()["wrapper_id"];
            Assert.Equal(a, b);
        }
    }

    // ── Native-cache telemetry: state-change emission via test hook ────

    [Collection("NativeCacheTelemetry")]
    public class StateChangeUnitTest : IDisposable
    {
        public StateChangeUnitTest() { NativeCache.Reset(); }
        public void Dispose() { NativeCache.Reset(); }

        private NativeCache MakeCache(int max)
        {
            Environment.SetEnvironmentVariable("GOLDLAPEL_NATIVE_CACHE_SIZE", max.ToString());
            try
            {
                var cache = new NativeCache();
                cache.SetConnected(true);
                return cache;
            }
            finally
            {
                Environment.SetEnvironmentVariable("GOLDLAPEL_NATIVE_CACHE_SIZE", null);
            }
        }

        [Fact]
        public void CacheFullFiresWhenEvictionsDominate()
        {
            // Capacity 4 — every put past the 4th evicts. Window = 200.
            var cache = MakeCache(4);
            var emissions = new List<string>();
            cache.SendHookForTests = line => { lock (emissions) emissions.Add(line); };

            for (int i = 0; i < NativeCache.EvictRateWindow + 10; i++)
                cache.Put($"SELECT {i}", null, new[] { new object[] { i } }, new[] { "id" });

            Assert.Contains(emissions, e => e.Contains("cache_full"));
        }

        [Fact]
        public void CacheFullDoesNotFireBelowWindow()
        {
            // With fewer puts than the window, no state-change fires.
            var cache = MakeCache(2);
            var emissions = new List<string>();
            cache.SendHookForTests = line => { lock (emissions) emissions.Add(line); };

            for (int i = 0; i < NativeCache.EvictRateWindow - 1; i++)
                cache.Put($"SELECT {i}", null, new[] { new object[] { i } }, new[] { "id" });

            Assert.DoesNotContain(emissions, e => e.Contains("cache_full"));
        }

        [Fact]
        public void RequestSnapshotEmitsResponse()
        {
            var cache = MakeCache(64);
            var emissions = new List<string>();
            cache.SendHookForTests = line => { lock (emissions) emissions.Add(line); };

            cache.ProcessRequest("snapshot");

            var rLines = emissions.Where(e => e.StartsWith("R:")).ToList();
            Assert.Single(rLines);
            using var doc = JsonDocument.Parse(rLines[0].Substring(2));
            Assert.Equal(cache.WrapperId, doc.RootElement.GetProperty("wrapper_id").GetString());
        }

        [Fact]
        public void RequestEmptyBodyTreatedAsSnapshot()
        {
            var cache = MakeCache(64);
            var emissions = new List<string>();
            cache.SendHookForTests = line => { lock (emissions) emissions.Add(line); };

            cache.ProcessRequest("");

            Assert.Single(emissions.Where(e => e.StartsWith("R:")));
        }

        [Fact]
        public void RequestUnknownBodySilentlyDropped()
        {
            var cache = MakeCache(64);
            var emissions = new List<string>();
            cache.SendHookForTests = line => { lock (emissions) emissions.Add(line); };

            cache.ProcessRequest("future_request_type");

            Assert.Empty(emissions.Where(e => e.StartsWith("R:")));
        }

        [Fact]
        public void UnknownProxyPrefixSilentlyIgnored()
        {
            // Backwards-compat: the wrapper must not crash when a future
            // proxy sends an unknown prefix.
            var cache = MakeCache(64);
            cache.ProcessSignal("Z:future-prefix");
            cache.ProcessSignal("$:bogus");
            // No assertion — just no exception.
        }

        [Fact]
        public void EmitWrapperDisconnectedOnlyOnce()
        {
            var cache = MakeCache(64);
            var emissions = new List<string>();
            cache.SendHookForTests = line => { lock (emissions) emissions.Add(line); };

            cache.EmitWrapperDisconnected();
            cache.EmitWrapperDisconnected();
            cache.EmitWrapperDisconnected();

            var sLines = emissions.Where(e => e.StartsWith("S:") && e.Contains("wrapper_disconnected")).ToList();
            Assert.Single(sLines);
        }
    }

    [Collection("NativeCacheTelemetry")]
    public class ReportStatsOptOutTest : IDisposable
    {
        public ReportStatsOptOutTest() { NativeCache.Reset(); }
        public void Dispose()
        {
            Environment.SetEnvironmentVariable("GOLDLAPEL_REPORT_STATS", null);
            NativeCache.Reset();
        }

        [Fact]
        public void DisabledSuppressesEmissions()
        {
            Environment.SetEnvironmentVariable("GOLDLAPEL_REPORT_STATS", "false");
            var cache = new NativeCache();
            cache.SetConnected(true);
            Assert.False(cache.ReportStats);

            var emissions = new List<string>();
            cache.SendHookForTests = line => { lock (emissions) emissions.Add(line); };

            cache.EmitStateChange("wrapper_connected");
            cache.ProcessRequest("snapshot");
            cache.EmitWrapperDisconnected();

            Assert.Empty(emissions);
        }
    }

    // ── DisableNativeCache: explicit native-cache disable, orthogonal to size ────

    [Collection("NativeCacheTelemetry")]
    public class DisableNativeCacheTest : IDisposable
    {
        public DisableNativeCacheTest() { NativeCache.Reset(); }
        public void Dispose() { NativeCache.Reset(); }

        private NativeCache MakeCache(bool disableNativeCache)
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            cache.DisableNativeCache = disableNativeCache;
            return cache;
        }

        [Fact]
        public void DefaultsToFalse()
        {
            var cache = new NativeCache();
            Assert.False(cache.DisableNativeCache);
        }

        [Fact]
        public void GetReturnsNullWhenDisabled()
        {
            var cache = MakeCache(disableNativeCache: true);
            // Even after a Put (which is also a no-op when disabled), Get must miss.
            cache.Put("SELECT * FROM users", null,
                new[] { new object[] { "1", "alice" } },
                new[] { "id", "name" });
            Assert.Null(cache.Get("SELECT * FROM users", null));
        }

        [Fact]
        public void PutIsNoOpWhenDisabled()
        {
            var cache = MakeCache(disableNativeCache: true);
            cache.Put("SELECT * FROM users", null,
                new[] { new object[] { "1", "alice" } },
                new[] { "id", "name" });
            // Cache stays empty — no entry, no LRU bookkeeping.
            Assert.Equal(0, cache.Size);
        }

        [Fact]
        public void MissesTickHitsAndEvictionsStayZero()
        {
            var cache = MakeCache(disableNativeCache: true);
            // Put first (no-op). Then three Gets — each must tick a miss.
            cache.Put("SELECT * FROM users", null,
                new[] { new object[] { "1" } }, new[] { "id" });
            cache.Get("SELECT * FROM users", null);
            cache.Get("SELECT * FROM users", null);
            cache.Get("SELECT 2", null);
            Assert.Equal(0, Interlocked.Read(ref cache.StatsHits));
            Assert.Equal(3, Interlocked.Read(ref cache.StatsMisses));
            Assert.Equal(0, Interlocked.Read(ref cache.StatsEvictions));
        }

        [Fact]
        public void PutDoesNotEvictWhenDisabled()
        {
            // With a tiny cache size, normally a flood of puts would evict.
            // With DisableNativeCache set, no entries are inserted so no
            // evictions occur.
            var origSize = Environment.GetEnvironmentVariable("GOLDLAPEL_NATIVE_CACHE_SIZE");
            try
            {
                Environment.SetEnvironmentVariable("GOLDLAPEL_NATIVE_CACHE_SIZE", "2");
                var cache = new NativeCache();
                cache.SetConnected(true);
                cache.DisableNativeCache = true;
                for (int i = 0; i < 50; i++)
                    cache.Put($"SELECT {i}", null, new[] { new object[] { i } }, new[] { "id" });
                Assert.Equal(0L, Interlocked.Read(ref cache.StatsEvictions));
                Assert.Equal(0, cache.Size);
            }
            finally
            {
                Environment.SetEnvironmentVariable("GOLDLAPEL_NATIVE_CACHE_SIZE", origSize);
            }
        }

        [Fact]
        public void SnapshotCarriesDisabledWhenSet()
        {
            var cache = MakeCache(disableNativeCache: true);
            cache.Get("SELECT 1", null);  // tick a miss for realism
            var snap = cache.BuildSnapshot();
            Assert.True(snap.ContainsKey("disabled"));
            Assert.Equal(true, snap["disabled"]);
            // Other counters must still surface — telemetry pipeline stays intact.
            Assert.Equal(1L, snap["misses"]);
            Assert.Equal(0L, snap["hits"]);
        }

        [Fact]
        public void SnapshotOmitsDisabledWhenUnset()
        {
            // Default (DisableNativeCache=false) snapshot must not carry
            // the flag at all — keeps the wire format minimal for the
            // common case.
            var cache = MakeCache(disableNativeCache: false);
            var snap = cache.BuildSnapshot();
            Assert.False(snap.ContainsKey("disabled"));
        }

        [Fact]
        public void DefaultPathStillCachesWhenNotDisabled()
        {
            // Regression guard: the disable path must not leak into the
            // default flow.
            var cache = MakeCache(disableNativeCache: false);
            cache.Put("SELECT * FROM users", null,
                new[] { new object[] { "1", "alice" } },
                new[] { "id", "name" });
            var entry = cache.Get("SELECT * FROM users", null);
            Assert.NotNull(entry);
            Assert.Equal(1L, Interlocked.Read(ref cache.StatsHits));
        }

        [Fact]
        public void TogglingDisableNativeCacheMidLifeFlipsBehavior()
        {
            // Set/get pattern: cache normally, then disable mid-flight —
            // subsequent gets miss even though the entry is still in the
            // dict. This matches the Ruby behavior: the layer is off, the
            // dict is irrelevant.
            var cache = MakeCache(disableNativeCache: false);
            cache.Put("SELECT 1", null, new[] { new object[] { "1" } }, new[] { "id" });
            Assert.NotNull(cache.Get("SELECT 1", null));

            cache.DisableNativeCache = true;
            Assert.Null(cache.Get("SELECT 1", null));

            cache.DisableNativeCache = false;
            // Entry is still present from earlier Put.
            Assert.NotNull(cache.Get("SELECT 1", null));
        }
    }

    // ── Native-cache telemetry: real-socket integration ────────────────

    [Collection("NativeCacheTelemetry")]
    public class StateChangeIntegrationTest : IDisposable
    {
        public StateChangeIntegrationTest() { NativeCache.Reset(); }
        public void Dispose() { NativeCache.Reset(); }

        private static (TcpListener listener, int port) SpawnServer()
        {
            var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            var port = ((IPEndPoint)listener.LocalEndpoint).Port;
            return (listener, port);
        }

        // Accept the wrapper's connection and start a buffered reader. Lines
        // accumulate in the returned list (lock on it before reading).
        private static (TcpClient conn, List<string> lines, ManualResetEventSlim stop) AcceptWithBuf(TcpListener server)
        {
            var conn = server.AcceptTcpClient();
            conn.ReceiveTimeout = 500;
            var lines = new List<string>();
            var stop = new ManualResetEventSlim(false);

            var t = new Thread(() =>
            {
                var stream = conn.GetStream();
                var buf = new byte[4096];
                var pending = new MemoryStream();
                while (!stop.IsSet)
                {
                    int n;
                    try { n = stream.Read(buf, 0, buf.Length); }
                    catch (IOException) { return; }
                    catch (ObjectDisposedException) { return; }
                    if (n <= 0) return;
                    pending.Write(buf, 0, n);
                    var pendingBytes = pending.ToArray();
                    pending.SetLength(0);
                    int start = 0;
                    for (int i = 0; i < pendingBytes.Length; i++)
                    {
                        if (pendingBytes[i] == (byte)'\n')
                        {
                            var line = Encoding.UTF8.GetString(pendingBytes, start, i - start);
                            lock (lines) lines.Add(line);
                            start = i + 1;
                        }
                    }
                    if (start < pendingBytes.Length)
                        pending.Write(pendingBytes, start, pendingBytes.Length - start);
                }
            }) { IsBackground = true };
            t.Start();

            return (conn, lines, stop);
        }

        private static bool WaitFor(Func<bool> predicate, double timeoutSec = 2.0)
        {
            var deadline = DateTime.UtcNow.AddSeconds(timeoutSec);
            while (DateTime.UtcNow < deadline)
            {
                if (predicate()) return true;
                Thread.Sleep(20);
            }
            return false;
        }

        [Fact]
        public void WrapperConnectedEmittedOnSocketConnect()
        {
            var cache = new NativeCache();
            var (server, port) = SpawnServer();
            try
            {
                cache.ConnectInvalidation(port);
                var (conn, lines, stop) = AcceptWithBuf(server);
                try
                {
                    Assert.True(WaitFor(() =>
                    {
                        lock (lines) return lines.Any(l => l.StartsWith("S:"));
                    }), "expected S: line within 2s");

                    string sLine;
                    lock (lines) sLine = lines.First(l => l.StartsWith("S:"));
                    using var doc = JsonDocument.Parse(sLine.Substring(2));
                    Assert.Equal("wrapper_connected", doc.RootElement.GetProperty("state").GetString());
                    Assert.Equal(cache.WrapperId, doc.RootElement.GetProperty("wrapper_id").GetString());
                    Assert.Equal("dotnet", doc.RootElement.GetProperty("lang").GetString());
                }
                finally
                {
                    stop.Set();
                    try { conn.Close(); } catch { }
                }
            }
            finally
            {
                cache.StopInvalidation();
                server.Stop();
            }
        }

        [Fact]
        public void SnapshotRequestReturnsResponse()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            cache.Put("SELECT 1", null, new[] { new object[] { 1 } }, new[] { "id" });
            cache.Get("SELECT 1", null);
            cache.SetConnected(false);

            var (server, port) = SpawnServer();
            try
            {
                cache.ConnectInvalidation(port);
                var (conn, lines, stop) = AcceptWithBuf(server);
                try
                {
                    // Wait for wrapper_connected first so we know the
                    // socket is wired.
                    Assert.True(WaitFor(() =>
                    {
                        lock (lines) return lines.Any(l => l.StartsWith("S:"));
                    }), "expected S: line first");

                    var writer = new StreamWriter(conn.GetStream(), new UTF8Encoding(false)) { AutoFlush = true };
                    writer.WriteLine("?:snapshot");

                    Assert.True(WaitFor(() =>
                    {
                        lock (lines) return lines.Any(l => l.StartsWith("R:"));
                    }), "expected R: reply within 2s");

                    string rLine;
                    lock (lines) rLine = lines.First(l => l.StartsWith("R:"));
                    using var doc = JsonDocument.Parse(rLine.Substring(2));
                    Assert.Equal(cache.WrapperId, doc.RootElement.GetProperty("wrapper_id").GetString());
                    Assert.Equal(1L, doc.RootElement.GetProperty("hits").GetInt64());
                    Assert.Equal(1L, doc.RootElement.GetProperty("current_size_entries").GetInt64());
                }
                finally
                {
                    stop.Set();
                    try { conn.Close(); } catch { }
                }
            }
            finally
            {
                cache.StopInvalidation();
                server.Stop();
            }
        }
    }

    // ── GUC-RLS cache safety (Option Y, wrapper-side native cache) ────
    //
    // Mirrors `goldlapel/src/guc_state.rs` (commit `3e02359`). Tests cover:
    //   * IsUnsafeGuc classification (short list + namespaced + case)
    //   * ParseSetCommand shapes (= / TO / SESSION / LOCAL / glued / quoted)
    //   * SplitStatements (string-literal-aware, doubled-quote escape)
    //   * ConnectionGucState invariants (insertion-order, RESET round-trip,
    //     SET LOCAL no-op, safe-GUC no-op, multi-statement)
    //   * Cache-key isolation by state hash (the actual leak the layer fixes)

    public class IsUnsafeGucTest
    {
        [Fact] public void SearchPathIsUnsafe() => Assert.True(NativeCache.IsUnsafeGuc("search_path"));
        [Fact] public void RoleIsUnsafe() => Assert.True(NativeCache.IsUnsafeGuc("role"));
        [Fact] public void SessionAuthorizationIsUnsafe() => Assert.True(NativeCache.IsUnsafeGuc("session_authorization"));
        [Fact] public void DefaultTxnIsolationIsUnsafe() => Assert.True(NativeCache.IsUnsafeGuc("default_transaction_isolation"));
        [Fact] public void DefaultTxnReadOnlyIsUnsafe() => Assert.True(NativeCache.IsUnsafeGuc("default_transaction_read_only"));
        [Fact] public void TransactionIsolationIsUnsafe() => Assert.True(NativeCache.IsUnsafeGuc("transaction_isolation"));
        [Fact] public void RowSecurityIsUnsafe() => Assert.True(NativeCache.IsUnsafeGuc("row_security"));

        [Fact] public void ClassificationCaseInsensitive()
        {
            Assert.True(NativeCache.IsUnsafeGuc("ROLE"));
            Assert.True(NativeCache.IsUnsafeGuc("Search_Path"));
            Assert.True(NativeCache.IsUnsafeGuc("SEARCH_PATH"));
        }

        [Fact] public void NamespacedGucsAreUnsafe()
        {
            Assert.True(NativeCache.IsUnsafeGuc("app.user_id"));
            Assert.True(NativeCache.IsUnsafeGuc("myapp.tenant"));
            Assert.True(NativeCache.IsUnsafeGuc("rls.account"));
            // Even unknown / arbitrarily nested namespaces.
            Assert.True(NativeCache.IsUnsafeGuc("a.b.c"));
            Assert.True(NativeCache.IsUnsafeGuc("APP.USER"));
        }

        [Fact] public void HarmlessGucsAreSafe()
        {
            Assert.False(NativeCache.IsUnsafeGuc("application_name"));
            Assert.False(NativeCache.IsUnsafeGuc("statement_timeout"));
            Assert.False(NativeCache.IsUnsafeGuc("work_mem"));
            Assert.False(NativeCache.IsUnsafeGuc("client_encoding"));
            Assert.False(NativeCache.IsUnsafeGuc("default_statistics_target"));
        }

        // Output-formatting and locale GUCs are unsafe — they alter the
        // textual representation of returned columns, so two connections
        // with different settings receive different bytes for the same
        // SELECT. Mirrors the dispatch spec for guc-rls-cache-safety.md.
        [Fact] public void FormattingGucsAreUnsafe()
        {
            Assert.True(NativeCache.IsUnsafeGuc("DateStyle"));
            Assert.True(NativeCache.IsUnsafeGuc("datestyle"));
            Assert.True(NativeCache.IsUnsafeGuc("IntervalStyle"));
            Assert.True(NativeCache.IsUnsafeGuc("intervalstyle"));
            Assert.True(NativeCache.IsUnsafeGuc("TimeZone"));
            Assert.True(NativeCache.IsUnsafeGuc("timezone"));
            Assert.True(NativeCache.IsUnsafeGuc("bytea_output"));
            Assert.True(NativeCache.IsUnsafeGuc("BYTEA_OUTPUT"));
        }

        [Fact] public void LocaleGucsAreUnsafe()
        {
            Assert.True(NativeCache.IsUnsafeGuc("lc_messages"));
            Assert.True(NativeCache.IsUnsafeGuc("lc_monetary"));
            Assert.True(NativeCache.IsUnsafeGuc("lc_numeric"));
            Assert.True(NativeCache.IsUnsafeGuc("lc_time"));
            Assert.True(NativeCache.IsUnsafeGuc("LC_MONETARY"));
        }

        [Fact] public void EmptyAndNullAreSafe()
        {
            Assert.False(NativeCache.IsUnsafeGuc(""));
            Assert.False(NativeCache.IsUnsafeGuc(null));
        }
    }

    public class ParseSetCommandTest
    {
        [Fact] public void ParseSetEqQuoted()
        {
            var cmd = NativeCache.ParseSetCommand("SET foo = 'bar'");
            Assert.Equal(SetCommand.CommandKind.Set, cmd.Kind);
            Assert.Equal("foo", cmd.Name);
            Assert.Equal("bar", cmd.Value);
        }

        [Fact] public void ParseSetToQuoted()
        {
            var cmd = NativeCache.ParseSetCommand("SET foo TO 'bar'");
            Assert.Equal(SetCommand.CommandKind.Set, cmd.Kind);
            Assert.Equal("foo", cmd.Name);
            Assert.Equal("bar", cmd.Value);
        }

        [Fact] public void ParseSetUnquoted()
        {
            var cmd = NativeCache.ParseSetCommand("SET foo = 42");
            Assert.Equal("foo", cmd.Name);
            Assert.Equal("42", cmd.Value);
        }

        [Fact] public void ParseSetSessionModifier()
        {
            var cmd = NativeCache.ParseSetCommand("SET SESSION foo = 'bar'");
            Assert.Equal(SetCommand.CommandKind.Set, cmd.Kind);
            Assert.Equal("foo", cmd.Name);
            Assert.Equal("bar", cmd.Value);
        }

        [Fact] public void ParseSetLocalModifier()
        {
            var cmd = NativeCache.ParseSetCommand("SET LOCAL foo = 'bar'");
            Assert.Equal(SetCommand.CommandKind.SetLocal, cmd.Kind);
            Assert.Equal("foo", cmd.Name);
            Assert.Equal("bar", cmd.Value);
        }

        [Fact] public void ParseResetNamed()
        {
            var cmd = NativeCache.ParseSetCommand("RESET foo");
            Assert.Equal(SetCommand.CommandKind.Reset, cmd.Kind);
            Assert.Equal("foo", cmd.Name);
            Assert.Null(cmd.Value);
        }

        [Fact] public void ParseResetAll()
        {
            var cmd = NativeCache.ParseSetCommand("RESET ALL");
            Assert.Equal(SetCommand.CommandKind.ResetAll, cmd.Kind);
            Assert.Null(cmd.Name);
        }

        [Fact] public void ParseCaseInsensitiveKeywords()
        {
            Assert.Equal(SetCommand.CommandKind.Set, NativeCache.ParseSetCommand("set foo = 'bar'").Kind);
            Assert.Equal(SetCommand.CommandKind.SetLocal, NativeCache.ParseSetCommand("Set Local foo To 'bar'").Kind);
            Assert.Equal(SetCommand.CommandKind.ResetAll, NativeCache.ParseSetCommand("reset all").Kind);
        }

        [Fact] public void ParseLowercasesGucName()
        {
            var cmd = NativeCache.ParseSetCommand("SET App.User_ID = '42'");
            Assert.Equal("app.user_id", cmd.Name);
            Assert.Equal("42", cmd.Value);
        }

        [Fact] public void ParseTrailingSemicolon()
        {
            Assert.Equal("foo", NativeCache.ParseSetCommand("SET foo = 'bar';").Name);
            Assert.Equal("foo", NativeCache.ParseSetCommand("RESET foo ;").Name);
        }

        [Fact] public void ParseExtraWhitespace()
        {
            var cmd = NativeCache.ParseSetCommand("   SET    foo   =   'bar'   ");
            Assert.Equal("foo", cmd.Name);
            Assert.Equal("bar", cmd.Value);
        }

        [Fact] public void ParseGluedEquals()
        {
            // Some clients send `SET name=value` with no spaces.
            var cmd = NativeCache.ParseSetCommand("SET app.user_id='42'");
            Assert.Equal("app.user_id", cmd.Name);
            Assert.Equal("42", cmd.Value);
        }

        [Fact] public void ParseDoubleQuotedValue()
        {
            var cmd = NativeCache.ParseSetCommand("SET foo = \"bar\"");
            Assert.Equal("bar", cmd.Value);
        }

        [Fact] public void ParseDoubleQuotedName()
        {
            // `"app.user_id"` is a quoted identifier — same value as bare.
            var cmd = NativeCache.ParseSetCommand("SET \"app.user_id\" = '42'");
            Assert.Equal("app.user_id", cmd.Name);
        }

        [Fact] public void ParseRejectsNonSetStatements()
        {
            Assert.Null(NativeCache.ParseSetCommand("SELECT 1"));
            Assert.Null(NativeCache.ParseSetCommand("BEGIN"));
            Assert.Null(NativeCache.ParseSetCommand("UPDATE t SET x = 1"));
        }

        [Fact] public void ParseRejectsEmpty()
        {
            Assert.Null(NativeCache.ParseSetCommand(""));
            Assert.Null(NativeCache.ParseSetCommand("   "));
            Assert.Null(NativeCache.ParseSetCommand(";"));
        }

        [Fact] public void ParseRejectsSetWithoutValue()
        {
            Assert.Null(NativeCache.ParseSetCommand("SET foo ="));
            Assert.Null(NativeCache.ParseSetCommand("SET foo TO"));
            Assert.Null(NativeCache.ParseSetCommand("SET foo"));
        }

        [Fact] public void ParseRejectsResetWithGarbage()
        {
            // `RESET foo bar` — second token after RESET is unexpected.
            Assert.Null(NativeCache.ParseSetCommand("RESET foo bar"));
        }

        [Fact] public void ParseRejectsSetTimeZoneTwoWordForm()
        {
            // `SET TIME ZONE 'UTC'` — legacy two-word form. We don't
            // model it. Returning null means the parser doesn't recognise
            // this shape — the wrapper falls back to the post-call
            // verify path (Concern 6) when this form is used. Modern
            // form `SET TimeZone = 'UTC'` IS recognised and mutates
            // state — TimeZone is in the unsafe list (formatting GUC).
            Assert.Null(NativeCache.ParseSetCommand("SET TIME ZONE 'UTC'"));
        }

        // ── DISCARD ─────────────────────────────────────────────────
        // PG's DISCARD command. ALL clears all session state including
        // SETs, so it must wipe the unsafe-GUC hash. Other variants
        // (PLANS / SEQUENCES / TEMP / TEMPORARY) don't affect GUCs.

        [Fact] public void ParseDiscardAll()
        {
            var cmd = NativeCache.ParseSetCommand("DISCARD ALL");
            Assert.NotNull(cmd);
            Assert.Equal(SetCommand.CommandKind.DiscardAll, cmd.Kind);
            Assert.Null(cmd.Name);
        }

        [Fact] public void ParseDiscardAllCaseInsensitive()
        {
            Assert.Equal(SetCommand.CommandKind.DiscardAll,
                NativeCache.ParseSetCommand("discard all").Kind);
            Assert.Equal(SetCommand.CommandKind.DiscardAll,
                NativeCache.ParseSetCommand("Discard All").Kind);
        }

        [Fact] public void ParseDiscardAllTrailingSemicolon()
        {
            Assert.Equal(SetCommand.CommandKind.DiscardAll,
                NativeCache.ParseSetCommand("DISCARD ALL;").Kind);
        }

        [Fact] public void ParseDiscardPlansIsNoop()
            => Assert.Null(NativeCache.ParseSetCommand("DISCARD PLANS"));

        [Fact] public void ParseDiscardSequencesIsNoop()
            => Assert.Null(NativeCache.ParseSetCommand("DISCARD SEQUENCES"));

        [Fact] public void ParseDiscardTempIsNoop()
            => Assert.Null(NativeCache.ParseSetCommand("DISCARD TEMP"));

        [Fact] public void ParseDiscardTemporaryIsNoop()
            => Assert.Null(NativeCache.ParseSetCommand("DISCARD TEMPORARY"));

        [Fact] public void ParseDiscardWithJunkRejected()
        {
            // `DISCARD` alone isn't valid; `DISCARD ALL extra` either.
            Assert.Null(NativeCache.ParseSetCommand("DISCARD"));
            Assert.Null(NativeCache.ParseSetCommand("DISCARD ALL extra"));
        }

        // ── SELECT set_config(...) ───────────────────────────────────
        // Supabase's canonical JWT pattern. is_local=false → state mutates;
        // is_local=true → SET LOCAL semantics (no-op for our hash).

        [Fact] public void ParseSetConfigBasic()
        {
            var cmd = NativeCache.ParseSetCommand("SELECT set_config('app.user_id', '42', false)");
            Assert.NotNull(cmd);
            Assert.Equal(SetCommand.CommandKind.Set, cmd.Kind);
            Assert.Equal("app.user_id", cmd.Name);
            Assert.Equal("42", cmd.Value);
        }

        [Fact] public void ParseSetConfigLocalTrue()
        {
            var cmd = NativeCache.ParseSetCommand("SELECT set_config('app.user_id', '42', true)");
            Assert.NotNull(cmd);
            Assert.Equal(SetCommand.CommandKind.SetLocal, cmd.Kind);
            Assert.Equal("app.user_id", cmd.Name);
            Assert.Equal("42", cmd.Value);
        }

        [Fact] public void ParseSetConfigPgCatalogPrefix()
        {
            var cmd = NativeCache.ParseSetCommand("SELECT pg_catalog.set_config('app.user_id', '42', false)");
            Assert.NotNull(cmd);
            Assert.Equal(SetCommand.CommandKind.Set, cmd.Kind);
            Assert.Equal("app.user_id", cmd.Name);
        }

        [Fact] public void ParseSetConfigCaseInsensitive()
        {
            var cmd = NativeCache.ParseSetCommand("select Set_Config('role', 'app_user', FALSE)");
            Assert.NotNull(cmd);
            Assert.Equal("role", cmd.Name);
        }

        [Fact] public void ParseSetConfigSingleQuotedTrueFalse()
        {
            // Some apps use `'t'`/`'f'` strings.
            var cmd1 = NativeCache.ParseSetCommand("SELECT set_config('app.id', '1', 't')");
            Assert.Equal(SetCommand.CommandKind.SetLocal, cmd1.Kind);
            var cmd2 = NativeCache.ParseSetCommand("SELECT set_config('app.id', '1', 'f')");
            Assert.Equal(SetCommand.CommandKind.Set, cmd2.Kind);
        }

        [Fact] public void ParseSetConfigBoolCast()
        {
            var cmd = NativeCache.ParseSetCommand("SELECT set_config('app.id', '42', false::bool)");
            Assert.NotNull(cmd);
            Assert.Equal(SetCommand.CommandKind.Set, cmd.Kind);
            Assert.Equal("app.id", cmd.Name);
        }

        [Fact] public void ParseSetConfigDoubledQuoteEscape()
        {
            // The value contains an escaped single quote `''` (PG syntax
            // for a literal `'`). The parser must collapse `''` → `'`.
            var cmd = NativeCache.ParseSetCommand("SELECT set_config('app.note', 'it''s ok', false)");
            Assert.NotNull(cmd);
            Assert.Equal("it's ok", cmd.Value);
        }

        [Fact] public void ParseSetConfigSafeNameNotTracked()
        {
            // set_config on a safe GUC parses but flagging it Set is fine —
            // ConnectionGucState.Apply gates on IsUnsafeGuc and ignores it.
            var cmd = NativeCache.ParseSetCommand("SELECT set_config('application_name', 'foo', false)");
            Assert.NotNull(cmd);
            Assert.Equal("application_name", cmd.Name);
        }

        [Fact] public void ParseSetConfigRejectsNonLiteralArgs()
        {
            // Cant statically resolve a parameter placeholder — return
            // null so the caller falls back to post-call verify.
            Assert.Null(NativeCache.ParseSetCommand("SELECT set_config($1, $2, $3)"));
            // Function call as second arg — too dynamic for static parse.
            Assert.Null(NativeCache.ParseSetCommand("SELECT set_config('app.id', current_user, false)"));
        }

        [Fact] public void ParseSetConfigRejectsExtraExpressions()
        {
            // `SELECT set_config(...) || 'x'` — there's stuff after the
            // closing `)`, so this is part of a larger expression. We
            // don't track those.
            Assert.Null(NativeCache.ParseSetCommand("SELECT set_config('app.id', '1', false) || 'x'"));
            // `SELECT 1, set_config(...)` — junk between SELECT and set_config.
            Assert.Null(NativeCache.ParseSetCommand("SELECT 1, set_config('app.id', '1', false)"));
        }

        [Fact] public void ParseSetConfigRejectsMalformed()
        {
            // Unclosed paren.
            Assert.Null(NativeCache.ParseSetCommand("SELECT set_config('app.id', '1', false"));
            // Wrong arg count.
            Assert.Null(NativeCache.ParseSetCommand("SELECT set_config('app.id', '1')"));
            Assert.Null(NativeCache.ParseSetCommand("SELECT set_config('app.id')"));
            // Non-bool third arg.
            Assert.Null(NativeCache.ParseSetCommand("SELECT set_config('app.id', '1', 42)"));
        }

        [Fact] public void ParseSetConfigTrailingSemicolon()
        {
            var cmd = NativeCache.ParseSetCommand("SELECT set_config('app.id', '1', false);");
            Assert.NotNull(cmd);
            Assert.Equal("app.id", cmd.Name);
        }

        [Fact] public void ParseSetConfigNullValueResetsUnsafe()
        {
            // PG's set_config treats NULL value as a reset; we model it
            // as a Reset for unsafe names (so the value drops from the
            // hash map) and ignore for safe names.
            var cmd = NativeCache.ParseSetCommand("SELECT set_config('app.id', NULL, false)");
            Assert.NotNull(cmd);
            Assert.Equal(SetCommand.CommandKind.Reset, cmd.Kind);
            Assert.Equal("app.id", cmd.Name);
        }
    }

    public class SplitStatementsTest
    {
        [Fact] public void SimpleTwoStatements()
        {
            var v = NativeCache.SplitStatements("SET foo = '42'; SELECT 1");
            Assert.Equal(new[] { "SET foo = '42'", "SELECT 1" }, v);
        }

        [Fact] public void DropsEmptySegments()
        {
            var v = NativeCache.SplitStatements("; SET foo = '42';;SELECT 1;");
            Assert.Equal(new[] { "SET foo = '42'", "SELECT 1" }, v);
        }

        [Fact] public void RespectsSingleQuotes()
        {
            // The `;` inside the literal must NOT split the statement.
            var v = NativeCache.SplitStatements("SET foo = 'a;b'; SELECT 1");
            Assert.Equal(new[] { "SET foo = 'a;b'", "SELECT 1" }, v);
        }

        [Fact] public void RespectsDoubleQuotes()
        {
            var v = NativeCache.SplitStatements("SET \"app;guc\" = 'x'; SELECT 1");
            Assert.Equal(new[] { "SET \"app;guc\" = 'x'", "SELECT 1" }, v);
        }

        [Fact] public void HandlesDoubledQuoteEscape()
        {
            // PG escapes a literal `'` inside a string by doubling: `''`.
            var v = NativeCache.SplitStatements("SET foo = 'it''s; ok'; SELECT 1");
            Assert.Equal(new[] { "SET foo = 'it''s; ok'", "SELECT 1" }, v);
        }

        [Fact] public void SingleStatementPassThrough()
        {
            var v = NativeCache.SplitStatements("SET foo = '42'");
            Assert.Equal(new[] { "SET foo = '42'" }, v);
        }

        [Fact] public void EmptyInput()
        {
            Assert.Empty(NativeCache.SplitStatements(""));
            Assert.Empty(NativeCache.SplitStatements("   "));
            Assert.Empty(NativeCache.SplitStatements(";;;"));
        }
    }

    // ── DetectWritesMulti ────────────────────────────────────
    //
    // Multi-statement Q-message write detection. The single-statement
    // DetectWrite() looks at the first token only, so a body like
    // `SET foo = '42'; INSERT INTO orders ...` would slip past write
    // detection and let stale `orders` cache entries survive the
    // INSERT. DetectWritesMulti unions across segments.

    public class DetectWritesMultiTest
    {
        [Fact] public void SingleSelectReturnsEmpty()
            => Assert.Empty(NativeCache.DetectWritesMulti("SELECT * FROM orders"));

        [Fact] public void SingleInsertReturnsTable()
        {
            var w = NativeCache.DetectWritesMulti("INSERT INTO orders VALUES (1)");
            Assert.Single(w);
            Assert.Contains("orders", w);
        }

        [Fact] public void SetThenInsertCatchesInsert()
        {
            var w = NativeCache.DetectWritesMulti("SET app.user_id = '42'; INSERT INTO orders VALUES (1)");
            Assert.Single(w);
            Assert.Contains("orders", w);
        }

        [Fact] public void TwoInsertsUnionsTables()
        {
            var w = NativeCache.DetectWritesMulti("INSERT INTO orders VALUES (1); INSERT INTO line_items VALUES (1)");
            Assert.Equal(2, w.Count);
            Assert.Contains("orders", w);
            Assert.Contains("line_items", w);
        }

        [Fact] public void DdlAnywhereShortCircuits()
        {
            var w = NativeCache.DetectWritesMulti("INSERT INTO orders VALUES (1); CREATE TABLE foo (id int)");
            Assert.Single(w);
            Assert.Contains(NativeCache.DdlSentinel, w);
        }

        [Fact] public void DdlFirstShortCircuits()
        {
            var w = NativeCache.DetectWritesMulti("DROP TABLE foo; INSERT INTO orders VALUES (1)");
            Assert.Single(w);
            Assert.Contains(NativeCache.DdlSentinel, w);
        }

        [Fact] public void TxBracketedInsertStillDetected()
        {
            var w = NativeCache.DetectWritesMulti("BEGIN; INSERT INTO orders VALUES (1); COMMIT");
            Assert.Single(w);
            Assert.Contains("orders", w);
        }

        [Fact] public void AllReadsReturnsEmpty()
        {
            var w = NativeCache.DetectWritesMulti("SELECT 1; SELECT * FROM orders; SELECT 2");
            Assert.Empty(w);
        }

        [Fact] public void EmptyInputReturnsEmpty()
        {
            Assert.Empty(NativeCache.DetectWritesMulti(""));
            Assert.Empty(NativeCache.DetectWritesMulti("   "));
        }

        [Fact] public void SemicolonInsideStringLiteralNotSplit()
        {
            // `INSERT INTO orders VALUES ('a;b')` is a single statement;
            // the `;` inside the literal must not be treated as a split.
            var w = NativeCache.DetectWritesMulti("INSERT INTO orders VALUES ('a;b')");
            Assert.Single(w);
            Assert.Contains("orders", w);
        }

        [Fact] public void NullInputReturnsEmpty()
            => Assert.Empty(NativeCache.DetectWritesMulti(null));
    }

    // ── IsSessionStateCommand ────────────────────────────────
    //
    // Cross-wrapper fix for the "wrappers cache SET responses
    // pointlessly" gap (docs/todos/wrapper-cache-set-responses.md).
    // Used by CachedConnection.CacheAndReturn to skip the cache-put on
    // session-state commands whose responses are empty rowsets.

    public class IsSessionStateCommandTest
    {
        [Fact] public void Set() => Assert.True(NativeCache.IsSessionStateCommand("SET app.user_id = '42'"));
        [Fact] public void Reset() => Assert.True(NativeCache.IsSessionStateCommand("RESET app.user_id"));
        [Fact] public void Listen() => Assert.True(NativeCache.IsSessionStateCommand("LISTEN channel_x"));
        [Fact] public void Unlisten() => Assert.True(NativeCache.IsSessionStateCommand("UNLISTEN channel_x"));
        [Fact] public void Notify() => Assert.True(NativeCache.IsSessionStateCommand("NOTIFY channel_x, 'payload'"));
        [Fact] public void Begin() => Assert.True(NativeCache.IsSessionStateCommand("BEGIN"));
        [Fact] public void Commit() => Assert.True(NativeCache.IsSessionStateCommand("COMMIT"));
        [Fact] public void Rollback() => Assert.True(NativeCache.IsSessionStateCommand("ROLLBACK"));
        [Fact] public void Savepoint() => Assert.True(NativeCache.IsSessionStateCommand("SAVEPOINT sp1"));
        // After the multi-segment tx-flag fix removed the early-return
        // short-circuit, START/END/RELEASE also flow through the cache
        // path and must be filtered to keep their empty rowsets out.
        [Fact] public void StartTransaction() => Assert.True(NativeCache.IsSessionStateCommand("START TRANSACTION"));
        [Fact] public void End() => Assert.True(NativeCache.IsSessionStateCommand("END"));
        [Fact] public void Release() => Assert.True(NativeCache.IsSessionStateCommand("RELEASE SAVEPOINT sp1"));
        [Fact] public void CaseInsensitive() => Assert.True(NativeCache.IsSessionStateCommand("set app.user_id = '42'"));
        [Fact] public void LeadingWhitespace() => Assert.True(NativeCache.IsSessionStateCommand("   SET foo = 'bar'"));
        [Fact] public void Select() => Assert.False(NativeCache.IsSessionStateCommand("SELECT * FROM orders"));
        [Fact] public void Insert() => Assert.False(NativeCache.IsSessionStateCommand("INSERT INTO orders VALUES (1)"));
        [Fact] public void Empty() => Assert.False(NativeCache.IsSessionStateCommand(""));
        [Fact] public void Whitespace() => Assert.False(NativeCache.IsSessionStateCommand("   "));
        [Fact] public void Null() => Assert.False(NativeCache.IsSessionStateCommand(null));
        // Substrings of session-state commands must not match — only
        // exact first tokens. e.g. `SETTLE` or `RESETTING` are not SET /
        // RESET. We bound the first-token at whitespace OR `;`.
        [Fact] public void NotPrefixMatch() => Assert.False(NativeCache.IsSessionStateCommand("SETTLE foo"));
        [Fact] public void TerminatedBySemicolon() => Assert.True(NativeCache.IsSessionStateCommand("SET;"));
    }

    public class ConnectionGucStateTest
    {
        [Fact] public void EmptyStateHashIsZero()
        {
            var s = new ConnectionGucState();
            Assert.Equal(0L, s.StateHash);
        }

        [Fact] public void SafeSetDoesNotChangeHash()
        {
            var s = new ConnectionGucState();
            s.ObserveSql("SET application_name = 'foo'");
            Assert.Equal(0L, s.StateHash);
            s.ObserveSql("SET statement_timeout = 5000");
            Assert.Equal(0L, s.StateHash);
            s.ObserveSql("SET work_mem = '64MB'");
            Assert.Equal(0L, s.StateHash);
        }

        [Fact] public void UnsafeSetChangesHash()
        {
            var s = new ConnectionGucState();
            var h0 = s.StateHash;
            s.ObserveSql("SET app.user_id = '42'");
            var h1 = s.StateHash;
            Assert.NotEqual(h0, h1);
        }

        [Fact] public void SameUnsafeSetYieldsSameHash()
        {
            var a = new ConnectionGucState();
            var b = new ConnectionGucState();
            a.ObserveSql("SET app.user_id = '42'");
            b.ObserveSql("SET app.user_id = '42'");
            Assert.Equal(a.StateHash, b.StateHash);
        }

        [Fact] public void DifferentValuesYieldDifferentHashes()
        {
            var a = new ConnectionGucState();
            var b = new ConnectionGucState();
            a.ObserveSql("SET app.user_id = '42'");
            b.ObserveSql("SET app.user_id = '43'");
            Assert.NotEqual(a.StateHash, b.StateHash);
        }

        [Fact] public void InsertionOrderDoesNotMatter()
        {
            var a = new ConnectionGucState();
            a.ObserveSql("SET app.user_id = '42'");
            a.ObserveSql("SET app.tenant = 'alpha'");

            var b = new ConnectionGucState();
            b.ObserveSql("SET app.tenant = 'alpha'");
            b.ObserveSql("SET app.user_id = '42'");

            Assert.Equal(a.StateHash, b.StateHash);
        }

        [Fact] public void ResetReturnsHashToBaseline()
        {
            var s = new ConnectionGucState();
            var baseline = s.StateHash;
            s.ObserveSql("SET app.user_id = '42'");
            Assert.NotEqual(baseline, s.StateHash);
            s.ObserveSql("RESET app.user_id");
            Assert.Equal(baseline, s.StateHash);
        }

        [Fact] public void ResetAllClearsAllUnsafeState()
        {
            var s = new ConnectionGucState();
            s.ObserveSql("SET app.user_id = '42'");
            s.ObserveSql("SET search_path TO 'tenant_a'");
            s.ObserveSql("SET role = 'app_user'");
            Assert.NotEqual(0L, s.StateHash);
            s.ObserveSql("RESET ALL");
            Assert.Equal(0L, s.StateHash);
        }

        [Fact] public void SetLocalDoesNotChangeHash()
        {
            // Even an unsafe-named SET LOCAL must not move the hash —
            // SET LOCAL only takes effect inside a txn, and the cache
            // bypasses transactions anyway.
            var s = new ConnectionGucState();
            s.ObserveSql("SET LOCAL app.user_id = '42'");
            Assert.Equal(0L, s.StateHash);
        }

        [Fact] public void ObserveSqlReturnsChangeFlag()
        {
            var s = new ConnectionGucState();
            Assert.True(s.ObserveSql("SET app.user_id = '42'"));
            Assert.False(s.ObserveSql("SELECT 1"));
            Assert.False(s.ObserveSql("SET application_name = 'foo'"));
            Assert.True(s.ObserveSql("RESET app.user_id"));
        }

        [Fact] public void ResetSafeGucIsNoop()
        {
            var s = new ConnectionGucState();
            s.ObserveSql("SET app.user_id = '42'");
            var h = s.StateHash;
            s.ObserveSql("RESET application_name");
            Assert.Equal(h, s.StateHash);
        }

        [Fact] public void OverwriteUnsafeValueChangesHash()
        {
            var s = new ConnectionGucState();
            s.ObserveSql("SET app.user_id = '42'");
            var h1 = s.StateHash;
            s.ObserveSql("SET app.user_id = '43'");
            var h2 = s.StateHash;
            Assert.NotEqual(h1, h2);
        }

        [Fact] public void ObserveMultiStatementAppliesAllSets()
        {
            // Real-world pattern: client batches a SET with the query.
            var s = new ConnectionGucState();
            s.ObserveSql("SET app.user_id = '42'; SELECT * FROM accounts");
            Assert.NotEqual(0L, s.StateHash);
        }

        [Fact] public void MultiStatementMatchesSeparateStatements()
        {
            var a = new ConnectionGucState();
            a.ObserveSql("SET app.user_id = '42'");
            a.ObserveSql("SET app.tenant = 'alpha'");

            var b = new ConnectionGucState();
            b.ObserveSql("SET app.user_id = '42'; SET app.tenant = 'alpha'");

            Assert.Equal(a.StateHash, b.StateHash);
        }

        [Fact] public void ObserveMultiStatementWithQuotedSemicolon()
        {
            // The `;` inside the value must not be treated as a statement
            // separator.
            var s = new ConnectionGucState();
            s.ObserveSql("SET app.tenant = 'has;semicolon'; SELECT 1");
            Assert.NotEqual(0L, s.StateHash);
        }

        [Fact] public void NullSqlIsNoOp()
        {
            // Defensive: ObserveSql(null) must not throw or perturb state.
            var s = new ConnectionGucState();
            Assert.False(s.ObserveSql(null));
            Assert.False(s.ObserveSql(""));
            Assert.Equal(0L, s.StateHash);
        }

        [Fact] public void SetTextInsideStringLiteralIsNotApplied()
        {
            // A SELECT whose value column happens to contain literal SQL
            // text that looks like a SET command must NOT be parsed as a
            // SET — the head token is SELECT, so ParseSetCommand rejects.
            // Regression guard: lexer-naive shortcuts that prefix-match
            // "SET" anywhere in the body would leak.
            var s = new ConnectionGucState();
            Assert.False(s.ObserveSql("SELECT 'SET app.user_id = ''42'''"));
            Assert.Equal(0L, s.StateHash);
        }

        [Fact] public void SetEmbeddedInStringLiteralBetweenStatementsIgnored()
        {
            // The full SQL is a single SELECT whose argument contains the
            // bytes `; SET app.user_id = '42'`. The semicolon is inside a
            // quoted literal, so SplitStatements must NOT split it; the
            // single segment is then parsed as SELECT (head != SET) and
            // produces no state mutation.
            var s = new ConnectionGucState();
            Assert.False(s.ObserveSql("SELECT 'a; SET app.user_id = ''42'''"));
            Assert.Equal(0L, s.StateHash);
        }

        // ── DISCARD ALL clears state ────────────────────────────────

        [Fact] public void DiscardAllClearsAllUnsafeState()
        {
            var s = new ConnectionGucState();
            s.ObserveSql("SET app.user_id = '42'");
            s.ObserveSql("SET search_path TO 'tenant_a'");
            Assert.NotEqual(0L, s.StateHash);
            Assert.True(s.ObserveSql("DISCARD ALL"));
            Assert.Equal(0L, s.StateHash);
        }

        [Fact] public void DiscardAllOnEmptyStateIsNoChange()
        {
            var s = new ConnectionGucState();
            Assert.False(s.ObserveSql("DISCARD ALL"));
            Assert.Equal(0L, s.StateHash);
        }

        [Fact] public void DiscardPlansDoesNotClearGucState()
        {
            var s = new ConnectionGucState();
            s.ObserveSql("SET app.user_id = '42'");
            var h = s.StateHash;
            Assert.False(s.ObserveSql("DISCARD PLANS"));
            Assert.Equal(h, s.StateHash);
        }

        [Fact] public void DiscardSequencesDoesNotClearGucState()
        {
            var s = new ConnectionGucState();
            s.ObserveSql("SET app.user_id = '42'");
            var h = s.StateHash;
            Assert.False(s.ObserveSql("DISCARD SEQUENCES"));
            Assert.Equal(h, s.StateHash);
        }

        [Fact] public void DiscardTempDoesNotClearGucState()
        {
            var s = new ConnectionGucState();
            s.ObserveSql("SET app.user_id = '42'");
            var h = s.StateHash;
            Assert.False(s.ObserveSql("DISCARD TEMP"));
            Assert.False(s.ObserveSql("DISCARD TEMPORARY"));
            Assert.Equal(h, s.StateHash);
        }

        [Fact] public void DiscardAllInMultiStatementBody()
        {
            var s = new ConnectionGucState();
            s.ObserveSql("SET app.user_id = '42'");
            Assert.NotEqual(0L, s.StateHash);
            // Pool returns sometimes batch DISCARD ALL with subsequent SQL.
            Assert.True(s.ObserveSql("DISCARD ALL; SELECT 1"));
            Assert.Equal(0L, s.StateHash);
        }

        // ── set_config function form ────────────────────────────────

        [Fact] public void SetConfigUnsafeMutatesHash()
        {
            var s = new ConnectionGucState();
            Assert.True(s.ObserveSql("SELECT set_config('app.user_id', '42', false)"));
            Assert.NotEqual(0L, s.StateHash);
        }

        [Fact] public void SetConfigEqualsRegularSet()
        {
            // The result of `SELECT set_config('x', 'y', false)` and
            // `SET x = 'y'` must produce the same state hash.
            var a = new ConnectionGucState();
            a.ObserveSql("SET app.user_id = '42'");

            var b = new ConnectionGucState();
            b.ObserveSql("SELECT set_config('app.user_id', '42', false)");

            Assert.Equal(a.StateHash, b.StateHash);
        }

        [Fact] public void SetConfigPgCatalogPrefixMutatesHash()
        {
            var s = new ConnectionGucState();
            s.ObserveSql("SELECT pg_catalog.set_config('app.user_id', '42', false)");
            Assert.NotEqual(0L, s.StateHash);
        }

        [Fact] public void SetConfigLocalIsNoop()
        {
            // is_local=true → SET LOCAL semantics, no hash change.
            var s = new ConnectionGucState();
            Assert.False(s.ObserveSql("SELECT set_config('app.user_id', '42', true)"));
            Assert.Equal(0L, s.StateHash);
        }

        [Fact] public void SetConfigSafeNameDoesNotChangeHash()
        {
            var s = new ConnectionGucState();
            Assert.False(s.ObserveSql("SELECT set_config('application_name', 'foo', false)"));
            Assert.Equal(0L, s.StateHash);
        }

        [Fact] public void SetConfigInMultiStatementBody()
        {
            var s = new ConnectionGucState();
            // Supabase's exact pattern: set_config + the actual query.
            Assert.True(s.ObserveSql(
                "SELECT set_config('app.jwt.user_id', 'alice', false); " +
                "SELECT * FROM accounts"));
            Assert.NotEqual(0L, s.StateHash);
        }

        [Fact] public void SetConfigNullResetsHash()
        {
            // PG's set_config with NULL value resets the parameter.
            var s = new ConnectionGucState();
            s.ObserveSql("SET app.user_id = '42'");
            var h = s.StateHash;
            Assert.True(s.ObserveSql("SELECT set_config('app.user_id', NULL, false)"));
            Assert.NotEqual(h, s.StateHash);
            Assert.Equal(0L, s.StateHash);
        }

        // ── Formatting / locale GUCs are unsafe ────────────────────

        [Fact] public void DateStyleSetMutatesHash()
        {
            var s = new ConnectionGucState();
            Assert.True(s.ObserveSql("SET DateStyle = 'ISO, MDY'"));
            Assert.NotEqual(0L, s.StateHash);
        }

        [Fact] public void TimeZoneSetMutatesHash()
        {
            var s = new ConnectionGucState();
            Assert.True(s.ObserveSql("SET TimeZone = 'UTC'"));
            Assert.NotEqual(0L, s.StateHash);
        }

        [Fact] public void DifferentDateStylesIsolateState()
        {
            var a = new ConnectionGucState();
            var b = new ConnectionGucState();
            a.ObserveSql("SET DateStyle = 'ISO, MDY'");
            b.ObserveSql("SET DateStyle = 'German, DMY'");
            Assert.NotEqual(a.StateHash, b.StateHash);
        }

        [Fact] public void LcMonetarySetMutatesHash()
        {
            var s = new ConnectionGucState();
            Assert.True(s.ObserveSql("SET lc_monetary = 'en_US.UTF-8'"));
            Assert.NotEqual(0L, s.StateHash);
        }

        // ── ApplyVerifiedState (verify-on-checkout fallback) ───────

        [Fact] public void ApplyVerifiedStateReplacesMap()
        {
            var s = new ConnectionGucState();
            s.ObserveSql("SET app.user_id = '42'");
            Assert.NotEqual(0L, s.StateHash);

            // Verify pulled fresh state showing only `app.tenant`. The
            // user_id should disappear (server-side state is authoritative).
            var verified = new Dictionary<string, string>
            {
                { "app.tenant", "alpha" }
            };
            s.ApplyVerifiedState(verified);

            // Verify hash matches a fresh state with only app.tenant set.
            var b = new ConnectionGucState();
            b.ObserveSql("SET app.tenant = 'alpha'");
            Assert.Equal(b.StateHash, s.StateHash);
        }

        [Fact] public void ApplyVerifiedStateFiltersSafeGucs()
        {
            // Verify can return all session-source GUCs; we drop the
            // safe ones. Result hash should match a state with only the
            // unsafe ones applied.
            var s = new ConnectionGucState();
            var verified = new Dictionary<string, string>
            {
                { "application_name", "myapp" },
                { "statement_timeout", "5000" },
                { "app.user_id", "42" },
                { "search_path", "public" },
            };
            s.ApplyVerifiedState(verified);

            var b = new ConnectionGucState();
            b.ObserveSql("SET app.user_id = '42'");
            b.ObserveSql("SET search_path = 'public'");
            Assert.Equal(b.StateHash, s.StateHash);
        }

        [Fact] public void ApplyVerifiedStateNullClearsMap()
        {
            var s = new ConnectionGucState();
            s.ObserveSql("SET app.user_id = '42'");
            s.ApplyVerifiedState(null);
            Assert.Equal(0L, s.StateHash);
        }

        [Fact] public void ApplyVerifiedStateEmptyClearsMap()
        {
            var s = new ConnectionGucState();
            s.ObserveSql("SET app.user_id = '42'");
            s.ApplyVerifiedState(new Dictionary<string, string>());
            Assert.Equal(0L, s.StateHash);
        }

        // ── Dirty flag for verify-on-checkout fallback ─────────────

        [Fact] public void DirtyFlagDefaultsFalse()
        {
            var s = new ConnectionGucState();
            Assert.False(s.IsDirty);
        }

        [Fact] public void MarkDirtySetsFlag()
        {
            var s = new ConnectionGucState();
            s.MarkDirty();
            Assert.True(s.IsDirty);
        }

        [Fact] public void ClearDirtyResetsFlag()
        {
            var s = new ConnectionGucState();
            s.MarkDirty();
            s.ClearDirty();
            Assert.False(s.IsDirty);
        }

        [Fact] public void ApplyVerifiedStateClearsDirty()
        {
            // The whole point of verify-on-checkout: after a successful
            // verify, dirty must be cleared so we don't re-verify on the
            // next checkout.
            var s = new ConnectionGucState();
            s.MarkDirty();
            s.ApplyVerifiedState(new Dictionary<string, string>
            {
                { "app.user_id", "42" }
            });
            Assert.False(s.IsDirty);
        }
    }

    // ── Cache isolation by state hash ─────────────────────────────
    //
    // The actual security goal: same SQL + same params + DIFFERENT unsafe
    // GUCs must NOT share a cache slot. Closes the GUC-driven RLS leak at
    // the wrapper-side native cache (the proxy commit `3e02359` closed it
    // at the proxy-cache layer).

    public class StateHashCacheIsolationTest : IDisposable
    {
        public StateHashCacheIsolationTest() { NativeCache.Reset(); }
        public void Dispose() { NativeCache.Reset(); }

        private NativeCache MakeCache()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            return cache;
        }

        [Fact]
        public void DifferentStateHashesIsolateEntries()
        {
            // Same SQL — but two callers with different unsafe-GUC state
            // store and retrieve their own rows.
            var cache = MakeCache();
            var sql = "SELECT * FROM accounts";

            cache.Put(sql, null,
                new[] { new object[] { "alice-row" } }, new[] { "name" }, 0x111);
            cache.Put(sql, null,
                new[] { new object[] { "bob-row" } }, new[] { "name" }, 0x222);

            var aliceEntry = cache.Get(sql, null, 0x111);
            var bobEntry = cache.Get(sql, null, 0x222);
            Assert.NotNull(aliceEntry);
            Assert.NotNull(bobEntry);
            Assert.Equal("alice-row", aliceEntry.Rows[0][0]);
            Assert.Equal("bob-row", bobEntry.Rows[0][0]);
        }

        [Fact]
        public void SameStateHashShare()
        {
            // Sanity check the other direction: same SQL + same state
            // hash → cache hit (the layer's whole job).
            var cache = MakeCache();
            cache.Put("SELECT 1", null,
                new[] { new object[] { "x" } }, new[] { "v" }, 0x42);
            var entry = cache.Get("SELECT 1", null, 0x42);
            Assert.NotNull(entry);
        }

        [Fact]
        public void DefaultOverloadIsStateHashZero()
        {
            // Get/Put without an explicit state hash use 0 (baseline).
            // Asserts the back-compat overload routes to the same slot.
            var cache = MakeCache();
            cache.Put("SELECT 1", null,
                new[] { new object[] { "x" } }, new[] { "v" });
            var entry = cache.Get("SELECT 1", null, 0);
            Assert.NotNull(entry);
        }

        [Fact]
        public void DisabledNativeCacheStillTicksMissesWithStateHash()
        {
            // DisableNativeCache short-circuits before key building, so
            // any state hash is fine — miss counter still ticks.
            var cache = MakeCache();
            cache.DisableNativeCache = true;
            Assert.Null(cache.Get("SELECT 1", null, 0xdead));
            Assert.Equal(1L, Interlocked.Read(ref cache.StatsMisses));
        }
    }
}
