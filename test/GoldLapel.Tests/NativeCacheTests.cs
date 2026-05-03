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

    // ── Transaction detection ────────────────────────────────

    public class TxDetectionTest
    {
        [Fact] public void Begin() => Assert.True(NativeCache.IsTxStart("BEGIN"));
        [Fact] public void StartTransaction() => Assert.True(NativeCache.IsTxStart("START TRANSACTION"));
        [Fact] public void Commit() => Assert.True(NativeCache.IsTxEnd("COMMIT"));
        [Fact] public void Rollback() => Assert.True(NativeCache.IsTxEnd("ROLLBACK"));
        [Fact] public void End() => Assert.True(NativeCache.IsTxEnd("END"));
        [Fact] public void SavepointNotStart() => Assert.False(NativeCache.IsTxStart("SAVEPOINT x"));
        [Fact] public void SelectNotStart() => Assert.False(NativeCache.IsTxStart("SELECT 1"));
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

    [Collection("L1Telemetry")]
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

    public class MakeKeyTest
    {
        [Fact]
        public void NullParams()
        {
            var key = NativeCache.MakeKey("SELECT 1", null);
            Assert.Equal("SELECT 1\0null", key);
        }

        [Fact]
        public void EmptyParams()
        {
            var key = NativeCache.MakeKey("SELECT 1", new object[0]);
            Assert.Equal("SELECT 1\0null", key);
        }

        [Fact]
        public void WithParams()
        {
            var key = NativeCache.MakeKey("SELECT $1", new object[] { 42 });
            Assert.Equal("SELECT $1\042", key);
        }

        [Fact]
        public void MultipleParams()
        {
            var key = NativeCache.MakeKey("SELECT $1, $2", new object[] { "a", "b" });
            Assert.Equal("SELECT $1, $2\0a,b", key);
        }
    }

    // ── L1 telemetry: counters + snapshot ────────────────────
    //
    // The telemetry tests below read GOLDLAPEL_NATIVE_CACHE_SIZE and
    // GOLDLAPEL_REPORT_STATS during construction. Process-global env vars
    // race when test classes run in parallel, so we put them all in one
    // collection to serialize them. See xUnit collection-fixture docs.

    [CollectionDefinition("L1Telemetry", DisableParallelization = true)]
    public class L1TelemetryCollection { }

    [Collection("L1Telemetry")]
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

    [Collection("L1Telemetry")]
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

    // ── L1 telemetry: state-change emission via test hook ────

    [Collection("L1Telemetry")]
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

    [Collection("L1Telemetry")]
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

    // ── L1 telemetry: real-socket integration ────────────────

    [Collection("L1Telemetry")]
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
}
