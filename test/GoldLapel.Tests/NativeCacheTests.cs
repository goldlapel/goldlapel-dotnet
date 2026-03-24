using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
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
}
