using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using Xunit;
using GL = GoldLapel.GoldLapel;

namespace GoldLapel.Tests
{
    // ── Connection property ──────────────────────────────────

    public class ConnectionPropertyTest
    {
        [Fact]
        public void ThrowsWhenNoConnection()
        {
            var gl = new GL("postgresql://localhost:5432/mydb");
            var ex = Assert.Throws<InvalidOperationException>(() => { var _ = gl.Connection; });
            Assert.Contains("No connection available", ex.Message);
            Assert.Contains("StartProxy()", ex.Message);
            Assert.Contains("Npgsql", ex.Message);
        }

        [Fact]
        public void ReturnsConnectionWhenSet()
        {
            var gl = new GL("postgresql://localhost:5432/mydb");
            var spy = new SpyConnection();
            InjectConnection(gl, spy);

            Assert.Same(spy, gl.Connection);
        }

        internal static void InjectConnection(GL gl, DbConnection conn)
        {
            var field = typeof(GL).GetField("_conn", BindingFlags.NonPublic | BindingFlags.Instance);
            field.SetValue(gl, conn);
        }
    }

    // ── Instance method delegation ───────────────────────────

    public class InstanceDocMethodsTest
    {
        private GL _gl;
        private SpyConnection _spy;

        public InstanceDocMethodsTest()
        {
            _gl = new GL("postgresql://localhost:5432/mydb");
            _spy = new SpyConnection();
            ConnectionPropertyTest.InjectConnection(_gl, _spy);
        }

        [Fact]
        public void DocInsertDelegates()
        {
            _gl.DocInsert("users", "{\"name\":\"alice\"}");

            Assert.Equal(2, _spy.Commands.Count);
            Assert.Contains("CREATE TABLE IF NOT EXISTS users", _spy.Commands[0].CommandText);
            Assert.Contains("INSERT INTO users", _spy.Commands[1].CommandText);
            Assert.Equal("{\"name\":\"alice\"}", _spy.Commands[1].ParamValue("@doc"));
        }

        [Fact]
        public void DocInsertManyDelegates()
        {
            var docs = new List<string> { "{\"a\":1}", "{\"b\":2}" };
            _gl.DocInsertMany("items", docs);

            // 1 create table + 2 inserts
            Assert.Equal(3, _spy.Commands.Count);
        }

        [Fact]
        public void DocFindDelegates()
        {
            _gl.DocFind("users", filterJson: "{\"active\":true}");

            var sql = _spy.LastCommandText;
            Assert.Contains("SELECT id, data, created_at, updated_at FROM users", sql);
            Assert.Contains("WHERE data @> @p0::jsonb", sql);
            Assert.Equal("{\"active\":true}", _spy.LastCommand.ParamValue("@p0"));
        }

        [Fact]
        public void DocFindOneDelegates()
        {
            _gl.DocFindOne("users", filterJson: "{\"id\":1}");

            var sql = _spy.LastCommandText;
            Assert.Contains("FROM users", sql);
            Assert.Contains("LIMIT 1", sql);
        }

        [Fact]
        public void DocUpdateDelegates()
        {
            _gl.DocUpdate("users", "{\"active\":true}", "{\"role\":\"admin\"}");

            var sql = _spy.LastCommandText;
            Assert.Contains("UPDATE users", sql);
            Assert.Contains("SET data = data || @p0::jsonb", sql);
        }

        [Fact]
        public void DocDeleteDelegates()
        {
            _gl.DocDelete("users", "{\"active\":false}");

            var sql = _spy.LastCommandText;
            Assert.Contains("DELETE FROM users", sql);
            Assert.Contains("WHERE data @> @p0::jsonb", sql);
        }

        [Fact]
        public void DocCountDelegates()
        {
            _spy.NextScalarResult = 42L;
            _gl.DocCount("users");

            Assert.Contains("SELECT COUNT(*) FROM users", _spy.LastCommandText);
        }

        [Fact]
        public void DocCreateIndexDelegates()
        {
            _gl.DocCreateIndex("users");

            Assert.Equal(2, _spy.Commands.Count);
            Assert.Contains("CREATE INDEX IF NOT EXISTS users_data_gin", _spy.Commands[1].CommandText);
        }

        [Fact]
        public void DocAggregateDelegates()
        {
            _gl.DocAggregate("orders",
                "[{\"$group\": {\"_id\": \"$region\", \"total\": {\"$sum\": \"$amount\"}}}]");

            var sql = _spy.LastCommandText;
            Assert.Contains("FROM orders", sql);
            Assert.Contains("GROUP BY", sql);
        }
    }

    public class InstanceSearchMethodsTest
    {
        private GL _gl;
        private SpyConnection _spy;

        public InstanceSearchMethodsTest()
        {
            _gl = new GL("postgresql://localhost:5432/mydb");
            _spy = new SpyConnection();
            ConnectionPropertyTest.InjectConnection(_gl, _spy);
        }

        [Fact]
        public void SearchSingleColumnDelegates()
        {
            _gl.Search("articles", "title", "hello world");

            var sql = _spy.LastCommandText;
            Assert.Contains("to_tsvector(@lang1, coalesce(title, ''))", sql);
            Assert.Contains("FROM articles", sql);
        }

        [Fact]
        public void SearchMultiColumnDelegates()
        {
            _gl.Search("articles", new[] { "title", "body" }, "hello");

            var sql = _spy.LastCommandText;
            Assert.Contains("coalesce(title, '') || ' ' || coalesce(body, '')", sql);
        }

        [Fact]
        public void SearchFuzzyDelegates()
        {
            _gl.SearchFuzzy("articles", "title", "helo");

            // First command is CREATE EXTENSION, second is the search
            Assert.Contains("similarity(title, @query)", _spy.LastCommandText);
        }

        [Fact]
        public void SearchPhoneticDelegates()
        {
            _gl.SearchPhonetic("articles", "title", "jon");

            Assert.Contains("soundex(title) = soundex(@query2)", _spy.LastCommandText);
        }

        [Fact]
        public void SimilarDelegates()
        {
            _gl.Similar("docs", "embedding", new double[] { 0.1, 0.2, 0.3 });

            var sql = _spy.LastCommandText;
            Assert.Contains("(embedding <=> @vec::vector)", sql);
        }

        [Fact]
        public void SuggestDelegates()
        {
            _gl.Suggest("cities", "name", "new y");

            Assert.Contains("similarity(name, @prefix)", _spy.LastCommandText);
            Assert.Contains("name ILIKE @pattern", _spy.LastCommandText);
        }

        [Fact]
        public void FacetsSingleColumnDelegates()
        {
            _gl.Facets("products", "category", queryColumn: (string)null);

            Assert.Contains("category AS value", _spy.LastCommandText);
            Assert.Contains("GROUP BY category", _spy.LastCommandText);
        }

        [Fact]
        public void FacetsMultiColumnDelegates()
        {
            _gl.Facets("products", "category", query: "laptop",
                queryColumn: new[] { "title", "description" });

            Assert.Contains("coalesce(title, '') || ' ' || coalesce(description, '')",
                _spy.LastCommandText);
        }

        [Fact]
        public void AggregateDelegates()
        {
            _gl.Aggregate("orders", "amount", "sum", groupBy: "category");

            var sql = _spy.LastCommandText;
            Assert.Contains("SUM(amount) AS value", sql);
            Assert.Contains("GROUP BY category", sql);
        }

        [Fact]
        public void CreateSearchConfigDelegates()
        {
            _gl.CreateSearchConfig("my_config");

            Assert.Contains("pg_ts_config", _spy.Commands[0].CommandText);
        }
    }

    public class InstancePubSubQueueMethodsTest
    {
        private GL _gl;
        private SpyConnection _spy;

        public InstancePubSubQueueMethodsTest()
        {
            _gl = new GL("postgresql://localhost:5432/mydb");
            _spy = new SpyConnection();
            ConnectionPropertyTest.InjectConnection(_gl, _spy);
        }

        [Fact]
        public void PublishDelegates()
        {
            _gl.Publish("events", "{\"type\":\"click\"}");

            Assert.Contains("pg_notify(@channel, @message)", _spy.LastCommandText);
            Assert.Equal("events", _spy.LastCommand.ParamValue("@channel"));
        }

        [Fact]
        public void EnqueueDelegates()
        {
            _gl.Enqueue("jobs", "{\"task\":\"email\"}");

            Assert.Contains("CREATE TABLE IF NOT EXISTS jobs", _spy.Commands[0].CommandText);
            Assert.Contains("INSERT INTO jobs", _spy.Commands[1].CommandText);
        }

        [Fact]
        public void DequeueDelegates()
        {
            _gl.Dequeue("jobs");

            Assert.Contains("DELETE FROM jobs", _spy.LastCommandText);
            Assert.Contains("FOR UPDATE SKIP LOCKED", _spy.LastCommandText);
        }

        [Fact]
        public void IncrDelegates()
        {
            _spy.NextScalarResult = 5L;
            var result = _gl.Incr("counters", "page_views");

            Assert.Equal(5L, result);
            Assert.Contains("INSERT INTO counters", _spy.LastCommandText);
        }

        [Fact]
        public void GetCounterDelegates()
        {
            _gl.GetCounter("counters", "page_views");

            Assert.Contains("SELECT value FROM counters", _spy.LastCommandText);
        }
    }

    public class InstanceHashMethodsTest
    {
        private GL _gl;
        private SpyConnection _spy;

        public InstanceHashMethodsTest()
        {
            _gl = new GL("postgresql://localhost:5432/mydb");
            _spy = new SpyConnection();
            ConnectionPropertyTest.InjectConnection(_gl, _spy);
        }

        [Fact]
        public void HsetDelegates()
        {
            _gl.Hset("cache", "session:1", "user", "\"alice\"");

            Assert.Contains("CREATE TABLE IF NOT EXISTS cache", _spy.Commands[0].CommandText);
            Assert.Contains("jsonb_build_object(@field, @val::jsonb)", _spy.Commands[1].CommandText);
        }

        [Fact]
        public void HgetDelegates()
        {
            _gl.Hget("cache", "session:1", "user");

            Assert.Contains("data->>@field", _spy.LastCommandText);
            Assert.Contains("WHERE key = @key", _spy.LastCommandText);
        }

        [Fact]
        public void HgetallDelegates()
        {
            _gl.Hgetall("cache", "session:1");

            Assert.Contains("SELECT data FROM cache", _spy.LastCommandText);
        }

        [Fact]
        public void HdelDelegates()
        {
            _gl.Hdel("cache", "session:1", "user");

            Assert.Contains("data ? @field", _spy.Commands[0].CommandText);
        }
    }

    public class InstanceSortedSetMethodsTest
    {
        private GL _gl;
        private SpyConnection _spy;

        public InstanceSortedSetMethodsTest()
        {
            _gl = new GL("postgresql://localhost:5432/mydb");
            _spy = new SpyConnection();
            ConnectionPropertyTest.InjectConnection(_gl, _spy);
        }

        [Fact]
        public void ZaddDelegates()
        {
            _gl.Zadd("leaderboard", "alice", 100.0);

            Assert.Contains("CREATE TABLE IF NOT EXISTS leaderboard", _spy.Commands[0].CommandText);
            Assert.Contains("INSERT INTO leaderboard", _spy.Commands[1].CommandText);
        }

        [Fact]
        public void ZincrbyDelegates()
        {
            _spy.NextScalarResult = 105.0;
            var result = _gl.Zincrby("leaderboard", "alice", 5.0);

            Assert.Equal(105.0, result);
        }

        [Fact]
        public void ZrangeDelegates()
        {
            _gl.Zrange("leaderboard");

            Assert.Contains("SELECT member, score FROM leaderboard", _spy.LastCommandText);
            Assert.Contains("ORDER BY score DESC", _spy.LastCommandText);
        }

        [Fact]
        public void ZrankDelegates()
        {
            _gl.Zrank("leaderboard", "alice");

            Assert.Contains("ROW_NUMBER() OVER", _spy.LastCommandText);
        }

        [Fact]
        public void ZscoreDelegates()
        {
            _gl.Zscore("leaderboard", "alice");

            Assert.Contains("SELECT score FROM leaderboard", _spy.LastCommandText);
        }

        [Fact]
        public void ZremDelegates()
        {
            _gl.Zrem("leaderboard", "alice");

            Assert.Contains("DELETE FROM leaderboard WHERE member = @member", _spy.LastCommandText);
        }
    }

    public class InstanceGeoMethodsTest
    {
        private GL _gl;
        private SpyConnection _spy;

        public InstanceGeoMethodsTest()
        {
            _gl = new GL("postgresql://localhost:5432/mydb");
            _spy = new SpyConnection();
            ConnectionPropertyTest.InjectConnection(_gl, _spy);
        }

        [Fact]
        public void GeoaddDelegates()
        {
            _gl.Geoadd("places", "name", "geom", "NYC", -74.006, 40.7128);

            Assert.Contains("CREATE EXTENSION IF NOT EXISTS postgis", _spy.Commands[0].CommandText);
            Assert.Contains("ST_SetSRID(ST_MakePoint(@lon, @lat), 4326)", _spy.Commands[2].CommandText);
        }

        [Fact]
        public void GeoradiusDelegates()
        {
            _gl.Georadius("places", "geom", -74.006, 40.7128, 5000.0);

            Assert.Contains("ST_DWithin", _spy.LastCommandText);
            Assert.Contains("FROM places", _spy.LastCommandText);
        }

        [Fact]
        public void GeodistDelegates()
        {
            _gl.Geodist("places", "geom", "name", "NYC", "LA");

            Assert.Contains("ST_Distance", _spy.LastCommandText);
        }
    }

    public class InstanceMiscMethodsTest
    {
        private GL _gl;
        private SpyConnection _spy;

        public InstanceMiscMethodsTest()
        {
            _gl = new GL("postgresql://localhost:5432/mydb");
            _spy = new SpyConnection();
            ConnectionPropertyTest.InjectConnection(_gl, _spy);
        }

        [Fact]
        public void CountDistinctDelegates()
        {
            _spy.NextScalarResult = 42L;
            _gl.CountDistinct("users", "email");

            Assert.Contains("COUNT(DISTINCT email)", _spy.LastCommandText);
        }

        [Fact]
        public void ScriptDelegates()
        {
            _spy.NextScalarResult = "hello";
            _gl.Script("return 'hello'");

            Assert.Contains("CREATE EXTENSION IF NOT EXISTS pllua", _spy.Commands[0].CommandText);
            Assert.Contains("LANGUAGE pllua", _spy.Commands[1].CommandText);
        }
    }

    public class InstanceStreamMethodsTest
    {
        private GL _gl;
        private SpyConnection _spy;

        public InstanceStreamMethodsTest()
        {
            _gl = new GL("postgresql://localhost:5432/mydb");
            _spy = new SpyConnection();
            ConnectionPropertyTest.InjectConnection(_gl, _spy);
        }

        [Fact]
        public void StreamAddDelegates()
        {
            _spy.NextScalarResult = 1L;
            var id = _gl.StreamAdd("events", "{\"type\":\"click\"}");

            Assert.Equal(1L, id);
            Assert.Contains("INSERT INTO events", _spy.LastCommandText);
        }

        [Fact]
        public void StreamCreateGroupDelegates()
        {
            _gl.StreamCreateGroup("events", "workers");

            Assert.Contains("events_groups", _spy.Commands[0].CommandText);
            Assert.Contains("events_cursors", _spy.Commands[1].CommandText);
        }

        [Fact]
        public void StreamReadDelegates()
        {
            _gl.StreamRead("events", "workers", "w1");

            Assert.Contains("SELECT id, payload, created_at FROM new_msgs", _spy.LastCommandText);
        }

        [Fact]
        public void StreamAckDelegates()
        {
            _gl.StreamAck("events", "workers", 1);

            Assert.Contains("acked = TRUE", _spy.LastCommandText);
        }

        [Fact]
        public void StreamClaimDelegates()
        {
            _gl.StreamClaim("events", "workers", "w2");

            Assert.Contains("claimed_at = NOW()", _spy.LastCommandText);
        }
    }

    public class InstancePercolateMethodsTest
    {
        private GL _gl;
        private SpyConnection _spy;

        public InstancePercolateMethodsTest()
        {
            _gl = new GL("postgresql://localhost:5432/mydb");
            _spy = new SpyConnection();
            ConnectionPropertyTest.InjectConnection(_gl, _spy);
        }

        [Fact]
        public void PercolateAddDelegates()
        {
            _gl.PercolateAdd("alerts", "q1", "breaking news");

            Assert.Contains("CREATE TABLE IF NOT EXISTS alerts", _spy.Commands[0].CommandText);
            Assert.Contains("INSERT INTO alerts", _spy.Commands[2].CommandText);
        }

        [Fact]
        public void PercolateDelegates()
        {
            _gl.Percolate("alerts", "big news today");

            Assert.Contains("to_tsvector(@lang, @text) @@ tsquery", _spy.LastCommandText);
        }

        [Fact]
        public void PercolateDeleteDelegates()
        {
            _gl.PercolateDelete("alerts", "q1");

            Assert.Contains("DELETE FROM alerts", _spy.LastCommandText);
            Assert.Contains("WHERE query_id = @queryId", _spy.LastCommandText);
        }
    }

    public class InstanceDebugMethodsTest
    {
        private GL _gl;
        private SpyConnection _spy;

        public InstanceDebugMethodsTest()
        {
            _gl = new GL("postgresql://localhost:5432/mydb");
            _spy = new SpyConnection();
            ConnectionPropertyTest.InjectConnection(_gl, _spy);
        }

        [Fact]
        public void AnalyzeDelegates()
        {
            _gl.Analyze("The quick brown fox");

            Assert.Contains("ts_debug(@lang, @text)", _spy.LastCommandText);
            Assert.Equal("english", _spy.LastCommand.ParamValue("@lang"));
        }

        [Fact]
        public void ExplainScoreDelegates()
        {
            _gl.ExplainScore("articles", "body", "search term", "id", 42);

            var sql = _spy.LastCommandText;
            Assert.Contains("ts_rank(", sql);
            Assert.Contains("FROM articles", sql);
            Assert.Contains("WHERE id = @idValue", sql);
        }
    }

    // ── Instance operational methods ────────────────────────

    public class InstanceOperationalMethodsTest
    {
        private GL _gl;
        private SpyConnection _spy;

        public InstanceOperationalMethodsTest()
        {
            _gl = new GL("postgresql://localhost:5432/mydb");
            _spy = new SpyConnection();
            ConnectionPropertyTest.InjectConnection(_gl, _spy);
        }

        [Fact]
        public void DocWatchDelegates()
        {
            // DocWatch creates trigger DDL then tries to LISTEN (which fails on SpyConnection)
            try { _gl.DocWatch("events", (ch, msg) => { }); }
            catch (Exception) { }

            var sqls = _spy.Commands.Select(c => c.CommandText).ToList();
            Assert.True(sqls.Any(s => s.Contains("CREATE OR REPLACE FUNCTION _gl_watch_events()")));
            Assert.True(sqls.Any(s => s.Contains("AFTER INSERT OR UPDATE OR DELETE ON events")));
        }

        [Fact]
        public void DocUnwatchDelegates()
        {
            _gl.DocUnwatch("events");

            var sqls = _spy.Commands.Select(c => c.CommandText).ToList();
            Assert.Contains("DROP TRIGGER IF EXISTS _gl_watch_events_trigger ON events", sqls);
            Assert.Contains("DROP FUNCTION IF EXISTS _gl_watch_events()", sqls);
        }

        [Fact]
        public void DocCreateTtlIndexDelegates()
        {
            _gl.DocCreateTtlIndex("sessions", 3600);

            var sqls = _spy.Commands.Select(c => c.CommandText).ToList();
            Assert.True(sqls.Any(s => s.Contains("CREATE INDEX IF NOT EXISTS idx_sessions_ttl")));
            Assert.True(sqls.Any(s => s.Contains("INTERVAL '3600 seconds'")));
        }

        [Fact]
        public void DocRemoveTtlIndexDelegates()
        {
            _gl.DocRemoveTtlIndex("sessions");

            var sqls = _spy.Commands.Select(c => c.CommandText).ToList();
            Assert.Contains("DROP TRIGGER IF EXISTS _gl_ttl_sessions_trigger ON sessions", sqls);
            Assert.Contains("DROP FUNCTION IF EXISTS _gl_ttl_sessions()", sqls);
            Assert.Contains("DROP INDEX IF EXISTS idx_sessions_ttl", sqls);
        }

        [Fact]
        public void DocCreateCappedDelegates()
        {
            _gl.DocCreateCapped("logs", 1000);

            var sqls = _spy.Commands.Select(c => c.CommandText).ToList();
            Assert.True(sqls.Any(s => s.Contains("CREATE TABLE IF NOT EXISTS logs")));
            Assert.True(sqls.Any(s => s.Contains("CREATE OR REPLACE FUNCTION _gl_cap_logs()")));
            Assert.True(sqls.Any(s => s.Contains("COUNT(*) - 1000")));
        }

        [Fact]
        public void DocRemoveCapDelegates()
        {
            _gl.DocRemoveCap("logs");

            var sqls = _spy.Commands.Select(c => c.CommandText).ToList();
            Assert.Contains("DROP TRIGGER IF EXISTS _gl_cap_logs_trigger ON logs", sqls);
            Assert.Contains("DROP FUNCTION IF EXISTS _gl_cap_logs()", sqls);
        }
    }

    // ── Connection cleanup ───────────────────────────────────

    public class ConnectionCleanupTest
    {
        [Fact]
        public void StopProxyClosesConnection()
        {
            var gl = new GL("postgresql://localhost:5432/mydb");
            var spy = new SpyConnection();
            ConnectionPropertyTest.InjectConnection(gl, spy);

            Assert.False(spy.WasClosed);
            Assert.False(spy.WasDisposed);

            gl.StopProxy();

            Assert.True(spy.WasClosed);
            Assert.True(spy.WasDisposed);

            // Connection should be null now
            Assert.Throws<InvalidOperationException>(() => { var _ = gl.Connection; });
        }

        [Fact]
        public void DisposeClosesConnection()
        {
            var gl = new GL("postgresql://localhost:5432/mydb");
            var spy = new SpyConnection();
            ConnectionPropertyTest.InjectConnection(gl, spy);

            gl.Dispose();

            Assert.True(spy.WasClosed);
            Assert.True(spy.WasDisposed);
        }

        [Fact]
        public void StopProxyIdempotentWithConnection()
        {
            var gl = new GL("postgresql://localhost:5432/mydb");
            var spy = new SpyConnection();
            ConnectionPropertyTest.InjectConnection(gl, spy);

            gl.StopProxy();
            // Should not throw on second call
            gl.StopProxy();
        }
    }

}
