using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Xunit;
using GL = GoldLapel.GoldLapel;
using GoldLapel;

namespace GoldLapel.Tests
{
    // Helper: create a test GoldLapel with an injected SpyConnection so wrapper
    // methods can run without spawning the binary.
    internal static class TestHelpers
    {
        public static GL MakeWithSpy(out SpyConnection spy)
        {
            spy = new SpyConnection();
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            var field = typeof(GL).GetField("_testConn", BindingFlags.NonPublic | BindingFlags.Instance);
            field.SetValue(gl, spy);
            return gl;
        }

        /// <summary>
        /// Pre-populate the instance's DDL cache with canonical stream patterns
        /// so stream* methods can run without hitting the dashboard.
        /// </summary>
        public static void InjectStreamPatterns(GL gl, string name)
        {
            var tbl = "_goldlapel.stream_" + name;
            var entry = new DdlEntry
            {
                Tables = new Dictionary<string, string>
                {
                    ["main"] = tbl,
                    ["groups"] = tbl + "_groups",
                    ["pending"] = tbl + "_pending",
                },
                QueryPatterns = new Dictionary<string, string>
                {
                    ["insert"] = "INSERT INTO " + tbl + " (payload) VALUES ($1) RETURNING id, created_at",
                    ["read_since"] = "SELECT id, payload, created_at FROM " + tbl + " WHERE id > $1 ORDER BY id LIMIT $2",
                    ["read_by_id"] = "SELECT id, payload, created_at FROM " + tbl + " WHERE id = $1",
                    ["group_get_cursor"] = "SELECT last_delivered_id FROM " + tbl + "_groups WHERE group_name = $1 FOR UPDATE",
                    ["group_advance_cursor"] = "UPDATE " + tbl + "_groups SET last_delivered_id = $1 WHERE group_name = $2",
                    ["pending_insert"] = "INSERT INTO " + tbl + "_pending (message_id, group_name, consumer) VALUES ($1, $2, $3) ON CONFLICT (group_name, message_id) DO NOTHING",
                    ["create_group"] = "INSERT INTO " + tbl + "_groups (group_name) VALUES ($1) ON CONFLICT DO NOTHING",
                    ["ack"] = "DELETE FROM " + tbl + "_pending WHERE group_name = $1 AND message_id = $2",
                    ["claim"] = "UPDATE " + tbl + "_pending SET consumer = $1, claimed_at = NOW(), delivery_count = delivery_count + 1 WHERE group_name = $2 AND claimed_at < NOW() - INTERVAL '1 millisecond' * $3 RETURNING message_id",
                },
            };
            var cacheField = typeof(GL).GetField("_ddlCache", BindingFlags.NonPublic | BindingFlags.Instance);
            var cache = (System.Collections.Concurrent.ConcurrentDictionary<string, DdlEntry>) cacheField.GetValue(gl);
            cache["stream:" + name] = entry;
        }
    }

    // ── Instance method delegation ───────────────────────────

    public class InstanceDocMethodsTest
    {
        private readonly GL _gl;
        private readonly SpyConnection _spy;

        public InstanceDocMethodsTest()
        {
            _gl = TestHelpers.MakeWithSpy(out _spy);
        }

        [Fact]
        public async Task DocInsertAsyncDelegates()
        {
            await _gl.DocInsertAsync("users", "{\"name\":\"alice\"}");

            Assert.Equal(2, _spy.Commands.Count);
            Assert.Contains("CREATE TABLE IF NOT EXISTS users", _spy.Commands[0].CommandText);
            Assert.Contains("INSERT INTO users", _spy.Commands[1].CommandText);
            Assert.Equal("{\"name\":\"alice\"}", _spy.Commands[1].ParamValue("@doc"));
        }

        [Fact]
        public async Task DocInsertManyAsyncDelegates()
        {
            var docs = new List<string> { "{\"a\":1}", "{\"b\":2}" };
            await _gl.DocInsertManyAsync("items", docs);

            Assert.Equal(3, _spy.Commands.Count);
        }

        [Fact]
        public async Task DocFindAsyncDelegates()
        {
            await _gl.DocFindAsync("users", filterJson: "{\"active\":true}");

            var sql = _spy.LastCommandText;
            Assert.Contains("SELECT _id, data, created_at, updated_at FROM users", sql);
            Assert.Contains("WHERE data @> @p0::jsonb", sql);
            Assert.Equal("{\"active\":true}", _spy.LastCommand.ParamValue("@p0"));
        }

        [Fact]
        public void DocFindCursorDelegates()
        {
            // Cursor stays synchronous-iterable (IEnumerable) — exhaust to issue SQL.
            foreach (var _ in _gl.DocFindCursor("users")) { }

            var sqls = _spy.Commands.Select(c => c.CommandText).ToList();
            Assert.Equal("BEGIN", sqls[0]);
            Assert.Contains("CURSOR FOR", sqls[1]);
            Assert.Contains("SELECT _id, data, created_at, updated_at FROM users", sqls[1]);
        }

        [Fact]
        public async Task DocFindOneAsyncDelegates()
        {
            await _gl.DocFindOneAsync("users", filterJson: "{\"id\":1}");

            var sql = _spy.LastCommandText;
            Assert.Contains("FROM users", sql);
            Assert.Contains("LIMIT 1", sql);
        }

        [Fact]
        public async Task DocUpdateAsyncDelegates()
        {
            await _gl.DocUpdateAsync("users", "{\"active\":true}", "{\"role\":\"admin\"}");

            var sql = _spy.LastCommandText;
            Assert.Contains("UPDATE users", sql);
            Assert.Contains("SET data = data || @p0::jsonb", sql);
        }

        [Fact]
        public async Task DocDeleteAsyncDelegates()
        {
            await _gl.DocDeleteAsync("users", "{\"active\":false}");

            var sql = _spy.LastCommandText;
            Assert.Contains("DELETE FROM users", sql);
            Assert.Contains("WHERE data @> @p0::jsonb", sql);
        }

        [Fact]
        public async Task DocCountAsyncDelegates()
        {
            _spy.NextScalarResult = 42L;
            await _gl.DocCountAsync("users");

            Assert.Contains("SELECT COUNT(*) FROM users", _spy.LastCommandText);
        }

        [Fact]
        public async Task DocCreateIndexAsyncDelegates()
        {
            await _gl.DocCreateIndexAsync("users");

            Assert.Equal(2, _spy.Commands.Count);
            Assert.Contains("CREATE INDEX IF NOT EXISTS users_data_gin", _spy.Commands[1].CommandText);
        }

        [Fact]
        public async Task DocAggregateAsyncDelegates()
        {
            await _gl.DocAggregateAsync("orders",
                "[{\"$group\": {\"_id\": \"$region\", \"total\": {\"$sum\": \"$amount\"}}}]");

            var sql = _spy.LastCommandText;
            Assert.Contains("FROM orders", sql);
            Assert.Contains("GROUP BY", sql);
        }
    }

    public class InstanceSearchMethodsTest
    {
        private readonly GL _gl;
        private readonly SpyConnection _spy;

        public InstanceSearchMethodsTest()
        {
            _gl = TestHelpers.MakeWithSpy(out _spy);
        }

        [Fact]
        public async Task SearchSingleColumnDelegates()
        {
            await _gl.SearchAsync("articles", "title", "hello world");

            var sql = _spy.LastCommandText;
            Assert.Contains("to_tsvector(@lang1, coalesce(title, ''))", sql);
            Assert.Contains("FROM articles", sql);
        }

        [Fact]
        public async Task SearchMultiColumnDelegates()
        {
            await _gl.SearchAsync("articles", new[] { "title", "body" }, "hello");

            var sql = _spy.LastCommandText;
            Assert.Contains("coalesce(title, '') || ' ' || coalesce(body, '')", sql);
        }

        [Fact]
        public async Task SearchFuzzyAsyncDelegates()
        {
            await _gl.SearchFuzzyAsync("articles", "title", "helo");

            Assert.Contains("similarity(title, @query)", _spy.LastCommandText);
        }

        [Fact]
        public async Task SearchPhoneticAsyncDelegates()
        {
            await _gl.SearchPhoneticAsync("articles", "title", "jon");

            Assert.Contains("soundex(title) = soundex(@query2)", _spy.LastCommandText);
        }

        [Fact]
        public async Task SimilarAsyncDelegates()
        {
            await _gl.SimilarAsync("docs", "embedding", new double[] { 0.1, 0.2, 0.3 });

            var sql = _spy.LastCommandText;
            Assert.Contains("(embedding <=> @vec::vector)", sql);
        }

        [Fact]
        public async Task SuggestAsyncDelegates()
        {
            await _gl.SuggestAsync("cities", "name", "new y");

            Assert.Contains("similarity(name, @prefix)", _spy.LastCommandText);
            Assert.Contains("name ILIKE @pattern", _spy.LastCommandText);
        }

        [Fact]
        public async Task FacetsSingleColumnDelegates()
        {
            await _gl.FacetsAsync("products", "category", queryColumn: (string)null);

            Assert.Contains("category AS value", _spy.LastCommandText);
            Assert.Contains("GROUP BY category", _spy.LastCommandText);
        }

        [Fact]
        public async Task FacetsMultiColumnDelegates()
        {
            await _gl.FacetsAsync("products", "category",
                queryColumns: new[] { "title", "description" },
                query: "laptop");

            Assert.Contains("coalesce(title, '') || ' ' || coalesce(description, '')",
                _spy.LastCommandText);
        }

        [Fact]
        public async Task AggregateAsyncDelegates()
        {
            await _gl.AggregateAsync("orders", "amount", "sum", groupBy: "category");

            var sql = _spy.LastCommandText;
            Assert.Contains("SUM(amount) AS value", sql);
            Assert.Contains("GROUP BY category", sql);
        }

        [Fact]
        public async Task CreateSearchConfigAsyncDelegates()
        {
            await _gl.CreateSearchConfigAsync("my_config");

            Assert.Contains("pg_ts_config", _spy.Commands[0].CommandText);
        }
    }

    public class InstancePubSubQueueMethodsTest
    {
        private readonly GL _gl;
        private readonly SpyConnection _spy;

        public InstancePubSubQueueMethodsTest()
        {
            _gl = TestHelpers.MakeWithSpy(out _spy);
        }

        [Fact]
        public async Task PublishAsyncDelegates()
        {
            await _gl.PublishAsync("events", "{\"type\":\"click\"}");

            Assert.Contains("pg_notify(@channel, @message)", _spy.LastCommandText);
            Assert.Equal("events", _spy.LastCommand.ParamValue("@channel"));
        }

        [Fact]
        public async Task EnqueueAsyncDelegates()
        {
            await _gl.EnqueueAsync("jobs", "{\"task\":\"email\"}");

            Assert.Contains("CREATE TABLE IF NOT EXISTS jobs", _spy.Commands[0].CommandText);
            Assert.Contains("INSERT INTO jobs", _spy.Commands[1].CommandText);
        }

        [Fact]
        public async Task DequeueAsyncDelegates()
        {
            await _gl.DequeueAsync("jobs");

            Assert.Contains("DELETE FROM jobs", _spy.LastCommandText);
            Assert.Contains("FOR UPDATE SKIP LOCKED", _spy.LastCommandText);
        }

        [Fact]
        public async Task IncrAsyncDelegates()
        {
            _spy.NextScalarResult = 5L;
            var result = await _gl.IncrAsync("counters", "page_views");

            Assert.Equal(5L, result);
            Assert.Contains("INSERT INTO counters", _spy.LastCommandText);
        }

        [Fact]
        public async Task GetCounterAsyncDelegates()
        {
            await _gl.GetCounterAsync("counters", "page_views");

            Assert.Contains("SELECT value FROM counters", _spy.LastCommandText);
        }
    }

    public class InstanceHashMethodsTest
    {
        private readonly GL _gl;
        private readonly SpyConnection _spy;

        public InstanceHashMethodsTest()
        {
            _gl = TestHelpers.MakeWithSpy(out _spy);
        }

        [Fact]
        public async Task HsetAsyncDelegates()
        {
            await _gl.HsetAsync("cache", "session:1", "user", "\"alice\"");

            Assert.Contains("CREATE TABLE IF NOT EXISTS cache", _spy.Commands[0].CommandText);
            Assert.Contains("jsonb_build_object(@field, @val::jsonb)", _spy.Commands[1].CommandText);
        }

        [Fact]
        public async Task HgetAsyncDelegates()
        {
            await _gl.HgetAsync("cache", "session:1", "user");

            Assert.Contains("data->>@field", _spy.LastCommandText);
            Assert.Contains("WHERE key = @key", _spy.LastCommandText);
        }

        [Fact]
        public async Task HgetallAsyncDelegates()
        {
            await _gl.HgetallAsync("cache", "session:1");

            Assert.Contains("SELECT data FROM cache", _spy.LastCommandText);
        }

        [Fact]
        public async Task HdelAsyncDelegates()
        {
            await _gl.HdelAsync("cache", "session:1", "user");

            Assert.Contains("data ? @field", _spy.Commands[0].CommandText);
        }
    }

    public class InstanceSortedSetMethodsTest
    {
        private readonly GL _gl;
        private readonly SpyConnection _spy;

        public InstanceSortedSetMethodsTest()
        {
            _gl = TestHelpers.MakeWithSpy(out _spy);
        }

        [Fact]
        public async Task ZaddAsyncDelegates()
        {
            await _gl.ZaddAsync("leaderboard", "alice", 100.0);

            Assert.Contains("CREATE TABLE IF NOT EXISTS leaderboard", _spy.Commands[0].CommandText);
            Assert.Contains("INSERT INTO leaderboard", _spy.Commands[1].CommandText);
        }

        [Fact]
        public async Task ZincrbyAsyncDelegates()
        {
            _spy.NextScalarResult = 105.0;
            var result = await _gl.ZincrbyAsync("leaderboard", "alice", 5.0);

            Assert.Equal(105.0, result);
        }

        [Fact]
        public async Task ZrangeAsyncDelegates()
        {
            await _gl.ZrangeAsync("leaderboard");

            Assert.Contains("SELECT member, score FROM leaderboard", _spy.LastCommandText);
            Assert.Contains("ORDER BY score DESC", _spy.LastCommandText);
        }

        [Fact]
        public async Task ZrankAsyncDelegates()
        {
            await _gl.ZrankAsync("leaderboard", "alice");

            Assert.Contains("ROW_NUMBER() OVER", _spy.LastCommandText);
        }

        [Fact]
        public async Task ZscoreAsyncDelegates()
        {
            await _gl.ZscoreAsync("leaderboard", "alice");

            Assert.Contains("SELECT score FROM leaderboard", _spy.LastCommandText);
        }

        [Fact]
        public async Task ZremAsyncDelegates()
        {
            await _gl.ZremAsync("leaderboard", "alice");

            Assert.Contains("DELETE FROM leaderboard WHERE member = @member", _spy.LastCommandText);
        }
    }

    public class InstanceGeoMethodsTest
    {
        private readonly GL _gl;
        private readonly SpyConnection _spy;

        public InstanceGeoMethodsTest()
        {
            _gl = TestHelpers.MakeWithSpy(out _spy);
        }

        [Fact]
        public async Task GeoaddAsyncDelegates()
        {
            await _gl.GeoaddAsync("places", "name", "geom", "NYC", -74.006, 40.7128);

            Assert.Contains("CREATE EXTENSION IF NOT EXISTS postgis", _spy.Commands[0].CommandText);
            Assert.Contains("ST_SetSRID(ST_MakePoint(@lon, @lat), 4326)", _spy.Commands[2].CommandText);
        }

        [Fact]
        public async Task GeoradiusAsyncDelegates()
        {
            await _gl.GeoradiusAsync("places", "geom", -74.006, 40.7128, 5000.0);

            Assert.Contains("ST_DWithin", _spy.LastCommandText);
            Assert.Contains("FROM places", _spy.LastCommandText);
        }

        [Fact]
        public async Task GeodistAsyncDelegates()
        {
            await _gl.GeodistAsync("places", "geom", "name", "NYC", "LA");

            Assert.Contains("ST_Distance", _spy.LastCommandText);
        }
    }

    public class InstanceMiscMethodsTest
    {
        private readonly GL _gl;
        private readonly SpyConnection _spy;

        public InstanceMiscMethodsTest()
        {
            _gl = TestHelpers.MakeWithSpy(out _spy);
        }

        [Fact]
        public async Task CountDistinctAsyncDelegates()
        {
            _spy.NextScalarResult = 42L;
            await _gl.CountDistinctAsync("users", "email");

            Assert.Contains("COUNT(DISTINCT email)", _spy.LastCommandText);
        }

        [Fact]
        public async Task ScriptAsyncDelegates()
        {
            _spy.NextScalarResult = "hello";
            await _gl.ScriptAsync("return 'hello'");

            Assert.Contains("CREATE EXTENSION IF NOT EXISTS pllua", _spy.Commands[0].CommandText);
            Assert.Contains("LANGUAGE pllua", _spy.Commands[1].CommandText);
        }
    }

    public class InstanceStreamMethodsTest
    {
        private readonly GL _gl;
        private readonly SpyConnection _spy;

        public InstanceStreamMethodsTest()
        {
            _gl = TestHelpers.MakeWithSpy(out _spy);
        }

        [Fact]
        public async Task StreamAddAsyncDelegates()
        {
            TestHelpers.InjectStreamPatterns(_gl, "events");
            // StreamAdd now reads (id, created_at) from a DataReader — the
            // canonical RETURNING pattern. Queue a 1-row reader on the spy.
            _spy.NextReaderFactory = () => new FakeDataReader(
                new object[][] { new object[] { 1L, DateTime.UtcNow } },
                new[] { "id", "created_at" }
            );
            var id = await _gl.StreamAddAsync("events", "{\"type\":\"click\"}");

            Assert.Equal(1L, id);
            Assert.Contains("INSERT INTO _goldlapel.stream_events", _spy.LastCommandText);
            // No in-wrapper CREATE TABLE — proxy owns DDL.
            Assert.DoesNotContain("CREATE TABLE", _spy.LastCommandText);
        }

        [Fact]
        public async Task StreamCreateGroupAsyncDelegates()
        {
            TestHelpers.InjectStreamPatterns(_gl, "events");
            await _gl.StreamCreateGroupAsync("events", "workers");

            // Only one statement should run (the INSERT into groups); the
            // CREATE TABLEs that used to live here are now proxy-side.
            Assert.Single(_spy.Commands);
            Assert.Contains("INSERT INTO _goldlapel.stream_events_groups", _spy.Commands[0].CommandText);
        }

        [Fact]
        public async Task StreamReadAsyncDelegates()
        {
            TestHelpers.InjectStreamPatterns(_gl, "events");
            // cursor lookup returns no row → streamRead returns early.
            await _gl.StreamReadAsync("events", "workers", "w1");

            Assert.Contains("last_delivered_id FROM _goldlapel.stream_events_groups", _spy.LastCommandText);
        }

        [Fact]
        public async Task StreamAckAsyncDelegates()
        {
            TestHelpers.InjectStreamPatterns(_gl, "events");
            await _gl.StreamAckAsync("events", "workers", 1);

            Assert.Contains("DELETE FROM _goldlapel.stream_events_pending", _spy.LastCommandText);
        }

        [Fact]
        public async Task StreamClaimAsyncDelegates()
        {
            TestHelpers.InjectStreamPatterns(_gl, "events");
            await _gl.StreamClaimAsync("events", "workers", "w2");

            Assert.Contains("delivery_count + 1", _spy.LastCommandText);
        }
    }

    public class InstancePercolateMethodsTest
    {
        private readonly GL _gl;
        private readonly SpyConnection _spy;

        public InstancePercolateMethodsTest()
        {
            _gl = TestHelpers.MakeWithSpy(out _spy);
        }

        [Fact]
        public async Task PercolateAddAsyncDelegates()
        {
            await _gl.PercolateAddAsync("alerts", "q1", "breaking news");

            Assert.Contains("CREATE TABLE IF NOT EXISTS alerts", _spy.Commands[0].CommandText);
            Assert.Contains("INSERT INTO alerts", _spy.Commands[2].CommandText);
        }

        [Fact]
        public async Task PercolateAsyncDelegates()
        {
            await _gl.PercolateAsync("alerts", "big news today");

            Assert.Contains("to_tsvector(@lang, @text) @@ tsquery", _spy.LastCommandText);
        }

        [Fact]
        public async Task PercolateDeleteAsyncDelegates()
        {
            await _gl.PercolateDeleteAsync("alerts", "q1");

            Assert.Contains("DELETE FROM alerts", _spy.LastCommandText);
            Assert.Contains("WHERE query_id = @queryId", _spy.LastCommandText);
        }
    }

    public class InstanceDebugMethodsTest
    {
        private readonly GL _gl;
        private readonly SpyConnection _spy;

        public InstanceDebugMethodsTest()
        {
            _gl = TestHelpers.MakeWithSpy(out _spy);
        }

        [Fact]
        public async Task AnalyzeAsyncDelegates()
        {
            await _gl.AnalyzeAsync("The quick brown fox");

            Assert.Contains("ts_debug(@lang, @text)", _spy.LastCommandText);
            Assert.Equal("english", _spy.LastCommand.ParamValue("@lang"));
        }

        [Fact]
        public async Task ExplainScoreAsyncDelegates()
        {
            await _gl.ExplainScoreAsync("articles", "body", "search term", "id", 42);

            var sql = _spy.LastCommandText;
            Assert.Contains("ts_rank(", sql);
            Assert.Contains("FROM articles", sql);
            Assert.Contains("WHERE id = @idValue", sql);
        }
    }

    public class InstanceOperationalMethodsTest
    {
        private readonly GL _gl;
        private readonly SpyConnection _spy;

        public InstanceOperationalMethodsTest()
        {
            _gl = TestHelpers.MakeWithSpy(out _spy);
        }

        [Fact]
        public async Task DocWatchAsyncDelegates()
        {
            // DocWatch creates trigger DDL then tries to LISTEN (which fails on SpyConnection).
            try { await _gl.DocWatchAsync("events", (ch, msg) => { }); }
            catch (Exception) { }

            var sqls = _spy.Commands.Select(c => c.CommandText).ToList();
            Assert.True(sqls.Any(s => s.Contains("CREATE OR REPLACE FUNCTION _gl_watch_events()")));
            Assert.True(sqls.Any(s => s.Contains("AFTER INSERT OR UPDATE OR DELETE ON events")));
        }

        [Fact]
        public async Task DocUnwatchAsyncDelegates()
        {
            await _gl.DocUnwatchAsync("events");

            var sqls = _spy.Commands.Select(c => c.CommandText).ToList();
            Assert.Contains("DROP TRIGGER IF EXISTS _gl_watch_events_trigger ON events", sqls);
            Assert.Contains("DROP FUNCTION IF EXISTS _gl_watch_events()", sqls);
        }

        [Fact]
        public async Task DocCreateTtlIndexAsyncDelegates()
        {
            await _gl.DocCreateTtlIndexAsync("sessions", 3600);

            var sqls = _spy.Commands.Select(c => c.CommandText).ToList();
            Assert.True(sqls.Any(s => s.Contains("CREATE INDEX IF NOT EXISTS idx_sessions_ttl")));
            Assert.True(sqls.Any(s => s.Contains("INTERVAL '3600 seconds'")));
        }

        [Fact]
        public async Task DocRemoveTtlIndexAsyncDelegates()
        {
            await _gl.DocRemoveTtlIndexAsync("sessions");

            var sqls = _spy.Commands.Select(c => c.CommandText).ToList();
            Assert.Contains("DROP TRIGGER IF EXISTS _gl_ttl_sessions_trigger ON sessions", sqls);
            Assert.Contains("DROP FUNCTION IF EXISTS _gl_ttl_sessions()", sqls);
            Assert.Contains("DROP INDEX IF EXISTS idx_sessions_ttl", sqls);
        }

        [Fact]
        public async Task DocCreateCappedAsyncDelegates()
        {
            await _gl.DocCreateCappedAsync("logs", 1000);

            var sqls = _spy.Commands.Select(c => c.CommandText).ToList();
            Assert.True(sqls.Any(s => s.Contains("CREATE TABLE IF NOT EXISTS logs")));
            Assert.True(sqls.Any(s => s.Contains("CREATE OR REPLACE FUNCTION _gl_cap_logs()")));
            Assert.True(sqls.Any(s => s.Contains("COUNT(*) - 1000")));
        }

        [Fact]
        public async Task DocRemoveCapAsyncDelegates()
        {
            await _gl.DocRemoveCapAsync("logs");

            var sqls = _spy.Commands.Select(c => c.CommandText).ToList();
            Assert.Contains("DROP TRIGGER IF EXISTS _gl_cap_logs_trigger ON logs", sqls);
            Assert.Contains("DROP FUNCTION IF EXISTS _gl_cap_logs()", sqls);
        }
    }

    // ── v0.2.0 — UsingAsync (scoped connection override) ──────────

    public class UsingAsyncScopeTest
    {
        private static void InjectTestConn(GL gl, DbConnection conn)
        {
            typeof(GL).GetField("_testConn", BindingFlags.NonPublic | BindingFlags.Instance)
                .SetValue(gl, conn);
        }

        [Fact]
        public async Task UsingAsyncOverridesConnection()
        {
            var spyDefault = new SpyConnection();
            var spyScoped = new SpyConnection();

            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            InjectTestConn(gl, spyDefault);

            await gl.UsingAsync(spyScoped, async scoped =>
            {
                await scoped.DocInsertAsync("events", "{\"type\":\"x\"}");
            });

            Assert.Empty(spyDefault.Commands);
            Assert.Equal(2, spyScoped.Commands.Count); // CREATE TABLE + INSERT
            Assert.Contains("INSERT INTO events", spyScoped.Commands[1].CommandText);
        }

        [Fact]
        public async Task UsingAsyncScopeUnwindsOnException()
        {
            var spyDefault = new SpyConnection();
            var spyScoped = new SpyConnection();

            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            InjectTestConn(gl, spyDefault);

            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await gl.UsingAsync(spyScoped, _ =>
                {
                    throw new InvalidOperationException("boom");
                });
            });

            // After UsingAsync returns, scope unwound — subsequent calls hit default.
            await gl.DocInsertAsync("users", "{\"name\":\"a\"}");
            Assert.Equal(2, spyDefault.Commands.Count);
        }

        [Fact]
        public async Task UsingAsyncHoldsScopeAcrossAwaits()
        {
            var spyDefault = new SpyConnection();
            var spyScoped = new SpyConnection();

            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            InjectTestConn(gl, spyDefault);

            await gl.UsingAsync(spyScoped, async scoped =>
            {
                await scoped.DocInsertAsync("events", "{\"n\":1}");
                await Task.Yield();            // force an await boundary
                await Task.Delay(1);
                await scoped.DocInsertAsync("events", "{\"n\":2}");
            });

            Assert.Empty(spyDefault.Commands);
            // CREATE TABLE + 2 inserts all land on the scoped spy.
            Assert.True(spyScoped.Commands.Count >= 3);
        }

        [Fact]
        public async Task UsingAsyncNestsProperly()
        {
            var a = new SpyConnection();
            var b = new SpyConnection();
            var c = new SpyConnection();

            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            InjectTestConn(gl, a);

            await gl.UsingAsync(b, async gl2 =>
            {
                await gl2.DocInsertAsync("x", "{}");
                await gl2.UsingAsync(c, async gl3 =>
                {
                    await gl3.DocInsertAsync("x", "{}");
                });
                await gl2.DocInsertAsync("x", "{}");
            });
            await gl.DocInsertAsync("x", "{}");

            Assert.Equal(2, a.Commands.Count); // create + 1 insert
            Assert.True(b.Commands.Count >= 3); // create + 2 inserts
            Assert.True(c.Commands.Count >= 2); // create + 1 insert
        }

        [Fact]
        public async Task UsingAsyncNullConnectionThrows()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            InjectTestConn(gl, new SpyConnection());
            await Assert.ThrowsAsync<ArgumentNullException>(
                () => gl.UsingAsync(null, _ => Task.CompletedTask));
        }

        [Fact]
        public async Task UsingAsyncGenericReturnsValue()
        {
            var spy = new SpyConnection();
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            InjectTestConn(gl, new SpyConnection());

            var result = await gl.UsingAsync<int>(spy, async _ =>
            {
                await Task.Yield();
                return 42;
            });
            Assert.Equal(42, result);
        }

        [Fact]
        public async Task UsingAsyncScopeIsPerInstance()
        {
            // Regression: _scopedConnection used to be static, so opening a scope on
            // gl1 would leak into gl2 and hijack wrapper calls on the second handle.
            // With an instance-scoped AsyncLocal, gl2 must ignore gl1's scope entirely.
            var gl1Default = new SpyConnection();
            var gl1Scoped = new SpyConnection();
            var gl2Default = new SpyConnection();

            var gl1 = GL.CreateForTest("postgresql://localhost:5432/mydb");
            InjectTestConn(gl1, gl1Default);

            var gl2 = GL.CreateForTest("postgresql://localhost:5432/mydb");
            InjectTestConn(gl2, gl2Default);

            await gl1.UsingAsync(gl1Scoped, async _ =>
            {
                // Inside gl1's scope — a wrapper call on gl2 must hit gl2's default,
                // NOT gl1Scoped (which would happen if the scope field were static).
                await gl2.DocInsertAsync("events", "{\"n\":1}");
            });

            // gl1's scoped conn saw no traffic — gl2 correctly ignored it.
            Assert.Empty(gl1Scoped.Commands);
            // gl1's default also untouched — nothing ran through gl1 at all.
            Assert.Empty(gl1Default.Commands);
            // gl2 routed to its own default (CREATE TABLE + INSERT).
            Assert.Equal(2, gl2Default.Commands.Count);
            Assert.Contains("INSERT INTO events", gl2Default.Commands[1].CommandText);
        }

        [Fact(DisplayName = "UsingAsync scope does not leak across sibling Task.WhenAll tasks")]
        public async Task UsingAsyncScopeDoesNotLeakToSiblingTask()
        {
            // Regression: if UsingAsync stored scope in a shared instance field
            // (e.g. `this._scopeConn = connection`) instead of `AsyncLocal<T>`,
            // a wrapper call on a *sibling* Task running concurrently with an
            // in-flight UsingAsync block would observe the scoped connection
            // and misroute its SQL. AsyncLocal<T> is THE .NET primitive for
            // async-flow-local state — this test pins it down.
            //
            // Ruby's equivalent (test_async_native.rb::test_using_scope_under_async_reactor)
            // revealed a real bug once in the Ruby wrapper. The .NET wrapper
            // has always used AsyncLocal<DbConnection>, but we want explicit
            // coverage so a future refactor can't regress silently.
            //
            // Deterministic sync via TaskCompletionSource (NO Task.Delay).

            var spyDefault = new SpyConnection();
            var spyScoped = new SpyConnection();

            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            InjectTestConn(gl, spyDefault);

            var enterUsing = new TaskCompletionSource();
            var bFinished = new TaskCompletionSource();

            async Task TaskA()
            {
                await gl.UsingAsync(spyScoped, async scoped =>
                {
                    // Signal B it's safe to run — we're inside the scope.
                    enterUsing.SetResult();
                    // Hold the scope open until B has observed its own routing.
                    await bFinished.Task;
                    // Still inside using: a call on `scoped` must go to spyScoped.
                    await scoped.DocInsertAsync("scoped_events", "{\"from\":\"A\"}");
                });
            }

            async Task TaskB()
            {
                // Wait until A is definitely inside its UsingAsync block.
                await enterUsing.Task;
                try
                {
                    // Sibling task — must NOT see A's scope. This call must
                    // route to spyDefault. If UsingAsync used instance state,
                    // this would route to spyScoped and the assertions below
                    // on spyScoped.Commands / spyDefault.Commands would flip.
                    await gl.DocInsertAsync("sibling_events", "{\"from\":\"B\"}");
                }
                finally
                {
                    // Always release A — even on failure — so the test can
                    // complete and the WhenAll won't hang indefinitely.
                    bFinished.SetResult();
                }
            }

            await Task.WhenAll(TaskA(), TaskB());

            // B's insert landed on spyDefault — scope did NOT leak.
            Assert.Contains(spyDefault.Commands,
                c => c.CommandText.Contains("INSERT INTO sibling_events"));
            Assert.DoesNotContain(spyScoped.Commands,
                c => c.CommandText.Contains("INSERT INTO sibling_events"));

            // A's insert (issued while still inside UsingAsync) landed on spyScoped —
            // scope held on A's own async flow across the TCS await.
            Assert.Contains(spyScoped.Commands,
                c => c.CommandText.Contains("INSERT INTO scoped_events"));
            Assert.DoesNotContain(spyDefault.Commands,
                c => c.CommandText.Contains("INSERT INTO scoped_events"));
        }
    }

    // ── v0.2.0 — ResolveActive fail-fast ──────────────────────────

    public class ResolveActiveTest
    {
        [Fact]
        public async Task WrapperWithoutConnectionThrows()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            // No _testConn injected, no internal _conn, no scope.
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(
                () => gl.DocInsertAsync("x", "{}"));
            Assert.Contains("No connection available", ex.Message);
        }
    }
}
