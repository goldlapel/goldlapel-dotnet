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
        /// Pre-populate the instance's DDL cache with a doc-store entry whose
        /// canonical table is the user-facing collection name. SQL-shape
        /// tests assert against literal collection names (e.g.
        /// <c>"INSERT INTO users"</c>), so we install <c>main = users</c>
        /// instead of the production <c>main = _goldlapel.doc_users</c>.
        /// Any namespace verb on <c>gl.Documents</c> will short-circuit
        /// the DDL HTTP fetch when the cache has an entry for the key.
        /// </summary>
        public static void InjectDocPatterns(GL gl, string collection)
        {
            var entry = new DdlEntry
            {
                Tables = new Dictionary<string, string> { ["main"] = collection },
                QueryPatterns = new Dictionary<string, string>(),
            };
            var cacheField = typeof(GL).GetField("_ddlCache", BindingFlags.NonPublic | BindingFlags.Instance);
            var cache = (System.Collections.Concurrent.ConcurrentDictionary<string, DdlEntry>) cacheField.GetValue(gl);
            cache["doc_store:" + collection] = entry;
        }

        /// <summary>
        /// Generic seed for a Phase 5 family DDL cache entry. Each family
        /// (counter / zset / hash / queue / geo) caches by key
        /// <c>"&lt;family&gt;:&lt;name&gt;"</c>; the namespace verbs short-circuit
        /// the HTTP fetch when the cache has an entry, so SQL-shape tests run
        /// without spawning the proxy.
        /// </summary>
        public static void InjectFamilyPatterns(GL gl, string family, string name,
            string mainTable, Dictionary<string, string> queryPatterns)
        {
            var entry = new DdlEntry
            {
                Tables = new Dictionary<string, string> { ["main"] = mainTable },
                QueryPatterns = queryPatterns,
            };
            var cacheField = typeof(GL).GetField("_ddlCache", BindingFlags.NonPublic | BindingFlags.Instance);
            var cache = (System.Collections.Concurrent.ConcurrentDictionary<string, DdlEntry>) cacheField.GetValue(gl);
            cache[family + ":" + name] = entry;
        }

        public static void InjectCounterPatterns(GL gl, string name)
        {
            var t = "_goldlapel.counter_" + name;
            InjectFamilyPatterns(gl, "counter", name, t, new Dictionary<string, string>
            {
                ["incr"]       = "INSERT INTO " + t + " (key, value, updated_at) VALUES ($1, $2, NOW()) ON CONFLICT (key) DO UPDATE SET value = " + t + ".value + EXCLUDED.value, updated_at = NOW() RETURNING value",
                ["set"]        = "INSERT INTO " + t + " (key, value, updated_at) VALUES ($1, $2, NOW()) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW() RETURNING value",
                ["get"]        = "SELECT value FROM " + t + " WHERE key = $1",
                ["delete"]     = "DELETE FROM " + t + " WHERE key = $1",
                ["delete_all"] = "DELETE FROM " + t,
                ["count_keys"] = "SELECT COUNT(*) FROM " + t,
            });
        }

        public static void InjectZsetPatterns(GL gl, string name)
        {
            var t = "_goldlapel.zset_" + name;
            InjectFamilyPatterns(gl, "zset", name, t, new Dictionary<string, string>
            {
                ["zadd"]          = "INSERT INTO " + t + " (zset_key, member, score) VALUES ($1, $2, $3) ON CONFLICT (zset_key, member) DO UPDATE SET score = EXCLUDED.score RETURNING score",
                ["zincrby"]       = "INSERT INTO " + t + " (zset_key, member, score) VALUES ($1, $2, $3) ON CONFLICT (zset_key, member) DO UPDATE SET score = " + t + ".score + EXCLUDED.score RETURNING score",
                ["zscore"]        = "SELECT score FROM " + t + " WHERE zset_key = $1 AND member = $2",
                ["zrem"]          = "DELETE FROM " + t + " WHERE zset_key = $1 AND member = $2",
                ["zrange_asc"]    = "SELECT member, score FROM " + t + " WHERE zset_key = $1 ORDER BY score ASC, member ASC LIMIT $2 OFFSET $3",
                ["zrange_desc"]   = "SELECT member, score FROM " + t + " WHERE zset_key = $1 ORDER BY score DESC, member DESC LIMIT $2 OFFSET $3",
                ["zrangebyscore"] = "SELECT member, score FROM " + t + " WHERE zset_key = $1 AND score >= $2 AND score <= $3 ORDER BY score ASC, member ASC LIMIT $4 OFFSET $5",
                ["zrank_asc"]     = "SELECT rank FROM ( SELECT member, ROW_NUMBER() OVER (ORDER BY score ASC, member ASC) - 1 AS rank FROM " + t + " WHERE zset_key = $1 ) ranked WHERE member = $2",
                ["zrank_desc"]    = "SELECT rank FROM ( SELECT member, ROW_NUMBER() OVER (ORDER BY score DESC, member DESC) - 1 AS rank FROM " + t + " WHERE zset_key = $1 ) ranked WHERE member = $2",
                ["zcard"]         = "SELECT COUNT(*) FROM " + t + " WHERE zset_key = $1",
                ["delete_key"]    = "DELETE FROM " + t + " WHERE zset_key = $1",
                ["delete_all"]    = "DELETE FROM " + t,
            });
        }

        public static void InjectHashPatterns(GL gl, string name)
        {
            var t = "_goldlapel.hash_" + name;
            InjectFamilyPatterns(gl, "hash", name, t, new Dictionary<string, string>
            {
                ["hset"]       = "INSERT INTO " + t + " (hash_key, field, value) VALUES ($1, $2, $3::jsonb) ON CONFLICT (hash_key, field) DO UPDATE SET value = EXCLUDED.value RETURNING value",
                ["hget"]       = "SELECT value FROM " + t + " WHERE hash_key = $1 AND field = $2",
                ["hgetall"]    = "SELECT field, value FROM " + t + " WHERE hash_key = $1 ORDER BY field",
                ["hkeys"]      = "SELECT field FROM " + t + " WHERE hash_key = $1 ORDER BY field",
                ["hvals"]      = "SELECT value FROM " + t + " WHERE hash_key = $1 ORDER BY field",
                ["hexists"]    = "SELECT EXISTS (SELECT 1 FROM " + t + " WHERE hash_key = $1 AND field = $2)",
                ["hdel"]       = "DELETE FROM " + t + " WHERE hash_key = $1 AND field = $2",
                ["hlen"]       = "SELECT COUNT(*) FROM " + t + " WHERE hash_key = $1",
                ["delete_key"] = "DELETE FROM " + t + " WHERE hash_key = $1",
                ["delete_all"] = "DELETE FROM " + t,
            });
        }

        public static void InjectQueuePatterns(GL gl, string name)
        {
            var t = "_goldlapel.queue_" + name;
            InjectFamilyPatterns(gl, "queue", name, t, new Dictionary<string, string>
            {
                ["enqueue"]       = "INSERT INTO " + t + " (payload) VALUES ($1::jsonb) RETURNING id, created_at",
                ["claim"]         = "WITH next_msg AS ( SELECT id FROM " + t + " WHERE status = 'ready' AND visible_at <= NOW() ORDER BY visible_at, id FOR UPDATE SKIP LOCKED LIMIT 1 ) UPDATE " + t + " SET status = 'claimed', visible_at = NOW() + INTERVAL '1 millisecond' * $1 FROM next_msg WHERE " + t + ".id = next_msg.id RETURNING " + t + ".id, " + t + ".payload, " + t + ".visible_at, " + t + ".created_at",
                ["ack"]           = "DELETE FROM " + t + " WHERE id = $1",
                ["extend"]        = "UPDATE " + t + " SET visible_at = visible_at + INTERVAL '1 millisecond' * $2 WHERE id = $1 AND status = 'claimed' RETURNING visible_at",
                ["nack"]          = "UPDATE " + t + " SET status = 'ready', visible_at = NOW() WHERE id = $1 AND status = 'claimed' RETURNING id",
                ["peek"]          = "SELECT id, payload, visible_at, status, created_at FROM " + t + " WHERE status = 'ready' AND visible_at <= NOW() ORDER BY visible_at, id LIMIT 1",
                ["count_ready"]   = "SELECT COUNT(*) FROM " + t + " WHERE status = 'ready' AND visible_at <= NOW()",
                ["count_claimed"] = "SELECT COUNT(*) FROM " + t + " WHERE status = 'claimed'",
                ["delete_all"]    = "DELETE FROM " + t,
            });
        }

        public static void InjectGeoPatterns(GL gl, string name)
        {
            var t = "_goldlapel.geo_" + name;
            // Proxy v1 SQL: $N indices match the canonical contract — every $N
            // appears at most once for georadius (CTE-anchor); geosearch_member
            // uses $1, $2 (both = anchor member), $3 = radius_m, $4 = limit.
            InjectFamilyPatterns(gl, "geo", name, t, new Dictionary<string, string>
            {
                ["geoadd"]               = "INSERT INTO " + t + " (member, location, updated_at) VALUES ($1, ST_SetSRID(ST_MakePoint($2, $3), 4326)::geography, NOW()) ON CONFLICT (member) DO UPDATE SET location = EXCLUDED.location, updated_at = NOW() RETURNING ST_X(location::geometry) AS lon, ST_Y(location::geometry) AS lat",
                ["geopos"]               = "SELECT ST_X(location::geometry) AS lon, ST_Y(location::geometry) AS lat FROM " + t + " WHERE member = $1",
                ["geodist"]              = "SELECT ST_Distance(a.location, b.location) AS distance_m FROM " + t + " a, " + t + " b WHERE a.member = $1 AND b.member = $2",
                ["georadius"]            = "WITH anchor AS ( SELECT ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography AS geog ) SELECT member, ST_X(location::geometry) AS lon, ST_Y(location::geometry) AS lat FROM " + t + ", anchor WHERE ST_DWithin(location, anchor.geog, $3) ORDER BY ST_Distance(location, anchor.geog) LIMIT $4",
                ["georadius_with_dist"]  = "WITH anchor AS ( SELECT ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography AS geog ) SELECT member, ST_X(location::geometry) AS lon, ST_Y(location::geometry) AS lat, ST_Distance(location, anchor.geog) AS distance_m FROM " + t + ", anchor WHERE ST_DWithin(location, anchor.geog, $3) ORDER BY distance_m LIMIT $4",
                ["geosearch_member"]     = "SELECT b.member, ST_X(b.location::geometry) AS lon, ST_Y(b.location::geometry) AS lat, ST_Distance(b.location, a.location) AS distance_m FROM " + t + " a, " + t + " b WHERE a.member = $1 AND ST_DWithin(b.location, a.location, $3) AND b.member <> $2 ORDER BY distance_m LIMIT $4",
                ["geo_remove"]           = "DELETE FROM " + t + " WHERE member = $1",
                ["geo_count"]            = "SELECT COUNT(*) FROM " + t,
                ["delete_all"]           = "DELETE FROM " + t,
            });
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
            // Pre-populate the DDL cache so gl.Documents.* methods skip the
            // HTTP round-trip to the dashboard. Tests assert SQL shape, not
            // proxy IO.
            foreach (var c in new[] { "users", "items", "orders" })
                TestHelpers.InjectDocPatterns(_gl, c);
        }

        [Fact]
        public async Task DocInsertAsyncDelegates()
        {
            await _gl.Documents.InsertAsync("users", "{\"name\":\"alice\"}");

            // Proxy owns DDL — only the INSERT runs through the wrapper.
            Assert.Single(_spy.Commands);
            Assert.Contains("INSERT INTO users", _spy.Commands[0].CommandText);
            Assert.Equal("{\"name\":\"alice\"}", _spy.Commands[0].ParamValue("@doc"));
        }

        [Fact]
        public async Task DocInsertManyAsyncDelegates()
        {
            var docs = new List<string> { "{\"a\":1}", "{\"b\":2}" };
            await _gl.Documents.InsertManyAsync("items", docs);

            // Proxy owns DDL — 2 inserts only (no leading CREATE TABLE).
            Assert.Equal(2, _spy.Commands.Count);
        }

        [Fact]
        public async Task DocFindAsyncDelegates()
        {
            await _gl.Documents.FindAsync("users", filterJson: "{\"active\":true}");

            var sql = _spy.LastCommandText;
            Assert.Contains("SELECT _id, data, created_at, updated_at FROM users", sql);
            Assert.Contains("WHERE data @> @p0::jsonb", sql);
            Assert.Equal("{\"active\":true}", _spy.LastCommand.ParamValue("@p0"));
        }

        [Fact]
        public async Task DocFindCursorDelegates()
        {
            // FindCursorAsync now awaits the DDL fetch up-front, then returns
            // an IEnumerable to stream batches. Iterate to issue SQL.
            TestHelpers.InjectDocPatterns(_gl, "users");
            foreach (var _ in await _gl.Documents.FindCursorAsync("users")) { }

            var sqls = _spy.Commands.Select(c => c.CommandText).ToList();
            Assert.Equal("BEGIN", sqls[0]);
            Assert.Contains("CURSOR FOR", sqls[1]);
            Assert.Contains("SELECT _id, data, created_at, updated_at FROM users", sqls[1]);
        }

        [Fact]
        public async Task DocFindOneAsyncDelegates()
        {
            await _gl.Documents.FindOneAsync("users", filterJson: "{\"id\":1}");

            var sql = _spy.LastCommandText;
            Assert.Contains("FROM users", sql);
            Assert.Contains("LIMIT 1", sql);
        }

        [Fact]
        public async Task DocUpdateAsyncDelegates()
        {
            await _gl.Documents.UpdateAsync("users", "{\"active\":true}", "{\"role\":\"admin\"}");

            var sql = _spy.LastCommandText;
            Assert.Contains("UPDATE users", sql);
            Assert.Contains("SET data = data || @p0::jsonb", sql);
        }

        [Fact]
        public async Task DocDeleteAsyncDelegates()
        {
            await _gl.Documents.DeleteAsync("users", "{\"active\":false}");

            var sql = _spy.LastCommandText;
            Assert.Contains("DELETE FROM users", sql);
            Assert.Contains("WHERE data @> @p0::jsonb", sql);
        }

        [Fact]
        public async Task DocCountAsyncDelegates()
        {
            _spy.NextScalarResult = 42L;
            await _gl.Documents.CountAsync("users");

            Assert.Contains("SELECT COUNT(*) FROM users", _spy.LastCommandText);
        }

        [Fact]
        public async Task DocCreateIndexAsyncDelegates()
        {
            await _gl.Documents.CreateIndexAsync("users");

            // Proxy owns DDL — only the CREATE INDEX runs through the wrapper.
            Assert.Single(_spy.Commands);
            Assert.Contains("CREATE INDEX IF NOT EXISTS idx_users_data_gin", _spy.Commands[0].CommandText);
        }

        [Fact]
        public async Task DocAggregateAsyncDelegates()
        {
            await _gl.Documents.AggregateAsync("orders",
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

    /// <summary>
    /// Pub/sub flat methods stay on the parent client (no DDL family in
    /// Phase 5). The redis-compat queue / counter / hash / zset / geo flat
    /// methods were retired in Phase 5 — see the per-family namespace tests
    /// (CountersNamespaceTests / ZsetsNamespaceTests / HashesNamespaceTests /
    /// QueuesNamespaceTests / GeosNamespaceTests).
    /// </summary>
    public class InstancePubSubMethodsTest
    {
        private readonly GL _gl;
        private readonly SpyConnection _spy;

        public InstancePubSubMethodsTest()
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
            var id = await _gl.Streams.AddAsync("events", "{\"type\":\"click\"}");

            Assert.Equal(1L, id);
            Assert.Contains("INSERT INTO _goldlapel.stream_events", _spy.LastCommandText);
            // No in-wrapper CREATE TABLE — proxy owns DDL.
            Assert.DoesNotContain("CREATE TABLE", _spy.LastCommandText);
        }

        [Fact]
        public async Task StreamCreateGroupAsyncDelegates()
        {
            TestHelpers.InjectStreamPatterns(_gl, "events");
            await _gl.Streams.CreateGroupAsync("events", "workers");

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
            await _gl.Streams.ReadAsync("events", "workers", "w1");

            Assert.Contains("last_delivered_id FROM _goldlapel.stream_events_groups", _spy.LastCommandText);
        }

        [Fact]
        public async Task StreamAckAsyncDelegates()
        {
            TestHelpers.InjectStreamPatterns(_gl, "events");
            await _gl.Streams.AckAsync("events", "workers", 1);

            Assert.Contains("DELETE FROM _goldlapel.stream_events_pending", _spy.LastCommandText);
        }

        [Fact]
        public async Task StreamClaimAsyncDelegates()
        {
            TestHelpers.InjectStreamPatterns(_gl, "events");
            await _gl.Streams.ClaimAsync("events", "workers", "w2");

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
            // Same DDL-cache pre-seeding as InstanceDocMethodsTest — every
            // `gl.Documents.<verb>Async` consults the cache first.
            foreach (var c in new[] { "events", "sessions", "logs" })
                TestHelpers.InjectDocPatterns(_gl, c);
        }

        [Fact]
        public async Task DocWatchAsyncDelegates()
        {
            // DocWatch creates trigger DDL then tries to LISTEN (which fails on SpyConnection).
            try { await _gl.Documents.WatchAsync("events", (ch, msg) => { }); }
            catch (Exception) { }

            var sqls = _spy.Commands.Select(c => c.CommandText).ToList();
            Assert.True(sqls.Any(s => s.Contains("CREATE OR REPLACE FUNCTION _gl_watch_events()")));
            Assert.True(sqls.Any(s => s.Contains("AFTER INSERT OR UPDATE OR DELETE ON events")));
        }

        [Fact]
        public async Task DocUnwatchAsyncDelegates()
        {
            await _gl.Documents.UnwatchAsync("events");

            var sqls = _spy.Commands.Select(c => c.CommandText).ToList();
            Assert.Contains("DROP TRIGGER IF EXISTS _gl_watch_events_trigger ON events", sqls);
            Assert.Contains("DROP FUNCTION IF EXISTS _gl_watch_events()", sqls);
        }

        [Fact]
        public async Task DocCreateTtlIndexAsyncDelegates()
        {
            await _gl.Documents.CreateTtlIndexAsync("sessions", 3600);

            var sqls = _spy.Commands.Select(c => c.CommandText).ToList();
            Assert.True(sqls.Any(s => s.Contains("CREATE INDEX IF NOT EXISTS idx_sessions_ttl")));
            Assert.True(sqls.Any(s => s.Contains("INTERVAL '3600 seconds'")));
        }

        [Fact]
        public async Task DocRemoveTtlIndexAsyncDelegates()
        {
            await _gl.Documents.RemoveTtlIndexAsync("sessions");

            var sqls = _spy.Commands.Select(c => c.CommandText).ToList();
            Assert.Contains("DROP TRIGGER IF EXISTS _gl_ttl_sessions_trigger ON sessions", sqls);
            Assert.Contains("DROP FUNCTION IF EXISTS _gl_ttl_sessions()", sqls);
            Assert.Contains("DROP INDEX IF EXISTS idx_sessions_ttl", sqls);
        }

        [Fact]
        public async Task DocCreateCappedAsyncDelegates()
        {
            await _gl.Documents.CreateCappedAsync("logs", 1000);

            var sqls = _spy.Commands.Select(c => c.CommandText).ToList();
            // Proxy owns the table — wrapper drives only the supporting
            // index + trigger / function. Index name uses the canonical
            // bare-table form (idx_<table>_<suffix>).
            Assert.True(sqls.Any(s => s.Contains("CREATE INDEX IF NOT EXISTS idx_logs_created_at")));
            Assert.True(sqls.Any(s => s.Contains("CREATE OR REPLACE FUNCTION _gl_cap_logs()")));
            Assert.True(sqls.Any(s => s.Contains("COUNT(*) - 1000")));
            Assert.False(sqls.Any(s => s.Contains("CREATE TABLE")));
        }

        [Fact]
        public async Task DocRemoveCapAsyncDelegates()
        {
            await _gl.Documents.RemoveCapAsync("logs");

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

        // Pre-seed the DDL cache for any collection name we reach for in
        // these tests so `gl.Documents.<verb>Async` skips the HTTP fetch.
        // Without this, the proxy-fetch path tries port 7933 and fails.
        private static GL MakeGlWithDocPatterns(params string[] collections)
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            foreach (var c in collections)
                TestHelpers.InjectDocPatterns(gl, c);
            return gl;
        }

        [Fact]
        public async Task UsingAsyncOverridesConnection()
        {
            var spyDefault = new SpyConnection();
            var spyScoped = new SpyConnection();

            var gl = MakeGlWithDocPatterns("events");
            InjectTestConn(gl, spyDefault);

            await gl.UsingAsync(spyScoped, async scoped =>
            {
                await scoped.Documents.InsertAsync("events", "{\"type\":\"x\"}");
            });

            Assert.Empty(spyDefault.Commands);
            // Proxy owns DDL — only the INSERT runs through the wrapper.
            Assert.Single(spyScoped.Commands);
            Assert.Contains("INSERT INTO events", spyScoped.Commands[0].CommandText);
        }

        [Fact]
        public async Task UsingAsyncScopeUnwindsOnException()
        {
            var spyDefault = new SpyConnection();
            var spyScoped = new SpyConnection();

            var gl = MakeGlWithDocPatterns("users");
            InjectTestConn(gl, spyDefault);

            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await gl.UsingAsync(spyScoped, _ =>
                {
                    throw new InvalidOperationException("boom");
                });
            });

            // After UsingAsync returns, scope unwound — subsequent calls hit default.
            await gl.Documents.InsertAsync("users", "{\"name\":\"a\"}");
            // Proxy owns DDL — single INSERT.
            Assert.Single(spyDefault.Commands);
        }

        [Fact]
        public async Task UsingAsyncHoldsScopeAcrossAwaits()
        {
            var spyDefault = new SpyConnection();
            var spyScoped = new SpyConnection();

            var gl = MakeGlWithDocPatterns("events");
            InjectTestConn(gl, spyDefault);

            await gl.UsingAsync(spyScoped, async scoped =>
            {
                await scoped.Documents.InsertAsync("events", "{\"n\":1}");
                await Task.Yield();            // force an await boundary
                await Task.Delay(1);
                await scoped.Documents.InsertAsync("events", "{\"n\":2}");
            });

            Assert.Empty(spyDefault.Commands);
            // 2 inserts land on the scoped spy (proxy owns DDL).
            Assert.Equal(2, spyScoped.Commands.Count);
        }

        [Fact]
        public async Task UsingAsyncNestsProperly()
        {
            var a = new SpyConnection();
            var b = new SpyConnection();
            var c = new SpyConnection();

            var gl = MakeGlWithDocPatterns("x");
            InjectTestConn(gl, a);

            await gl.UsingAsync(b, async gl2 =>
            {
                await gl2.Documents.InsertAsync("x", "{}");
                await gl2.UsingAsync(c, async gl3 =>
                {
                    await gl3.Documents.InsertAsync("x", "{}");
                });
                await gl2.Documents.InsertAsync("x", "{}");
            });
            await gl.Documents.InsertAsync("x", "{}");

            // Proxy owns DDL — counts are now just inserts.
            Assert.Single(a.Commands);     // 1 insert (the trailing call on gl)
            Assert.Equal(2, b.Commands.Count); // 2 inserts inside gl.UsingAsync(b, ...)
            Assert.Single(c.Commands);     // 1 insert in the inner UsingAsync(c, ...)
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

            var gl1 = MakeGlWithDocPatterns("events");
            InjectTestConn(gl1, gl1Default);

            var gl2 = MakeGlWithDocPatterns("events");
            InjectTestConn(gl2, gl2Default);

            await gl1.UsingAsync(gl1Scoped, async _ =>
            {
                // Inside gl1's scope — a wrapper call on gl2 must hit gl2's default,
                // NOT gl1Scoped (which would happen if the scope field were static).
                await gl2.Documents.InsertAsync("events", "{\"n\":1}");
            });

            // gl1's scoped conn saw no traffic — gl2 correctly ignored it.
            Assert.Empty(gl1Scoped.Commands);
            // gl1's default also untouched — nothing ran through gl1 at all.
            Assert.Empty(gl1Default.Commands);
            // gl2 routed to its own default — proxy owns DDL, single INSERT.
            Assert.Single(gl2Default.Commands);
            Assert.Contains("INSERT INTO events", gl2Default.Commands[0].CommandText);
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

            var gl = MakeGlWithDocPatterns("scoped_events", "sibling_events");
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
                    await scoped.Documents.InsertAsync("scoped_events", "{\"from\":\"A\"}");
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
                    await gl.Documents.InsertAsync("sibling_events", "{\"from\":\"B\"}");
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
            // Seed the DDL cache so the patterns fetch is bypassed; we want
            // ResolveActive itself to be the failing step.
            TestHelpers.InjectDocPatterns(gl, "x");
            // No _testConn injected, no internal _conn, no scope.
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(
                () => gl.Documents.InsertAsync("x", "{}"));
            Assert.Contains("No connection available", ex.Message);
        }
    }
}
