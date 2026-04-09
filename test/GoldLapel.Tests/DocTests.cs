using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace GoldLapel.Tests
{
    // ── DocInsert ───────────────────────────────────────────────

    public class DocInsertTest
    {
        [Fact]
        public void CreatesTableThenInserts()
        {
            var conn = new SpyConnection();
            Utils.DocInsert(conn, "users", "{\"name\":\"alice\"}");

            Assert.Equal(2, conn.Commands.Count);
            Assert.Contains("CREATE TABLE IF NOT EXISTS users", conn.Commands[0].CommandText);
            Assert.Contains("BIGSERIAL PRIMARY KEY", conn.Commands[0].CommandText);
            Assert.Contains("data JSONB NOT NULL", conn.Commands[0].CommandText);
            Assert.Contains("created_at TIMESTAMPTZ", conn.Commands[0].CommandText);
            Assert.Contains("updated_at TIMESTAMPTZ", conn.Commands[0].CommandText);
        }

        [Fact]
        public void InsertSqlAndParams()
        {
            var conn = new SpyConnection();
            Utils.DocInsert(conn, "users", "{\"name\":\"alice\"}");

            var cmd = conn.Commands[1];
            Assert.Contains("INSERT INTO users", cmd.CommandText);
            Assert.Contains("VALUES (@doc::jsonb)", cmd.CommandText);
            Assert.Contains("RETURNING id, data, created_at, updated_at", cmd.CommandText);
            Assert.Equal("{\"name\":\"alice\"}", cmd.ParamValue("@doc"));
        }

        [Fact]
        public void InvalidCollectionThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.DocInsert(conn, "bad table!", "{\"x\":1}"));
        }
    }

    // ── DocInsertMany ──────────────────────────────────────────

    public class DocInsertManyTest
    {
        [Fact]
        public void InsertsMultipleDocuments()
        {
            var conn = new SpyConnection();
            var docs = new List<string> { "{\"a\":1}", "{\"b\":2}", "{\"c\":3}" };
            Utils.DocInsertMany(conn, "items", docs);

            // 1 create table + 3 inserts
            Assert.Equal(4, conn.Commands.Count);
        }

        [Fact]
        public void EachInsertHasCorrectParam()
        {
            var conn = new SpyConnection();
            var docs = new List<string> { "{\"x\":1}", "{\"y\":2}" };
            Utils.DocInsertMany(conn, "items", docs);

            // Commands[0] = CREATE TABLE, Commands[1] = first insert, Commands[2] = second insert
            Assert.Equal("{\"x\":1}", conn.Commands[1].ParamValue("@doc"));
            Assert.Equal("{\"y\":2}", conn.Commands[2].ParamValue("@doc"));
        }

        [Fact]
        public void EmptyListOnlyCreatesTable()
        {
            var conn = new SpyConnection();
            Utils.DocInsertMany(conn, "items", new List<string>());

            Assert.Single(conn.Commands);
            Assert.Contains("CREATE TABLE IF NOT EXISTS items", conn.Commands[0].CommandText);
        }

        [Fact]
        public void InvalidCollectionThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.DocInsertMany(conn, "1bad", new List<string> { "{}" }));
        }
    }

    // ── DocFind ────────────────────────────────────────────────

    public class DocFindTest
    {
        [Fact]
        public void BasicFindSql()
        {
            var conn = new SpyConnection();
            Utils.DocFind(conn, "users");

            var sql = conn.LastCommandText;
            Assert.Contains("SELECT id, data, created_at, updated_at FROM users", sql);
            Assert.DoesNotContain("WHERE", sql);
            Assert.DoesNotContain("ORDER BY", sql);
            Assert.DoesNotContain("LIMIT", sql);
            Assert.DoesNotContain("OFFSET", sql);
        }

        [Fact]
        public void WithFilter()
        {
            var conn = new SpyConnection();
            Utils.DocFind(conn, "users", filterJson: "{\"active\":true}");

            var sql = conn.LastCommandText;
            Assert.Contains("WHERE data @> @p0::jsonb", sql);
            Assert.Equal("{\"active\":true}", conn.LastCommand.ParamValue("@p0"));
        }

        [Fact]
        public void WithSort()
        {
            var conn = new SpyConnection();
            Utils.DocFind(conn, "users", sort: new Dictionary<string, int> { { "name", 1 } });

            var sql = conn.LastCommandText;
            Assert.Contains("ORDER BY data->>'name' ASC", sql);
        }

        [Fact]
        public void WithSortDescending()
        {
            var conn = new SpyConnection();
            Utils.DocFind(conn, "users", sort: new Dictionary<string, int> { { "age", -1 } });

            var sql = conn.LastCommandText;
            Assert.Contains("ORDER BY data->>'age' DESC", sql);
        }

        [Fact]
        public void WithLimit()
        {
            var conn = new SpyConnection();
            Utils.DocFind(conn, "users", limit: 10);

            var sql = conn.LastCommandText;
            Assert.Contains("LIMIT @limit", sql);
            Assert.Equal(10, conn.LastCommand.ParamValue("@limit"));
        }

        [Fact]
        public void WithSkip()
        {
            var conn = new SpyConnection();
            Utils.DocFind(conn, "users", skip: 5);

            var sql = conn.LastCommandText;
            Assert.Contains("OFFSET @skip", sql);
            Assert.Equal(5, conn.LastCommand.ParamValue("@skip"));
        }

        [Fact]
        public void AllOptionsCombined()
        {
            var conn = new SpyConnection();
            Utils.DocFind(conn, "posts",
                filterJson: "{\"status\":\"published\"}",
                sort: new Dictionary<string, int> { { "date", -1 } },
                limit: 20,
                skip: 10);

            var sql = conn.LastCommandText;
            Assert.Contains("FROM posts", sql);
            Assert.Contains("WHERE data @> @p0::jsonb", sql);
            Assert.Contains("ORDER BY data->>'date' DESC", sql);
            Assert.Contains("LIMIT @limit", sql);
            Assert.Contains("OFFSET @skip", sql);

            Assert.Equal("{\"status\":\"published\"}", conn.LastCommand.ParamValue("@p0"));
            Assert.Equal(20, conn.LastCommand.ParamValue("@limit"));
            Assert.Equal(10, conn.LastCommand.ParamValue("@skip"));
        }

        [Fact]
        public void InvalidCollectionThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.DocFind(conn, "drop table--"));
        }

        [Fact]
        public void InvalidSortKeyThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.DocFind(conn, "users",
                    sort: new Dictionary<string, int> { { "bad key!", 1 } }));
        }
    }

    // ── DocFindOne ─────────────────────────────────────────────

    public class DocFindOneTest
    {
        [Fact]
        public void BasicSql()
        {
            var conn = new SpyConnection();
            Utils.DocFindOne(conn, "users");

            var sql = conn.LastCommandText;
            Assert.Contains("SELECT id, data, created_at, updated_at FROM users", sql);
            Assert.Contains("LIMIT 1", sql);
            Assert.DoesNotContain("WHERE", sql);
        }

        [Fact]
        public void WithFilter()
        {
            var conn = new SpyConnection();
            Utils.DocFindOne(conn, "users", filterJson: "{\"email\":\"a@b.com\"}");

            var sql = conn.LastCommandText;
            Assert.Contains("WHERE data @> @p0::jsonb", sql);
            Assert.Contains("LIMIT 1", sql);
            Assert.Equal("{\"email\":\"a@b.com\"}", conn.LastCommand.ParamValue("@p0"));
        }

        [Fact]
        public void InvalidCollectionThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.DocFindOne(conn, "bad;name"));
        }
    }

    // ── DocUpdate ──────────────────────────────────────────────

    public class DocUpdateTest
    {
        [Fact]
        public void SqlGeneration()
        {
            var conn = new SpyConnection();
            Utils.DocUpdate(conn, "users", "{\"active\":true}", "{\"role\":\"admin\"}");

            var sql = conn.LastCommandText;
            Assert.Contains("UPDATE users", sql);
            Assert.Contains("SET data = data || @p0::jsonb", sql);
            Assert.Contains("updated_at = NOW()", sql);
            Assert.Contains("WHERE data @> @p1::jsonb", sql);
        }

        [Fact]
        public void Parameters()
        {
            var conn = new SpyConnection();
            Utils.DocUpdate(conn, "users", "{\"active\":true}", "{\"role\":\"admin\"}");

            Assert.Equal("{\"role\":\"admin\"}", conn.LastCommand.ParamValue("@p0"));
            Assert.Equal("{\"active\":true}", conn.LastCommand.ParamValue("@p1"));
        }

        [Fact]
        public void InvalidCollectionThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.DocUpdate(conn, "bad name", "{}", "{}"));
        }
    }

    // ── DocUpdateOne ───────────────────────────────────────────

    public class DocUpdateOneTest
    {
        [Fact]
        public void SqlGeneration()
        {
            var conn = new SpyConnection();
            Utils.DocUpdateOne(conn, "users", "{\"name\":\"alice\"}", "{\"age\":30}");

            var sql = conn.LastCommandText;
            Assert.Contains("UPDATE users", sql);
            Assert.Contains("SET data = data || @p1::jsonb", sql);
            Assert.Contains("updated_at = NOW()", sql);
            Assert.Contains("WHERE id = (SELECT id FROM users WHERE data @> @p0::jsonb LIMIT 1)", sql);
        }

        [Fact]
        public void Parameters()
        {
            var conn = new SpyConnection();
            Utils.DocUpdateOne(conn, "users", "{\"name\":\"alice\"}", "{\"age\":30}");

            Assert.Equal("{\"age\":30}", conn.LastCommand.ParamValue("@p1"));
            Assert.Equal("{\"name\":\"alice\"}", conn.LastCommand.ParamValue("@p0"));
        }

        [Fact]
        public void InvalidCollectionThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.DocUpdateOne(conn, "123bad", "{}", "{}"));
        }
    }

    // ── DocDelete ──────────────────────────────────────────────

    public class DocDeleteTest
    {
        [Fact]
        public void SqlGeneration()
        {
            var conn = new SpyConnection();
            Utils.DocDelete(conn, "users", "{\"active\":false}");

            var sql = conn.LastCommandText;
            Assert.Contains("DELETE FROM users", sql);
            Assert.Contains("WHERE data @> @p0::jsonb", sql);
        }

        [Fact]
        public void Parameters()
        {
            var conn = new SpyConnection();
            Utils.DocDelete(conn, "users", "{\"active\":false}");

            Assert.Equal("{\"active\":false}", conn.LastCommand.ParamValue("@p0"));
        }

        [Fact]
        public void InvalidCollectionThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.DocDelete(conn, "bad table", "{}"));
        }
    }

    // ── DocDeleteOne ───────────────────────────────────────────

    public class DocDeleteOneTest
    {
        [Fact]
        public void SqlGeneration()
        {
            var conn = new SpyConnection();
            Utils.DocDeleteOne(conn, "users", "{\"name\":\"alice\"}");

            var sql = conn.LastCommandText;
            Assert.Contains("DELETE FROM users", sql);
            Assert.Contains("WHERE id = (", sql);
            Assert.Contains("SELECT id FROM users WHERE data @> @p0::jsonb LIMIT 1)", sql);
        }

        [Fact]
        public void Parameters()
        {
            var conn = new SpyConnection();
            Utils.DocDeleteOne(conn, "users", "{\"name\":\"alice\"}");

            Assert.Equal("{\"name\":\"alice\"}", conn.LastCommand.ParamValue("@p0"));
        }

        [Fact]
        public void InvalidCollectionThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.DocDeleteOne(conn, "x;y", "{}"));
        }
    }

    // ── DocCount ───────────────────────────────────────────────

    public class DocCountTest
    {
        [Fact]
        public void CountAllSql()
        {
            var conn = new SpyConnection();
            // DocCount calls ExecuteScalar, SpyCommand returns null by default.
            // We need to set up the spy to return a value.
            conn.NextScalarResult = 42L;
            Utils.DocCount(conn, "users");

            var sql = conn.LastCommandText;
            Assert.Contains("SELECT COUNT(*) FROM users", sql);
            Assert.DoesNotContain("WHERE", sql);
        }

        [Fact]
        public void CountWithFilter()
        {
            var conn = new SpyConnection();
            conn.NextScalarResult = 5L;
            Utils.DocCount(conn, "users", filterJson: "{\"active\":true}");

            var sql = conn.LastCommandText;
            Assert.Contains("SELECT COUNT(*) FROM users", sql);
            Assert.Contains("WHERE data @> @p0::jsonb", sql);
            Assert.Equal("{\"active\":true}", conn.LastCommand.ParamValue("@p0"));
        }

        [Fact]
        public void InvalidCollectionThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.DocCount(conn, "bad!name"));
        }
    }

    // ── DocCreateIndex ─────────────────────────────────────────

    public class DocCreateIndexTest
    {
        [Fact]
        public void DefaultGinIndex()
        {
            var conn = new SpyConnection();
            Utils.DocCreateIndex(conn, "users");

            // Commands[0] = CREATE TABLE, Commands[1] = CREATE INDEX
            Assert.Equal(2, conn.Commands.Count);
            var indexCmd = conn.Commands[1];
            Assert.Contains("CREATE INDEX IF NOT EXISTS users_data_gin", indexCmd.CommandText);
            Assert.Contains("USING GIN (data)", indexCmd.CommandText);
        }

        [Fact]
        public void NullKeysCreatesGinIndex()
        {
            var conn = new SpyConnection();
            Utils.DocCreateIndex(conn, "users", null);

            var indexCmd = conn.Commands[1];
            Assert.Contains("USING GIN (data)", indexCmd.CommandText);
        }

        [Fact]
        public void EmptyKeysCreatesGinIndex()
        {
            var conn = new SpyConnection();
            Utils.DocCreateIndex(conn, "users", new List<string>());

            var indexCmd = conn.Commands[1];
            Assert.Contains("USING GIN (data)", indexCmd.CommandText);
        }

        [Fact]
        public void SingleKeyBtreeIndex()
        {
            var conn = new SpyConnection();
            Utils.DocCreateIndex(conn, "users", new List<string> { "email" });

            var indexCmd = conn.Commands[1];
            Assert.Contains("CREATE INDEX IF NOT EXISTS users_email_idx", indexCmd.CommandText);
            Assert.Contains("(data->>'email')", indexCmd.CommandText);
            Assert.DoesNotContain("GIN", indexCmd.CommandText);
        }

        [Fact]
        public void MultiKeyBtreeIndex()
        {
            var conn = new SpyConnection();
            Utils.DocCreateIndex(conn, "orders", new List<string> { "status", "date" });

            var indexCmd = conn.Commands[1];
            Assert.Contains("CREATE INDEX IF NOT EXISTS orders_status_date_idx", indexCmd.CommandText);
            Assert.Contains("(data->>'status'), (data->>'date')", indexCmd.CommandText);
        }

        [Fact]
        public void CreatesTableFirst()
        {
            var conn = new SpyConnection();
            Utils.DocCreateIndex(conn, "users");

            Assert.Contains("CREATE TABLE IF NOT EXISTS users", conn.Commands[0].CommandText);
        }

        [Fact]
        public void InvalidCollectionThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.DocCreateIndex(conn, "bad name"));
        }

        [Fact]
        public void InvalidKeyThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.DocCreateIndex(conn, "users", new List<string> { "bad key!" }));
        }
    }

    // ── DocAggregate ────────────────────────────────────────────

    public class DocAggregateTest
    {
        [Fact]
        public void FullPipeline()
        {
            var conn = new SpyConnection();
            Utils.DocAggregate(conn, "orders",
                "[{\"$match\": {\"status\":\"shipped\"}}, " +
                "{\"$group\": {\"_id\": \"$region\", \"total\": {\"$sum\": \"$amount\"}}}, " +
                "{\"$sort\": {\"total\": -1}}, " +
                "{\"$limit\": 10}, " +
                "{\"$skip\": 5}]");

            var sql = conn.LastCommandText;
            Assert.Contains("SELECT data->>'region' AS _id, SUM((data->>'amount')::numeric) AS total", sql);
            Assert.Contains("FROM orders", sql);
            Assert.Contains("WHERE data @> @p0::jsonb", sql);
            Assert.Contains("GROUP BY data->>'region'", sql);
            Assert.Contains("ORDER BY total DESC", sql);
            Assert.Contains("LIMIT @limit", sql);
            Assert.Contains("OFFSET @skip", sql);
            Assert.Equal("{\"status\":\"shipped\"}", conn.LastCommand.ParamValue("@p0"));
            Assert.Equal(10, conn.LastCommand.ParamValue("@limit"));
            Assert.Equal(5, conn.LastCommand.ParamValue("@skip"));
        }

        [Fact]
        public void Accumulators()
        {
            var conn = new SpyConnection();
            Utils.DocAggregate(conn, "orders",
                "[{\"$group\": {" +
                "\"_id\": \"$category\", " +
                "\"cnt\": {\"$sum\": 1}, " +
                "\"total\": {\"$sum\": \"$price\"}, " +
                "\"mean\": {\"$avg\": \"$price\"}, " +
                "\"lo\": {\"$min\": \"$price\"}, " +
                "\"hi\": {\"$max\": \"$price\"}}}]");

            var sql = conn.LastCommandText;
            Assert.Contains("COUNT(*) AS cnt", sql);
            Assert.Contains("SUM((data->>'price')::numeric) AS total", sql);
            Assert.Contains("AVG((data->>'price')::numeric) AS mean", sql);
            Assert.Contains("MIN((data->>'price')::numeric) AS lo", sql);
            Assert.Contains("MAX((data->>'price')::numeric) AS hi", sql);
            Assert.Contains("GROUP BY data->>'category'", sql);
        }

        [Fact]
        public void NullGroupId()
        {
            var conn = new SpyConnection();
            Utils.DocAggregate(conn, "orders",
                "[{\"$group\": {\"_id\": null, \"total\": {\"$sum\": \"$amount\"}}}]");

            var sql = conn.LastCommandText;
            Assert.Contains("SUM((data->>'amount')::numeric) AS total", sql);
            Assert.DoesNotContain("GROUP BY", sql);
            Assert.DoesNotContain("AS _id", sql);
        }

        [Fact]
        public void MatchOnly()
        {
            var conn = new SpyConnection();
            Utils.DocAggregate(conn, "users",
                "[{\"$match\": {\"active\":true}}]");

            var sql = conn.LastCommandText;
            Assert.Contains("SELECT id, data, created_at, updated_at FROM users", sql);
            Assert.Contains("WHERE data @> @p0::jsonb", sql);
            Assert.DoesNotContain("GROUP BY", sql);
            Assert.Equal("{\"active\":true}", conn.LastCommand.ParamValue("@p0"));
        }

        [Fact]
        public void SortBeforeGroup()
        {
            var conn = new SpyConnection();
            Utils.DocAggregate(conn, "users",
                "[{\"$sort\": {\"name\": 1}}]");

            var sql = conn.LastCommandText;
            Assert.Contains("ORDER BY data->>'name' ASC", sql);
        }

        [Fact]
        public void SortAfterGroup()
        {
            var conn = new SpyConnection();
            Utils.DocAggregate(conn, "users",
                "[{\"$group\": {\"_id\": \"$role\", \"cnt\": {\"$sum\": 1}}}, " +
                "{\"$sort\": {\"cnt\": -1}}]");

            var sql = conn.LastCommandText;
            Assert.Contains("ORDER BY cnt DESC", sql);
            Assert.DoesNotContain("data->>'cnt'", sql);
        }

        [Fact]
        public void EmptyPipeline()
        {
            var conn = new SpyConnection();
            Utils.DocAggregate(conn, "users", "[]");

            var sql = conn.LastCommandText;
            Assert.Contains("SELECT id, data, created_at, updated_at FROM users", sql);
            Assert.DoesNotContain("WHERE", sql);
            Assert.DoesNotContain("GROUP BY", sql);
        }

        [Fact]
        public void CountAccumulator()
        {
            var conn = new SpyConnection();
            Utils.DocAggregate(conn, "events",
                "[{\"$group\": {\"_id\": \"$type\", \"n\": {\"$count\": {}}}}]");

            var sql = conn.LastCommandText;
            Assert.Contains("COUNT(*) AS n", sql);
        }

        [Fact]
        public void UnsupportedStageThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.DocAggregate(conn, "users", "[{\"$bucket\": {}}]"));
        }

        [Fact]
        public void InvalidCollectionThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.DocAggregate(conn, "bad table", "[]"));
        }

        [Fact]
        public void NullPipelineThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.DocAggregate(conn, "users", null));
        }

        [Fact]
        public void InvalidGroupFieldThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.DocAggregate(conn, "users",
                    "[{\"$group\": {\"_id\": \"$bad field!\", \"n\": {\"$sum\": 1}}}]"));
        }

        [Fact]
        public void InvalidAccumulatorFieldThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.DocAggregate(conn, "users",
                    "[{\"$group\": {\"_id\": null, \"n\": {\"$sum\": \"$bad field!\"}}}]"));
        }

        [Fact]
        public void UnsupportedAccumulatorThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.DocAggregate(conn, "users",
                    "[{\"$group\": {\"_id\": null, \"n\": {\"$first\": \"$name\"}}}]"));
        }

        [Fact]
        public void CompositeGroupId()
        {
            var conn = new SpyConnection();
            Utils.DocAggregate(conn, "orders",
                "[{\"$group\": {\"_id\": {\"region\": \"$region\", \"year\": \"$year\"}, \"total\": {\"$sum\": \"$amount\"}}}]");

            var sql = conn.LastCommandText;
            Assert.Contains("json_build_object('region', data->>'region', 'year', data->>'year') AS _id", sql);
            Assert.Contains("SUM((data->>'amount')::numeric) AS total", sql);
            Assert.Contains("GROUP BY data->>'region', data->>'year'", sql);
        }

        [Fact]
        public void CompositeGroupIdSingleKey()
        {
            var conn = new SpyConnection();
            Utils.DocAggregate(conn, "events",
                "[{\"$group\": {\"_id\": {\"type\": \"$type\"}, \"cnt\": {\"$sum\": 1}}}]");

            var sql = conn.LastCommandText;
            Assert.Contains("json_build_object('type', data->>'type') AS _id", sql);
            Assert.Contains("COUNT(*) AS cnt", sql);
            Assert.Contains("GROUP BY data->>'type'", sql);
        }

        [Fact]
        public void CompositeGroupIdInvalidFieldThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.DocAggregate(conn, "orders",
                    "[{\"$group\": {\"_id\": {\"x\": \"$bad field!\"}, \"n\": {\"$sum\": 1}}}]"));
        }

        [Fact]
        public void CompositeGroupIdInvalidKeyThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.DocAggregate(conn, "orders",
                    "[{\"$group\": {\"_id\": {\"bad key!\": \"$region\"}, \"n\": {\"$sum\": 1}}}]"));
        }

        [Fact]
        public void PushAccumulator()
        {
            var conn = new SpyConnection();
            Utils.DocAggregate(conn, "orders",
                "[{\"$group\": {\"_id\": \"$region\", \"items\": {\"$push\": \"$item\"}}}]");

            var sql = conn.LastCommandText;
            Assert.Contains("array_agg(data->>'item') AS items", sql);
            Assert.Contains("GROUP BY data->>'region'", sql);
        }

        [Fact]
        public void AddToSetAccumulator()
        {
            var conn = new SpyConnection();
            Utils.DocAggregate(conn, "orders",
                "[{\"$group\": {\"_id\": \"$region\", \"tags\": {\"$addToSet\": \"$tag\"}}}]");

            var sql = conn.LastCommandText;
            Assert.Contains("array_agg(DISTINCT data->>'tag') AS tags", sql);
            Assert.Contains("GROUP BY data->>'region'", sql);
        }

        [Fact]
        public void PushWithNullGroupId()
        {
            var conn = new SpyConnection();
            Utils.DocAggregate(conn, "events",
                "[{\"$group\": {\"_id\": null, \"names\": {\"$push\": \"$name\"}}}]");

            var sql = conn.LastCommandText;
            Assert.Contains("array_agg(data->>'name') AS names", sql);
            Assert.DoesNotContain("GROUP BY", sql);
            Assert.DoesNotContain("AS _id", sql);
        }

        [Fact]
        public void AddToSetInvalidFieldThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.DocAggregate(conn, "orders",
                    "[{\"$group\": {\"_id\": null, \"x\": {\"$addToSet\": \"$bad field!\"}}}]"));
        }
    }

    // ── $project ────────────────────────────────────────────────

    public class DocProjectTest
    {
        [Fact]
        public void ProjectInclude()
        {
            var conn = new SpyConnection();
            Utils.DocAggregate(conn, "users",
                "[{\"$project\": {\"name\": 1, \"status\": 1}}]");

            var sql = conn.LastCommandText;
            Assert.Contains("data->>'name' AS name", sql);
            Assert.Contains("data->>'status' AS status", sql);
        }

        [Fact]
        public void ProjectExcludeId()
        {
            var conn = new SpyConnection();
            Utils.DocAggregate(conn, "users",
                "[{\"$project\": {\"_id\": 0, \"name\": 1}}]");

            var sql = conn.LastCommandText;
            Assert.DoesNotContain("AS _id", sql);
            Assert.Contains("data->>'name' AS name", sql);
        }

        [Fact]
        public void ProjectRename()
        {
            var conn = new SpyConnection();
            Utils.DocAggregate(conn, "users",
                "[{\"$project\": {\"fullName\": \"$name\"}}]");

            var sql = conn.LastCommandText;
            Assert.Contains("data->>'name' AS fullName", sql);
        }

        [Fact]
        public void ProjectAfterGroup()
        {
            var conn = new SpyConnection();
            Utils.DocAggregate(conn, "products",
                "[{\"$group\": {\"_id\": \"$category\", \"count\": {\"$sum\": 1}}}, " +
                "{\"$project\": {\"_id\": 1, \"count\": 1}}]");

            var sql = conn.LastCommandText;
            // $project after $group should pass through aliases, not data->>
            Assert.DoesNotContain("data->>'_id'", sql);
            Assert.DoesNotContain("data->>'count'", sql);
            Assert.Contains("GROUP BY", sql);
        }

        [Fact]
        public void ProjectDotNotation()
        {
            var conn = new SpyConnection();
            Utils.DocAggregate(conn, "users",
                "[{\"$project\": {\"city\": \"$addr.city\"}}]");

            var sql = conn.LastCommandText;
            Assert.Contains("data->'addr'->>'city' AS city", sql);
        }
    }

    // ── $unwind ────────────────────────────────────────────────

    public class DocUnwindTest
    {
        [Fact]
        public void UnwindBasic()
        {
            var conn = new SpyConnection();
            Utils.DocAggregate(conn, "posts",
                "[{\"$unwind\": \"$tags\"}]");

            var sql = conn.LastCommandText;
            Assert.Contains("jsonb_array_elements_text(data->'tags') AS _unwound_tags", sql);
        }

        [Fact]
        public void UnwindThenGroup()
        {
            var conn = new SpyConnection();
            Utils.DocAggregate(conn, "posts",
                "[{\"$unwind\": \"$tags\"}, " +
                "{\"$group\": {\"_id\": \"$tags\", \"count\": {\"$sum\": 1}}}]");

            var sql = conn.LastCommandText;
            Assert.Contains("_unwound_tags AS _id", sql);
            Assert.Contains("GROUP BY _unwound_tags", sql);
            Assert.DoesNotContain("data->>'tags'", sql);
        }

        [Fact]
        public void UnwindObjectForm()
        {
            var conn = new SpyConnection();
            Utils.DocAggregate(conn, "posts",
                "[{\"$unwind\": {\"path\": \"$tags\"}}]");

            var sql = conn.LastCommandText;
            Assert.Contains("jsonb_array_elements_text(data->'tags') AS _unwound_tags", sql);
        }

        [Fact]
        public void UnwindInvalid()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.DocAggregate(conn, "posts",
                    "[{\"$unwind\": \"no_dollar\"}]"));
        }
    }

    // ── $lookup ────────────────────────────────────────────────

    public class DocLookupTest
    {
        [Fact]
        public void LookupBasic()
        {
            var conn = new SpyConnection();
            Utils.DocAggregate(conn, "users",
                "[{\"$lookup\": {" +
                "\"from\": \"orders\", " +
                "\"localField\": \"uid\", " +
                "\"foreignField\": \"uid\", " +
                "\"as\": \"user_orders\"}}]");

            var sql = conn.LastCommandText;
            Assert.Contains("COALESCE(", sql);
            Assert.Contains("json_agg(b.data)", sql);
            Assert.Contains("FROM orders b", sql);
            Assert.Contains("b.data->>'uid'", sql);
            Assert.Contains("users.data->>'uid'", sql);
            Assert.Contains("AS user_orders", sql);
        }

        [Fact]
        public void LookupMissingField()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.DocAggregate(conn, "users",
                    "[{\"$lookup\": {\"localField\": \"uid\", \"foreignField\": \"uid\", \"as\": \"x\"}}]"));
        }

        [Fact]
        public void LookupValidatesIdentifiers()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.DocAggregate(conn, "users",
                    "[{\"$lookup\": {" +
                    "\"from\": \"DROP TABLE; --\", " +
                    "\"localField\": \"uid\", " +
                    "\"foreignField\": \"uid\", " +
                    "\"as\": \"x\"}}]"));
        }
    }

    // ── Full pipeline (unwind + group + match + sort + limit) ──

    public class DocFullPipelineTest
    {
        [Fact]
        public void UnwindGroupMatchSortLimit()
        {
            var conn = new SpyConnection();
            Utils.DocAggregate(conn, "posts",
                "[{\"$match\": {\"status\":\"published\"}}, " +
                "{\"$unwind\": \"$tags\"}, " +
                "{\"$group\": {\"_id\": \"$tags\", \"count\": {\"$sum\": 1}}}, " +
                "{\"$sort\": {\"count\": -1}}, " +
                "{\"$limit\": 5}]");

            var sql = conn.LastCommandText;
            // FROM has the unwind cross join
            Assert.Contains("jsonb_array_elements_text(data->'tags')", sql);
            // GROUP BY uses the unwound alias
            Assert.Contains("GROUP BY _unwound_tags", sql);
            // SELECT uses the unwound alias
            Assert.Contains("_unwound_tags AS _id", sql);
            // WHERE from $match
            Assert.Contains("WHERE data @> @p0::jsonb", sql);
            // ORDER BY + LIMIT
            Assert.Contains("ORDER BY count DESC", sql);
            Assert.Contains("LIMIT @limit", sql);
            Assert.Equal("{\"status\":\"published\"}", conn.LastCommand.ParamValue("@p0"));
            Assert.Equal(5, conn.LastCommand.ParamValue("@limit"));
        }
    }

    // ── BuildFilter (comparison operators) ─────────────────────

    public class BuildFilterTest
    {
        [Fact]
        public void PlainContainmentPassthrough()
        {
            var r = Utils.BuildFilter("{\"active\":true}");
            Assert.Equal("data @> @p0::jsonb", r.WhereClause);
            Assert.Single(r.Params);
            Assert.Equal("{\"active\":true}", r.Params[0]);
        }

        [Fact]
        public void GtNumeric()
        {
            var r = Utils.BuildFilter("{\"age\": {\"$gt\": 21}}");
            Assert.Equal("(data->>'age')::numeric > @p0", r.WhereClause);
            Assert.Single(r.Params);
            Assert.Equal(21.0, r.Params[0]);
        }

        [Fact]
        public void GteNumeric()
        {
            var r = Utils.BuildFilter("{\"score\": {\"$gte\": 90}}");
            Assert.Equal("(data->>'score')::numeric >= @p0", r.WhereClause);
            Assert.Single(r.Params);
            Assert.Equal(90.0, r.Params[0]);
        }

        [Fact]
        public void LtAndLte()
        {
            var r = Utils.BuildFilter("{\"price\": {\"$lt\": 100, \"$gte\": 10}}");
            Assert.Equal("(data->>'price')::numeric < @p0 AND (data->>'price')::numeric >= @p1", r.WhereClause);
            Assert.Equal(2, r.Params.Count);
            Assert.Equal(100.0, r.Params[0]);
            Assert.Equal(10.0, r.Params[1]);
        }

        [Fact]
        public void EqString()
        {
            var r = Utils.BuildFilter("{\"status\": {\"$eq\": \"active\"}}");
            Assert.Equal("data->>'status' = @p0", r.WhereClause);
            Assert.Single(r.Params);
            Assert.Equal("active", r.Params[0]);
        }

        [Fact]
        public void NeOperator()
        {
            var r = Utils.BuildFilter("{\"status\": {\"$ne\": \"deleted\"}}");
            Assert.Equal("data->>'status' != @p0", r.WhereClause);
            Assert.Single(r.Params);
            Assert.Equal("deleted", r.Params[0]);
        }

        [Fact]
        public void InOperator()
        {
            var r = Utils.BuildFilter("{\"color\": {\"$in\": [\"red\", \"blue\"]}}");
            Assert.Equal("data->>'color' IN (@p0, @p1)", r.WhereClause);
            Assert.Equal(2, r.Params.Count);
            Assert.Equal("red", r.Params[0]);
            Assert.Equal("blue", r.Params[1]);
        }

        [Fact]
        public void NinOperator()
        {
            var r = Utils.BuildFilter("{\"color\": {\"$nin\": [\"red\"]}}");
            Assert.Equal("data->>'color' NOT IN (@p0)", r.WhereClause);
            Assert.Single(r.Params);
            Assert.Equal("red", r.Params[0]);
        }

        [Fact]
        public void ExistsTrue()
        {
            var r = Utils.BuildFilter("{\"email\": {\"$exists\": true}}");
            Assert.Equal("data ?? @p0", r.WhereClause);
            Assert.Single(r.Params);
            Assert.Equal("email", r.Params[0]);
        }

        [Fact]
        public void ExistsFalse()
        {
            var r = Utils.BuildFilter("{\"email\": {\"$exists\": false}}");
            Assert.Equal("NOT (data ?? @p0)", r.WhereClause);
            Assert.Single(r.Params);
            Assert.Equal("email", r.Params[0]);
        }

        [Fact]
        public void RegexOperator()
        {
            var r = Utils.BuildFilter("{\"name\": {\"$regex\": \"^A.*\"}}");
            Assert.Equal("data->>'name' ~ @p0", r.WhereClause);
            Assert.Single(r.Params);
            Assert.Equal("^A.*", r.Params[0]);
        }

        [Fact]
        public void MixedContainmentAndOperator()
        {
            var r = Utils.BuildFilter("{\"active\": true, \"age\": {\"$gte\": 18}}");
            Assert.Equal("data @> @p0::jsonb AND (data->>'age')::numeric >= @p1", r.WhereClause);
            Assert.Equal(2, r.Params.Count);
            Assert.Equal("{\"active\": true}", r.Params[0]);
            Assert.Equal(18.0, r.Params[1]);
        }

        [Fact]
        public void NestedFieldPath()
        {
            var r = Utils.BuildFilter("{\"address.city\": {\"$eq\": \"NYC\"}}");
            Assert.Equal("data->'address'->>'city' = @p0", r.WhereClause);
            Assert.Single(r.Params);
            Assert.Equal("NYC", r.Params[0]);
        }

        [Fact]
        public void NullFilterReturnsEmpty()
        {
            var r = Utils.BuildFilter(null);
            Assert.Equal("", r.WhereClause);
            Assert.Empty(r.Params);
        }

        [Fact]
        public void UnsupportedOperatorThrows()
        {
            Assert.Throws<ArgumentException>(() =>
                Utils.BuildFilter("{\"x\": {\"$unknown\": 1}}"));
        }
    }

    // ── Dot-notation expansion in plain containment filters ────────

    public class DotNotationExpansionTest
    {
        [Fact]
        public void SingleDottedKeyExpandsToNestedObject()
        {
            var conn = new SpyConnection();
            Utils.DocFind(conn, "users", filterJson: "{\"addr.city\": \"NY\"}");

            var sql = conn.LastCommandText;
            Assert.Contains("data @> @p0::jsonb", sql);
            Assert.Equal("{\"addr\": {\"city\": \"NY\"}}", conn.LastCommand.ParamValue("@p0"));
        }

        [Fact]
        public void MultiLevelDottedKeyExpandsToDeeplyNested()
        {
            var conn = new SpyConnection();
            Utils.DocFind(conn, "users", filterJson: "{\"a.b.c\": 42}");

            Assert.Equal("{\"a\": {\"b\": {\"c\": 42}}}", conn.LastCommand.ParamValue("@p0"));
        }

        [Fact]
        public void NonDottedKeyPassesThroughUnchanged()
        {
            var conn = new SpyConnection();
            Utils.DocFind(conn, "users", filterJson: "{\"status\": \"active\"}");

            Assert.Equal("{\"status\": \"active\"}", conn.LastCommand.ParamValue("@p0"));
        }

        [Fact]
        public void MixedDottedAndPlainKeys()
        {
            var conn = new SpyConnection();
            Utils.DocFind(conn, "users", filterJson: "{\"addr.city\": \"NY\", \"active\": true}");

            Assert.Equal("{\"addr\": {\"city\": \"NY\"}, \"active\": true}", conn.LastCommand.ParamValue("@p0"));
        }

        [Fact]
        public void MultipleDottedKeysSharingPrefixMerge()
        {
            var conn = new SpyConnection();
            Utils.DocFind(conn, "users", filterJson: "{\"addr.city\": \"NY\", \"addr.zip\": \"10001\"}");

            Assert.Equal("{\"addr\": {\"city\": \"NY\", \"zip\": \"10001\"}}", conn.LastCommand.ParamValue("@p0"));
        }

        [Fact]
        public void DotExpansionWorksInDocCount()
        {
            var conn = new SpyConnection();
            conn.NextScalarResult = 3L;
            Utils.DocCount(conn, "orders", filterJson: "{\"ship.country\": \"US\"}");

            var sql = conn.LastCommandText;
            Assert.Contains("data @> @p0::jsonb", sql);
            Assert.Equal("{\"ship\": {\"country\": \"US\"}}", conn.LastCommand.ParamValue("@p0"));
        }

        [Fact]
        public void DotExpansionWorksInDocUpdate()
        {
            var conn = new SpyConnection();
            Utils.DocUpdate(conn, "users", "{\"profile.verified\": true}", "{\"level\":\"pro\"}");

            Assert.Equal("{\"level\":\"pro\"}", conn.LastCommand.ParamValue("@p0"));
            Assert.Equal("{\"profile\": {\"verified\": true}}", conn.LastCommand.ParamValue("@p1"));
        }

        [Fact]
        public void DotExpansionWorksInDocDelete()
        {
            var conn = new SpyConnection();
            Utils.DocDelete(conn, "logs", "{\"meta.source\": \"test\"}");

            Assert.Equal("{\"meta\": {\"source\": \"test\"}}", conn.LastCommand.ParamValue("@p0"));
        }
    }

    // ── DocWatch ──────────────────────────────────────────────────

    public class DocWatchTest
    {
        [Fact]
        public void CreatesTriggerFunction()
        {
            var conn = new SpyConnection();
            // DocWatch will fail when trying to create a listen connection
            // (SpyConnection isn't Npgsql), but trigger DDL is executed first
            try { Utils.DocWatch(conn, "events", (ch, msg) => { }); }
            catch (Exception) { }

            var sqls = conn.Commands.Select(c => c.CommandText).ToList();
            Assert.True(sqls.Any(s => s.Contains("CREATE OR REPLACE FUNCTION _gl_watch_events()")));
            Assert.True(sqls.Any(s => s.Contains("pg_notify")));
            Assert.True(sqls.Any(s => s.Contains("AFTER INSERT OR UPDATE OR DELETE ON events")));
        }

        [Fact]
        public void TriggerUsesCorrectChannel()
        {
            var conn = new SpyConnection();
            try { Utils.DocWatch(conn, "orders", (ch, msg) => { }); }
            catch (Exception) { }

            var sqls = conn.Commands.Select(c => c.CommandText).ToList();
            Assert.True(sqls.Any(s => s.Contains("_gl_changes_orders")));
        }

        [Fact]
        public void InvalidCollectionThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.DocWatch(conn, "bad; name", (ch, msg) => { }));
        }
    }

    // ── DocUnwatch ────────────────────────────────────────────────

    public class DocUnwatchTest
    {
        [Fact]
        public void DropsTriggerAndFunction()
        {
            var conn = new SpyConnection();
            Utils.DocUnwatch(conn, "events");

            var sqls = conn.Commands.Select(c => c.CommandText).ToList();
            Assert.Contains("DROP TRIGGER IF EXISTS _gl_watch_events_trigger ON events", sqls);
            Assert.Contains("DROP FUNCTION IF EXISTS _gl_watch_events()", sqls);
        }

        [Fact]
        public void InvalidCollectionThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.DocUnwatch(conn, "bad; name"));
        }
    }

    // ── DocCreateTtlIndex ─────────────────────────────────────────

    public class DocCreateTtlIndexTest
    {
        [Fact]
        public void CreatesIndexAndTrigger()
        {
            var conn = new SpyConnection();
            Utils.DocCreateTtlIndex(conn, "sessions", 3600);

            var sqls = conn.Commands.Select(c => c.CommandText).ToList();
            Assert.True(sqls.Any(s => s.Contains("CREATE INDEX IF NOT EXISTS idx_sessions_ttl")));
            Assert.True(sqls.Any(s => s.Contains("CREATE OR REPLACE FUNCTION _gl_ttl_sessions()")));
            Assert.True(sqls.Any(s => s.Contains("INTERVAL '3600 seconds'")));
            Assert.True(sqls.Any(s => s.Contains("BEFORE INSERT ON sessions")));
        }

        [Fact]
        public void InvalidCollectionThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.DocCreateTtlIndex(conn, "bad; name", 3600));
        }
    }

    // ── DocRemoveTtlIndex ─────────────────────────────────────────

    public class DocRemoveTtlIndexTest
    {
        [Fact]
        public void DropsTriggerFunctionAndIndex()
        {
            var conn = new SpyConnection();
            Utils.DocRemoveTtlIndex(conn, "sessions");

            var sqls = conn.Commands.Select(c => c.CommandText).ToList();
            Assert.Contains("DROP TRIGGER IF EXISTS _gl_ttl_sessions_trigger ON sessions", sqls);
            Assert.Contains("DROP FUNCTION IF EXISTS _gl_ttl_sessions()", sqls);
            Assert.Contains("DROP INDEX IF EXISTS idx_sessions_ttl", sqls);
        }

        [Fact]
        public void InvalidCollectionThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.DocRemoveTtlIndex(conn, "bad; name"));
        }
    }

    // ── DocCreateCollection ────────────────────────────────────────

    public class DocCreateCollectionTest
    {
        [Fact]
        public void CreatesRegularTableByDefault()
        {
            var conn = new SpyConnection();
            Utils.DocCreateCollection(conn, "users");

            Assert.Single(conn.Commands);
            var sql = conn.Commands[0].CommandText;
            Assert.Contains("CREATE TABLE IF NOT EXISTS users", sql);
            Assert.DoesNotContain("UNLOGGED", sql);
            Assert.Contains("BIGSERIAL PRIMARY KEY", sql);
            Assert.Contains("data JSONB NOT NULL", sql);
            Assert.Contains("created_at TIMESTAMPTZ", sql);
            Assert.Contains("updated_at TIMESTAMPTZ", sql);
        }

        [Fact]
        public void CreatesUnloggedTable()
        {
            var conn = new SpyConnection();
            Utils.DocCreateCollection(conn, "ephemeral", true);

            Assert.Single(conn.Commands);
            var sql = conn.Commands[0].CommandText;
            Assert.Contains("CREATE UNLOGGED TABLE IF NOT EXISTS ephemeral", sql);
            Assert.DoesNotContain("CREATE TABLE IF NOT EXISTS", sql);
        }

        [Fact]
        public void InvalidCollectionThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.DocCreateCollection(conn, "bad; name"));
        }
    }

    // ── DocCreateCapped ───────────────────────────────────────────

    public class DocCreateCappedTest
    {
        [Fact]
        public void EnsuresCollectionAndCreatesTrigger()
        {
            var conn = new SpyConnection();
            Utils.DocCreateCapped(conn, "logs", 1000);

            var sqls = conn.Commands.Select(c => c.CommandText).ToList();
            Assert.True(sqls.Any(s => s.Contains("CREATE TABLE IF NOT EXISTS logs")));
            Assert.True(sqls.Any(s => s.Contains("CREATE OR REPLACE FUNCTION _gl_cap_logs()")));
            Assert.True(sqls.Any(s => s.Contains("COUNT(*) - 1000")));
            Assert.True(sqls.Any(s => s.Contains("AFTER INSERT ON logs")));
        }

        [Fact]
        public void InvalidCollectionThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.DocCreateCapped(conn, "bad; name", 1000));
        }
    }

    // ── DocRemoveCap ──────────────────────────────────────────────

    public class DocRemoveCapTest
    {
        [Fact]
        public void DropsTriggerAndFunction()
        {
            var conn = new SpyConnection();
            Utils.DocRemoveCap(conn, "logs");

            var sqls = conn.Commands.Select(c => c.CommandText).ToList();
            Assert.Contains("DROP TRIGGER IF EXISTS _gl_cap_logs_trigger ON logs", sqls);
            Assert.Contains("DROP FUNCTION IF EXISTS _gl_cap_logs()", sqls);
        }

        [Fact]
        public void InvalidCollectionThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.DocRemoveCap(conn, "bad; name"));
        }
    }

    // ── Logical operators ($or, $and, $not) ──────────────────────

    public class LogicalOperatorsTest
    {
        [Fact]
        public void OrSimple()
        {
            var r = Utils.BuildFilter("{\"$or\": [{\"status\":\"active\"}, {\"status\":\"inactive\"}]}");
            Assert.Contains("OR", r.WhereClause);
            Assert.StartsWith("(", r.WhereClause);
            Assert.Equal(2, r.Params.Count);
            Assert.Equal("{\"status\":\"active\"}", r.Params[0]);
            Assert.Equal("{\"status\":\"inactive\"}", r.Params[1]);
        }

        [Fact]
        public void AndExplicit()
        {
            var r = Utils.BuildFilter("{\"$and\": [{\"age\": {\"$gt\": 18}}, {\"age\": {\"$lt\": 65}}]}");
            Assert.Contains("AND", r.WhereClause);
            Assert.StartsWith("(", r.WhereClause);
            Assert.Equal(2, r.Params.Count);
            Assert.Equal(18.0, r.Params[0]);
            Assert.Equal(65.0, r.Params[1]);
        }

        [Fact]
        public void NotOperator()
        {
            var r = Utils.BuildFilter("{\"$not\": {\"status\":\"active\"}}");
            Assert.StartsWith("NOT (", r.WhereClause);
            Assert.Single(r.Params);
            Assert.Equal("{\"status\":\"active\"}", r.Params[0]);
        }

        [Fact]
        public void OrWithOperators()
        {
            var r = Utils.BuildFilter("{\"$or\": [{\"status\":\"active\"}, {\"age\": {\"$gt\": 25}}]}");
            Assert.Contains("OR", r.WhereClause);
            Assert.Equal(2, r.Params.Count);
            Assert.Equal("{\"status\":\"active\"}", r.Params[0]);
            Assert.Equal(25.0, r.Params[1]);
        }

        [Fact]
        public void NestedOrAnd()
        {
            var r = Utils.BuildFilter(
                "{\"$or\": [{\"$and\": [{\"a\": 1}, {\"b\": 2}]}, {\"$not\": {\"c\": 3}}]}");
            Assert.Contains("OR", r.WhereClause);
            Assert.Contains("AND", r.WhereClause);
            Assert.Contains("NOT", r.WhereClause);
        }

        [Fact]
        public void MixedLogicalAndField()
        {
            var r = Utils.BuildFilter(
                "{\"name\": \"alice\", \"$or\": [{\"status\":\"active\"}, {\"age\": {\"$gt\": 25}}]}");
            Assert.Contains("AND", r.WhereClause);
            Assert.Contains("OR", r.WhereClause);
            // First param should be containment for name
            Assert.Contains("alice", r.Params[0].ToString());
        }

        [Fact]
        public void OrEmptyThrows()
        {
            Assert.Throws<ArgumentException>(() =>
                Utils.BuildFilter("{\"$or\": []}"));
        }

        [Fact]
        public void OrNonArrayThrows()
        {
            Assert.Throws<ArgumentException>(() =>
                Utils.BuildFilter("{\"$or\": {\"a\": 1}}"));
        }

        [Fact]
        public void NotNonObjectThrows()
        {
            Assert.Throws<ArgumentException>(() =>
                Utils.BuildFilter("{\"$not\": [{\"a\": 1}]}"));
        }

        [Fact]
        public void OrInDocFind()
        {
            var conn = new SpyConnection();
            Utils.DocFind(conn, "users",
                filterJson: "{\"$or\": [{\"status\":\"active\"}, {\"status\":\"inactive\"}]}");
            Assert.Contains("OR", conn.LastCommandText);
        }

        [Fact]
        public void NotInDocCount()
        {
            var conn = new SpyConnection();
            conn.NextScalarResult = 5L;
            Utils.DocCount(conn, "users", filterJson: "{\"$not\": {\"status\":\"suspended\"}}");
            Assert.Contains("NOT", conn.LastCommandText);
        }
    }

    // ── Field update operators ($set, $inc, $unset, $mul, $rename) ──

    public class FieldUpdateOperatorsTest
    {
        [Fact]
        public void PlainUpdateFallback()
        {
            var r = Utils.BuildUpdate("{\"name\":\"new\"}");
            Assert.Equal("data || @p0::jsonb", r.Expression);
            Assert.Single(r.Params);
            Assert.Equal("{\"name\":\"new\"}", r.Params[0]);
        }

        [Fact]
        public void SetOperator()
        {
            var r = Utils.BuildUpdate("{\"$set\": {\"name\":\"new\", \"age\": 30}}");
            Assert.Contains("|| @p0::jsonb", r.Expression);
            Assert.Single(r.Params);
            Assert.Equal("{\"name\":\"new\", \"age\": 30}", r.Params[0]);
        }

        [Fact]
        public void IncOperator()
        {
            var r = Utils.BuildUpdate("{\"$inc\": {\"count\": 1}}");
            Assert.Contains("jsonb_set", r.Expression);
            Assert.Contains("COALESCE", r.Expression);
            Assert.Contains("+ @p1", r.Expression);
            Assert.Equal(2, r.Params.Count);
            Assert.Equal("{count}", r.Params[0]);
            Assert.Equal(1.0, r.Params[1]);
        }

        [Fact]
        public void IncNested()
        {
            var r = Utils.BuildUpdate("{\"$inc\": {\"stats.views\": 5}}");
            Assert.Contains("{stats,views}", r.Params[0].ToString());
            Assert.Contains("data->'stats'->>'views'", r.Expression);
        }

        [Fact]
        public void UnsetTopLevel()
        {
            var r = Utils.BuildUpdate("{\"$unset\": {\"old_field\": \"\"}}");
            Assert.Contains("- @p0", r.Expression);
            Assert.Single(r.Params);
            Assert.Equal("old_field", r.Params[0]);
        }

        [Fact]
        public void UnsetNested()
        {
            var r = Utils.BuildUpdate("{\"$unset\": {\"nested.field\": \"\"}}");
            Assert.Contains("#- @p0::text[]", r.Expression);
            Assert.Single(r.Params);
            Assert.Equal("{nested,field}", r.Params[0]);
        }

        [Fact]
        public void MulOperator()
        {
            var r = Utils.BuildUpdate("{\"$mul\": {\"price\": 1.1}}");
            Assert.Contains("jsonb_set", r.Expression);
            Assert.Contains("* @p1", r.Expression);
            Assert.Equal(2, r.Params.Count);
            Assert.Equal("{price}", r.Params[0]);
            Assert.Equal(1.1, r.Params[1]);
        }

        [Fact]
        public void RenameOperator()
        {
            var r = Utils.BuildUpdate("{\"$rename\": {\"old_name\": \"new_name\"}}");
            Assert.Contains("jsonb_set", r.Expression);
            Assert.Contains("- @p0", r.Expression);
            Assert.Equal(2, r.Params.Count);
            Assert.Equal("old_name", r.Params[0]);
            Assert.Equal("{new_name}", r.Params[1]);
        }

        [Fact]
        public void CombinedSetIncUnset()
        {
            var r = Utils.BuildUpdate(
                "{\"$set\": {\"name\":\"new\"}, \"$inc\": {\"count\": 1}, \"$unset\": {\"temp\": \"\"}}");
            Assert.Contains("|| @p0::jsonb", r.Expression);
            Assert.Contains("jsonb_set", r.Expression);
            Assert.Contains("- @p", r.Expression);
            // $set param, then $unset param (temp), then $inc params (path + amount)
            Assert.Contains("new", r.Params[0].ToString());
            Assert.Contains("temp", r.Params.Cast<object>().Select(p => p.ToString()).ToList());
        }

        [Fact]
        public void SetInDocUpdate()
        {
            var conn = new SpyConnection();
            Utils.DocUpdate(conn, "users",
                "{\"status\": \"old\"}",
                "{\"$set\": {\"status\": \"new\"}}");

            var sql = conn.LastCommandText;
            Assert.Contains("|| @p0::jsonb", sql);
            Assert.Contains("UPDATE users SET data =", sql);
        }

        [Fact]
        public void IncInDocUpdateOne()
        {
            var conn = new SpyConnection();
            Utils.DocUpdateOne(conn, "users",
                "{\"name\": \"alice\"}",
                "{\"$inc\": {\"score\": 10}}");

            var sql = conn.LastCommandText;
            Assert.Contains("jsonb_set", sql);
            Assert.Contains("COALESCE", sql);
        }

        [Fact]
        public void InvalidFieldKeyThrows()
        {
            Assert.Throws<ArgumentException>(() =>
                Utils.BuildUpdate("{\"$inc\": {\"bad;field\": 1}}"));
        }
    }

    // ── Array update operators ($push, $pull, $addToSet) ─────────

    public class ArrayUpdateOperatorsTest
    {
        [Fact]
        public void PushString()
        {
            var r = Utils.BuildUpdate("{\"$push\": {\"tags\": \"new_tag\"}}");
            Assert.Contains("jsonb_set", r.Expression);
            Assert.Contains("COALESCE", r.Expression);
            Assert.Contains("to_jsonb(@p1::text)", r.Expression);
            Assert.Equal(2, r.Params.Count);
            Assert.Equal("{tags}", r.Params[0]);
            Assert.Equal("new_tag", r.Params[1]);
        }

        [Fact]
        public void PushNumber()
        {
            var r = Utils.BuildUpdate("{\"$push\": {\"scores\": 99}}");
            Assert.Contains("to_jsonb(@p1::numeric)", r.Expression);
            Assert.Equal(2, r.Params.Count);
            Assert.Equal("{scores}", r.Params[0]);
            Assert.Equal(99.0, r.Params[1]);
        }

        [Fact]
        public void Pull()
        {
            var r = Utils.BuildUpdate("{\"$pull\": {\"tags\": \"old_tag\"}}");
            Assert.Contains("jsonb_agg(elem)", r.Expression);
            Assert.Contains("WHERE elem !=", r.Expression);
            Assert.Equal(2, r.Params.Count);
            Assert.Equal("{tags}", r.Params[0]);
            Assert.Equal("old_tag", r.Params[1]);
        }

        [Fact]
        public void AddToSet()
        {
            var r = Utils.BuildUpdate("{\"$addToSet\": {\"tags\": \"maybe\"}}");
            Assert.Contains("CASE WHEN", r.Expression);
            Assert.Contains("@>", r.Expression);
            Assert.Equal(3, r.Params.Count);
            Assert.Equal("{tags}", r.Params[0]);
            Assert.Equal("maybe", r.Params[1]);
            Assert.Equal("maybe", r.Params[2]);
        }

        [Fact]
        public void PushInDocUpdate()
        {
            var conn = new SpyConnection();
            Utils.DocUpdate(conn, "users",
                "{\"name\":\"alice\"}",
                "{\"$push\": {\"tags\": \"python\"}}");

            var sql = conn.LastCommandText;
            Assert.Contains("jsonb_set", sql);
            Assert.Contains("COALESCE", sql);
        }

        [Fact]
        public void CombinedSetPush()
        {
            var r = Utils.BuildUpdate(
                "{\"$set\": {\"name\":\"new\"}, \"$push\": {\"tags\": \"added\"}}");
            Assert.Contains("|| @p0::jsonb", r.Expression);
            Assert.Contains("jsonb_set", r.Expression);
        }
    }

    // ── DocFindOneAndUpdate ──────────────────────────────────────

    public class DocFindOneAndUpdateTest
    {
        [Fact]
        public void SqlGeneration()
        {
            var conn = new SpyConnection();
            Utils.DocFindOneAndUpdate(conn, "users",
                "{\"name\":\"alice\"}", "{\"$inc\": {\"score\": 5}}");

            var sql = conn.LastCommandText;
            Assert.Contains("WITH target AS", sql);
            Assert.Contains("RETURNING", sql);
            Assert.Contains("jsonb_set", sql);
            Assert.Contains("SELECT id FROM users", sql);
            Assert.Contains("WHERE data @> @p0::jsonb", sql);
        }

        [Fact]
        public void PlainUpdate()
        {
            var conn = new SpyConnection();
            Utils.DocFindOneAndUpdate(conn, "users",
                "{\"name\":\"alice\"}", "{\"status\":\"updated\"}");

            var sql = conn.LastCommandText;
            Assert.Contains("|| @p1::jsonb", sql);
            Assert.Contains("RETURNING", sql);
        }

        [Fact]
        public void InvalidCollectionThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.DocFindOneAndUpdate(conn, "bad; name", "{}", "{\"a\":1}"));
        }
    }

    // ── DocFindOneAndDelete ──────────────────────────────────────

    public class DocFindOneAndDeleteTest
    {
        [Fact]
        public void SqlGeneration()
        {
            var conn = new SpyConnection();
            Utils.DocFindOneAndDelete(conn, "users", "{\"name\":\"alice\"}");

            var sql = conn.LastCommandText;
            Assert.Contains("WITH target AS", sql);
            Assert.Contains("DELETE FROM users", sql);
            Assert.Contains("RETURNING", sql);
            Assert.Contains("SELECT id FROM users", sql);
            Assert.Contains("WHERE data @> @p0::jsonb", sql);
        }

        [Fact]
        public void WithoutFilter()
        {
            var conn = new SpyConnection();
            Utils.DocFindOneAndDelete(conn, "users", null);

            var sql = conn.LastCommandText;
            Assert.Contains("WITH target AS", sql);
            Assert.Contains("DELETE FROM users", sql);
            Assert.DoesNotContain("WHERE data @>", sql);
        }

        [Fact]
        public void InvalidCollectionThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.DocFindOneAndDelete(conn, "bad; name", "{}"));
        }
    }

    // ── DocDistinct ─────────────────────────────────────────────

    public class DocDistinctTest
    {
        [Fact]
        public void BasicDistinct()
        {
            var conn = new SpyConnection();
            Utils.DocDistinct(conn, "users", "status");

            var sql = conn.LastCommandText;
            Assert.Contains("SELECT DISTINCT", sql);
            Assert.Contains("data->>'status'", sql);
            Assert.Contains("IS NOT NULL", sql);
        }

        [Fact]
        public void DotNotation()
        {
            var conn = new SpyConnection();
            Utils.DocDistinct(conn, "users", "address.city");

            var sql = conn.LastCommandText;
            Assert.Contains("data->'address'->>'city'", sql);
        }

        [Fact]
        public void WithFilter()
        {
            var conn = new SpyConnection();
            Utils.DocDistinct(conn, "users", "status",
                filterJson: "{\"age\": {\"$gt\": 25}}");

            var sql = conn.LastCommandText;
            Assert.Contains("SELECT DISTINCT", sql);
            Assert.Contains("IS NOT NULL", sql);
            Assert.Contains("(data->>'age')::numeric > @p0", sql);
        }

        [Fact]
        public void NoFilter()
        {
            var conn = new SpyConnection();
            Utils.DocDistinct(conn, "users", "status");

            var sql = conn.LastCommandText;
            Assert.Contains("SELECT DISTINCT", sql);
            Assert.Contains("IS NOT NULL", sql);
        }

        [Fact]
        public void InvalidFieldThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.DocDistinct(conn, "users", "bad;field"));
        }

        [Fact]
        public void InvalidCollectionThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.DocDistinct(conn, "bad; name", "status"));
        }
    }

    // ── Helper function tests ───────────────────────────────────

    public class HelperFunctionTest
    {
        [Fact]
        public void FieldPathJsonSingle()
        {
            Assert.Equal("data->'name'", Utils.FieldPathJson("name"));
        }

        [Fact]
        public void FieldPathJsonNested()
        {
            Assert.Equal("data->'addr'->'city'", Utils.FieldPathJson("addr.city"));
        }

        [Fact]
        public void FieldPathJsonInvalidThrows()
        {
            Assert.Throws<ArgumentException>(() => Utils.FieldPathJson("bad;key"));
        }

        [Fact]
        public void JsonbPathSingle()
        {
            Assert.Equal("{name}", Utils.JsonbPath("name"));
        }

        [Fact]
        public void JsonbPathNested()
        {
            Assert.Equal("{addr,city}", Utils.JsonbPath("addr.city"));
        }

        [Fact]
        public void JsonbPathInvalidThrows()
        {
            Assert.Throws<ArgumentException>(() => Utils.JsonbPath("bad;key"));
        }
    }

    // ── $elemMatch ──────────────────────────────────────────────

    public class ElemMatchTest
    {
        [Fact]
        public void NumericRange()
        {
            var r = Utils.BuildFilter("{\"scores\": {\"$elemMatch\": {\"$gt\": 80, \"$lt\": 90}}}");
            Assert.Contains("EXISTS", r.WhereClause);
            Assert.Contains("jsonb_array_elements", r.WhereClause);
            Assert.Contains("elem#>>'{}'", r.WhereClause);
            Assert.Contains("::numeric", r.WhereClause);
            Assert.Contains(80.0, r.Params);
            Assert.Contains(90.0, r.Params);
        }

        [Fact]
        public void StringRegex()
        {
            var r = Utils.BuildFilter("{\"tags\": {\"$elemMatch\": {\"$regex\": \"^py\"}}}");
            Assert.Contains("EXISTS", r.WhereClause);
            Assert.Contains("elem#>>'{}' ~ @p0", r.WhereClause);
            Assert.Single(r.Params);
            Assert.Equal("^py", r.Params[0]);
        }

        [Fact]
        public void SingleCondition()
        {
            var r = Utils.BuildFilter("{\"scores\": {\"$elemMatch\": {\"$eq\": 100}}}");
            Assert.Contains("EXISTS", r.WhereClause);
            Assert.Contains("elem#>>'{}'", r.WhereClause);
            Assert.Single(r.Params);
            Assert.Equal(100.0, r.Params[0]);
        }

        [Fact]
        public void InvalidOperandThrows()
        {
            Assert.Throws<ArgumentException>(() =>
                Utils.BuildFilter("{\"scores\": {\"$elemMatch\": [1, 2]}}"));
        }

        [Fact]
        public void UnsupportedSubOpThrows()
        {
            Assert.Throws<ArgumentException>(() =>
                Utils.BuildFilter("{\"scores\": {\"$elemMatch\": {\"$foo\": 1}}}"));
        }

        [Fact]
        public void InDocFind()
        {
            var conn = new SpyConnection();
            Utils.DocFind(conn, "users",
                filterJson: "{\"scores\": {\"$elemMatch\": {\"$gt\": 80}}}");
            var sql = conn.LastCommandText;
            Assert.Contains("EXISTS", sql);
            Assert.Contains("jsonb_array_elements", sql);
        }

        [Fact]
        public void UsesFieldPathJson()
        {
            var r = Utils.BuildFilter("{\"data_arr\": {\"$elemMatch\": {\"$gt\": 5}}}");
            Assert.Contains("data->'data_arr'", r.WhereClause);
        }

        [Fact]
        public void ParamIdxThreading()
        {
            // Two conditions: $gt and $lt produce 2 params at @p0 and @p1
            var r = Utils.BuildFilter("{\"scores\": {\"$elemMatch\": {\"$gt\": 10, \"$lt\": 20}}}");
            Assert.Equal(2, r.Params.Count);
            Assert.Equal(10.0, r.Params[0]);
            Assert.Equal(20.0, r.Params[1]);
            Assert.Contains("@p0", r.WhereClause);
            Assert.Contains("@p1", r.WhereClause);
        }
    }

    // ── $text in filters ────────────────────────────────────────

    public class TextFilterTest
    {
        [Fact]
        public void TopLevel()
        {
            var r = Utils.BuildFilter("{\"$text\": {\"$search\": \"hello world\"}}");
            Assert.Contains("to_tsvector", r.WhereClause);
            Assert.Contains("plainto_tsquery", r.WhereClause);
            Assert.Contains("data::text", r.WhereClause);
            Assert.Equal(3, r.Params.Count);
            Assert.Equal("english", r.Params[0]);
            Assert.Equal("english", r.Params[1]);
            Assert.Equal("hello world", r.Params[2]);
        }

        [Fact]
        public void FieldLevel()
        {
            var r = Utils.BuildFilter("{\"content\": {\"$text\": {\"$search\": \"hello\"}}}");
            Assert.Contains("to_tsvector", r.WhereClause);
            Assert.Contains("plainto_tsquery", r.WhereClause);
            Assert.Contains("data->>'content'", r.WhereClause);
            Assert.Equal(3, r.Params.Count);
            Assert.Equal("english", r.Params[0]);
            Assert.Equal("english", r.Params[1]);
            Assert.Equal("hello", r.Params[2]);
        }

        [Fact]
        public void CustomLanguage()
        {
            var r = Utils.BuildFilter("{\"$text\": {\"$search\": \"bonjour\", \"$language\": \"french\"}}");
            Assert.Contains("to_tsvector", r.WhereClause);
            Assert.Equal(3, r.Params.Count);
            Assert.Equal("french", r.Params[0]);
            Assert.Equal("french", r.Params[1]);
            Assert.Equal("bonjour", r.Params[2]);
        }

        [Fact]
        public void MissingSearchThrows()
        {
            Assert.Throws<ArgumentException>(() =>
                Utils.BuildFilter("{\"$text\": {\"$language\": \"english\"}}"));
        }

        [Fact]
        public void NonDictThrows()
        {
            Assert.Throws<ArgumentException>(() =>
                Utils.BuildFilter("{\"$text\": \"hello\"}"));
        }

        [Fact]
        public void FieldLevelMissingSearchThrows()
        {
            Assert.Throws<ArgumentException>(() =>
                Utils.BuildFilter("{\"content\": {\"$text\": {\"$language\": \"english\"}}}"));
        }

        [Fact]
        public void InDocFind()
        {
            var conn = new SpyConnection();
            Utils.DocFind(conn, "users",
                filterJson: "{\"$text\": {\"$search\": \"hello\"}}");
            var sql = conn.LastCommandText;
            Assert.Contains("to_tsvector", sql);
            Assert.Contains("@@", sql);
        }

        [Fact]
        public void InDocCount()
        {
            var conn = new SpyConnection();
            conn.NextScalarResult = 3L;
            Utils.DocCount(conn, "users",
                filterJson: "{\"bio\": {\"$text\": {\"$search\": \"python\"}}}");
            var sql = conn.LastCommandText;
            Assert.Contains("to_tsvector", sql);
        }

        [Fact]
        public void FieldLevelCustomLanguage()
        {
            var r = Utils.BuildFilter("{\"bio\": {\"$text\": {\"$search\": \"hola\", \"$language\": \"spanish\"}}}");
            Assert.Contains("data->>'bio'", r.WhereClause);
            Assert.Equal("spanish", r.Params[0]);
            Assert.Equal("spanish", r.Params[1]);
            Assert.Equal("hola", r.Params[2]);
        }
    }

    // ── DocFindCursor ───────────────────────────────────────────

    public class DocFindCursorTest
    {
        [Fact]
        public void IssuesBeginDeclareFetchCommit()
        {
            var conn = new SpyConnection();
            // Default FakeDataReader returns false from Read() — empty result set
            var results = new List<Dictionary<string, object>>();
            foreach (var row in Utils.DocFindCursor(conn, "users"))
                results.Add(row);

            Assert.Empty(results);
            // Commands: BEGIN, DECLARE, FETCH, CLOSE, COMMIT
            var sqls = conn.Commands.Select(c => c.CommandText).ToList();
            Assert.Equal("BEGIN", sqls[0]);
            Assert.Contains("DECLARE", sqls[1]);
            Assert.Contains("CURSOR FOR", sqls[1]);
            Assert.Contains("SELECT id, data, created_at, updated_at FROM users", sqls[1]);
            Assert.Contains("FETCH", sqls[2]);
            Assert.Contains("CLOSE", sqls[3]);
            Assert.Equal("COMMIT", sqls[4]);
        }

        [Fact]
        public void WithFilter()
        {
            var conn = new SpyConnection();
            foreach (var _ in Utils.DocFindCursor(conn, "users",
                filterJson: "{\"active\":true}")) { }

            var declareSql = conn.Commands[1].CommandText;
            Assert.Contains("WHERE data @> @p0::jsonb", declareSql);
            Assert.Equal("{\"active\":true}", conn.Commands[1].ParamValue("@p0"));
        }

        [Fact]
        public void WithSort()
        {
            var conn = new SpyConnection();
            foreach (var _ in Utils.DocFindCursor(conn, "users",
                sortJson: "{\"name\": 1}")) { }

            var declareSql = conn.Commands[1].CommandText;
            Assert.Contains("ORDER BY data->>'name' ASC", declareSql);
        }

        [Fact]
        public void WithLimitAndSkip()
        {
            var conn = new SpyConnection();
            foreach (var _ in Utils.DocFindCursor(conn, "users",
                limit: 100, skip: 50)) { }

            var declareSql = conn.Commands[1].CommandText;
            Assert.Contains("LIMIT @limit", declareSql);
            Assert.Contains("OFFSET @skip", declareSql);
            Assert.Equal(100, conn.Commands[1].ParamValue("@limit"));
            Assert.Equal(50, conn.Commands[1].ParamValue("@skip"));
        }

        [Fact]
        public void BatchSizeInFetch()
        {
            var conn = new SpyConnection();
            foreach (var _ in Utils.DocFindCursor(conn, "users",
                batchSize: 50)) { }

            var fetchSql = conn.Commands[2].CommandText;
            Assert.Contains("FETCH 50", fetchSql);
        }

        [Fact]
        public void InvalidCollectionThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
            {
                foreach (var _ in Utils.DocFindCursor(conn, "bad; name")) { }
            });
        }

        [Fact]
        public void ReturnsEnumerable()
        {
            var conn = new SpyConnection();
            var result = Utils.DocFindCursor(conn, "users");
            Assert.IsAssignableFrom<IEnumerable<Dictionary<string, object>>>(result);
        }
    }
}
