using System;
using System.Collections.Generic;
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
            Assert.Contains("WHERE data @> @filter::jsonb", sql);
            Assert.Equal("{\"active\":true}", conn.LastCommand.ParamValue("@filter"));
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
            Assert.Contains("WHERE data @> @filter::jsonb", sql);
            Assert.Contains("ORDER BY data->>'date' DESC", sql);
            Assert.Contains("LIMIT @limit", sql);
            Assert.Contains("OFFSET @skip", sql);

            Assert.Equal("{\"status\":\"published\"}", conn.LastCommand.ParamValue("@filter"));
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
            Assert.Contains("WHERE data @> @filter::jsonb", sql);
            Assert.Contains("LIMIT 1", sql);
            Assert.Equal("{\"email\":\"a@b.com\"}", conn.LastCommand.ParamValue("@filter"));
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
            Assert.Contains("SET data = data || @update::jsonb", sql);
            Assert.Contains("updated_at = NOW()", sql);
            Assert.Contains("WHERE data @> @filter::jsonb", sql);
        }

        [Fact]
        public void Parameters()
        {
            var conn = new SpyConnection();
            Utils.DocUpdate(conn, "users", "{\"active\":true}", "{\"role\":\"admin\"}");

            Assert.Equal("{\"role\":\"admin\"}", conn.LastCommand.ParamValue("@update"));
            Assert.Equal("{\"active\":true}", conn.LastCommand.ParamValue("@filter"));
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
            Assert.Contains("SET data = data || @update::jsonb", sql);
            Assert.Contains("updated_at = NOW()", sql);
            Assert.Contains("WHERE id = (SELECT id FROM users WHERE data @> @filter::jsonb LIMIT 1)", sql);
        }

        [Fact]
        public void Parameters()
        {
            var conn = new SpyConnection();
            Utils.DocUpdateOne(conn, "users", "{\"name\":\"alice\"}", "{\"age\":30}");

            Assert.Equal("{\"age\":30}", conn.LastCommand.ParamValue("@update"));
            Assert.Equal("{\"name\":\"alice\"}", conn.LastCommand.ParamValue("@filter"));
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
            Assert.Contains("WHERE data @> @filter::jsonb", sql);
        }

        [Fact]
        public void Parameters()
        {
            var conn = new SpyConnection();
            Utils.DocDelete(conn, "users", "{\"active\":false}");

            Assert.Equal("{\"active\":false}", conn.LastCommand.ParamValue("@filter"));
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
            Assert.Contains("SELECT id FROM users WHERE data @> @filter::jsonb LIMIT 1)", sql);
        }

        [Fact]
        public void Parameters()
        {
            var conn = new SpyConnection();
            Utils.DocDeleteOne(conn, "users", "{\"name\":\"alice\"}");

            Assert.Equal("{\"name\":\"alice\"}", conn.LastCommand.ParamValue("@filter"));
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
            Assert.Contains("WHERE data @> @filter::jsonb", sql);
            Assert.Equal("{\"active\":true}", conn.LastCommand.ParamValue("@filter"));
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
            Assert.Contains("WHERE data @> @filter::jsonb", sql);
            Assert.Contains("GROUP BY data->>'region'", sql);
            Assert.Contains("ORDER BY total DESC", sql);
            Assert.Contains("LIMIT @limit", sql);
            Assert.Contains("OFFSET @skip", sql);
            Assert.Equal("{\"status\":\"shipped\"}", conn.LastCommand.ParamValue("@filter"));
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
            Assert.Contains("WHERE data @> @filter::jsonb", sql);
            Assert.DoesNotContain("GROUP BY", sql);
            Assert.Equal("{\"active\":true}", conn.LastCommand.ParamValue("@filter"));
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
                Utils.DocAggregate(conn, "users", "[{\"$lookup\": {}}]"));
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
    }
}
