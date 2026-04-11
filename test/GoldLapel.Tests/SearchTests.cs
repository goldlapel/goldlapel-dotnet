using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using Xunit;

namespace GoldLapel.Tests
{
    // ── Search ──────────────────────────────────────────────────

    public class SearchTest
    {
        [Fact]
        public void SingleColumnSqlGeneration()
        {
            var conn = new SpyConnection();
            Utils.Search(conn, "articles", "title", "hello world");

            var sql = conn.LastCommandText;
            Assert.Contains("to_tsvector(@lang1, coalesce(title, ''))", sql);
            Assert.Contains("plainto_tsquery(@lang2, @query)", sql);
            Assert.Contains("ts_rank(", sql);
            Assert.Contains("FROM articles", sql);
            Assert.Contains("@@ plainto_tsquery(@lang2, @query)", sql);
            Assert.Contains("ORDER BY _score DESC", sql);
            Assert.Contains("LIMIT @limit", sql);
            Assert.DoesNotContain("ts_headline", sql);
        }

        [Fact]
        public void SingleColumnParameters()
        {
            var conn = new SpyConnection();
            Utils.Search(conn, "articles", "title", "hello world", limit: 20, lang: "french");

            var cmd = conn.LastCommand;
            Assert.Equal("french", cmd.ParamValue("@lang1"));
            Assert.Equal("french", cmd.ParamValue("@lang2"));
            Assert.Equal("hello world", cmd.ParamValue("@query"));
            Assert.Equal(20, cmd.ParamValue("@limit"));
        }

        [Fact]
        public void MultiColumnCoalesceWrapping()
        {
            var conn = new SpyConnection();
            Utils.Search(conn, "articles", new[] { "title", "body" }, "hello");

            var sql = conn.LastCommandText;
            Assert.Contains("coalesce(title, '') || ' ' || coalesce(body, '')", sql);
        }

        [Fact]
        public void ThreeColumnCoalesceWrapping()
        {
            var conn = new SpyConnection();
            Utils.Search(conn, "posts", new[] { "title", "body", "tags" }, "search");

            var sql = conn.LastCommandText;
            Assert.Contains("coalesce(title, '') || ' ' || coalesce(body, '') || ' ' || coalesce(tags, '')", sql);
        }

        [Fact]
        public void HighlightAddsHeadline()
        {
            var conn = new SpyConnection();
            Utils.Search(conn, "articles", "title", "hello", highlight: true);

            var sql = conn.LastCommandText;
            Assert.Contains("ts_headline(@lang3, coalesce(title, ''), plainto_tsquery(@lang2, @query)", sql);
            Assert.Contains("<mark>", sql);
            Assert.Contains("</mark>", sql);
            Assert.Contains("AS _highlight", sql);

            var cmd = conn.LastCommand;
            Assert.Equal("english", cmd.ParamValue("@lang3"));
        }

        [Fact]
        public void DefaultParameters()
        {
            var conn = new SpyConnection();
            Utils.Search(conn, "articles", "title", "test");

            var cmd = conn.LastCommand;
            Assert.Equal("english", cmd.ParamValue("@lang1"));
            Assert.Equal(50, cmd.ParamValue("@limit"));
        }

        [Fact]
        public void InvalidTableThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.Search(conn, "drop table--", "title", "test"));
        }

        [Fact]
        public void InvalidColumnThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.Search(conn, "articles", "col; DROP", "test"));
        }
    }

    // ── SearchFuzzy ─────────────────────────────────────────────

    public class SearchFuzzyTest
    {
        [Fact]
        public void SqlGeneration()
        {
            var conn = new SpyConnection();
            Utils.SearchFuzzy(conn, "articles", "title", "helo");

            var sql = conn.LastCommandText;
            Assert.Contains("similarity(title, @query)", sql);
            Assert.Contains("similarity(title, @query2) > @threshold", sql);
            Assert.Contains("FROM articles", sql);
            Assert.Contains("ORDER BY _score DESC", sql);
            Assert.Contains("LIMIT @limit", sql);
        }

        [Fact]
        public void CreatesExtension()
        {
            var conn = new SpyConnection();
            Utils.SearchFuzzy(conn, "articles", "title", "helo");

            var extensionCmd = conn.Commands[0];
            Assert.Equal("CREATE EXTENSION IF NOT EXISTS pg_trgm", extensionCmd.CommandText);
        }

        [Fact]
        public void Parameters()
        {
            var conn = new SpyConnection();
            Utils.SearchFuzzy(conn, "articles", "title", "helo", limit: 25, threshold: 0.5);

            var cmd = conn.LastCommand;
            Assert.Equal("helo", cmd.ParamValue("@query"));
            Assert.Equal("helo", cmd.ParamValue("@query2"));
            Assert.Equal(0.5, cmd.ParamValue("@threshold"));
            Assert.Equal(25, cmd.ParamValue("@limit"));
        }

        [Fact]
        public void DefaultThreshold()
        {
            var conn = new SpyConnection();
            Utils.SearchFuzzy(conn, "articles", "title", "test");

            var cmd = conn.LastCommand;
            Assert.Equal(0.3, cmd.ParamValue("@threshold"));
        }

        [Fact]
        public void InvalidTableThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.SearchFuzzy(conn, "bad table", "title", "test"));
        }

        [Fact]
        public void InvalidColumnThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.SearchFuzzy(conn, "articles", "1col", "test"));
        }
    }

    // ── SearchPhonetic ──────────────────────────────────────────

    public class SearchPhoneticTest
    {
        [Fact]
        public void SqlGeneration()
        {
            var conn = new SpyConnection();
            Utils.SearchPhonetic(conn, "articles", "title", "jon");

            var sql = conn.LastCommandText;
            Assert.Contains("similarity(title, @query)", sql);
            Assert.Contains("soundex(title) = soundex(@query2)", sql);
            Assert.Contains("FROM articles", sql);
            Assert.Contains("ORDER BY _score DESC, title", sql);
            Assert.Contains("LIMIT @limit", sql);
        }

        [Fact]
        public void CreatesExtensions()
        {
            var conn = new SpyConnection();
            Utils.SearchPhonetic(conn, "articles", "title", "jon");

            Assert.Equal("CREATE EXTENSION IF NOT EXISTS fuzzystrmatch", conn.Commands[0].CommandText);
            Assert.Equal("CREATE EXTENSION IF NOT EXISTS pg_trgm", conn.Commands[1].CommandText);
        }

        [Fact]
        public void Parameters()
        {
            var conn = new SpyConnection();
            Utils.SearchPhonetic(conn, "articles", "name", "smith", limit: 30);

            var cmd = conn.LastCommand;
            Assert.Equal("smith", cmd.ParamValue("@query"));
            Assert.Equal("smith", cmd.ParamValue("@query2"));
            Assert.Equal(30, cmd.ParamValue("@limit"));
        }

        [Fact]
        public void DefaultLimit()
        {
            var conn = new SpyConnection();
            Utils.SearchPhonetic(conn, "articles", "title", "test");

            Assert.Equal(50, conn.LastCommand.ParamValue("@limit"));
        }
    }

    // ── Similar ─────────────────────────────────────────────────

    public class SimilarTest
    {
        [Fact]
        public void SqlGeneration()
        {
            var conn = new SpyConnection();
            Utils.Similar(conn, "docs", "embedding", new double[] { 0.1, 0.2, 0.3 });

            var sql = conn.LastCommandText;
            Assert.Contains("(embedding <=> @vec::vector)", sql);
            Assert.Contains("AS _score", sql);
            Assert.Contains("FROM docs", sql);
            Assert.Contains("ORDER BY _score", sql);
            Assert.Contains("LIMIT @limit", sql);
        }

        [Fact]
        public void CreatesExtension()
        {
            var conn = new SpyConnection();
            Utils.Similar(conn, "docs", "embedding", new double[] { 0.1 });

            Assert.Equal("CREATE EXTENSION IF NOT EXISTS vector", conn.Commands[0].CommandText);
        }

        [Fact]
        public void VectorParameter()
        {
            var conn = new SpyConnection();
            Utils.Similar(conn, "docs", "embedding", new double[] { 1.5, 2.5, 3.5 });

            var cmd = conn.LastCommand;
            Assert.Equal("[1.5,2.5,3.5]", cmd.ParamValue("@vec"));
        }

        [Fact]
        public void VectorParameterInvariantCulture()
        {
            var conn = new SpyConnection();
            Utils.Similar(conn, "docs", "embedding", new double[] { 0.1, 0.2 });

            var vec = (string)conn.LastCommand.ParamValue("@vec");
            Assert.Equal("[0.1,0.2]", vec);
            Assert.DoesNotContain(",1", vec); // no locale comma-as-decimal
        }

        [Fact]
        public void Parameters()
        {
            var conn = new SpyConnection();
            Utils.Similar(conn, "docs", "embedding", new double[] { 0.1 }, limit: 5);

            var cmd = conn.LastCommand;
            Assert.Equal(5, cmd.ParamValue("@limit"));
        }

        [Fact]
        public void DefaultLimit()
        {
            var conn = new SpyConnection();
            Utils.Similar(conn, "docs", "embedding", new double[] { 0.1 });

            Assert.Equal(10, conn.LastCommand.ParamValue("@limit"));
        }
    }

    // ── Suggest ─────────────────────────────────────────────────

    public class SuggestTest
    {
        [Fact]
        public void SqlGeneration()
        {
            var conn = new SpyConnection();
            Utils.Suggest(conn, "cities", "name", "new y");

            var sql = conn.LastCommandText;
            Assert.Contains("similarity(name, @prefix)", sql);
            Assert.Contains("name ILIKE @pattern", sql);
            Assert.Contains("FROM cities", sql);
            Assert.Contains("ORDER BY _score DESC, name", sql);
            Assert.Contains("LIMIT @limit", sql);
        }

        [Fact]
        public void CreatesExtension()
        {
            var conn = new SpyConnection();
            Utils.Suggest(conn, "cities", "name", "new");

            Assert.Equal("CREATE EXTENSION IF NOT EXISTS pg_trgm", conn.Commands[0].CommandText);
        }

        [Fact]
        public void Parameters()
        {
            var conn = new SpyConnection();
            Utils.Suggest(conn, "cities", "name", "new y", limit: 5);

            var cmd = conn.LastCommand;
            Assert.Equal("new y", cmd.ParamValue("@prefix"));
            Assert.Equal("new y%", cmd.ParamValue("@pattern"));
            Assert.Equal(5, cmd.ParamValue("@limit"));
        }

        [Fact]
        public void DefaultLimit()
        {
            var conn = new SpyConnection();
            Utils.Suggest(conn, "cities", "name", "bos");

            Assert.Equal(10, conn.LastCommand.ParamValue("@limit"));
        }
    }

    // ── Facets ──────────────────────────────────────────────────

    public class FacetsTest
    {
        [Fact]
        public void SqlGenerationWithoutQuery()
        {
            var conn = new SpyConnection();
            Utils.Facets(conn, "products", "category", queryColumn: (string)null);

            var sql = conn.LastCommandText;
            Assert.Contains("category AS value", sql);
            Assert.Contains("COUNT(*) AS count", sql);
            Assert.Contains("FROM products", sql);
            Assert.Contains("GROUP BY category", sql);
            Assert.Contains("ORDER BY count DESC, category", sql);
            Assert.Contains("LIMIT @limit", sql);
        }

        [Fact]
        public void SqlGenerationWithQuery()
        {
            var conn = new SpyConnection();
            Utils.Facets(conn, "products", "category", query: "laptop",
                queryColumn: "description");

            var sql = conn.LastCommandText;
            Assert.Contains("to_tsvector(@lang, coalesce(description, ''))", sql);
            Assert.Contains("plainto_tsquery(@lang2, @query)", sql);
            Assert.Contains("GROUP BY category", sql);
        }

        [Fact]
        public void SqlGenerationWithMultiColumnQuery()
        {
            var conn = new SpyConnection();
            Utils.Facets(conn, "products", "category", query: "laptop",
                queryColumns: new[] { "title", "description" });

            var sql = conn.LastCommandText;
            Assert.Contains("coalesce(title, '') || ' ' || coalesce(description, '')", sql);
        }

        [Fact]
        public void ParametersWithQuery()
        {
            var conn = new SpyConnection();
            Utils.Facets(conn, "products", "category", query: "laptop",
                queryColumn: "description", lang: "french");

            var cmd = conn.LastCommand;
            Assert.Equal("french", cmd.ParamValue("@lang"));
            Assert.Equal("french", cmd.ParamValue("@lang2"));
            Assert.Equal("laptop", cmd.ParamValue("@query"));
            Assert.Equal(50, cmd.ParamValue("@limit"));
        }

        [Fact]
        public void ParametersWithoutQuery()
        {
            var conn = new SpyConnection();
            Utils.Facets(conn, "products", "category", limit: 20, queryColumn: (string)null);

            var cmd = conn.LastCommand;
            Assert.Equal(20, cmd.ParamValue("@limit"));
        }

        [Fact]
        public void DefaultLimit()
        {
            var conn = new SpyConnection();
            Utils.Facets(conn, "products", "category", queryColumn: (string)null);

            Assert.Equal(50, conn.LastCommand.ParamValue("@limit"));
        }

        [Fact]
        public void NoTsvectorWithoutQuery()
        {
            var conn = new SpyConnection();
            Utils.Facets(conn, "products", "category", queryColumn: (string)null);

            Assert.DoesNotContain("to_tsvector", conn.LastCommandText);
            Assert.DoesNotContain("plainto_tsquery", conn.LastCommandText);
        }
    }

    // ── Aggregate ───────────────────────────────────────────────

    public class AggregateTest
    {
        [Fact]
        public void CountSqlGeneration()
        {
            var conn = new SpyConnection();
            Utils.Aggregate(conn, "orders", "id", "count");

            var sql = conn.LastCommandText;
            Assert.Contains("COUNT(*) AS value", sql);
            Assert.Contains("FROM orders", sql);
            Assert.DoesNotContain("GROUP BY", sql);
        }

        [Fact]
        public void SumSqlGeneration()
        {
            var conn = new SpyConnection();
            Utils.Aggregate(conn, "orders", "amount", "sum");

            Assert.Contains("SUM(amount) AS value", conn.LastCommandText);
        }

        [Fact]
        public void AvgSqlGeneration()
        {
            var conn = new SpyConnection();
            Utils.Aggregate(conn, "orders", "amount", "avg");

            Assert.Contains("AVG(amount) AS value", conn.LastCommandText);
        }

        [Fact]
        public void MinSqlGeneration()
        {
            var conn = new SpyConnection();
            Utils.Aggregate(conn, "orders", "price", "min");

            Assert.Contains("MIN(price) AS value", conn.LastCommandText);
        }

        [Fact]
        public void MaxSqlGeneration()
        {
            var conn = new SpyConnection();
            Utils.Aggregate(conn, "orders", "price", "max");

            Assert.Contains("MAX(price) AS value", conn.LastCommandText);
        }

        [Fact]
        public void WithGroupBy()
        {
            var conn = new SpyConnection();
            Utils.Aggregate(conn, "orders", "amount", "sum", groupBy: "category");

            var sql = conn.LastCommandText;
            Assert.Contains("category, SUM(amount) AS value", sql);
            Assert.Contains("GROUP BY category", sql);
            Assert.Contains("ORDER BY value DESC", sql);
            Assert.Contains("LIMIT @limit", sql);
        }

        [Fact]
        public void GroupByHasLimitParam()
        {
            var conn = new SpyConnection();
            Utils.Aggregate(conn, "orders", "amount", "sum", groupBy: "category", limit: 25);

            Assert.Equal(25, conn.LastCommand.ParamValue("@limit"));
        }

        [Fact]
        public void WithoutGroupByNoLimit()
        {
            var conn = new SpyConnection();
            Utils.Aggregate(conn, "orders", "amount", "sum");

            var sql = conn.LastCommandText;
            Assert.DoesNotContain("LIMIT", sql);
        }

        [Fact]
        public void InvalidFuncThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.Aggregate(conn, "orders", "id", "invalid_func"));
        }

        [Fact]
        public void FuncIsCaseInsensitive()
        {
            var conn = new SpyConnection();
            Utils.Aggregate(conn, "orders", "amount", "SUM");

            Assert.Contains("SUM(amount)", conn.LastCommandText);
        }

        [Fact]
        public void CountUsesStarNotColumn()
        {
            var conn = new SpyConnection();
            Utils.Aggregate(conn, "orders", "anything", "count");

            Assert.Contains("COUNT(*)", conn.LastCommandText);
            Assert.DoesNotContain("COUNT(anything)", conn.LastCommandText);
        }
    }

    // ── CreateSearchConfig ──────────────────────────────────────

    public class CreateSearchConfigTest
    {
        [Fact]
        public void ChecksExistenceSql()
        {
            var conn = new SpyConnection();
            Utils.CreateSearchConfig(conn, "my_config");

            var checkCmd = conn.Commands[0];
            Assert.Contains("pg_ts_config", checkCmd.CommandText);
            Assert.Contains("cfgname = @name", checkCmd.CommandText);
            Assert.Equal("my_config", checkCmd.ParamValue("@name"));
        }

        [Fact]
        public void CreatesConfigWhenNotExists()
        {
            // By default SpyConnection returns empty reader (no rows), so config "not found"
            var conn = new SpyConnection();
            Utils.CreateSearchConfig(conn, "my_config", "french");

            Assert.Equal(2, conn.Commands.Count);
            var createCmd = conn.Commands[1];
            Assert.Equal(
                "CREATE TEXT SEARCH CONFIGURATION my_config (COPY = french)",
                createCmd.CommandText);
        }

        [Fact]
        public void SkipsCreateWhenExists()
        {
            var conn = new SpyConnection();
            conn.NextReaderFactory = () => new FakeDataReader(
                new[] { new object[] { 1 } }, new[] { "exists" });

            Utils.CreateSearchConfig(conn, "my_config");

            // Only the check command, no create
            Assert.Single(conn.Commands);
        }

        [Fact]
        public void DefaultCopyFrom()
        {
            var conn = new SpyConnection();
            Utils.CreateSearchConfig(conn, "my_config");

            var createCmd = conn.Commands[1];
            Assert.Contains("COPY = english", createCmd.CommandText);
        }

        [Fact]
        public void InvalidNameThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.CreateSearchConfig(conn, "my config!"));
        }

        [Fact]
        public void InvalidCopyFromThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.CreateSearchConfig(conn, "my_config", "bad; sql"));
        }
    }

    // ── PercolateAdd ────────────────────────────────────────────

    public class PercolateAddTest
    {
        [Fact]
        public void CreatesTableSql()
        {
            var conn = new SpyConnection();
            Utils.PercolateAdd(conn, "alerts", "q1", "breaking news");

            var createCmd = conn.Commands[0];
            Assert.Contains("CREATE TABLE IF NOT EXISTS alerts", createCmd.CommandText);
            Assert.Contains("query_id TEXT PRIMARY KEY", createCmd.CommandText);
            Assert.Contains("query_text TEXT NOT NULL", createCmd.CommandText);
            Assert.Contains("tsquery TSQUERY NOT NULL", createCmd.CommandText);
            Assert.Contains("lang TEXT NOT NULL", createCmd.CommandText);
            Assert.Contains("metadata JSONB", createCmd.CommandText);
        }

        [Fact]
        public void CreatesIndexSql()
        {
            var conn = new SpyConnection();
            Utils.PercolateAdd(conn, "alerts", "q1", "breaking news");

            var idxCmd = conn.Commands[1];
            Assert.Contains("CREATE INDEX IF NOT EXISTS alerts_tsq_idx", idxCmd.CommandText);
            Assert.Contains("USING GIN (tsquery)", idxCmd.CommandText);
        }

        [Fact]
        public void InsertSql()
        {
            var conn = new SpyConnection();
            Utils.PercolateAdd(conn, "alerts", "q1", "breaking news");

            var insertCmd = conn.Commands[2];
            Assert.Contains("INSERT INTO alerts", insertCmd.CommandText);
            Assert.Contains("plainto_tsquery(@lang, @query)", insertCmd.CommandText);
            Assert.Contains("ON CONFLICT (query_id) DO UPDATE", insertCmd.CommandText);
        }

        [Fact]
        public void InsertParameters()
        {
            var conn = new SpyConnection();
            Utils.PercolateAdd(conn, "alerts", "q1", "breaking news",
                lang: "french", metadataJson: "{\"priority\":1}");

            var cmd = conn.Commands[2];
            Assert.Equal("q1", cmd.ParamValue("@queryId"));
            Assert.Equal("breaking news", cmd.ParamValue("@query"));
            Assert.Equal("french", cmd.ParamValue("@lang"));
            Assert.Equal("{\"priority\":1}", cmd.ParamValue("@metadata"));
        }

        [Fact]
        public void NullMetadataBindsDbNull()
        {
            var conn = new SpyConnection();
            Utils.PercolateAdd(conn, "alerts", "q1", "test");

            var cmd = conn.Commands[2];
            Assert.Equal(DBNull.Value, cmd.ParamValue("@metadata"));
        }

        [Fact]
        public void DefaultLangIsEnglish()
        {
            var conn = new SpyConnection();
            Utils.PercolateAdd(conn, "alerts", "q1", "test");

            Assert.Equal("english", conn.Commands[2].ParamValue("@lang"));
        }
    }

    // ── Percolate ───────────────────────────────────────────────

    public class PercolateTest
    {
        [Fact]
        public void SqlGeneration()
        {
            var conn = new SpyConnection();
            Utils.Percolate(conn, "alerts", "breaking news happened today");

            var sql = conn.LastCommandText;
            Assert.Contains("query_id, query_text, metadata", sql);
            Assert.Contains("ts_rank(to_tsvector(@lang, @text), tsquery)", sql);
            Assert.Contains("AS _score", sql);
            Assert.Contains("FROM alerts", sql);
            Assert.Contains("to_tsvector(@lang, @text) @@ tsquery", sql);
            Assert.Contains("ORDER BY _score DESC", sql);
            Assert.Contains("LIMIT @limit", sql);
        }

        [Fact]
        public void Parameters()
        {
            var conn = new SpyConnection();
            Utils.Percolate(conn, "alerts", "big event", limit: 10, lang: "german");

            var cmd = conn.LastCommand;
            Assert.Equal("german", cmd.ParamValue("@lang"));
            Assert.Equal("big event", cmd.ParamValue("@text"));
            Assert.Equal(10, cmd.ParamValue("@limit"));
        }

        [Fact]
        public void DefaultParameters()
        {
            var conn = new SpyConnection();
            Utils.Percolate(conn, "alerts", "test text");

            var cmd = conn.LastCommand;
            Assert.Equal("english", cmd.ParamValue("@lang"));
            Assert.Equal(50, cmd.ParamValue("@limit"));
        }
    }

    // ── PercolateDelete ─────────────────────────────────────────

    public class PercolateDeleteTest
    {
        [Fact]
        public void SqlGeneration()
        {
            var conn = new SpyConnection();
            Utils.PercolateDelete(conn, "alerts", "q1");

            var sql = conn.LastCommandText;
            Assert.Contains("DELETE FROM alerts", sql);
            Assert.Contains("WHERE query_id = @queryId", sql);
            Assert.Contains("RETURNING query_id", sql);
        }

        [Fact]
        public void Parameters()
        {
            var conn = new SpyConnection();
            Utils.PercolateDelete(conn, "alerts", "my_query_42");

            Assert.Equal("my_query_42", conn.LastCommand.ParamValue("@queryId"));
        }

        [Fact]
        public void InvalidNameThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.PercolateDelete(conn, "bad name!", "q1"));
        }
    }

    // ── Analyze ─────────────────────────────────────────────────

    public class AnalyzeTest
    {
        [Fact]
        public void SqlGeneration()
        {
            var conn = new SpyConnection();
            Utils.Analyze(conn, "The quick brown fox");

            var sql = conn.LastCommandText;
            Assert.Contains("alias, description, token, dictionaries, dictionary, lexemes", sql);
            Assert.Contains("ts_debug(@lang, @text)", sql);
        }

        [Fact]
        public void Parameters()
        {
            var conn = new SpyConnection();
            Utils.Analyze(conn, "The quick brown fox", lang: "german");

            var cmd = conn.LastCommand;
            Assert.Equal("german", cmd.ParamValue("@lang"));
            Assert.Equal("The quick brown fox", cmd.ParamValue("@text"));
        }

        [Fact]
        public void DefaultLang()
        {
            var conn = new SpyConnection();
            Utils.Analyze(conn, "hello world");

            Assert.Equal("english", conn.LastCommand.ParamValue("@lang"));
        }
    }

    // ── ExplainScore ────────────────────────────────────────────

    public class ExplainScoreTest
    {
        [Fact]
        public void SqlGeneration()
        {
            var conn = new SpyConnection();
            Utils.ExplainScore(conn, "articles", "body", "search term", "id", 42);

            var sql = conn.LastCommandText;
            Assert.Contains("body AS document_text", sql);
            Assert.Contains("to_tsvector(@lang, body)::text AS document_tokens", sql);
            Assert.Contains("plainto_tsquery(@lang, @query)::text AS query_tokens", sql);
            Assert.Contains("to_tsvector(@lang, body) @@ plainto_tsquery(@lang, @query) AS matches", sql);
            Assert.Contains("ts_rank(to_tsvector(@lang, body), plainto_tsquery(@lang, @query)) AS score", sql);
            Assert.Contains("ts_headline(@lang, body, plainto_tsquery(@lang, @query)", sql);
            Assert.Contains("StartSel=**, StopSel=**", sql);
            Assert.Contains("AS headline", sql);
            Assert.Contains("FROM articles", sql);
            Assert.Contains("WHERE id = @idValue", sql);
        }

        [Fact]
        public void Parameters()
        {
            var conn = new SpyConnection();
            Utils.ExplainScore(conn, "articles", "body", "search query", "id", 42, lang: "french");

            var cmd = conn.LastCommand;
            Assert.Equal("french", cmd.ParamValue("@lang"));
            Assert.Equal("search query", cmd.ParamValue("@query"));
            Assert.Equal(42, cmd.ParamValue("@idValue"));
        }

        [Fact]
        public void DefaultLang()
        {
            var conn = new SpyConnection();
            Utils.ExplainScore(conn, "articles", "body", "test", "id", 1);

            Assert.Equal("english", conn.LastCommand.ParamValue("@lang"));
        }

        [Fact]
        public void StringIdValue()
        {
            var conn = new SpyConnection();
            Utils.ExplainScore(conn, "articles", "body", "test", "slug", "my-article");

            Assert.Equal("my-article", conn.LastCommand.ParamValue("@idValue"));
        }

        [Fact]
        public void InvalidTableThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.ExplainScore(conn, "bad table", "body", "test", "id", 1));
        }

        [Fact]
        public void InvalidColumnThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.ExplainScore(conn, "articles", "bad col", "test", "id", 1));
        }

        [Fact]
        public void InvalidIdColumnThrows()
        {
            var conn = new SpyConnection();
            Assert.Throws<ArgumentException>(() =>
                Utils.ExplainScore(conn, "articles", "body", "test", "bad id", 1));
        }
    }

    // ── Spy infrastructure ──────────────────────────────────────

    internal class SpyConnection : DbConnection
    {
        public List<SpyCommand> Commands { get; } = new List<SpyCommand>();
        public Func<FakeDataReader> NextReaderFactory { get; set; }
        public int NextNonQueryResult { get; set; }
        public object NextScalarResult { get; set; }
        public bool WasClosed { get; private set; }
        public bool WasDisposed { get; private set; }

        public string LastCommandText => Commands.Last().CommandText;
        public SpyCommand LastCommand => Commands.Last();

        public override string ConnectionString { get; set; } = "spy";
        public override string Database => "spy";
        public override string DataSource => "spy";
        public override string ServerVersion => "1.0";
        public override ConnectionState State => ConnectionState.Open;

        public override void ChangeDatabase(string databaseName) { }
        public override void Open() { }
        public override void Close() { WasClosed = true; }

        protected override void Dispose(bool disposing)
        {
            WasDisposed = true;
            base.Dispose(disposing);
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            return new FakeTransaction();
        }

        protected override DbCommand CreateDbCommand()
        {
            var cmd = new SpyCommand(this);
            Commands.Add(cmd);
            return cmd;
        }
    }

    internal class SpyCommand : DbCommand
    {
        private readonly SpyConnection _conn;
        private readonly FakeParameterCollection _params = new FakeParameterCollection();

        public SpyCommand(SpyConnection conn) { _conn = conn; }

        public override string CommandText { get; set; }
        public override int CommandTimeout { get; set; }
        public override CommandType CommandType { get; set; }
        public override bool DesignTimeVisible { get; set; }
        public override UpdateRowSource UpdatedRowSource { get; set; }
        protected override DbConnection DbConnection { get; set; }
        protected override DbParameterCollection DbParameterCollection => _params;
        protected override DbTransaction DbTransaction { get; set; }

        public override void Prepare() { }
        public override void Cancel() { }
        protected override DbParameter CreateDbParameter() => new FakeParameter();

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            if (_conn.NextReaderFactory != null)
            {
                var reader = _conn.NextReaderFactory();
                _conn.NextReaderFactory = null;
                return reader;
            }
            return new FakeDataReader(new object[0][], new string[0]);
        }

        public override int ExecuteNonQuery() => _conn.NextNonQueryResult;
        public override object ExecuteScalar() => _conn.NextScalarResult;

        public object ParamValue(string name)
        {
            for (int i = 0; i < _params.Count; i++)
            {
                var p = (DbParameter)_params[i];
                if (p.ParameterName == name)
                    return p.Value;
            }
            throw new KeyNotFoundException("Parameter not found: " + name);
        }
    }
}
