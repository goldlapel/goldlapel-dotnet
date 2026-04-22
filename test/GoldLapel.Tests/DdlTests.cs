using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using GoldLapel;

namespace GoldLapel.Tests
{
    /// <summary>
    /// Unit tests for the DDL API client. Uses a swappable Ddl.PostAsync to
    /// fake the HTTP layer so no port is bound.
    /// </summary>
    public class DdlTests : IDisposable
    {
        private readonly Func<string, string, byte[], CancellationToken, Task<(int, string)>> _origPost;
        private readonly Queue<(int, string)> _responses = new Queue<(int, string)>();
        private readonly List<(string Url, string Token, string Body)> _captured = new List<(string, string, string)>();

        public DdlTests()
        {
            _origPost = Ddl.PostAsync;
            Ddl.PostAsync = (url, token, body, ct) =>
            {
                _captured.Add((url, token, Encoding.UTF8.GetString(body)));
                if (_responses.Count == 0)
                    return Task.FromResult((500, "{\"error\":\"no_response\"}"));
                return Task.FromResult(_responses.Dequeue());
            };
        }

        public void Dispose()
        {
            Ddl.PostAsync = _origPost;
        }

        private void Queue(int status, string body) => _responses.Enqueue((status, body));

        [Fact]
        public void SupportedVersion_Stream_IsV1()
        {
            Assert.Equal("v1", Ddl.SupportedVersion("stream"));
        }

        [Fact]
        public async Task FetchAsync_HappyPath_PostsCorrectBodyAndHeaders()
        {
            Queue(200,
                "{\"accepted\":true,\"family\":\"stream\",\"schema_version\":\"v1\"," +
                "\"tables\":{\"main\":\"_goldlapel.stream_events\"}," +
                "\"query_patterns\":{\"insert\":\"INSERT ...\"}}");
            var cache = new ConcurrentDictionary<string, DdlEntry>();
            var entry = await Ddl.FetchAsync(cache, "stream", "events", 9999, "tok");
            Assert.Equal("_goldlapel.stream_events", entry.Tables["main"]);
            Assert.Equal("INSERT ...", entry.QueryPatterns["insert"]);

            Assert.Single(_captured);
            Assert.EndsWith("/api/ddl/stream/create", _captured[0].Url);
            Assert.Equal("tok", _captured[0].Token);
            Assert.Contains("\"name\":\"events\"", _captured[0].Body);
            Assert.Contains("\"schema_version\":\"v1\"", _captured[0].Body);
        }

        [Fact]
        public async Task FetchAsync_CacheHit_SkipsRepost()
        {
            Queue(200, "{\"tables\":{\"main\":\"x\"},\"query_patterns\":{\"insert\":\"X\"}}");
            var cache = new ConcurrentDictionary<string, DdlEntry>();
            var r1 = await Ddl.FetchAsync(cache, "stream", "events", 9999, "tok");
            var r2 = await Ddl.FetchAsync(cache, "stream", "events", 9999, "tok");
            Assert.Same(r1, r2);
            Assert.Single(_captured);
        }

        [Fact]
        public async Task FetchAsync_DifferentOwners_Isolated()
        {
            Queue(200, "{\"tables\":{\"main\":\"_goldlapel.stream_events\"},\"query_patterns\":{\"insert\":\"X\"}}");
            Queue(200, "{\"tables\":{\"main\":\"_goldlapel.stream_events\"},\"query_patterns\":{\"insert\":\"X\"}}");
            var cacheA = new ConcurrentDictionary<string, DdlEntry>();
            var cacheB = new ConcurrentDictionary<string, DdlEntry>();
            await Ddl.FetchAsync(cacheA, "stream", "events", 9999, "tok");
            await Ddl.FetchAsync(cacheB, "stream", "events", 9999, "tok");
            Assert.Equal(2, _captured.Count);
        }

        [Fact]
        public async Task FetchAsync_DifferentNames_MissCache()
        {
            Queue(200, "{\"tables\":{\"main\":\"_goldlapel.stream_events\"},\"query_patterns\":{\"insert\":\"INSERT events\"}}");
            Queue(200, "{\"tables\":{\"main\":\"_goldlapel.stream_orders\"},\"query_patterns\":{\"insert\":\"INSERT orders\"}}");
            var cache = new ConcurrentDictionary<string, DdlEntry>();
            await Ddl.FetchAsync(cache, "stream", "events", 9999, "tok");
            await Ddl.FetchAsync(cache, "stream", "orders", 9999, "tok");
            Assert.Equal(2, _captured.Count);
        }

        [Fact]
        public async Task FetchAsync_Invalidate_DropsCache()
        {
            Queue(200, "{\"tables\":{\"main\":\"_goldlapel.stream_events\"},\"query_patterns\":{\"insert\":\"X\"}}");
            Queue(200, "{\"tables\":{\"main\":\"_goldlapel.stream_events\"},\"query_patterns\":{\"insert\":\"X\"}}");
            var cache = new ConcurrentDictionary<string, DdlEntry>();
            await Ddl.FetchAsync(cache, "stream", "events", 9999, "tok");
            // Dispose()/DisposeAsync() on GoldLapel clears the cache via
            // _ddlCache.Clear(); simulate that directly so we don't need a
            // running proxy.
            cache.Clear();
            await Ddl.FetchAsync(cache, "stream", "events", 9999, "tok");
            Assert.Equal(2, _captured.Count);
        }

        [Fact]
        public async Task FetchAsync_VersionMismatch_ActionableError()
        {
            Queue(409,
                "{\"error\":\"version_mismatch\",\"detail\":\"wrapper requested v1; proxy speaks v2 — upgrade proxy\"}");
            var cache = new ConcurrentDictionary<string, DdlEntry>();
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                Ddl.FetchAsync(cache, "stream", "events", 9999, "tok"));
            Assert.Contains("schema version mismatch", ex.Message);
        }

        [Fact]
        public async Task FetchAsync_Forbidden_TokenError()
        {
            Queue(403, "{\"error\":\"forbidden\"}");
            var cache = new ConcurrentDictionary<string, DdlEntry>();
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                Ddl.FetchAsync(cache, "stream", "events", 9999, "tok"));
            Assert.Contains("dashboard token", ex.Message);
        }

        [Fact]
        public async Task FetchAsync_MissingToken_ErrsBeforeHttp()
        {
            var cache = new ConcurrentDictionary<string, DdlEntry>();
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                Ddl.FetchAsync(cache, "stream", "events", 9999, null));
            Assert.Contains("No dashboard token", ex.Message);
            Assert.Empty(_captured);
        }

        [Fact]
        public async Task FetchAsync_MissingPort_Errs()
        {
            var cache = new ConcurrentDictionary<string, DdlEntry>();
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                Ddl.FetchAsync(cache, "stream", "events", 0, "tok"));
            Assert.Contains("No dashboard port", ex.Message);
        }

        [Fact]
        public void ToNpgsqlPlaceholders_ConvertsNumberedPlaceholders()
        {
            var sql = Ddl.ToNpgsqlPlaceholders("INSERT INTO t (a, b) VALUES ($1, $2) RETURNING $3");
            Assert.Equal("INSERT INTO t (a, b) VALUES (@p1, @p2) RETURNING @p3", sql);
        }

        [Fact]
        public void ToNpgsqlPlaceholders_HandlesDoubleDigit()
        {
            var sql = Ddl.ToNpgsqlPlaceholders("SELECT $10, $11");
            Assert.Equal("SELECT @p10, @p11", sql);
        }
    }
}
