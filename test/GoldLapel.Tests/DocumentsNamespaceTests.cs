using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Xunit;
using GL = GoldLapel.GoldLapel;

namespace GoldLapel.Tests
{
    /// <summary>
    /// Unit tests for <see cref="DocumentsApi"/> — the nested
    /// <c>gl.Documents</c> namespace introduced in Phase 4 of schema-to-core.
    /// Mirrors <c>tests/test_documents.py</c> in the Python wrapper.
    /// </summary>
    public class DocumentsNamespaceShapeTest
    {
        [Fact]
        public void DocumentsIsADocumentsApi()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            Assert.IsType<DocumentsApi>(gl.Documents);
        }

        [Fact]
        public void DocumentsHoldsBackReferenceToParent()
        {
            // The sub-API must store a `_gl` back-reference (not duplicate
            // state). We verify via reflection — the field is private and
            // load-bearing for the shared-state design.
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            var field = typeof(DocumentsApi).GetField("_gl", BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(field);
            Assert.Same(gl, field.GetValue(gl.Documents));
        }

        [Fact]
        public void StreamsHoldsBackReferenceToParent()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            var field = typeof(StreamsApi).GetField("_gl", BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(field);
            Assert.Same(gl, field.GetValue(gl.Streams));
        }

        [Fact]
        public void NoLegacyDocMethodsOnGl()
        {
            // Hard cut — the flat Doc*Async methods are gone.
            var t = typeof(GL);
            foreach (var legacy in new[] {
                "DocInsertAsync", "DocFindAsync", "DocUpdateAsync",
                "DocDeleteAsync", "DocCountAsync", "DocCreateCollectionAsync",
            })
            {
                Assert.Null(t.GetMethod(legacy, BindingFlags.Public | BindingFlags.Instance));
            }
        }

        [Fact]
        public void NoLegacyStreamMethodsOnGl()
        {
            // Hard cut — the flat Stream*Async methods are gone.
            var t = typeof(GL);
            foreach (var legacy in new[] {
                "StreamAddAsync", "StreamCreateGroupAsync", "StreamReadAsync",
                "StreamAckAsync", "StreamClaimAsync",
            })
            {
                Assert.Null(t.GetMethod(legacy, BindingFlags.Public | BindingFlags.Instance));
            }
        }
    }

    /// <summary>
    /// End-to-end verb tests covering each <see cref="DocumentsApi"/> method.
    /// The DDL cache is pre-seeded so the namespace methods bypass the HTTP
    /// fetch; we then assert SQL shape on a <see cref="SpyConnection"/>.
    /// </summary>
    public class DocumentsNamespaceVerbTest
    {
        private readonly GL _gl;
        private readonly SpyConnection _spy;

        public DocumentsNamespaceVerbTest()
        {
            _gl = TestHelpers.MakeWithSpy(out _spy);
            foreach (var c in new[] { "users", "items", "orders", "events", "sessions", "logs" })
                TestHelpers.InjectDocPatterns(_gl, c);
        }

        [Fact]
        public async Task FindOneAndUpdateGoesThroughCanonicalTable()
        {
            await _gl.Documents.FindOneAndUpdateAsync(
                "users", "{\"name\":\"alice\"}", "{\"role\":\"admin\"}");

            var sql = _spy.LastCommandText;
            Assert.Contains("WITH target AS", sql);
            Assert.Contains("UPDATE users", sql);
            Assert.Contains("RETURNING users._id", sql);
        }

        [Fact]
        public async Task FindOneAndDeleteGoesThroughCanonicalTable()
        {
            await _gl.Documents.FindOneAndDeleteAsync("users", "{\"name\":\"alice\"}");

            var sql = _spy.LastCommandText;
            Assert.Contains("DELETE FROM users USING target", sql);
            Assert.Contains("RETURNING users._id", sql);
        }

        [Fact]
        public async Task DistinctTakesFieldAsSecondArg()
        {
            await _gl.Documents.DistinctAsync("users", "email", "{\"active\":true}");

            var sql = _spy.LastCommandText;
            Assert.Contains("SELECT DISTINCT", sql);
            Assert.Contains("FROM users", sql);
        }

        [Fact]
        public async Task UpdateOneTouchesUpdatedAt()
        {
            await _gl.Documents.UpdateOneAsync(
                "users", "{\"name\":\"alice\"}", "{\"age\":30}");

            var sql = _spy.LastCommandText;
            Assert.Contains("UPDATE users", sql);
            Assert.Contains("updated_at = NOW()", sql);
        }

        [Fact]
        public async Task DeleteOneScopesViaSubquery()
        {
            await _gl.Documents.DeleteOneAsync("users", "{\"name\":\"alice\"}");

            var sql = _spy.LastCommandText;
            Assert.Contains("DELETE FROM users WHERE _id = (", sql);
            Assert.Contains("LIMIT 1)", sql);
        }

        [Fact]
        public async Task CreateCollectionIsNoOpOnTheWire()
        {
            // The proxy already issued the CREATE TABLE when patterns were
            // fetched — the wrapper's CreateCollectionAsync is just an
            // explicit cache-warmer.
            await _gl.Documents.CreateCollectionAsync("users");
            Assert.Empty(_spy.Commands);
        }
    }

    /// <summary>
    /// Stream namespace shape tests — mirrors <c>tests/test_streams.py</c>
    /// in the Python wrapper.
    /// </summary>
    public class StreamsNamespaceVerbTest
    {
        private readonly GL _gl;
        private readonly SpyConnection _spy;

        public StreamsNamespaceVerbTest()
        {
            _gl = TestHelpers.MakeWithSpy(out _spy);
            TestHelpers.InjectStreamPatterns(_gl, "events");
        }

        [Fact]
        public async Task AddDelegatesThroughCanonicalTable()
        {
            _spy.NextReaderFactory = () => new FakeDataReader(
                new object[][] { new object[] { 1L, System.DateTime.UtcNow } },
                new[] { "id", "created_at" });
            var id = await _gl.Streams.AddAsync("events", "{\"type\":\"click\"}");
            Assert.Equal(1L, id);
            Assert.Contains("INSERT INTO _goldlapel.stream_events", _spy.LastCommandText);
        }

        [Fact]
        public async Task CreateGroupDelegatesThroughCanonicalTable()
        {
            await _gl.Streams.CreateGroupAsync("events", "workers");
            Assert.Contains("INSERT INTO _goldlapel.stream_events_groups", _spy.LastCommandText);
        }

        [Fact]
        public async Task AckDelegatesThroughCanonicalTable()
        {
            await _gl.Streams.AckAsync("events", "workers", 42L);
            Assert.Contains("DELETE FROM _goldlapel.stream_events_pending", _spy.LastCommandText);
        }
    }

    /// <summary>
    /// Lookup-resolution tests for <see cref="DocumentsApi.AggregateAsync"/>.
    /// Each <c>$lookup.from</c> must trigger a separate DDL fetch (or hit the
    /// per-session cache) so the canonical proxy table is plumbed into the
    /// generated SQL — wrappers don't emit literal user names.
    /// </summary>
    public class DocumentsAggregateLookupTest
    {
        [Fact]
        public async Task LookupFromCollectionsAreResolvedFromCache()
        {
            var gl = TestHelpers.MakeWithSpy(out var spy);
            // Pre-seed both the source and the lookup target. We use the
            // canonical FQ form for the target so the SQL output reflects
            // the proxy mapping unambiguously.
            TestHelpers.InjectDocPatterns(gl, "users");
            var ordersEntry = new DdlEntry
            {
                Tables = new Dictionary<string, string> { ["main"] = "_goldlapel.doc_orders" },
                QueryPatterns = new Dictionary<string, string>(),
            };
            var cacheField = typeof(GL).GetField("_ddlCache",
                BindingFlags.NonPublic | BindingFlags.Instance);
            var cache = (System.Collections.Concurrent.ConcurrentDictionary<string, DdlEntry>)
                cacheField.GetValue(gl);
            cache["doc_store:orders"] = ordersEntry;

            await gl.Documents.AggregateAsync("users",
                "[{\"$match\": {\"active\": true}}, " +
                "{\"$lookup\": {" +
                "\"from\": \"orders\", " +
                "\"localField\": \"id\", " +
                "\"foreignField\": \"userId\", " +
                "\"as\": \"user_orders\"}}]");

            var sql = spy.LastCommandText;
            Assert.Contains("FROM _goldlapel.doc_orders b", sql);
            Assert.DoesNotContain("FROM orders b", sql);
            Assert.Contains("AS user_orders", sql);
        }
    }
}
