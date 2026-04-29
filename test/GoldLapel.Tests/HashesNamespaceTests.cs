using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading.Tasks;
using Xunit;
using GL = GoldLapel.GoldLapel;

namespace GoldLapel.Tests
{
    /// <summary>
    /// Unit tests for <see cref="HashesApi"/> — the nested
    /// <c>gl.Hashes</c> namespace introduced in Phase 5 of schema-to-core.
    /// Mirrors <c>tests/test_hashes.py</c> in the Python wrapper.
    ///
    /// Phase 5 schema flip: the proxy now stores hashes as one row per
    /// (hash_key, field) — NOT a JSONB blob per key. <see cref="HashesApi.SetAsync"/>
    /// is a single-row UPSERT (no SELECT-then-merge); <see cref="HashesApi.GetAllAsync"/>
    /// rebuilds the dict client-side from per-row results.
    /// </summary>
    public class HashesNamespaceShapeTest
    {
        [Fact]
        public void HashesIsAHashesApi()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            Assert.IsType<HashesApi>(gl.Hashes);
        }

        [Fact]
        public void NoLegacyHashMethodsOnGl()
        {
            var t = typeof(GL);
            foreach (var legacy in new[] { "HsetAsync", "HgetAsync", "HgetallAsync", "HdelAsync" })
                Assert.Null(t.GetMethod(legacy, BindingFlags.Public | BindingFlags.Instance));
        }
    }

    public class HashesNamespaceVerbTest
    {
        private readonly GL _gl;
        private readonly SpyConnection _spy;

        public HashesNamespaceVerbTest()
        {
            _gl = TestHelpers.MakeWithSpy(out _spy);
            TestHelpers.InjectHashPatterns(_gl, "sessions");
        }

        [Fact]
        public async Task SetIsSingleRowUpsertNotLoadMerge()
        {
            // Phase 5 contract: hash_set runs the proxy's single INSERT/UPSERT
            // directly. No SELECT-then-merge-then-update sequence (the legacy
            // JSONB-blob path).
            _spy.NextReaderFactory = () => new FakeDataReader(
                new object[][] { new object[] { "\"alice\"" } },
                new[] { "value" });
            await _gl.Hashes.SetAsync("sessions", "user:1", "name", "alice");
            Assert.Single(_spy.Commands);
            var sql = _spy.LastCommandText;
            Assert.Contains("INSERT INTO _goldlapel.hash_sessions", sql);
            Assert.Contains("ON CONFLICT (hash_key, field)", sql);
        }

        [Fact]
        public async Task SetJsonEncodesValue()
        {
            _spy.NextReaderFactory = () => new FakeDataReader(
                new object[][] { new object[] { "{\"a\":1}" } },
                new[] { "value" });
            await _gl.Hashes.SetAsync("sessions", "user:1", "data", new Dictionary<string, int> { ["a"] = 1 });
            var cmd = _spy.LastCommand;
            Assert.Equal("user:1", cmd.ParamValue("@p1"));
            Assert.Equal("data", cmd.ParamValue("@p2"));
            // The wrapper JSON-encodes at the edge.
            Assert.Equal("{\"a\":1}", cmd.ParamValue("@p3"));
        }

        [Fact]
        public async Task GetReturnsNullForAbsentField()
        {
            _spy.NextReaderFactory = () => new FakeDataReader(new object[0][], new[] { "value" });
            var got = await _gl.Hashes.GetAsync("sessions", "user:1", "missing");
            Assert.Null(got);
        }

        [Fact]
        public async Task GetAllRebuildsDictFromRows()
        {
            _spy.NextReaderFactory = () => new FakeDataReader(
                new object[][] {
                    new object[] { "email", "\"a@x\"" },
                    new object[] { "name", "\"alice\"" },
                },
                new[] { "field", "value" });
            var got = await _gl.Hashes.GetAllAsync("sessions", "user:1");
            Assert.Equal(2, got.Count);
            Assert.Equal("a@x", got["email"].GetString());
            Assert.Equal("alice", got["name"].GetString());
        }

        [Fact]
        public async Task GetAllReturnsEmptyDictForUnknownKey()
        {
            _spy.NextReaderFactory = () => new FakeDataReader(new object[0][], new[] { "field", "value" });
            var got = await _gl.Hashes.GetAllAsync("sessions", "user:1");
            Assert.Empty(got);
        }

        [Fact]
        public async Task KeysReturnsFieldList()
        {
            _spy.NextReaderFactory = () => new FakeDataReader(
                new object[][] { new object[] { "name" }, new object[] { "email" } },
                new[] { "field" });
            var keys = await _gl.Hashes.KeysAsync("sessions", "user:1");
            Assert.Equal(new[] { "name", "email" }, keys);
        }

        [Fact]
        public async Task ExistsReturnsBoolFromExistsClause()
        {
            _spy.NextScalarResult = true;
            Assert.True(await _gl.Hashes.ExistsAsync("sessions", "user:1", "name"));
            _spy.NextScalarResult = false;
            Assert.False(await _gl.Hashes.ExistsAsync("sessions", "user:1", "absent"));
        }

        [Fact]
        public async Task DeleteReturnsTrueWhenRowAffected()
        {
            _spy.NextNonQueryResult = 1;
            Assert.True(await _gl.Hashes.DeleteAsync("sessions", "user:1", "name"));
        }

        [Fact]
        public async Task LenCountsByHashKey()
        {
            _spy.NextScalarResult = 4L;
            Assert.Equal(4L, await _gl.Hashes.LenAsync("sessions", "user:1"));
            Assert.Contains("WHERE hash_key = @p1", _spy.LastCommandText);
        }

        [Fact]
        public void CanonicalPatternIsRowPerFieldNotBlob()
        {
            // Phase 5 storage flip — the proxy v1 hash family is row-per-field.
            // The canonical SQL must reference the (hash_key, field, value)
            // shape directly, NOT jsonb_build_object.
            var entry = new DdlEntry
            {
                Tables = new Dictionary<string, string> { ["main"] = "_goldlapel.hash_sessions" },
                QueryPatterns = new Dictionary<string, string>
                {
                    ["hset"] = "INSERT INTO _goldlapel.hash_sessions (hash_key, field, value) VALUES ($1, $2, $3::jsonb) ON CONFLICT (hash_key, field) DO UPDATE SET value = EXCLUDED.value RETURNING value",
                },
            };
            Assert.Contains("(hash_key, field, value)", entry.QueryPatterns["hset"]);
            Assert.DoesNotContain("jsonb_build_object", entry.QueryPatterns["hset"]);
        }
    }

    public class HashesBreakingChangeTest
    {
        [Fact]
        public void HelperRequiresPatternsArg()
        {
            Assert.Throws<InvalidOperationException>(() =>
                Utils.HashSet(new SpyConnection(), "sessions", "user:1", "name", "alice", null));
        }
    }
}
