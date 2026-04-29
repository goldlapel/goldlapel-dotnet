using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Xunit;
using GL = GoldLapel.GoldLapel;

namespace GoldLapel.Tests
{
    /// <summary>
    /// Unit tests for <see cref="CountersApi"/> — the nested
    /// <c>gl.Counters</c> namespace introduced in Phase 5 of schema-to-core.
    /// Mirrors <c>tests/test_counters.py</c> in the Python wrapper.
    ///
    /// Phase 5 contract: counter UPSERTs stamp <c>updated_at = NOW()</c> on
    /// the proxy side. The wrapper executes the proxy's canonical pattern
    /// verbatim (after <c>$N → @pN</c> translation) and binds positionally.
    /// </summary>
    public class CountersNamespaceShapeTest
    {
        [Fact]
        public void CountersIsACountersApi()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            Assert.IsType<CountersApi>(gl.Counters);
        }

        [Fact]
        public void CountersHoldsBackReferenceToParent()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            var field = typeof(CountersApi).GetField("_gl", BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(field);
            Assert.Same(gl, field.GetValue(gl.Counters));
        }

        [Fact]
        public void NoLegacyCounterMethodsOnGl()
        {
            // Hard cut — the flat IncrAsync / GetCounterAsync methods are gone.
            var t = typeof(GL);
            foreach (var legacy in new[] { "IncrAsync", "GetCounterAsync" })
                Assert.Null(t.GetMethod(legacy, BindingFlags.Public | BindingFlags.Instance));
        }
    }

    public class CountersNamespaceVerbTest
    {
        private readonly GL _gl;
        private readonly SpyConnection _spy;

        public CountersNamespaceVerbTest()
        {
            _gl = TestHelpers.MakeWithSpy(out _spy);
            TestHelpers.InjectCounterPatterns(_gl, "pageviews");
        }

        [Fact]
        public async Task IncrUsesCanonicalProxyTable()
        {
            _spy.NextScalarResult = 5L;
            var result = await _gl.Counters.IncrAsync("pageviews", "home");
            Assert.Equal(5L, result);
            var sql = _spy.LastCommandText;
            Assert.Contains("INSERT INTO _goldlapel.counter_pageviews", sql);
            Assert.Contains("ON CONFLICT (key)", sql);
        }

        [Fact]
        public async Task IncrPattern_StampsUpdatedAt()
        {
            // Phase 5 contract: every counter UPSERT stamps updated_at = NOW().
            _spy.NextScalarResult = 1L;
            await _gl.Counters.IncrAsync("pageviews", "home");
            Assert.Contains("updated_at = NOW()", _spy.LastCommandText);
        }

        [Fact]
        public async Task IncrBindsKeyAndAmount()
        {
            _spy.NextScalarResult = 7L;
            await _gl.Counters.IncrAsync("pageviews", "home", 5);
            var cmd = _spy.LastCommand;
            Assert.Equal("home", cmd.ParamValue("@p1"));
            Assert.Equal(5L, cmd.ParamValue("@p2"));
        }

        [Fact]
        public async Task DecrPassesNegativeAmount()
        {
            _spy.NextScalarResult = -3L;
            await _gl.Counters.DecrAsync("pageviews", "home", 3);
            // Decr is incr-with-negation — same `incr` pattern, negative @p2.
            Assert.Contains("INSERT INTO _goldlapel.counter_pageviews", _spy.LastCommandText);
            Assert.Equal(-3L, _spy.LastCommand.ParamValue("@p2"));
        }

        [Fact]
        public async Task SetUsesSetPatternNotIncr()
        {
            _spy.NextScalarResult = 100L;
            await _gl.Counters.SetAsync("pageviews", "home", 100);
            var sql = _spy.LastCommandText;
            // The set pattern overwrites; the incr pattern adds. Both use the
            // canonical UPSERT form, but `set` does `value = EXCLUDED.value`
            // rather than `value = table.value + EXCLUDED.value`.
            Assert.Contains("DO UPDATE SET value = EXCLUDED.value", sql);
            Assert.DoesNotContain(".value + EXCLUDED.value", sql);
        }

        [Fact]
        public async Task GetReturnsZeroForUnknownKey()
        {
            _spy.NextScalarResult = null;
            var result = await _gl.Counters.GetAsync("pageviews", "missing");
            Assert.Equal(0L, result);
        }

        [Fact]
        public async Task DeleteReturnsTrueWhenRowAffected()
        {
            _spy.NextNonQueryResult = 1;
            Assert.True(await _gl.Counters.DeleteAsync("pageviews", "home"));
        }

        [Fact]
        public async Task DeleteReturnsFalseWhenAbsent()
        {
            _spy.NextNonQueryResult = 0;
            Assert.False(await _gl.Counters.DeleteAsync("pageviews", "missing"));
        }

        [Fact]
        public async Task CountKeysQueriesAggregate()
        {
            _spy.NextScalarResult = 3L;
            var n = await _gl.Counters.CountKeysAsync("pageviews");
            Assert.Equal(3L, n);
            Assert.Contains("SELECT COUNT(*) FROM _goldlapel.counter_pageviews", _spy.LastCommandText);
        }

        [Fact]
        public async Task CreateAsyncIsNoOpOnTheWire()
        {
            // The proxy already issued the CREATE TABLE when patterns were
            // fetched — CreateAsync just warms the cache.
            await _gl.Counters.CreateAsync("pageviews");
            Assert.Empty(_spy.Commands);
        }
    }

    public class CountersBreakingChangeTest
    {
        [Fact]
        public void HelperRequiresPatternsArg()
        {
            // Phase 5 contract: the proxy owns DDL — passing patterns=null
            // should raise rather than CREATE TABLE behind the user's back.
            // A bad identifier would also raise but earlier; use a valid name
            // so we hit the patterns check.
            Assert.Throws<InvalidOperationException>(() =>
                Utils.CounterIncr(new SpyConnection(), "pageviews", "home", 1, null));
        }
    }
}
