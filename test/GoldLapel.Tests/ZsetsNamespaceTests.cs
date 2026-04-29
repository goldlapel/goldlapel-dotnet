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
    /// Unit tests for <see cref="ZsetsApi"/> — the nested
    /// <c>gl.Zsets</c> namespace introduced in Phase 5 of schema-to-core.
    /// Mirrors <c>tests/test_zsets.py</c> in the Python wrapper.
    ///
    /// Phase 5 schema: a <c>zset_key</c> column lets one namespace table hold
    /// many sorted sets — matching Redis's mental model. Every method threads
    /// <c>zsetKey</c> as the first positional arg after the namespace name.
    /// </summary>
    public class ZsetsNamespaceShapeTest
    {
        [Fact]
        public void ZsetsIsAZsetsApi()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            Assert.IsType<ZsetsApi>(gl.Zsets);
        }

        [Fact]
        public void NoLegacyZsetMethodsOnGl()
        {
            var t = typeof(GL);
            foreach (var legacy in new[] {
                "ZaddAsync", "ZincrbyAsync", "ZrangeAsync",
                "ZrankAsync", "ZscoreAsync", "ZremAsync",
            })
                Assert.Null(t.GetMethod(legacy, BindingFlags.Public | BindingFlags.Instance));
        }
    }

    public class ZsetsNamespaceVerbTest
    {
        private readonly GL _gl;
        private readonly SpyConnection _spy;

        public ZsetsNamespaceVerbTest()
        {
            _gl = TestHelpers.MakeWithSpy(out _spy);
            TestHelpers.InjectZsetPatterns(_gl, "leaderboard");
        }

        [Fact]
        public async Task AddBindsZsetKeyMemberScore()
        {
            _spy.NextScalarResult = 100.0;
            var result = await _gl.Zsets.AddAsync("leaderboard", "global", "alice", 100.0);
            Assert.Equal(100.0, result);
            var cmd = _spy.LastCommand;
            // Phase 5 binding order: ($1=zset_key, $2=member, $3=score).
            Assert.Equal("global", cmd.ParamValue("@p1"));
            Assert.Equal("alice", cmd.ParamValue("@p2"));
            Assert.Equal(100.0, cmd.ParamValue("@p3"));
        }

        [Fact]
        public async Task AddPatternIsUpsertOnZsetKeyMember()
        {
            _spy.NextScalarResult = 1.0;
            await _gl.Zsets.AddAsync("leaderboard", "global", "alice", 1.0);
            var sql = _spy.LastCommandText;
            Assert.Contains("INSERT INTO _goldlapel.zset_leaderboard", sql);
            Assert.Contains("ON CONFLICT (zset_key, member)", sql);
        }

        [Fact]
        public async Task IncrByPassesDelta()
        {
            _spy.NextScalarResult = 110.0;
            var result = await _gl.Zsets.IncrByAsync("leaderboard", "global", "alice", 10.0);
            Assert.Equal(110.0, result);
            // Distinct from `zadd` — the increment pattern adds EXCLUDED.score
            // to the existing row's score.
            Assert.Contains(".score + EXCLUDED.score", _spy.LastCommandText);
        }

        [Fact]
        public async Task ScoreReturnsNullForUnknownMember()
        {
            _spy.NextScalarResult = null;
            var s = await _gl.Zsets.ScoreAsync("leaderboard", "global", "ghost");
            Assert.Null(s);
        }

        [Fact]
        public async Task RangePicksDescPattern()
        {
            await _gl.Zsets.RangeAsync("leaderboard", "global", 0, 10, desc: true);
            Assert.Contains("ORDER BY score DESC", _spy.LastCommandText);
        }

        [Fact]
        public async Task RangePicksAscPattern()
        {
            await _gl.Zsets.RangeAsync("leaderboard", "global", 0, 10, desc: false);
            Assert.Contains("ORDER BY score ASC", _spy.LastCommandText);
        }

        [Fact]
        public async Task RangeTranslatesInclusiveStopToLimit()
        {
            await _gl.Zsets.RangeAsync("leaderboard", "global", 0, 9);
            // Inclusive Redis-style: stop=9, start=0 → LIMIT 10 OFFSET 0.
            var cmd = _spy.LastCommand;
            Assert.Equal("global", cmd.ParamValue("@p1"));
            Assert.Equal(10, cmd.ParamValue("@p2"));
            Assert.Equal(0, cmd.ParamValue("@p3"));
        }

        [Fact]
        public async Task RangeTranslatesNegativeStopToLargeLimit()
        {
            // stop=-1 sentinel → "to the end"; mapped to LIMIT 10000.
            await _gl.Zsets.RangeAsync("leaderboard", "global");
            var cmd = _spy.LastCommand;
            Assert.Equal("global", cmd.ParamValue("@p1"));
            Assert.Equal(10000, cmd.ParamValue("@p2"));
        }

        [Fact]
        public async Task RangeByScoreInclusiveBounds()
        {
            await _gl.Zsets.RangeByScoreAsync("leaderboard", "global", 50.0, 200.0, limit: 10, offset: 2);
            var cmd = _spy.LastCommand;
            // Param order: (zset_key, min, max, limit, offset).
            Assert.Equal("global", cmd.ParamValue("@p1"));
            Assert.Equal(50.0, cmd.ParamValue("@p2"));
            Assert.Equal(200.0, cmd.ParamValue("@p3"));
            Assert.Equal(10, cmd.ParamValue("@p4"));
            Assert.Equal(2, cmd.ParamValue("@p5"));
        }

        [Fact]
        public async Task RankReturnsNullForUnknownMember()
        {
            _spy.NextScalarResult = null;
            var r = await _gl.Zsets.RankAsync("leaderboard", "global", "ghost");
            Assert.Null(r);
        }

        [Fact]
        public async Task RankPicksDescByDefault()
        {
            _spy.NextScalarResult = 0L;
            await _gl.Zsets.RankAsync("leaderboard", "global", "alice");
            Assert.Contains("ORDER BY score DESC", _spy.LastCommandText);
        }

        [Fact]
        public async Task RemoveReturnsTrueOnRowcountOne()
        {
            _spy.NextNonQueryResult = 1;
            Assert.True(await _gl.Zsets.RemoveAsync("leaderboard", "global", "alice"));
        }

        [Fact]
        public async Task RemoveReturnsFalseWhenAbsent()
        {
            _spy.NextNonQueryResult = 0;
            Assert.False(await _gl.Zsets.RemoveAsync("leaderboard", "global", "ghost"));
        }

        [Fact]
        public async Task CardCountsByZsetKey()
        {
            _spy.NextScalarResult = 3L;
            var n = await _gl.Zsets.CardAsync("leaderboard", "global");
            Assert.Equal(3L, n);
            Assert.Contains("WHERE zset_key = @p1", _spy.LastCommandText);
        }
    }

    public class ZsetsBreakingChangeTest
    {
        [Fact]
        public void HelperRequiresPatternsArg()
        {
            Assert.Throws<InvalidOperationException>(() =>
                Utils.ZsetAdd(new SpyConnection(), "leaderboard", "global", "alice", 1.0, null));
        }
    }
}
