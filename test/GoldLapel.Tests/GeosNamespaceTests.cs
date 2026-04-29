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
    /// Unit tests for <see cref="GeosApi"/> — the nested
    /// <c>gl.Geos</c> namespace introduced in Phase 5 of schema-to-core.
    /// Mirrors <c>tests/test_geos.py</c> in the Python wrapper.
    ///
    /// Phase 5 schema decisions:
    /// <list type="bullet">
    /// <item>GEOGRAPHY column type (not GEOMETRY) — distance returns are
    /// meters native.</item>
    /// <item><c>member TEXT PRIMARY KEY</c> — re-adding a member updates its
    /// location (idempotent), matching Redis GEOADD semantics.</item>
    /// <item><c>updated_at</c> stamped on every UPSERT.</item>
    /// </list>
    ///
    /// Geo radius parameter binding (Npgsql uses named <c>@pN</c> after the
    /// translation, NOT source-position):
    /// <list type="bullet">
    /// <item><c>georadius_with_dist</c>: <c>$1=lon, $2=lat, $3=radius_m,
    /// $4=limit</c> — bind 4 args <c>(lon, lat, radius_m, limit)</c>.</item>
    /// <item><c>geosearch_member</c>: <c>$1=member, $2=member, $3=radius_m,
    /// $4=limit</c> — bind 4 args <c>(member, member, radius_m, limit)</c>.
    /// We pass the member twice because Npgsql's named-param binding fills
    /// each <c>@pN</c> slot independently.</item>
    /// </list>
    /// </summary>
    public class GeosNamespaceShapeTest
    {
        [Fact]
        public void GeosIsAGeosApi()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            Assert.IsType<GeosApi>(gl.Geos);
        }

        [Fact]
        public void NoLegacyGeoMethodsOnGl()
        {
            var t = typeof(GL);
            foreach (var legacy in new[] { "GeoaddAsync", "GeoradiusAsync", "GeodistAsync" })
                Assert.Null(t.GetMethod(legacy, BindingFlags.Public | BindingFlags.Instance));
        }
    }

    public class GeosNamespaceVerbTest
    {
        private readonly GL _gl;
        private readonly SpyConnection _spy;

        public GeosNamespaceVerbTest()
        {
            _gl = TestHelpers.MakeWithSpy(out _spy);
            TestHelpers.InjectGeoPatterns(_gl, "riders");
        }

        [Fact]
        public async Task AddIsIdempotentViaOnConflict()
        {
            _spy.NextReaderFactory = () => new FakeDataReader(
                new object[][] { new object[] { 13.4, 52.5 } },
                new[] { "lon", "lat" });
            var pos = await _gl.Geos.AddAsync("riders", "alice", 13.4, 52.5);
            Assert.NotNull(pos);
            Assert.Equal(13.4, pos.Lon);
            Assert.Equal(52.5, pos.Lat);
            var sql = _spy.LastCommandText;
            Assert.Contains("ON CONFLICT (member)", sql);
            Assert.Contains("DO UPDATE", sql);
        }

        [Fact]
        public async Task AddPatternIsGeographyNative()
        {
            // Phase 5: column is GEOGRAPHY natively. The proxy SQL casts the
            // ST_MakePoint output to ::geography (not ::geometry).
            _spy.NextReaderFactory = () => new FakeDataReader(
                new object[][] { new object[] { 0.0, 0.0 } }, new[] { "lon", "lat" });
            await _gl.Geos.AddAsync("riders", "alice", 0.0, 0.0);
            Assert.Contains("::geography", _spy.LastCommandText);
        }

        [Fact]
        public async Task PosReturnsNullForUnknownMember()
        {
            _spy.NextReaderFactory = () => new FakeDataReader(
                new object[0][], new[] { "lon", "lat" });
            var pos = await _gl.Geos.PosAsync("riders", "missing");
            Assert.Null(pos);
        }

        [Fact]
        public async Task DistReturnsMetersByDefault()
        {
            _spy.NextReaderFactory = () => new FakeDataReader(
                new object[][] { new object[] { 1234.0 } }, new[] { "distance_m" });
            var d = await _gl.Geos.DistAsync("riders", "a", "b");
            Assert.Equal(1234.0, d);
        }

        [Fact]
        public async Task DistConvertsToKm()
        {
            _spy.NextReaderFactory = () => new FakeDataReader(
                new object[][] { new object[] { 1234.0 } }, new[] { "distance_m" });
            var d = await _gl.Geos.DistAsync("riders", "a", "b", unit: "km");
            Assert.Equal(1.234, d);
        }

        [Fact]
        public async Task DistConvertsToMiles()
        {
            _spy.NextReaderFactory = () => new FakeDataReader(
                new object[][] { new object[] { 1609.344 } }, new[] { "distance_m" });
            var d = await _gl.Geos.DistAsync("riders", "a", "b", unit: "mi");
            Assert.NotNull(d);
            Assert.Equal(1.0, d.Value, precision: 6);
        }

        [Fact]
        public async Task DistUnknownUnitThrows()
        {
            await Assert.ThrowsAsync<ArgumentException>(async () =>
                await _gl.Geos.DistAsync("riders", "a", "b", unit: "parsec"));
        }

        [Fact]
        public async Task RadiusConvertsUnitToMetersForQuery()
        {
            _spy.NextReaderFactory = () => new FakeDataReader(
                new object[0][], new[] { "member", "lon", "lat", "distance_m" });
            await _gl.Geos.RadiusAsync("riders", 13.4, 52.5, 5, unit: "km");
            // Proxy contract: $1=lon, $2=lat, $3=radius_m, $4=limit. CTE anchor
            // means each $N appears exactly once in the rendered SQL — Npgsql
            // binds 4 args by name.
            var cmd = _spy.LastCommand;
            Assert.Equal(13.4, cmd.ParamValue("@p1"));
            Assert.Equal(52.5, cmd.ParamValue("@p2"));
            Assert.Equal(5000.0, cmd.ParamValue("@p3"));
            Assert.Equal(50, cmd.ParamValue("@p4"));
        }

        [Fact]
        public async Task RadiusByMemberPassesMemberTwiceAsP1AndP2()
        {
            _spy.NextReaderFactory = () => new FakeDataReader(
                new object[0][], new[] { "member", "lon", "lat", "distance_m" });
            await _gl.Geos.RadiusByMemberAsync("riders", "alice", 1000);
            // Proxy `geosearch_member`: WHERE a.member = $1 AND ST_DWithin(...,$3)
            // AND b.member <> $2 ... LIMIT $4. Npgsql binds 4 args by name.
            var cmd = _spy.LastCommand;
            Assert.Equal("alice", cmd.ParamValue("@p1"));
            Assert.Equal("alice", cmd.ParamValue("@p2"));
            Assert.Equal(1000.0, cmd.ParamValue("@p3"));
            Assert.Equal(50, cmd.ParamValue("@p4"));
        }

        [Fact]
        public async Task RadiusReturnsListOfGeoMatch()
        {
            _spy.NextReaderFactory = () => new FakeDataReader(
                new object[][] {
                    new object[] { "alice", 13.4, 52.5, 100.0 },
                    new object[] { "bob", 13.5, 52.6, 200.0 },
                },
                new[] { "member", "lon", "lat", "distance_m" });
            var matches = await _gl.Geos.RadiusAsync("riders", 13.4, 52.5, 1000);
            Assert.Equal(2, matches.Count);
            Assert.Equal("alice", matches[0].Member);
            Assert.Equal(100.0, matches[0].DistanceMeters);
            Assert.Equal("bob", matches[1].Member);
        }

        [Fact]
        public async Task RemoveReturnsTrueWhenDeleted()
        {
            _spy.NextNonQueryResult = 1;
            Assert.True(await _gl.Geos.RemoveAsync("riders", "alice"));
        }

        [Fact]
        public async Task RemoveReturnsFalseWhenAbsent()
        {
            _spy.NextNonQueryResult = 0;
            Assert.False(await _gl.Geos.RemoveAsync("riders", "ghost"));
        }

        [Fact]
        public async Task CountQueriesAggregate()
        {
            _spy.NextScalarResult = 3L;
            Assert.Equal(3L, await _gl.Geos.CountAsync("riders"));
            Assert.Contains("SELECT COUNT(*)", _spy.LastCommandText);
        }
    }

    public class GeosBreakingChangeTest
    {
        [Fact]
        public void HelperRequiresPatternsArg()
        {
            Assert.Throws<InvalidOperationException>(() =>
                Utils.GeoAdd(new SpyConnection(), "riders", "alice", 0.0, 0.0, null));
        }
    }
}
