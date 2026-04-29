using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using Npgsql;

namespace GoldLapel
{
    /// <summary>
    /// A (lon, lat) pair returned by <see cref="GeosApi.PosAsync"/> and
    /// <see cref="GeosApi.AddAsync"/>.
    /// </summary>
    public sealed class GeoPosition
    {
        public double Lon { get; set; }
        public double Lat { get; set; }
    }

    /// <summary>
    /// One row from <see cref="GeosApi.RadiusAsync"/> /
    /// <see cref="GeosApi.RadiusByMemberAsync"/> — the matched member, its
    /// position, and its distance from the anchor (in meters).
    /// </summary>
    public sealed class GeoMatch
    {
        public string Member { get; set; }
        public double Lon { get; set; }
        public double Lat { get; set; }
        public double DistanceMeters { get; set; }
    }
    /// <summary>
    /// The geos sub-API — accessible as <c>gl.Geos</c>.
    ///
    /// Phase 5 of schema-to-core. The proxy's v1 geo schema uses GEOGRAPHY
    /// (not GEOMETRY), <c>member TEXT PRIMARY KEY</c> (not <c>BIGSERIAL</c> +
    /// <c>name</c>), and a GIST index on the location column. <see cref="AddAsync"/>
    /// is idempotent on the member name — re-adding a member updates its location.
    ///
    /// Distance unit: methods accept <c>unit = "m" | "km" | "mi" | "ft"</c>.
    /// The proxy column is meters-native (GEOGRAPHY default); wrappers convert
    /// at the edge.
    ///
    /// Geo radius parameter binding (Npgsql <c>$N → @pN</c>):
    /// <list type="bullet">
    /// <item><c>georadius_with_dist</c>: <c>$1=lon, $2=lat, $3=radius_m, $4=limit</c>
    /// — pass <c>(lon, lat, radius_m, limit)</c>.</item>
    /// <item><c>geosearch_member</c>: <c>$1</c> and <c>$2</c> are both the
    /// anchor member, <c>$3=radius_m, $4=limit</c> — pass
    /// <c>(member, member, radius_m, limit)</c> (Npgsql named-param binding,
    /// not source-position).</item>
    /// </list>
    /// </summary>
    public class GeosApi
    {
        private readonly GoldLapel _gl;

        internal GeosApi(GoldLapel gl) => _gl = gl;

        private Task<DdlEntry> PatternsAsync(string name)
        {
            Utils.ValidateIdentifier(name);
            var token = _gl._dashboardToken ?? Ddl.TokenFromEnvOrFile();
            return Ddl.FetchPatternsAsync(
                _gl._ddlCache, "geo", name,
                _gl.DashboardPort, token, options: null);
        }

        public async Task CreateAsync(string name)
        {
            await PatternsAsync(name).ConfigureAwait(false);
        }

        /// <summary>
        /// Set-or-update a member's lon/lat. Idempotent on member (PK).
        /// Returns the just-stored position.
        /// </summary>
        public async Task<GeoPosition> AddAsync(string name, string member, double lon, double lat,
            NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(name).ConfigureAwait(false);
            return Utils.GeoAdd(_gl.ResolveActiveDb(connection), name, member, lon, lat, patterns);
        }

        /// <summary>Fetch a member's (lon, lat), or null if absent.</summary>
        public async Task<GeoPosition> PosAsync(string name, string member, NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(name).ConfigureAwait(false);
            return Utils.GeoPos(_gl.ResolveActiveDb(connection), name, member, patterns);
        }

        /// <summary>
        /// Distance between two members, in <paramref name="unit"/> (m / km /
        /// mi / ft). Returns null if either member is absent.
        /// </summary>
        public async Task<double?> DistAsync(string name, string memberA, string memberB, string unit = "m",
            NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(name).ConfigureAwait(false);
            return Utils.GeoDist(_gl.ResolveActiveDb(connection), name, memberA, memberB, unit, patterns);
        }

        /// <summary>
        /// Members within <paramref name="radius"/> of <paramref name="lon"/>/<paramref name="lat"/>.
        /// </summary>
        public async Task<List<GeoMatch>> RadiusAsync(string name, double lon, double lat, double radius,
            string unit = "m", int limit = 50, NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(name).ConfigureAwait(false);
            return Utils.GeoRadius(_gl.ResolveActiveDb(connection), name, lon, lat, radius, unit, limit, patterns);
        }

        /// <summary>
        /// Members within <paramref name="radius"/> of <paramref name="member"/>'s
        /// location.
        /// </summary>
        public async Task<List<GeoMatch>> RadiusByMemberAsync(string name, string member, double radius,
            string unit = "m", int limit = 50, NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(name).ConfigureAwait(false);
            return Utils.GeoRadiusByMember(_gl.ResolveActiveDb(connection), name, member, radius, unit, limit, patterns);
        }

        public async Task<bool> RemoveAsync(string name, string member, NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(name).ConfigureAwait(false);
            return Utils.GeoRemove(_gl.ResolveActiveDb(connection), name, member, patterns);
        }

        public async Task<long> CountAsync(string name, NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(name).ConfigureAwait(false);
            return Utils.GeoCount(_gl.ResolveActiveDb(connection), name, patterns);
        }
    }
}
