using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using Npgsql;

namespace GoldLapel
{
    /// <summary>
    /// The zsets sub-API — accessible as <c>gl.Zsets</c>.
    ///
    /// Phase 5 of schema-to-core. The proxy's v1 zset schema introduces a
    /// <c>zset_key</c> column so a single namespace table holds many sorted
    /// sets — matching Redis's mental model. Every method below threads
    /// <paramref name="zsetKey"/> as the first positional arg after the
    /// namespace <paramref name="name"/>.
    /// </summary>
    public class ZsetsApi
    {
        private readonly GoldLapel _gl;

        internal ZsetsApi(GoldLapel gl) => _gl = gl;

        private Task<DdlEntry> PatternsAsync(string name)
        {
            Utils.ValidateIdentifier(name);
            var token = _gl._dashboardToken ?? Ddl.TokenFromEnvOrFile();
            return Ddl.FetchPatternsAsync(
                _gl._ddlCache, "zset", name,
                _gl.DashboardPort, token, options: null);
        }

        public async Task CreateAsync(string name)
        {
            await PatternsAsync(name).ConfigureAwait(false);
        }

        public async Task<double> AddAsync(string name, string zsetKey, string member, double score,
            NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(name).ConfigureAwait(false);
            return Utils.ZsetAdd(_gl.ResolveActiveDb(connection), name, zsetKey, member, score, patterns);
        }

        public async Task<double> IncrByAsync(string name, string zsetKey, string member, double delta = 1.0,
            NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(name).ConfigureAwait(false);
            return Utils.ZsetIncrBy(_gl.ResolveActiveDb(connection), name, zsetKey, member, delta, patterns);
        }

        public async Task<double?> ScoreAsync(string name, string zsetKey, string member,
            NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(name).ConfigureAwait(false);
            return Utils.ZsetScore(_gl.ResolveActiveDb(connection), name, zsetKey, member, patterns);
        }

        public async Task<long?> RankAsync(string name, string zsetKey, string member, bool desc = true,
            NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(name).ConfigureAwait(false);
            return Utils.ZsetRank(_gl.ResolveActiveDb(connection), name, zsetKey, member, desc, patterns);
        }

        /// <summary>
        /// Members by rank within <paramref name="zsetKey"/>. Inclusive
        /// <paramref name="start"/>/<paramref name="stop"/> bounds Redis-style;
        /// <c>stop=-1</c> is a sentinel meaning "to the end" — mapped to a
        /// large limit (10000) since the proxy's pattern is LIMIT/OFFSET-based.
        /// </summary>
        public async Task<List<(string Member, double Score)>> RangeAsync(
            string name, string zsetKey, int start = 0, int stop = -1, bool desc = true,
            NpgsqlConnection connection = null)
        {
            if (stop == -1) stop = 9999;
            var patterns = await PatternsAsync(name).ConfigureAwait(false);
            return Utils.ZsetRange(_gl.ResolveActiveDb(connection), name, zsetKey, start, stop, desc, patterns);
        }

        public async Task<List<(string Member, double Score)>> RangeByScoreAsync(
            string name, string zsetKey, double minScore, double maxScore, int limit = 100, int offset = 0,
            NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(name).ConfigureAwait(false);
            return Utils.ZsetRangeByScore(
                _gl.ResolveActiveDb(connection), name, zsetKey, minScore, maxScore, limit, offset, patterns);
        }

        public async Task<bool> RemoveAsync(string name, string zsetKey, string member,
            NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(name).ConfigureAwait(false);
            return Utils.ZsetRemove(_gl.ResolveActiveDb(connection), name, zsetKey, member, patterns);
        }

        public async Task<long> CardAsync(string name, string zsetKey, NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(name).ConfigureAwait(false);
            return Utils.ZsetCard(_gl.ResolveActiveDb(connection), name, zsetKey, patterns);
        }
    }
}
