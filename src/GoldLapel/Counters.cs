using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using Npgsql;

namespace GoldLapel
{
    /// <summary>
    /// The counters sub-API — accessible as <c>gl.Counters</c>.
    ///
    /// Phase 5 of schema-to-core: the proxy owns counter DDL. Each call here:
    ///
    /// <list type="number">
    /// <item>Calls <c>POST /api/ddl/counter/create</c> (idempotent) to materialize
    /// the canonical <c>_goldlapel.counter_&lt;name&gt;</c> table and pull its
    /// query patterns.</item>
    /// <item>Caches <c>(tables, query_patterns)</c> on the parent
    /// <see cref="GoldLapel"/> instance for the session's lifetime.</item>
    /// <item>Hands the patterns off to <see cref="Utils"/> <c>Counter*</c>
    /// helpers, which execute against the canonical table name.</item>
    /// </list>
    ///
    /// Phase 5 contract: every UPSERT stamps <c>updated_at = NOW()</c> on the
    /// proxy side. Mirrors <c>goldlapel.counters.CountersAPI</c> (Python is
    /// canonical).
    /// </summary>
    public class CountersApi
    {
        private readonly GoldLapel _gl;

        internal CountersApi(GoldLapel gl) => _gl = gl;

        private Task<DdlEntry> PatternsAsync(string name)
        {
            Utils.ValidateIdentifier(name);
            var token = _gl._dashboardToken ?? Ddl.TokenFromEnvOrFile();
            return Ddl.FetchPatternsAsync(
                _gl._ddlCache, "counter", name,
                _gl.DashboardPort, token, options: null);
        }

        /// <summary>
        /// Eagerly materialize the counter table. Other methods will also
        /// materialize on first use, so calling this is optional.
        /// </summary>
        public async Task CreateAsync(string name)
        {
            await PatternsAsync(name).ConfigureAwait(false);
        }

        public async Task<long> IncrAsync(string name, string key, long amount = 1, NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(name).ConfigureAwait(false);
            return Utils.CounterIncr(_gl.ResolveActiveDb(connection), name, key, amount, patterns);
        }

        public async Task<long> DecrAsync(string name, string key, long amount = 1, NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(name).ConfigureAwait(false);
            return Utils.CounterDecr(_gl.ResolveActiveDb(connection), name, key, amount, patterns);
        }

        public async Task<long> SetAsync(string name, string key, long value, NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(name).ConfigureAwait(false);
            return Utils.CounterSet(_gl.ResolveActiveDb(connection), name, key, value, patterns);
        }

        public async Task<long> GetAsync(string name, string key, NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(name).ConfigureAwait(false);
            return Utils.CounterGet(_gl.ResolveActiveDb(connection), name, key, patterns);
        }

        public async Task<bool> DeleteAsync(string name, string key, NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(name).ConfigureAwait(false);
            return Utils.CounterDelete(_gl.ResolveActiveDb(connection), name, key, patterns);
        }

        public async Task<long> CountKeysAsync(string name, NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(name).ConfigureAwait(false);
            return Utils.CounterCountKeys(_gl.ResolveActiveDb(connection), name, patterns);
        }
    }
}
