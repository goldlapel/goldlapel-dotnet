using System.Collections.Generic;
using System.Threading.Tasks;
using Npgsql;

namespace GoldLapel
{
    /// <summary>
    /// The streams sub-API — accessible as <c>gl.Streams</c>.
    ///
    /// Wraps the wire-level stream methods in a sub-API instance held on the
    /// parent <see cref="GoldLapel"/> client. The instance shares all state
    /// (license, dashboard token, http session, conn) by reference back to
    /// the parent — no duplication.
    ///
    /// This is the canonical sub-API shape for the schema-to-core wrapper
    /// rollout. Other namespaces (cache, search, queues, counters, hashes,
    /// zsets, geo, …) stay flat for now; they migrate to nested form
    /// one-at-a-time as their own schema-to-core phase fires.
    /// </summary>
    public class StreamsApi
    {
        private readonly GoldLapel _gl;

        internal StreamsApi(GoldLapel gl) => _gl = gl;

        /// <summary>
        /// Fetch (and cache) canonical stream DDL + query patterns from the
        /// proxy. Cache lives on the parent <see cref="GoldLapel"/> instance —
        /// see <see cref="Ddl.FetchPatternsAsync"/>.
        /// </summary>
        private Task<DdlEntry> PatternsAsync(string stream)
        {
            Utils.ValidateIdentifier(stream);
            var token = _gl._dashboardToken ?? Ddl.TokenFromEnvOrFile();
            return Ddl.FetchPatternsAsync(_gl._ddlCache, "stream", stream, _gl.DashboardPort, token);
        }

        public async Task<long> AddAsync(string stream, string payload, NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(stream).ConfigureAwait(false);
            return Utils.StreamAdd(_gl.ResolveActiveDb(connection), stream, payload, patterns);
        }

        public async Task CreateGroupAsync(string stream, string group, NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(stream).ConfigureAwait(false);
            Utils.StreamCreateGroup(_gl.ResolveActiveDb(connection), stream, group, patterns);
        }

        public async Task<List<Dictionary<string, object>>> ReadAsync(string stream,
            string group, string consumer, int count = 1, NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(stream).ConfigureAwait(false);
            return Utils.StreamRead(_gl.ResolveActiveDb(connection), stream, group, consumer, count, patterns);
        }

        public async Task<bool> AckAsync(string stream, string group, long messageId,
            NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(stream).ConfigureAwait(false);
            return Utils.StreamAck(_gl.ResolveActiveDb(connection), stream, group, messageId, patterns);
        }

        public async Task<List<Dictionary<string, object>>> ClaimAsync(string stream,
            string group, string consumer, long minIdleMs = 60000, NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(stream).ConfigureAwait(false);
            return Utils.StreamClaim(_gl.ResolveActiveDb(connection), stream, group, consumer, minIdleMs, patterns);
        }
    }
}
