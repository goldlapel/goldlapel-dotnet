using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;
using Npgsql;

namespace GoldLapel
{
    /// <summary>
    /// The documents sub-API — accessible as <c>gl.Documents</c>.
    ///
    /// Wraps the doc-store methods in a sub-API instance held on the parent
    /// <see cref="GoldLapel"/> client. The instance shares all state (license,
    /// dashboard token, http session, conn, DDL pattern cache) by reference
    /// back to the parent — no duplication.
    ///
    /// The proxy owns doc-store DDL (Phase 4 of schema-to-core). Each call
    /// here:
    ///
    /// <list type="number">
    /// <item>Calls <c>POST /api/ddl/doc_store/create</c> (idempotent) to
    /// materialize the canonical <c>_goldlapel.doc_&lt;name&gt;</c> table and
    /// fetch its query patterns.</item>
    /// <item>Caches <c>(tables, query_patterns)</c> on the parent
    /// <see cref="GoldLapel"/> instance for the session's lifetime — one HTTP
    /// round-trip per <c>(family, name)</c> per session.</item>
    /// <item>Hands the patterns off to the existing <see cref="Utils"/>
    /// <c>Doc*</c> functions so they execute against the canonical table name
    /// instead of <c>CREATE</c>-ing their own.</item>
    /// </list>
    ///
    /// Sub-API class shape mirrors <see cref="StreamsApi"/> — this is the
    /// canonical pattern for the wrapper rollout. Other namespaces (cache,
    /// search, queues, counters, hashes, zsets, geo, …) stay flat for now;
    /// they migrate to nested form one-at-a-time as their own schema-to-core
    /// phase fires.
    /// </summary>
    public class DocumentsApi
    {
        // Hold a back-reference to the parent client. Never copy lifecycle
        // state (token, port, conn) onto this instance — always read through
        // `_gl` so a config change on the parent (e.g. restart with a new
        // dashboard token) is reflected immediately on the next call.
        private readonly GoldLapel _gl;

        internal DocumentsApi(GoldLapel gl) => _gl = gl;

        /// <summary>
        /// Fetch (and cache) canonical doc-store DDL + query patterns from
        /// the proxy. Cache lives on the parent <see cref="GoldLapel"/>
        /// instance.
        ///
        /// <paramref name="unlogged"/> is a creation-time option; passed only
        /// on the first call for a given <c>(family, name)</c> since the
        /// proxy's <c>CREATE TABLE IF NOT EXISTS</c> makes subsequent calls
        /// no-op DDL-wise. If a caller flips <paramref name="unlogged"/>
        /// across calls in the same session, the table's storage type is
        /// whatever it was on first create — wrappers don't migrate it.
        /// </summary>
        private Task<DdlEntry> PatternsAsync(string collection, bool unlogged = false)
        {
            Utils.ValidateIdentifier(collection);
            var token = _gl._dashboardToken ?? Ddl.TokenFromEnvOrFile();
            IDictionary<string, object> options = unlogged
                ? new Dictionary<string, object> { ["unlogged"] = true }
                : null;
            return Ddl.FetchPatternsAsync(
                _gl._ddlCache, "doc_store", collection,
                _gl.DashboardPort, token, options);
        }

        // -- Collection lifecycle ---------------------------------------

        /// <summary>
        /// Eagerly materialize the doc-store table. Other methods will also
        /// materialize on first use, so calling this is optional — provided
        /// for callers that want explicit setup at startup time.
        /// </summary>
        public async Task CreateCollectionAsync(string collection, bool unlogged = false)
        {
            await PatternsAsync(collection, unlogged).ConfigureAwait(false);
        }

        // -- CRUD --------------------------------------------------------

        public async Task<Dictionary<string, object>> InsertAsync(string collection, string documentJson, NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(collection).ConfigureAwait(false);
            return Utils.DocInsert(_gl.ResolveActiveDb(connection), collection, documentJson, patterns);
        }

        public async Task<List<Dictionary<string, object>>> InsertManyAsync(string collection, List<string> documents, NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(collection).ConfigureAwait(false);
            return Utils.DocInsertMany(_gl.ResolveActiveDb(connection), collection, documents, patterns);
        }

        public async Task<List<Dictionary<string, object>>> FindAsync(string collection,
            string filterJson = null, Dictionary<string, int> sort = null, int? limit = null, int? skip = null,
            NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(collection).ConfigureAwait(false);
            return Utils.DocFind(_gl.ResolveActiveDb(connection), collection, patterns, filterJson, sort, limit, skip);
        }

        /// <summary>
        /// Server-side cursor variant of <see cref="FindAsync"/>. Returns an
        /// <see cref="IEnumerable{T}"/> so consumers can stream rows. The
        /// pattern fetch is awaited eagerly before iteration starts — once
        /// patterns are in the parent client's cache, subsequent calls skip
        /// the round-trip.
        /// </summary>
        public async Task<IEnumerable<Dictionary<string, object>>> FindCursorAsync(string collection,
            string filterJson = null, string sortJson = null, int? limit = null, int? skip = null,
            int batchSize = 100, NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(collection).ConfigureAwait(false);
            return Utils.DocFindCursor(_gl.ResolveActiveDb(connection), collection, patterns,
                filterJson, sortJson, limit, skip, batchSize);
        }

        public async Task<Dictionary<string, object>> FindOneAsync(string collection,
            string filterJson = null, NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(collection).ConfigureAwait(false);
            return Utils.DocFindOne(_gl.ResolveActiveDb(connection), collection, patterns, filterJson);
        }

        public async Task<int> UpdateAsync(string collection, string filterJson, string updateJson,
            NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(collection).ConfigureAwait(false);
            return Utils.DocUpdate(_gl.ResolveActiveDb(connection), collection, filterJson, updateJson, patterns);
        }

        public async Task<int> UpdateOneAsync(string collection, string filterJson, string updateJson,
            NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(collection).ConfigureAwait(false);
            return Utils.DocUpdateOne(_gl.ResolveActiveDb(connection), collection, filterJson, updateJson, patterns);
        }

        public async Task<int> DeleteAsync(string collection, string filterJson, NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(collection).ConfigureAwait(false);
            return Utils.DocDelete(_gl.ResolveActiveDb(connection), collection, filterJson, patterns);
        }

        public async Task<int> DeleteOneAsync(string collection, string filterJson, NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(collection).ConfigureAwait(false);
            return Utils.DocDeleteOne(_gl.ResolveActiveDb(connection), collection, filterJson, patterns);
        }

        public async Task<long> CountAsync(string collection, string filterJson = null, NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(collection).ConfigureAwait(false);
            return Utils.DocCount(_gl.ResolveActiveDb(connection), collection, patterns, filterJson);
        }

        public async Task<Dictionary<string, object>> FindOneAndUpdateAsync(string collection, string filterJson,
            string updateJson, NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(collection).ConfigureAwait(false);
            return Utils.DocFindOneAndUpdate(_gl.ResolveActiveDb(connection), collection, filterJson, updateJson, patterns);
        }

        public async Task<Dictionary<string, object>> FindOneAndDeleteAsync(string collection, string filterJson,
            NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(collection).ConfigureAwait(false);
            return Utils.DocFindOneAndDelete(_gl.ResolveActiveDb(connection), collection, filterJson, patterns);
        }

        public async Task<List<string>> DistinctAsync(string collection, string field, string filterJson = null,
            NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(collection).ConfigureAwait(false);
            return Utils.DocDistinct(_gl.ResolveActiveDb(connection), collection, field, patterns, filterJson);
        }

        public async Task CreateIndexAsync(string collection, List<string> keys = null, NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(collection).ConfigureAwait(false);
            Utils.DocCreateIndex(_gl.ResolveActiveDb(connection), collection, patterns, keys);
        }

        /// <summary>
        /// Run a Mongo-style aggregation pipeline.
        ///
        /// <c>$lookup.from</c> references are resolved to their canonical
        /// proxy tables (<c>_goldlapel.doc_&lt;name&gt;</c>) — each unique
        /// <c>from</c> collection triggers an idempotent describe/create
        /// against the proxy and is cached for the session.
        /// </summary>
        public async Task<List<Dictionary<string, object>>> AggregateAsync(string collection, string pipelineJson,
            NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(collection).ConfigureAwait(false);
            // Walk the pipeline once to find every $lookup.from collection,
            // fetch patterns for each (cached after first call), and pass
            // the resolved map down to DocAggregate.
            var lookupTables = new Dictionary<string, string>();
            var lookupNames = ExtractLookupFromNames(pipelineJson);
            foreach (var fromName in lookupNames)
            {
                if (lookupTables.ContainsKey(fromName)) continue;
                var lp = await PatternsAsync(fromName).ConfigureAwait(false);
                if (lp != null && lp.Tables != null && lp.Tables.TryGetValue("main", out var t))
                    lookupTables[fromName] = t;
            }
            return Utils.DocAggregate(_gl.ResolveActiveDb(connection), collection, pipelineJson, patterns, lookupTables);
        }

        /// <summary>
        /// Pull the <c>from</c> field from every <c>$lookup</c> stage in the
        /// pipeline. Reuses <see cref="Utils.ParsePipeline"/> for a single
        /// source-of-truth parser; the aggregate verb re-parses internally,
        /// which is cheap given pipeline JSON is small.
        /// </summary>
        private static List<string> ExtractLookupFromNames(string pipelineJson)
        {
            var names = new List<string>();
            if (string.IsNullOrEmpty(pipelineJson)) return names;
            try
            {
                var stages = Utils.ParsePipeline(pipelineJson);
                foreach (var stage in stages)
                {
                    if (!stage.TryGetValue("_type", out var stype) || stype != "$lookup") continue;
                    if (stage.TryGetValue("from", out var fromName) && !string.IsNullOrEmpty(fromName))
                        names.Add(fromName);
                }
            }
            catch
            {
                // Pipeline parse errors are surfaced by DocAggregate itself —
                // don't double-throw here.
            }
            return names;
        }

        // -- Watch / TTL / capped ----------------------------------------

        public async Task WatchAsync(string collection, Action<string, string> callback, bool blocking = true,
            NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(collection).ConfigureAwait(false);
            Utils.DocWatch(_gl.ResolveActiveDb(connection), collection, callback, patterns, blocking);
        }

        public async Task UnwatchAsync(string collection, NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(collection).ConfigureAwait(false);
            Utils.DocUnwatch(_gl.ResolveActiveDb(connection), collection, patterns);
        }

        public async Task CreateTtlIndexAsync(string collection, int expireAfterSeconds, string field = "created_at",
            NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(collection).ConfigureAwait(false);
            Utils.DocCreateTtlIndex(_gl.ResolveActiveDb(connection), collection, expireAfterSeconds, patterns, field);
        }

        public async Task RemoveTtlIndexAsync(string collection, NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(collection).ConfigureAwait(false);
            Utils.DocRemoveTtlIndex(_gl.ResolveActiveDb(connection), collection, patterns);
        }

        public async Task CreateCappedAsync(string collection, int maxDocuments, NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(collection).ConfigureAwait(false);
            Utils.DocCreateCapped(_gl.ResolveActiveDb(connection), collection, maxDocuments, patterns);
        }

        public async Task RemoveCapAsync(string collection, NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(collection).ConfigureAwait(false);
            Utils.DocRemoveCap(_gl.ResolveActiveDb(connection), collection, patterns);
        }
    }
}
