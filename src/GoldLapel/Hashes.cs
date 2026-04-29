using System.Collections.Generic;
using System.Data.Common;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;

namespace GoldLapel
{
    /// <summary>
    /// The hashes sub-API — accessible as <c>gl.Hashes</c>.
    ///
    /// Phase 5 of schema-to-core. The proxy's v1 hash schema is per-field rows
    /// (<c>hash_key</c>, <c>field</c>, <c>value</c>) — NOT the legacy JSONB
    /// blob-per-key shape. <see cref="GetAllAsync"/> aggregates rows back into
    /// a <c>Dictionary&lt;string, JsonElement&gt;</c> client-side. Every method
    /// threads <paramref name="hashKey"/> as the first positional arg after
    /// the namespace <paramref name="name"/>.
    /// </summary>
    public class HashesApi
    {
        private readonly GoldLapel _gl;

        internal HashesApi(GoldLapel gl) => _gl = gl;

        private Task<DdlEntry> PatternsAsync(string name)
        {
            Utils.ValidateIdentifier(name);
            var token = _gl._dashboardToken ?? Ddl.TokenFromEnvOrFile();
            return Ddl.FetchPatternsAsync(
                _gl._ddlCache, "hash", name,
                _gl.DashboardPort, token, options: null);
        }

        public async Task CreateAsync(string name)
        {
            await PatternsAsync(name).ConfigureAwait(false);
        }

        /// <summary>
        /// Set a field's value (single-row UPSERT). The value is JSON-encoded
        /// at the wrapper edge so callers can store arbitrary structured
        /// payloads — pass any object that <see cref="JsonSerializer"/>
        /// supports (string, number, bool, dict, list, …). Returns the
        /// just-stored value as a <see cref="JsonElement"/>.
        /// </summary>
        public async Task<JsonElement?> SetAsync(string name, string hashKey, string field, object value,
            NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(name).ConfigureAwait(false);
            return Utils.HashSet(_gl.ResolveActiveDb(connection), name, hashKey, field, value, patterns);
        }

        public async Task<JsonElement?> GetAsync(string name, string hashKey, string field,
            NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(name).ConfigureAwait(false);
            return Utils.HashGet(_gl.ResolveActiveDb(connection), name, hashKey, field, patterns);
        }

        /// <summary>
        /// Reassemble every <c>(field, value)</c> under <paramref name="hashKey"/>
        /// into a <see cref="Dictionary{TKey, TValue}"/>. Empty dict if the key
        /// has no fields. Each value is decoded as a <see cref="JsonElement"/>
        /// — callers index into it via <see cref="JsonElement.GetString"/>,
        /// <see cref="JsonElement.GetInt32"/>, etc.
        /// </summary>
        public async Task<Dictionary<string, JsonElement>> GetAllAsync(string name, string hashKey,
            NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(name).ConfigureAwait(false);
            return Utils.HashGetAll(_gl.ResolveActiveDb(connection), name, hashKey, patterns);
        }

        public async Task<List<string>> KeysAsync(string name, string hashKey,
            NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(name).ConfigureAwait(false);
            return Utils.HashKeys(_gl.ResolveActiveDb(connection), name, hashKey, patterns);
        }

        public async Task<List<JsonElement>> ValuesAsync(string name, string hashKey,
            NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(name).ConfigureAwait(false);
            return Utils.HashValues(_gl.ResolveActiveDb(connection), name, hashKey, patterns);
        }

        public async Task<bool> ExistsAsync(string name, string hashKey, string field,
            NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(name).ConfigureAwait(false);
            return Utils.HashExists(_gl.ResolveActiveDb(connection), name, hashKey, field, patterns);
        }

        public async Task<bool> DeleteAsync(string name, string hashKey, string field,
            NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(name).ConfigureAwait(false);
            return Utils.HashDelete(_gl.ResolveActiveDb(connection), name, hashKey, field, patterns);
        }

        public async Task<long> LenAsync(string name, string hashKey, NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(name).ConfigureAwait(false);
            return Utils.HashLen(_gl.ResolveActiveDb(connection), name, hashKey, patterns);
        }
    }
}
