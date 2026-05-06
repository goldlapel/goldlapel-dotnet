using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text.Json;

namespace GoldLapel
{
    /// <summary>
    /// Override the smart-auto-enable logic for "aggressive verify" — the
    /// safety-net mode that schedules an async <c>pg_settings</c> verify
    /// after every INSERT / UPDATE / DELETE / MERGE / TRUNCATE in addition
    /// to the default function-call trigger.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Aggressive verify is only useful when the user's schema has triggers
    /// (or extension hooks) that mutate session state — e.g. a row-level
    /// security trigger that does <c>PERFORM set_config('app.user_id', ..., true)</c>
    /// during an INSERT. Without it, that trigger-internal SET escapes our
    /// wire-observation parser and the wrapper's GUC state hash diverges
    /// from server-truth.
    /// </para>
    /// <para>
    /// The cost is small (~1ms per write — the verify is async and
    /// fire-and-forget) but non-zero, so we only flip it on when there's a
    /// reason to. <see cref="AggressiveVerifyMode.Auto"/> (the default)
    /// runs a one-shot detection probe lazily on the first DML observed
    /// per upstream and caches the result process-wide. Pass
    /// <see cref="AggressiveVerifyMode.On"/> to force on (paranoid
    /// mode) or <see cref="AggressiveVerifyMode.Off"/> to force off
    /// (no-trigger schema, you know your data).
    /// </para>
    /// <para>
    /// HQ may also flip this on via the license payload's
    /// <c>aggressive_verify_active</c> claim, which overrides Auto detection
    /// (but not an explicit On/Off override).
    /// </para>
    /// </remarks>
    public enum AggressiveVerifyMode
    {
        /// <summary>
        /// Smart auto-detect: probe pg_trigger / pg_proc once per
        /// upstream (on the first DML the wrapper observes for that
        /// upstream) and cache the result process-wide. Subsequent
        /// connections to the same upstream resolve eagerly without a
        /// round-trip.
        /// </summary>
        Auto = 0,

        /// <summary>
        /// Force aggressive verify ON. Every DML schedules a post-call
        /// async verify.
        /// </summary>
        On = 1,

        /// <summary>
        /// Force aggressive verify OFF. Only function calls / stored
        /// procedures schedule async verify (the Wave 1 default).
        /// </summary>
        Off = 2,
    }

    /// <summary>
    /// Resolution + caching for aggressive-verify mode. Per-upstream
    /// detection runs at most once per process; the result is cached in a
    /// process-wide <see cref="ConcurrentDictionary{TKey, TValue}"/>
    /// keyed by the normalized upstream connection-string key.
    /// </summary>
    internal static class AggressiveVerify
    {
        // Probe: is there any user-defined trigger whose function body
        // *might* mutate session state? Cheap heuristic — we look for
        // function bodies that contain `set_config(` or a bare `SET ` /
        // `PERFORM SET ` inside a non-internal trigger function. False
        // positives are fine (we just turn on the safety net unnecessarily,
        // costing ~1ms/write); false negatives are the bug. The query is
        // intentionally tolerant — `~*` is case-insensitive regex match,
        // and we look at trigger functions only (tgisinternal=false skips
        // PG's own constraint triggers).
        //
        // Returns a single boolean column: t / f.
        internal const string DetectionSql =
            "SELECT EXISTS (" +
            "SELECT 1 FROM pg_trigger t " +
            "JOIN pg_proc p ON t.tgfoid = p.oid " +
            "WHERE NOT t.tgisinternal " +
            "AND (p.prosrc ~* '(^|[^a-zA-Z0-9_])set_config[[:space:]]*\\(' " +
            "OR p.prosrc ~* '(^|[^a-zA-Z0-9_])SET[[:space:]]+[a-zA-Z_]'))";

        // Process-wide cache. Key: normalised connection-string upstream
        // (Host+Port+Database, lower-cased). Value: detection result —
        // true if at least one trigger looks like it mutates session state.
        // ConcurrentDictionary so concurrent first-Open() races resolve
        // safely (worst case, two probes run before one wins; both cache
        // the same answer).
        private static readonly ConcurrentDictionary<string, bool> _cache =
            new ConcurrentDictionary<string, bool>(StringComparer.Ordinal);

        /// <summary>
        /// Run the detection probe (or read from cache) for the given
        /// connection's upstream. Errors return false — concierge, not
        /// bouncer. The detection path is best-effort; if we can't probe,
        /// we don't synthesize a paranoid default.
        /// </summary>
        internal static bool Detect(DbConnection inner)
        {
            if (inner == null) return false;
            var key = UpstreamKey(inner);
            if (key == null) return false;

            if (_cache.TryGetValue(key, out var cached)) return cached;

            bool detected;
            try
            {
                detected = RunDetectionQuery(inner);
            }
            catch
            {
                // Probe failed (no Pg, permissions, network) — don't
                // synthesize a paranoid default. The Wave 1 verify path
                // remains in place; trigger-internal SETs are a known
                // gap that aggressive-verify is the *opt-in* safety net for.
                detected = false;
            }
            _cache.TryAdd(key, detected);
            return detected;
        }

        /// <summary>
        /// Test hook — drop the per-upstream cache so unit tests can
        /// re-run detection deterministically. Not called from product
        /// code.
        /// </summary>
        internal static void ResetCache()
        {
            _cache.Clear();
        }

        /// <summary>
        /// Peek whether a key is cached (and what value). Used by
        /// <see cref="CachedConnection"/>'s cheap-path resolution and by
        /// tests asserting cache state.
        /// </summary>
        internal static bool TryGetCached(string upstreamKey, out bool value)
        {
            return _cache.TryGetValue(upstreamKey, out value);
        }

        /// <summary>
        /// Build a stable per-upstream cache key from a DbConnection's
        /// connection string. Different drivers expose different keyword
        /// sets, but Host / Port / Database are the universal trio. We
        /// normalise via DbConnectionStringBuilder so quoting and case
        /// don't fragment the cache.
        /// </summary>
        internal static string UpstreamKey(DbConnection inner)
        {
            if (inner == null) return null;
            string cs;
            try { cs = inner.ConnectionString; }
            catch { return null; }
            if (string.IsNullOrEmpty(cs)) return null;

            try
            {
                var b = new DbConnectionStringBuilder { ConnectionString = cs };
                string host = ReadKey(b, "host", "server", "data source");
                string port = ReadKey(b, "port");
                string db = ReadKey(b, "database", "initial catalog");
                if (host == null && port == null && db == null)
                {
                    // No recognized keys — fall back to the raw connection
                    // string. Better to over-fragment the cache than to
                    // collapse two distinct upstreams onto one key.
                    return cs;
                }
                return (host ?? "") + "|" + (port ?? "") + "|" + (db ?? "");
            }
            catch
            {
                return cs;
            }
        }

        private static string ReadKey(DbConnectionStringBuilder b, params string[] keys)
        {
            foreach (var k in keys)
            {
                if (b.ContainsKey(k))
                {
                    var v = b[k]?.ToString();
                    if (!string.IsNullOrEmpty(v))
                        return v.ToLowerInvariant();
                }
            }
            return null;
        }

        // Run DetectionSql against the inner connection. Returns true iff
        // the single boolean column comes back true.
        private static bool RunDetectionQuery(DbConnection inner)
        {
            if (inner.State != ConnectionState.Open) return false;
            using (var cmd = inner.CreateCommand())
            {
                cmd.CommandText = DetectionSql;
                cmd.CommandTimeout = 5;
                using (var reader = cmd.ExecuteReader())
                {
                    if (!reader.Read()) return false;
                    if (reader.IsDBNull(0)) return false;
                    var v = reader.GetValue(0);
                    if (v is bool b) return b;
                    var s = v?.ToString();
                    return s != null && (s.Equals("true", StringComparison.OrdinalIgnoreCase)
                                         || s == "t" || s == "1");
                }
            }
        }

        // Read aggressive_verify_active from a license payload (already
        // parsed by the caller). Accepts either a boolean or any of the
        // permissive truthy strings ("true", "t", "1", "yes"). Missing or
        // unrecognised values return false.
        internal static bool LicenseClaimsActive(IReadOnlyDictionary<string, object> licensePayload)
        {
            if (licensePayload == null) return false;
            if (!licensePayload.TryGetValue("aggressive_verify_active", out var raw) || raw == null)
                return false;
            return CoerceBool(raw);
        }

        /// <summary>
        /// Parse a license payload from raw JSON text — convenience for
        /// callers that have the on-disk license file as a string.
        /// Returns null on parse failure (caller defaults to no-claim).
        /// </summary>
        internal static IReadOnlyDictionary<string, object> ParseLicensePayload(string json)
        {
            if (string.IsNullOrWhiteSpace(json)) return null;
            try
            {
                using (var doc = JsonDocument.Parse(json))
                {
                    if (doc.RootElement.ValueKind != JsonValueKind.Object) return null;
                    var dict = new Dictionary<string, object>(StringComparer.Ordinal);
                    foreach (var prop in doc.RootElement.EnumerateObject())
                    {
                        dict[prop.Name] = JsonValueToObject(prop.Value);
                    }
                    return dict;
                }
            }
            catch
            {
                return null;
            }
        }

        private static object JsonValueToObject(JsonElement el)
        {
            switch (el.ValueKind)
            {
                case JsonValueKind.True: return true;
                case JsonValueKind.False: return false;
                case JsonValueKind.Null: return null;
                case JsonValueKind.Number:
                    if (el.TryGetInt64(out var i)) return i;
                    return el.GetDouble();
                case JsonValueKind.String: return el.GetString();
                default: return el.GetRawText();
            }
        }

        private static bool CoerceBool(object raw)
        {
            if (raw is bool b) return b;
            var s = raw.ToString();
            if (string.IsNullOrEmpty(s)) return false;
            s = s.Trim();
            return s.Equals("true", StringComparison.OrdinalIgnoreCase)
                || s.Equals("t", StringComparison.OrdinalIgnoreCase)
                || s.Equals("yes", StringComparison.OrdinalIgnoreCase)
                || s.Equals("on", StringComparison.OrdinalIgnoreCase)
                || s == "1";
        }
    }
}
