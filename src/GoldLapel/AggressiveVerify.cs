using System;
using System.Collections.Generic;
using System.Text.Json;

namespace GoldLapel
{
    /// <summary>
    /// Override the "aggressive verify" safety-net mode that bumps the
    /// per-connection cache key after every INSERT / UPDATE / DELETE /
    /// MERGE / TRUNCATE.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Trigger-internal SETs — e.g. a row-level-security trigger that does
    /// <c>PERFORM set_config('app.user_id', ..., true)</c> during an INSERT
    /// — mutate session state without anything observable on the wire. The
    /// wrapper's GUC state hash then diverges from server-truth and a
    /// subsequent <c>SELECT</c> could be served from a cache slot that
    /// belongs to a different identity. The safety net: after every DML,
    /// bump a per-connection monotonic counter (<see cref="ConnectionGucState.BumpDmlSeq"/>)
    /// that mixes into the cache-key hash. The next read is guaranteed to
    /// miss the wrapper's L1 cache and route to the proxy, which carries
    /// the authoritative session state.
    /// </para>
    /// <para>
    /// This replaces the earlier "smart auto-enable" design that ran a
    /// <c>pg_trigger</c> detection probe on first DML and cached the bool
    /// per-upstream. The probe was a round-trip-and-a-half tax on every
    /// new upstream, racy across concurrent connections, and never
    /// completely safe — false-negative detection (a trigger function
    /// whose body the regex didn't catch) silently leaked. The dml_seq
    /// bump costs nothing on the wire and is unconditionally correct: a
    /// post-DML read always re-fetches.
    /// </para>
    /// <para>
    /// <see cref="AggressiveVerifyMode.Auto"/> and <see cref="AggressiveVerifyMode.On"/>
    /// both enable the bump (Auto is no longer distinguishable from On in
    /// practice — it's preserved as the default for source-compatibility).
    /// <see cref="AggressiveVerifyMode.Off"/> is the opt-out for callers
    /// who have audited their schema and want maximum L1 hit rate; the
    /// wrapper emits a one-time stderr warning when Off is selected so
    /// operators can't miss the safety implications.
    /// </para>
    /// <para>
    /// HQ may force the bump on via the license payload's
    /// <c>aggressive_verify_active</c> claim, which overrides an Auto
    /// default but loses to an explicit Off override (auth-elevation tier
    /// principle: explicit operator choice wins).
    /// </para>
    /// </remarks>
    public enum AggressiveVerifyMode
    {
        /// <summary>
        /// Default: bump the per-connection dml_seq after every DML so
        /// post-write reads route to the proxy. License-claim path can
        /// also flip this on; equivalent to <see cref="On"/> in this
        /// version (kept as a distinct value for API stability).
        /// </summary>
        Auto = 0,

        /// <summary>
        /// Force the dml_seq bump on. Same effect as <see cref="Auto"/>
        /// today; preserved so paranoid callers can pin behaviour
        /// independently of any future default-shift.
        /// </summary>
        On = 1,

        /// <summary>
        /// Opt out of the dml_seq bump. The wrapper's L1 cache may serve
        /// stale rows if your schema has triggers that mutate session
        /// state on DML. Audit before flipping. A one-time stderr warning
        /// is emitted at construction so operators don't silently regress.
        /// </summary>
        Off = 2,
    }

    /// <summary>
    /// License-payload helpers for aggressive verify. The detection probe
    /// and per-upstream cache that lived here previously have been
    /// removed — the safety net is now an unconditional
    /// <see cref="ConnectionGucState.BumpDmlSeq"/> bump on every DML.
    /// </summary>
    internal static class AggressiveVerify
    {
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
