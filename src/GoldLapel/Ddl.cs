using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace GoldLapel
{
    /// <summary>
    /// Canonical DDL + query-pattern entry for a single helper instance
    /// (e.g. stream "events"). Returned by <see cref="Ddl.FetchPatternsAsync"/>.
    /// </summary>
    public sealed class DdlEntry
    {
        public Dictionary<string, string> Tables { get; set; }
        public Dictionary<string, string> QueryPatterns { get; set; }
    }

    /// <summary>
    /// DDL API client — fetches canonical helper-table DDL + query patterns
    /// from the Rust proxy's dashboard port so the wrapper never hand-writes
    /// CREATE TABLE for helper families (streams, docs, counters, ...).
    ///
    /// See docs/wrapper-v0.2/SCHEMA-TO-CORE-PLAN.md in the goldlapel repo.
    /// </summary>
    public static class Ddl
    {
        private static readonly Dictionary<string, string> SupportedVersions = new Dictionary<string, string>
        {
            ["stream"] = "v1",
            ["doc_store"] = "v1",
        };

        /// <summary>
        /// Resolve the dashboard token for externally-launched proxies.
        /// Priority: GOLDLAPEL_DASHBOARD_TOKEN env > ~/.goldlapel/dashboard-token.
        /// </summary>
        public static string TokenFromEnvOrFile()
        {
            var envVar = Environment.GetEnvironmentVariable("GOLDLAPEL_DASHBOARD_TOKEN");
            if (!string.IsNullOrWhiteSpace(envVar))
                return envVar.Trim();
            var home = Environment.GetEnvironmentVariable("HOME")
                ?? Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
            if (string.IsNullOrEmpty(home)) return null;
            var path = Path.Combine(home, ".goldlapel", "dashboard-token");
            if (!File.Exists(path)) return null;
            try
            {
                var t = File.ReadAllText(path).Trim();
                return string.IsNullOrEmpty(t) ? null : t;
            }
            catch { return null; }
        }

        public static string SupportedVersion(string family)
        {
            if (!SupportedVersions.TryGetValue(family, out var v))
                throw new ArgumentException("Unknown helper family: " + family);
            return v;
        }

        /// <summary>
        /// Swappable HTTP layer — test-only seam the unit tests replace with
        /// a counting/faking implementation. Exposed via InternalsVisibleTo
        /// on GoldLapel.Tests so it is not part of the public API surface.
        /// </summary>
        internal static Func<string, string, byte[], CancellationToken, Task<(int status, string body)>> PostAsync =
            DefaultPostAsync;

        private static readonly HttpClient _http = new HttpClient { Timeout = TimeSpan.FromSeconds(10) };

        private static async Task<(int, string)> DefaultPostAsync(string url, string token, byte[] body, CancellationToken ct)
        {
            try
            {
                using var req = new HttpRequestMessage(HttpMethod.Post, url);
                req.Content = new ByteArrayContent(body);
                req.Content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/json");
                req.Headers.Add("X-GL-Dashboard", token);
                var resp = await _http.SendAsync(req, ct).ConfigureAwait(false);
                var text = await resp.Content.ReadAsStringAsync().ConfigureAwait(false);
                return ((int)resp.StatusCode, text);
            }
            catch (HttpRequestException e)
            {
                throw new InvalidOperationException(
                    "Gold Lapel dashboard not reachable at " + url + ": " + e.Message
                    + ". Is `goldlapel` running? The dashboard port must be open for "
                    + "helper families (streams, docs, ...) to work.", e);
            }
            catch (TaskCanceledException e)
            {
                throw new InvalidOperationException(
                    "Gold Lapel dashboard request timed out at " + url + ": " + e.Message, e);
            }
        }

        /// <summary>
        /// Fetch (and cache) the canonical DDL + query patterns for a helper.
        ///
        /// <paramref name="options"/> is an optional bag of per-family creation
        /// options (e.g. doc_store accepts <c>{"unlogged": true}</c>). Only
        /// honored on the first call for a given (family, name) — once the
        /// table exists on the proxy, its shape is fixed and subsequent
        /// options are silently ignored proxy-side (idempotent
        /// <c>CREATE TABLE IF NOT EXISTS</c>).
        /// </summary>
        public static async Task<DdlEntry> FetchPatternsAsync(
            ConcurrentDictionary<string, DdlEntry> cache,
            string family, string name,
            int dashboardPort, string dashboardToken,
            IDictionary<string, object> options = null,
            CancellationToken ct = default)
        {
            var key = family + ":" + name;
            if (cache.TryGetValue(key, out var cached)) return cached;

            if (string.IsNullOrEmpty(dashboardToken))
                throw new InvalidOperationException(
                    "No dashboard token available. Set GOLDLAPEL_DASHBOARD_TOKEN or let "
                    + "GoldLapel.StartAsync spawn the proxy (which provisions a token automatically).");
            if (dashboardPort <= 0)
                throw new InvalidOperationException(
                    "No dashboard port available. Gold Lapel's helper families ("
                    + family + ", ...) require the proxy's dashboard to be reachable.");

            var url = "http://127.0.0.1:" + dashboardPort + "/api/ddl/" + family + "/create";
            byte[] reqBody;
            if (options != null && options.Count > 0)
            {
                reqBody = JsonSerializer.SerializeToUtf8Bytes(new
                {
                    name,
                    schema_version = SupportedVersion(family),
                    options,
                });
            }
            else
            {
                reqBody = JsonSerializer.SerializeToUtf8Bytes(new
                {
                    name,
                    schema_version = SupportedVersion(family),
                });
            }

            var (status, respBody) = await PostAsync(url, dashboardToken, reqBody, ct).ConfigureAwait(false);

            if (status != 200)
            {
                string error = "unknown", detail = respBody;
                try
                {
                    using var doc = JsonDocument.Parse(respBody);
                    if (doc.RootElement.TryGetProperty("error", out var e)) error = e.GetString() ?? error;
                    if (doc.RootElement.TryGetProperty("detail", out var d)) detail = d.GetString() ?? detail;
                }
                catch { /* keep raw body */ }

                if (status == 409 && error == "version_mismatch")
                    throw new InvalidOperationException(
                        "Gold Lapel schema version mismatch for " + family + " '" + name + "': " + detail
                        + ". Upgrade the proxy or the wrapper so versions agree.");
                if (status == 403)
                    throw new InvalidOperationException(
                        "Gold Lapel dashboard rejected the DDL request (403). "
                        + "The dashboard token is missing or incorrect — check "
                        + "GOLDLAPEL_DASHBOARD_TOKEN or ~/.goldlapel/dashboard-token.");
                throw new InvalidOperationException(
                    "Gold Lapel DDL API " + family + "/" + name
                    + " failed with " + status + " " + error + ": " + detail);
            }

            var entry = new DdlEntry();
            using (var doc = JsonDocument.Parse(respBody))
            {
                if (doc.RootElement.TryGetProperty("tables", out var t))
                    entry.Tables = t.Deserialize<Dictionary<string, string>>();
                if (doc.RootElement.TryGetProperty("query_patterns", out var qp))
                    entry.QueryPatterns = qp.Deserialize<Dictionary<string, string>>();
            }

            // GetOrAdd handles concurrent fetch safely.
            return cache.GetOrAdd(key, entry);
        }

        private static readonly Regex _positional = new Regex(@"\$(\d+)", RegexOptions.Compiled);

        /// <summary>
        /// Translate the proxy's $1, $2, ... placeholders to Npgsql's @p1, @p2.
        /// Callers bind by index (see BindNumbered in Utils).
        /// </summary>
        public static string ToNpgsqlPlaceholders(string sql)
        {
            return _positional.Replace(sql, match => "@p" + match.Groups[1].Value);
        }
    }
}
