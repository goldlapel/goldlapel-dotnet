using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace GoldLapel
{
    /// <summary>
    /// Construction-time options for <see cref="GoldLapel.StartAsync(string, Action{GoldLapelOptions})"/>.
    /// Populated via a configurator lambda:
    /// <code>
    /// await GoldLapel.StartAsync(url, opts => { opts.ProxyPort = 7932; opts.LogLevel = "info"; });
    /// </code>
    /// </summary>
    public class GoldLapelOptions
    {
        /// <summary>
        /// Proxy listen port. When null (default), the lowest port from 7932
        /// up is chosen whose dashboard port (port + 1) is free too, skipping
        /// ports held by this process's other proxies or by any other program.
        /// An explicit port that another proxy of this process holds is
        /// refused with an error; one held by another program makes the proxy
        /// refuse to start, and its message is in the exception.
        /// </summary>
        public int? ProxyPort { get; set; }

        /// <summary>
        /// Dashboard listen port. When null (default), the port is derived as
        /// the proxy port + 1. Set to 0 to disable the dashboard entirely.
        /// </summary>
        public int? DashboardPort { get; set; }

        /// <summary>
        /// Log level for the proxy. Accepted values: <c>"trace"</c>, <c>"debug"</c>,
        /// <c>"info"</c>, <c>"warn"</c>, <c>"error"</c>. Only trace/debug/info produce
        /// visible output; warn/error are the binary's default level. Translated to
        /// <c>-v</c>/<c>-vv</c>/<c>-vvv</c> when emitting argv.
        /// </summary>
        public string LogLevel { get; set; }

        /// <summary>Operating mode (e.g. <c>"waiter"</c>, <c>"consideration"</c>). Passed as <c>--mode</c>.</summary>
        public string Mode { get; set; }

        /// <summary>License file path. Passed as <c>--license</c>.</summary>
        public string License { get; set; }

        /// <summary>
        /// Client identifier (emitted via <c>GOLDLAPEL_CLIENT</c> env var so the proxy can
        /// tag telemetry with the originating wrapper). Defaults to <c>"dotnet"</c> when unset.
        /// </summary>
        public string Client { get; set; }

        /// <summary>
        /// Config file path. Passed as <c>--config</c> so the Rust binary parses the TOML.
        /// Distinct from <see cref="Config"/> (structured map of tuning keys).
        /// </summary>
        public string ConfigFile { get; set; }

        /// <summary>Structured config map passed as CLI flags (e.g. {"poolSize", 50}).</summary>
        public Dictionary<string, object> Config { get; set; }

        /// <summary>Additional raw CLI flags appended to the binary invocation.</summary>
        public string[] ExtraArgs { get; set; }

        /// <summary>
        /// When <c>true</c>, suppress the startup banner entirely. When <c>false</c>
        /// (the default), the banner is written to <c>Console.Error</c> (stderr) so
        /// it doesn't pollute stdout consumers (ASP.NET Core logs, piped app output,
        /// shells redirecting stdout, etc.).
        /// </summary>
        public bool Silent { get; set; }

        /// <summary>
        /// When <c>true</c>, opt into the mesh at startup. HQ enforces the license;
        /// if the current plan doesn't cover mesh the proxy continues running
        /// normally without clustering (concierge, not bouncer). Equivalent CLI
        /// flag: <c>--mesh</c>. Env: <c>GOLDLAPEL_MESH</c>. TOML: <c>[mesh] enabled</c>.
        /// </summary>
        public bool Mesh { get; set; }

        /// <summary>
        /// Optional mesh tag — instances sharing a tag cluster together. When
        /// unset, mesh-enabled instances join the account's default mesh.
        /// Equivalent CLI flag: <c>--mesh-tag</c>. Env: <c>GOLDLAPEL_MESH_TAG</c>.
        /// </summary>
        public string MeshTag { get; set; }

        /// <summary>
        /// When <c>true</c>, disable the proxy's result cache (the
        /// origin-side L2 cache). Maps 1:1 to the proxy's
        /// <c>--disable-proxy-cache</c> CLI flag. Promoted to a top-level
        /// option (Model B): the caller flips a single boolean instead of
        /// reaching into the structured <c>Config</c> map.
        /// </summary>
        public bool DisableProxyCache { get; set; }

        /// <summary>
        /// When <c>true</c>, disable the proxy's SQL-optimization master
        /// switch (copy-rewrite, expression-rewrite, N+1, coalescing). Maps 1:1 to
        /// <c>--disable-sqloptimize</c>.
        /// </summary>
        public bool DisableSqloptimize { get; set; }

        /// <summary>
        /// When <c>true</c>, disable the proxy's auto-index discovery
        /// (btree / trigram / expression / partial). Maps 1:1 to
        /// <c>--disable-auto-indexes</c>.
        /// </summary>
        public bool DisableAutoIndexes { get; set; }
    }

    /// <summary>
    /// Factory + handle for a Gold Lapel proxy process. Construct via
    /// <see cref="StartAsync(string, Action{GoldLapelOptions})"/>; dispose with
    /// <c>await using</c> (or call <see cref="DisposeAsync"/> directly) to stop the proxy.
    /// </summary>
    public class GoldLapel : IAsyncDisposable, IDisposable
    {
        internal const int DefaultProxyPort = 7932;
        internal const int DefaultDashboardPort = 7933;
        internal const long StartupTimeoutMs = 10000;
        internal const long StartupPollIntervalMs = 50;
        private const int GracefulTimeoutMs = 5000;
        private const int BusyPortGraceMs = 2000;
        private const int StderrTailChars = 4000;

        // Keys that are valid inside the structured `Config` map. Top-level
        // concepts (proxyPort, dashboardPort, logLevel, mode,
        // license, client, configFile) live on GoldLapelOptions directly and
        // are NOT accepted here — passing them through Config raises.
        private static readonly HashSet<string> ValidConfigKeys = new HashSet<string>(new[]
        {
            "minPatternCount", "deepPaginationThreshold",
            "reportIntervalSecs", "proxyCacheSize", "batchCacheSize",
            "batchCacheTtlSecs", "poolSize", "poolTimeoutSecs",
            "poolMode", "mgmtIdleTimeout", "fallback", "readAfterWriteSecs",
            "n1Threshold", "n1WindowMs", "n1CrossThreshold",
            "tlsCert", "tlsKey", "tlsClientCa",
            "disableBtreeIndexes",
            "disableTrigramIndexes", "disableExpressionIndexes",
            "disablePartialIndexes", "disableRewritePreparedCache",
            "disablePool",
            "disableN1", "disableN1CrossConnection",
            "disableCoalescing", "replica", "excludeTables"
        });

        // Config keys that used to exist, with why they are gone, so a stale
        // caller gets a reason instead of a bare "unknown key".
        private static readonly Dictionary<string, string> RemovedConfigKeys = new Dictionary<string, string>
        {
            { "invalidationPort", "it was removed with the in-process cache" },
            { "disableNativeCache", "it was removed with the in-process cache" },
            { "nativeCacheSize", "it was removed with the in-process cache" },
            { "aggressiveVerify", "it was removed with the in-process cache" },
            { "licensePayload", "it was removed with the in-process cache" },
            { "refreshIntervalSecs", "it was removed with materialized views" },
            { "patternTtlSecs", "it was removed with materialized views" },
            { "maxTablesPerView", "it was removed with materialized views" },
            { "maxColumnsPerView", "it was removed with materialized views" },
            { "disableConsolidation", "it was removed with materialized views" },
            { "disableRewrite", "it was removed with materialized views" },
            { "disableShadowMode", "it was removed with materialized views" },
            { "disableMatviews", "it was removed with materialized views" },
            { "enableCoalescing", "coalescing is on by default; use disableCoalescing to turn it off" },
        };

        private static readonly HashSet<string> BooleanKeys = new HashSet<string>(new[]
        {
            "disableBtreeIndexes",
            "disableTrigramIndexes", "disableExpressionIndexes",
            "disablePartialIndexes", "disableRewritePreparedCache",
            "disablePool",
            "disableN1", "disableN1CrossConnection",
            "disableCoalescing"
        });

        private static readonly HashSet<string> ListKeys = new HashSet<string>(new[]
        {
            "replica", "excludeTables"
        });

        // AsyncLocal: scoping connection overrides across awaits within UsingAsync.
        // Each wrapper method consults _scopedConnection first, then falls back to
        // the internal _conn opened during StartAsync.
        // Instance-scoped (not static): when two GoldLapel instances coexist, a scope
        // opened on one must not bleed into the other.
        private readonly AsyncLocal<DbConnection> _scopedConnection = new AsyncLocal<DbConnection>();

        private readonly string _upstream;
        // Resolved when the proxy is registered: explicit, auto-assigned, or
        // (on reuse) the running proxy's.
        private int _proxyPort;
        private int _dashboardPort;
        private readonly bool _proxyPortExplicit;
        // True only when the user passed a non-null DashboardPort.
        // Used at spawn time to decide whether to emit the flag explicitly (vs
        // letting the Rust binary apply its own default). Keeping this separate
        // from the resolved port lets `DashboardPort` expose the effective
        // value unambiguously.
        private readonly bool _dashboardPortExplicit;
        private readonly bool _clientTls;
        private readonly string _logLevel;
        private readonly string _mode;
        private readonly string _license;
        private readonly string _client;
        private readonly string _configFile;
        private readonly string[] _extraArgs;
        private readonly Dictionary<string, object> _config;
        private readonly bool _silent;
        private readonly bool _mesh;
        private readonly string _meshTag;
        private readonly bool _disableProxyCache;
        private readonly bool _disableSqloptimize;
        private readonly bool _disableAutoIndexes;
        private Process _process;
        private ProxyEntry _entry;
        private string _proxyUrl;
        private bool _disposed;
        private NpgsqlConnection _conn;  // eagerly opened internal connection
        // Dashboard token + DDL cache — see Ddl.cs.
        internal string _dashboardToken;
        internal readonly System.Collections.Concurrent.ConcurrentDictionary<string, DdlEntry> _ddlCache
            = new System.Collections.Concurrent.ConcurrentDictionary<string, DdlEntry>();

        // Test-only factory: returns an instance with config/port state but without
        // spawning the binary or opening a connection. Unit tests inject a
        // SpyConnection via _testConn to exercise wrapper methods.
        internal static GoldLapel CreateForTest(string upstream, GoldLapelOptions options = null)
        {
            return new GoldLapel(upstream, options ?? new GoldLapelOptions());
        }

        // Private: construct via StartAsync.
        private GoldLapel(string upstream, GoldLapelOptions options)
        {
            if (upstream == null) throw new ArgumentNullException(nameof(upstream));
            _upstream = upstream;
            _proxyPortExplicit = options.ProxyPort.HasValue;
            _proxyPort = options.ProxyPort ?? DefaultProxyPort;
            _logLevel = options.LogLevel;
            LogLevelToVerboseFlag(_logLevel);
            _mode = options.Mode;
            _license = options.License;
            _client = options.Client;
            _configFile = options.ConfigFile;
            _extraArgs = options.ExtraArgs ?? Array.Empty<string>();
            _config = options.Config != null
                ? new Dictionary<string, object>(options.Config)
                : null;
            // Validate structured-config keys eagerly so the error surfaces at
            // construction (same behavior as ConfigToArgs previously). Leaving
            // validation to spawn time would let unit tests that never spawn
            // miss bad keys.
            ValidateConfigKeys(_config);
            _silent = options.Silent;
            // Mesh membership — startup intent (HQ enforces license).
            _mesh = options.Mesh;
            _meshTag = string.IsNullOrEmpty(options.MeshTag) ? null : options.MeshTag;
            // Promoted disable flags (Model B). Each maps 1:1 to a proxy
            // CLI flag. Top-level so the canonical-surface caller flips a
            // single boolean instead of stuffing a boolean into the
            // structured `Config` map. Removed from ValidConfigKeys/BooleanKeys
            // simultaneously — atomic break, no aliases.
            _disableProxyCache = options.DisableProxyCache;
            _disableSqloptimize = options.DisableSqloptimize;
            _disableAutoIndexes = options.DisableAutoIndexes;
            // Dashboard defaults to proxy port + 1 (matches what the Rust binary
            // binds when no --dashboard-port is passed). A user-supplied value
            // on the top-level DashboardPort option overrides the derivation.
            // DashboardPort=0 means "disable dashboard".
            _dashboardPortExplicit = options.DashboardPort.HasValue;
            _dashboardPort = options.DashboardPort ?? _proxyPort + 1;
            _clientTls = ClientTlsConfigured(_config, _extraArgs);

            // Nested namespaces — canonical schema-to-core sub-API instances.
            // Each holds a back-reference to this client for shared state
            // (license, dashboard token, http session, conn, DDL pattern
            // cache).
            //
            // As of Phase 5 the Redis-compat helper families (counter / zset /
            // hash / queue / geo) are nested too, alongside Streams (Phase 1+2)
            // and Documents (Phase 4). Search / cache / auth remain flat —
            // they'll migrate when their own schema-to-core phase fires.
            Documents = new DocumentsApi(this);
            Streams = new StreamsApi(this);
            Counters = new CountersApi(this);
            Zsets = new ZsetsApi(this);
            Hashes = new HashesApi(this);
            Queues = new QueuesApi(this);
            Geos = new GeosApi(this);
        }

        /// <summary>
        /// Document-store namespace — <c>gl.Documents.&lt;Verb&gt;Async(...)</c>.
        /// See <see cref="DocumentsApi"/>.
        /// </summary>
        public DocumentsApi Documents { get; }

        /// <summary>
        /// Stream namespace — <c>gl.Streams.&lt;Verb&gt;Async(...)</c>.
        /// See <see cref="StreamsApi"/>.
        /// </summary>
        public StreamsApi Streams { get; }

        /// <summary>
        /// Counter namespace — <c>gl.Counters.&lt;Verb&gt;Async(...)</c>.
        /// Phase 5 of schema-to-core. See <see cref="CountersApi"/>.
        /// </summary>
        public CountersApi Counters { get; }

        /// <summary>
        /// Sorted-set namespace — <c>gl.Zsets.&lt;Verb&gt;Async(...)</c>.
        /// Phase 5 of schema-to-core. See <see cref="ZsetsApi"/>.
        /// </summary>
        public ZsetsApi Zsets { get; }

        /// <summary>
        /// Hash namespace — <c>gl.Hashes.&lt;Verb&gt;Async(...)</c>.
        /// Phase 5 of schema-to-core. See <see cref="HashesApi"/>.
        /// </summary>
        public HashesApi Hashes { get; }

        /// <summary>
        /// Queue namespace — <c>gl.Queues.&lt;Verb&gt;Async(...)</c>.
        /// Phase 5 of schema-to-core (at-least-once with visibility timeout).
        /// See <see cref="QueuesApi"/>.
        /// </summary>
        public QueuesApi Queues { get; }

        /// <summary>
        /// Geo namespace — <c>gl.Geos.&lt;Verb&gt;Async(...)</c>.
        /// Phase 5 of schema-to-core (PostGIS GEOGRAPHY-native).
        /// See <see cref="GeosApi"/>.
        /// </summary>
        public GeosApi Geos { get; }

        // Test-only accessors for mesh wiring.
        internal bool IsMesh => _mesh;
        internal string MeshTag => _meshTag;
        internal bool IsDisableProxyCache => _disableProxyCache;
        internal bool IsDisableSqloptimize => _disableSqloptimize;
        internal bool IsDisableAutoIndexes => _disableAutoIndexes;
        internal bool ClientTls => _clientTls;

        // ── Factory ─────────────────────────────────────────────────

        /// <summary>
        /// Spawn the Gold Lapel proxy for the given upstream URL, wait for it to accept
        /// connections, eagerly open an internal <see cref="NpgsqlConnection"/> against
        /// the proxy, and return a ready <see cref="GoldLapel"/> handle.
        /// </summary>
        /// <remarks>
        /// One proxy runs per upstream URL in a process. Starting an upstream that
        /// is already running returns a new handle on that proxy (its own internal
        /// connection, the running proxy's ports and options); the proxy stops when
        /// the last handle is disposed.
        /// </remarks>
        /// <example>
        /// <code>
        /// await using var gl = await GoldLapel.StartAsync(
        ///     "postgresql://user:pass@db/mydb",
        ///     opts => { opts.ProxyPort = 7932; opts.LogLevel = "info"; });
        /// var hits = await gl.SearchAsync("articles", "body", "postgres");
        /// </code>
        /// </example>
        public static Task<GoldLapel> StartAsync(string upstream)
        {
            return StartAsync(upstream, null);
        }

        /// <inheritdoc cref="StartAsync(string)"/>
        public static async Task<GoldLapel> StartAsync(string upstream, Action<GoldLapelOptions> configure)
        {
            if (upstream == null) throw new ArgumentNullException(nameof(upstream));
            var options = new GoldLapelOptions();
            configure?.Invoke(options);

            var gl = new GoldLapel(upstream, options);
            var owner = gl.Register();
            var entry = gl._entry;
            try
            {
                if (owner)
                {
                    try
                    {
                        await gl.SpawnAsync().ConfigureAwait(false);
                        entry.Ready.TrySetResult(true);
                    }
                    catch (Exception e)
                    {
                        entry.Ready.TrySetException(e);
                        throw;
                    }
                }
                else
                {
                    await entry.Ready.Task.ConfigureAwait(false);
                }
                gl.Attach();
                await gl.OpenAsync(owner).ConfigureAwait(false);
            }
            catch
            {
                if (gl._conn != null)
                {
                    try { gl._conn.Dispose(); } catch { }
                    gl._conn = null;
                }
                gl.ReleaseProxy();
                throw;
            }
            return gl;
        }

        private static void ValidateConfigKeys(Dictionary<string, object> config)
        {
            if (config == null) return;
            foreach (var key in config.Keys)
                CheckConfigKey(key);
        }

        private static void CheckConfigKey(string key)
        {
            if (ValidConfigKeys.Contains(key)) return;
            if (RemovedConfigKeys.TryGetValue(key, out var why))
                throw new ArgumentException("Config key '" + key + "' is no longer supported: " + why + ".");
            throw new ArgumentException("Unknown config key: " + key);
        }

        // ── Proxy registry ──────────────────────────────────────────
        //
        // One proxy per upstream per process, shared by every handle started
        // for it and stopped when the last one is disposed. The registry is
        // also the record of which ports this process's proxies hold, so a
        // new proxy never lands on another's proxy or dashboard port.

        internal sealed class ProxyEntry
        {
            internal string Upstream;
            internal int ProxyPort;
            internal int DashboardPort;
            internal Process Process;
            internal string ProxyUrl;
            internal string DashboardToken;
            internal int RefCount;
            internal readonly TaskCompletionSource<bool> Ready =
                new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            internal ProxyEntry()
            {
                // A failed start nobody else waited on must not surface as an
                // unobserved task exception.
                Ready.Task.ContinueWith(t => { var _ = t.Exception; }, TaskContinuationOptions.OnlyOnFaulted);
            }

            // Starting (no process yet) or running. A proxy whose start failed
            // or whose process has exited holds nothing.
            internal bool Live
            {
                get
                {
                    if (Ready.Task.IsFaulted) return false;
                    try { return Process == null || !Process.HasExited; }
                    catch (InvalidOperationException) { return false; }
                }
            }
        }

        private static readonly object RegistryLock = new object();
        private static readonly Dictionary<string, ProxyEntry> Registry = new Dictionary<string, ProxyEntry>();

        // Join the running proxy for this upstream, or claim ports for a new
        // one. Returns true when this handle must spawn it.
        private bool Register()
        {
            lock (RegistryLock)
            {
                if (Registry.TryGetValue(_upstream, out var existing) && existing.Live)
                {
                    existing.RefCount++;
                    _entry = existing;
                    return false;
                }
                Registry.Remove(_upstream);

                if (!_proxyPortExplicit)
                    _proxyPort = PickProxyPort(_dashboardPortExplicit ? _dashboardPort : (int?)null);
                if (!_dashboardPortExplicit)
                    _dashboardPort = _proxyPort + 1;
                CheckPortsUnclaimed(_proxyPort, _dashboardPort);

                _entry = new ProxyEntry
                {
                    Upstream = _upstream,
                    ProxyPort = _proxyPort,
                    DashboardPort = _dashboardPort,
                    RefCount = 1,
                };
                Registry[_upstream] = _entry;
                return true;
            }
        }

        // Take on the (now ready) proxy's state.
        private void Attach()
        {
            _proxyPort = _entry.ProxyPort;
            _dashboardPort = _entry.DashboardPort;
            _process = _entry.Process;
            _proxyUrl = _entry.ProxyUrl;
            _dashboardToken = _entry.DashboardToken;
        }

        // Drop this handle's hold on its proxy; the last holder stops it.
        private void ReleaseProxy()
        {
            var entry = _entry;
            _entry = null;
            _process = null;
            _proxyUrl = null;
            if (entry == null) return;
            lock (RegistryLock)
            {
                if (--entry.RefCount > 0) return;
                if (Registry.TryGetValue(entry.Upstream, out var current) && current == entry)
                    Registry.Remove(entry.Upstream);
            }
            // Npgsql pools by connection string. A proxy started later on the
            // same port gets the same string, and would be handed this one's
            // dead pooled connections.
            if (entry.ProxyUrl != null)
            {
                try
                {
                    using (var pooled = new NpgsqlConnection(UrlToNpgsqlConnectionString(entry.ProxyUrl)))
                        NpgsqlConnection.ClearPool(pooled);
                }
                catch { }
            }
            StopProcess(entry.Process);
        }

        // Ports held by this process's live proxies: port -> (upstream, role).
        private static Dictionary<int, KeyValuePair<string, string>> ClaimedPorts()
        {
            var claimed = new Dictionary<int, KeyValuePair<string, string>>();
            foreach (var entry in Registry.Values)
            {
                if (!entry.Live) continue;
                claimed[entry.ProxyPort] = new KeyValuePair<string, string>(entry.Upstream, "proxy");
                if (entry.DashboardPort > 0)
                    claimed[entry.DashboardPort] = new KeyValuePair<string, string>(entry.Upstream, "dashboard");
            }
            return claimed;
        }

        // The smallest port from 7932 whose pair (the port and its dashboard
        // port, port + 1 unless given) no proxy of this process holds and
        // nothing else has bound. An explicit dashboard port is the caller's
        // choice, so only the proxy port is checked then.
        internal static int PickProxyPort(int? dashboardPort)
        {
            lock (RegistryLock)
            {
                var claimed = ClaimedPorts();
                for (var port = DefaultProxyPort; port < 65535; port++)
                {
                    if (claimed.ContainsKey(port)) continue;
                    if (dashboardPort.HasValue)
                    {
                        if (port == dashboardPort.Value || !CanBind(port)) continue;
                    }
                    else if (claimed.ContainsKey(port + 1) || !CanBind(port) || !CanBind(port + 1))
                    {
                        continue;
                    }
                    return port;
                }
            }
            throw new InvalidOperationException("Gold Lapel could not find a free proxy port");
        }

        // True when nothing is listening on the port: bind 0.0.0.0:port the way
        // the proxy's own start-up check does (no SO_REUSEPORT), then release it.
        internal static bool CanBind(int port)
        {
            var listener = new TcpListener(IPAddress.Any, port);
            try
            {
                listener.Start();
                return true;
            }
            catch (SocketException)
            {
                return false;
            }
            finally
            {
                listener.Stop();
            }
        }

        // Without this, an explicit port would hand the caller another
        // upstream's proxy (or the proxy would refuse it with a less useful
        // message).
        private static void CheckPortsUnclaimed(int proxyPort, int dashboardPort)
        {
            var claimed = ClaimedPorts();
            foreach (var (port, role) in new[] { (proxyPort, "proxy"), (dashboardPort, "dashboard") })
            {
                if (port > 0 && claimed.TryGetValue(port, out var held))
                {
                    throw new InvalidOperationException(
                        "Gold Lapel cannot use port " + port + " as the " + role + " port: this " +
                        "process's proxy for " + RedactPassword(held.Key) + " already holds it as its " +
                        held.Value + " port. Choose another port, or leave ProxyPort and DashboardPort " +
                        "unset to have a free pair assigned.");
                }
            }
        }

        // The URL with its password replaced by ***, for error messages. The
        // userinfo ends at the last @ before the query, as in
        // UrlToNpgsqlConnectionString.
        internal static string RedactPassword(string url)
        {
            var scheme = url.IndexOf("://", StringComparison.Ordinal);
            if (scheme < 0) return url;
            var start = scheme + 3;
            var query = url.IndexOf('?', start);
            var at = url.LastIndexOf('@', query < 0 ? url.Length - 1 : query - 1);
            if (at < start) return url;
            var colon = url.IndexOf(':', start, at - start);
            if (colon < 0) return url;
            return url.Substring(0, colon + 1) + "***" + url.Substring(at);
        }

        internal static void ClaimForTest(string upstream, int proxyPort, int dashboardPort, Process process = null)
        {
            lock (RegistryLock)
            {
                Registry[upstream] = new ProxyEntry
                {
                    Upstream = upstream,
                    ProxyPort = proxyPort,
                    DashboardPort = dashboardPort,
                    Process = process,
                    RefCount = 1,
                };
            }
        }

        internal static void ForgetForTest(string upstream)
        {
            lock (RegistryLock) Registry.Remove(upstream);
        }

        internal static bool IsRegistered(string upstream)
        {
            lock (RegistryLock) return Registry.ContainsKey(upstream);
        }

        // ── Properties ──────────────────────────────────────────────

        /// <summary>
        /// Proxy connection string in Npgsql keyword form (<c>Host=localhost;Port=7932;...</c>).
        /// Pass directly to <c>new NpgsqlConnection(gl.Url)</c>.
        /// For the URL form, use <see cref="ProxyUrl"/>.
        /// </summary>
        public string Url => _proxyUrl != null ? UrlToNpgsqlConnectionString(_proxyUrl) : null;

        /// <summary>URL form of the proxy connection string (<c>postgresql://user:pass@localhost:7932/db</c>).</summary>
        public string ProxyUrl => _proxyUrl;

        /// <summary>Proxy port.</summary>
        public int ProxyPort => _proxyPort;

        /// <summary>True while the proxy subprocess is alive.</summary>
        public bool IsRunning => _process != null && !_process.HasExited;

        /// <summary>
        /// Dashboard URL (<c>http://127.0.0.1:{dashboardPort}</c>) while the proxy
        /// is running, otherwise <c>null</c>. Returns <c>null</c> pre-start, after
        /// disposal, or when <c>dashboardPort</c> is 0 (dashboard disabled).
        /// </summary>
        /// <remarks>
        /// This matches the behavior of the Python, Go, Java, and PHP wrappers:
        /// no live URL is reported until the proxy process is up. If you only
        /// need the port number at construction time, use <see cref="DashboardPort"/>
        /// or derive it as <c>ProxyPort + 1</c>.
        /// </remarks>
        public string DashboardUrl =>
            _dashboardPort > 0 && _process != null && !_process.HasExited
                ? "http://127.0.0.1:" + _dashboardPort
                : null;

        /// <summary>
        /// Dashboard port (always proxy port + 1 unless overridden via
        /// the top-level <c>DashboardPort</c> option). Used by the DDL API
        /// client to POST to <c>/api/ddl/stream/create</c> and friends.
        /// </summary>
        public int DashboardPort => _dashboardPort;

        /// <summary>
        /// Dashboard token this instance provisioned for the proxy subprocess.
        /// Returns null when the proxy was launched externally — in that case
        /// <see cref="Ddl.TokenFromEnvOrFile"/> falls back to env / file.
        /// </summary>
        public string DashboardToken => _dashboardToken;

        /// <summary>
        /// The internal connection opened by <see cref="StartAsync"/>. Wrapper methods use
        /// this by default; user code typically opens its own <see cref="NpgsqlConnection"/>
        /// against <see cref="Url"/> for raw SQL.
        /// </summary>
        public NpgsqlConnection Connection => _conn;

        // ── Scoped connection override ──────────────────────────────

        /// <summary>
        /// Run <paramref name="action"/> with <paramref name="connection"/> bound as the
        /// active connection for every wrapper method called on the passed handle. The
        /// binding is scoped via <see cref="AsyncLocal{T}"/>, so it correctly follows
        /// awaits. Nesting is supported; the inner scope wins and is restored on exit.
        /// </summary>
        /// <example>
        /// <code>
        /// await gl.UsingAsync(conn, async gl => {
        ///     await gl.Documents.InsertAsync("events", "{\"type\":\"order.created\"}");
        /// });
        /// </code>
        /// </example>
        public async Task UsingAsync(DbConnection connection, Func<GoldLapel, Task> action)
        {
            if (connection == null) throw new ArgumentNullException(nameof(connection));
            if (action == null) throw new ArgumentNullException(nameof(action));

            var previous = _scopedConnection.Value;
            _scopedConnection.Value = connection;
            try
            {
                await action(this).ConfigureAwait(false);
            }
            finally
            {
                _scopedConnection.Value = previous;
            }
        }

        /// <inheritdoc cref="UsingAsync(DbConnection, Func{GoldLapel, Task})"/>
        public async Task<T> UsingAsync<T>(DbConnection connection, Func<GoldLapel, Task<T>> action)
        {
            if (connection == null) throw new ArgumentNullException(nameof(connection));
            if (action == null) throw new ArgumentNullException(nameof(action));

            var previous = _scopedConnection.Value;
            _scopedConnection.Value = connection;
            try
            {
                return await action(this).ConfigureAwait(false);
            }
            finally
            {
                _scopedConnection.Value = previous;
            }
        }

        // ── Dispose ─────────────────────────────────────────────────

        public async ValueTask DisposeAsync()
        {
            if (_disposed) return;
            _disposed = true;

            // Drop cached DDL patterns — they're tied to the proxy we're
            // about to terminate.
            _ddlCache.Clear();
            _dashboardToken = null;

            if (_conn != null)
            {
                try { await _conn.CloseAsync().ConfigureAwait(false); } catch { }
                try { await _conn.DisposeAsync().ConfigureAwait(false); } catch { }
                _conn = null;
            }
            ReleaseProxy();
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            _ddlCache.Clear();
            _dashboardToken = null;

            if (_conn != null)
            {
                try { _conn.Close(); } catch { }
                try { _conn.Dispose(); } catch { }
                _conn = null;
            }
            ReleaseProxy();
        }

        private static void StopProcess(Process proc)
        {
            if (proc == null) return;
            try
            {
                if (!proc.HasExited) GracefulStop(proc);
            }
            catch { }
            try { proc.Dispose(); } catch { }
        }

        // ── Process spawn ───────────────────────────────────────────

        /// <summary>
        /// Build the argv list passed to the spawned <c>goldlapel</c> binary.
        /// Order: required flags first (<c>--upstream</c>, <c>--proxy-port</c>),
        /// then top-level options that emit only when the user set them, then
        /// the tuning-knob structured config map, then any caller-supplied
        /// <c>ExtraArgs</c>. Internal so unit tests can verify CLI-flag
        /// emission for top-level options without spawning the binary.
        /// </summary>
        /// <remarks>
        /// The result does NOT include the binary path — that's set separately
        /// on <see cref="ProcessStartInfo.FileName"/>. This differs from
        /// Java's <c>buildSpawnCmd(String)</c>, which prepends the binary
        /// because Java's <c>ProcessBuilder</c> takes a single command list.
        /// </remarks>
        internal List<string> BuildSpawnArgs()
        {
            var args = new List<string> { "--upstream", _upstream, "--proxy-port", _proxyPort.ToString() };
            // Top-level options emit their own CLI flags before the structured
            // config map — keeps the argv order predictable for argv-diffing
            // tests. An explicit dashboard port overrides the binary's
            // derived default.
            if (_dashboardPortExplicit)
            {
                args.Add("--dashboard-port");
                args.Add(_dashboardPort.ToString());
            }
            if (!string.IsNullOrEmpty(_logLevel))
            {
                var verboseFlag = LogLevelToVerboseFlag(_logLevel);
                if (verboseFlag != null)
                    args.Add(verboseFlag);
            }
            if (!string.IsNullOrEmpty(_mode))
            {
                args.Add("--mode");
                args.Add(_mode);
            }
            if (!string.IsNullOrEmpty(_license))
            {
                args.Add("--license");
                args.Add(_license);
            }
            if (!string.IsNullOrEmpty(_client))
            {
                args.Add("--client");
                args.Add(_client);
            }
            if (!string.IsNullOrEmpty(_configFile))
            {
                args.Add("--config");
                args.Add(_configFile);
            }
            if (_mesh)
            {
                args.Add("--mesh");
            }
            if (!string.IsNullOrEmpty(_meshTag))
            {
                args.Add("--mesh-tag");
                args.Add(_meshTag);
            }
            // Promoted disable flags. Each emits its proxy CLI flag 1:1
            // when set. Order is alphabetical (matches the proxy's
            // declaration order in main.rs) for predictable argv-diffs.
            if (_disableAutoIndexes)
            {
                args.Add("--disable-auto-indexes");
            }
            if (_disableProxyCache)
            {
                args.Add("--disable-proxy-cache");
            }
            if (_disableSqloptimize)
            {
                args.Add("--disable-sqloptimize");
            }
            args.AddRange(ConfigToArgs(_config));
            args.AddRange(_extraArgs);
            return args;
        }

        // Spawn the proxy for this handle's (registered) entry and wait until
        // it serves its port.
        private async Task SpawnAsync()
        {
            var entry = _entry;
            var binary = FindBinary();
            var args = BuildSpawnArgs();

            var psi = new ProcessStartInfo
            {
                FileName = binary,
                Arguments = JoinArgs(args),
                UseShellExecute = false,
                RedirectStandardInput = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true
            };
            // GOLDLAPEL_CLIENT env var is only set when the user hasn't opted
            // out via opts.Client (explicit --client flag takes precedence).
            if (string.IsNullOrEmpty(_client)
                && !psi.EnvironmentVariables.ContainsKey("GOLDLAPEL_CLIENT"))
            {
                psi.EnvironmentVariables["GOLDLAPEL_CLIENT"] = "dotnet";
            }
            // Provision a session-scoped dashboard token for /api/ddl/* calls.
            // Pre-set env wins; otherwise generate a fresh one per session.
            string envToken = psi.EnvironmentVariables.ContainsKey("GOLDLAPEL_DASHBOARD_TOKEN")
                ? psi.EnvironmentVariables["GOLDLAPEL_DASHBOARD_TOKEN"]
                : null;
            if (!string.IsNullOrEmpty(envToken))
            {
                entry.DashboardToken = envToken;
            }
            else
            {
                var buf = new byte[32];
                using (var rng = System.Security.Cryptography.RandomNumberGenerator.Create())
                {
                    rng.GetBytes(buf);
                }
                entry.DashboardToken = BitConverter.ToString(buf).Replace("-", "").ToLowerInvariant();
                psi.EnvironmentVariables["GOLDLAPEL_DASHBOARD_TOKEN"] = entry.DashboardToken;
            }

            // Something already listening on an explicit proxy port would
            // answer the readiness connect on the proxy's behalf.
            var portWasBusy = !CanBind(_proxyPort);

            Process proc;
            try
            {
                proc = Process.Start(psi);
                entry.Process = proc;
                proc.StandardInput.Close();
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to start Gold Lapel process", e);
            }

            // Drain stderr to prevent pipe-buffer deadlock, keeping a bounded
            // tail for start-up errors.
            var stderrBuf = new StringBuilder();
            var stderrThread = new Thread(() =>
            {
                try
                {
                    var buffer = new char[1024];
                    int n;
                    while ((n = proc.StandardError.Read(buffer, 0, buffer.Length)) > 0)
                    {
                        lock (stderrBuf)
                        {
                            stderrBuf.Append(buffer, 0, n);
                            if (stderrBuf.Length > 2 * StderrTailChars)
                                stderrBuf.Remove(0, stderrBuf.Length - StderrTailChars);
                        }
                    }
                }
                catch { }
            })
            { IsBackground = true };
            stderrThread.Start();

            // Drain stdout so the child doesn't block writing to it.
            var stdoutThread = new Thread(() =>
            {
                try
                {
                    var buffer = new char[1024];
                    while (proc.StandardOutput.Read(buffer, 0, buffer.Length) > 0) { }
                }
                catch { }
            })
            { IsBackground = true };
            stdoutThread.Start();

            // The proxy refuses a port in use and exits at once; let it,
            // rather than trust a connect the other listener answers.
            if (portWasBusy)
                await Task.Run(() => proc.WaitForExit(BusyPortGraceMs)).ConfigureAwait(false);

            // Poll for port readiness within a single StartupTimeoutMs budget.
            // Ready means the port answers AND our child is still alive then.
            var ready = !proc.HasExited && await PollForPortAsync(
                "127.0.0.1", _proxyPort, StartupTimeoutMs,
                () => proc.HasExited).ConfigureAwait(false);

            if (!ready || proc.HasExited)
            {
                string reason;
                if (proc.HasExited)
                {
                    reason = "Gold Lapel exited with status " + proc.ExitCode +
                             " before it was ready on port " + _proxyPort + ".";
                }
                else
                {
                    try { proc.Kill(); } catch { }
                    try { proc.WaitForExit(5000); } catch { }
                    reason = "Gold Lapel failed to start on port " + _proxyPort +
                             " within " + (StartupTimeoutMs / 1000) + "s.";
                }
                try { stderrThread.Join(2000); } catch { }
                string stderr;
                lock (stderrBuf) stderr = stderrBuf.ToString().Trim();
                throw new InvalidOperationException(reason + "\nstderr: " + stderr);
            }

            entry.ProxyUrl = MakeProxyUrl(_upstream, _proxyPort, _clientTls);
        }

        // Eagerly open this handle's internal Npgsql connection. Npgsql does
        // not accept URL-style connection strings, so convert to key-value form.
        private async Task OpenAsync(bool announce)
        {
            _conn = new NpgsqlConnection(UrlToNpgsqlConnectionString(_proxyUrl));
            await _conn.OpenAsync().ConfigureAwait(false);

            // Write the startup banner to stderr (not stdout) so it doesn't pollute
            // application stdout — ASP.NET Core logs, CLI app output, shells that
            // redirect stdout, etc. Opt out entirely via GoldLapelOptions.Silent.
            // Only the start that spawned the proxy announces it.
            if (announce && !_silent)
            {
                if (_dashboardPort > 0)
                    Console.Error.WriteLine($"goldlapel \u2192 :{_proxyPort} (proxy) | http://127.0.0.1:{_dashboardPort} (dashboard)");
                else
                    Console.Error.WriteLine($"goldlapel \u2192 :{_proxyPort} (proxy)");
            }
        }

        private static void GracefulStop(Process proc)
        {
            // Unix: SIGTERM first for clean shutdown (telemetry flush, closes, etc.).
            // Fall back to Kill on timeout. Windows has no SIGTERM — Kill is the only option.
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                try
                {
                    if (SendSignal(proc.Id, 15)) // SIGTERM
                    {
                        if (proc.WaitForExit(GracefulTimeoutMs))
                            return;
                    }
                }
                catch { }
            }

            try { proc.Kill(); } catch { }
            try { proc.WaitForExit(GracefulTimeoutMs); } catch { }
        }

        [DllImport("libc", SetLastError = true, EntryPoint = "kill")]
        private static extern int sys_kill(int pid, int sig);

        internal static bool SendSignal(int pid, int signal)
        {
            return sys_kill(pid, signal) == 0;
        }

        // ── Config helpers ──────────────────────────────────────────

        public static IReadOnlyCollection<string> ConfigKeys() => ValidConfigKeys;

        internal static List<string> ConfigToArgs(Dictionary<string, object> config)
        {
            var result = new List<string>();
            if (config == null || config.Count == 0)
                return result;

            foreach (var kvp in config)
            {
                var key = kvp.Key;
                var value = kvp.Value;

                CheckConfigKey(key);

                var flag = "--" + CamelToKebab(key);

                if (BooleanKeys.Contains(key))
                {
                    if (!(value is bool))
                        throw new ArgumentException(
                            "Config key '" + key + "' must be a boolean, got " + value.GetType().Name);
                    if ((bool)value)
                        result.Add(flag);
                    continue;
                }

                if (ListKeys.Contains(key))
                {
                    var enumerable = value as IEnumerable;
                    if (enumerable == null || value is string)
                        throw new ArgumentException(
                            "Config key '" + key + "' must be a list/array, got " + value.GetType().Name);
                    foreach (var item in enumerable)
                    {
                        result.Add(flag);
                        result.Add(item.ToString());
                    }
                    continue;
                }

                result.Add(flag);
                result.Add(value.ToString());
            }

            return result;
        }

        // Translate the ergonomic log_level string into the proxy binary's
        // count-based verbosity flag (-v/-vv/-vvv). Returns null when no
        // flag should be emitted (warn/error map to the default level).
        // Throws ArgumentException for any other value.
        internal static string LogLevelToVerboseFlag(string level)
        {
            if (string.IsNullOrEmpty(level)) return null;
            switch (level.ToLowerInvariant())
            {
                case "trace":   return "-vvv";
                case "debug":   return "-vv";
                case "info":    return "-v";
                case "warn":
                case "warning":
                case "error":
                    return null;
                default:
                    throw new ArgumentException(
                        "logLevel must be one of: trace, debug, info, warn, error (got '" + level + "')");
            }
        }

        private static string CamelToKebab(string key)
        {
            var sb = new StringBuilder();
            foreach (var c in key)
            {
                if (char.IsUpper(c))
                {
                    sb.Append('-');
                    sb.Append(char.ToLower(c));
                }
                else
                {
                    sb.Append(c);
                }
            }
            return sb.ToString();
        }

        // ── URL / binary / port helpers ─────────────────────────────

        private static readonly Regex WithPort =
            new Regex(@"^(postgres(?:ql)?://(?:.*@)?)([^:/?#]+):(\d+)(.*)$");

        private static readonly Regex NoPort =
            new Regex(@"^(postgres(?:ql)?://(?:.*@)?)([^:/?#]+)(.*)$");

        private static readonly Regex AppNamePresent =
            new Regex(@"[?&]application_name=", RegexOptions.IgnoreCase);

        /// <summary>
        /// Wrapper version, used to build the application_name marker on PG
        /// connections. Read from the assembly's
        /// <c>AssemblyInformationalVersionAttribute</c> (CI sets this from the
        /// git tag at publish time); falls back to <c>"0.0.0"</c> for dev
        /// builds where no version attribute is set.
        /// </summary>
        internal static string WrapperVersion()
        {
            var asm = typeof(GoldLapel).Assembly;
            var attr = asm.GetCustomAttributes(typeof(System.Reflection.AssemblyInformationalVersionAttribute), false);
            if (attr.Length > 0)
            {
                var v = ((System.Reflection.AssemblyInformationalVersionAttribute)attr[0]).InformationalVersion;
                if (!string.IsNullOrEmpty(v))
                {
                    // Strip a leading "v" and any "+gitsha" build-metadata suffix.
                    var plus = v.IndexOf('+');
                    if (plus >= 0) v = v.Substring(0, plus);
                    if (v.StartsWith("v", StringComparison.OrdinalIgnoreCase)) v = v.Substring(1);
                    if (!string.IsNullOrEmpty(v)) return v;
                }
            }
            var ver = asm.GetName().Version;
            if (ver != null && ver.ToString() != "0.0.0.0") return ver.ToString();
            return "0.0.0";
        }

        internal static string ApplicationNameMarker()
        {
            return "goldlapel:dotnet:" + WrapperVersion();
        }

        /// <summary>
        /// Append <c>application_name=goldlapel:dotnet:&lt;version&gt;</c> to
        /// the URL unless one is already present (or <c>PGAPPNAME</c> is set
        /// in the env), so the connection is recognisable in
        /// <c>pg_stat_activity</c>. The proxy caches it like any other client.
        /// Idempotent and override-respecting.
        /// </summary>
        internal static string InjectApplicationName(string url)
        {
            if (AppNamePresent.IsMatch(url)) return url;
            var pgAppName = Environment.GetEnvironmentVariable("PGAPPNAME");
            if (!string.IsNullOrEmpty(pgAppName)) return url;
            char sep = url.IndexOf('?') >= 0 ? '&' : '?';
            return url + sep + "application_name=" + ApplicationNameMarker();
        }

        internal static string FindBinary()
        {
            var envPath = Environment.GetEnvironmentVariable("GOLDLAPEL_BINARY");
            if (!string.IsNullOrEmpty(envPath))
            {
                if (File.Exists(envPath)) return envPath;
                throw new InvalidOperationException(
                    "GOLDLAPEL_BINARY points to " + envPath + " but file not found");
            }

            var bundled = FindBundledBinary();
            if (bundled != null) return bundled;

            var onPath = FindOnPath();
            if (onPath != null) return onPath;

            throw new InvalidOperationException(
                "Gold Lapel binary not found. Set GOLDLAPEL_BINARY env var, " +
                "install the NuGet package with bundled binaries, or ensure 'goldlapel' is on PATH.");
        }

        // TLS/GSS parameters for the upstream hop. The proxy keeps using them
        // upstream, but declines client TLS unless started with
        // --tls-cert/--tls-key, so in the app's URL they made every
        // connection to the proxy fail (sslmode=require on Neon, Supabase, RDS).
        private static readonly HashSet<string> UpstreamOnlyParams = new HashSet<string>(new[]
        {
            "sslmode", "sslcert", "sslkey", "sslrootcert", "sslcrl", "sslcrldir",
            "sslpassword", "sslsni", "sslnegotiation", "ssl_min_protocol_version",
            "ssl_max_protocol_version", "requiressl", "channel_binding",
            "gssencmode", "krbsrvname", "gsslib"
        }, StringComparer.OrdinalIgnoreCase);

        // Client-facing TLS is on when the wrapper passes --tls-cert/--tls-key
        // (Config or ExtraArgs) or the proxy inherits GOLDLAPEL_TLS_CERT/KEY.
        private static bool ClientTlsConfigured(Dictionary<string, object> config, string[] extraArgs)
        {
            if (config != null && (config.ContainsKey("tlsCert") || config.ContainsKey("tlsKey")))
                return true;
            foreach (var arg in extraArgs)
            {
                if (arg.StartsWith("--tls-cert", StringComparison.Ordinal) ||
                    arg.StartsWith("--tls-key", StringComparison.Ordinal))
                    return true;
            }
            return !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("GOLDLAPEL_TLS_CERT")) ||
                   !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("GOLDLAPEL_TLS_KEY"));
        }

        // Drop UpstreamOnlyParams from the query of a URL tail ("/db?a=1&b=2").
        private static string StripUpstreamOnlyParams(string tail)
        {
            var q = tail.IndexOf('?');
            if (q < 0) return tail;
            var kept = new List<string>();
            foreach (var pair in tail.Substring(q + 1).Split('&'))
            {
                if (pair.Length == 0) continue;
                var eq = pair.IndexOf('=');
                var key = Uri.UnescapeDataString(eq < 0 ? pair : pair.Substring(0, eq));
                if (!UpstreamOnlyParams.Contains(key)) kept.Add(pair);
            }
            return kept.Count == 0
                ? tail.Substring(0, q)
                : tail.Substring(0, q + 1) + string.Join("&", kept);
        }

        internal static string MakeProxyUrl(string upstream, int port, bool clientTls = false)
        {
            // Use regex not System.Uri to preserve percent-encoded characters in passwords
            // (e.g. %40 for @), which Uri would decode and corrupt the URL.
            var m = WithPort.Match(upstream);
            if (m.Success)
            {
                var tail = clientTls ? m.Groups[4].Value : StripUpstreamOnlyParams(m.Groups[4].Value);
                return InjectApplicationName(m.Groups[1].Value + "localhost:" + port + tail);
            }

            m = NoPort.Match(upstream);
            if (m.Success)
            {
                var tail = clientTls ? m.Groups[3].Value : StripUpstreamOnlyParams(m.Groups[3].Value);
                return InjectApplicationName(m.Groups[1].Value + "localhost:" + port + tail);
            }

            // Bare-host form skips the marker — atypical caller path.
            if (!upstream.Contains("://") && upstream.Contains(":"))
                return "localhost:" + port;

            return "localhost:" + port;
        }

        /// <summary>
        /// Convert a postgres URL (e.g. <c>postgresql://user:pass@host:port/db?sslmode=require</c>)
        /// into the key-value form Npgsql expects (<c>Host=host;Port=port;Username=user;Password=pass;Database=db;SSL Mode=Require;...</c>).
        /// If the input is already key-value form it is returned unchanged.
        /// </summary>
        /// <remarks>
        /// Query parameters use their libpq names and map to the Npgsql keyword with the
        /// same meaning (<c>connect_timeout</c> to <c>Timeout</c>, <c>sslrootcert</c> to
        /// <c>Root Certificate</c>, ...). libpq parameters Npgsql has no counterpart for,
        /// such as <c>channel_binding</c> or <c>gssencmode</c>, are dropped; any other
        /// parameter throws <see cref="ArgumentException"/> naming it.
        /// </remarks>
        public static string UrlToNpgsqlConnectionString(string url)
        {
            if (string.IsNullOrEmpty(url)) return url;

            // Already key-value form — pass through.
            if (!url.StartsWith("postgres://", StringComparison.OrdinalIgnoreCase) &&
                !url.StartsWith("postgresql://", StringComparison.OrdinalIgnoreCase))
                return url;

            // Strip scheme.
            var schemeIdx = url.IndexOf("://", StringComparison.Ordinal);
            var rest = url.Substring(schemeIdx + 3);

            var hashIdx = rest.IndexOf('#');
            if (hashIdx >= 0) rest = rest.Substring(0, hashIdx);

            // Split query string off.
            string query = null;
            var qIdx = rest.IndexOf('?');
            if (qIdx >= 0)
            {
                query = rest.Substring(qIdx + 1);
                rest = rest.Substring(0, qIdx);
            }

            // userinfo@host-stuff — rightmost @ separates (since passwords can contain @).
            string userinfo = null;
            var atIdx = rest.LastIndexOf('@');
            if (atIdx >= 0)
            {
                userinfo = rest.Substring(0, atIdx);
                rest = rest.Substring(atIdx + 1);
            }

            // host[:port][,host[:port]...][/database]
            string database = null;
            var slashIdx = rest.IndexOf('/');
            if (slashIdx >= 0)
            {
                database = rest.Substring(slashIdx + 1);
                rest = rest.Substring(0, slashIdx);
            }

            var builder = new NpgsqlConnectionStringBuilder();
            var multiHost = rest.IndexOf(',') >= 0;
            if (multiHost)
            {
                // Npgsql takes the same comma-separated host:port list.
                builder.Host = Uri.UnescapeDataString(rest);
            }
            else
            {
                string host = rest;
                string port = null;
                if (rest.StartsWith("[", StringComparison.Ordinal))
                {
                    var close = rest.IndexOf(']');
                    if (close < 0) throw new ArgumentException("Invalid IPv6 host in URL: " + rest);
                    host = rest.Substring(1, close - 1);
                    if (close + 1 < rest.Length && rest[close + 1] == ':')
                        port = rest.Substring(close + 2);
                }
                else
                {
                    var colonIdx = rest.LastIndexOf(':');
                    if (colonIdx >= 0)
                    {
                        host = rest.Substring(0, colonIdx);
                        port = rest.Substring(colonIdx + 1);
                    }
                }
                if (!string.IsNullOrEmpty(host)) builder.Host = Uri.UnescapeDataString(host);
                if (!string.IsNullOrEmpty(port)) SetUrlParam(builder, "port", port, false);
            }

            if (userinfo != null)
            {
                var uColon = userinfo.IndexOf(':');
                var user = uColon >= 0 ? userinfo.Substring(0, uColon) : userinfo;
                if (user.Length > 0) builder.Username = Uri.UnescapeDataString(user);
                if (uColon >= 0) builder.Password = Uri.UnescapeDataString(userinfo.Substring(uColon + 1));
            }
            if (!string.IsNullOrEmpty(database)) builder.Database = Uri.UnescapeDataString(database);

            string fallbackApplicationName = null;
            if (!string.IsNullOrEmpty(query))
            {
                foreach (var pair in query.Split('&'))
                {
                    if (string.IsNullOrEmpty(pair)) continue;
                    var eq = pair.IndexOf('=');
                    var key = Uri.UnescapeDataString(eq < 0 ? pair : pair.Substring(0, eq));
                    var value = eq < 0 ? "" : Uri.UnescapeDataString(pair.Substring(eq + 1));
                    if (key.Equals("fallback_application_name", StringComparison.OrdinalIgnoreCase))
                        fallbackApplicationName = value;
                    else
                        SetUrlParam(builder, key, value, multiHost);
                }
            }
            if (fallbackApplicationName != null && string.IsNullOrEmpty(builder.ApplicationName))
                builder.ApplicationName = fallbackApplicationName;

            return builder.ConnectionString;
        }

        // libpq parameters with no Npgsql counterpart that are safe to leave
        // out client-side: they tune TLS/GSS negotiation or TCP keepalives
        // that Npgsql handles its own way.
        private static readonly HashSet<string> LibpqParamsWithoutNpgsqlMeaning = new HashSet<string>(new[]
        {
            "channel_binding", "gssencmode", "gsslib", "gssdelegation",
            "sslcrl", "sslcrldir", "sslsni", "sslnegotiation", "sslcompression",
            "sslcertmode", "ssl_min_protocol_version", "ssl_max_protocol_version",
            "requiressl", "keepalives", "keepalives_idle", "keepalives_interval",
            "keepalives_count", "tcp_user_timeout", "min_protocol_version",
            "max_protocol_version", "sslkeylogfile"
        }, StringComparer.OrdinalIgnoreCase);

        private static void SetUrlParam(NpgsqlConnectionStringBuilder builder, string key, string value, bool multiHost)
        {
            var known = true;
            try
            {
                switch (key.ToLowerInvariant())
                {
                    case "host": builder.Host = value; break;
                    case "port": builder.Port = int.Parse(value, System.Globalization.CultureInfo.InvariantCulture); break;
                    case "dbname": builder.Database = value; break;
                    case "user": builder.Username = value; break;
                    case "password": builder.Password = value; break;
                    case "application_name": builder.ApplicationName = value; break;
                    case "options": builder.Options = value; break;
                    case "search_path": builder.SearchPath = value; break;
                    case "client_encoding": builder.ClientEncoding = value; break;
                    case "passfile": builder.Passfile = value; break;
                    case "connect_timeout":
                        builder.Timeout = int.Parse(value, System.Globalization.CultureInfo.InvariantCulture);
                        break;
                    case "target_session_attrs":
                        // Npgsql only accepts it with several hosts. With one,
                        // libpq merely checks the server's role; nothing to map.
                        if (multiHost) builder["Target Session Attributes"] = value;
                        break;
                    case "sslmode":
                        if (!Enum.TryParse<SslMode>(value.Replace("-", ""), true, out var mode) ||
                            !Enum.IsDefined(typeof(SslMode), mode))
                            throw new FormatException("expected disable, allow, prefer, require, verify-ca or verify-full");
                        builder.SslMode = mode;
                        // libpq verifies the certificate only in verify-ca/verify-full;
                        // Npgsql 6 refuses Require without saying so explicitly.
                        if (mode == SslMode.Allow || mode == SslMode.Prefer || mode == SslMode.Require)
                            builder.TrustServerCertificate = true;
                        break;
                    case "sslrootcert": builder.RootCertificate = value; break;
                    case "sslcert": builder.SslCertificate = value; break;
                    case "sslkey": builder.SslKey = value; break;
                    case "sslpassword": builder.SslPassword = value; break;
                    case "krbsrvname": builder.KerberosServiceName = value; break;
                    default:
                        known = LibpqParamsWithoutNpgsqlMeaning.Contains(key);
                        break;
                }
            }
            catch (Exception e) when (e is FormatException || e is OverflowException || e is ArgumentException)
            {
                throw new ArgumentException(
                    "Invalid value for connection URL parameter '" + key + "': " + e.Message, e);
            }
            if (!known)
                throw new ArgumentException(
                    "Connection URL parameter '" + key + "' is not supported: it has no Npgsql equivalent.");
        }

        internal static bool WaitForPort(string host, int port, long timeoutMs)
        {
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < timeoutMs)
            {
                try
                {
                    using (var client = new TcpClient())
                    {
                        var result = client.BeginConnect(host, port, null, null);
                        var connected = result.AsyncWaitHandle.WaitOne(500);
                        if (connected && client.Connected)
                            return true;
                    }
                }
                catch { }
                Thread.Sleep((int)StartupPollIntervalMs);
            }
            return false;
        }

        // Single TCP connect attempt with a per-attempt timeout. Returns true on
        // successful connect, false on timeout/error. The caller is responsible
        // for retrying against an overall startup budget.
        internal static async Task<bool> TryConnectOnceAsync(string host, int port, int timeoutMs)
        {
            try
            {
                using (var client = new TcpClient())
                {
                    var connectTask = client.ConnectAsync(host, port);
                    var finished = await Task.WhenAny(connectTask, Task.Delay(timeoutMs)).ConfigureAwait(false);
                    if (finished == connectTask && !connectTask.IsFaulted && client.Connected)
                        return true;
                }
            }
            catch { }
            return false;
        }

        // Poll the given port with a per-attempt connect timeout and a small
        // delay between attempts. Returns true when the port accepts a
        // connection before <paramref name="budgetMs"/> elapses; otherwise
        // false. <paramref name="abort"/> is consulted each iteration and
        // short-circuits the loop when it returns true (e.g. the child
        // process has already exited). The total elapsed time is bounded by
        // <paramref name="budgetMs"/> plus at most one per-attempt connect
        // timeout — not <c>budgetMs * N</c>.
        internal static async Task<bool> PollForPortAsync(
            string host, int port, long budgetMs, Func<bool> abort = null)
        {
            // Per-attempt connect timeout scales to the budget so very short
            // budgets (used in tests) don't spend their entire time inside a
            // single connect call.
            var attemptMs = (int)Math.Min(500, Math.Max(50, budgetMs / 4));
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < budgetMs)
            {
                if (abort != null && abort()) return false;
                if (await TryConnectOnceAsync(host, port, attemptMs).ConfigureAwait(false))
                    return true;
                if (sw.ElapsedMilliseconds >= budgetMs) break;
                await Task.Delay((int)StartupPollIntervalMs).ConfigureAwait(false);
            }
            return false;
        }

        private static string FindBundledBinary()
        {
            var isWindows = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
            var binaryName = isWindows ? "goldlapel.exe" : "goldlapel";

            string rid;
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                var arch = RuntimeInformation.OSArchitecture == Architecture.Arm64 ? "arm64" : "x64";
                var musl = File.Exists("/lib/ld-musl-" + (arch == "arm64" ? "aarch64" : "x86_64") + ".so.1");
                rid = musl ? "linux-musl-" + arch : "linux-" + arch;
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            {
                rid = RuntimeInformation.OSArchitecture == Architecture.Arm64
                    ? "osx-arm64" : "osx-x64";
            }
            else if (isWindows)
            {
                rid = "win-x64";
            }
            else
            {
                return null;
            }

            var assemblyDir = Path.GetDirectoryName(typeof(GoldLapel).Assembly.Location);
            if (!string.IsNullOrEmpty(assemblyDir))
            {
                var nextTo = Path.Combine(assemblyDir, binaryName);
                if (File.Exists(nextTo)) return nextTo;

                var runtimePath = Path.Combine(assemblyDir, "runtimes", rid, "native", binaryName);
                if (File.Exists(runtimePath)) return runtimePath;
            }

            return null;
        }

        private static string FindOnPath()
        {
            var isWindows = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
            var names = isWindows
                ? new[] { "goldlapel.exe", "goldlapel" }
                : new[] { "goldlapel" };

            var pathEnv = Environment.GetEnvironmentVariable("PATH");
            if (string.IsNullOrEmpty(pathEnv)) return null;

            var separator = isWindows ? ';' : ':';
            foreach (var dir in pathEnv.Split(separator))
            {
                foreach (var name in names)
                {
                    var full = Path.Combine(dir, name);
                    if (File.Exists(full)) return full;
                }
            }
            return null;
        }

        private static string JoinArgs(List<string> args)
        {
            var parts = new string[args.Count];
            for (int i = 0; i < args.Count; i++)
            {
                var a = args[i];
                if (a.Contains(" ") || a.Contains("\""))
                    parts[i] = "\"" + a.Replace("\"", "\\\"") + "\"";
                else
                    parts[i] = a;
            }
            return string.Join(" ", parts);
        }

        // Test hook: internal injection of a DbConnection so unit tests can exercise
        // wrapper methods against a SpyConnection without spawning the binary.
        // Not exposed publicly — tests access via reflection on the field below.
        internal DbConnection _testConn;

        // Resolve the active connection for a wrapper call.
        // Precedence: explicit per-call > UsingAsync scope > test hook > internal _conn.
        private DbConnection ResolveActive(DbConnection perCall)
        {
            if (perCall != null) return perCall;
            var scoped = _scopedConnection.Value;
            if (scoped != null) return scoped;
            if (_testConn != null) return _testConn;
            if (_conn == null)
                throw new InvalidOperationException(
                    "No connection available. Did you await GoldLapel.StartAsync(...)?");
            return _conn;
        }

        // Internal accessor used by the nested namespace classes
        // (DocumentsApi, StreamsApi). Keeps `ResolveActive` private so
        // direct subclassing/external mocking of the resolution policy
        // stays off-limits.
        internal DbConnection ResolveActiveDb(DbConnection perCall) => ResolveActive(perCall);

        // ── Wrapper methods ─────────────────────────────────────────
        // All methods:
        //   - Have an Async suffix (.NET convention)
        //   - Return Task / Task<T>
        //   - Accept an optional `connection:` named argument for per-call override
        //   - Use ResolveActive(connection) to pick the target connection
        //
        // Underlying Utils.XXX calls remain synchronous (Npgsql is thread-safe for
        // a single connection in one request flow). The Task return is a natural
        // .NET shape; per-method actual async-IO is a future enhancement.

        // Document store: gl.Documents.<Verb>Async(...). See Documents.cs.

        // Search
        public Task<List<Dictionary<string, object>>> SearchAsync(string table,
            string column, string query, int limit = 50, string lang = "english", bool highlight = false,
            NpgsqlConnection connection = null)
            => Task.FromResult(Utils.Search(ResolveActive(connection), table, column, query, limit, lang, highlight));

        public Task<List<Dictionary<string, object>>> SearchAsync(string table,
            string[] columns, string query, int limit = 50, string lang = "english", bool highlight = false,
            NpgsqlConnection connection = null)
            => Task.FromResult(Utils.Search(ResolveActive(connection), table, columns, query, limit, lang, highlight));

        public Task<List<Dictionary<string, object>>> SearchFuzzyAsync(string table,
            string column, string query, int limit = 50, double threshold = 0.3,
            NpgsqlConnection connection = null)
            => Task.FromResult(Utils.SearchFuzzy(ResolveActive(connection), table, column, query, limit, threshold));

        public Task<List<Dictionary<string, object>>> SearchPhoneticAsync(string table,
            string column, string query, int limit = 50, NpgsqlConnection connection = null)
            => Task.FromResult(Utils.SearchPhonetic(ResolveActive(connection), table, column, query, limit));

        public Task<List<Dictionary<string, object>>> SimilarAsync(string table,
            string column, double[] vector, int limit = 10, NpgsqlConnection connection = null)
            => Task.FromResult(Utils.Similar(ResolveActive(connection), table, column, vector, limit));

        public Task<List<Dictionary<string, object>>> SuggestAsync(string table,
            string column, string prefix, int limit = 10, NpgsqlConnection connection = null)
            => Task.FromResult(Utils.Suggest(ResolveActive(connection), table, column, prefix, limit));

        public Task<List<Dictionary<string, object>>> FacetsAsync(string table,
            string column, int limit = 50, string query = null, string queryColumn = null,
            string lang = "english", NpgsqlConnection connection = null)
            => Task.FromResult(Utils.Facets(ResolveActive(connection), table, column, limit, query, queryColumn, lang));

        public Task<List<Dictionary<string, object>>> FacetsAsync(string table,
            string column, string[] queryColumns, int limit = 50, string query = null,
            string lang = "english", NpgsqlConnection connection = null)
            => Task.FromResult(Utils.Facets(ResolveActive(connection), table, column, limit, query, queryColumns, lang));

        public Task<List<Dictionary<string, object>>> AggregateAsync(string table,
            string column, string func, string groupBy = null, int limit = 50,
            NpgsqlConnection connection = null)
            => Task.FromResult(Utils.Aggregate(ResolveActive(connection), table, column, func, groupBy, limit));

        public Task CreateSearchConfigAsync(string name, string copyFrom = "english", NpgsqlConnection connection = null)
        {
            Utils.CreateSearchConfig(ResolveActive(connection), name, copyFrom);
            return Task.CompletedTask;
        }

        // Pub/Sub
        public Task PublishAsync(string channel, string message, NpgsqlConnection connection = null)
        {
            Utils.Publish(ResolveActive(connection), channel, message);
            return Task.CompletedTask;
        }

        public Task SubscribeAsync(string channel, Action<string, string> callback, bool blocking = true,
            NpgsqlConnection connection = null)
        {
            Utils.Subscribe(ResolveActive(connection), channel, callback, blocking);
            return Task.CompletedTask;
        }

        // Phase 5 of schema-to-core retired the legacy flat redis-compat
        // methods (EnqueueAsync / DequeueAsync / IncrAsync / GetCounterAsync /
        // Hset / Hget / Hgetall / Hdel / Zadd / Zincrby / Zrange / Zrank /
        // Zscore / Zrem / Geoadd / Geoaddius / Geodist). Use the proxy-owned
        // family namespaces instead: gl.Counters, gl.Zsets, gl.Hashes,
        // gl.Queues, gl.Geos. Pub/sub stays flat (no DDL family).

        // Misc
        public Task<long> CountDistinctAsync(string table, string column, NpgsqlConnection connection = null)
            => Task.FromResult(Utils.CountDistinct(ResolveActive(connection), table, column));

        public Task<string> ScriptAsync(string luaCode, string[] args = null, NpgsqlConnection connection = null)
            => Task.FromResult(Utils.Script(ResolveActive(connection), luaCode, args ?? Array.Empty<string>()));

        // Streams: gl.Streams.<Verb>Async(...). See Streams.cs.

        // Percolate
        public Task PercolateAddAsync(string name, string queryId, string query,
            string lang = "english", string metadataJson = null, NpgsqlConnection connection = null)
        {
            Utils.PercolateAdd(ResolveActive(connection), name, queryId, query, lang, metadataJson);
            return Task.CompletedTask;
        }

        public Task<List<Dictionary<string, object>>> PercolateAsync(string name,
            string text, int limit = 50, string lang = "english", NpgsqlConnection connection = null)
            => Task.FromResult(Utils.Percolate(ResolveActive(connection), name, text, limit, lang));

        public Task<bool> PercolateDeleteAsync(string name, string queryId, NpgsqlConnection connection = null)
            => Task.FromResult(Utils.PercolateDelete(ResolveActive(connection), name, queryId));

        // Debug
        public Task<List<Dictionary<string, object>>> AnalyzeAsync(string text, string lang = "english",
            NpgsqlConnection connection = null)
            => Task.FromResult(Utils.Analyze(ResolveActive(connection), text, lang));

        public Task<Dictionary<string, object>> ExplainScoreAsync(string table,
            string column, string query, string idColumn, object idValue, string lang = "english",
            NpgsqlConnection connection = null)
            => Task.FromResult(Utils.ExplainScore(ResolveActive(connection), table, column, query, idColumn, idValue, lang));
    }
}
