using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace Goldlapel
{
    /// <summary>
    /// Construction-time options for <see cref="GoldLapel.StartAsync(string, Action{GoldLapelOptions})"/>.
    /// Populated via a configurator lambda:
    /// <code>
    /// await GoldLapel.StartAsync(url, opts => { opts.Port = 7932; opts.LogLevel = "info"; });
    /// </code>
    /// </summary>
    public class GoldLapelOptions
    {
        /// <summary>Proxy listen port (default: 7932).</summary>
        public int Port { get; set; } = GoldLapel.DefaultPort;

        /// <summary>
        /// Log level for the proxy. Accepted values: <c>"trace"</c>, <c>"debug"</c>,
        /// <c>"info"</c>, <c>"warn"</c>, <c>"error"</c>. Only trace/debug/info produce
        /// visible output; warn/error are the binary's default level.
        /// Shortcut for <c>Config["logLevel"]</c>.
        /// </summary>
        public string LogLevel { get; set; }

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
    }

    /// <summary>
    /// Factory + handle for a Gold Lapel proxy process. Construct via
    /// <see cref="StartAsync(string, Action{GoldLapelOptions})"/>; dispose with
    /// <c>await using</c> (or call <see cref="DisposeAsync"/> directly) to stop the proxy.
    /// </summary>
    public class GoldLapel : IAsyncDisposable, IDisposable
    {
        internal const int DefaultPort = 7932;
        internal const int DefaultDashboardPort = 7933;
        internal const long StartupTimeoutMs = 10000;
        internal const long StartupPollIntervalMs = 50;
        private const int GracefulTimeoutMs = 5000;

        private static readonly HashSet<string> ValidConfigKeys = new HashSet<string>(new[]
        {
            "mode", "minPatternCount", "refreshIntervalSecs", "patternTtlSecs",
            "maxTablesPerView", "maxColumnsPerView", "deepPaginationThreshold",
            "reportIntervalSecs", "resultCacheSize", "batchCacheSize",
            "batchCacheTtlSecs", "poolSize", "poolTimeoutSecs",
            "poolMode", "mgmtIdleTimeout", "fallback", "readAfterWriteSecs",
            "n1Threshold", "n1WindowMs", "n1CrossThreshold",
            "tlsCert", "tlsKey", "tlsClientCa", "config", "dashboardPort",
            "logLevel",
            "disableMatviews", "disableConsolidation", "disableBtreeIndexes",
            "disableTrigramIndexes", "disableExpressionIndexes",
            "disablePartialIndexes", "disableRewrite", "disablePreparedCache",
            "disableResultCache", "disablePool",
            "disableN1", "disableN1CrossConnection", "disableShadowMode",
            "enableCoalescing", "replica", "excludeTables",
            "invalidationPort"
        });

        private static readonly HashSet<string> BooleanKeys = new HashSet<string>(new[]
        {
            "disableMatviews", "disableConsolidation", "disableBtreeIndexes",
            "disableTrigramIndexes", "disableExpressionIndexes",
            "disablePartialIndexes", "disableRewrite", "disablePreparedCache",
            "disableResultCache", "disablePool",
            "disableN1", "disableN1CrossConnection", "disableShadowMode",
            "enableCoalescing"
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
        private readonly int _port;
        private readonly int _dashboardPort;
        private readonly string[] _extraArgs;
        private readonly Dictionary<string, object> _config;
        private readonly bool _silent;
        private Process _process;
        private string _proxyUrl;
        private bool _disposed;
        private NpgsqlConnection _conn;  // eagerly opened internal connection

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
            _port = options.Port;
            _extraArgs = options.ExtraArgs ?? Array.Empty<string>();
            _config = MergeConfig(options);
            _silent = options.Silent;
            // Dashboard defaults to proxy port + 1 (matches what the Rust binary
            // binds when no --dashboard-port is passed). Only when the user
            // supplies an explicit dashboardPort via Config does that value
            // override the derivation. This ensures Port=17932 correctly
            // reports the dashboard at :17933 rather than the hardcoded 7933.
            _dashboardPort = _config != null && _config.ContainsKey("dashboardPort")
                ? Convert.ToInt32(_config["dashboardPort"])
                : _port + 1;
        }

        // ── Factory ─────────────────────────────────────────────────

        /// <summary>
        /// Spawn the Gold Lapel proxy for the given upstream URL, wait for it to accept
        /// connections, eagerly open an internal <see cref="NpgsqlConnection"/> against
        /// the proxy, and return a ready <see cref="GoldLapel"/> handle.
        /// </summary>
        /// <example>
        /// <code>
        /// await using var gl = await GoldLapel.StartAsync(
        ///     "postgresql://user:pass@db/mydb",
        ///     opts => { opts.Port = 7932; opts.LogLevel = "info"; });
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
            try
            {
                await gl.SpawnAsync().ConfigureAwait(false);
            }
            catch
            {
                gl.StopProcessInternal();
                throw;
            }
            return gl;
        }

        private static Dictionary<string, object> MergeConfig(GoldLapelOptions options)
        {
            // Merge Config dictionary + LogLevel shortcut into one map.
            if (options.Config == null && string.IsNullOrEmpty(options.LogLevel))
                return null;

            var merged = options.Config != null
                ? new Dictionary<string, object>(options.Config)
                : new Dictionary<string, object>();

            if (!string.IsNullOrEmpty(options.LogLevel))
                merged["logLevel"] = options.LogLevel;

            return merged;
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
        public int Port => _port;

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
        /// need the port number at construction time, read it from your
        /// <c>Config["dashboardPort"]</c> or derive it as <c>Port + 1</c>.
        /// </remarks>
        public string DashboardUrl =>
            _dashboardPort > 0 && _process != null && !_process.HasExited
                ? "http://127.0.0.1:" + _dashboardPort
                : null;

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
        ///     await gl.DocInsertAsync("events", new { type = "order.created" });
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

            if (_conn != null)
            {
                try { await _conn.CloseAsync().ConfigureAwait(false); } catch { }
                try { await _conn.DisposeAsync().ConfigureAwait(false); } catch { }
                _conn = null;
            }
            StopProcessInternal();
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            if (_conn != null)
            {
                try { _conn.Close(); } catch { }
                try { _conn.Dispose(); } catch { }
                _conn = null;
            }
            StopProcessInternal();
        }

        private void StopProcessInternal()
        {
            var proc = _process;
            _process = null;
            _proxyUrl = null;
            if (proc != null)
            {
                if (!proc.HasExited) GracefulStop(proc);
                try { proc.Dispose(); } catch { }
            }
        }

        // ── Process spawn ───────────────────────────────────────────

        private async Task SpawnAsync()
        {
            var binary = FindBinary();
            var args = new List<string> { "--upstream", _upstream, "--proxy-port", _port.ToString() };
            args.AddRange(ConfigToArgs(_config));
            args.AddRange(_extraArgs);

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
            if (!psi.EnvironmentVariables.ContainsKey("GOLDLAPEL_CLIENT"))
                psi.EnvironmentVariables["GOLDLAPEL_CLIENT"] = "dotnet";

            try
            {
                _process = Process.Start(psi);
                _process.StandardInput.Close();
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to start Gold Lapel process", e);
            }

            // Drain stderr to prevent pipe-buffer deadlock.
            var stderrBuf = new StringBuilder();
            var stderrThread = new Thread(() =>
            {
                try
                {
                    var buffer = new char[1024];
                    int n;
                    while ((n = _process.StandardError.Read(buffer, 0, buffer.Length)) > 0)
                        stderrBuf.Append(buffer, 0, n);
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
                    while (_process.StandardOutput.Read(buffer, 0, buffer.Length) > 0) { }
                }
                catch { }
            })
            { IsBackground = true };
            stdoutThread.Start();

            // Poll for port readiness within a single StartupTimeoutMs budget.
            // Earlier versions wrapped a looping WaitForPortAsync in this loop,
            // which double-budgeted the timeout (each outer iteration consumed
            // up to 500ms of inner retries inside a 500ms per-attempt budget,
            // so total elapsed could exceed StartupTimeoutMs).
            var ready = await PollForPortAsync(
                "127.0.0.1", _port, StartupTimeoutMs,
                () => _process.HasExited).ConfigureAwait(false);

            if (!ready)
            {
                try { _process.Kill(); } catch { }
                try { _process.WaitForExit(5000); } catch { }
                try { _process.Dispose(); } catch { }
                _process = null;
                try { stderrThread.Join(2000); } catch { }
                throw new InvalidOperationException(
                    "Gold Lapel failed to start on port " + _port +
                    " within " + (StartupTimeoutMs / 1000) + "s.\nstderr: " + stderrBuf);
            }

            _proxyUrl = MakeProxyUrl(_upstream, _port);

            // Eagerly open the internal Npgsql connection. Npgsql does not accept
            // URL-style connection strings, so convert to key-value form.
            _conn = new NpgsqlConnection(UrlToNpgsqlConnectionString(_proxyUrl));
            await _conn.OpenAsync().ConfigureAwait(false);

            // Write the startup banner to stderr (not stdout) so it doesn't pollute
            // application stdout — ASP.NET Core logs, CLI app output, shells that
            // redirect stdout, etc. Opt out entirely via GoldLapelOptions.Silent.
            if (!_silent)
            {
                if (_dashboardPort > 0)
                    Console.Error.WriteLine($"goldlapel \u2192 :{_port} (proxy) | http://127.0.0.1:{_dashboardPort} (dashboard)");
                else
                    Console.Error.WriteLine($"goldlapel \u2192 :{_port} (proxy)");
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

                if (!ValidConfigKeys.Contains(key))
                    throw new ArgumentException("Unknown config key: " + key);

                // logLevel is a wrapper-side concept. The proxy binary uses
                // count-based verbosity (-v/-vv/-vvv), not --log-level.
                if (key == "logLevel")
                {
                    var levelStr = value as string;
                    if (levelStr == null)
                        throw new ArgumentException(
                            "Config key 'logLevel' must be a string, got " + value.GetType().Name);
                    var verboseFlag = LogLevelToVerboseFlag(levelStr);
                    if (verboseFlag != null)
                        result.Add(verboseFlag);
                    continue;
                }

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

        internal static string MakeProxyUrl(string upstream, int port)
        {
            // Use regex not System.Uri to preserve percent-encoded characters in passwords
            // (e.g. %40 for @), which Uri would decode and corrupt the URL.
            var m = WithPort.Match(upstream);
            if (m.Success)
                return m.Groups[1].Value + "localhost:" + port + m.Groups[4].Value;

            m = NoPort.Match(upstream);
            if (m.Success)
                return m.Groups[1].Value + "localhost:" + port + m.Groups[3].Value;

            if (!upstream.Contains("://") && upstream.Contains(":"))
                return "localhost:" + port;

            return "localhost:" + port;
        }

        /// <summary>
        /// Convert a postgres URL (e.g. <c>postgresql://user:pass@host:port/db?sslmode=require</c>)
        /// into the key-value form Npgsql expects (<c>Host=host;Port=port;Username=user;Password=pass;Database=db;SslMode=Require</c>).
        /// If the input is already key-value form (contains <c>=</c>) it is returned unchanged.
        /// </summary>
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

            // host[:port][/database]
            string database = null;
            var slashIdx = rest.IndexOf('/');
            if (slashIdx >= 0)
            {
                database = rest.Substring(slashIdx + 1);
                rest = rest.Substring(0, slashIdx);
            }

            string host = rest;
            string port = null;
            var colonIdx = rest.LastIndexOf(':');
            if (colonIdx >= 0)
            {
                host = rest.Substring(0, colonIdx);
                port = rest.Substring(colonIdx + 1);
            }

            string user = null;
            string password = null;
            if (userinfo != null)
            {
                var uColon = userinfo.IndexOf(':');
                if (uColon >= 0)
                {
                    user = Uri.UnescapeDataString(userinfo.Substring(0, uColon));
                    password = Uri.UnescapeDataString(userinfo.Substring(uColon + 1));
                }
                else
                {
                    user = Uri.UnescapeDataString(userinfo);
                }
            }

            var sb = new StringBuilder();
            sb.Append("Host=").Append(host).Append(';');
            if (!string.IsNullOrEmpty(port)) sb.Append("Port=").Append(port).Append(';');
            if (!string.IsNullOrEmpty(user)) sb.Append("Username=").Append(user).Append(';');
            if (!string.IsNullOrEmpty(password)) sb.Append("Password=").Append(password).Append(';');
            if (!string.IsNullOrEmpty(database)) sb.Append("Database=").Append(database).Append(';');

            // Query params map 1:1 to Npgsql keywords (sslmode -> SslMode, etc.).
            if (!string.IsNullOrEmpty(query))
            {
                foreach (var pair in query.Split('&'))
                {
                    if (string.IsNullOrEmpty(pair)) continue;
                    var eq = pair.IndexOf('=');
                    if (eq < 0)
                    {
                        sb.Append(pair).Append(';');
                    }
                    else
                    {
                        var k = pair.Substring(0, eq);
                        var v = Uri.UnescapeDataString(pair.Substring(eq + 1));
                        sb.Append(k).Append('=').Append(v).Append(';');
                    }
                }
            }

            return sb.ToString();
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

        // Document store
        public Task<Dictionary<string, object>> DocInsertAsync(string collection, string documentJson, NpgsqlConnection connection = null)
            => Task.FromResult(Utils.DocInsert(ResolveActive(connection), collection, documentJson));

        public Task<List<Dictionary<string, object>>> DocInsertManyAsync(string collection, List<string> documents, NpgsqlConnection connection = null)
            => Task.FromResult(Utils.DocInsertMany(ResolveActive(connection), collection, documents));

        public Task<List<Dictionary<string, object>>> DocFindAsync(string collection,
            string filterJson = null, Dictionary<string, int> sort = null, int? limit = null, int? skip = null,
            NpgsqlConnection connection = null)
            => Task.FromResult(Utils.DocFind(ResolveActive(connection), collection, filterJson, sort, limit, skip));

        public IEnumerable<Dictionary<string, object>> DocFindCursor(string collection,
            string filterJson = null, string sortJson = null, int? limit = null, int? skip = null,
            int batchSize = 100, NpgsqlConnection connection = null)
            => Utils.DocFindCursor(ResolveActive(connection), collection, filterJson, sortJson, limit, skip, batchSize);

        public Task<Dictionary<string, object>> DocFindOneAsync(string collection,
            string filterJson = null, NpgsqlConnection connection = null)
            => Task.FromResult(Utils.DocFindOne(ResolveActive(connection), collection, filterJson));

        public Task<int> DocUpdateAsync(string collection, string filterJson, string updateJson,
            NpgsqlConnection connection = null)
            => Task.FromResult(Utils.DocUpdate(ResolveActive(connection), collection, filterJson, updateJson));

        public Task<int> DocUpdateOneAsync(string collection, string filterJson, string updateJson,
            NpgsqlConnection connection = null)
            => Task.FromResult(Utils.DocUpdateOne(ResolveActive(connection), collection, filterJson, updateJson));

        public Task<int> DocDeleteAsync(string collection, string filterJson, NpgsqlConnection connection = null)
            => Task.FromResult(Utils.DocDelete(ResolveActive(connection), collection, filterJson));

        public Task<int> DocDeleteOneAsync(string collection, string filterJson, NpgsqlConnection connection = null)
            => Task.FromResult(Utils.DocDeleteOne(ResolveActive(connection), collection, filterJson));

        public Task<long> DocCountAsync(string collection, string filterJson = null, NpgsqlConnection connection = null)
            => Task.FromResult(Utils.DocCount(ResolveActive(connection), collection, filterJson));

        public Task<Dictionary<string, object>> DocFindOneAndUpdateAsync(string collection, string filterJson,
            string updateJson, NpgsqlConnection connection = null)
            => Task.FromResult(Utils.DocFindOneAndUpdate(ResolveActive(connection), collection, filterJson, updateJson));

        public Task<Dictionary<string, object>> DocFindOneAndDeleteAsync(string collection, string filterJson,
            NpgsqlConnection connection = null)
            => Task.FromResult(Utils.DocFindOneAndDelete(ResolveActive(connection), collection, filterJson));

        public Task<List<string>> DocDistinctAsync(string collection, string field, string filterJson = null,
            NpgsqlConnection connection = null)
            => Task.FromResult(Utils.DocDistinct(ResolveActive(connection), collection, field, filterJson));

        public Task DocCreateIndexAsync(string collection, List<string> keys = null, NpgsqlConnection connection = null)
        {
            Utils.DocCreateIndex(ResolveActive(connection), collection, keys);
            return Task.CompletedTask;
        }

        public Task<List<Dictionary<string, object>>> DocAggregateAsync(string collection, string pipelineJson,
            NpgsqlConnection connection = null)
            => Task.FromResult(Utils.DocAggregate(ResolveActive(connection), collection, pipelineJson));

        public Task DocWatchAsync(string collection, Action<string, string> callback, bool blocking = true,
            NpgsqlConnection connection = null)
        {
            Utils.DocWatch(ResolveActive(connection), collection, callback, blocking);
            return Task.CompletedTask;
        }

        public Task DocUnwatchAsync(string collection, NpgsqlConnection connection = null)
        {
            Utils.DocUnwatch(ResolveActive(connection), collection);
            return Task.CompletedTask;
        }

        public Task DocCreateTtlIndexAsync(string collection, int expireAfterSeconds, string field = "created_at",
            NpgsqlConnection connection = null)
        {
            Utils.DocCreateTtlIndex(ResolveActive(connection), collection, expireAfterSeconds, field);
            return Task.CompletedTask;
        }

        public Task DocRemoveTtlIndexAsync(string collection, NpgsqlConnection connection = null)
        {
            Utils.DocRemoveTtlIndex(ResolveActive(connection), collection);
            return Task.CompletedTask;
        }

        public Task DocCreateCollectionAsync(string collection, bool unlogged = false, NpgsqlConnection connection = null)
        {
            Utils.DocCreateCollection(ResolveActive(connection), collection, unlogged);
            return Task.CompletedTask;
        }

        public Task DocCreateCappedAsync(string collection, int maxDocuments, NpgsqlConnection connection = null)
        {
            Utils.DocCreateCapped(ResolveActive(connection), collection, maxDocuments);
            return Task.CompletedTask;
        }

        public Task DocRemoveCapAsync(string collection, NpgsqlConnection connection = null)
        {
            Utils.DocRemoveCap(ResolveActive(connection), collection);
            return Task.CompletedTask;
        }

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

        // Pub/Sub & Queue
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

        public Task EnqueueAsync(string queueTable, string payloadJson, NpgsqlConnection connection = null)
        {
            Utils.Enqueue(ResolveActive(connection), queueTable, payloadJson);
            return Task.CompletedTask;
        }

        public Task<string> DequeueAsync(string queueTable, NpgsqlConnection connection = null)
            => Task.FromResult(Utils.Dequeue(ResolveActive(connection), queueTable));

        public Task<long> IncrAsync(string table, string key, long amount = 1, NpgsqlConnection connection = null)
            => Task.FromResult(Utils.Incr(ResolveActive(connection), table, key, amount));

        public Task<long> GetCounterAsync(string table, string key, NpgsqlConnection connection = null)
            => Task.FromResult(Utils.GetCounter(ResolveActive(connection), table, key));

        // Hash
        public Task HsetAsync(string table, string key, string field, string valueJson, NpgsqlConnection connection = null)
        {
            Utils.Hset(ResolveActive(connection), table, key, field, valueJson);
            return Task.CompletedTask;
        }

        public Task<string> HgetAsync(string table, string key, string field, NpgsqlConnection connection = null)
            => Task.FromResult(Utils.Hget(ResolveActive(connection), table, key, field));

        public Task<string> HgetallAsync(string table, string key, NpgsqlConnection connection = null)
            => Task.FromResult(Utils.Hgetall(ResolveActive(connection), table, key));

        public Task<bool> HdelAsync(string table, string key, string field, NpgsqlConnection connection = null)
            => Task.FromResult(Utils.Hdel(ResolveActive(connection), table, key, field));

        // Sorted set
        public Task ZaddAsync(string table, string member, double score, NpgsqlConnection connection = null)
        {
            Utils.Zadd(ResolveActive(connection), table, member, score);
            return Task.CompletedTask;
        }

        public Task<double> ZincrbyAsync(string table, string member, double amount = 1,
            NpgsqlConnection connection = null)
            => Task.FromResult(Utils.Zincrby(ResolveActive(connection), table, member, amount));

        public Task<List<(string Member, double Score)>> ZrangeAsync(string table,
            int start = 0, int stop = 10, bool desc = true, NpgsqlConnection connection = null)
            => Task.FromResult(Utils.Zrange(ResolveActive(connection), table, start, stop, desc));

        public Task<long?> ZrankAsync(string table, string member, bool desc = true,
            NpgsqlConnection connection = null)
            => Task.FromResult(Utils.Zrank(ResolveActive(connection), table, member, desc));

        public Task<double?> ZscoreAsync(string table, string member, NpgsqlConnection connection = null)
            => Task.FromResult(Utils.Zscore(ResolveActive(connection), table, member));

        public Task<bool> ZremAsync(string table, string member, NpgsqlConnection connection = null)
            => Task.FromResult(Utils.Zrem(ResolveActive(connection), table, member));

        // Geo
        public Task<List<Dictionary<string, object>>> GeoradiusAsync(string table,
            string geomColumn, double lon, double lat, double radiusMeters, int limit = 50,
            NpgsqlConnection connection = null)
            => Task.FromResult(Utils.Georadius(ResolveActive(connection), table, geomColumn, lon, lat, radiusMeters, limit));

        public Task GeoaddAsync(string table, string nameColumn, string geomColumn,
            string name, double lon, double lat, NpgsqlConnection connection = null)
        {
            Utils.Geoadd(ResolveActive(connection), table, nameColumn, geomColumn, name, lon, lat);
            return Task.CompletedTask;
        }

        public Task<double?> GeodistAsync(string table, string geomColumn, string nameColumn,
            string nameA, string nameB, NpgsqlConnection connection = null)
            => Task.FromResult(Utils.Geodist(ResolveActive(connection), table, geomColumn, nameColumn, nameA, nameB));

        // Misc
        public Task<long> CountDistinctAsync(string table, string column, NpgsqlConnection connection = null)
            => Task.FromResult(Utils.CountDistinct(ResolveActive(connection), table, column));

        public Task<string> ScriptAsync(string luaCode, string[] args = null, NpgsqlConnection connection = null)
            => Task.FromResult(Utils.Script(ResolveActive(connection), luaCode, args ?? Array.Empty<string>()));

        // Streams
        public Task<long> StreamAddAsync(string stream, string payload, NpgsqlConnection connection = null)
            => Task.FromResult(Utils.StreamAdd(ResolveActive(connection), stream, payload));

        public Task StreamCreateGroupAsync(string stream, string group, NpgsqlConnection connection = null)
        {
            Utils.StreamCreateGroup(ResolveActive(connection), stream, group);
            return Task.CompletedTask;
        }

        public Task<List<Dictionary<string, object>>> StreamReadAsync(string stream,
            string group, string consumer, int count = 1, NpgsqlConnection connection = null)
            => Task.FromResult(Utils.StreamRead(ResolveActive(connection), stream, group, consumer, count));

        public Task<bool> StreamAckAsync(string stream, string group, long messageId,
            NpgsqlConnection connection = null)
            => Task.FromResult(Utils.StreamAck(ResolveActive(connection), stream, group, messageId));

        public Task<List<Dictionary<string, object>>> StreamClaimAsync(string stream,
            string group, string consumer, long minIdleMs = 60000, NpgsqlConnection connection = null)
            => Task.FromResult(Utils.StreamClaim(ResolveActive(connection), stream, group, consumer, minIdleMs));

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
