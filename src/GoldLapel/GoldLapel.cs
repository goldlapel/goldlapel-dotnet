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

namespace GoldLapel
{
    public class GoldLapelOptions
    {
        public int? Port { get; set; }
        public string[] ExtraArgs { get; set; }
        public Dictionary<string, object> Config { get; set; }
    }

    public class GoldLapel : IDisposable
    {
        internal const int DefaultPort = 7932;
        internal const int DefaultDashboardPort = 7933;
        internal const long StartupTimeoutMs = 10000;
        internal const long StartupPollIntervalMs = 50;

        private static readonly HashSet<string> ValidConfigKeys = new HashSet<string>(new[]
        {
            "mode", "minPatternCount", "refreshIntervalSecs", "patternTtlSecs",
            "maxTablesPerView", "maxColumnsPerView", "deepPaginationThreshold",
            "reportIntervalSecs", "resultCacheSize", "batchCacheSize",
            "batchCacheTtlSecs", "poolSize", "poolTimeoutSecs",
            "poolMode", "mgmtIdleTimeout", "fallback", "readAfterWriteSecs",
            "n1Threshold", "n1WindowMs", "n1CrossThreshold",
            "tlsCert", "tlsKey", "tlsClientCa", "config", "dashboardPort",
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

        private readonly string _upstream;
        private readonly int _port;
        private readonly int _dashboardPort;
        private readonly string[] _extraArgs;
        private readonly Dictionary<string, object> _config;
        private Process _process;
        private string _proxyUrl;
        private bool _disposed;
        private DbConnection _wrappedConn;
        private DbConnection _conn;

        public GoldLapel(string upstream) : this(upstream, null) { }

        public GoldLapel(string upstream, GoldLapelOptions options)
        {
            if (upstream == null) throw new ArgumentNullException(nameof(upstream));
            _upstream = upstream;
            _port = options?.Port ?? DefaultPort;
            _dashboardPort = options?.Config != null && options.Config.ContainsKey("dashboardPort")
                ? Convert.ToInt32(options.Config["dashboardPort"])
                : DefaultDashboardPort;
            _extraArgs = options?.ExtraArgs ?? Array.Empty<string>();
            _config = options?.Config;
        }

        public string StartProxy()
        {
            if (_process != null && !_process.HasExited)
                return _proxyUrl;

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

            // Drain stderr to prevent pipe-buffer deadlock
            var stderrBuf = new System.Text.StringBuilder();
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
            });
            stderrThread.IsBackground = true;
            stderrThread.Start();

            // Drain stdout
            var stdoutThread = new Thread(() =>
            {
                try
                {
                    var buffer = new char[1024];
                    while (_process.StandardOutput.Read(buffer, 0, buffer.Length) > 0) { }
                }
                catch { }
            });
            stdoutThread.IsBackground = true;
            stdoutThread.Start();

            // Poll for port readiness
            var sw = Stopwatch.StartNew();
            var ready = false;
            while (sw.ElapsedMilliseconds < StartupTimeoutMs)
            {
                if (_process.HasExited) break;
                if (WaitForPort("127.0.0.1", _port, 500))
                {
                    ready = true;
                    break;
                }
            }

            if (!ready)
            {
                try { _process.Kill(); } catch { }
                try { _process.WaitForExit(5000); } catch { }
                try { _process.Dispose(); } catch { }
                _process = null;
                try { stderrThread.Join(2000); } catch { }
                throw new InvalidOperationException(
                    "Gold Lapel failed to start on port " + _port +
                    " within " + (StartupTimeoutMs / 1000) + "s.\nstderr: " + stderrBuf
                );
            }

            _proxyUrl = MakeProxyUrl(_upstream, _port);

            // Create the instance connection for convenience methods
            _conn = TryCreateConnection();

            if (_dashboardPort > 0)
                Console.WriteLine($"goldlapel \u2192 :{_port} (proxy) | http://127.0.0.1:{_dashboardPort} (dashboard)");
            else
                Console.WriteLine($"goldlapel \u2192 :{_port} (proxy)");

            return _proxyUrl;
        }

        public void StopProxy()
        {
            if (_conn != null)
            {
                try { _conn.Close(); } catch { }
                try { _conn.Dispose(); } catch { }
                _conn = null;
            }

            var proc = _process;
            _process = null;
            _proxyUrl = null;
            if (proc != null)
            {
                if (!proc.HasExited)
                {
                    GracefulStop(proc);
                }
                try { proc.Dispose(); } catch { }
            }
        }

        private const int GracefulTimeoutMs = 5000;

        private static void GracefulStop(Process proc)
        {
            // On Unix/macOS, send SIGTERM first for graceful shutdown (flush data, close
            // connections, write telemetry). Fall back to SIGKILL if the process doesn't
            // exit within the timeout. On Windows, there is no SIGTERM equivalent, so
            // Kill() is the only option.
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                try
                {
                    if (SendSignal(proc.Id, 15)) // SIGTERM = 15
                    {
                        if (proc.WaitForExit(GracefulTimeoutMs))
                            return;
                    }
                }
                catch { }
            }

            // Fallback: forceful kill
            try { proc.Kill(); } catch { }
            try { proc.WaitForExit(GracefulTimeoutMs); } catch { }
        }

        [DllImport("libc", SetLastError = true, EntryPoint = "kill")]
        private static extern int sys_kill(int pid, int sig);

        internal static bool SendSignal(int pid, int signal)
        {
            return sys_kill(pid, signal) == 0;
        }

        public DbConnection Connection
        {
            get
            {
                if (_conn == null)
                    throw new InvalidOperationException(
                        "No connection available. Call StartProxy() first, and ensure Npgsql " +
                        "is installed (dotnet add package Npgsql).");
                return _conn;
            }
        }

        public string Url => _proxyUrl;

        public int Port => _port;

        public bool IsRunning => _process != null && !_process.HasExited;

        public string DashboardUrl
        {
            get
            {
                if (_dashboardPort > 0 && _process != null && !_process.HasExited)
                    return $"http://127.0.0.1:{_dashboardPort}";
                return null;
            }
        }

        public void Dispose()
        {
            if (!_disposed)
            {
                StopProxy();
                _disposed = true;
            }
        }

        private DbConnection TryCreateConnection()
        {
            try
            {
                var npgsqlAssembly = System.Reflection.Assembly.Load("Npgsql");
                var connType = npgsqlAssembly.GetType("Npgsql.NpgsqlConnection");
                if (connType == null) return null;

                var conn = (DbConnection)Activator.CreateInstance(connType, _proxyUrl);
                conn.Open();
                return conn;
            }
            catch
            {
                return null;
            }
        }

        // ── Instance methods (delegate to Utils with stored connection) ──

        // Document store
        public Dictionary<string, object> DocInsert(string collection, string documentJson)
            => Utils.DocInsert(Connection, collection, documentJson);

        public List<Dictionary<string, object>> DocInsertMany(string collection, List<string> documents)
            => Utils.DocInsertMany(Connection, collection, documents);

        public List<Dictionary<string, object>> DocFind(string collection,
            string filterJson = null, Dictionary<string, int> sort = null, int? limit = null, int? skip = null)
            => Utils.DocFind(Connection, collection, filterJson, sort, limit, skip);

        public IEnumerable<Dictionary<string, object>> DocFindCursor(string collection,
            string filterJson = null, string sortJson = null, int? limit = null, int? skip = null, int batchSize = 100)
            => Utils.DocFindCursor(Connection, collection, filterJson, sortJson, limit, skip, batchSize);

        public Dictionary<string, object> DocFindOne(string collection, string filterJson = null)
            => Utils.DocFindOne(Connection, collection, filterJson);

        public int DocUpdate(string collection, string filterJson, string updateJson)
            => Utils.DocUpdate(Connection, collection, filterJson, updateJson);

        public int DocUpdateOne(string collection, string filterJson, string updateJson)
            => Utils.DocUpdateOne(Connection, collection, filterJson, updateJson);

        public int DocDelete(string collection, string filterJson)
            => Utils.DocDelete(Connection, collection, filterJson);

        public int DocDeleteOne(string collection, string filterJson)
            => Utils.DocDeleteOne(Connection, collection, filterJson);

        public long DocCount(string collection, string filterJson = null)
            => Utils.DocCount(Connection, collection, filterJson);

        public Dictionary<string, object> DocFindOneAndUpdate(string collection, string filterJson, string updateJson)
            => Utils.DocFindOneAndUpdate(Connection, collection, filterJson, updateJson);

        public Dictionary<string, object> DocFindOneAndDelete(string collection, string filterJson)
            => Utils.DocFindOneAndDelete(Connection, collection, filterJson);

        public List<string> DocDistinct(string collection, string field, string filterJson = null)
            => Utils.DocDistinct(Connection, collection, field, filterJson);

        public void DocCreateIndex(string collection, List<string> keys = null)
            => Utils.DocCreateIndex(Connection, collection, keys);

        public List<Dictionary<string, object>> DocAggregate(string collection, string pipelineJson)
            => Utils.DocAggregate(Connection, collection, pipelineJson);

        public void DocWatch(string collection, Action<string, string> callback, bool blocking = true)
            => Utils.DocWatch(Connection, collection, callback, blocking);

        public void DocUnwatch(string collection)
            => Utils.DocUnwatch(Connection, collection);

        public void DocCreateTtlIndex(string collection, int expireAfterSeconds, string field = "created_at")
            => Utils.DocCreateTtlIndex(Connection, collection, expireAfterSeconds, field);

        public void DocRemoveTtlIndex(string collection)
            => Utils.DocRemoveTtlIndex(Connection, collection);

        public void DocCreateCapped(string collection, int maxDocuments)
            => Utils.DocCreateCapped(Connection, collection, maxDocuments);

        public void DocRemoveCap(string collection)
            => Utils.DocRemoveCap(Connection, collection);

        // Search
        public List<Dictionary<string, object>> Search(string table,
            string column, string query, int limit = 50, string lang = "english", bool highlight = false)
            => Utils.Search(Connection, table, column, query, limit, lang, highlight);

        public List<Dictionary<string, object>> Search(string table,
            string[] columns, string query, int limit = 50, string lang = "english", bool highlight = false)
            => Utils.Search(Connection, table, columns, query, limit, lang, highlight);

        public List<Dictionary<string, object>> SearchFuzzy(string table,
            string column, string query, int limit = 50, double threshold = 0.3)
            => Utils.SearchFuzzy(Connection, table, column, query, limit, threshold);

        public List<Dictionary<string, object>> SearchPhonetic(string table,
            string column, string query, int limit = 50)
            => Utils.SearchPhonetic(Connection, table, column, query, limit);

        public List<Dictionary<string, object>> Similar(string table,
            string column, double[] vector, int limit = 10)
            => Utils.Similar(Connection, table, column, vector, limit);

        public List<Dictionary<string, object>> Suggest(string table,
            string column, string prefix, int limit = 10)
            => Utils.Suggest(Connection, table, column, prefix, limit);

        public List<Dictionary<string, object>> Facets(string table,
            string column, int limit = 50, string query = null, string queryColumn = null,
            string lang = "english")
            => Utils.Facets(Connection, table, column, limit, query, queryColumn, lang);

        public List<Dictionary<string, object>> Facets(string table,
            string column, int limit = 50, string query = null, string[] queryColumn = null,
            string lang = "english")
            => Utils.Facets(Connection, table, column, limit, query, queryColumn, lang);

        public List<Dictionary<string, object>> Aggregate(string table,
            string column, string func, string groupBy = null, int limit = 50)
            => Utils.Aggregate(Connection, table, column, func, groupBy, limit);

        public void CreateSearchConfig(string name, string copyFrom = "english")
            => Utils.CreateSearchConfig(Connection, name, copyFrom);

        // Pub/Sub & Queue
        public void Publish(string channel, string message)
            => Utils.Publish(Connection, channel, message);

        public void Subscribe(string channel, Action<string, string> callback, bool blocking = true)
            => Utils.Subscribe(Connection, channel, callback, blocking);

        public void Enqueue(string queueTable, string payloadJson)
            => Utils.Enqueue(Connection, queueTable, payloadJson);

        public string Dequeue(string queueTable)
            => Utils.Dequeue(Connection, queueTable);

        public long Incr(string table, string key, long amount = 1)
            => Utils.Incr(Connection, table, key, amount);

        public long GetCounter(string table, string key)
            => Utils.GetCounter(Connection, table, key);

        // Hash
        public void Hset(string table, string key, string field, string valueJson)
            => Utils.Hset(Connection, table, key, field, valueJson);

        public string Hget(string table, string key, string field)
            => Utils.Hget(Connection, table, key, field);

        public string Hgetall(string table, string key)
            => Utils.Hgetall(Connection, table, key);

        public bool Hdel(string table, string key, string field)
            => Utils.Hdel(Connection, table, key, field);

        // Sorted set
        public void Zadd(string table, string member, double score)
            => Utils.Zadd(Connection, table, member, score);

        public double Zincrby(string table, string member, double amount = 1)
            => Utils.Zincrby(Connection, table, member, amount);

        public List<(string Member, double Score)> Zrange(string table,
            int start = 0, int stop = 10, bool desc = true)
            => Utils.Zrange(Connection, table, start, stop, desc);

        public long? Zrank(string table, string member, bool desc = true)
            => Utils.Zrank(Connection, table, member, desc);

        public double? Zscore(string table, string member)
            => Utils.Zscore(Connection, table, member);

        public bool Zrem(string table, string member)
            => Utils.Zrem(Connection, table, member);

        // Geo
        public List<Dictionary<string, object>> Georadius(string table,
            string geomColumn, double lon, double lat, double radiusMeters, int limit = 50)
            => Utils.Georadius(Connection, table, geomColumn, lon, lat, radiusMeters, limit);

        public void Geoadd(string table, string nameColumn,
            string geomColumn, string name, double lon, double lat)
            => Utils.Geoadd(Connection, table, nameColumn, geomColumn, name, lon, lat);

        public double? Geodist(string table, string geomColumn,
            string nameColumn, string nameA, string nameB)
            => Utils.Geodist(Connection, table, geomColumn, nameColumn, nameA, nameB);

        // Misc
        public long CountDistinct(string table, string column)
            => Utils.CountDistinct(Connection, table, column);

        public string Script(string luaCode, params string[] args)
            => Utils.Script(Connection, luaCode, args);

        // Streams
        public long StreamAdd(string stream, string payload)
            => Utils.StreamAdd(Connection, stream, payload);

        public void StreamCreateGroup(string stream, string group)
            => Utils.StreamCreateGroup(Connection, stream, group);

        public List<Dictionary<string, object>> StreamRead(string stream,
            string group, string consumer, int count = 1)
            => Utils.StreamRead(Connection, stream, group, consumer, count);

        public bool StreamAck(string stream, string group, long messageId)
            => Utils.StreamAck(Connection, stream, group, messageId);

        public List<Dictionary<string, object>> StreamClaim(string stream,
            string group, string consumer, long minIdleMs = 60000)
            => Utils.StreamClaim(Connection, stream, group, consumer, minIdleMs);

        // Percolate
        public void PercolateAdd(string name, string queryId,
            string query, string lang = "english", string metadataJson = null)
            => Utils.PercolateAdd(Connection, name, queryId, query, lang, metadataJson);

        public List<Dictionary<string, object>> Percolate(string name,
            string text, int limit = 50, string lang = "english")
            => Utils.Percolate(Connection, name, text, limit, lang);

        public bool PercolateDelete(string name, string queryId)
            => Utils.PercolateDelete(Connection, name, queryId);

        // Debug
        public List<Dictionary<string, object>> Analyze(string text, string lang = "english")
            => Utils.Analyze(Connection, text, lang);

        public Dictionary<string, object> ExplainScore(string table,
            string column, string query, string idColumn, object idValue, string lang = "english")
            => Utils.ExplainScore(Connection, table, column, query, idColumn, idValue, lang);

        // ── Singleton ─────────────────────────────────────────

        private static GoldLapel _instance;
        private static bool _cleanupRegistered;
        private static readonly object _lock = new object();

        public static string Start(string upstream)
        {
            return Start(upstream, null);
        }

        public static string Start(string upstream, GoldLapelOptions options)
        {
            lock (_lock)
            {
                if (_instance != null && _instance.IsRunning)
                {
                    if (_instance._upstream != upstream)
                    {
                        throw new InvalidOperationException(
                            "Gold Lapel is already running for a different upstream. " +
                            "Call GoldLapel.Stop() before starting with a new upstream."
                        );
                    }
                    return _instance.Url;
                }
                _instance?.Dispose();
                _instance = new GoldLapel(upstream, options);
                if (!_cleanupRegistered)
                {
                    AppDomain.CurrentDomain.ProcessExit += (s, e) =>
                    {
                        lock (_lock)
                        {
                            if (_instance != null)
                            {
                                _instance.StopProxy();
                                _instance = null;
                            }
                        }
                    };
                    _cleanupRegistered = true;
                }
                return _instance.StartProxy();
            }
        }

        public static DbConnection StartConnection(string upstream)
        {
            return StartConnection(upstream, null);
        }

        public static DbConnection StartConnection(string upstream, GoldLapelOptions options)
        {
            lock (_lock)
            {
                if (_instance != null && _instance.IsRunning)
                {
                    if (_instance._upstream != upstream)
                    {
                        throw new InvalidOperationException(
                            "Gold Lapel is already running for a different upstream. " +
                            "Call GoldLapel.Stop() before starting with a new upstream."
                        );
                    }
                    if (_instance._wrappedConn != null)
                        return _instance._wrappedConn;
                    var existing = TryWrapConnection(_instance);
                    if (existing == null)
                    {
                        throw new InvalidOperationException(
                            "No supported database driver found. " +
                            "Add Npgsql to your dependencies (dotnet add package Npgsql) " +
                            "or use GoldLapel.Start() / GoldLapel.ProxyUrl if you only need the connection string."
                        );
                    }
                    return existing;
                }
                _instance?.Dispose();
                _instance = new GoldLapel(upstream, options);
                if (!_cleanupRegistered)
                {
                    AppDomain.CurrentDomain.ProcessExit += (s, e) =>
                    {
                        lock (_lock)
                        {
                            if (_instance != null)
                            {
                                _instance.StopProxy();
                                _instance = null;
                            }
                        }
                    };
                    _cleanupRegistered = true;
                }
                _instance.StartProxy();
                var wrapped = TryWrapConnection(_instance);
                if (wrapped == null)
                {
                    throw new InvalidOperationException(
                        "No supported database driver found. " +
                        "Add Npgsql to your dependencies (dotnet add package Npgsql) " +
                        "or use GoldLapel.Start() / GoldLapel.ProxyUrl if you only need the connection string."
                    );
                }
                return wrapped;
            }
        }

        private static DbConnection TryWrapConnection(GoldLapel inst)
        {
            try
            {
                // Try to load Npgsql dynamically
                var npgsqlAssembly = System.Reflection.Assembly.Load("Npgsql");
                var connType = npgsqlAssembly.GetType("Npgsql.NpgsqlConnection");
                if (connType == null) return null;

                var conn = (DbConnection)Activator.CreateInstance(connType, inst._proxyUrl);
                conn.Open();

                var cache = NativeCache.GetInstance();
                int invPort = inst._port + 2;
                if (inst._config != null && inst._config.ContainsKey("invalidationPort"))
                    invPort = Convert.ToInt32(inst._config["invalidationPort"]);
                cache.ConnectInvalidation(invPort);

                var wrapped = new CachedConnection(conn, cache);
                inst._wrappedConn = wrapped;
                return wrapped;
            }
            catch
            {
                return null;
            }
        }

        public static void Stop()
        {
            lock (_lock)
            {
                if (_instance != null)
                {
                    _instance.StopProxy();
                    _instance = null;
                }
            }
        }

        public static string ProxyUrl
        {
            get
            {
                lock (_lock)
                {
                    return _instance?.Url;
                }
            }
        }

        public static string DashboardProxyUrl
        {
            get
            {
                lock (_lock)
                {
                    return _instance?.DashboardUrl;
                }
            }
        }

        public static IReadOnlyCollection<string> ConfigKeys()
        {
            return ValidConfigKeys;
        }

        // ── Internal methods ──────────────────────────────────

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

        private static readonly Regex WithPort =
            new Regex(@"^(postgres(?:ql)?://(?:.*@)?)([^:/?#]+):(\d+)(.*)$");

        private static readonly Regex NoPort =
            new Regex(@"^(postgres(?:ql)?://(?:.*@)?)([^:/?#]+)(.*)$");

        internal static string FindBinary()
        {
            // 1. Explicit override via env var
            var envPath = Environment.GetEnvironmentVariable("GOLDLAPEL_BINARY");
            if (!string.IsNullOrEmpty(envPath))
            {
                if (File.Exists(envPath)) return envPath;
                throw new InvalidOperationException(
                    "GOLDLAPEL_BINARY points to " + envPath + " but file not found"
                );
            }

            // 2. NuGet runtime asset (binary next to assembly or runtimes/<rid>/native/)
            var bundled = FindBundledBinary();
            if (bundled != null) return bundled;

            // 3. On PATH
            var onPath = FindOnPath();
            if (onPath != null) return onPath;

            throw new InvalidOperationException(
                "Gold Lapel binary not found. Set GOLDLAPEL_BINARY env var, " +
                "install the NuGet package with bundled binaries, or ensure 'goldlapel' is on PATH."
            );
        }

        internal static string MakeProxyUrl(string upstream, int port)
        {
            // Uses regex instead of System.Uri to avoid decoding percent-encoded
            // characters in passwords (e.g. %40 for @), which would corrupt the URL.

            var m = WithPort.Match(upstream);
            if (m.Success)
                return m.Groups[1].Value + "localhost:" + port + m.Groups[4].Value;

            m = NoPort.Match(upstream);
            if (m.Success)
                return m.Groups[1].Value + "localhost:" + port + m.Groups[3].Value;

            // bare host:port
            if (!upstream.Contains("://") && upstream.Contains(":"))
                return "localhost:" + port;

            // bare host
            return "localhost:" + port;
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

        private static string FindBundledBinary()
        {
            var isWindows = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
            var binaryName = isWindows ? "goldlapel.exe" : "goldlapel";

            // Determine RID
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

            // Check next to the assembly (NuGet copies runtime assets here)
            var assemblyDir = Path.GetDirectoryName(typeof(GoldLapel).Assembly.Location);
            if (!string.IsNullOrEmpty(assemblyDir))
            {
                var nextTo = Path.Combine(assemblyDir, binaryName);
                if (File.Exists(nextTo)) return nextTo;

                // Check runtimes/<rid>/native/ relative to assembly
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
    }
}
