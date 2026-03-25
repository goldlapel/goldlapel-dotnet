using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text.RegularExpressions;
using System.Threading;

namespace GoldLapel
{
    public class CacheEntry
    {
        public object[][] Rows { get; }
        public string[] Columns { get; }
        public HashSet<string> Tables { get; }

        public CacheEntry(object[][] rows, string[] columns, HashSet<string> tables)
        {
            Rows = rows;
            Columns = columns;
            Tables = tables;
        }
    }

    public class NativeCache
    {
        internal const string DdlSentinel = "__ddl__";

        private static readonly Regex TxStart = new Regex(@"^\s*(BEGIN|START\s+TRANSACTION)\b", RegexOptions.IgnoreCase);
        private static readonly Regex TxEnd = new Regex(@"^\s*(COMMIT|ROLLBACK|END)\b", RegexOptions.IgnoreCase);
        private static readonly Regex TablePattern = new Regex(@"\b(?:FROM|JOIN)\s+(?:ONLY\s+)?(?:(\w+)\.)?(\w+)", RegexOptions.IgnoreCase);

        private static readonly HashSet<string> SqlKeywords = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "select", "from", "where", "and", "or", "not", "in", "exists",
            "between", "like", "is", "null", "true", "false", "as", "on",
            "left", "right", "inner", "outer", "cross", "full", "natural",
            "group", "order", "having", "limit", "offset", "union", "intersect",
            "except", "all", "distinct", "lateral", "values"
        };

        private readonly ConcurrentDictionary<string, CacheEntry> _cache = new ConcurrentDictionary<string, CacheEntry>();
        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, byte>> _tableIndex = new ConcurrentDictionary<string, ConcurrentDictionary<string, byte>>();
        private readonly ConcurrentDictionary<string, long> _accessOrder = new ConcurrentDictionary<string, long>();
        private long _counter;
        private readonly int _maxEntries;
        private readonly bool _enabled;

        private volatile bool _invalidationConnected;
        private volatile bool _invalidationStop;
        private Thread _invalidationThread;
        private TcpClient _invalidationClient;
        private int _invalidationPort;
        private int _reconnectAttempt;

        internal long StatsHits;
        internal long StatsMisses;
        internal long StatsInvalidations;

        private static NativeCache _instance;
        private static readonly object InstanceLock = new object();

        public NativeCache()
        {
            var sizeStr = Environment.GetEnvironmentVariable("GOLDLAPEL_NATIVE_CACHE_SIZE");
            _maxEntries = !string.IsNullOrEmpty(sizeStr) ? int.Parse(sizeStr) : 32768;
            var enabledStr = Environment.GetEnvironmentVariable("GOLDLAPEL_NATIVE_CACHE");
            _enabled = string.IsNullOrEmpty(enabledStr) || !enabledStr.Equals("false", StringComparison.OrdinalIgnoreCase);
        }

        public static NativeCache GetInstance()
        {
            lock (InstanceLock)
            {
                if (_instance == null)
                    _instance = new NativeCache();
                return _instance;
            }
        }

        public static void Reset()
        {
            lock (InstanceLock)
            {
                if (_instance != null)
                {
                    _instance.StopInvalidation();
                    _instance = null;
                }
            }
        }

        public bool IsConnected => _invalidationConnected;
        public bool IsEnabled => _enabled;
        public int Size => _cache.Count;

        // --- Cache operations ---

        public CacheEntry Get(string sql, object[] parameters)
        {
            if (!_enabled || !_invalidationConnected) return null;
            var key = MakeKey(sql, parameters);
            if (key == null) return null;
            CacheEntry entry;
            if (_cache.TryGetValue(key, out entry))
            {
                _accessOrder[key] = Interlocked.Increment(ref _counter);
                Interlocked.Increment(ref StatsHits);
                return entry;
            }
            Interlocked.Increment(ref StatsMisses);
            return null;
        }

        public void Put(string sql, object[] parameters, object[][] rows, string[] columns)
        {
            if (!_enabled || !_invalidationConnected) return;
            var key = MakeKey(sql, parameters);
            if (key == null) return;
            var tables = ExtractTables(sql);
            if (!_cache.ContainsKey(key) && _cache.Count >= _maxEntries)
            {
                EvictOne();
            }
            _cache[key] = new CacheEntry(rows, columns, tables);
            _accessOrder[key] = Interlocked.Increment(ref _counter);
            foreach (var table in tables)
            {
                var keys = _tableIndex.GetOrAdd(table, _ => new ConcurrentDictionary<string, byte>());
                keys[key] = 0;
            }
        }

        public void InvalidateTable(string table)
        {
            table = table.ToLower();
            ConcurrentDictionary<string, byte> keys;
            if (!_tableIndex.TryRemove(table, out keys)) return;
            foreach (var key in keys.Keys)
            {
                CacheEntry entry;
                _cache.TryRemove(key, out entry);
                long removed;
                _accessOrder.TryRemove(key, out removed);
                if (entry != null)
                {
                    foreach (var otherTable in entry.Tables)
                    {
                        if (!otherTable.Equals(table))
                        {
                            ConcurrentDictionary<string, byte> otherKeys;
                            if (_tableIndex.TryGetValue(otherTable, out otherKeys))
                            {
                                byte b;
                                otherKeys.TryRemove(key, out b);
                                if (otherKeys.IsEmpty) _tableIndex.TryRemove(otherTable, out _);
                            }
                        }
                    }
                }
            }
            Interlocked.Add(ref StatsInvalidations, keys.Count);
        }

        public void InvalidateAll()
        {
            long count = _cache.Count;
            _cache.Clear();
            _tableIndex.Clear();
            _accessOrder.Clear();
            Interlocked.Add(ref StatsInvalidations, count);
        }

        // --- Invalidation ---

        public void ConnectInvalidation(int port)
        {
            if (_invalidationThread != null && _invalidationThread.IsAlive) return;
            _invalidationPort = port;
            _invalidationStop = false;
            _reconnectAttempt = 0;
            _invalidationThread = new Thread(InvalidationLoop)
            {
                IsBackground = true,
                Name = "goldlapel-invalidation"
            };
            _invalidationThread.Start();
        }

        public void StopInvalidation()
        {
            _invalidationStop = true;
            if (_invalidationClient != null)
            {
                try { _invalidationClient.Close(); } catch { }
            }
            if (_invalidationThread != null)
            {
                try { _invalidationThread.Join(5000); } catch { }
                _invalidationThread = null;
            }
            _invalidationConnected = false;
        }

        private void InvalidationLoop()
        {
            while (!_invalidationStop)
            {
                try
                {
                    _invalidationClient = new TcpClient("127.0.0.1", _invalidationPort);
                    _invalidationConnected = true;
                    _reconnectAttempt = 0;

                    var stream = _invalidationClient.GetStream();
                    stream.ReadTimeout = 30000;
                    var reader = new StreamReader(stream);

                    while (!_invalidationStop)
                    {
                        try
                        {
                            var line = reader.ReadLine();
                            if (line == null) break;
                            ProcessSignal(line);
                        }
                        catch (IOException)
                        {
                            break;
                        }
                    }
                }
                catch
                {
                    // Connection failed
                }
                finally
                {
                    if (_invalidationConnected)
                    {
                        _invalidationConnected = false;
                        InvalidateAll();
                    }
                    if (_invalidationClient != null)
                    {
                        try { _invalidationClient.Close(); } catch { }
                        _invalidationClient = null;
                    }
                }

                if (_invalidationStop) break;
                var delay = Math.Min(1 << _reconnectAttempt, 15);
                _reconnectAttempt++;
                Thread.Sleep(delay * 1000);
            }
        }

        internal void ProcessSignal(string line)
        {
            if (line.StartsWith("I:"))
            {
                var table = line.Substring(2).Trim();
                if (table == "*")
                    InvalidateAll();
                else
                    InvalidateTable(table);
            }
        }

        // --- SQL parsing ---

        internal static string MakeKey(string sql, object[] parameters)
        {
            if (parameters == null || parameters.Length == 0)
                return sql + "\0null";
            return sql + "\0" + string.Join(",", parameters.Select(p => p?.ToString() ?? "null"));
        }

        internal static string DetectWrite(string sql)
        {
            var trimmed = sql.Trim();
            var tokens = trimmed.Split(new[] { ' ', '\t', '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries);
            if (tokens.Length == 0) return null;
            var first = tokens[0].ToUpper();

            switch (first)
            {
                case "INSERT":
                    if (tokens.Length < 3 || !tokens[1].Equals("INTO", StringComparison.OrdinalIgnoreCase)) return null;
                    return BareTable(tokens[2]);
                case "UPDATE":
                    if (tokens.Length < 2) return null;
                    return BareTable(tokens[1]);
                case "DELETE":
                    if (tokens.Length < 3 || !tokens[1].Equals("FROM", StringComparison.OrdinalIgnoreCase)) return null;
                    return BareTable(tokens[2]);
                case "TRUNCATE":
                    if (tokens.Length < 2) return null;
                    if (tokens[1].Equals("TABLE", StringComparison.OrdinalIgnoreCase))
                    {
                        if (tokens.Length < 3) return null;
                        return BareTable(tokens[2]);
                    }
                    return BareTable(tokens[1]);
                case "CREATE":
                case "ALTER":
                case "DROP":
                case "REFRESH":
                case "DO":
                case "CALL":
                    return DdlSentinel;
                case "MERGE":
                    if (tokens.Length < 3 || !tokens[1].Equals("INTO", StringComparison.OrdinalIgnoreCase)) return null;
                    return BareTable(tokens[2]);
                case "SELECT":
                    var sawInto = false;
                    string intoTarget = null;
                    for (int i = 1; i < tokens.Length; i++)
                    {
                        var upper = tokens[i].ToUpper();
                        if (upper == "INTO" && !sawInto)
                        {
                            sawInto = true;
                            continue;
                        }
                        if (sawInto && intoTarget == null)
                        {
                            if (upper == "TEMPORARY" || upper == "TEMP" || upper == "UNLOGGED")
                                continue;
                            intoTarget = tokens[i];
                            continue;
                        }
                        if (sawInto && intoTarget != null && upper == "FROM")
                            return DdlSentinel;
                        if (upper == "FROM")
                            return null;
                    }
                    return null;
                case "COPY":
                    if (tokens.Length < 2) return null;
                    var raw = tokens[1];
                    if (raw.StartsWith("(")) return null;
                    var tablePart = raw.Split('(')[0];
                    for (int i = 2; i < tokens.Length; i++)
                    {
                        var upper = tokens[i].ToUpper();
                        if (upper == "FROM") return BareTable(tablePart);
                        if (upper == "TO") return null;
                    }
                    return null;
                case "WITH":
                    var restUpper = trimmed.Substring(tokens[0].Length).ToUpper();
                    foreach (var token in restUpper.Split(new[] { ' ', '\t', '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries))
                    {
                        var word = token.TrimStart('(');
                        if (word == "INSERT" || word == "UPDATE" || word == "DELETE")
                            return DdlSentinel;
                    }
                    return null;
                default:
                    return null;
            }
        }

        internal static string BareTable(string raw)
        {
            var table = raw.Split('(')[0];
            var parts = table.Split('.');
            table = parts[parts.Length - 1];
            return table.ToLower();
        }

        internal static HashSet<string> ExtractTables(string sql)
        {
            var tables = new HashSet<string>();
            var matches = TablePattern.Matches(sql);
            foreach (Match m in matches)
            {
                var table = m.Groups[2].Value.ToLower();
                if (!SqlKeywords.Contains(table))
                    tables.Add(table);
            }
            return tables;
        }

        internal static bool IsTxStart(string sql) { return TxStart.IsMatch(sql); }
        internal static bool IsTxEnd(string sql) { return TxEnd.IsMatch(sql); }

        private void EvictOne()
        {
            string lruKey = null;
            long minCounter = long.MaxValue;
            foreach (var kvp in _accessOrder)
            {
                if (kvp.Value < minCounter)
                {
                    minCounter = kvp.Value;
                    lruKey = kvp.Key;
                }
            }
            if (lruKey == null) return;
            CacheEntry entry;
            _cache.TryRemove(lruKey, out entry);
            long removed;
            _accessOrder.TryRemove(lruKey, out removed);
            if (entry != null)
            {
                foreach (var table in entry.Tables)
                {
                    ConcurrentDictionary<string, byte> keys;
                    if (_tableIndex.TryGetValue(table, out keys))
                    {
                        byte b;
                        keys.TryRemove(lruKey, out b);
                        if (keys.IsEmpty) _tableIndex.TryRemove(table, out _);
                    }
                }
            }
        }

        // For testing: force the connected state
        internal void SetConnected(bool connected)
        {
            _invalidationConnected = connected;
        }
    }
}
