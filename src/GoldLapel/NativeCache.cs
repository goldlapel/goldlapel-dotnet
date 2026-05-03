using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Text.Json;
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

        // --- L1 telemetry tuning ---
        //
        // Demand-driven model (mirrored from goldlapel-python cache.py): the
        // wrapper has NO background timer. Cache counters increment on cache
        // ops (free); state-change events are emitted synchronously when a
        // relevant counter crosses a threshold; snapshot replies are sent
        // only when the proxy asks via ?:<request>.
        //
        // Eviction-rate sliding window. cache_full fires when ≥ EvictRateHigh
        // of the last EvictRateWindow cache writes (puts) caused an eviction;
        // cache_recovered fires when the rate falls back below EvictRateLow.
        // With a 32k-entry default capacity, a steady-state high eviction
        // rate means the working set exceeds the cache — actionable signal
        // for the dashboard.
        internal const int EvictRateWindow = 200;
        internal const double EvictRateHigh = 0.5; // 50% of recent puts evicted → cache_full
        internal const double EvictRateLow = 0.1;  // ≤ 10% → cache_recovered

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
        private readonly object _putLock = new object();
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
        // L1 telemetry: eviction counter — bumped in EvictOne (matches the
        // Python `stats_evictions` field). Read in BuildSnapshot under the
        // put-lock for an internally consistent snapshot.
        internal long StatsEvictions;

        // --- L1 telemetry: identity + opt-out ---
        //
        // Stable wrapper identity for the lifetime of the process. Lets the
        // proxy aggregate per-wrapper across reconnects.
        internal readonly string WrapperId = Guid.NewGuid().ToString();
        internal const string WrapperLang = "dotnet";
        internal readonly string WrapperVersion;
        // Set false via GOLDLAPEL_REPORT_STATS=false to suppress all snapshot
        // replies and state-change emissions. Cache continues to function;
        // only telemetry output is suppressed.
        internal readonly bool ReportStats;

        // --- L1 telemetry: send + state ---
        //
        // The recv loop owns reads; writes can come from the recv thread (R:
        // replies) or any caller thread (S: state events). _sendLock
        // serializes writes so two concurrent sends can't tear each other's
        // bytes on the wire.
        private readonly object _sendLock = new object();
        // Held under _sendLock; null when not connected. Drop here on
        // teardown so emitters don't write to a closed FD.
        private NetworkStream _stream;

        // Eviction-rate sliding window. A bounded ring buffer of length
        // EvictRateWindow; updates are O(1) amortised. Values are 0
        // (insert without eviction) or 1 (eviction occurred). Held under
        // _putLock — the same lock that serializes the put+eviction path.
        private readonly int[] _recentEvictions = new int[EvictRateWindow];
        private int _recentEvictionsCount;     // number of slots filled (caps at EvictRateWindow)
        private int _recentEvictionsIdx;       // next overwrite slot once full
        private long _recentEvictionsSum;      // running sum to avoid O(N) scan in the rate check
        // Latched state — only emit a state-change event when the state
        // transitions. Without latching the wrapper would re-emit every
        // put after the rate stays bad.
        private bool _stateCacheFull;

        // ProcessExit handler (registered once per AppDomain to fire
        // wrapper_disconnected on ungraceful shutdown). Stored so we can
        // unregister on Reset.
        private EventHandler _processExitHandler;
        // Latched: only emit wrapper_disconnected once across Dispose +
        // ProcessExit. Either path is fine; we just must not double-emit.
        private int _disconnectedEmitted;

        // ---- Pluggable send hook for unit tests ----
        // Tests swap this for a list.append-style capture. When non-null,
        // EmitStateChange / EmitResponse route through this instead of the
        // socket — no socket needed for shape tests.
        internal Action<string> SendHookForTests;

        private static NativeCache _instance;
        private static readonly object InstanceLock = new object();

        public NativeCache()
        {
            var sizeStr = Environment.GetEnvironmentVariable("GOLDLAPEL_NATIVE_CACHE_SIZE");
            _maxEntries = !string.IsNullOrEmpty(sizeStr) ? int.Parse(sizeStr) : 32768;
            var enabledStr = Environment.GetEnvironmentVariable("GOLDLAPEL_NATIVE_CACHE");
            _enabled = string.IsNullOrEmpty(enabledStr) || !enabledStr.Equals("false", StringComparison.OrdinalIgnoreCase);
            var reportStr = Environment.GetEnvironmentVariable("GOLDLAPEL_REPORT_STATS");
            ReportStats = string.IsNullOrEmpty(reportStr) || !reportStr.Equals("false", StringComparison.OrdinalIgnoreCase);
            WrapperVersion = ResolveWrapperVersion();

            // Register a ProcessExit hook so a non-graceful shutdown (no
            // explicit Dispose) still emits wrapper_disconnected best-effort.
            // Dispose path emits and latches first; ProcessExit becomes a
            // no-op in that case.
            _processExitHandler = (s, e) => EmitWrapperDisconnected();
            try { AppDomain.CurrentDomain.ProcessExit += _processExitHandler; } catch { /* best effort */ }
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
                    if (_instance._processExitHandler != null)
                    {
                        try { AppDomain.CurrentDomain.ProcessExit -= _instance._processExitHandler; } catch { }
                        _instance._processExitHandler = null;
                    }
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

            // Lock the put+eviction path to prevent two threads from both seeing
            // count < max and both adding, which would exceed _maxEntries.
            int evicted = 0;
            lock (_putLock)
            {
                if (!_cache.ContainsKey(key) && _cache.Count >= _maxEntries)
                {
                    EvictOne();
                    evicted = 1;
                }
                _cache[key] = new CacheEntry(rows, columns, tables);
                _accessOrder[key] = Interlocked.Increment(ref _counter);
                foreach (var table in tables)
                {
                    var keys = _tableIndex.GetOrAdd(table, _ => new ConcurrentDictionary<string, byte>());
                    keys[key] = 0;
                }
                RecordEvictionLocked(evicted);
            }
            // Eviction-rate threshold check happens outside the put-lock —
            // emit may take _sendLock and we don't want to nest locks.
            MaybeEmitEvictionRateStateChange();
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
            // TCP-only: the GL proxy's invalidation endpoint always listens on a TCP port,
            // so Unix domain sockets are not applicable here.
            while (!_invalidationStop)
            {
                try
                {
                    _invalidationClient = new TcpClient("127.0.0.1", _invalidationPort);
                    _invalidationConnected = true;
                    _reconnectAttempt = 0;

                    var stream = _invalidationClient.GetStream();
                    stream.ReadTimeout = 30000;
                    // Stash the stream under _sendLock so EmitStateChange /
                    // EmitResponse (called from any thread) can write to the
                    // live FD. Set BEFORE the wrapper_connected emit so the
                    // very first message goes out cleanly.
                    lock (_sendLock) { _stream = stream; }
                    EmitStateChange("wrapper_connected");

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
                    // Drop the stream reference under _sendLock so any
                    // concurrent emitter doesn't write to a closed FD.
                    lock (_sendLock) { _stream = null; }
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
            // Backwards-compat: unknown prefixes are silently ignored. Older
            // proxies sent only I:/C:/P:; newer proxies may add request types
            // (?:) here. Forward-compat: the wrapper accepts any well-formed
            // prefix and routes by type.
            if (line.StartsWith("I:"))
            {
                var table = line.Substring(2).Trim();
                if (table == "*")
                    InvalidateAll();
                else
                    InvalidateTable(table);
            }
            else if (line.StartsWith("?:"))
            {
                // Snapshot request from the proxy. Reply with R:<json>.
                ProcessRequest(line.Substring(2));
            }
            // C: (config), P: (ping), and anything else — ignored.
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
            Interlocked.Increment(ref StatsEvictions);
        }

        // ---- L1 telemetry: sliding-window bookkeeping ----

        // Caller holds _putLock. Bounded ring; once full, overwrites oldest
        // in O(1). _recentEvictionsSum tracks the running sum so the rate
        // check below doesn't scan the array.
        private void RecordEvictionLocked(int evicted)
        {
            if (_recentEvictionsCount < EvictRateWindow)
            {
                _recentEvictions[_recentEvictionsCount] = evicted;
                _recentEvictionsCount++;
                _recentEvictionsSum += evicted;
            }
            else
            {
                var oldest = _recentEvictions[_recentEvictionsIdx];
                _recentEvictions[_recentEvictionsIdx] = evicted;
                _recentEvictionsSum += (evicted - oldest);
                _recentEvictionsIdx = (_recentEvictionsIdx + 1) % EvictRateWindow;
            }
        }

        // ---- L1 telemetry: snapshot ----

        // Build the L1 snapshot dict the proxy aggregates per-tick. All
        // counters + cache size read in a single critical section so the
        // snapshot is internally consistent (no torn reads where, e.g., hits
        // and misses straddle a concurrent get()). The proxy computes deltas
        // across ticks; we just expose the raw counters.
        internal Dictionary<string, object> BuildSnapshot()
        {
            lock (_putLock)
            {
                return new Dictionary<string, object>
                {
                    { "wrapper_id", WrapperId },
                    { "lang", WrapperLang },
                    { "version", WrapperVersion },
                    { "hits", Interlocked.Read(ref StatsHits) },
                    { "misses", Interlocked.Read(ref StatsMisses) },
                    { "evictions", Interlocked.Read(ref StatsEvictions) },
                    { "invalidations", Interlocked.Read(ref StatsInvalidations) },
                    { "current_size_entries", (long)_cache.Count },
                    { "capacity_entries", (long)_maxEntries },
                };
            }
        }

        // ---- L1 telemetry: emission ----

        private static long NowMs()
        {
            return (long)(DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalMilliseconds;
        }

        private static string SerializeSnapshot(Dictionary<string, object> snap)
        {
            // Hand-build the JSON object preserving the field order the
            // protocol doc specifies. System.Text.Json's
            // JsonSerializer.Serialize on Dictionary<string, object>
            // preserves insertion order in practice; using JsonWriter
            // explicitly avoids any future regression and lets us emit
            // long values as numbers (default object boxing would also
            // do that, but explicit is clearer).
            using var ms = new MemoryStream();
            using (var writer = new Utf8JsonWriter(ms))
            {
                writer.WriteStartObject();
                foreach (var kvp in snap)
                {
                    switch (kvp.Value)
                    {
                        case string s:
                            writer.WriteString(kvp.Key, s);
                            break;
                        case long l:
                            writer.WriteNumber(kvp.Key, l);
                            break;
                        case int i:
                            writer.WriteNumber(kvp.Key, i);
                            break;
                        case bool b:
                            writer.WriteBoolean(kvp.Key, b);
                            break;
                        default:
                            // Fallback — should not hit on the documented
                            // snapshot shape. Stringify rather than crash.
                            writer.WriteString(kvp.Key, kvp.Value?.ToString() ?? "");
                            break;
                    }
                }
                writer.WriteEndObject();
            }
            return Encoding.UTF8.GetString(ms.ToArray());
        }

        // Best-effort line write under _sendLock. Socket errors are
        // swallowed (the recv loop will detect the broken connection on
        // its next iteration and reconnect — don't try to repair from the
        // send path; we'd race the reconnect logic).
        internal void SendLine(string line)
        {
            if (!ReportStats) return;

            // Test hook short-circuit — let unit tests capture emissions
            // without a real socket.
            var hook = SendHookForTests;
            if (hook != null)
            {
                hook(line);
                return;
            }

            var data = line.EndsWith("\n") ? line : line + "\n";
            var bytes = Encoding.UTF8.GetBytes(data);
            lock (_sendLock)
            {
                var s = _stream;
                if (s == null) return;
                try
                {
                    s.Write(bytes, 0, bytes.Length);
                    s.Flush();
                }
                catch (IOException) { }
                catch (ObjectDisposedException) { }
                catch (SocketException) { }
            }
        }

        // Emit S:<json> with snapshot + state name + ts_ms.
        internal void EmitStateChange(string state)
        {
            if (!ReportStats) return;
            var snap = BuildSnapshot();
            snap["state"] = state;
            snap["ts_ms"] = NowMs();
            string json;
            try { json = SerializeSnapshot(snap); }
            catch { return; }
            SendLine("S:" + json);
        }

        // Emit R:<json> snapshot reply to a ?:<request>.
        private void EmitResponse(Dictionary<string, object> snapshot = null)
        {
            if (!ReportStats) return;
            if (snapshot == null) snapshot = BuildSnapshot();
            if (!snapshot.ContainsKey("ts_ms")) snapshot["ts_ms"] = NowMs();
            string json;
            try { json = SerializeSnapshot(snapshot); }
            catch { return; }
            SendLine("R:" + json);
        }

        // Check the eviction-rate sliding window and emit a state change if
        // the latched state should flip. Hysteresis-guarded: crossing HIGH
        // emits cache_full, falling back below LOW emits cache_recovered,
        // and rates between LOW and HIGH leave the latched state unchanged
        // (no flapping).
        private void MaybeEmitEvictionRateStateChange()
        {
            // Read window state + flip latched flag under _putLock so two
            // concurrent puts that both cross the threshold can't both emit.
            // Need at least a full window before reporting state — a single
            // eviction in 3 puts is noise.
            string emit = null;
            lock (_putLock)
            {
                if (_recentEvictionsCount < EvictRateWindow) return;
                var rate = (double)_recentEvictionsSum / _recentEvictionsCount;
                if (!_stateCacheFull && rate >= EvictRateHigh)
                {
                    _stateCacheFull = true;
                    emit = "cache_full";
                }
                else if (_stateCacheFull && rate <= EvictRateLow)
                {
                    _stateCacheFull = false;
                    emit = "cache_recovered";
                }
            }
            // Emit outside the lock — EmitStateChange takes _sendLock and
            // may block on a socket write; we don't want to nest locks or
            // hold _putLock across I/O.
            if (emit != null) EmitStateChange(emit);
        }

        // Handle ?:<request> from the proxy. Today the only request is
        // `snapshot` — the proxy asks for a current counter snapshot and we
        // reply with R:<json>. Future requests can extend this without
        // breaking older proxies (they'd ignore unknown R: lines, but only
        // the proxy that sent ?:<x> will be expecting a reply, so the
        // contract is local to the request type).
        internal void ProcessRequest(string raw)
        {
            // `raw` is the body after the `?:` prefix; today we accept any
            // empty value or `snapshot` literal — the proxy doesn't
            // differentiate request types yet.
            var body = raw == null ? "" : raw.Trim();
            if (body.Length == 0 || body == "snapshot") EmitResponse();
        }

        // Best-effort final wrapper_disconnected on graceful shutdown
        // (Dispose) or process exit. Latched: only the first call emits.
        public void EmitWrapperDisconnected()
        {
            if (Interlocked.Exchange(ref _disconnectedEmitted, 1) != 0) return;
            try { EmitStateChange("wrapper_disconnected"); }
            catch { /* shutdown — best effort */ }
        }

        // For testing: force the connected state
        internal void SetConnected(bool connected)
        {
            _invalidationConnected = connected;
        }

        // Read the wrapper's package version from the assembly's
        // AssemblyInformationalVersionAttribute (CI sets this from the git
        // tag at publish time); fall back to the assembly Version, then
        // "unknown".
        private static string ResolveWrapperVersion()
        {
            try
            {
                var asm = typeof(NativeCache).Assembly;
                var attr = asm.GetCustomAttribute<AssemblyInformationalVersionAttribute>();
                if (attr != null && !string.IsNullOrEmpty(attr.InformationalVersion))
                {
                    var v = attr.InformationalVersion;
                    // Strip "+gitsha" build-metadata suffix; keep the
                    // semver part the package was published as.
                    var plus = v.IndexOf('+');
                    if (plus >= 0) v = v.Substring(0, plus);
                    if (!string.IsNullOrEmpty(v)) return v;
                }
                var ver = asm.GetName().Version;
                if (ver != null && ver.ToString() != "0.0.0.0") return ver.ToString();
            }
            catch { }
            return "unknown";
        }
    }
}
