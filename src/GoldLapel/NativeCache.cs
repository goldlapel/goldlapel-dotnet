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

    // ── Per-connection unsafe-GUC state tracking ────────────────────────
    //
    // Mirrors the proxy's `guc_state.rs` (commit `3e02359`). Custom-GUC-driven
    // RLS (e.g. `SET app.user_id = '42'; SELECT * FROM accounts;` where the
    // policy reads `current_setting('app.user_id')`) is a real cache leak: the
    // wrapper's native cache today keys by SQL+params, so user A's cached rows
    // could be served to user B over the same connection after a SET.
    //
    // We fingerprint the subset of GUC values that can change query results
    // and fold the fingerprint into the cache key. A GUC is **unsafe** if it
    // is in a short hardcoded list (search_path, role, isolation, etc.) OR
    // contains a `.` (namespaced — `app.*`, `myapp.*`).
    //
    // SET LOCAL is intentionally ignored: cached entries only originate from
    // outside-of-transaction reads (CachedConnection.InTransaction gates
    // ExecuteDbDataReader caching), and SET LOCAL effects are scoped to the
    // current transaction — they never influence a cacheable response.

    /// <summary>
    /// Parsed <c>SET</c> / <c>RESET</c> command extracted from a SQL
    /// statement. Used by <see cref="NativeCache.ParseSetCommand"/> and the
    /// per-connection <see cref="ConnectionGucState"/> tracker.
    /// </summary>
    public class SetCommand
    {
        public enum CommandKind { Set, SetLocal, Reset, ResetAll }

        public CommandKind Kind { get; }
        /// <summary>Lowercased GUC name. Null for <c>RESET ALL</c>.</summary>
        public string Name { get; }
        /// <summary>Raw value string with surrounding quotes stripped. Null for RESET / RESET ALL.</summary>
        public string Value { get; }

        private SetCommand(CommandKind kind, string name, string value)
        {
            Kind = kind;
            Name = name;
            Value = value;
        }

        internal static SetCommand Set(string name, string value) =>
            new SetCommand(CommandKind.Set, name, value);
        internal static SetCommand Local(string name, string value) =>
            new SetCommand(CommandKind.SetLocal, name, value);
        internal static SetCommand Reset(string name) =>
            new SetCommand(CommandKind.Reset, name, null);
        internal static SetCommand ResetAll() =>
            new SetCommand(CommandKind.ResetAll, null, null);
    }

    /// <summary>
    /// Per-connection unsafe-GUC state. Each <see cref="CachedConnection"/>
    /// owns one. The cached <see cref="StateHash"/> is folded into the
    /// native cache key so two connections with different unsafe-GUC values
    /// (different <c>app.user_id</c>, different <c>role</c>, etc.) never
    /// share a cache slot.
    /// </summary>
    /// <remarks>
    /// .NET concurrency: a single <c>DbCommand</c> is not generally safe
    /// for concurrent use, so per-connection state is effectively
    /// single-writer. A plain <see cref="Dictionary{TKey, TValue}"/> guarded
    /// by a private lock is the right-sized primitive — <see cref="ConcurrentDictionary{TKey, TValue}"/>
    /// would be overkill and the recompute step needs an atomic
    /// snapshot of the map anyway. <see cref="StateHash"/> is published
    /// via <see cref="Interlocked.Exchange(ref long, long)"/> so a reader on
    /// another thread (e.g. the recv loop snapshotting state) sees a
    /// torn-free 64-bit value on 32-bit hosts.
    /// </remarks>
    public class ConnectionGucState
    {
        // BTreeMap-equivalent: ordered iteration so the hash is invariant
        // under insertion order. SortedDictionary keys ordered by ordinal
        // string compare (matches the Rust BTreeMap<String, String> default).
        private readonly SortedDictionary<string, string> _values =
            new SortedDictionary<string, string>(StringComparer.Ordinal);
        private readonly object _lock = new object();
        // Cached state hash. 0 for empty (baseline) state — matches the
        // proxy's `0` for fresh connections, so a fresh wrapper connection
        // hits the same cache slot as another connection with no unsafe
        // GUCs set. Read via Interlocked.Read for torn-free access on
        // 32-bit hosts; written via Interlocked.Exchange.
        private long _stateHash;

        /// <summary>
        /// Current unsafe-GUC state hash. <c>0</c> for the empty baseline
        /// (fresh connection or after <c>RESET ALL</c> on an empty state).
        /// </summary>
        public long StateHash => Interlocked.Read(ref _stateHash);

        /// <summary>
        /// Apply a parsed <see cref="SetCommand"/>. No-op for
        /// <see cref="SetCommand.CommandKind.SetLocal"/> (transient — cache
        /// participation is gated on transaction-idle anyway), no-op for
        /// safe GUC names.
        /// </summary>
        public void Apply(SetCommand cmd)
        {
            if (cmd == null) return;
            bool changed = false;
            lock (_lock)
            {
                switch (cmd.Kind)
                {
                    case SetCommand.CommandKind.Set:
                        if (NativeCache.IsUnsafeGuc(cmd.Name))
                        {
                            _values[cmd.Name] = cmd.Value;
                            changed = true;
                        }
                        break;
                    case SetCommand.CommandKind.SetLocal:
                        // Intentionally ignored — see class remarks.
                        break;
                    case SetCommand.CommandKind.Reset:
                        if (NativeCache.IsUnsafeGuc(cmd.Name) && _values.Remove(cmd.Name))
                        {
                            changed = true;
                        }
                        break;
                    case SetCommand.CommandKind.ResetAll:
                        if (_values.Count > 0)
                        {
                            _values.Clear();
                            changed = true;
                        }
                        break;
                }
                if (changed) RecomputeHashLocked();
            }
        }

        /// <summary>
        /// Convenience: parse a SQL string and apply every recognised
        /// <c>SET</c> / <c>RESET</c> it contains. Multi-statement bodies
        /// are split on top-level <c>;</c> (string literals respected).
        /// Returns <c>true</c> if the hash changed.
        /// </summary>
        public bool ObserveSql(string sql)
        {
            if (string.IsNullOrEmpty(sql)) return false;
            var before = StateHash;

            // Fast path for the common single-statement case — avoid
            // allocating the segments list for every SQL that isn't a
            // multi-statement body. Strip trailing `;`s (any number) and
            // scan the remaining prefix for an inner top-level `;` not
            // inside a string literal.
            var trimmed = sql.TrimEnd();
            while (trimmed.EndsWith(";", StringComparison.Ordinal))
                trimmed = trimmed.Substring(0, trimmed.Length - 1).TrimEnd();
            bool hasInnerSemicolon = false;
            char? quote = null;
            for (int i = 0; i < trimmed.Length; i++)
            {
                var c = trimmed[i];
                if (quote.HasValue)
                {
                    if (c == quote.Value)
                    {
                        // PG's `''` doubled-quote escape (and SQL `""`).
                        if (i + 1 < trimmed.Length && trimmed[i + 1] == quote.Value)
                        { i++; continue; }
                        quote = null;
                    }
                }
                else
                {
                    if (c == '\'' || c == '"') quote = c;
                    else if (c == ';') { hasInnerSemicolon = true; break; }
                }
            }

            if (!hasInnerSemicolon)
            {
                var cmd = NativeCache.ParseSetCommand(sql);
                if (cmd != null) Apply(cmd);
            }
            else
            {
                foreach (var stmt in NativeCache.SplitStatements(sql))
                {
                    var cmd = NativeCache.ParseSetCommand(stmt);
                    if (cmd != null) Apply(cmd);
                }
            }
            return StateHash != before;
        }

        // Caller holds _lock. Recomputes the deterministic FNV-1a-style
        // hash of the (ordered) name=value pairs. Empty state hashes to 0
        // — matches the proxy's BTreeMap-empty -> 0 invariant.
        private void RecomputeHashLocked()
        {
            if (_values.Count == 0)
            {
                Interlocked.Exchange(ref _stateHash, 0);
                return;
            }
            // FNV-1a 64-bit. Stable across runs (no DefaultHasher
            // randomization), order-invariant via SortedDictionary
            // iteration, fast enough for the hot path.
            const ulong FnvOffset = 14695981039346656037UL;
            const ulong FnvPrime = 1099511628211UL;
            ulong h = FnvOffset;
            foreach (var kvp in _values)
            {
                foreach (var b in Encoding.UTF8.GetBytes(kvp.Key))
                {
                    h ^= b;
                    h *= FnvPrime;
                }
                // 0x00 separator between key and value to avoid the
                // ("ab","c") vs ("a","bc") collision class.
                h ^= 0;
                h *= FnvPrime;
                foreach (var b in Encoding.UTF8.GetBytes(kvp.Value))
                {
                    h ^= b;
                    h *= FnvPrime;
                }
                // 0x01 separator between pairs.
                h ^= 1;
                h *= FnvPrime;
            }
            // Avoid hashing to 0 by accident — 0 is reserved for "empty".
            // Probability is ~1/2^64, but we'd rather be deterministic.
            if (h == 0) h = 1;
            Interlocked.Exchange(ref _stateHash, unchecked((long)h));
        }
    }

    public class NativeCache
    {
        internal const string DdlSentinel = "__ddl__";

        // --- Native-cache telemetry tuning ---
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
        // Explicit native-cache disable — orthogonal to _enabled (the
        // GOLDLAPEL_NATIVE_CACHE env-var kill-switch) and orthogonal to
        // _maxEntries (the cache size). When set, Get always returns null
        // (incrementing misses) and Put is a silent no-op. The
        // invalidation thread continues running so telemetry signal flow
        // (wrapper_connected / snapshot replies) keeps working — Manor
        // and the dashboard need to see the wrapper even when the native
        // cache is off. Set via the DisableNativeCache option on
        // GoldLapelOptions; pushed onto the singleton in SpawnAsync
        // before the invalidation socket connects so the very first
        // wrapper_connected snapshot carries the correct `disabled`
        // field. volatile so writes from the SpawnAsync thread are
        // visible to the recv loop without a lock.
        private volatile bool _disableNativeCache;

        private volatile bool _invalidationConnected;
        private volatile bool _invalidationStop;
        private Thread _invalidationThread;
        private TcpClient _invalidationClient;
        private int _invalidationPort;
        private int _reconnectAttempt;

        internal long StatsHits;
        internal long StatsMisses;
        internal long StatsInvalidations;
        // Native-cache telemetry: eviction counter — bumped in EvictOne
        // (matches the Python `stats_evictions` field). Read in
        // BuildSnapshot under the put-lock for an internally consistent
        // snapshot.
        internal long StatsEvictions;

        // --- Native-cache telemetry: identity + opt-out ---
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

        // --- Native-cache telemetry: send + state ---
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

        /// <summary>
        /// When <c>true</c>, <see cref="Get"/> always returns null (miss)
        /// and <see cref="Put"/> is a silent no-op. The invalidation
        /// thread keeps running and telemetry emissions still fire — only
        /// the local hit path is suppressed. Surfaced via the
        /// <c>disabled</c> field on the native-cache telemetry snapshot
        /// when set. Set via the <c>DisableNativeCache</c> option on
        /// <see cref="GoldLapelOptions"/>.
        /// </summary>
        public bool DisableNativeCache
        {
            get => _disableNativeCache;
            set => _disableNativeCache = value;
        }

        // --- Cache operations ---

        public CacheEntry Get(string sql, object[] parameters)
        {
            return Get(sql, parameters, 0);
        }

        // State-hash-aware cache lookup. The per-connection
        // <see cref="ConnectionGucState.StateHash"/> is folded into the
        // key so two connections with different unsafe-GUC values never
        // share a cache slot — closes the GUC-driven RLS leak that the
        // proxy commit `3e02359` fixed at the proxy-cache layer.
        public CacheEntry Get(string sql, object[] parameters, long stateHash)
        {
            if (!_enabled || !_invalidationConnected) return null;
            // DisableNativeCache: tick misses (callers measure miss rate),
            // never hit. Skip the key build + cache lookup entirely — no
            // point.
            if (_disableNativeCache)
            {
                Interlocked.Increment(ref StatsMisses);
                return null;
            }
            var key = MakeKey(sql, parameters, stateHash);
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
            Put(sql, parameters, rows, columns, 0);
        }

        // State-hash-aware cache insert. See <see cref="Get(string, object[], long)"/>.
        public void Put(string sql, object[] parameters, object[][] rows, string[] columns, long stateHash)
        {
            if (!_enabled || !_invalidationConnected) return;
            // DisableNativeCache: silent no-op. Don't touch cache state,
            // the eviction-rate window, or counters — the layer is off.
            if (_disableNativeCache) return;
            var key = MakeKey(sql, parameters, stateHash);
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
            return MakeKey(sql, parameters, 0);
        }

        // Cache key shape including the per-connection unsafe-GUC state
        // hash. Format: `<sql>\0<state_hash_hex>\0<params>`. State hash 0
        // is the empty/baseline — fresh connections still hit cache slots
        // populated by other state-0 connections.
        internal static string MakeKey(string sql, object[] parameters, long stateHash)
        {
            var paramsPart = (parameters == null || parameters.Length == 0)
                ? "null"
                : string.Join(",", parameters.Select(p => p?.ToString() ?? "null"));
            // Format the state hash as lowercase hex for parity with the
            // proxy's `{:x}` rendering — same human-readable form across
            // proxy and wrapper (eases log correlation).
            var sh = ((ulong)stateHash).ToString("x", System.Globalization.CultureInfo.InvariantCulture);
            return sql + "\0" + sh + "\0" + paramsPart;
        }

        /// <summary>
        /// Replace the contents of <c>'...'</c> and <c>"..."</c> string
        /// literals with spaces, preserving overall length so positions
        /// line up with the original. PG's doubled-quote <c>''</c> /
        /// <c>""</c> escapes are handled the same way as in
        /// <see cref="SplitStatements"/>. Used by
        /// <see cref="DetectWrite"/>'s SELECT branch so that bare words
        /// like <c>INTO</c> inside a literal (e.g.
        /// <c>SELECT 'INSERT INTO orders' FROM audit_log</c>) don't trip
        /// the SELECT-INTO DDL classifier. Mirrors goldlapel-js commit
        /// <c>63753fe</c>.
        /// </summary>
        internal static string StripStringLiterals(string sql)
        {
            if (string.IsNullOrEmpty(sql)) return sql;
            var buf = sql.ToCharArray();
            char? quote = null;
            int i = 0;
            while (i < sql.Length)
            {
                var c = sql[i];
                if (quote.HasValue)
                {
                    if (c == quote.Value)
                    {
                        if (i + 1 < sql.Length && sql[i + 1] == quote.Value)
                        {
                            // Doubled-quote escape: blank both, stay inside literal.
                            buf[i] = ' ';
                            buf[i + 1] = ' ';
                            i += 2;
                            continue;
                        }
                        // Closing quote: leave the delimiter, drop the literal body.
                        quote = null;
                    }
                    else
                    {
                        buf[i] = ' ';
                    }
                }
                else
                {
                    if (c == '\'' || c == '"') quote = c;
                }
                i++;
            }
            return new string(buf);
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
                    // Re-tokenize from a literal-stripped form so that bare
                    // words like `INTO` or `FROM` inside `'...'` / `"..."`
                    // don't trigger the SELECT-INTO DDL classifier (e.g.
                    // `SELECT 'INSERT INTO orders' FROM audit_log`,
                    // `SELECT * FROM "into_table"`). Other branches above
                    // use fixed-position token checks (tokens[1], tokens[2])
                    // and aren't affected — only SELECT scans the full token
                    // stream looking for INTO. Mirrors goldlapel-js commit
                    // `63753fe`.
                    var scanTokens = StripStringLiterals(trimmed)
                        .Split(new[] { ' ', '\t', '\n', '\r' }, StringSplitOptions.RemoveEmptyEntries);
                    var sawInto = false;
                    string intoTarget = null;
                    for (int i = 1; i < scanTokens.Length; i++)
                    {
                        var upper = scanTokens[i].ToUpper();
                        if (upper == "INTO" && !sawInto)
                        {
                            sawInto = true;
                            continue;
                        }
                        if (sawInto && intoTarget == null)
                        {
                            if (upper == "TEMPORARY" || upper == "TEMP" || upper == "UNLOGGED")
                                continue;
                            intoTarget = scanTokens[i];
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

        // Tokens that flip the wrapper's per-connection InTransaction
        // flag. Case-insensitive match against the first non-whitespace
        // token of each segment. SAVEPOINT and RELEASE are intentionally
        // omitted — they are intra-transaction markers, not boundaries:
        //   - SAVEPOINT errors outside a tx (Postgres requires an open
        //     transaction). The flag is already true; flipping to true
        //     is a no-op.
        //   - RELEASE SAVEPOINT does NOT end the outer transaction.
        //     Flipping to false here would desync wrapper state from
        //     server state — wrapper out-of-tx while server in-tx —
        //     causing stale cache reads inside the still-open tx.
        // Mirrors the JS (0d19816) and Ruby (77d4313) classifiers.
        private static readonly HashSet<string> TxStartTokens =
            new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            { "BEGIN", "START" };
        private static readonly HashSet<string> TxEndTokens =
            new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            { "COMMIT", "ROLLBACK", "END" };

        /// <summary>
        /// Walk every statement segment in <paramref name="sql"/> and
        /// return the final wrapper-side <c>InTransaction</c> state change.
        /// Returns <c>true</c> if the last tx-relevant segment is a tx
        /// start (<c>BEGIN</c> / <c>START TRANSACTION</c>), <c>false</c>
        /// if it's a tx end (<c>COMMIT</c> / <c>ROLLBACK</c> / <c>END</c>),
        /// or <c>null</c> if no segment matches (no flag change).
        /// Last-segment-wins so a multi-statement body like
        /// <c>BEGIN; INSERT...; COMMIT</c> correctly settles to
        /// out-of-transaction at the end — the pre-fix single-token check
        /// only saw <c>BEGIN</c> and left the wrapper stuck in
        /// <c>InTransaction = true</c> forever, bypassing cache reads
        /// permanently. <c>SAVEPOINT</c> and <c>RELEASE SAVEPOINT</c> are
        /// intra-transaction markers and produce no flag change — see
        /// <see cref="TxStartTokens"/> for rationale.
        /// </summary>
        internal static bool? DetectTxTransition(string sql)
        {
            if (string.IsNullOrEmpty(sql)) return null;

            // Single-statement fast path: avoid SplitStatements when there's
            // no `;` at all. The splitter handles quoted `;` correctly but
            // the overwhelming majority of SQL is single-statement.
            var hasSemi = sql.IndexOf(';') >= 0;
            var segments = hasSemi ? SplitStatements(sql) : new List<string> { sql };

            bool? final = null;
            foreach (var seg in segments)
            {
                var first = FirstToken(seg);
                if (first == null) continue;
                if (TxStartTokens.Contains(first)) final = true;
                else if (TxEndTokens.Contains(first)) final = false;
            }
            return final;
        }

        // Return the first whitespace-delimited token of <paramref name="sql"/>
        // (uppercase-preserved — caller does case-insensitive compare),
        // or <c>null</c> for empty/whitespace input. Used by
        // DetectTxTransition to inspect the head of each segment without
        // allocating a full token array.
        private static string FirstToken(string sql)
        {
            if (string.IsNullOrEmpty(sql)) return null;
            int i = 0;
            while (i < sql.Length && char.IsWhiteSpace(sql[i])) i++;
            int start = i;
            while (i < sql.Length && !char.IsWhiteSpace(sql[i]) && sql[i] != ';') i++;
            return i > start ? sql.Substring(start, i - start) : null;
        }

        // Session-state commands whose responses are not cacheable. SET /
        // RESET / LISTEN / UNLISTEN / NOTIFY return empty rowsets but
        // would otherwise satisfy the "rows + columns are non-null" gate
        // in CacheAndReturn — caching them bloats the cache with no-row
        // entries that never serve real data and costs needless eviction
        // pressure on chatty sessions. BEGIN/COMMIT/ROLLBACK/SAVEPOINT
        // are also listed: after the multi-segment tx-flag fix removed
        // the early-return short-circuit, bare BEGIN/COMMIT/ROLLBACK now
        // flow through to CacheAndReturn, and this filter is what keeps
        // their empty rowsets out of the cache. Mirrors the cross-wrapper
        // fix (docs/todos/wrapper-cache-set-responses.md).
        private static readonly HashSet<string> SessionStateCommands =
            new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            {
                "SET", "RESET", "LISTEN", "UNLISTEN", "NOTIFY",
                "BEGIN", "START", "COMMIT", "ROLLBACK", "END",
                "SAVEPOINT", "RELEASE",
            };

        /// <summary>
        /// Return true when the SQL's first token is a session-state
        /// command (SET / RESET / LISTEN / UNLISTEN / NOTIFY / BEGIN /
        /// START / COMMIT / ROLLBACK / END / SAVEPOINT / RELEASE). Used
        /// to skip cache-put on commands whose responses are empty
        /// rowsets and have no value as cached entries.
        /// </summary>
        internal static bool IsSessionStateCommand(string sql)
        {
            if (string.IsNullOrEmpty(sql)) return false;
            var trimmed = sql.TrimStart();
            if (trimmed.Length == 0) return false;
            int end = 0;
            while (end < trimmed.Length && !char.IsWhiteSpace(trimmed[end])
                   && trimmed[end] != ';') end++;
            if (end == 0) return false;
            return SessionStateCommands.Contains(trimmed.Substring(0, end));
        }

        /// <summary>
        /// Run <see cref="DetectWrite"/> over each statement in a
        /// possibly-multi-statement SQL body and return the union of
        /// affected tables. Returns a set containing
        /// <see cref="DdlSentinel"/> as soon as any segment is DDL, so
        /// callers can short-circuit to <c>InvalidateAll()</c>. Returns
        /// an empty set for read-only or non-write SQL. Mirrors the
        /// cross-wrapper fix
        /// (docs/todos/wrapper-multistatement-write-detection.md).
        /// </summary>
        internal static HashSet<string> DetectWritesMulti(string sql)
        {
            var result = new HashSet<string>();
            if (string.IsNullOrEmpty(sql)) return result;

            // Single-statement fast path: avoid the splitter overhead when
            // there's no `;` at all. The splitter handles quoted `;`
            // correctly, but the overwhelming majority of queries are
            // single-statement and we want to keep the hot path tight.
            var hasSemi = sql.IndexOf(';') >= 0;
            var segments = hasSemi ? SplitStatements(sql) : new List<string> { sql };

            foreach (var seg in segments)
            {
                var t = DetectWrite(seg);
                if (t == null) continue;
                if (t == DdlSentinel)
                {
                    result.Clear();
                    result.Add(DdlSentinel);
                    return result;
                }
                result.Add(t);
            }
            return result;
        }

        // ── Unsafe-GUC classification + SET parsing ───────────────
        //
        // Mirrors the proxy's guc_state.rs. See ConnectionGucState class
        // docs for the design rationale.

        // GUC names whose value can change query results without
        // changing the SQL text. Matched case-insensitively. Any GUC
        // with a `.` in the name is also treated as unsafe (namespaced
        // GUCs are the canonical custom-RLS pattern).
        private static readonly HashSet<string> UnsafeGucShortList =
            new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            {
                "search_path",
                "role",
                "session_authorization",
                "default_transaction_isolation",
                "default_transaction_read_only",
                "transaction_isolation",
                "row_security",
            };

        /// <summary>
        /// Classify a GUC name as state-affecting (<c>true</c>) or
        /// harmless (<c>false</c>). A GUC is unsafe if it's in the short
        /// hardcoded list OR contains a <c>.</c> (namespaced —
        /// <c>app.*</c>, <c>myapp.*</c>, etc.). Comparison is
        /// case-insensitive.
        /// </summary>
        public static bool IsUnsafeGuc(string name)
        {
            if (string.IsNullOrEmpty(name)) return false;
            if (name.IndexOf('.') >= 0) return true;
            return UnsafeGucShortList.Contains(name);
        }

        /// <summary>
        /// Split a SQL string on top-level <c>;</c> characters,
        /// respecting single- and double-quoted string literals. PG's
        /// doubled-quote escape (<c>''</c> / <c>""</c>) is honored.
        /// Empty segments are dropped, surrounding whitespace trimmed.
        /// </summary>
        /// <remarks>
        /// Lightest-possible statement splitter: does not understand
        /// dollar-quoted strings, comments, or any other lexical
        /// nuance. Good enough for the only thing it needs to do —
        /// splitting <c>SET foo = 'a'; SELECT 1</c>-style multi-statement
        /// bodies. Mirrors the proxy's <c>split_statements</c>.
        /// </remarks>
        public static List<string> SplitStatements(string sql)
        {
            var result = new List<string>();
            if (string.IsNullOrEmpty(sql)) return result;

            int start = 0;
            char? quote = null;
            int i = 0;
            while (i < sql.Length)
            {
                var c = sql[i];
                if (quote.HasValue)
                {
                    if (c == quote.Value)
                    {
                        // PG's `''` doubled-quote escape (and SQL `""`).
                        if (i + 1 < sql.Length && sql[i + 1] == quote.Value)
                        {
                            i += 2;
                            continue;
                        }
                        quote = null;
                    }
                }
                else
                {
                    if (c == '\'' || c == '"')
                    {
                        quote = c;
                    }
                    else if (c == ';')
                    {
                        var segment = sql.Substring(start, i - start).Trim();
                        if (segment.Length > 0) result.Add(segment);
                        start = i + 1;
                    }
                }
                i++;
            }
            var tail = sql.Substring(start).Trim();
            if (tail.Length > 0) result.Add(tail);
            return result;
        }

        /// <summary>
        /// Parse a <c>SET</c> / <c>RESET</c> command out of a single SQL
        /// statement. Recognises <c>SET name = value</c>, <c>SET name TO
        /// value</c>, <c>SET SESSION ...</c>, <c>SET LOCAL ...</c>,
        /// <c>RESET name</c>, <c>RESET ALL</c>. Returns <c>null</c> for
        /// anything else (including the legacy <c>SET TIME ZONE 'UTC'</c>
        /// two-word form — timezone is harmless, treating it as
        /// "not-a-trackable-SET" is correct for cache safety).
        /// </summary>
        public static SetCommand ParseSetCommand(string sql)
        {
            if (string.IsNullOrEmpty(sql)) return null;
            var s = sql.Trim();
            if (s.EndsWith(";", StringComparison.Ordinal))
                s = s.Substring(0, s.Length - 1).TrimEnd();
            if (s.Length == 0) return null;

            // Split on whitespace into a token stream.
            var tokens = s.Split(new[] { ' ', '\t', '\n', '\r' },
                StringSplitOptions.RemoveEmptyEntries);
            if (tokens.Length == 0) return null;

            var head = tokens[0];

            // ── RESET ───────────────────────────────────────────────
            if (head.Equals("RESET", StringComparison.OrdinalIgnoreCase))
            {
                if (tokens.Length < 2) return null;
                var target = tokens[1];
                // `RESET name` — anything after `name` is junk we don't expect.
                if (tokens.Length > 2) return null;
                if (target.Equals("ALL", StringComparison.OrdinalIgnoreCase))
                    return SetCommand.ResetAll();
                var name = NormalizeGucName(target);
                if (name == null) return null;
                return SetCommand.Reset(name);
            }

            // ── SET ─────────────────────────────────────────────────
            if (!head.Equals("SET", StringComparison.OrdinalIgnoreCase))
                return null;
            if (tokens.Length < 2) return null;

            int idx = 1;
            bool isLocal = false;
            var modifier = tokens[idx];
            if (modifier.Equals("LOCAL", StringComparison.OrdinalIgnoreCase))
            {
                isLocal = true;
                idx++;
            }
            else if (modifier.Equals("SESSION", StringComparison.OrdinalIgnoreCase))
            {
                idx++;
            }
            if (idx >= tokens.Length) return null;

            var nameToken = tokens[idx];
            idx++;

            // The token may have an `=` glued onto it (e.g. `SET app.user='42'`).
            string gluedValue = null;
            var eqPos = nameToken.IndexOf('=');
            if (eqPos >= 0)
            {
                var n = nameToken.Substring(0, eqPos);
                var rest = nameToken.Substring(eqPos + 1);
                nameToken = n;
                if (rest.Length > 0) gluedValue = rest;
            }

            var gucName = NormalizeGucName(nameToken);
            if (gucName == null) return null;

            string valueStr;
            if (gluedValue != null)
            {
                if (idx < tokens.Length)
                {
                    var rest = string.Join(" ", tokens, idx, tokens.Length - idx);
                    valueStr = (rest.Length > 0) ? gluedValue + " " + rest : gluedValue;
                }
                else
                {
                    valueStr = gluedValue;
                }
            }
            else
            {
                if (idx >= tokens.Length) return null;
                var sep = tokens[idx];
                idx++;
                if (!(sep == "=" || sep.Equals("TO", StringComparison.OrdinalIgnoreCase)))
                    return null;
                if (idx >= tokens.Length) return null;
                valueStr = string.Join(" ", tokens, idx, tokens.Length - idx);
            }

            var value = StripValueQuotes(valueStr.Trim());
            if (value.Length == 0 && valueStr.Trim().Length == 0)
                return null;

            return isLocal ? SetCommand.Local(gucName, value) : SetCommand.Set(gucName, value);
        }

        // Lowercase the GUC name and strip surrounding double quotes
        // (PG treats `"app.user_id"` and `app.user_id` as the same
        // identifier when it's a configuration parameter).
        private static string NormalizeGucName(string token)
        {
            if (string.IsNullOrEmpty(token)) return null;
            var trimmed = token.Trim('"');
            if (trimmed.Length == 0) return null;
            return trimmed.ToLowerInvariant();
        }

        // Strip a single layer of matching surrounding quotes (`'...'`
        // or `"..."`) from a value. Multi-token quoted values arrive as
        // the joined string; this just peels the outer quotes.
        private static string StripValueQuotes(string value)
        {
            var v = value.Trim();
            if (v.Length >= 2)
            {
                var first = v[0];
                var last = v[v.Length - 1];
                if ((first == '\'' && last == '\'') || (first == '"' && last == '"'))
                    return v.Substring(1, v.Length - 2);
            }
            return v;
        }

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

        // ---- Native-cache telemetry: sliding-window bookkeeping ----

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

        // ---- Native-cache telemetry: snapshot ----

        // Build the native-cache snapshot dict the proxy aggregates
        // per-tick. All counters + cache size read in a single critical
        // section so the snapshot is internally consistent (no torn reads
        // where, e.g., hits and misses straddle a concurrent get()). The
        // proxy computes deltas across ticks; we just expose the raw
        // counters.
        internal Dictionary<string, object> BuildSnapshot()
        {
            lock (_putLock)
            {
                var snap = new Dictionary<string, object>
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
                // Forward-compat: surface the disable flag so HQ/Manor can
                // render the wrapper's native-cache state correctly. Only
                // emitted when set; older consumers that don't know the
                // field will simply ignore it.
                if (_disableNativeCache) snap["disabled"] = true;
                return snap;
            }
        }

        // ---- Native-cache telemetry: emission ----

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
