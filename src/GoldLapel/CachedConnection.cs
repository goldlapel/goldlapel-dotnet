using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace GoldLapel
{
    public class CachedConnection : DbConnection
    {
        private readonly DbConnection _inner;
        private readonly NativeCache _cache;
        // Per-connection unsafe-GUC state. Mirrors the proxy's
        // per-connection ConnectionGucState (commit `3e02359`). Each
        // wrapper-side cache key folds in this connection's state hash so
        // two connections with different `app.user_id` (or any namespaced
        // GUC, role, search_path, etc.) never share a cache slot.
        private readonly ConnectionGucState _gucState = new ConnectionGucState();
        private bool _inTransaction;

        // ── Pool / verify integration ────────────────────────────────
        //
        // Npgsql's connection pool does its own DISCARD ALL on close
        // (`No Reset On Close=false`, the default). That happens INSIDE
        // Npgsql, not via a command sent through CachedCommand — so our
        // wire-observation parser never sees it. We have to compensate
        // explicitly.
        //
        // For non-Npgsql DbConnections we don't know the pool's reset
        // policy. Conservative fallback: mark state dirty on Open() and
        // run a server-side verify on the first command, before the
        // cache lookup, to reconstruct authoritative GUC state from
        // pg_settings.
        //
        // _npgsqlAutoReset is detected once at construction:
        //   true  → on Open() we know the server reset state,
        //           ApplyVerifiedState(empty) clears our map cheaply
        //   false → on Open() we MarkDirty for verify-on-checkout
        // If detection couldn't determine the value (non-Npgsql, or
        // the connection string was malformed), we treat it as `false`.
        private readonly bool _npgsqlAutoReset;

        // Cancellation source for in-flight async verifies. Cancelled
        // on Dispose / Close so a verify that fires off as the user is
        // tearing down doesn't keep a connection alive past its scope.
        // Replaced on Open() so a reused CachedConnection (Close →
        // Open round-trip) doesn't refuse new schedule calls because
        // the CTS is permanently cancelled.
        private CancellationTokenSource _verifyCts = new CancellationTokenSource();
        private readonly object _verifyCtsLock = new object();

        // Single-slot semaphore guarding the inner DbConnection during
        // async verify. Prevents the verify path from racing a user-
        // initiated command on the same connection (DbCommand isn't
        // thread-safe). If a verify is already running and we observe
        // another function call, we just MarkDirty and let
        // verify-on-checkout pick it up — the inner connection isn't
        // deadlocked, only deferred.
        private readonly SemaphoreSlim _verifyGate = new SemaphoreSlim(1, 1);

        public CachedConnection(DbConnection inner, NativeCache cache)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            _cache = cache ?? throw new ArgumentNullException(nameof(cache));
            _npgsqlAutoReset = DetectNpgsqlAutoReset(inner);
        }

        internal DbConnection Inner => _inner;
        internal NativeCache Cache => _cache;
        internal ConnectionGucState GucState => _gucState;
        internal bool InTransaction
        {
            get => _inTransaction;
            set => _inTransaction = value;
        }
        internal bool NpgsqlAutoReset => _npgsqlAutoReset;
        internal CancellationToken VerifyCancellationToken
        {
            get
            {
                lock (_verifyCtsLock) { return _verifyCts.Token; }
            }
        }

        public override string ConnectionString
        {
            get => _inner.ConnectionString;
            set => _inner.ConnectionString = value;
        }

        public override string Database => _inner.Database;
        public override string DataSource => _inner.DataSource;
        public override string ServerVersion => _inner.ServerVersion;
        public override ConnectionState State => _inner.State;

        public override void ChangeDatabase(string databaseName) => _inner.ChangeDatabase(databaseName);

        public override void Open()
        {
            _inner.Open();
            // If the CTS was cancelled by a previous Close() (or never
            // initialised after Dispose — that's a programming error
            // and will throw on use anyway), spin up a fresh one so
            // post-call verifies can schedule again.
            lock (_verifyCtsLock)
            {
                if (_verifyCts.IsCancellationRequested)
                {
                    try { _verifyCts.Dispose(); } catch { }
                    _verifyCts = new CancellationTokenSource();
                }
            }
            // Pool checkout. Two cases:
            //   - Npgsql with `No Reset On Close=false` (the default):
            //     Npgsql sent DISCARD ALL on the previous close, so
            //     server-side state is clean. Apply an empty verified
            //     state so our hash matches.
            //   - Anything else (Npgsql w/ NoResetOnClose=true, other
            //     drivers, or detection failed): the server may still
            //     hold state from a previous session. Mark dirty for
            //     verify-on-checkout on the first command.
            if (_npgsqlAutoReset)
                _gucState.ApplyVerifiedState(null);
            else
                _gucState.MarkDirty();
        }

        public override void Close()
        {
            // Cancel any in-flight verify before the inner closes —
            // verify holds an inner DbCommand that becomes unusable.
            lock (_verifyCtsLock)
            {
                try { _verifyCts.Cancel(); } catch { }
            }
            _inner.Close();
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            _inTransaction = true;
            return new CachedTransaction(_inner.BeginTransaction(isolationLevel), this);
        }

        protected override DbCommand CreateDbCommand()
        {
            return new CachedCommand(_inner.CreateCommand(), this);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                lock (_verifyCtsLock)
                {
                    try { _verifyCts.Cancel(); } catch { }
                    try { _verifyCts.Dispose(); } catch { }
                }
                try { _verifyGate.Dispose(); } catch { }
                _inner.Dispose();
            }
            base.Dispose(disposing);
        }

        // ── Verify-on-checkout fallback ──────────────────────────────
        //
        // Synchronously runs a `SELECT name, setting FROM pg_settings
        // WHERE source='session'` against the inner connection,
        // reconstructs unsafe-GUC state from the result, and clears the
        // dirty flag. Errors are swallowed (mark dirty stays set so the
        // next checkout retries) — verify must never throw user-facing.
        //
        // Called by CachedCommand before executing a user command, but
        // ONLY if IsDirty is set. The cost is one extra round-trip per
        // pool-checkout in the fallback case; on the steady-state hot
        // path (no checkout) it's a single dirty-flag read.
        internal void VerifyAndClearDirty()
        {
            if (!_gucState.IsDirty) return;
            // Single-slot semaphore — if another verify (async) is in
            // flight, just leave dirty set and bail. The async verify
            // will finish and clear it; if it fails, the next checkout
            // tries again.
            bool acquired = false;
            try
            {
                try { acquired = _verifyGate.Wait(0); }
                catch (ObjectDisposedException) { return; }
                if (!acquired) return;
                var verified = QueryPgSettingsSession(CancellationToken.None);
                if (verified != null) _gucState.ApplyVerifiedState(verified);
            }
            catch
            {
                // Mark dirty stays set; next checkout retries. We never
                // throw user-facing exceptions from the verify path.
            }
            finally
            {
                if (acquired)
                {
                    try { _verifyGate.Release(); }
                    catch (ObjectDisposedException) { }
                    catch (SemaphoreFullException) { }
                }
            }
        }

        // ── Async post-call verify ───────────────────────────────────
        //
        // Schedule a verify on a thread-pool task. Used after observing
        // a top-level `SELECT <fn>(...)` or `EXEC` / `CALL` — the
        // function body might have done a SET we couldn't see on the
        // wire. The verify reconstructs server-truth state from
        // pg_settings and atomically swaps it in.
        //
        // Failures (cancelled, connection dropped, etc.) MarkDirty so
        // the next command-checkout retries. The user's hot path is
        // never blocked: this method returns synchronously after
        // dispatching the task.
        internal void ScheduleAsyncVerify()
        {
            CancellationToken ct;
            lock (_verifyCtsLock)
            {
                ct = _verifyCts.Token;
            }
            if (ct.IsCancellationRequested) return;

            // Task.Run posts to the default thread pool. We don't await
            // — fire-and-forget. Any exception is caught inside the
            // lambda; the outer caller never sees it.
            _ = Task.Run(() =>
            {
                bool acquired = false;
                try
                {
                    try
                    {
                        acquired = _verifyGate.Wait(0, ct);
                    }
                    catch (OperationCanceledException)
                    {
                        _gucState.MarkDirty();
                        return;
                    }
                    catch (ObjectDisposedException)
                    {
                        // Connection disposed mid-verify; nothing to do.
                        return;
                    }
                    if (!acquired)
                    {
                        // Another verify in flight (or user command
                        // serializing on the gate). Mark dirty and let
                        // verify-on-checkout retry.
                        _gucState.MarkDirty();
                        return;
                    }
                    if (ct.IsCancellationRequested)
                    {
                        _gucState.MarkDirty();
                        return;
                    }
                    var verified = QueryPgSettingsSession(ct);
                    if (verified != null) _gucState.ApplyVerifiedState(verified);
                    else _gucState.MarkDirty();
                }
                catch
                {
                    _gucState.MarkDirty();
                }
                finally
                {
                    if (acquired)
                    {
                        try { _verifyGate.Release(); }
                        catch (ObjectDisposedException) { }
                        catch (SemaphoreFullException) { }
                    }
                }
            }, ct);
        }

        // Issue the pg_settings query and return name→setting for all
        // session-source GUCs. Returns null on any error (caller decides
        // whether to mark dirty).
        private Dictionary<string, string> QueryPgSettingsSession(CancellationToken ct)
        {
            if (_inner.State != ConnectionState.Open) return null;
            try
            {
                using (var cmd = _inner.CreateCommand())
                {
                    cmd.CommandText = "SELECT name, setting FROM pg_settings WHERE source='session'";
                    cmd.CommandTimeout = 5;
                    using (var reader = cmd.ExecuteReader())
                    {
                        var result = new Dictionary<string, string>(StringComparer.Ordinal);
                        while (reader.Read())
                        {
                            if (ct.IsCancellationRequested) return null;
                            var name = reader.IsDBNull(0) ? null : reader.GetString(0);
                            var value = reader.IsDBNull(1) ? null : reader.GetString(1);
                            if (!string.IsNullOrEmpty(name))
                                result[name.ToLowerInvariant()] = value ?? string.Empty;
                        }
                        return result;
                    }
                }
            }
            catch
            {
                return null;
            }
        }

        // ── Npgsql No Reset On Close detection ───────────────────────

        // Sniff whether <paramref name="conn"/> is an NpgsqlConnection
        // and, if so, parse `No Reset On Close` from its connection
        // string. Defaults to `false` for non-Npgsql or any error —
        // that triggers the more conservative verify-on-checkout path.
        //
        // Detection by name+namespace rather than `is NpgsqlConnection`
        // so the wrapper assembly stays usable even if a downstream
        // consumer pulls in a forked Npgsql or a future major-version
        // change. The connection-string parsing uses
        // DbConnectionStringBuilder (in the netstandard2.0 surface) so
        // we don't have a hard runtime cast to NpgsqlConnectionStringBuilder.
        internal static bool DetectNpgsqlAutoReset(DbConnection conn)
        {
            if (conn == null) return false;
            var t = conn.GetType();
            // Walk the inheritance chain — the user might pass a
            // subclass of NpgsqlConnection (e.g. test wrappers).
            bool isNpgsql = false;
            for (var cursor = t; cursor != null; cursor = cursor.BaseType)
            {
                if (cursor.FullName == "Npgsql.NpgsqlConnection")
                {
                    isNpgsql = true;
                    break;
                }
            }
            if (!isNpgsql) return false;

            string cs;
            try { cs = conn.ConnectionString; }
            catch { return false; }
            if (string.IsNullOrEmpty(cs)) return false;

            return ParseNoResetOnClose(cs);
        }

        /// <summary>
        /// Parse the <c>No Reset On Close</c> setting (or its
        /// underscore-spelled siblings) out of an Npgsql connection
        /// string, returning <c>true</c> when Npgsql will issue a
        /// DISCARD ALL on connection-close (the safe default we can
        /// rely on for cheap state-clearing).
        /// </summary>
        /// <remarks>
        /// Npgsql accepts the keyword case-insensitively and tolerates
        /// underscores or spaces (`No_Reset_On_Close` ≡ `No Reset On Close`).
        /// We use <see cref="DbConnectionStringBuilder"/> for parsing —
        /// it normalises quoting and escapes — and cast the value
        /// permissively (<c>"true"</c>, <c>"1"</c>, <c>"yes"</c>, etc.).
        /// Returns <c>true</c> when the setting is absent (Npgsql's
        /// default is to RESET — i.e. <c>NoResetOnClose=false</c>, which
        /// means our state IS cleared on close).
        /// </remarks>
        internal static bool ParseNoResetOnClose(string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString)) return false;
            try
            {
                var b = new DbConnectionStringBuilder { ConnectionString = connectionString };
                // Normalised keys are lowercase. Both `no reset on close`
                // and `noresetonclose` are accepted — Npgsql's parser
                // strips the spaces / underscores. DbConnectionStringBuilder
                // doesn't strip; we have to check both spellings.
                string raw = null;
                if (b.ContainsKey("no reset on close"))
                    raw = b["no reset on close"]?.ToString();
                else if (b.ContainsKey("noresetonclose"))
                    raw = b["noresetonclose"]?.ToString();

                bool noResetOnClose;
                if (string.IsNullOrEmpty(raw))
                    noResetOnClose = false; // Npgsql default: reset enabled.
                else if (!TryParseBoolPermissive(raw, out noResetOnClose))
                    noResetOnClose = false; // Garbage value → assume default.

                // Auto-reset is the inverse of "no reset on close":
                // NoResetOnClose=false → reset happens → autoReset=true.
                return !noResetOnClose;
            }
            catch
            {
                return false;
            }
        }

        // Accept any of the booleans Npgsql/PG drivers tolerate.
        private static bool TryParseBoolPermissive(string s, out bool value)
        {
            value = false;
            if (string.IsNullOrEmpty(s)) return false;
            var v = s.Trim();
            if (v.Equals("true", StringComparison.OrdinalIgnoreCase) ||
                v.Equals("yes", StringComparison.OrdinalIgnoreCase) ||
                v.Equals("on", StringComparison.OrdinalIgnoreCase) ||
                v == "1") { value = true; return true; }
            if (v.Equals("false", StringComparison.OrdinalIgnoreCase) ||
                v.Equals("no", StringComparison.OrdinalIgnoreCase) ||
                v.Equals("off", StringComparison.OrdinalIgnoreCase) ||
                v == "0") { value = false; return true; }
            return false;
        }
    }

    internal class CachedTransaction : DbTransaction
    {
        private readonly DbTransaction _inner;
        private readonly CachedConnection _conn;

        public CachedTransaction(DbTransaction inner, CachedConnection conn)
        {
            _inner = inner;
            _conn = conn;
        }

        internal DbTransaction InnerTransaction => _inner;

        public override IsolationLevel IsolationLevel => _inner.IsolationLevel;
        protected override DbConnection DbConnection => _conn;

        public override void Commit()
        {
            _inner.Commit();
            _conn.InTransaction = false;
        }

        public override void Rollback()
        {
            _inner.Rollback();
            _conn.InTransaction = false;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _conn.InTransaction = false;
                _inner.Dispose();
            }
            base.Dispose(disposing);
        }
    }

    internal class CachedCommand : DbCommand
    {
        private readonly DbCommand _inner;
        private readonly CachedConnection _conn;

        public CachedCommand(DbCommand inner, CachedConnection conn)
        {
            _inner = inner;
            _conn = conn;
        }

        public override string CommandText
        {
            get => _inner.CommandText;
            set => _inner.CommandText = value;
        }

        public override int CommandTimeout
        {
            get => _inner.CommandTimeout;
            set => _inner.CommandTimeout = value;
        }

        public override CommandType CommandType
        {
            get => _inner.CommandType;
            set => _inner.CommandType = value;
        }

        public override bool DesignTimeVisible
        {
            get => _inner.DesignTimeVisible;
            set => _inner.DesignTimeVisible = value;
        }

        public override UpdateRowSource UpdatedRowSource
        {
            get => _inner.UpdatedRowSource;
            set => _inner.UpdatedRowSource = value;
        }

        protected override DbConnection DbConnection
        {
            get => _conn;
            set => throw new NotSupportedException(
                "Cannot change the connection of a CachedCommand. " +
                "Create a new command from the desired connection instead.");
        }

        protected override DbParameterCollection DbParameterCollection => _inner.Parameters;

        protected override DbTransaction DbTransaction
        {
            get => _inner.Transaction;
            set => _inner.Transaction = value is CachedTransaction ct ? ct.InnerTransaction : value;
        }

        public override void Prepare() => _inner.Prepare();
        public override void Cancel() => _inner.Cancel();

        protected override DbParameter CreateDbParameter() => _inner.CreateParameter();

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            var sql = CommandText ?? "";
            var cache = _conn.Cache;

            // Verify-on-checkout: if state is dirty (Open() marked us
            // dirty for non-Npgsql, or a previous async verify failed),
            // synchronously reconstruct authoritative state from
            // pg_settings BEFORE building any cache key. This is the
            // safety net that closes the gap between observation and
            // server-side truth.
            _conn.VerifyAndClearDirty();

            // Transaction tracking via SQL — walk all segments so a
            // multi-statement body like `BEGIN; INSERT...; COMMIT`
            // settles to the correct final InTransaction state. The
            // pre-fix single-token check only saw the first token (BEGIN)
            // and left the wrapper stuck in InTransaction=true forever,
            // bypassing cache reads permanently after the body completed.
            // See NativeCache.DetectTxTransition.
            var txFinal = NativeCache.DetectTxTransition(sql);
            if (txFinal.HasValue) _conn.InTransaction = txFinal.Value;

            // GUC-RLS cache safety: observe every SQL for SET / RESET so
            // the per-connection state hash is up to date before the
            // cache key is built. Multi-statement bodies (e.g.
            // `SET app.user_id = '42'; SELECT ...`) update state on the
            // SET segment and the SELECT segment looks up against the
            // new state hash.
            _conn.GucState.ObserveSql(sql);

            // Write detection — run per-segment so multi-statement bodies
            // like `SET app.user_id = '42'; INSERT INTO orders VALUES (1)`
            // still trigger invalidation for the INSERT segment. See
            // NativeCache.DetectWritesMulti.
            var writes = NativeCache.DetectWritesMulti(sql);
            if (writes.Count > 0)
            {
                if (writes.Contains(NativeCache.DdlSentinel))
                    cache.InvalidateAll();
                else
                    foreach (var t in writes) cache.InvalidateTable(t);
                var writerReader = _inner.ExecuteReader(behavior);
                MaybeScheduleAsyncVerify(sql);
                return writerReader;
            }

            // In transaction: bypass cache
            if (_conn.InTransaction)
            {
                var txReader = _inner.ExecuteReader(behavior);
                MaybeScheduleAsyncVerify(sql);
                return txReader;
            }

            // Check native cache — fold in the connection's state hash so
            // two connections with different unsafe-GUC values never
            // share a cache slot.
            var parameters = GetParameterArray();
            var stateHash = _conn.GucState.StateHash;
            var entry = cache.Get(sql, parameters, stateHash);
            if (entry != null)
                return new CachedDataReader(entry.Rows, entry.Columns);

            // Cache miss
            var reader = _inner.ExecuteReader(behavior);
            var result = CacheAndReturn(sql, parameters, reader, stateHash);
            MaybeScheduleAsyncVerify(sql);
            return result;
        }

        public override int ExecuteNonQuery()
        {
            var sql = CommandText ?? "";
            var cache = _conn.Cache;

            _conn.VerifyAndClearDirty();

            // Multi-segment tx transition: see ExecuteDbDataReader.
            var txFinal = NativeCache.DetectTxTransition(sql);
            if (txFinal.HasValue) _conn.InTransaction = txFinal.Value;

            // GUC-RLS cache safety: see ExecuteDbDataReader.
            _conn.GucState.ObserveSql(sql);

            // Multi-segment write detection: see ExecuteDbDataReader.
            var writes = NativeCache.DetectWritesMulti(sql);
            if (writes.Count > 0)
            {
                if (writes.Contains(NativeCache.DdlSentinel))
                    cache.InvalidateAll();
                else
                    foreach (var t in writes) cache.InvalidateTable(t);
            }
            var rv = _inner.ExecuteNonQuery();
            MaybeScheduleAsyncVerify(sql);
            return rv;
        }

        public override object ExecuteScalar()
        {
            var sql = CommandText ?? "";
            var cache = _conn.Cache;

            _conn.VerifyAndClearDirty();

            // Multi-segment tx transition: see ExecuteDbDataReader.
            var txFinal = NativeCache.DetectTxTransition(sql);
            if (txFinal.HasValue) _conn.InTransaction = txFinal.Value;

            // GUC-RLS cache safety: see ExecuteDbDataReader.
            _conn.GucState.ObserveSql(sql);

            // Multi-segment write detection: see ExecuteDbDataReader.
            var writes = NativeCache.DetectWritesMulti(sql);
            if (writes.Count > 0)
            {
                if (writes.Contains(NativeCache.DdlSentinel))
                    cache.InvalidateAll();
                else
                    foreach (var t in writes) cache.InvalidateTable(t);
            }
            var rv = _inner.ExecuteScalar();
            MaybeScheduleAsyncVerify(sql);
            return rv;
        }

        // Schedule an async verify if the SQL is a top-level function
        // call or stored-procedure invocation. The function body might
        // have done a SET we couldn't see on the wire; the async path
        // catches it without blocking the user.
        private void MaybeScheduleAsyncVerify(string sql)
        {
            if (NativeCache.IsFunctionCallStatement(sql))
                _conn.ScheduleAsyncVerify();
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                _inner.Dispose();
            base.Dispose(disposing);
        }

        private object[] GetParameterArray()
        {
            if (_inner.Parameters.Count == 0) return null;
            var arr = new object[_inner.Parameters.Count];
            for (int i = 0; i < _inner.Parameters.Count; i++)
                arr[i] = _inner.Parameters[i].Value;
            return arr;
        }

        private DbDataReader CacheAndReturn(string sql, object[] parameters, DbDataReader reader, long stateHash)
        {
            try
            {
                var colCount = reader.FieldCount;
                var columns = new string[colCount];
                for (int i = 0; i < colCount; i++)
                    columns[i] = reader.GetName(i);

                var rows = new List<object[]>();
                while (reader.Read())
                {
                    var row = new object[colCount];
                    for (int i = 0; i < colCount; i++)
                        row[i] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                    rows.Add(row);
                }
                reader.Close();

                var rowArray = rows.ToArray();
                // Skip cache-put for session-state commands (SET / RESET /
                // LISTEN / UNLISTEN / NOTIFY / etc.). They return empty
                // rowsets but would otherwise satisfy the "rows + columns
                // are non-null" gate and bloat the cache with no-row
                // entries that never serve real data. See
                // NativeCache.IsSessionStateCommand and
                // docs/todos/wrapper-cache-set-responses.md.
                if (!NativeCache.IsSessionStateCommand(sql))
                    _conn.Cache.Put(sql, parameters, rowArray, columns, stateHash);
                return new CachedDataReader(rowArray, columns);
            }
            catch
            {
                return reader;
            }
        }
    }
}
