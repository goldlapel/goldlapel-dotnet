using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

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

        public CachedConnection(DbConnection inner, NativeCache cache)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            _cache = cache ?? throw new ArgumentNullException(nameof(cache));
        }

        internal DbConnection Inner => _inner;
        internal NativeCache Cache => _cache;
        internal ConnectionGucState GucState => _gucState;
        internal bool InTransaction
        {
            get => _inTransaction;
            set => _inTransaction = value;
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
        public override void Open() => _inner.Open();
        public override void Close() => _inner.Close();

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
                _inner.Dispose();
            base.Dispose(disposing);
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
                return _inner.ExecuteReader(behavior);
            }

            // In transaction: bypass cache
            if (_conn.InTransaction)
                return _inner.ExecuteReader(behavior);

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
            return CacheAndReturn(sql, parameters, reader, stateHash);
        }

        public override int ExecuteNonQuery()
        {
            var sql = CommandText ?? "";
            var cache = _conn.Cache;

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
            return _inner.ExecuteNonQuery();
        }

        public override object ExecuteScalar()
        {
            var sql = CommandText ?? "";
            var cache = _conn.Cache;

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
            return _inner.ExecuteScalar();
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
