using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace GoldLapel.Tests
{
    /// <summary>
    /// Regression tests for <c>Utils.StreamRead</c> transaction wrapping.
    ///
    /// Under autocommit, <c>SELECT ... FOR UPDATE</c> releases its row lock
    /// as soon as the statement returns, so concurrent consumers can both
    /// advance the group cursor and claim the same pending messages. These
    /// tests prove that StreamRead wraps the cursor-read → advance →
    /// pending-insert sequence in an explicit transaction.
    /// </summary>
    public class StreamReadTxTests
    {
        private static DdlEntry Patterns() => new DdlEntry
        {
            Tables = new Dictionary<string, string>
            {
                ["stream"] = "_goldlapel.stream_orders",
                ["groups"] = "_goldlapel.stream_orders_groups",
                ["pending"] = "_goldlapel.stream_orders_pending",
            },
            QueryPatterns = new Dictionary<string, string>
            {
                ["group_get_cursor"] = "SELECT last_delivered_id FROM g WHERE group_name = $1 FOR UPDATE",
                ["read_since"] = "SELECT id, payload, created_at FROM m WHERE id > $1 ORDER BY id LIMIT $2",
                ["group_advance_cursor"] = "UPDATE g SET last_delivered_id = $1 WHERE group_name = $2",
                ["pending_insert"] = "INSERT INTO p (message_id, group_name, consumer) VALUES ($1, $2, $3)",
            },
        };

        [Fact]
        public void WrapsInTransactionAndCommits()
        {
            var conn = new TxSpyConnection { NextScalarResult = 0L };
            Utils.StreamRead(conn, "orders", "workers", "c", 10, Patterns());

            Assert.Equal(1, conn.BeginCount);
            Assert.Equal(1, conn.LastTx.CommitCount);
            Assert.Equal(0, conn.LastTx.RollbackCount);
            Assert.True(conn.LastTx.Disposed);

            Assert.NotEmpty(conn.Commands);
            // Every DbCommand issued by StreamRead must have its Transaction
            // set to the tx we opened — otherwise the ExecuteScalar() /
            // ExecuteNonQuery() calls wouldn't participate in it.
            foreach (var c in conn.Commands)
            {
                Assert.Same(conn.LastTx, c.TransactionForTest);
            }

            Assert.Contains(conn.Commands, c => c.CommandText.Contains("FOR UPDATE"));
        }

        [Fact]
        public void CommitsEvenWhenGroupCursorMissing()
        {
            var conn = new TxSpyConnection { NextScalarResult = null }; // DBNull-ish
            var result = Utils.StreamRead(conn, "orders", "workers", "c", 10, Patterns());
            Assert.Empty(result);
            Assert.Equal(1, conn.BeginCount);
            Assert.Equal(1, conn.LastTx.CommitCount);
            Assert.Equal(0, conn.LastTx.RollbackCount);
            Assert.True(conn.LastTx.Disposed);
        }

        [Fact]
        public void RollsBackOnScalarException()
        {
            var conn = new TxSpyConnection { ThrowOnScalar = new InvalidOperationException("boom") };
            Assert.Throws<InvalidOperationException>(() =>
                Utils.StreamRead(conn, "orders", "workers", "c", 10, Patterns()));
            Assert.Equal(1, conn.BeginCount);
            Assert.Equal(0, conn.LastTx.CommitCount);
            Assert.Equal(1, conn.LastTx.RollbackCount);
            Assert.True(conn.LastTx.Disposed);
        }

        [Fact]
        public void DisposesTransactionEvenIfRollbackThrows()
        {
            var conn = new TxSpyConnection
            {
                ThrowOnScalar = new InvalidOperationException("boom"),
                MakeTxRollbackThrow = true,
            };
            // The original error should surface — rollback exceptions are
            // swallowed so the caller sees the real failure.
            Assert.Throws<InvalidOperationException>(() =>
                Utils.StreamRead(conn, "orders", "workers", "c", 10, Patterns()));
            Assert.True(conn.LastTx.Disposed);
        }

        /// <summary>
        /// Concurrency: two "consumers" racing on the same group must each
        /// receive a disjoint subset of the pending messages.
        ///
        /// This test uses a fake Postgres-like engine where FOR UPDATE only
        /// blocks the other reader while an explicit tx is open on that
        /// connection. Under the autocommit bug (no BEGIN wrapping) the two
        /// consumers both see cursor=0 and claim the same messages.
        /// </summary>
        [Fact]
        public async Task ConcurrentConsumersDoNotDoubleClaim()
        {
            var engine = new FakeStreamEngine(new long[] { 1, 2, 3, 4 });

            async Task<List<long>> Run(string consumer)
            {
                var ids = new List<long>();
                while (true)
                {
                    using var conn = engine.NewConnection();
                    conn.Open();
                    var batch = Utils.StreamRead(conn, "orders", "workers", consumer, 4, Patterns());
                    if (batch.Count == 0) break;
                    foreach (var m in batch)
                        ids.Add(Convert.ToInt64(m["id"]));
                }
                return ids;
            }

            var a = Run("ca");
            var b = Run("cb");
            var aIds = await a;
            var bIds = await b;

            var all = aIds.Concat(bIds).OrderBy(x => x).ToList();
            Assert.Equal(new long[] { 1, 2, 3, 4 }, all);
            var overlap = aIds.Intersect(bIds).ToList();
            Assert.Empty(overlap);
        }

        // ── test-doubles ────────────────────────────────────────────

        private class TxSpyConnection : DbConnection
        {
            public List<TxSpyCommand> Commands { get; } = new();
            public int BeginCount { get; private set; }
            public TxSpyTransaction LastTx { get; private set; }
            public object NextScalarResult { get; set; }
            public Exception ThrowOnScalar { get; set; }
            public bool MakeTxRollbackThrow { get; set; }

            public override string ConnectionString { get; set; } = "spy";
            public override string Database => "spy";
            public override string DataSource => "spy";
            public override string ServerVersion => "1.0";
            public override ConnectionState State => ConnectionState.Open;
            public override void ChangeDatabase(string name) { }
            public override void Open() { }
            public override void Close() { }

            protected override DbTransaction BeginDbTransaction(IsolationLevel level)
            {
                BeginCount++;
                LastTx = new TxSpyTransaction(this, MakeTxRollbackThrow);
                return LastTx;
            }

            protected override DbCommand CreateDbCommand()
            {
                var c = new TxSpyCommand(this);
                Commands.Add(c);
                return c;
            }
        }

        private class TxSpyTransaction : DbTransaction
        {
            private readonly TxSpyConnection _conn;
            private readonly bool _throwOnRollback;
            public int CommitCount;
            public int RollbackCount;
            public bool Disposed;
            public TxSpyTransaction(TxSpyConnection c, bool throwOnRollback)
            {
                _conn = c; _throwOnRollback = throwOnRollback;
            }
            public override IsolationLevel IsolationLevel => IsolationLevel.ReadCommitted;
            protected override DbConnection DbConnection => _conn;
            public override void Commit() { CommitCount++; }
            public override void Rollback()
            {
                RollbackCount++;
                if (_throwOnRollback) throw new Exception("rollback-threw");
            }
            protected override void Dispose(bool disposing) { Disposed = true; base.Dispose(disposing); }
        }

        private class TxSpyCommand : DbCommand
        {
            private readonly TxSpyConnection _conn;
            private readonly FakeParameterCollection _params = new();
            public TxSpyCommand(TxSpyConnection c) { _conn = c; }
            public override string CommandText { get; set; }
            public override int CommandTimeout { get; set; }
            public override CommandType CommandType { get; set; }
            public override bool DesignTimeVisible { get; set; }
            public override UpdateRowSource UpdatedRowSource { get; set; }
            protected override DbConnection DbConnection { get; set; }
            protected override DbParameterCollection DbParameterCollection => _params;
            protected override DbTransaction DbTransaction { get; set; }
            public DbTransaction TransactionForTest => DbTransaction;
            public override void Prepare() { }
            public override void Cancel() { }
            protected override DbParameter CreateDbParameter() => new FakeParameter();
            protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
                => new FakeDataReader(Array.Empty<object[]>(), Array.Empty<string>());
            public override int ExecuteNonQuery() => 1;
            public override object ExecuteScalar()
            {
                if (_conn.ThrowOnScalar != null) throw _conn.ThrowOnScalar;
                return _conn.NextScalarResult;
            }
        }

        // ── concurrency engine ──────────────────────────────────────

        /// <summary>
        /// Postgres-like stream cursor simulator. FOR UPDATE inside an open
        /// tx holds the group row exclusively; under autocommit the lock is
        /// released immediately. Mirrors the real bug being fixed.
        /// </summary>
        internal class FakeStreamEngine
        {
            private readonly List<long> _messageIds;
            private long _cursor = 0;
            private readonly SemaphoreSlim _lock = new(1, 1);
            public FakeStreamEngine(long[] ids) { _messageIds = ids.ToList(); }

            public long Cursor { get { lock (_messageIds) return _cursor; } }

            public void AcquireCursorLock() => _lock.Wait();
            public void ReleaseCursorLock() => _lock.Release();

            public long ReadCursor() => Volatile.Read(ref _cursor);
            public void AdvanceCursor(long v) => Volatile.Write(ref _cursor, v);

            public List<object[]> ReadSince(long lastId, int count)
            {
                var rows = new List<object[]>();
                foreach (var id in _messageIds)
                {
                    if (id > lastId && rows.Count < count)
                    {
                        rows.Add(new object[] { id, "{}", DateTime.UtcNow });
                    }
                }
                return rows;
            }

            public FakeEngineConnection NewConnection() => new FakeEngineConnection(this);
        }

        internal class FakeEngineConnection : DbConnection
        {
            private readonly FakeStreamEngine _engine;
            public FakeEngineTransaction CurrentTx;
            public FakeEngineConnection(FakeStreamEngine e) { _engine = e; }
            public FakeStreamEngine Engine => _engine;

            public override string ConnectionString { get; set; } = "fake";
            public override string Database => "fake";
            public override string DataSource => "fake";
            public override string ServerVersion => "1.0";
            public override ConnectionState State => ConnectionState.Open;
            public override void ChangeDatabase(string n) { }
            public override void Open() { }
            public override void Close() { }

            protected override DbTransaction BeginDbTransaction(IsolationLevel level)
            {
                CurrentTx = new FakeEngineTransaction(this);
                return CurrentTx;
            }
            protected override DbCommand CreateDbCommand() => new FakeEngineCommand(this);
        }

        internal class FakeEngineTransaction : DbTransaction
        {
            private readonly FakeEngineConnection _conn;
            public bool LockHeld;
            public FakeEngineTransaction(FakeEngineConnection c) { _conn = c; }
            public override IsolationLevel IsolationLevel => IsolationLevel.ReadCommitted;
            protected override DbConnection DbConnection => _conn;
            public override void Commit() { Release(); _conn.CurrentTx = null; }
            public override void Rollback() { Release(); _conn.CurrentTx = null; }
            protected override void Dispose(bool disposing) { Release(); base.Dispose(disposing); }
            private void Release()
            {
                if (LockHeld) { _conn.Engine.ReleaseCursorLock(); LockHeld = false; }
            }
        }

        internal class FakeEngineCommand : DbCommand
        {
            private readonly FakeEngineConnection _conn;
            private readonly FakeParameterCollection _params = new();
            public FakeEngineCommand(FakeEngineConnection c) { _conn = c; }
            public override string CommandText { get; set; }
            public override int CommandTimeout { get; set; }
            public override CommandType CommandType { get; set; }
            public override bool DesignTimeVisible { get; set; }
            public override UpdateRowSource UpdatedRowSource { get; set; }
            protected override DbConnection DbConnection { get; set; }
            protected override DbParameterCollection DbParameterCollection => _params;
            protected override DbTransaction DbTransaction { get; set; }
            public override void Prepare() { }
            public override void Cancel() { }
            protected override DbParameter CreateDbParameter() => new FakeParameter();

            public override object ExecuteScalar()
            {
                // group_get_cursor: FOR UPDATE
                if (CommandText.Contains("FOR UPDATE"))
                {
                    var tx = (FakeEngineTransaction)DbTransaction;
                    if (tx == null)
                    {
                        // autocommit: lock + release immediately (the bug).
                        _conn.Engine.AcquireCursorLock();
                        _conn.Engine.ReleaseCursorLock();
                    }
                    else
                    {
                        _conn.Engine.AcquireCursorLock();
                        tx.LockHeld = true;
                    }
                    return _conn.Engine.ReadCursor();
                }
                return null;
            }

            public override int ExecuteNonQuery()
            {
                // group_advance_cursor: UPDATE g SET last_delivered_id = $1
                if (CommandText.StartsWith("UPDATE g"))
                {
                    var newCursor = Convert.ToInt64(((DbParameter)_params[0]).Value);
                    _conn.Engine.AdvanceCursor(newCursor);
                }
                return 1;
            }

            protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
            {
                if (CommandText.Contains("ORDER BY id"))
                {
                    var lastId = Convert.ToInt64(((DbParameter)_params[0]).Value);
                    var count = Convert.ToInt32(((DbParameter)_params[1]).Value);
                    var rows = _conn.Engine.ReadSince(lastId, count);
                    return new FakeDataReader(rows.ToArray(), new[] { "id", "payload", "created_at" });
                }
                return new FakeDataReader(Array.Empty<object[]>(), Array.Empty<string>());
            }
        }
    }
}
