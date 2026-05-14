using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using Xunit;

namespace GoldLapel.Tests
{
    // ── CachedDataReader ─────────────────────────────────────

    public class CachedDataReaderTest
    {
        [Fact]
        public void ReadRows()
        {
            var rows = new[]
            {
                new object[] { 1, "alice" },
                new object[] { 2, "bob" }
            };
            var reader = new CachedDataReader(rows, new[] { "id", "name" });

            Assert.True(reader.HasRows);
            Assert.Equal(2, reader.FieldCount);

            Assert.True(reader.Read());
            Assert.Equal(1, reader.GetValue(0));
            Assert.Equal("alice", reader.GetValue(1));

            Assert.True(reader.Read());
            Assert.Equal(2, reader.GetValue(0));
            Assert.Equal("bob", reader.GetValue(1));

            Assert.False(reader.Read());
        }

        [Fact]
        public void GetByName()
        {
            var rows = new[] { new object[] { 42, "test" } };
            var reader = new CachedDataReader(rows, new[] { "id", "name" });
            reader.Read();

            Assert.Equal(42, reader["id"]);
            Assert.Equal("test", reader["name"]);
        }

        [Fact]
        public void GetOrdinal()
        {
            var reader = new CachedDataReader(new object[0][], new[] { "id", "name" });
            Assert.Equal(0, reader.GetOrdinal("id"));
            Assert.Equal(1, reader.GetOrdinal("name"));
        }

        [Fact]
        public void GetOrdinalCaseInsensitive()
        {
            var reader = new CachedDataReader(new object[0][], new[] { "Id", "Name" });
            Assert.Equal(0, reader.GetOrdinal("id"));
            Assert.Equal(1, reader.GetOrdinal("NAME"));
        }

        [Fact]
        public void GetOrdinalNotFound()
        {
            var reader = new CachedDataReader(new object[0][], new[] { "id" });
            Assert.Throws<IndexOutOfRangeException>(() => reader.GetOrdinal("missing"));
        }

        [Fact]
        public void GetName()
        {
            var reader = new CachedDataReader(new object[0][], new[] { "id", "name" });
            Assert.Equal("id", reader.GetName(0));
            Assert.Equal("name", reader.GetName(1));
        }

        [Fact]
        public void IsDBNull()
        {
            var rows = new[] { new object[] { null, "test" } };
            var reader = new CachedDataReader(rows, new[] { "id", "name" });
            reader.Read();
            Assert.True(reader.IsDBNull(0));
            Assert.False(reader.IsDBNull(1));
        }

        [Fact]
        public void EmptyResultSet()
        {
            var reader = new CachedDataReader(new object[0][], new[] { "id" });
            Assert.False(reader.HasRows);
            Assert.False(reader.Read());
        }

        [Fact]
        public void GetString()
        {
            var rows = new[] { new object[] { "hello" } };
            var reader = new CachedDataReader(rows, new[] { "val" });
            reader.Read();
            Assert.Equal("hello", reader.GetString(0));
        }

        [Fact]
        public void GetInt32()
        {
            var rows = new[] { new object[] { 42 } };
            var reader = new CachedDataReader(rows, new[] { "val" });
            reader.Read();
            Assert.Equal(42, reader.GetInt32(0));
        }

        [Fact]
        public void GetInt64()
        {
            var rows = new[] { new object[] { 99L } };
            var reader = new CachedDataReader(rows, new[] { "val" });
            reader.Read();
            Assert.Equal(99L, reader.GetInt64(0));
        }

        [Fact]
        public void GetBoolean()
        {
            var rows = new[] { new object[] { true } };
            var reader = new CachedDataReader(rows, new[] { "val" });
            reader.Read();
            Assert.True(reader.GetBoolean(0));
        }

        [Fact]
        public void GetDouble()
        {
            var rows = new[] { new object[] { 3.14 } };
            var reader = new CachedDataReader(rows, new[] { "val" });
            reader.Read();
            Assert.Equal(3.14, reader.GetDouble(0));
        }

        [Fact]
        public void GetValues()
        {
            var rows = new[] { new object[] { 1, "test", true } };
            var reader = new CachedDataReader(rows, new[] { "a", "b", "c" });
            reader.Read();
            var values = new object[3];
            var count = reader.GetValues(values);
            Assert.Equal(3, count);
            Assert.Equal(1, values[0]);
            Assert.Equal("test", values[1]);
            Assert.Equal(true, values[2]);
        }

        [Fact]
        public void CloseAndIsClosed()
        {
            var reader = new CachedDataReader(new object[0][], new[] { "id" });
            Assert.False(reader.IsClosed);
            reader.Close();
            Assert.True(reader.IsClosed);
        }

        [Fact]
        public void SchemaTable()
        {
            var reader = new CachedDataReader(new object[0][], new[] { "id", "name" });
            var schema = reader.GetSchemaTable();
            Assert.Equal(2, schema.Rows.Count);
            Assert.Equal("id", schema.Rows[0]["ColumnName"]);
            Assert.Equal("name", schema.Rows[1]["ColumnName"]);
        }
    }

    // ── CachedConnection ─────────────────────────────────────

    public class CachedConnectionTest : IDisposable
    {
        public CachedConnectionTest() { NativeCache.Reset(); }
        public void Dispose() { NativeCache.Reset(); }

        [Fact]
        public void ConstructorRejectsNull()
        {
            var cache = new NativeCache();
            Assert.Throws<ArgumentNullException>(() => new CachedConnection(null, cache));
        }

        [Fact]
        public void ConstructorRejectsNullCache()
        {
            Assert.Throws<ArgumentNullException>(() => new CachedConnection(new FakeConnection(), null));
        }

        [Fact]
        public void CreateCommandReturnsCachedCommand()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = new FakeConnection();
            var conn = new CachedConnection(inner, cache);
            var cmd = conn.CreateCommand();
            Assert.IsType<CachedCommand>(cmd);
        }

        [Fact]
        public void SelectCachesAndReturns()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = new FakeConnection();
            inner.NextReader = new FakeDataReader(
                new[] { new object[] { 1, "alice" } },
                new[] { "id", "name" }
            );
            var conn = new CachedConnection(inner, cache);
            var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT * FROM users";
            var reader = cmd.ExecuteReader();

            Assert.True(reader.Read());
            Assert.Equal(1, reader.GetValue(0));
            Assert.Equal("alice", reader.GetValue(1));
            Assert.False(reader.Read());

            // Second call should hit cache (no new reader on inner)
            inner.NextReader = null;
            var cmd2 = conn.CreateCommand();
            cmd2.CommandText = "SELECT * FROM users";
            var reader2 = cmd2.ExecuteReader();
            Assert.True(reader2.Read());
            Assert.Equal(1, reader2.GetValue(0));
        }

        [Fact]
        public void WriteInvalidatesCache()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = new FakeConnection();
            inner.NextReader = new FakeDataReader(
                new[] { new object[] { 1 } },
                new[] { "id" }
            );
            var conn = new CachedConnection(inner, cache);

            // Populate cache
            var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT * FROM orders";
            cmd.ExecuteReader();

            // Write invalidates
            var writeCmd = conn.CreateCommand();
            writeCmd.CommandText = "INSERT INTO orders VALUES (2)";
            inner.NextNonQueryResult = 1;
            writeCmd.ExecuteNonQuery();

            // Cache miss (needs new reader)
            inner.NextReader = new FakeDataReader(
                new[] { new object[] { 1 }, new object[] { 2 } },
                new[] { "id" }
            );
            var cmd2 = conn.CreateCommand();
            cmd2.CommandText = "SELECT * FROM orders";
            var reader = cmd2.ExecuteReader();
            Assert.True(reader.Read());
        }

        [Fact]
        public void TransactionBypassesCache()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = new FakeConnection();

            // Pre-populate cache
            cache.Put("SELECT * FROM users", null,
                new[] { new object[] { "cached" } }, new[] { "val" });

            var conn = new CachedConnection(inner, cache);

            // Start transaction via BeginTransaction
            inner.NextTransaction = new FakeTransaction();
            var tx = conn.BeginTransaction();

            // Should bypass cache and hit the inner connection
            inner.NextReader = new FakeDataReader(
                new[] { new object[] { "from_db" } },
                new[] { "val" }
            );
            var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT * FROM users";
            var reader = cmd.ExecuteReader();
            Assert.True(reader.Read());
            Assert.Equal("from_db", reader.GetValue(0));

            tx.Commit();
        }

        [Fact]
        public void DdlInvalidatesAll()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            cache.Put("SELECT * FROM users", null,
                new[] { new object[] { "1" } }, new[] { "id" });
            cache.Put("SELECT * FROM orders", null,
                new[] { new object[] { "2" } }, new[] { "id" });

            var inner = new FakeConnection();
            var conn = new CachedConnection(inner, cache);
            var cmd = conn.CreateCommand();
            cmd.CommandText = "CREATE TABLE foo (id int)";
            inner.NextNonQueryResult = 0;
            cmd.ExecuteNonQuery();

            Assert.Equal(0, cache.Size);
        }

        // ── GUC-RLS cache safety integration (wrapper-side native cache) ──
        //
        // End-to-end through CachedConnection: a SET on an unsafe GUC
        // updates the per-connection state hash, and subsequent SELECTs
        // key against the new state — so two queries with the same SQL
        // but different `app.user_id` don't share a cache slot.

        [Fact]
        public void UnsafeSetTracksOnGucState()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = new FakeConnection();
            var conn = new CachedConnection(inner, cache);

            Assert.Equal(0L, conn.GucState.StateHash);

            // SET app.user_id = '42' goes through ExecuteNonQuery (no
            // resultset); the inner FakeConnection ignores the SQL.
            var setCmd = conn.CreateCommand();
            setCmd.CommandText = "SET app.user_id = '42'";
            inner.NextNonQueryResult = 0;
            setCmd.ExecuteNonQuery();

            Assert.NotEqual(0L, conn.GucState.StateHash);
        }

        [Fact]
        public void SafeSetDoesNotPerturbGucState()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = new FakeConnection();
            var conn = new CachedConnection(inner, cache);

            // application_name is harmless — wrappers see it on every
            // libpq handshake and we don't want a per-connection setting
            // to fragment the cache. timezone moved to the unsafe list
            // (it changes the textual representation of timestamp cols).
            var setCmd = conn.CreateCommand();
            setCmd.CommandText = "SET application_name = 'foo'";
            inner.NextNonQueryResult = 0;
            setCmd.ExecuteNonQuery();

            Assert.Equal(0L, conn.GucState.StateHash);
        }

        [Fact]
        public void SelectAfterSetUsesNewStateHashKey()
        {
            // Pre-populate the cache for state_hash=0 with a value the
            // post-SET SELECT must NOT return — proves the SELECT misses
            // and re-fetches under the new state hash.
            var cache = new NativeCache();
            cache.SetConnected(true);
            cache.Put("SELECT * FROM accounts", null,
                new[] { new object[] { "leak-row" } }, new[] { "name" }, 0);

            var inner = new FakeConnection();
            var conn = new CachedConnection(inner, cache);

            // SET app.user_id moves the state hash off 0.
            var setCmd = conn.CreateCommand();
            setCmd.CommandText = "SET app.user_id = '42'";
            inner.NextNonQueryResult = 0;
            setCmd.ExecuteNonQuery();
            Assert.NotEqual(0L, conn.GucState.StateHash);

            // SELECT now misses (state hash differs), re-fetches from
            // the inner connection. If the leak existed, "leak-row"
            // would appear instead of "fresh-row".
            inner.NextReader = new FakeDataReader(
                new[] { new object[] { "fresh-row" } },
                new[] { "name" });
            var selCmd = conn.CreateCommand();
            selCmd.CommandText = "SELECT * FROM accounts";
            var reader = selCmd.ExecuteReader();
            Assert.True(reader.Read());
            Assert.Equal("fresh-row", reader.GetValue(0));
        }

        [Fact]
        public void DistinctConnectionsHaveIndependentGucState()
        {
            // Per-connection state: SET on connection A must not affect
            // connection B's state hash.
            var cache = new NativeCache();
            cache.SetConnected(true);
            var connA = new CachedConnection(new FakeConnection(), cache);
            var connB = new CachedConnection(new FakeConnection(), cache);

            var setA = connA.CreateCommand();
            setA.CommandText = "SET app.user_id = '42'";
            setA.ExecuteNonQuery();

            Assert.NotEqual(0L, connA.GucState.StateHash);
            Assert.Equal(0L, connB.GucState.StateHash);
        }

        [Fact]
        public void MultiStatementSetThroughExecuteReader()
        {
            // `SET app.user_id = '42'; SELECT 1` arriving as one
            // ExecuteReader: the SET segment must update state, and
            // since the SELECT segment has no FROM, write detection
            // doesn't trip — the inner reader is invoked.
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = new FakeConnection();
            inner.NextReader = new FakeDataReader(
                new[] { new object[] { 1 } }, new[] { "v" });
            var conn = new CachedConnection(inner, cache);

            var cmd = conn.CreateCommand();
            cmd.CommandText = "SET app.user_id = '42'; SELECT 1";
            cmd.ExecuteReader();

            Assert.NotEqual(0L, conn.GucState.StateHash);
        }

        // ── Multi-statement write detection (cross-wrapper bug fix) ──
        //
        // The old single-token DetectWrite() looks at the first token of
        // the SQL only. A body like
        // `SET app.user_id = '42'; INSERT INTO orders VALUES (1)` would
        // see SET and slip past write detection, leaving stale `orders`
        // cache entries alive across the INSERT. Each path
        // (ExecuteReader, ExecuteNonQuery, ExecuteScalar) now runs
        // DetectWritesMulti and unions invalidations across segments.

        [Fact]
        public void MultiStatementInsertInvalidatesViaExecuteNonQuery()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            cache.Put("SELECT * FROM orders", null,
                new[] { new object[] { 1 } }, new[] { "id" });
            Assert.Equal(1, cache.Size);

            var inner = new FakeConnection();
            var conn = new CachedConnection(inner, cache);
            var cmd = conn.CreateCommand();
            cmd.CommandText = "SET app.user_id = '42'; INSERT INTO orders VALUES (1)";
            inner.NextNonQueryResult = 1;
            cmd.ExecuteNonQuery();

            // The INSERT segment must trigger orders invalidation.
            Assert.Equal(0, cache.Size);
        }

        [Fact]
        public void MultiStatementInsertInvalidatesViaExecuteReader()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            cache.Put("SELECT * FROM orders", null,
                new[] { new object[] { 1 } }, new[] { "id" });
            Assert.Equal(1, cache.Size);

            var inner = new FakeConnection();
            inner.NextReader = new FakeDataReader(new object[0][], new string[0]);
            var conn = new CachedConnection(inner, cache);
            var cmd = conn.CreateCommand();
            cmd.CommandText = "SET app.user_id = '42'; INSERT INTO orders VALUES (1)";
            cmd.ExecuteReader();

            Assert.Equal(0, cache.Size);
        }

        [Fact]
        public void MultiStatementInsertInvalidatesViaExecuteScalar()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            cache.Put("SELECT * FROM orders", null,
                new[] { new object[] { 1 } }, new[] { "id" });

            var inner = new FakeConnection();
            var conn = new CachedConnection(inner, cache);
            var cmd = conn.CreateCommand();
            cmd.CommandText = "SET app.user_id = '42'; INSERT INTO orders VALUES (1)";
            cmd.ExecuteScalar();

            Assert.Equal(0, cache.Size);
        }

        [Fact]
        public void MultiStatementDdlInvalidatesAll()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            cache.Put("SELECT * FROM users", null,
                new[] { new object[] { 1 } }, new[] { "id" });
            cache.Put("SELECT * FROM orders", null,
                new[] { new object[] { 1 } }, new[] { "id" });
            Assert.Equal(2, cache.Size);

            var inner = new FakeConnection();
            var conn = new CachedConnection(inner, cache);
            var cmd = conn.CreateCommand();
            cmd.CommandText = "INSERT INTO orders VALUES (1); CREATE TABLE foo (id int)";
            inner.NextNonQueryResult = 0;
            cmd.ExecuteNonQuery();

            // DDL anywhere → invalidate all.
            Assert.Equal(0, cache.Size);
        }

        // ── Session-state commands not cached (cross-wrapper bug fix) ──
        //
        // ExecuteReader on `SET foo = 'bar'` returns an empty rowset
        // (FieldCount = 0). Without the IsSessionStateCommand guard the
        // wrapper would put `(rows=[], columns=[])` into the cache —
        // bloating the cache with no-row entries that never serve real
        // data and applying needless eviction pressure on chatty
        // sessions.

        [Fact]
        public void SetThroughExecuteReaderIsNotCached()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = new FakeConnection();
            inner.NextReader = new FakeDataReader(new object[0][], new string[0]);
            var conn = new CachedConnection(inner, cache);

            var cmd = conn.CreateCommand();
            cmd.CommandText = "SET application_name = 'app1'";
            cmd.ExecuteReader();

            Assert.Equal(0, cache.Size);
        }

        [Fact]
        public void ResetThroughExecuteReaderIsNotCached()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = new FakeConnection();
            inner.NextReader = new FakeDataReader(new object[0][], new string[0]);
            var conn = new CachedConnection(inner, cache);

            var cmd = conn.CreateCommand();
            cmd.CommandText = "RESET application_name";
            cmd.ExecuteReader();

            Assert.Equal(0, cache.Size);
        }

        [Fact]
        public void ListenThroughExecuteReaderIsNotCached()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = new FakeConnection();
            inner.NextReader = new FakeDataReader(new object[0][], new string[0]);
            var conn = new CachedConnection(inner, cache);

            var cmd = conn.CreateCommand();
            cmd.CommandText = "LISTEN channel_x";
            cmd.ExecuteReader();

            Assert.Equal(0, cache.Size);
        }

        [Fact]
        public void EmptySelectStillCaches()
        {
            // Sanity check: the IsSessionStateCommand guard must NOT
            // catch genuine SELECTs that happen to return zero rows.
            // Those should still be cached so the next call hits.
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = new FakeConnection();
            inner.NextReader = new FakeDataReader(new object[0][], new[] { "id" });
            var conn = new CachedConnection(inner, cache);

            var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT * FROM orders WHERE 1=0";
            cmd.ExecuteReader();

            Assert.Equal(1, cache.Size);
        }
    }

    // ── RLS hardening: function-call detection ───────────────
    //
    // Top-level function calls (SELECT my_fn(...), CALL fn(...),
    // EXEC fn ...) might run a SET internally that we can't see on
    // the wire. The detector flags these so the wrapper can schedule
    // an async post-call verify against pg_settings.

    public class IsFunctionCallStatementTest
    {
        [Fact] public void PlainSelect()
            => Assert.False(NativeCache.IsFunctionCallStatement("SELECT * FROM accounts"));
        [Fact] public void ScalarSelect()
            => Assert.False(NativeCache.IsFunctionCallStatement("SELECT 1"));
        [Fact] public void SelectFunctionCall()
            => Assert.True(NativeCache.IsFunctionCallStatement("SELECT my_func()"));
        [Fact] public void SelectFunctionWithArgs()
            => Assert.True(NativeCache.IsFunctionCallStatement("SELECT my_func(1, 2, 'x')"));
        [Fact] public void SelectFunctionSchemaQualified()
            => Assert.True(NativeCache.IsFunctionCallStatement("SELECT public.my_func(1)"));
        [Fact] public void SelectFunctionPgCatalog()
            => Assert.True(NativeCache.IsFunctionCallStatement("SELECT pg_catalog.set_config('app.id', '42', false)"));
        [Fact] public void SelectFunctionWithSpaces()
            => Assert.True(NativeCache.IsFunctionCallStatement("SELECT  my_func  (  1  )"));
        [Fact] public void SelectChainedExpressionRejected()
        {
            // `SELECT my_func() || 'x'` — there's stuff after `)`. Not a
            // pure function-call shape; skip verify (we'll observe any
            // SET via parser).
            Assert.False(NativeCache.IsFunctionCallStatement("SELECT my_func() || 'x'"));
        }
        [Fact] public void SelectMultiColumnRejected()
            => Assert.False(NativeCache.IsFunctionCallStatement("SELECT my_func(), my_other_func()"));
        [Fact] public void SelectFromRejected()
            => Assert.False(NativeCache.IsFunctionCallStatement("SELECT * FROM my_func()"));
        [Fact] public void SelectDistinctRejected()
            => Assert.False(NativeCache.IsFunctionCallStatement("SELECT DISTINCT id FROM accounts"));
        [Fact] public void Call()
            => Assert.True(NativeCache.IsFunctionCallStatement("CALL my_proc()"));
        [Fact] public void CallSchemaQualified()
            => Assert.True(NativeCache.IsFunctionCallStatement("CALL public.my_proc(1)"));
        [Fact] public void Exec()
            => Assert.True(NativeCache.IsFunctionCallStatement("EXEC sp_my_proc"));
        [Fact] public void ExecuteFn()
            => Assert.True(NativeCache.IsFunctionCallStatement("EXECUTE my_fn"));
        [Fact] public void TrailingSemicolonAccepted()
            => Assert.True(NativeCache.IsFunctionCallStatement("SELECT my_func();"));
        [Fact] public void Empty()
            => Assert.False(NativeCache.IsFunctionCallStatement(""));
        [Fact] public void WhitespaceOnly()
            => Assert.False(NativeCache.IsFunctionCallStatement("   "));
        [Fact] public void Null()
            => Assert.False(NativeCache.IsFunctionCallStatement(null));
        [Fact] public void CaseInsensitive()
        {
            Assert.True(NativeCache.IsFunctionCallStatement("select my_func()"));
            Assert.True(NativeCache.IsFunctionCallStatement("call my_proc()"));
        }
        [Fact] public void MultiStatementContainingFunctionCall()
        {
            // Any segment matching makes the whole body trigger verify.
            Assert.True(NativeCache.IsFunctionCallStatement(
                "SET app.id = '1'; SELECT my_func()"));
        }
        [Fact] public void MultiStatementAllPlainReturnsFalse()
        {
            Assert.False(NativeCache.IsFunctionCallStatement(
                "SELECT 1; SELECT * FROM orders"));
        }
        [Fact] public void UnterminatedParenRejected()
            => Assert.False(NativeCache.IsFunctionCallStatement("SELECT my_func(1"));
    }

    // ── RLS hardening: Npgsql No-Reset-On-Close detection ────

    public class NpgsqlAutoResetTest
    {
        [Fact] public void NonNpgsqlReturnsFalse()
        {
            var conn = new FakeConnection();
            Assert.False(CachedConnection.DetectNpgsqlAutoReset(conn));
        }

        [Fact] public void NullConnectionReturnsFalse()
            => Assert.False(CachedConnection.DetectNpgsqlAutoReset(null));

        [Fact] public void DefaultEnablesReset()
        {
            // No `No Reset On Close` in the connection string → Npgsql
            // default (reset enabled). ParseNoResetOnClose returns true.
            Assert.True(CachedConnection.ParseNoResetOnClose(
                "Host=localhost;Database=mydb"));
        }

        [Fact] public void EmptyStringReturnsFalse()
            => Assert.False(CachedConnection.ParseNoResetOnClose(""));

        [Fact] public void NoResetOnCloseTrueDisablesAutoReset()
        {
            // `No Reset On Close=true` → reset DOESN'T happen → autoReset=false.
            Assert.False(CachedConnection.ParseNoResetOnClose(
                "Host=localhost;No Reset On Close=true"));
        }

        [Fact] public void NoResetOnCloseFalseEnablesAutoReset()
        {
            Assert.True(CachedConnection.ParseNoResetOnClose(
                "Host=localhost;No Reset On Close=false"));
        }

        [Fact] public void CaseInsensitiveBoolValue()
        {
            Assert.False(CachedConnection.ParseNoResetOnClose(
                "Host=localhost;No Reset On Close=TRUE"));
            Assert.False(CachedConnection.ParseNoResetOnClose(
                "Host=localhost;No Reset On Close=Yes"));
            Assert.False(CachedConnection.ParseNoResetOnClose(
                "Host=localhost;No Reset On Close=1"));
        }

        [Fact] public void UnknownValueDefaultsToReset()
        {
            // Garbage value → assume default (reset enabled).
            Assert.True(CachedConnection.ParseNoResetOnClose(
                "Host=localhost;No Reset On Close=maybe"));
        }
    }

    // ── RLS hardening: Open() pool-checkout behavior ─────────

    public class OpenPoolCheckoutTest
    {
        [Fact]
        public void OpenOnNonNpgsqlMarksDirty()
        {
            // FakeConnection isn't Npgsql — Open() should mark dirty so
            // verify-on-checkout fires on the first command.
            var cache = new NativeCache();
            cache.SetConnected(true);
            var conn = new CachedConnection(new FakeConnection(), cache);

            Assert.False(conn.GucState.IsDirty);
            conn.Open();
            Assert.True(conn.GucState.IsDirty);
        }

        [Fact]
        public void OpenOnNonNpgsqlClearsPriorState()
        {
            // Prior session might have set state. Open() doesn't clear
            // it directly — the verify-on-checkout path will. But the
            // dirty flag must be set so the next command triggers verify.
            var cache = new NativeCache();
            cache.SetConnected(true);
            var conn = new CachedConnection(new FakeConnection(), cache);
            conn.GucState.ObserveSql("SET app.user_id = '42'");
            Assert.NotEqual(0L, conn.GucState.StateHash);

            conn.Open();
            Assert.True(conn.GucState.IsDirty);
        }
    }

    // ── RLS hardening: VerifyAndClearDirty (synchronous fallback) ─

    public class VerifyAndClearDirtyTest
    {
        // Helper: build a FakeConnection that responds to the
        // pg_settings verify query with a fixed (name, setting) row set.
        private static FakeConnection MakeInnerWithVerify(params (string name, string value)[] rows)
        {
            var inner = new FakeConnection();
            var rowArray = new object[rows.Length][];
            for (int i = 0; i < rows.Length; i++)
                rowArray[i] = new object[] { rows[i].name, rows[i].value };
            inner.ReaderBySql["SELECT name, setting FROM pg_settings WHERE source='session'"] =
                () => new FakeDataReader(rowArray, new[] { "name", "setting" });
            return inner;
        }

        [Fact]
        public void NotDirtyIsNoOp()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = MakeInnerWithVerify();
            var conn = new CachedConnection(inner, cache);

            // No MarkDirty — VerifyAndClearDirty should skip entirely.
            conn.VerifyAndClearDirty();
            Assert.DoesNotContain(
                "SELECT name, setting FROM pg_settings WHERE source='session'",
                inner.ExecutedSql);
        }

        [Fact]
        public void DirtyTriggersVerify()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = MakeInnerWithVerify(
                ("app.user_id", "42"),
                ("application_name", "myapp"));
            var conn = new CachedConnection(inner, cache);
            conn.GucState.MarkDirty();

            conn.VerifyAndClearDirty();

            Assert.Contains(
                "SELECT name, setting FROM pg_settings WHERE source='session'",
                inner.ExecutedSql);
            Assert.False(conn.GucState.IsDirty);
            // Only the unsafe `app.user_id` should contribute to the hash.
            var b = new ConnectionGucState();
            b.ObserveSql("SET app.user_id = '42'");
            Assert.Equal(b.StateHash, conn.GucState.StateHash);
        }

        [Fact]
        public void VerifyOnClosedConnectionIsNoop()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = MakeInnerWithVerify(("app.user_id", "42"));
            inner.ForceClosed = true;
            var conn = new CachedConnection(inner, cache);
            conn.GucState.MarkDirty();

            conn.VerifyAndClearDirty();

            // Closed connection — verify must skip without throwing.
            // Dirty stays set so the next checkout retries.
            Assert.True(conn.GucState.IsDirty);
        }

        [Fact]
        public void VerifyBeforeCommandReadsServerTruth()
        {
            // Open() marks dirty on non-Npgsql; the next ExecuteReader
            // must verify FIRST, then build the cache key from the
            // post-verify state hash.
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = MakeInnerWithVerify(("app.user_id", "alice"));
            inner.NextReader = new FakeDataReader(
                new[] { new object[] { "x" } }, new[] { "v" });
            var conn = new CachedConnection(inner, cache);
            conn.Open();
            Assert.True(conn.GucState.IsDirty);

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT v FROM accounts";
                cmd.ExecuteReader().Dispose();
            }

            Assert.False(conn.GucState.IsDirty);
            // After verify, state hash should reflect server-truth
            // (`app.user_id=alice`).
            var b = new ConnectionGucState();
            b.ObserveSql("SET app.user_id = 'alice'");
            Assert.Equal(b.StateHash, conn.GucState.StateHash);
        }
    }

    // ── RLS hardening: async post-call verify ───────────────

    public class AsyncPostCallVerifyTest
    {
        private static FakeConnection MakeInnerWithVerify(params (string name, string value)[] rows)
        {
            var inner = new FakeConnection();
            var rowArray = new object[rows.Length][];
            for (int i = 0; i < rows.Length; i++)
                rowArray[i] = new object[] { rows[i].name, rows[i].value };
            inner.ReaderBySql["SELECT name, setting FROM pg_settings WHERE source='session'"] =
                () => new FakeDataReader(rowArray, new[] { "name", "setting" });
            return inner;
        }

        // Spin until predicate is true or timeout elapses. Async verify
        // is fire-and-forget; tests need a small bounded wait.
        private static bool SpinUntil(Func<bool> predicate, int timeoutMs = 2000)
        {
            var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
            while (DateTime.UtcNow < deadline)
            {
                if (predicate()) return true;
                Thread.Sleep(10);
            }
            return predicate();
        }

        [Fact]
        public void FunctionCallSchedulesVerify()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            // The function call body might have set app.user_id; the
            // verify catches it.
            var inner = MakeInnerWithVerify(("app.user_id", "from-fn"));
            var conn = new CachedConnection(inner, cache);

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT my_func()";
                cmd.ExecuteReader().Dispose();
            }

            // Wait for the async verify to run.
            Assert.True(SpinUntil(() =>
            {
                lock (inner.ExecutedSql)
                    return inner.ExecutedSql.Contains(
                        "SELECT name, setting FROM pg_settings WHERE source='session'");
            }));

            // After verify, state reflects server truth.
            Assert.True(SpinUntil(() =>
            {
                var b = new ConnectionGucState();
                b.ObserveSql("SET app.user_id = 'from-fn'");
                return conn.GucState.StateHash == b.StateHash;
            }));
        }

        [Fact]
        public void PlainSelectDoesNotScheduleVerify()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = MakeInnerWithVerify(("app.user_id", "noop"));
            inner.NextReader = new FakeDataReader(
                new[] { new object[] { "x" } }, new[] { "v" });
            var conn = new CachedConnection(inner, cache);

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT v FROM accounts";
                cmd.ExecuteReader().Dispose();
            }

            // Give any spurious task a chance to run.
            Thread.Sleep(100);
            lock (inner.ExecutedSql)
                Assert.DoesNotContain(
                    "SELECT name, setting FROM pg_settings WHERE source='session'",
                    inner.ExecutedSql);
        }

        [Fact]
        public void CallStatementSchedulesVerify()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = MakeInnerWithVerify(("role", "elevated"));
            var conn = new CachedConnection(inner, cache);

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "CALL my_proc()";
                cmd.ExecuteNonQuery();
            }

            Assert.True(SpinUntil(() =>
            {
                lock (inner.ExecutedSql)
                    return inner.ExecutedSql.Contains(
                        "SELECT name, setting FROM pg_settings WHERE source='session'");
            }));
        }

        [Fact]
        public void DisposeCancelsInFlightVerify()
        {
            // Dispose must not throw, and the verify token must be
            // cancelled so the task exits cleanly.
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = MakeInnerWithVerify();
            var conn = new CachedConnection(inner, cache);

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT my_func()";
                cmd.ExecuteScalar();
            }
            conn.Dispose();
            // No assertion — just verifying we don't deadlock or throw.
            // The test passing means cancellation worked.
        }

        [Fact]
        public void VerifyFailureMarksDirty()
        {
            // Inner connection returns no row factory for pg_settings;
            // the default empty FakeDataReader has 0 columns and reading
            // GetString(0) on an empty result is fine, so we need to
            // force an exception. Closed-connection state is the
            // cleanest forcer.
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = new FakeConnection();
            inner.ForceClosed = true;
            var conn = new CachedConnection(inner, cache);

            // Manually invoke the async verify path so we can assert
            // on the resulting state without timing out.
            conn.ScheduleAsyncVerify();

            Assert.True(SpinUntil(() => conn.GucState.IsDirty));
        }

        [Fact]
        public void OpenAfterCloseRefreshesCancellationToken()
        {
            // After Close cancels the CTS, a subsequent Open must mint
            // a new CTS so post-call verifies still schedule.
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = MakeInnerWithVerify(("app.user_id", "after-reopen"));
            var conn = new CachedConnection(inner, cache);

            conn.Close();
            Assert.True(conn.VerifyCancellationToken.IsCancellationRequested);

            conn.Open();
            Assert.False(conn.VerifyCancellationToken.IsCancellationRequested);

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT my_func()";
                cmd.ExecuteScalar();
            }

            Assert.True(SpinUntil(() =>
            {
                lock (inner.ExecutedSql)
                    return inner.ExecutedSql.Contains(
                        "SELECT name, setting FROM pg_settings WHERE source='session'");
            }));
        }
    }

    // ── SET-actually-applied: defer state-hash mutation until success ──
    //
    // Wave 2 fix (`dotnet-set-actually-applied-2026-05-05`): the wrapper
    // observed SET commands optimistically and mutated the per-connection
    // GUC state hash before the inner DbCommand ran. If the command then
    // threw — e.g. the SET names a GUC that doesn't exist server-side, or
    // a multi-statement body has a syntax error past the SET — the
    // wrapper's observed state diverged from server-truth, leaking the
    // old state hash into subsequent cache lookups.
    //
    // Fix: SnapshotAndObserveSql captures the pre-observation hash + values
    // and applies the SETs in-place. CachedCommand wraps each inner execute
    // in try/catch; on exception it Restores the snapshot AND MarkDirty(),
    // so the next checkout reconciles via the existing pg_settings verify
    // path. (MarkDirty handles the multi-statement edge case where
    // Postgres applied a prefix of SETs before the failure.)

    public class SetActuallyAppliedTest : IDisposable
    {
        public SetActuallyAppliedTest() { NativeCache.Reset(); }
        public void Dispose() { NativeCache.Reset(); }

        // Spin until predicate is true or timeout elapses. Async verify
        // is fire-and-forget; tests need a small bounded wait. (Mirrors
        // the helper in AsyncPostCallVerifyTest — kept private here so
        // the SET-actually-applied tests stay self-contained.)
        private static bool SpinUntil(Func<bool> predicate, int timeoutMs = 2000)
        {
            var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
            while (DateTime.UtcNow < deadline)
            {
                if (predicate()) return true;
                Thread.Sleep(10);
            }
            return predicate();
        }

        // ── ExecuteNonQuery ──

        [Fact]
        public void NonQuerySetSuccessAppliesStateHash()
        {
            // Sanity check: the success path still mutates state. The
            // pending mutation isn't the snapshot's restore target —
            // the snapshot is only restored on exception.
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = new FakeConnection();
            var conn = new CachedConnection(inner, cache);

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SET app.user_id = '42'";
                cmd.ExecuteNonQuery();
            }

            Assert.NotEqual(0L, conn.GucState.StateHash);
            var b = new ConnectionGucState();
            b.ObserveSql("SET app.user_id = '42'");
            Assert.Equal(b.StateHash, conn.GucState.StateHash);
        }

        [Fact]
        public void NonQuerySetExceptionRevertsStateHash()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = new FakeConnection();
            inner.ThrowOnNextNonQuery = new InvalidOperationException("simulated DbException");
            var conn = new CachedConnection(inner, cache);

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SET app.user_id = '42'";
                Assert.Throws<InvalidOperationException>(() => cmd.ExecuteNonQuery());
            }

            // Pre-fix bug: state hash was 0 → mutated to non-zero
            // unconditionally, so the next SELECT keyed against the
            // wrong hash. Post-fix: snapshot restored to 0.
            Assert.Equal(0L, conn.GucState.StateHash);
            // Marked dirty so the next checkout reconciles via verify
            // (covers the multi-statement-prefix-applied edge case).
            Assert.True(conn.GucState.IsDirty);
        }

        [Fact]
        public void NonQueryExceptionDoesNotAddSpuriousStateHash()
        {
            // No SET in the SQL — exception path should still not
            // perturb the (already-zero) hash. Snapshot+restore is a
            // no-op for non-observing SQL; only MarkDirty fires.
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = new FakeConnection();
            inner.ThrowOnNextNonQuery = new InvalidOperationException("boom");
            var conn = new CachedConnection(inner, cache);

            Assert.Equal(0L, conn.GucState.StateHash);
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "INSERT INTO orders VALUES (1)";
                Assert.Throws<InvalidOperationException>(() => cmd.ExecuteNonQuery());
            }
            Assert.Equal(0L, conn.GucState.StateHash);
        }

        // ── ExecuteScalar ──

        [Fact]
        public void ScalarSetExceptionRevertsStateHash()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = new FakeConnection();
            inner.ThrowOnNextScalar = new InvalidOperationException("simulated");
            var conn = new CachedConnection(inner, cache);

            // Pre-perturb to a known non-zero hash so we can detect a
            // diverged state on the exception path. The success-then-
            // failure sequence is the realistic one: prior SETs already
            // landed, then a new SET fails.
            conn.GucState.ObserveSql("SET role = 'admin'");
            var preHash = conn.GucState.StateHash;
            Assert.NotEqual(0L, preHash);

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SET app.user_id = '42'";
                Assert.Throws<InvalidOperationException>(() => cmd.ExecuteScalar());
            }

            // Snapshot restored to the pre-call hash (role='admin' still
            // applies, app.user_id='42' does NOT).
            Assert.Equal(preHash, conn.GucState.StateHash);
            Assert.True(conn.GucState.IsDirty);
        }

        // ── ExecuteReader / ExecuteDbDataReader ──

        [Fact]
        public void ReaderSetExceptionRevertsStateHash()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = new FakeConnection();
            inner.ThrowOnNextReader = new InvalidOperationException("simulated");
            var conn = new CachedConnection(inner, cache);

            using (var cmd = conn.CreateCommand())
            {
                // Multi-statement that observation will parse a SET
                // out of, but ExecuteReader throws — ensure no diverge.
                cmd.CommandText = "SET app.user_id = '42'; SELECT 1";
                Assert.Throws<InvalidOperationException>(() => cmd.ExecuteReader());
            }

            Assert.Equal(0L, conn.GucState.StateHash);
            Assert.True(conn.GucState.IsDirty);
        }

        [Fact]
        public void ReaderExceptionInTransactionRevertsState()
        {
            // In-transaction reader path bypasses the cache but still
            // observes SET; same revert contract applies.
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = new FakeConnection();
            var conn = new CachedConnection(inner, cache);

            // Enter transaction the same way the existing test does.
            inner.NextTransaction = new FakeTransaction();
            using (var tx = conn.BeginTransaction())
            {
                inner.ThrowOnNextReader = new InvalidOperationException("simulated");
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SET app.user_id = '99'";
                    Assert.Throws<InvalidOperationException>(() => cmd.ExecuteReader());
                }
                tx.Rollback();
            }

            Assert.Equal(0L, conn.GucState.StateHash);
            Assert.True(conn.GucState.IsDirty);
        }

        [Fact]
        public void ReaderExceptionOnWritePathRevertsState()
        {
            // Write-detection path: observation runs, invalidation runs,
            // then ExecuteReader is invoked. If it throws, GUC state must
            // revert (and the eager invalidation already happened — that
            // doesn't unwind, which is fine: invalidation is conservative).
            var cache = new NativeCache();
            cache.SetConnected(true);
            cache.Put("SELECT * FROM orders", null,
                new[] { new object[] { 1 } }, new[] { "id" });

            var inner = new FakeConnection();
            inner.ThrowOnNextReader = new InvalidOperationException("simulated");
            var conn = new CachedConnection(inner, cache);

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SET app.user_id = '42'; INSERT INTO orders VALUES (1)";
                Assert.Throws<InvalidOperationException>(() => cmd.ExecuteReader());
            }

            // GUC state reverted.
            Assert.Equal(0L, conn.GucState.StateHash);
            Assert.True(conn.GucState.IsDirty);
            // Invalidation already happened — orders cache entry is gone.
            // (We deliberately don't try to undo eager invalidation: a
            // failed write may have already taken effect on the server,
            // and serving stale rows is the worse failure mode.)
            Assert.Equal(0, cache.Size);
        }

        // ── Multi-statement edge cases ──

        [Fact]
        public void MultiStatementExceptionMarksDirtyForReconciliation()
        {
            // Pg semantics: in a multi-statement body without an
            // explicit BEGIN, pre-error statements DO commit. The
            // wrapper can't tell which prefix landed, so it MarkDirty's
            // and lets verify-on-checkout reconcile from pg_settings
            // on the next command.
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = new FakeConnection();
            inner.ThrowOnNextNonQuery = new InvalidOperationException("syntax error past SET");
            var conn = new CachedConnection(inner, cache);

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SET app.user_id = '42'; SET role = 'admin'; INSERT INTO orders garbage";
                Assert.Throws<InvalidOperationException>(() => cmd.ExecuteNonQuery());
            }

            // The optimistic hash mutation is rolled back. Server may
            // have applied 0, 1, or 2 of the SETs — we don't know.
            // MarkDirty triggers verify-on-checkout to reconcile.
            Assert.Equal(0L, conn.GucState.StateHash);
            Assert.True(conn.GucState.IsDirty);
        }

        [Fact]
        public void DirtyTriggersVerifyOnNextCommand()
        {
            // End-to-end: SET fails → MarkDirty → next ExecuteReader
            // runs verify against pg_settings before building the
            // cache key. Server reports `app.user_id = 'truth'`; the
            // wrapper picks that up.
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = new FakeConnection();
            // Verify response: server actually has app.user_id='truth'.
            inner.ReaderBySql["SELECT name, setting FROM pg_settings WHERE source='session'"] =
                () => new FakeDataReader(
                    new[] { new object[] { "app.user_id", "truth" } },
                    new[] { "name", "setting" });
            var conn = new CachedConnection(inner, cache);

            // First command throws.
            inner.ThrowOnNextNonQuery = new InvalidOperationException("simulated");
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SET app.user_id = 'attempt'";
                Assert.Throws<InvalidOperationException>(() => cmd.ExecuteNonQuery());
            }
            Assert.True(conn.GucState.IsDirty);

            // Second command — verify-on-checkout fires before exec,
            // pulls truth from pg_settings.
            inner.NextReader = new FakeDataReader(
                new[] { new object[] { 1 } }, new[] { "v" });
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT v FROM accounts";
                cmd.ExecuteReader().Dispose();
            }

            Assert.False(conn.GucState.IsDirty);
            var b = new ConnectionGucState();
            b.ObserveSql("SET app.user_id = 'truth'");
            Assert.Equal(b.StateHash, conn.GucState.StateHash);
        }

        // ── Tx-flag bookkeeping integration (Wave 1 + Wave 2) ──

        [Fact]
        public void BeginThenExceptionRevertsTxFlag()
        {
            // Multi-segment body opens a transaction then throws — the
            // wrapper's InTransaction flag must NOT be left flipped to
            // true. Pg won't have entered the tx (the whole body
            // failed in this simulation), and leaving InTransaction=true
            // would permanently bypass the cache for this connection.
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = new FakeConnection();
            inner.ThrowOnNextNonQuery = new InvalidOperationException("simulated");
            var conn = new CachedConnection(inner, cache);

            Assert.False(conn.InTransaction);
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "BEGIN; INSERT INTO orders VALUES (1)";
                Assert.Throws<InvalidOperationException>(() => cmd.ExecuteNonQuery());
            }

            // Pre-fix: would be true (DetectTxTransition saw BEGIN; flipped
            // optimistically; throw didn't undo). Post-fix: reverted.
            Assert.False(conn.InTransaction);
        }

        // ── Async path coverage ──
        //
        // DbCommand's default async overrides delegate to the sync
        // Execute* methods (Task.FromResult / Task.Run wrappers in the
        // base class). Wrapping the sync paths in try/catch therefore
        // protects the async paths automatically — these tests confirm
        // it end-to-end without overriding the async methods.

        [Fact]
        public async System.Threading.Tasks.Task ExecuteNonQueryAsyncThrowRevertsStateHash()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = new FakeConnection();
            inner.ThrowOnNextNonQuery = new InvalidOperationException("simulated");
            var conn = new CachedConnection(inner, cache);

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SET app.user_id = 'async-attempt'";
                await Assert.ThrowsAsync<InvalidOperationException>(
                    async () => await cmd.ExecuteNonQueryAsync());
            }

            Assert.Equal(0L, conn.GucState.StateHash);
            Assert.True(conn.GucState.IsDirty);
        }

        [Fact]
        public async System.Threading.Tasks.Task ExecuteReaderAsyncThrowRevertsStateHash()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = new FakeConnection();
            inner.ThrowOnNextReader = new InvalidOperationException("simulated");
            var conn = new CachedConnection(inner, cache);

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SET app.user_id = 'async-r'; SELECT 1";
                await Assert.ThrowsAsync<InvalidOperationException>(
                    async () => { using var r = await cmd.ExecuteReaderAsync(); });
            }

            Assert.Equal(0L, conn.GucState.StateHash);
            Assert.True(conn.GucState.IsDirty);
        }

        [Fact]
        public async System.Threading.Tasks.Task ExecuteScalarAsyncThrowRevertsStateHash()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = new FakeConnection();
            inner.ThrowOnNextScalar = new InvalidOperationException("simulated");
            var conn = new CachedConnection(inner, cache);

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SET app.user_id = 'async-s'";
                await Assert.ThrowsAsync<InvalidOperationException>(
                    async () => await cmd.ExecuteScalarAsync());
            }

            Assert.Equal(0L, conn.GucState.StateHash);
            Assert.True(conn.GucState.IsDirty);
        }

        // ── Pure ConnectionGucState contract ──

        [Fact]
        public void SnapshotRestoreReturnsToPriorState()
        {
            // Direct test of the snapshot mechanism, no DbCommand path.
            var s = new ConnectionGucState();
            s.ObserveSql("SET role = 'baseline'");
            var pre = s.StateHash;
            Assert.NotEqual(0L, pre);

            var snap = s.SnapshotAndObserveSql("SET app.user_id = 'temp'");
            Assert.NotEqual(pre, s.StateHash);  // optimistic apply

            snap.Restore();
            Assert.Equal(pre, s.StateHash);     // back to baseline
        }

        [Fact]
        public void DefaultSnapshotRestoreIsNoop()
        {
            // A default-constructed snapshot's Restore must not throw —
            // the wrapper's RevertOptimisticState relies on this for
            // the non-observing-SQL path.
            var snap = default(GucStateSnapshot);
            snap.Restore();  // no exception, no owner to mutate.
        }

        [Fact]
        public void SnapshotRestoreClearsOptimisticReset()
        {
            // RESET ALL on a populated state empties the map. If the
            // command throws, restore must rebuild the original map.
            var s = new ConnectionGucState();
            s.ObserveSql("SET app.user_id = '1'");
            s.ObserveSql("SET role = 'admin'");
            var pre = s.StateHash;

            var snap = s.SnapshotAndObserveSql("RESET ALL");
            Assert.Equal(0L, s.StateHash);  // optimistically empty.

            snap.Restore();
            Assert.Equal(pre, s.StateHash);  // both unsafe GUCs back.
        }
    }

    // ── CachedCommand.DbConnection setter ─────────────────────

    public class CachedCommandConnectionSetterTest
    {
        [Fact]
        public void SetConnectionThrowsNotSupported()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = new FakeConnection();
            var conn = new CachedConnection(inner, cache);
            var cmd = conn.CreateCommand();

            // The protected DbConnection setter is exposed via the public Connection property
            Assert.Throws<NotSupportedException>(() =>
            {
                cmd.Connection = new FakeConnection();
            });
        }

        [Fact]
        public void GetConnectionReturnsCachedConnection()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = new FakeConnection();
            var conn = new CachedConnection(inner, cache);
            var cmd = conn.CreateCommand();

            Assert.Same(conn, cmd.Connection);
        }
    }

    // ── Always-on dml_seq bump (replaces smart-auto-enable) ──
    //
    // The trigger-internal-SET safety net is now an unconditional
    // ConnectionGucState.BumpDmlSeq() call after every observed DML.
    // The dml_seq counter mixes into the cache-key hash, so the next
    // L1 lookup on this connection misses and routes through to the
    // proxy (which carries authoritative session state). No SQL probe,
    // no async verify, no per-upstream cache — the bump is free and
    // always correct.
    //
    // AggressiveVerifyMode preserved as a knob:
    //   * Auto (default) / On → bump enabled.
    //   * Off → opt out; the wrapper emits a one-time stderr warning at
    //     construction so operators don't silently regress.
    //   * License-claim aggressive_verify_active=true → forces On unless
    //     the explicit Off override wins.

    public class AggressiveVerifyResolutionTest : IDisposable
    {
        public AggressiveVerifyResolutionTest() { NativeCache.Reset(); }
        public void Dispose() { NativeCache.Reset(); }

        [Fact]
        public void AutoIsEnabled()
        {
            var cache = new NativeCache();
            var conn = new CachedConnection(new FakeConnection(), cache, AggressiveVerifyMode.Auto, null);
            Assert.True(conn.IsAggressiveVerifyEnabled());
        }

        [Fact]
        public void OnIsEnabled()
        {
            var cache = new NativeCache();
            var conn = new CachedConnection(new FakeConnection(), cache, AggressiveVerifyMode.On, null);
            Assert.True(conn.IsAggressiveVerifyEnabled());
        }

        [Fact]
        public void OffIsDisabled()
        {
            var cache = new NativeCache();
            var conn = new CachedConnection(new FakeConnection(), cache, AggressiveVerifyMode.Off, null);
            Assert.False(conn.IsAggressiveVerifyEnabled());
        }

        [Fact]
        public void LicenseClaimDoesNotChangeAutoOutcome()
        {
            // Auto is already enabled — a license claim is a no-op for
            // the resolved bool. Kept as a test so the API surface
            // doesn't silently regress (HQ payloads still flow in).
            var cache = new NativeCache();
            var payload = new Dictionary<string, object> { { "aggressive_verify_active", true } };
            var conn = new CachedConnection(new FakeConnection(), cache, AggressiveVerifyMode.Auto, payload);
            Assert.True(conn.IsAggressiveVerifyEnabled());
        }

        [Fact]
        public void OffOverrideBeatsLicenseClaim()
        {
            // Operator explicit Off wins over HQ "force on" — auth is
            // auth; the wrapper-side knob is the operator's contract.
            var cache = new NativeCache();
            var payload = new Dictionary<string, object> { { "aggressive_verify_active", true } };
            var conn = new CachedConnection(new FakeConnection(), cache, AggressiveVerifyMode.Off, payload);
            Assert.False(conn.IsAggressiveVerifyEnabled());
        }

        [Fact]
        public void DefaultConstructorIsAutoAndEnabled()
        {
            var cache = new NativeCache();
            var conn = new CachedConnection(new FakeConnection(), cache);
            Assert.Equal(AggressiveVerifyMode.Auto, conn.AggressiveVerifyModeValue);
            Assert.Null(conn.LicensePayloadValue);
            Assert.True(conn.IsAggressiveVerifyEnabled());
        }

        [Fact]
        public void LicenseClaimPermissiveTruthy()
        {
            // Accept "true" / "1" / "yes" — license loaders may vary.
            var payloads = new[]
            {
                new Dictionary<string, object> { { "aggressive_verify_active", "true" } },
                new Dictionary<string, object> { { "aggressive_verify_active", "1" } },
                new Dictionary<string, object> { { "aggressive_verify_active", 1L } },
            };
            foreach (var payload in payloads)
            {
                Assert.True(AggressiveVerify.LicenseClaimsActive(payload));
            }
            Assert.False(AggressiveVerify.LicenseClaimsActive(null));
            Assert.False(AggressiveVerify.LicenseClaimsActive(new Dictionary<string, object>()));
            Assert.False(AggressiveVerify.LicenseClaimsActive(
                new Dictionary<string, object> { { "aggressive_verify_active", false } }));
        }

        [Fact]
        public void ParseLicensePayloadRoundtrip()
        {
            var payload = GoldLapel.ParseLicensePayload(
                "{\"aggressive_verify_active\": true, \"plan\": \"esquire\"}");
            Assert.NotNull(payload);
            Assert.True(AggressiveVerify.LicenseClaimsActive(payload));
            Assert.Equal("esquire", payload["plan"]);
        }

        [Fact]
        public void ParseLicensePayloadGracefullyHandlesGarbage()
        {
            Assert.Null(GoldLapel.ParseLicensePayload(null));
            Assert.Null(GoldLapel.ParseLicensePayload(""));
            Assert.Null(GoldLapel.ParseLicensePayload("not json"));
            Assert.Null(GoldLapel.ParseLicensePayload("[1, 2, 3]"));
        }

        [Fact]
        public void OffEmitsStderrWarning()
        {
            // One-time stderr warning gives operators a chance to notice
            // that they've opted out of the safety net. The warning is
            // process-wide one-shot — captured here by swapping
            // Console.Error and asserting the first Off-construction
            // emits, then constructing again is a no-op.
            var prevErr = Console.Error;
            var sw1 = new System.IO.StringWriter();
            try
            {
                Console.SetError(sw1);
                var cache = new NativeCache();
                ResetOffWarningForTest();
                var conn1 = new CachedConnection(new FakeConnection(), cache, AggressiveVerifyMode.Off, null);
                Assert.Contains("AggressiveVerifyMode.Off", sw1.ToString());

                var sw2 = new System.IO.StringWriter();
                Console.SetError(sw2);
                var conn2 = new CachedConnection(new FakeConnection(), cache, AggressiveVerifyMode.Off, null);
                // Already warned once; second construction is silent.
                Assert.Equal("", sw2.ToString());
            }
            finally
            {
                Console.SetError(prevErr);
            }
        }

        // Reflection helper: reset the static one-shot warning flag so
        // each [Fact] that exercises the warning path starts from a
        // clean slate. The field is private; reflection is the cleanest
        // option that doesn't add a product-code test hook for a
        // single-line invariant.
        private static void ResetOffWarningForTest()
        {
            var field = typeof(CachedConnection).GetField(
                "_offWarningEmitted",
                System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic);
            field.SetValue(null, 0);
        }
    }

    // ── Always-on dml_seq bump: post-DML cache invalidation ──
    //
    // Replaces the earlier post-DML async pg_settings verify pattern.
    // After every observed DML the wrapper bumps a per-connection
    // monotonic counter that mixes into the L1 cache key. The next read
    // on the same connection misses the L1 slot and routes through to
    // the proxy — which carries authoritative session state.

    public class AggressiveVerifyPostDmlTest : IDisposable
    {
        public AggressiveVerifyPostDmlTest() { NativeCache.Reset(); }
        public void Dispose() { NativeCache.Reset(); }

        private static FakeConnection MakeInnerWithVerify(params (string name, string value)[] rows)
        {
            var inner = new FakeConnection();
            var rowArray = new object[rows.Length][];
            for (int i = 0; i < rows.Length; i++)
                rowArray[i] = new object[] { rows[i].name, rows[i].value };
            inner.ReaderBySql["SELECT name, setting FROM pg_settings WHERE source='session'"] =
                () => new FakeDataReader(rowArray, new[] { "name", "setting" });
            return inner;
        }

        // Spin until predicate is true or timeout — async verify is
        // fire-and-forget (still used for the function-call path).
        private static bool SpinUntil(Func<bool> predicate, int timeoutMs = 2000)
        {
            var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
            while (DateTime.UtcNow < deadline)
            {
                if (predicate()) return true;
                Thread.Sleep(10);
            }
            return predicate();
        }

        private static bool VerifyRan(FakeConnection inner)
        {
            lock (inner.ExecutedSql)
                return inner.ExecutedSql.Contains(
                    "SELECT name, setting FROM pg_settings WHERE source='session'");
        }

        [Fact]
        public void InsertBumpsDmlSeqWhenOn()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = new FakeConnection();
            var conn = new CachedConnection(inner, cache, AggressiveVerifyMode.On, null);

            Assert.Equal(0L, conn.GucState.DmlSeq);
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "INSERT INTO orders (id) VALUES (1)";
                cmd.ExecuteNonQuery();
            }
            Assert.Equal(1L, conn.GucState.DmlSeq);
        }

        [Fact]
        public void UpdateBumpsDmlSeqWhenOn()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            var conn = new CachedConnection(new FakeConnection(), cache, AggressiveVerifyMode.On, null);
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "UPDATE orders SET total = 1 WHERE id = 2";
                cmd.ExecuteNonQuery();
            }
            Assert.Equal(1L, conn.GucState.DmlSeq);
        }

        [Fact]
        public void DeleteBumpsDmlSeqWhenOn()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            var conn = new CachedConnection(new FakeConnection(), cache, AggressiveVerifyMode.On, null);
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "DELETE FROM orders WHERE id = 3";
                cmd.ExecuteNonQuery();
            }
            Assert.Equal(1L, conn.GucState.DmlSeq);
        }

        [Fact]
        public void TruncateBumpsDmlSeqWhenOn()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            var conn = new CachedConnection(new FakeConnection(), cache, AggressiveVerifyMode.On, null);
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "TRUNCATE TABLE orders";
                cmd.ExecuteNonQuery();
            }
            Assert.Equal(1L, conn.GucState.DmlSeq);
        }

        [Fact]
        public void MergeBumpsDmlSeqWhenOn()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            var conn = new CachedConnection(new FakeConnection(), cache, AggressiveVerifyMode.On, null);
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "MERGE INTO orders USING staging ON orders.id = staging.id WHEN MATCHED THEN UPDATE SET total = staging.total";
                cmd.ExecuteNonQuery();
            }
            Assert.Equal(1L, conn.GucState.DmlSeq);
        }

        [Fact]
        public void DmlDoesNotBumpWhenOff()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            var conn = new CachedConnection(new FakeConnection(), cache, AggressiveVerifyMode.Off, null);
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "INSERT INTO orders (id) VALUES (1)";
                cmd.ExecuteNonQuery();
            }
            Assert.Equal(0L, conn.GucState.DmlSeq);
        }

        [Fact]
        public void PlainSelectDoesNotBumpEvenWhenOn()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = new FakeConnection();
            inner.NextReader = new FakeDataReader(new[] { new object[] { "x" } }, new[] { "v" });
            var conn = new CachedConnection(inner, cache, AggressiveVerifyMode.On, null);
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT v FROM accounts WHERE id = 1";
                cmd.ExecuteReader().Dispose();
            }
            Assert.Equal(0L, conn.GucState.DmlSeq);
        }

        [Fact]
        public void DmlInExecuteScalarBumpsWhenOn()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            var conn = new CachedConnection(new FakeConnection(), cache, AggressiveVerifyMode.On, null);
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "DELETE FROM orders WHERE id = 1";
                cmd.ExecuteScalar();
            }
            Assert.Equal(1L, conn.GucState.DmlSeq);
        }

        [Fact]
        public void DmlInExecuteReaderBumpsWhenOn()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = new FakeConnection();
            inner.NextReader = new FakeDataReader(new object[0][], new string[0]);
            var conn = new CachedConnection(inner, cache, AggressiveVerifyMode.On, null);
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "INSERT INTO orders (id) VALUES (1) RETURNING id";
                cmd.ExecuteReader().Dispose();
            }
            Assert.Equal(1L, conn.GucState.DmlSeq);
        }

        [Fact]
        public void FunctionCallStillSchedulesAsyncVerify()
        {
            // The function-call → async pg_settings verify path is
            // independent of aggressive-verify and still active. It
            // catches SETs inside server-side function bodies that
            // wouldn't be observable on the wire even with dml_seq.
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = MakeInnerWithVerify();
            var conn = new CachedConnection(inner, cache, AggressiveVerifyMode.Off, null);
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT my_func()";
                cmd.ExecuteReader().Dispose();
            }
            Assert.True(SpinUntil(() => VerifyRan(inner)));
        }

        [Fact]
        public void MultiSegmentSetThenInsertBumpsWhenOn()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            var conn = new CachedConnection(new FakeConnection(), cache, AggressiveVerifyMode.On, null);
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SET app.user_id = '42'; INSERT INTO orders (id) VALUES (1)";
                cmd.ExecuteNonQuery();
            }
            Assert.Equal(1L, conn.GucState.DmlSeq);
        }

        [Fact]
        public void DmlDoesNotScheduleAsyncVerifyAnyMore()
        {
            // Regression guard for the smart-auto-enable removal: post-DML
            // SHOULD NOT issue a pg_settings async verify. The dml_seq
            // bump replaces that round-trip entirely.
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = MakeInnerWithVerify();
            var conn = new CachedConnection(inner, cache, AggressiveVerifyMode.On, null);
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "INSERT INTO orders (id) VALUES (1)";
                cmd.ExecuteNonQuery();
            }
            // Give any spurious task a chance to run.
            Thread.Sleep(100);
            Assert.False(VerifyRan(inner));
        }

        [Fact]
        public void SequentialDmlBumpsDmlSeqEachTime()
        {
            var cache = new NativeCache();
            cache.SetConnected(true);
            var conn = new CachedConnection(new FakeConnection(), cache, AggressiveVerifyMode.On, null);
            for (int i = 1; i <= 5; i++)
            {
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "INSERT INTO orders (id) VALUES (1)";
                    cmd.ExecuteNonQuery();
                }
                Assert.Equal((long)i, conn.GucState.DmlSeq);
            }
        }

        [Fact]
        public void DmlBumpChangesStateHashAndCacheKey()
        {
            // The whole point of the bump: after a DML, the next read
            // keys against a different state-hash bucket and misses any
            // previously-cached rowset for the same SQL. Without
            // dml_seq mixed into the hash, the post-DML SELECT would
            // hit the pre-DML cache entry and serve stale rows.
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = new FakeConnection();

            var conn = new CachedConnection(inner, cache, AggressiveVerifyMode.On, null);
            // Warm the cache with a SELECT — this populates a slot keyed
            // by stateHash=0 (fresh connection, no DML yet).
            inner.NextReader = new FakeDataReader(
                new[] { new object[] { "pre-dml" } }, new[] { "v" });
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT v FROM accounts WHERE id = 1";
                cmd.ExecuteReader().Dispose();
            }

            var preHash = conn.GucState.StateHash;
            Assert.Equal(0L, preHash);

            // DML on an unrelated table — would NOT invalidate the
            // SELECT slot through table-write invalidation (different
            // table). But the dml_seq bump rolls the state-hash key
            // past the pre-DML slot, so a subsequent SELECT misses.
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "INSERT INTO unrelated VALUES (1)";
                cmd.ExecuteNonQuery();
            }

            Assert.NotEqual(preHash, conn.GucState.StateHash);

            // Re-run the SELECT — it should now hit the inner connection
            // again (cache miss) and produce a fresh result. We swap
            // the inner's next reader so the new rowset is
            // distinguishable.
            inner.NextReader = new FakeDataReader(
                new[] { new object[] { "post-dml" } }, new[] { "v" });
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT v FROM accounts WHERE id = 1";
                using var r = cmd.ExecuteReader();
                Assert.True(r.Read());
                Assert.Equal("post-dml", r.GetString(0));
            }
        }

        [Fact]
        public void DmlBumpDoesNotFireWhenOff()
        {
            // Off opts out of the bump entirely. The pre-DML cache slot
            // remains the hit target after a DML — which is the
            // operator's explicit choice and what they were warned
            // about.
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = new FakeConnection();
            inner.NextReader = new FakeDataReader(
                new[] { new object[] { "pre-dml" } }, new[] { "v" });
            var conn = new CachedConnection(inner, cache, AggressiveVerifyMode.Off, null);
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT v FROM accounts WHERE id = 1";
                cmd.ExecuteReader().Dispose();
            }
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "INSERT INTO unrelated VALUES (1)";
                cmd.ExecuteNonQuery();
            }
            Assert.Equal(0L, conn.GucState.StateHash);
            // Second SELECT hits the cached row. Inner reader factory
            // is NOT replaced — proving the cache slot survived.
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT v FROM accounts WHERE id = 1";
                using var r = cmd.ExecuteReader();
                Assert.True(r.Read());
                Assert.Equal("pre-dml", r.GetString(0));
            }
        }
    }

    // ── Dirty flag bypasses the L1 cache lookup ──────────────
    //
    // If the wrapper's view of GUC state may be stale (verify failed,
    // pool reset semantics unknown, etc.), L1 cache lookups must bypass
    // entirely so the user's read routes to the proxy which has
    // authoritative session state. Both the cache GET and the current
    // command's potential cache PUT are skipped.

    public class DirtyBypassTest : IDisposable
    {
        public DirtyBypassTest() { NativeCache.Reset(); }
        public void Dispose() { NativeCache.Reset(); }

        // Helper: seed pg_settings so the verify query throws, leaving
        // dirty set after VerifyAndClearDirty returns. Throwing is the
        // cleanest way to make the verify path fail without ForceClosed
        // (which also breaks ExecuteReader on the user's command).
        private static void SeedFailingVerify(FakeConnection inner)
        {
            inner.ReaderBySql["SELECT name, setting FROM pg_settings WHERE source='session'"] =
                () => throw new InvalidOperationException("simulated verify failure");
        }

        [Fact]
        public void DirtyCacheLookupBypasses()
        {
            // Pre-populate the cache. Mark dirty. Stub the verify query
            // to throw so VerifyAndClearDirty fails and dirty stays set.
            // The next SELECT must route to the inner connection
            // instead of hitting the cache.
            var cache = new NativeCache();
            cache.SetConnected(true);
            // Pre-warmed slot keyed by stateHash=0.
            cache.Put("SELECT v FROM accounts", null,
                new[] { new object[] { "cached" } }, new[] { "v" });

            var inner = new FakeConnection();
            SeedFailingVerify(inner);
            inner.NextReader = new FakeDataReader(
                new[] { new object[] { "from-inner" } }, new[] { "v" });
            var conn = new CachedConnection(inner, cache);
            conn.GucState.MarkDirty();

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT v FROM accounts";
                using var r = cmd.ExecuteReader();
                Assert.True(r.Read());
                Assert.Equal("from-inner", r.GetString(0));
            }
            // Dirty still set — verify failed.
            Assert.True(conn.GucState.IsDirty);
        }

        [Fact]
        public void DirtyCachePutSuppressed()
        {
            // When dirty, the bypass path runs the inner reader but
            // must NOT cache the result — caching a rowset keyed by
            // stale state would poison subsequent reads.
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = new FakeConnection();
            SeedFailingVerify(inner);
            inner.NextReader = new FakeDataReader(
                new[] { new object[] { "poison" } }, new[] { "v" });
            var conn = new CachedConnection(inner, cache);
            conn.GucState.MarkDirty();

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT v FROM accounts";
                cmd.ExecuteReader().Dispose();
            }
            // Cache wasn't populated by this command — Size still 0.
            Assert.Equal(0, cache.Size);
        }

        [Fact]
        public void DirtyAfterFailedSetBypassesCache()
        {
            // End-to-end: a failed SET marks dirty; the very next
            // SELECT bypasses cache instead of serving a possibly-
            // stale slot. Combines RevertOptimisticState + dirty
            // bypass.
            var cache = new NativeCache();
            cache.SetConnected(true);
            cache.Put("SELECT v FROM accounts", null,
                new[] { new object[] { "cached" } }, new[] { "v" });

            var inner = new FakeConnection();
            SeedFailingVerify(inner);  // verify after the failed SET fails too.
            // First command throws — sets dirty, leaves cache untouched.
            inner.ThrowOnNextNonQuery = new InvalidOperationException("simulated");
            var conn = new CachedConnection(inner, cache);
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SET app.user_id = '42'";
                Assert.Throws<InvalidOperationException>(() => cmd.ExecuteNonQuery());
            }
            Assert.True(conn.GucState.IsDirty);

            inner.NextReader = new FakeDataReader(
                new[] { new object[] { "from-inner" } }, new[] { "v" });
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT v FROM accounts";
                using var r = cmd.ExecuteReader();
                Assert.True(r.Read());
                Assert.Equal("from-inner", r.GetString(0));
            }
        }

        [Fact]
        public void NotDirtyHitsCacheNormally()
        {
            // Control: with dirty unset, cache lookups proceed as
            // normal. Required so the bypass test above isn't a false
            // positive (e.g. cache.Get returning null for some other
            // reason).
            var cache = new NativeCache();
            cache.SetConnected(true);
            cache.Put("SELECT v FROM accounts", null,
                new[] { new object[] { "cached" } }, new[] { "v" });

            var inner = new FakeConnection();
            // Inner reader would return "from-inner" if reached.
            inner.NextReader = new FakeDataReader(
                new[] { new object[] { "from-inner" } }, new[] { "v" });
            var conn = new CachedConnection(inner, cache);
            Assert.False(conn.GucState.IsDirty);

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT v FROM accounts";
                using var r = cmd.ExecuteReader();
                Assert.True(r.Read());
                Assert.Equal("cached", r.GetString(0));
            }
        }

        [Fact]
        public void VerifySuccessClearsDirtyAndAllowsCacheHit()
        {
            // After dirty, the verify path runs against pg_settings,
            // succeeds, clears the flag, and the next read can hit
            // the cache (assuming the verified state matches the
            // cached entry's state hash).
            var cache = new NativeCache();
            cache.SetConnected(true);
            cache.Put("SELECT v FROM accounts", null,
                new[] { new object[] { "cached" } }, new[] { "v" });

            var inner = new FakeConnection();
            // pg_settings returns empty (no unsafe GUCs set) — verified
            // state hash is 0, which matches the cached slot.
            inner.ReaderBySql["SELECT name, setting FROM pg_settings WHERE source='session'"] =
                () => new FakeDataReader(new object[0][], new[] { "name", "setting" });

            var conn = new CachedConnection(inner, cache);
            conn.GucState.MarkDirty();
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT v FROM accounts";
                using var r = cmd.ExecuteReader();
                Assert.True(r.Read());
                Assert.Equal("cached", r.GetString(0));
            }
            Assert.False(conn.GucState.IsDirty);
        }
    }

    // ── Serialize user queries behind in-flight async verify ──
    //
    // VerifyAndClearDirty acquires the per-connection _verifyGate
    // semaphore with a bounded blocking wait. If an async verify is
    // running (e.g. scheduled after a function call), the user's next
    // command must wait for the gate before issuing its own verify
    // (or running against the inner DbCommand) — DbCommand is not
    // thread-safe and the verify path uses the inner connection.

    public class VerifyGateSerializationTest : IDisposable
    {
        public VerifyGateSerializationTest() { NativeCache.Reset(); }
        public void Dispose() { NativeCache.Reset(); }

        [Fact]
        public void UserCommandBlocksUntilAsyncVerifyReleasesGate()
        {
            // Reach into the SemaphoreSlim that guards verify and hold
            // it from the test thread. Then mark dirty and call
            // VerifyAndClearDirty on a worker; the worker must block
            // until we Release(). With Wait(0) (pre-fix), the worker
            // would have skipped past immediately.
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = new FakeConnection();
            inner.ReaderBySql["SELECT name, setting FROM pg_settings WHERE source='session'"] =
                () => new FakeDataReader(new object[0][], new[] { "name", "setting" });

            var conn = new CachedConnection(inner, cache);
            conn.GucState.MarkDirty();

            var gateField = typeof(CachedConnection).GetField(
                "_verifyGate",
                System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
            var gate = (SemaphoreSlim)gateField.GetValue(conn);
            Assert.True(gate.Wait(0));  // hold the gate from the test thread.

            try
            {
                var verifyStarted = new ManualResetEventSlim(false);
                var verifyCompleted = new ManualResetEventSlim(false);
                var worker = new Thread(() =>
                {
                    verifyStarted.Set();
                    conn.VerifyAndClearDirty();
                    verifyCompleted.Set();
                });
                worker.IsBackground = true;
                worker.Start();

                // The worker has spun up. Give it ~250ms — it should
                // STILL be blocked on the gate (we hold it).
                Assert.True(verifyStarted.Wait(2000));
                Assert.False(verifyCompleted.Wait(250));
                // Dirty still set; verify hasn't reconciled yet.
                Assert.True(conn.GucState.IsDirty);
            }
            finally
            {
                gate.Release();
            }

            // After release, the worker proceeds and clears dirty.
            Assert.True(SpinUntil(() => !conn.GucState.IsDirty, 2000));
        }

        [Fact]
        public void GateTimeoutFallsThroughWithoutHanging()
        {
            // If the gate stays held forever (a wedged verify), the
            // user command's bounded wait MUST give up rather than
            // hanging. We patch the per-connection constant via the
            // static field — but it's `internal const`, so we just
            // observe behavior with the gate held: the worker still
            // returns within VerifyGateWaitMs + slack.
            //
            // Skip this if the constant is large enough to make the
            // test impractical; the production value (5s) is bounded
            // by the verify-query timeout, so this DOES eventually
            // return.
            //
            // Cheap variant: pre-mark dirty, hold the gate from the
            // test thread, run VerifyAndClearDirty on the worker, and
            // wait up to VerifyGateWaitMs + 1s for it to return.
            //
            // We don't reduce the timeout (no test hook on a const);
            // instead we assert structural correctness: the worker
            // eventually completes even with the gate held.
            //
            // To keep the test fast we bound the test budget at
            // VerifyGateWaitMs + 1500ms.
            var cache = new NativeCache();
            cache.SetConnected(true);
            var inner = new FakeConnection();
            var conn = new CachedConnection(inner, cache);
            conn.GucState.MarkDirty();

            var gateField = typeof(CachedConnection).GetField(
                "_verifyGate",
                System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
            var gate = (SemaphoreSlim)gateField.GetValue(conn);
            Assert.True(gate.Wait(0));

            try
            {
                var done = new ManualResetEventSlim(false);
                var worker = new Thread(() =>
                {
                    conn.VerifyAndClearDirty();
                    done.Set();
                });
                worker.IsBackground = true;
                worker.Start();

                // Worker must eventually return — bounded by the
                // VerifyGateWaitMs constant.
                Assert.True(done.Wait(CachedConnection.VerifyGateWaitMs + 1500));
                // Verify never ran — dirty remains set.
                Assert.True(conn.GucState.IsDirty);
            }
            finally
            {
                gate.Release();
            }
        }

        // Local SpinUntil for the serialization tests — duplicated to
        // keep the test class self-contained.
        private static bool SpinUntil(Func<bool> predicate, int timeoutMs = 2000)
        {
            var deadline = DateTime.UtcNow.AddMilliseconds(timeoutMs);
            while (DateTime.UtcNow < deadline)
            {
                if (predicate()) return true;
                Thread.Sleep(10);
            }
            return predicate();
        }
    }

    // ── Fake implementations for testing ─────────────────────

    internal class FakeConnection : DbConnection
    {
        public FakeDataReader NextReader;
        public int NextNonQueryResult;
        public FakeTransaction NextTransaction;
        // Per-SQL reader factory — keyed by exact-match CommandText.
        // Returns a fresh reader each invocation so tests can re-run
        // the same query and re-walk the rows. Used by RLS-hardening
        // tests to seed pg_settings query responses.
        public Dictionary<string, Func<FakeDataReader>> ReaderBySql = new Dictionary<string, Func<FakeDataReader>>(StringComparer.Ordinal);
        // Recorded SQL text of every command executed against this
        // connection — lets tests assert that a verify query fired
        // (asynchronously or synchronously).
        public List<string> ExecutedSql = new List<string>();
        // Force every command on this connection to report its
        // connection state as Closed — used to test the "no verify on
        // closed connection" guard in VerifyAndClearDirty.
        public bool ForceClosed;

        // Fault injection — when set, the matching execute path on the
        // FakeCommand throws this exception. Used by the SET-actually-
        // applied regression tests to simulate a DbException raised by
        // the underlying driver after observation but before a result.
        // Cleared after one throw so tests can sequence (throw on
        // first call, succeed on second).
        public Exception ThrowOnNextNonQuery;
        public Exception ThrowOnNextReader;
        public Exception ThrowOnNextScalar;

        public override string ConnectionString { get; set; } = "fake";
        public override string Database => "fake";
        public override string DataSource => "fake";
        public override string ServerVersion => "1.0";
        public override ConnectionState State => ForceClosed ? ConnectionState.Closed : ConnectionState.Open;

        public override void ChangeDatabase(string databaseName) { }
        public override void Open() { }
        public override void Close() { }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            return NextTransaction ?? new FakeTransaction();
        }

        protected override DbCommand CreateDbCommand()
        {
            return new FakeCommand(this);
        }
    }

    internal class FakeTransaction : DbTransaction
    {
        public override IsolationLevel IsolationLevel => IsolationLevel.ReadCommitted;
        protected override DbConnection DbConnection => null;
        public override void Commit() { }
        public override void Rollback() { }
    }

    internal class FakeCommand : DbCommand
    {
        private readonly FakeConnection _conn;
        private readonly FakeParameterCollection _params = new FakeParameterCollection();

        public FakeCommand(FakeConnection conn) { _conn = conn; }

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

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            lock (_conn.ExecutedSql) _conn.ExecutedSql.Add(CommandText ?? "");
            if (_conn.ThrowOnNextReader != null)
            {
                var ex = _conn.ThrowOnNextReader;
                _conn.ThrowOnNextReader = null;
                throw ex;
            }
            if (CommandText != null && _conn.ReaderBySql.TryGetValue(CommandText, out var factory))
                return factory();
            return _conn.NextReader ?? new FakeDataReader(new object[0][], new string[0]);
        }

        public override int ExecuteNonQuery()
        {
            lock (_conn.ExecutedSql) _conn.ExecutedSql.Add(CommandText ?? "");
            if (_conn.ThrowOnNextNonQuery != null)
            {
                var ex = _conn.ThrowOnNextNonQuery;
                _conn.ThrowOnNextNonQuery = null;
                throw ex;
            }
            return _conn.NextNonQueryResult;
        }
        public override object ExecuteScalar()
        {
            lock (_conn.ExecutedSql) _conn.ExecutedSql.Add(CommandText ?? "");
            if (_conn.ThrowOnNextScalar != null)
            {
                var ex = _conn.ThrowOnNextScalar;
                _conn.ThrowOnNextScalar = null;
                throw ex;
            }
            return null;
        }
    }

    internal class FakeParameter : DbParameter
    {
        public override DbType DbType { get; set; }
        public override ParameterDirection Direction { get; set; }
        public override bool IsNullable { get; set; }
        public override string ParameterName { get; set; }
        public override int Size { get; set; }
        public override string SourceColumn { get; set; }
        public override bool SourceColumnNullMapping { get; set; }
        public override object Value { get; set; }
        public override void ResetDbType() { }
    }

    internal class FakeParameterCollection : DbParameterCollection
    {
        private readonly List<DbParameter> _list = new List<DbParameter>();
        public override int Count => _list.Count;
        public override object SyncRoot => _list;
        public override int Add(object value) { _list.Add((DbParameter)value); return _list.Count - 1; }
        public override void AddRange(Array values) { foreach (var v in values) Add(v); }
        public override void Clear() => _list.Clear();
        public override bool Contains(object value) => _list.Contains((DbParameter)value);
        public override bool Contains(string value) => _list.Exists(p => p.ParameterName == value);
        public override void CopyTo(Array array, int index) { }
        public override System.Collections.IEnumerator GetEnumerator() => _list.GetEnumerator();
        public override int IndexOf(object value) => _list.IndexOf((DbParameter)value);
        public override int IndexOf(string parameterName) => _list.FindIndex(p => p.ParameterName == parameterName);
        public override void Insert(int index, object value) => _list.Insert(index, (DbParameter)value);
        public override void Remove(object value) => _list.Remove((DbParameter)value);
        public override void RemoveAt(int index) => _list.RemoveAt(index);
        public override void RemoveAt(string parameterName) => _list.RemoveAt(IndexOf(parameterName));
        protected override DbParameter GetParameter(int index) => _list[index];
        protected override DbParameter GetParameter(string parameterName) => _list.Find(p => p.ParameterName == parameterName);
        protected override void SetParameter(int index, DbParameter value) => _list[index] = value;
        protected override void SetParameter(string parameterName, DbParameter value) => _list[IndexOf(parameterName)] = value;
    }

    internal class FakeDataReader : DbDataReader
    {
        private readonly object[][] _rows;
        private readonly string[] _columns;
        private int _cursor = -1;
        private bool _closed;

        public FakeDataReader(object[][] rows, string[] columns) { _rows = rows; _columns = columns; }

        public override bool Read() { _cursor++; return _cursor < _rows.Length; }
        public override bool NextResult() => false;
        public override void Close() { _closed = true; }
        public override int FieldCount => _columns.Length;
        public override int RecordsAffected => -1;
        public override bool HasRows => _rows.Length > 0;
        public override bool IsClosed => _closed;
        public override int Depth => 0;

        public override object this[int ordinal] => GetValue(ordinal);
        public override object this[string name] => GetValue(GetOrdinal(name));
        public override string GetName(int ordinal) => _columns[ordinal];
        public override int GetOrdinal(string name)
        {
            for (int i = 0; i < _columns.Length; i++)
                if (_columns[i].Equals(name, StringComparison.OrdinalIgnoreCase)) return i;
            throw new IndexOutOfRangeException(name);
        }
        public override object GetValue(int ordinal) => _rows[_cursor][ordinal];
        public override int GetValues(object[] values)
        {
            var row = _rows[_cursor];
            var count = Math.Min(values.Length, row.Length);
            Array.Copy(row, values, count);
            return count;
        }
        public override bool IsDBNull(int ordinal) => _rows[_cursor][ordinal] == null;
        public override string GetString(int ordinal) => GetValue(ordinal)?.ToString();
        public override int GetInt32(int ordinal) => Convert.ToInt32(GetValue(ordinal));
        public override long GetInt64(int ordinal) => Convert.ToInt64(GetValue(ordinal));
        public override double GetDouble(int ordinal) => Convert.ToDouble(GetValue(ordinal));
        public override float GetFloat(int ordinal) => Convert.ToSingle(GetValue(ordinal));
        public override bool GetBoolean(int ordinal) => Convert.ToBoolean(GetValue(ordinal));
        public override byte GetByte(int ordinal) => Convert.ToByte(GetValue(ordinal));
        public override short GetInt16(int ordinal) => Convert.ToInt16(GetValue(ordinal));
        public override decimal GetDecimal(int ordinal) => Convert.ToDecimal(GetValue(ordinal));
        public override char GetChar(int ordinal) => Convert.ToChar(GetValue(ordinal));
        public override DateTime GetDateTime(int ordinal) => Convert.ToDateTime(GetValue(ordinal));
        public override Guid GetGuid(int ordinal) => Guid.Parse(GetValue(ordinal).ToString());
        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length) => 0;
        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length) => 0;
        public override string GetDataTypeName(int ordinal) => "object";
        public override Type GetFieldType(int ordinal) => typeof(object);
        public override System.Collections.IEnumerator GetEnumerator() => throw new NotSupportedException();
    }
}
