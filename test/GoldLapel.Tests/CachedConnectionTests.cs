using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using Xunit;

namespace Goldlapel.Tests
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

    // ── Fake implementations for testing ─────────────────────

    internal class FakeConnection : DbConnection
    {
        public FakeDataReader NextReader;
        public int NextNonQueryResult;
        public FakeTransaction NextTransaction;

        public override string ConnectionString { get; set; } = "fake";
        public override string Database => "fake";
        public override string DataSource => "fake";
        public override string ServerVersion => "1.0";
        public override ConnectionState State => ConnectionState.Open;

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
            return _conn.NextReader ?? new FakeDataReader(new object[0][], new string[0]);
        }

        public override int ExecuteNonQuery() => _conn.NextNonQueryResult;
        public override object ExecuteScalar() => null;
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
