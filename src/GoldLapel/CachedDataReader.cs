using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace Goldlapel
{
    public class CachedDataReader : DbDataReader
    {
        private readonly object[][] _rows;
        private readonly string[] _columns;
        private readonly Dictionary<string, int> _columnIndex;
        private int _cursor = -1;
        private bool _closed;

        public CachedDataReader(object[][] rows, string[] columns)
        {
            _rows = rows;
            _columns = columns;
            _columnIndex = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < columns.Length; i++)
                _columnIndex[columns[i]] = i;
        }

        public override bool Read()
        {
            _cursor++;
            return _cursor < _rows.Length;
        }

        public override bool NextResult() { return false; }

        public override void Close()
        {
            _closed = true;
        }

        protected override void Dispose(bool disposing)
        {
            _closed = true;
            base.Dispose(disposing);
        }

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
            int idx;
            if (_columnIndex.TryGetValue(name, out idx))
                return idx;
            throw new IndexOutOfRangeException("Column not found: " + name);
        }

        public override object GetValue(int ordinal)
        {
            return _rows[_cursor][ordinal];
        }

        public override int GetValues(object[] values)
        {
            var row = _rows[_cursor];
            var count = Math.Min(values.Length, row.Length);
            Array.Copy(row, values, count);
            return count;
        }

        public override bool IsDBNull(int ordinal)
        {
            return _rows[_cursor][ordinal] == null || _rows[_cursor][ordinal] is DBNull;
        }

        public override string GetString(int ordinal)
        {
            var val = GetValue(ordinal);
            return val?.ToString();
        }

        public override int GetInt32(int ordinal)
        {
            var val = GetValue(ordinal);
            if (val is int i) return i;
            return Convert.ToInt32(val);
        }

        public override long GetInt64(int ordinal)
        {
            var val = GetValue(ordinal);
            if (val is long l) return l;
            return Convert.ToInt64(val);
        }

        public override double GetDouble(int ordinal)
        {
            var val = GetValue(ordinal);
            if (val is double d) return d;
            return Convert.ToDouble(val);
        }

        public override float GetFloat(int ordinal)
        {
            var val = GetValue(ordinal);
            if (val is float f) return f;
            return Convert.ToSingle(val);
        }

        public override bool GetBoolean(int ordinal)
        {
            var val = GetValue(ordinal);
            if (val is bool b) return b;
            return Convert.ToBoolean(val);
        }

        public override byte GetByte(int ordinal)
        {
            var val = GetValue(ordinal);
            if (val is byte b) return b;
            return Convert.ToByte(val);
        }

        public override short GetInt16(int ordinal)
        {
            var val = GetValue(ordinal);
            if (val is short s) return s;
            return Convert.ToInt16(val);
        }

        public override decimal GetDecimal(int ordinal)
        {
            var val = GetValue(ordinal);
            if (val is decimal d) return d;
            return Convert.ToDecimal(val);
        }

        public override char GetChar(int ordinal)
        {
            var val = GetValue(ordinal);
            if (val is char c) return c;
            return Convert.ToChar(val);
        }

        public override DateTime GetDateTime(int ordinal)
        {
            var val = GetValue(ordinal);
            if (val is DateTime dt) return dt;
            return Convert.ToDateTime(val);
        }

        public override Guid GetGuid(int ordinal)
        {
            var val = GetValue(ordinal);
            if (val is Guid g) return g;
            return Guid.Parse(val.ToString());
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            throw new NotSupportedException();
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            throw new NotSupportedException();
        }

        public override string GetDataTypeName(int ordinal) => "object";
        public override Type GetFieldType(int ordinal)
        {
            if (_rows.Length > 0 && _cursor >= 0 && _cursor < _rows.Length)
            {
                var val = _rows[_cursor][ordinal];
                if (val != null) return val.GetType();
            }
            return typeof(object);
        }

        public override IEnumerator GetEnumerator()
        {
            return new DbEnumerator(this);
        }

        public override DataTable GetSchemaTable()
        {
            var table = new DataTable("SchemaTable");
            table.Columns.Add("ColumnName", typeof(string));
            table.Columns.Add("ColumnOrdinal", typeof(int));
            for (int i = 0; i < _columns.Length; i++)
            {
                var row = table.NewRow();
                row["ColumnName"] = _columns[i];
                row["ColumnOrdinal"] = i;
                table.Rows.Add(row);
            }
            return table;
        }
    }
}
