using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading;

namespace GoldLapel
{
    public static class Utils
    {
        /// <summary>
        /// Publish a message to a channel. Like redis.publish().
        /// Uses PostgreSQL NOTIFY under the hood.
        /// </summary>
        public static void Publish(DbConnection conn, string channel, string message)
        {
            ValidateIdentifier(channel);
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT pg_notify(@channel, @message)";
                AddParameter(cmd, "@channel", channel);
                AddParameter(cmd, "@message", message);
                cmd.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Add a job to a queue table. Like redis.lpush().
        /// Creates the queue table if it doesn't exist. Payload is stored as JSONB.
        /// </summary>
        public static void Enqueue(DbConnection conn, string queueTable, string payloadJson)
        {
            ValidateIdentifier(queueTable);
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "CREATE TABLE IF NOT EXISTS " + queueTable + " (" +
                    "id BIGSERIAL PRIMARY KEY, " +
                    "payload JSONB NOT NULL, " +
                    "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "INSERT INTO " + queueTable + " (payload) VALUES (@payload::jsonb)";
                AddParameter(cmd, "@payload", payloadJson);
                cmd.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Pop the next job from a queue table. Like redis.brpop() (non-blocking).
        /// Uses FOR UPDATE SKIP LOCKED for safe concurrent access.
        /// Returns the payload JSON string, or null if the queue is empty.
        /// </summary>
        public static string Dequeue(DbConnection conn, string queueTable)
        {
            ValidateIdentifier(queueTable);
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "DELETE FROM " + queueTable +
                    " WHERE id = (" +
                    "SELECT id FROM " + queueTable +
                    " ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1" +
                    ") RETURNING payload";

                using (var reader = cmd.ExecuteReader())
                {
                    if (reader.Read())
                        return reader.GetValue(0)?.ToString();
                    return null;
                }
            }
        }

        /// <summary>
        /// Increment a counter. Like redis.incr().
        /// Creates the counter table if it doesn't exist. Returns the new value.
        /// </summary>
        public static long Incr(DbConnection conn, string table, string key, long amount = 1)
        {
            ValidateIdentifier(table);
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "CREATE TABLE IF NOT EXISTS " + table + " (" +
                    "key TEXT PRIMARY KEY, " +
                    "value BIGINT NOT NULL DEFAULT 0)";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "INSERT INTO " + table + " (key, value) VALUES (@key, @amount) " +
                    "ON CONFLICT (key) DO UPDATE SET value = " + table + ".value + @incr " +
                    "RETURNING value";
                AddParameter(cmd, "@key", key);
                AddParameter(cmd, "@amount", amount);
                AddParameter(cmd, "@incr", amount);
                return (long)cmd.ExecuteScalar();
            }
        }

        /// <summary>
        /// Add a member with a score to a sorted set. Like redis.zadd().
        /// Creates the sorted set table if it doesn't exist.
        /// If the member already exists, updates the score.
        /// </summary>
        public static void Zadd(DbConnection conn, string table, string member, double score)
        {
            ValidateIdentifier(table);
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "CREATE TABLE IF NOT EXISTS " + table + " (" +
                    "member TEXT PRIMARY KEY, " +
                    "score DOUBLE PRECISION NOT NULL)";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "INSERT INTO " + table + " (member, score) VALUES (@member, @score) " +
                    "ON CONFLICT (member) DO UPDATE SET score = EXCLUDED.score";
                AddParameter(cmd, "@member", member);
                AddParameter(cmd, "@score", score);
                cmd.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Get members by score rank. Like redis.zrange().
        /// Returns a list of (Member, Score) tuples.
        /// desc=true returns highest scores first (leaderboard order).
        /// </summary>
        public static List<(string Member, double Score)> Zrange(
            DbConnection conn, string table, int start = 0, int stop = 10, bool desc = true)
        {
            ValidateIdentifier(table);
            var order = desc ? "DESC" : "ASC";
            var limit = stop - start;

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "SELECT member, score FROM " + table +
                    " ORDER BY score " + order +
                    " LIMIT @limit OFFSET @offset";
                AddParameter(cmd, "@limit", limit);
                AddParameter(cmd, "@offset", start);

                var results = new List<(string Member, double Score)>();
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        results.Add((reader.GetString(0), reader.GetDouble(1)));
                    }
                }
                return results;
            }
        }

        /// <summary>
        /// Set a field in a hash. Like redis.hset().
        /// Creates the hash table if it doesn't exist. Uses JSONB for storage.
        /// </summary>
        public static void Hset(DbConnection conn, string table, string key, string field, string valueJson)
        {
            ValidateIdentifier(table);
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "CREATE TABLE IF NOT EXISTS " + table + " (" +
                    "key TEXT PRIMARY KEY, " +
                    "data JSONB NOT NULL DEFAULT '{}'::jsonb)";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "INSERT INTO " + table + " (key, data) VALUES (@key, jsonb_build_object(@field, @val::jsonb)) " +
                    "ON CONFLICT (key) DO UPDATE SET data = " + table + ".data || jsonb_build_object(@field2, @val2::jsonb)";
                AddParameter(cmd, "@key", key);
                AddParameter(cmd, "@field", field);
                AddParameter(cmd, "@val", valueJson);
                AddParameter(cmd, "@field2", field);
                AddParameter(cmd, "@val2", valueJson);
                cmd.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Get a field from a hash. Like redis.hget().
        /// Returns the value as a JSON string, or null if key or field doesn't exist.
        /// </summary>
        public static string Hget(DbConnection conn, string table, string key, string field)
        {
            ValidateIdentifier(table);
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT data->>@field FROM " + table + " WHERE key = @key";
                AddParameter(cmd, "@field", field);
                AddParameter(cmd, "@key", key);

                using (var reader = cmd.ExecuteReader())
                {
                    if (reader.Read() && !reader.IsDBNull(0))
                        return reader.GetString(0);
                    return null;
                }
            }
        }

        /// <summary>
        /// Get all fields from a hash. Like redis.hgetall().
        /// Returns the full JSONB object as a string, or null if key doesn't exist.
        /// </summary>
        public static string Hgetall(DbConnection conn, string table, string key)
        {
            ValidateIdentifier(table);
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT data FROM " + table + " WHERE key = @key";
                AddParameter(cmd, "@key", key);

                using (var reader = cmd.ExecuteReader())
                {
                    if (reader.Read() && !reader.IsDBNull(0))
                        return reader.GetValue(0)?.ToString();
                    return null;
                }
            }
        }

        /// <summary>
        /// Remove a field from a hash. Like redis.hdel().
        /// Returns true if the field existed, false otherwise.
        /// </summary>
        public static bool Hdel(DbConnection conn, string table, string key, string field)
        {
            ValidateIdentifier(table);
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT data ? @field AS existed FROM " + table + " WHERE key = @key";
                AddParameter(cmd, "@field", field);
                AddParameter(cmd, "@key", key);

                using (var reader = cmd.ExecuteReader())
                {
                    if (!reader.Read() || !reader.GetBoolean(0))
                        return false;
                }
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "UPDATE " + table + " SET data = data - @field WHERE key = @key";
                AddParameter(cmd, "@field", field);
                AddParameter(cmd, "@key", key);
                cmd.ExecuteNonQuery();
            }
            return true;
        }

        /// <summary>
        /// Add a location to a geo table. Like redis.geoadd().
        /// Creates the table with PostGIS geometry column if it doesn't exist.
        /// Requires PostGIS extension.
        /// </summary>
        public static void Geoadd(DbConnection conn, string table, string nameColumn,
            string geomColumn, string name, double lon, double lat)
        {
            ValidateIdentifier(table);
            ValidateIdentifier(nameColumn);
            ValidateIdentifier(geomColumn);
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "CREATE EXTENSION IF NOT EXISTS postgis";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "CREATE TABLE IF NOT EXISTS " + table + " (" +
                    "id BIGSERIAL PRIMARY KEY, " +
                    nameColumn + " TEXT NOT NULL, " +
                    geomColumn + " GEOMETRY(Point, 4326) NOT NULL)";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "INSERT INTO " + table + " (" + nameColumn + ", " + geomColumn + ") " +
                    "VALUES (@name, ST_SetSRID(ST_MakePoint(@lon, @lat), 4326))";
                AddParameter(cmd, "@name", name);
                AddParameter(cmd, "@lon", lon);
                AddParameter(cmd, "@lat", lat);
                cmd.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Find rows within a radius of a point. Like redis.georadius().
        /// Requires PostGIS extension. Uses ST_DWithin with geography type
        /// for accurate distance on the Earth's surface.
        /// Returns a list of dictionaries with all columns plus a "distance_m" field.
        /// </summary>
        public static List<Dictionary<string, object>> Georadius(DbConnection conn, string table,
            string geomColumn, double lon, double lat, double radiusMeters, int limit = 50)
        {
            ValidateIdentifier(table);
            ValidateIdentifier(geomColumn);
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "SELECT *, ST_Distance(" +
                    geomColumn + "::geography, " +
                    "ST_SetSRID(ST_MakePoint(@lon1, @lat1), 4326)::geography" +
                    ") AS distance_m " +
                    "FROM " + table + " " +
                    "WHERE ST_DWithin(" +
                    geomColumn + "::geography, " +
                    "ST_SetSRID(ST_MakePoint(@lon2, @lat2), 4326)::geography, " +
                    "@radius) " +
                    "ORDER BY distance_m " +
                    "LIMIT @limit";
                AddParameter(cmd, "@lon1", lon);
                AddParameter(cmd, "@lat1", lat);
                AddParameter(cmd, "@lon2", lon);
                AddParameter(cmd, "@lat2", lat);
                AddParameter(cmd, "@radius", radiusMeters);
                AddParameter(cmd, "@limit", limit);

                var results = new List<Dictionary<string, object>>();
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var row = new Dictionary<string, object>();
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                        }
                        results.Add(row);
                    }
                }
                return results;
            }
        }

        /// <summary>
        /// Get distance between two members in meters. Like redis.geodist().
        /// Returns the distance in meters, or null if either member doesn't exist.
        /// </summary>
        public static double? Geodist(DbConnection conn, string table, string geomColumn,
            string nameColumn, string nameA, string nameB)
        {
            ValidateIdentifier(table);
            ValidateIdentifier(geomColumn);
            ValidateIdentifier(nameColumn);
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "SELECT ST_Distance(a." + geomColumn + "::geography, b." + geomColumn + "::geography) " +
                    "FROM " + table + " a, " + table + " b " +
                    "WHERE a." + nameColumn + " = @nameA AND b." + nameColumn + " = @nameB";
                AddParameter(cmd, "@nameA", nameA);
                AddParameter(cmd, "@nameB", nameB);

                using (var reader = cmd.ExecuteReader())
                {
                    if (reader.Read() && !reader.IsDBNull(0))
                        return reader.GetDouble(0);
                    return null;
                }
            }
        }

        public static void Subscribe(DbConnection conn, string channel, Action<string, string> callback, bool blocking = true)
        {
            ValidateIdentifier(channel);
            if (blocking)
            {
                ListenLoop(conn, channel, callback);
            }
            else
            {
                var thread = new Thread(() => ListenLoop(conn, channel, callback));
                thread.IsBackground = true;
                thread.Start();
            }
        }

        private static void ListenLoop(DbConnection conn, string channel, Action<string, string> callback)
        {
            var listenConn = CreateListenConnection(conn);

            var notificationEvent = listenConn.GetType().GetEvent("Notification");
            if (notificationEvent == null)
                throw new InvalidOperationException(
                    "Subscribe requires Npgsql. The connection must be an NpgsqlConnection.");

            var handlerType = notificationEvent.EventHandlerType;
            var invokeParams = handlerType.GetMethod("Invoke").GetParameters();
            var argsType = invokeParams[1].ParameterType;
            var channelProp = argsType.GetProperty("Channel");
            var payloadProp = argsType.GetProperty("Payload");

            var senderParam = Expression.Parameter(invokeParams[0].ParameterType, "sender");
            var argsParam = Expression.Parameter(argsType, "e");
            var callbackConst = Expression.Constant(callback);
            var body = Expression.Invoke(callbackConst,
                Expression.Property(argsParam, channelProp),
                Expression.Property(argsParam, payloadProp));
            var lambda = Expression.Lambda(handlerType, body, senderParam, argsParam);
            notificationEvent.AddEventHandler(listenConn, lambda.Compile());

            using (var cmd = listenConn.CreateCommand())
            {
                cmd.CommandText = "LISTEN " + channel;
                cmd.ExecuteNonQuery();
            }

            var waitMethod = listenConn.GetType().GetMethod("Wait",
                BindingFlags.Public | BindingFlags.Instance, null, Type.EmptyTypes, null);

            if (waitMethod == null)
                throw new InvalidOperationException(
                    "Subscribe requires Npgsql with Wait() support.");

            while (true)
            {
                waitMethod.Invoke(listenConn, null);
            }
        }

        private static DbConnection CreateListenConnection(DbConnection conn)
        {
            var inner = conn is CachedConnection cached ? cached.Inner : conn;
            var connString = inner.ConnectionString;
            var listenConn = (DbConnection)Activator.CreateInstance(inner.GetType(), connString);
            listenConn.Open();
            return listenConn;
        }

        public static long GetCounter(DbConnection conn, string table, string key)
        {
            ValidateIdentifier(table);
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT value FROM " + table + " WHERE key = @key";
                AddParameter(cmd, "@key", key);

                using (var reader = cmd.ExecuteReader())
                {
                    if (reader.Read() && !reader.IsDBNull(0))
                        return reader.GetInt64(0);
                    return 0;
                }
            }
        }

        public static double Zincrby(DbConnection conn, string table, string member, double amount = 1)
        {
            ValidateIdentifier(table);
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "CREATE TABLE IF NOT EXISTS " + table + " (" +
                    "member TEXT PRIMARY KEY, " +
                    "score DOUBLE PRECISION NOT NULL)";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "INSERT INTO " + table + " (member, score) VALUES (@member, @amount) " +
                    "ON CONFLICT (member) DO UPDATE SET score = " + table + ".score + @incr " +
                    "RETURNING score";
                AddParameter(cmd, "@member", member);
                AddParameter(cmd, "@amount", amount);
                AddParameter(cmd, "@incr", amount);
                return (double)cmd.ExecuteScalar();
            }
        }

        public static long? Zrank(DbConnection conn, string table, string member, bool desc = true)
        {
            ValidateIdentifier(table);
            var order = desc ? "DESC" : "ASC";

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "SELECT rank FROM (" +
                    "SELECT member, ROW_NUMBER() OVER (ORDER BY score " + order + ") - 1 AS rank " +
                    "FROM " + table +
                    ") ranked WHERE member = @member";
                AddParameter(cmd, "@member", member);

                using (var reader = cmd.ExecuteReader())
                {
                    if (reader.Read() && !reader.IsDBNull(0))
                        return reader.GetInt64(0);
                    return null;
                }
            }
        }

        public static double? Zscore(DbConnection conn, string table, string member)
        {
            ValidateIdentifier(table);
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT score FROM " + table + " WHERE member = @member";
                AddParameter(cmd, "@member", member);

                using (var reader = cmd.ExecuteReader())
                {
                    if (reader.Read() && !reader.IsDBNull(0))
                        return reader.GetDouble(0);
                    return null;
                }
            }
        }

        public static bool Zrem(DbConnection conn, string table, string member)
        {
            ValidateIdentifier(table);
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "DELETE FROM " + table + " WHERE member = @member";
                AddParameter(cmd, "@member", member);
                return cmd.ExecuteNonQuery() > 0;
            }
        }

        public static long CountDistinct(DbConnection conn, string table, string column)
        {
            ValidateIdentifier(table);
            ValidateIdentifier(column);
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT COUNT(DISTINCT " + column + ") FROM " + table;
                return (long)cmd.ExecuteScalar();
            }
        }

        public static string Script(DbConnection conn, string luaCode, params string[] args)
        {
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "CREATE EXTENSION IF NOT EXISTS pllua";
                cmd.ExecuteNonQuery();
            }

            var funcName = "_gl_lua_" + Guid.NewGuid().ToString("N").Substring(0, 8);
            var paramDefs = string.Join(", ", Enumerable.Range(0, args.Length).Select(i => $"p{i + 1} text"));

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = $"CREATE OR REPLACE FUNCTION pg_temp.{funcName}({paramDefs}) RETURNS text LANGUAGE pllua AS $pllua$ {luaCode} $pllua$";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                var placeholders = string.Join(", ", Enumerable.Range(0, args.Length).Select(i => $"@p{i + 1}"));
                cmd.CommandText = $"SELECT pg_temp.{funcName}({placeholders})";
                for (int i = 0; i < args.Length; i++)
                {
                    AddParameter(cmd, $"@p{i + 1}", args[i]);
                }
                var result = cmd.ExecuteScalar();
                return result == DBNull.Value ? null : (string)result;
            }
        }

        private static string RequireStreamPattern(DdlEntry patterns, string key, string fn)
        {
            if (patterns == null || patterns.QueryPatterns == null)
                throw new InvalidOperationException(
                    fn + " requires DDL patterns from the proxy — call via "
                    + "gl." + fn + "(...) rather than Utils." + fn + " directly.");
            if (!patterns.QueryPatterns.TryGetValue(key, out var sql))
                throw new InvalidOperationException(
                    "DDL API response missing pattern '" + key + "' for " + fn);
            return sql;
        }

        /// <summary>
        /// Bind an ordered args list to a command whose SQL uses @p1, @p2, ...
        /// placeholders (see <see cref="Ddl.ToNpgsqlPlaceholders"/>).
        /// </summary>
        private static void BindNumbered(DbCommand cmd, params object[] args)
        {
            for (int i = 0; i < args.Length; i++)
                AddParameter(cmd, "@p" + (i + 1), args[i]);
        }

        public static long StreamAdd(DbConnection conn, string stream, string payload, DdlEntry patterns)
        {
            ValidateIdentifier(stream);
            var rawSql = RequireStreamPattern(patterns, "insert", "StreamAdd");
            var (sql, _) = Ddl.ToNpgsqlPlaceholders(rawSql);
            // JSONB binding: cast @p1 to jsonb at SQL site.
            sql = sql.Replace("VALUES (@p1)", "VALUES (@p1::jsonb)");
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = sql;
                BindNumbered(cmd, payload);
                // Pattern returns (id, created_at); we only need id.
                using (var reader = cmd.ExecuteReader())
                {
                    reader.Read();
                    return reader.GetInt64(0);
                }
            }
        }

        public static void StreamCreateGroup(DbConnection conn, string stream, string group, DdlEntry patterns)
        {
            ValidateIdentifier(stream);
            var rawSql = RequireStreamPattern(patterns, "create_group", "StreamCreateGroup");
            var (sql, _) = Ddl.ToNpgsqlPlaceholders(rawSql);
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = sql;
                BindNumbered(cmd, group);
                cmd.ExecuteNonQuery();
            }
        }

        public static List<Dictionary<string, object>> StreamRead(DbConnection conn, string stream,
            string group, string consumer, int count, DdlEntry patterns)
        {
            ValidateIdentifier(stream);
            var cursorSql = Ddl.ToNpgsqlPlaceholders(RequireStreamPattern(patterns, "group_get_cursor", "StreamRead")).Sql;
            var readSql = Ddl.ToNpgsqlPlaceholders(RequireStreamPattern(patterns, "read_since", "StreamRead")).Sql;
            var advanceSql = Ddl.ToNpgsqlPlaceholders(RequireStreamPattern(patterns, "group_advance_cursor", "StreamRead")).Sql;
            var pendingSql = Ddl.ToNpgsqlPlaceholders(RequireStreamPattern(patterns, "pending_insert", "StreamRead")).Sql;

            long lastId;
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = cursorSql;
                BindNumbered(cmd, group);
                var obj = cmd.ExecuteScalar();
                if (obj == null || obj == DBNull.Value) return new List<Dictionary<string, object>>();
                lastId = Convert.ToInt64(obj);
            }

            var messages = new List<Dictionary<string, object>>();
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = readSql;
                BindNumbered(cmd, lastId, count);
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var row = new Dictionary<string, object>();
                        for (int i = 0; i < reader.FieldCount; i++)
                            row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                        messages.Add(row);
                    }
                }
            }

            if (messages.Count > 0)
            {
                long maxId = 0;
                foreach (var m in messages)
                {
                    var id = Convert.ToInt64(m["id"]);
                    if (id > maxId) maxId = id;
                }
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = advanceSql;
                    BindNumbered(cmd, maxId, group);
                    cmd.ExecuteNonQuery();
                }
                foreach (var m in messages)
                {
                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = pendingSql;
                        BindNumbered(cmd, Convert.ToInt64(m["id"]), group, consumer);
                        cmd.ExecuteNonQuery();
                    }
                }
            }
            return messages;
        }

        public static bool StreamAck(DbConnection conn, string stream, string group, long messageId, DdlEntry patterns)
        {
            ValidateIdentifier(stream);
            var sql = Ddl.ToNpgsqlPlaceholders(RequireStreamPattern(patterns, "ack", "StreamAck")).Sql;
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = sql;
                BindNumbered(cmd, group, messageId);
                return cmd.ExecuteNonQuery() > 0;
            }
        }

        public static List<Dictionary<string, object>> StreamClaim(DbConnection conn, string stream,
            string group, string consumer, long minIdleMs, DdlEntry patterns)
        {
            ValidateIdentifier(stream);
            var claimSql = Ddl.ToNpgsqlPlaceholders(RequireStreamPattern(patterns, "claim", "StreamClaim")).Sql;
            var readByIdSql = Ddl.ToNpgsqlPlaceholders(RequireStreamPattern(patterns, "read_by_id", "StreamClaim")).Sql;

            var ids = new List<long>();
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = claimSql;
                BindNumbered(cmd, consumer, group, minIdleMs);
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read()) ids.Add(reader.GetInt64(0));
                }
            }

            var messages = new List<Dictionary<string, object>>();
            foreach (var id in ids)
            {
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = readByIdSql;
                    BindNumbered(cmd, id);
                    using (var reader = cmd.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            var row = new Dictionary<string, object>();
                            for (int i = 0; i < reader.FieldCount; i++)
                                row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                            messages.Add(row);
                        }
                    }
                }
            }
            return messages;
        }

        public static List<Dictionary<string, object>> Search(DbConnection conn, string table,
            string column, string query, int limit = 50, string lang = "english", bool highlight = false)
        {
            return Search(conn, table, new[] { column }, query, limit, lang, highlight);
        }

        public static List<Dictionary<string, object>> Search(DbConnection conn, string table,
            string[] columns, string query, int limit = 50, string lang = "english", bool highlight = false)
        {
            ValidateIdentifier(table);
            foreach (var col in columns)
                ValidateIdentifier(col);

            // Build tsvector expression: coalesce(col1, '') || ' ' || coalesce(col2, '')
            var tsvParts = string.Join(" || ' ' || ", columns.Select(c => "coalesce(" + c + ", '')"));
            var tsv = "to_tsvector(@lang1, " + tsvParts + ")";
            var tsq = "plainto_tsquery(@lang2, @query)";
            var highlightCol = columns[0];

            string fields;
            if (highlight)
                fields = "*, ts_rank(" + tsv + ", " + tsq + ") AS _score, " +
                         "ts_headline(@lang3, coalesce(" + highlightCol + ", ''), " + tsq +
                         ", 'StartSel=<mark>, StopSel=</mark>, MaxWords=35, MinWords=15') AS _highlight";
            else
                fields = "*, ts_rank(" + tsv + ", " + tsq + ") AS _score";

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "SELECT " + fields + " FROM " + table +
                    " WHERE " + tsv + " @@ " + tsq +
                    " ORDER BY _score DESC LIMIT @limit";
                AddParameter(cmd, "@lang1", lang);
                AddParameter(cmd, "@lang2", lang);
                AddParameter(cmd, "@query", query);
                if (highlight)
                    AddParameter(cmd, "@lang3", lang);
                AddParameter(cmd, "@limit", limit);

                var results = new List<Dictionary<string, object>>();
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var row = new Dictionary<string, object>();
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                        }
                        results.Add(row);
                    }
                }
                return results;
            }
        }

        public static List<Dictionary<string, object>> SearchFuzzy(DbConnection conn, string table,
            string column, string query, int limit = 50, double threshold = 0.3)
        {
            ValidateIdentifier(table);
            ValidateIdentifier(column);

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "SELECT *, similarity(" + column + ", @query) AS _score FROM " + table +
                    " WHERE similarity(" + column + ", @query2) > @threshold" +
                    " ORDER BY _score DESC LIMIT @limit";
                AddParameter(cmd, "@query", query);
                AddParameter(cmd, "@query2", query);
                AddParameter(cmd, "@threshold", threshold);
                AddParameter(cmd, "@limit", limit);

                var results = new List<Dictionary<string, object>>();
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var row = new Dictionary<string, object>();
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                        }
                        results.Add(row);
                    }
                }
                return results;
            }
        }

        public static List<Dictionary<string, object>> SearchPhonetic(DbConnection conn, string table,
            string column, string query, int limit = 50)
        {
            ValidateIdentifier(table);
            ValidateIdentifier(column);

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "SELECT *, similarity(" + column + ", @query) AS _score FROM " + table +
                    " WHERE soundex(" + column + ") = soundex(@query2)" +
                    " ORDER BY _score DESC, " + column + " LIMIT @limit";
                AddParameter(cmd, "@query", query);
                AddParameter(cmd, "@query2", query);
                AddParameter(cmd, "@limit", limit);

                var results = new List<Dictionary<string, object>>();
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var row = new Dictionary<string, object>();
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                        }
                        results.Add(row);
                    }
                }
                return results;
            }
        }

        public static void PercolateAdd(DbConnection conn, string name, string queryId,
            string query, string lang = "english", string metadataJson = null)
        {
            ValidateIdentifier(name);

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "CREATE TABLE IF NOT EXISTS " + name + " (" +
                    "query_id TEXT PRIMARY KEY, " +
                    "query_text TEXT NOT NULL, " +
                    "tsquery TSQUERY NOT NULL, " +
                    "lang TEXT NOT NULL DEFAULT 'english', " +
                    "metadata JSONB)";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "CREATE INDEX IF NOT EXISTS " + name + "_tsq_idx ON " + name +
                    " USING GIST (tsquery)";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "INSERT INTO " + name + " (query_id, query_text, tsquery, lang, metadata) " +
                    "VALUES (@queryId, @query, plainto_tsquery(@lang, @query), @lang, @metadata::jsonb) " +
                    "ON CONFLICT (query_id) DO UPDATE SET " +
                    "query_text = EXCLUDED.query_text, " +
                    "tsquery = EXCLUDED.tsquery, " +
                    "lang = EXCLUDED.lang, " +
                    "metadata = EXCLUDED.metadata";
                AddParameter(cmd, "@queryId", queryId);
                AddParameter(cmd, "@query", query);
                AddParameter(cmd, "@lang", lang);
                AddParameter(cmd, "@metadata", (object)metadataJson ?? DBNull.Value);
                cmd.ExecuteNonQuery();
            }
        }

        public static List<Dictionary<string, object>> Percolate(DbConnection conn, string name,
            string text, int limit = 50, string lang = "english")
        {
            ValidateIdentifier(name);

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "SELECT query_id, query_text, metadata, " +
                    "ts_rank(to_tsvector(@lang, @text), tsquery) AS _score " +
                    "FROM " + name +
                    " WHERE to_tsvector(@lang, @text) @@ tsquery" +
                    " ORDER BY _score DESC LIMIT @limit";
                AddParameter(cmd, "@lang", lang);
                AddParameter(cmd, "@text", text);
                AddParameter(cmd, "@limit", limit);

                var results = new List<Dictionary<string, object>>();
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var row = new Dictionary<string, object>();
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                        }
                        results.Add(row);
                    }
                }
                return results;
            }
        }

        public static bool PercolateDelete(DbConnection conn, string name, string queryId)
        {
            ValidateIdentifier(name);

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "DELETE FROM " + name + " WHERE query_id = @queryId RETURNING query_id";
                AddParameter(cmd, "@queryId", queryId);

                using (var reader = cmd.ExecuteReader())
                {
                    return reader.Read();
                }
            }
        }

        public static List<Dictionary<string, object>> Similar(DbConnection conn, string table,
            string column, double[] vector, int limit = 10)
        {
            ValidateIdentifier(table);
            ValidateIdentifier(column);

            var vectorLiteral = "[" + string.Join(",",
                vector.Select(v => v.ToString(CultureInfo.InvariantCulture))) + "]";

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "SELECT *, (" + column + " <=> @vec::vector) AS _score FROM " + table +
                    " ORDER BY _score LIMIT @limit";
                AddParameter(cmd, "@vec", vectorLiteral);
                AddParameter(cmd, "@limit", limit);

                var results = new List<Dictionary<string, object>>();
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var row = new Dictionary<string, object>();
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                        }
                        results.Add(row);
                    }
                }
                return results;
            }
        }

        public static List<Dictionary<string, object>> Suggest(DbConnection conn, string table,
            string column, string prefix, int limit = 10)
        {
            ValidateIdentifier(table);
            ValidateIdentifier(column);

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "SELECT *, similarity(" + column + ", @prefix) AS _score FROM " + table +
                    " WHERE " + column + " ILIKE @pattern" +
                    " ORDER BY _score DESC, " + column + " LIMIT @limit";
                AddParameter(cmd, "@prefix", prefix);
                AddParameter(cmd, "@pattern", prefix + "%");
                AddParameter(cmd, "@limit", limit);

                var results = new List<Dictionary<string, object>>();
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var row = new Dictionary<string, object>();
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                        }
                        results.Add(row);
                    }
                }
                return results;
            }
        }

        public static List<Dictionary<string, object>> Facets(DbConnection conn, string table,
            string column, int limit = 50, string query = null, string queryColumn = null,
            string lang = "english")
        {
            return Facets(conn, table, column, limit, query,
                queryColumn != null ? new[] { queryColumn } : null, lang);
        }

        public static List<Dictionary<string, object>> Facets(DbConnection conn, string table,
            string column, int limit = 50, string query = null, string[] queryColumns = null,
            string lang = "english")
        {
            ValidateIdentifier(table);
            ValidateIdentifier(column);

            var hasQuery = query != null && queryColumns != null && queryColumns.Length > 0;

            if (hasQuery)
            {
                foreach (var col in queryColumns)
                    ValidateIdentifier(col);
            }

            using (var cmd = conn.CreateCommand())
            {
                if (hasQuery)
                {
                    var tsvParts = string.Join(" || ' ' || ",
                        queryColumns.Select(c => "coalesce(" + c + ", '')"));
                    var tsv = "to_tsvector(@lang, " + tsvParts + ")";

                    cmd.CommandText =
                        "SELECT " + column + " AS value, COUNT(*) AS count FROM " + table +
                        " WHERE " + tsv + " @@ plainto_tsquery(@lang2, @query)" +
                        " GROUP BY " + column +
                        " ORDER BY count DESC, " + column +
                        " LIMIT @limit";
                    AddParameter(cmd, "@lang", lang);
                    AddParameter(cmd, "@lang2", lang);
                    AddParameter(cmd, "@query", query);
                }
                else
                {
                    cmd.CommandText =
                        "SELECT " + column + " AS value, COUNT(*) AS count FROM " + table +
                        " GROUP BY " + column +
                        " ORDER BY count DESC, " + column +
                        " LIMIT @limit";
                }
                AddParameter(cmd, "@limit", limit);

                var results = new List<Dictionary<string, object>>();
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var row = new Dictionary<string, object>();
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                        }
                        results.Add(row);
                    }
                }
                return results;
            }
        }

        private static readonly string[] AllowedAggregateFuncs = { "count", "sum", "avg", "min", "max" };

        public static List<Dictionary<string, object>> Aggregate(DbConnection conn, string table,
            string column, string func, string groupBy = null, int limit = 50)
        {
            ValidateIdentifier(table);
            ValidateIdentifier(column);

            var funcLower = func.ToLowerInvariant();
            if (Array.IndexOf(AllowedAggregateFuncs, funcLower) < 0)
                throw new ArgumentException(
                    "func must be one of: count, sum, avg, min, max — got: " + func);

            if (groupBy != null)
                ValidateIdentifier(groupBy);

            var expr = funcLower == "count"
                ? "COUNT(*)"
                : funcLower.ToUpperInvariant() + "(" + column + ")";

            using (var cmd = conn.CreateCommand())
            {
                if (groupBy != null)
                {
                    cmd.CommandText =
                        "SELECT " + groupBy + ", " + expr + " AS value FROM " + table +
                        " GROUP BY " + groupBy +
                        " ORDER BY value DESC" +
                        " LIMIT @limit";
                    AddParameter(cmd, "@limit", limit);
                }
                else
                {
                    cmd.CommandText = "SELECT " + expr + " AS value FROM " + table;
                }

                var results = new List<Dictionary<string, object>>();
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var row = new Dictionary<string, object>();
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                        }
                        results.Add(row);
                    }
                }
                return results;
            }
        }

        public static void CreateSearchConfig(DbConnection conn, string name, string copyFrom = "english")
        {
            ValidateIdentifier(name);
            ValidateIdentifier(copyFrom);

            bool exists;
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT 1 FROM pg_ts_config WHERE cfgname = @name";
                AddParameter(cmd, "@name", name);
                using (var reader = cmd.ExecuteReader())
                {
                    exists = reader.Read();
                }
            }

            if (!exists)
            {
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "CREATE TEXT SEARCH CONFIGURATION " + name +
                        " (COPY = " + copyFrom + ")";
                    cmd.ExecuteNonQuery();
                }
            }
        }

        /// <summary>
        /// Analyze how PostgreSQL text search tokenizes and processes text.
        /// Returns token details including dictionaries consulted and resulting lexemes.
        /// Useful for debugging why certain search terms do or don't match.
        /// </summary>
        public static List<Dictionary<string, object>> Analyze(DbConnection conn, string text, string lang = "english")
        {
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "SELECT alias, description, token, dictionaries, dictionary, lexemes " +
                    "FROM ts_debug(@lang, @text)";
                AddParameter(cmd, "@lang", lang);
                AddParameter(cmd, "@text", text);

                var results = new List<Dictionary<string, object>>();
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var row = new Dictionary<string, object>();
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                        }
                        results.Add(row);
                    }
                }
                return results;
            }
        }

        /// <summary>
        /// Explain the full-text search score for a specific row.
        /// Returns document text, parsed tokens, match status, rank score, and a highlighted headline.
        /// Useful for understanding why a particular row ranked the way it did.
        /// </summary>
        public static Dictionary<string, object> ExplainScore(DbConnection conn, string table,
            string column, string query, string idColumn, object idValue, string lang = "english")
        {
            ValidateIdentifier(table);
            ValidateIdentifier(column);
            ValidateIdentifier(idColumn);

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "SELECT " + column + " AS document_text, " +
                    "to_tsvector(@lang, " + column + ")::text AS document_tokens, " +
                    "plainto_tsquery(@lang, @query)::text AS query_tokens, " +
                    "to_tsvector(@lang, " + column + ") @@ plainto_tsquery(@lang, @query) AS matches, " +
                    "ts_rank(to_tsvector(@lang, " + column + "), plainto_tsquery(@lang, @query)) AS score, " +
                    "ts_headline(@lang, " + column + ", plainto_tsquery(@lang, @query), " +
                    "'StartSel=**, StopSel=**, MaxWords=50, MinWords=20') AS headline " +
                    "FROM " + table + " WHERE " + idColumn + " = @idValue";
                AddParameter(cmd, "@lang", lang);
                AddParameter(cmd, "@query", query);
                AddParameter(cmd, "@idValue", idValue);

                using (var reader = cmd.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        var row = new Dictionary<string, object>();
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                        }
                        return row;
                    }
                    return null;
                }
            }
        }

        // ── DocX: MongoDB-like document store ────────────────────

        private static void EnsureCollection(DbConnection conn, string collection, bool unlogged = false)
        {
            ValidateIdentifier(collection);

            var prefix = unlogged ? "CREATE UNLOGGED TABLE" : "CREATE TABLE";
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    prefix + " IF NOT EXISTS " + collection + " (" +
                    "_id UUID PRIMARY KEY DEFAULT gen_random_uuid(), " +
                    "data JSONB NOT NULL, " +
                    "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), " +
                    "updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())";
                cmd.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Explicitly create a collection table. Like MongoDB createCollection().
        /// Optionally creates an UNLOGGED table for high-throughput ephemeral data.
        /// UNLOGGED tables are not crash-safe but significantly faster for writes.
        /// </summary>
        public static void DocCreateCollection(DbConnection conn, string collection, bool unlogged = false)
        {
            ValidateIdentifier(collection);
            EnsureCollection(conn, collection, unlogged);
        }

        private static Dictionary<string, object> ReadRow(DbDataReader reader)
        {
            var row = new Dictionary<string, object>();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            }
            return row;
        }

        public static Dictionary<string, object> DocInsert(DbConnection conn, string collection, string documentJson)
        {
            EnsureCollection(conn, collection);

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "INSERT INTO " + collection + " (data) VALUES (@doc::jsonb) " +
                    "RETURNING _id, data, created_at, updated_at";
                AddParameter(cmd, "@doc", documentJson);

                using (var reader = cmd.ExecuteReader())
                {
                    if (reader.Read())
                        return ReadRow(reader);
                    return new Dictionary<string, object>();
                }
            }
        }

        public static List<Dictionary<string, object>> DocInsertMany(DbConnection conn, string collection, List<string> documents)
        {
            EnsureCollection(conn, collection);

            var results = new List<Dictionary<string, object>>();
            for (int i = 0; i < documents.Count; i++)
            {
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText =
                        "INSERT INTO " + collection + " (data) VALUES (@doc::jsonb) " +
                        "RETURNING _id, data, created_at, updated_at";
                    AddParameter(cmd, "@doc", documents[i]);

                    using (var reader = cmd.ExecuteReader())
                    {
                        if (reader.Read())
                            results.Add(ReadRow(reader));
                    }
                }
            }
            return results;
        }

        public static List<Dictionary<string, object>> DocFind(DbConnection conn, string collection,
            string filterJson = null, Dictionary<string, int> sort = null, int? limit = null, int? skip = null)
        {
            ValidateIdentifier(collection);
            var filter = BuildFilter(filterJson);

            using (var cmd = conn.CreateCommand())
            {
                var sql = "SELECT _id, data, created_at, updated_at FROM " + collection;

                if (!string.IsNullOrEmpty(filter.WhereClause))
                {
                    sql += " WHERE " + filter.WhereClause;
                    ApplyFilterParams(cmd, filter);
                }

                if (sort != null && sort.Count > 0)
                {
                    var orderParts = new List<string>();
                    foreach (var kv in sort)
                    {
                        ValidateIdentifier(kv.Key);
                        var dir = kv.Value >= 0 ? "ASC" : "DESC";
                        orderParts.Add("data->>'" + kv.Key + "' " + dir);
                    }
                    sql += " ORDER BY " + string.Join(", ", orderParts);
                }

                if (limit.HasValue)
                {
                    sql += " LIMIT @limit";
                    AddParameter(cmd, "@limit", limit.Value);
                }

                if (skip.HasValue)
                {
                    sql += " OFFSET @skip";
                    AddParameter(cmd, "@skip", skip.Value);
                }

                cmd.CommandText = sql;

                var results = new List<Dictionary<string, object>>();
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        results.Add(ReadRow(reader));
                    }
                }
                return results;
            }
        }

        public static IEnumerable<Dictionary<string, object>> DocFindCursor(DbConnection conn, string collection,
            string filterJson = null, string sortJson = null, int? limit = null, int? skip = null, int batchSize = 100)
        {
            ValidateIdentifier(collection);
            var filter = BuildFilter(filterJson);

            var sql = "SELECT _id, data, created_at, updated_at FROM " + collection;
            var filterParams = new List<KeyValuePair<string, object>>();

            if (!string.IsNullOrEmpty(filter.WhereClause))
            {
                sql += " WHERE " + filter.WhereClause;
                for (int i = 0; i < filter.Params.Count; i++)
                    filterParams.Add(new KeyValuePair<string, object>("@p" + (filter.ParamOffset + i), filter.Params[i]));
            }

            if (!string.IsNullOrEmpty(sortJson))
            {
                var sortClause = ParseDataSortClause(sortJson);
                if (sortClause != null)
                    sql += " ORDER BY " + sortClause;
            }

            if (limit.HasValue)
            {
                sql += " LIMIT @limit";
                filterParams.Add(new KeyValuePair<string, object>("@limit", limit.Value));
            }

            if (skip.HasValue)
            {
                sql += " OFFSET @skip";
                filterParams.Add(new KeyValuePair<string, object>("@skip", skip.Value));
            }

            var cursorName = "gl_cursor_" + Guid.NewGuid().ToString("N").Substring(0, 8);

            // BEGIN transaction
            using (var beginCmd = conn.CreateCommand())
            {
                beginCmd.CommandText = "BEGIN";
                beginCmd.ExecuteNonQuery();
            }

            try
            {
                // DECLARE CURSOR
                using (var declareCmd = conn.CreateCommand())
                {
                    declareCmd.CommandText = "DECLARE " + cursorName + " CURSOR FOR " + sql;
                    foreach (var p in filterParams)
                        AddParameter(declareCmd, p.Key, p.Value);
                    declareCmd.ExecuteNonQuery();
                }

                // FETCH in batches
                while (true)
                {
                    var batch = new List<Dictionary<string, object>>();
                    using (var fetchCmd = conn.CreateCommand())
                    {
                        fetchCmd.CommandText = "FETCH " + batchSize + " FROM " + cursorName;
                        using (var reader = fetchCmd.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                batch.Add(ReadRow(reader));
                            }
                        }
                    }

                    if (batch.Count == 0)
                        break;

                    foreach (var row in batch)
                        yield return row;
                }
            }
            finally
            {
                // CLOSE cursor and COMMIT
                try
                {
                    using (var closeCmd = conn.CreateCommand())
                    {
                        closeCmd.CommandText = "CLOSE " + cursorName;
                        closeCmd.ExecuteNonQuery();
                    }
                }
                catch { }

                try
                {
                    using (var commitCmd = conn.CreateCommand())
                    {
                        commitCmd.CommandText = "COMMIT";
                        commitCmd.ExecuteNonQuery();
                    }
                }
                catch { }
            }
        }

        public static Dictionary<string, object> DocFindOne(DbConnection conn, string collection, string filterJson = null)
        {
            ValidateIdentifier(collection);
            var filter = BuildFilter(filterJson);

            using (var cmd = conn.CreateCommand())
            {
                var sql = "SELECT _id, data, created_at, updated_at FROM " + collection;

                if (!string.IsNullOrEmpty(filter.WhereClause))
                {
                    sql += " WHERE " + filter.WhereClause;
                    ApplyFilterParams(cmd, filter);
                }

                sql += " LIMIT 1";
                cmd.CommandText = sql;

                using (var reader = cmd.ExecuteReader())
                {
                    if (reader.Read())
                        return ReadRow(reader);
                    return null;
                }
            }
        }

        public static int DocUpdate(DbConnection conn, string collection, string filterJson, string updateJson)
        {
            ValidateIdentifier(collection);
            int paramIdx = 0;
            var update = BuildUpdate(updateJson, ref paramIdx);
            var filter = BuildFilter(filterJson, ref paramIdx);

            using (var cmd = conn.CreateCommand())
            {
                var sql = "UPDATE " + collection + " SET data = " + update.Expression + ", updated_at = NOW()";
                ApplyUpdateParams(cmd, update, 0);

                if (!string.IsNullOrEmpty(filter.WhereClause))
                {
                    sql += " WHERE " + filter.WhereClause;
                    ApplyFilterParams(cmd, filter);
                }

                cmd.CommandText = sql;
                return cmd.ExecuteNonQuery();
            }
        }

        public static int DocUpdateOne(DbConnection conn, string collection, string filterJson, string updateJson)
        {
            ValidateIdentifier(collection);
            // Filter params come first (used in subquery), then update params
            int paramIdx = 0;
            var filter = BuildFilter(filterJson, ref paramIdx);
            var update = BuildUpdate(updateJson, ref paramIdx);

            using (var cmd = conn.CreateCommand())
            {
                var sql = "UPDATE " + collection + " SET data = " + update.Expression + ", updated_at = NOW() " +
                    "WHERE _id = (SELECT _id FROM " + collection;

                ApplyFilterParams(cmd, filter);
                ApplyUpdateParams(cmd, update, filter.ParamOffset + filter.Params.Count);

                if (!string.IsNullOrEmpty(filter.WhereClause))
                {
                    sql += " WHERE " + filter.WhereClause;
                }

                sql += " LIMIT 1)";

                cmd.CommandText = sql;
                return cmd.ExecuteNonQuery();
            }
        }

        public static int DocDelete(DbConnection conn, string collection, string filterJson)
        {
            ValidateIdentifier(collection);
            var filter = BuildFilter(filterJson);

            using (var cmd = conn.CreateCommand())
            {
                var sql = "DELETE FROM " + collection;

                if (!string.IsNullOrEmpty(filter.WhereClause))
                {
                    sql += " WHERE " + filter.WhereClause;
                    ApplyFilterParams(cmd, filter);
                }

                cmd.CommandText = sql;
                return cmd.ExecuteNonQuery();
            }
        }

        public static int DocDeleteOne(DbConnection conn, string collection, string filterJson)
        {
            ValidateIdentifier(collection);
            var filter = BuildFilter(filterJson);

            using (var cmd = conn.CreateCommand())
            {
                var sql = "DELETE FROM " + collection + " WHERE _id = (" +
                    "SELECT _id FROM " + collection;

                if (!string.IsNullOrEmpty(filter.WhereClause))
                {
                    sql += " WHERE " + filter.WhereClause;
                    ApplyFilterParams(cmd, filter);
                }

                sql += " LIMIT 1)";
                cmd.CommandText = sql;
                return cmd.ExecuteNonQuery();
            }
        }

        public static long DocCount(DbConnection conn, string collection, string filterJson = null)
        {
            ValidateIdentifier(collection);
            var filter = BuildFilter(filterJson);

            using (var cmd = conn.CreateCommand())
            {
                var sql = "SELECT COUNT(*) FROM " + collection;

                if (!string.IsNullOrEmpty(filter.WhereClause))
                {
                    sql += " WHERE " + filter.WhereClause;
                    ApplyFilterParams(cmd, filter);
                }

                cmd.CommandText = sql;
                return (long)cmd.ExecuteScalar();
            }
        }

        public static Dictionary<string, object> DocFindOneAndUpdate(DbConnection conn, string collection,
            string filterJson, string updateJson)
        {
            ValidateIdentifier(collection);
            int paramIdx = 0;
            var filter = BuildFilter(filterJson, ref paramIdx);
            var update = BuildUpdate(updateJson, ref paramIdx);

            using (var cmd = conn.CreateCommand())
            {
                var cteWhere = "";
                if (!string.IsNullOrEmpty(filter.WhereClause))
                    cteWhere = " WHERE " + filter.WhereClause;

                var sql = "WITH target AS (SELECT _id FROM " + collection + cteWhere + " LIMIT 1) " +
                    "UPDATE " + collection + " SET data = " + update.Expression + ", updated_at = NOW() " +
                    "FROM target WHERE " + collection + "._id = target._id " +
                    "RETURNING " + collection + "._id, " + collection + ".data, " +
                    collection + ".created_at, " + collection + ".updated_at";

                ApplyFilterParams(cmd, filter);
                ApplyUpdateParams(cmd, update, filter.ParamOffset + filter.Params.Count);

                cmd.CommandText = sql;

                using (var reader = cmd.ExecuteReader())
                {
                    if (reader.Read())
                        return ReadRow(reader);
                    return null;
                }
            }
        }

        public static Dictionary<string, object> DocFindOneAndDelete(DbConnection conn, string collection,
            string filterJson)
        {
            ValidateIdentifier(collection);
            var filter = BuildFilter(filterJson);

            using (var cmd = conn.CreateCommand())
            {
                var cteWhere = "";
                if (!string.IsNullOrEmpty(filter.WhereClause))
                    cteWhere = " WHERE " + filter.WhereClause;

                var sql = "WITH target AS (SELECT _id FROM " + collection + cteWhere + " LIMIT 1) " +
                    "DELETE FROM " + collection + " USING target " +
                    "WHERE " + collection + "._id = target._id " +
                    "RETURNING " + collection + "._id, " + collection + ".data, " +
                    collection + ".created_at, " + collection + ".updated_at";

                ApplyFilterParams(cmd, filter);
                cmd.CommandText = sql;

                using (var reader = cmd.ExecuteReader())
                {
                    if (reader.Read())
                        return ReadRow(reader);
                    return null;
                }
            }
        }

        public static List<string> DocDistinct(DbConnection conn, string collection, string field,
            string filterJson = null)
        {
            ValidateIdentifier(collection);
            var fieldExpr = FieldPath(field);
            var filter = BuildFilter(filterJson);

            using (var cmd = conn.CreateCommand())
            {
                var sql = "SELECT DISTINCT " + fieldExpr + " FROM " + collection;
                var whereParts = new List<string> { fieldExpr + " IS NOT NULL" };

                if (!string.IsNullOrEmpty(filter.WhereClause))
                {
                    whereParts.Add(filter.WhereClause);
                    ApplyFilterParams(cmd, filter);
                }

                sql += " WHERE " + string.Join(" AND ", whereParts);
                cmd.CommandText = sql;

                var results = new List<string>();
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        results.Add(reader.IsDBNull(0) ? null : reader.GetValue(0).ToString());
                    }
                }
                return results;
            }
        }

        public static List<Dictionary<string, object>> DocAggregate(DbConnection conn, string collection, string pipelineJson)
        {
            ValidateIdentifier(collection);
            if (pipelineJson == null)
                throw new ArgumentException("Pipeline must not be null");

            var stages = ParsePipeline(pipelineJson);

            FilterResult matchResult = null;
            bool hasGroup = false;
            bool hasProject = false;
            string groupIdField = null;
            var selectExprs = new List<string>();
            var groupByExprs = new List<string>();
            string sortClause = null;
            bool sortAfterGroup = false;
            int? limitVal = null;
            int? skipVal = null;
            var unwindFields = new List<string>();
            var lookupStages = new List<Dictionary<string, string>>();
            Dictionary<string, string> projectStage = null;

            // Collect all stage data first
            var groupStageData = (Dictionary<string, string>)null;
            foreach (var stage in stages)
            {
                string stageType = stage["_type"];
                switch (stageType)
                {
                    case "$match":
                        matchResult = BuildFilter(stage["_body"]);
                        break;
                    case "$group":
                        hasGroup = true;
                        groupStageData = stage;
                        break;
                    case "$sort":
                        sortAfterGroup = hasGroup;
                        if (sortAfterGroup || hasProject)
                            sortClause = ParseAliasSortClause(stage["_body"]);
                        else
                            sortClause = ParseDataSortClause(stage["_body"]);
                        break;
                    case "$limit":
                        limitVal = int.Parse(stage["_body"]);
                        break;
                    case "$skip":
                        skipVal = int.Parse(stage["_body"]);
                        break;
                    case "$unwind":
                        unwindFields.Add(stage["_field"]);
                        break;
                    case "$lookup":
                        lookupStages.Add(stage);
                        break;
                    case "$project":
                        hasProject = true;
                        projectStage = stage;
                        break;
                    default:
                        throw new ArgumentException("Unsupported pipeline stage: " + stageType);
                }
            }

            // Build unwind map
            Dictionary<string, string> unwindMap = null;
            var fromExtras = new List<string>();
            if (unwindFields.Count > 0)
            {
                unwindMap = new Dictionary<string, string>();
                foreach (var field in unwindFields)
                {
                    var alias = "_unwound_" + field;
                    unwindMap[field] = alias;
                    fromExtras.Add("jsonb_array_elements_text(data->'" + field + "') AS " + alias);
                }
            }

            // Build group expressions
            if (groupStageData != null)
            {
                if (groupStageData.ContainsKey("_id"))
                {
                    var idRaw = groupStageData["_id"];
                    if (idRaw != null && idRaw != "null")
                    {
                        if (idRaw.StartsWith("{") && idRaw.EndsWith("}"))
                        {
                            var idBody = idRaw.Substring(1, idRaw.Length - 2).Trim();
                            var idPairs = SplitKeyValuePairs(idBody);
                            var buildParts = new List<string>();
                            foreach (var pair in idPairs)
                            {
                                var key = pair[0].Trim().Trim('"');
                                ValidateIdentifier(key);
                                var fieldRef = ExtractFieldRef(pair[1]);
                                var resolved = ResolveField(fieldRef, unwindMap);
                                buildParts.Add("'" + key + "', " + resolved);
                                groupByExprs.Add(resolved);
                            }
                            selectExprs.Add("json_build_object(" + string.Join(", ", buildParts) + ") AS _id");
                        }
                        else
                        {
                            if (idRaw.StartsWith("\"$") && idRaw.EndsWith("\""))
                                groupIdField = idRaw.Substring(2, idRaw.Length - 3);
                            else if (idRaw.StartsWith("$"))
                                groupIdField = idRaw.Substring(1);
                            else
                                groupIdField = idRaw.Trim('"');

                            ValidateFieldName(groupIdField);
                            var resolved = ResolveField(groupIdField, unwindMap);
                            selectExprs.Add(resolved + " AS _id");
                            groupByExprs.Add(resolved);
                        }
                    }
                }
                foreach (var kv in groupStageData)
                {
                    if (kv.Key == "_type" || kv.Key == "_id" || kv.Key == "_body")
                        continue;
                    ValidateIdentifier(kv.Key);
                    var accExpr = ParseAccumulator(kv.Value, unwindMap);
                    selectExprs.Add(accExpr + " AS " + kv.Key);
                }
            }

            // Build $project SELECT (overrides group select if present)
            if (projectStage != null)
            {
                var groupAliases = new HashSet<string>();
                if (hasGroup && groupStageData != null)
                {
                    foreach (var kv in groupStageData)
                    {
                        if (kv.Key == "_type" || kv.Key == "_body")
                            continue;
                        if (kv.Key == "_id")
                            groupAliases.Add("_id");
                        else
                            groupAliases.Add(kv.Key);
                    }
                }

                var projectExprs = new List<string>();
                foreach (var kv in projectStage)
                {
                    if (kv.Key == "_type" || kv.Key == "_body")
                        continue;
                    var key = kv.Key;
                    var val = kv.Value.Trim();

                    if (key == "_id" && val == "0")
                        continue;

                    ValidateIdentifier(key);

                    if (val == "1")
                    {
                        if (groupAliases.Count > 0 && groupAliases.Contains(key))
                            projectExprs.Add(key);
                        else
                            projectExprs.Add("data->>'" + key + "' AS " + key);
                    }
                    else if (val.StartsWith("\"$") && val.EndsWith("\""))
                    {
                        var refField = val.Substring(2, val.Length - 3);
                        ValidateFieldName(refField);
                        if (groupAliases.Count > 0 && groupAliases.Contains(refField))
                            projectExprs.Add(refField + " AS " + key);
                        else
                            projectExprs.Add(FieldPath(refField) + " AS " + key);
                    }
                    else if (val.StartsWith("$"))
                    {
                        var refField = val.Substring(1);
                        ValidateFieldName(refField);
                        if (groupAliases.Count > 0 && groupAliases.Contains(refField))
                            projectExprs.Add(refField + " AS " + key);
                        else
                            projectExprs.Add(FieldPath(refField) + " AS " + key);
                    }
                    else
                    {
                        throw new ArgumentException("Invalid $project value for " + key + ": " + val);
                    }
                }
                selectExprs = projectExprs;
            }

            // Append $lookup subqueries to SELECT
            foreach (var lookup in lookupStages)
            {
                var fromTable = lookup["from"];
                var localField = lookup["localField"];
                var foreignField = lookup["foreignField"];
                var asName = lookup["as"];

                var localExpr = FieldPath(localField);
                var foreignParts = foreignField.Split('.');
                string foreignExpr;
                if (foreignParts.Length == 1)
                    foreignExpr = "b.data->>'" + foreignParts[0] + "'";
                else
                {
                    foreignExpr = "b.data";
                    for (int i = 0; i < foreignParts.Length - 1; i++)
                        foreignExpr += "->'" + foreignParts[i] + "'";
                    foreignExpr += "->>'" + foreignParts[foreignParts.Length - 1] + "'";
                }

                var subquery = "COALESCE((SELECT json_agg(b.data) FROM " + fromTable + " b" +
                    " WHERE " + foreignExpr + " = " + collection + "." + localExpr +
                    "), '[]'::json) AS " + asName;
                selectExprs.Add(subquery);
            }

            using (var cmd = conn.CreateCommand())
            {
                var sql = "";
                if (selectExprs.Count > 0)
                    sql = "SELECT " + string.Join(", ", selectExprs);
                else
                    sql = "SELECT _id, data, created_at, updated_at";

                // Build FROM clause
                var fromClause = collection;
                foreach (var extra in fromExtras)
                    fromClause += ", " + extra;

                sql += " FROM " + fromClause;

                if (matchResult != null && !string.IsNullOrEmpty(matchResult.WhereClause))
                {
                    sql += " WHERE " + matchResult.WhereClause;
                    ApplyFilterParams(cmd, matchResult);
                }

                if (groupByExprs.Count > 0)
                    sql += " GROUP BY " + string.Join(", ", groupByExprs);

                if (sortClause != null)
                    sql += " ORDER BY " + sortClause;

                if (limitVal.HasValue)
                {
                    sql += " LIMIT @limit";
                    AddParameter(cmd, "@limit", limitVal.Value);
                }

                if (skipVal.HasValue)
                {
                    sql += " OFFSET @skip";
                    AddParameter(cmd, "@skip", skipVal.Value);
                }

                cmd.CommandText = sql;

                var results = new List<Dictionary<string, object>>();
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        results.Add(ReadRow(reader));
                    }
                }
                return results;
            }
        }

        public static void DocCreateIndex(DbConnection conn, string collection, List<string> keys = null)
        {
            EnsureCollection(conn, collection);

            if (keys == null || keys.Count == 0)
            {
                // Default: GIN index on the entire data column
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText =
                        "CREATE INDEX IF NOT EXISTS " + collection + "_data_gin ON " +
                        collection + " USING GIN (data)";
                    cmd.ExecuteNonQuery();
                }
            }
            else
            {
                // Create a btree index on specific JSONB keys
                foreach (var key in keys)
                    ValidateIdentifier(key);

                var indexName = collection + "_" + string.Join("_", keys) + "_idx";
                var indexExprs = string.Join(", ",
                    keys.Select(k => "(data->>'" + k + "')"));

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText =
                        "CREATE INDEX IF NOT EXISTS " + indexName + " ON " +
                        collection + " (" + indexExprs + ")";
                    cmd.ExecuteNonQuery();
                }
            }
        }

        // ── Pipeline parsing helpers ────────────────────────────────

        internal static List<Dictionary<string, string>> ParsePipeline(string pipelineJson)
        {
            var s = pipelineJson.Trim();
            if (!s.StartsWith("[") || !s.EndsWith("]"))
                throw new ArgumentException("Pipeline must be a JSON array");

            s = s.Substring(1, s.Length - 2).Trim();
            if (s.Length == 0)
                return new List<Dictionary<string, string>>();

            var objects = SplitTopLevelObjects(s);
            var stages = new List<Dictionary<string, string>>();
            foreach (var obj in objects)
                stages.Add(ParseStageObject(obj.Trim()));
            return stages;
        }

        private static List<string> SplitTopLevelObjects(string s)
        {
            var result = new List<string>();
            int depth = 0;
            int start = -1;
            for (int i = 0; i < s.Length; i++)
            {
                char c = s[i];
                if (c == '{')
                {
                    if (depth == 0) start = i;
                    depth++;
                }
                else if (c == '}')
                {
                    depth--;
                    if (depth == 0 && start >= 0)
                    {
                        result.Add(s.Substring(start, i - start + 1));
                        start = -1;
                    }
                }
            }
            return result;
        }

        private static Dictionary<string, string> ParseStageObject(string obj)
        {
            if (!obj.StartsWith("{") || !obj.EndsWith("}"))
                throw new ArgumentException("Invalid stage: " + obj);

            var body = obj.Substring(1, obj.Length - 2).Trim();
            int colonPos = body.IndexOf(':');
            if (colonPos < 0)
                throw new ArgumentException("Invalid stage: " + obj);

            var stageKey = body.Substring(0, colonPos).Trim().Trim('"');
            var stageValue = body.Substring(colonPos + 1).Trim();

            var stage = new Dictionary<string, string>();
            stage["_type"] = stageKey;

            if (stageKey == "$group")
                ParseGroupStage(stageValue, stage);
            else if (stageKey == "$sort")
                stage["_body"] = stageValue;
            else if (stageKey == "$match")
                stage["_body"] = stageValue;
            else if (stageKey == "$project")
                ParseProjectStage(stageValue, stage);
            else if (stageKey == "$unwind")
                ParseUnwindStage(stageValue, stage);
            else if (stageKey == "$lookup")
                ParseLookupStage(stageValue, stage);
            else
                stage["_body"] = stageValue.Trim();

            return stage;
        }

        private static void ParseGroupStage(string value, Dictionary<string, string> stage)
        {
            if (!value.StartsWith("{") || !value.EndsWith("}"))
                throw new ArgumentException("$group value must be an object");

            var body = value.Substring(1, value.Length - 2).Trim();
            var pairs = SplitKeyValuePairs(body);
            foreach (var kv in pairs)
            {
                var key = kv[0].Trim().Trim('"');
                var val = kv[1].Trim();
                if (key == "_id")
                    stage["_id"] = val;
                else
                    stage[key] = val;
            }
        }

        private static void ParseProjectStage(string value, Dictionary<string, string> stage)
        {
            if (!value.StartsWith("{") || !value.EndsWith("}"))
                throw new ArgumentException("$project value must be an object");

            var body = value.Substring(1, value.Length - 2).Trim();
            var pairs = SplitKeyValuePairs(body);
            foreach (var kv in pairs)
            {
                var key = kv[0].Trim().Trim('"');
                var val = kv[1].Trim();
                stage[key] = val;
            }
        }

        private static void ParseUnwindStage(string value, Dictionary<string, string> stage)
        {
            var v = value.Trim();
            string path;
            if (v.StartsWith("{"))
            {
                // Object form: {"path": "$tags"}
                if (!v.EndsWith("}"))
                    throw new ArgumentException("Invalid $unwind value");
                var body = v.Substring(1, v.Length - 2).Trim();
                var pairs = SplitKeyValuePairs(body);
                path = null;
                foreach (var kv in pairs)
                {
                    var key = kv[0].Trim().Trim('"');
                    if (key == "path")
                    {
                        path = kv[1].Trim().Trim('"');
                        break;
                    }
                }
                if (path == null)
                    throw new ArgumentException("$unwind object must have a 'path' field");
            }
            else
            {
                // String form: "$tags"
                path = v.Trim('"');
            }

            if (!path.StartsWith("$"))
                throw new ArgumentException("$unwind path must be a string starting with '$': " + path);

            var field = path.Substring(1);
            ValidateFieldName(field);
            stage["_field"] = field;
        }

        private static void ParseLookupStage(string value, Dictionary<string, string> stage)
        {
            if (!value.StartsWith("{") || !value.EndsWith("}"))
                throw new ArgumentException("$lookup value must be an object");

            var body = value.Substring(1, value.Length - 2).Trim();
            var pairs = SplitKeyValuePairs(body);
            foreach (var kv in pairs)
            {
                var key = kv[0].Trim().Trim('"');
                var val = kv[1].Trim().Trim('"');
                stage[key] = val;
            }

            var requiredFields = new[] { "from", "localField", "foreignField", "as" };
            foreach (var required in requiredFields)
            {
                if (!stage.ContainsKey(required))
                    throw new ArgumentException("$lookup missing required field: " + required);
            }

            ValidateIdentifier(stage["from"]);
            ValidateFieldName(stage["localField"]);
            ValidateFieldName(stage["foreignField"]);
            ValidateIdentifier(stage["as"]);
        }

        private static List<string[]> SplitKeyValuePairs(string body)
        {
            var pairs = new List<string[]>();
            int i = 0;
            while (i < body.Length)
            {
                // Skip whitespace and commas
                while (i < body.Length && (body[i] == ',' || body[i] == ' '
                    || body[i] == '\n' || body[i] == '\r' || body[i] == '\t'))
                    i++;

                if (i >= body.Length) break;

                // Find key
                int keyStart, keyEnd;
                if (body[i] == '"')
                {
                    keyStart = i;
                    i++;
                    while (i < body.Length && body[i] != '"')
                    {
                        if (body[i] == '\\') i++;
                        i++;
                    }
                    keyEnd = i + 1;
                    i++;
                }
                else
                {
                    keyStart = i;
                    while (i < body.Length && body[i] != ':') i++;
                    keyEnd = i;
                }
                var key = body.Substring(keyStart, keyEnd - keyStart).Trim();

                // Skip colon
                while (i < body.Length && body[i] != ':') i++;
                i++; // skip ':'

                // Skip whitespace
                while (i < body.Length && (body[i] == ' ' || body[i] == '\t')) i++;

                // Find value (could be object, array, string, number, null)
                int valStart = i;
                if (i < body.Length && body[i] == '{')
                {
                    int depth = 0;
                    while (i < body.Length)
                    {
                        if (body[i] == '{') depth++;
                        else if (body[i] == '}') { depth--; if (depth == 0) { i++; break; } }
                        i++;
                    }
                }
                else if (i < body.Length && body[i] == '[')
                {
                    int depth = 0;
                    while (i < body.Length)
                    {
                        if (body[i] == '[') depth++;
                        else if (body[i] == ']') { depth--; if (depth == 0) { i++; break; } }
                        i++;
                    }
                }
                else if (i < body.Length && body[i] == '"')
                {
                    i++;
                    while (i < body.Length && body[i] != '"')
                    {
                        if (body[i] == '\\') i++; // skip escaped char
                        i++;
                    }
                    i++; // closing quote
                }
                else
                {
                    // number or null or bare token
                    while (i < body.Length && body[i] != ',' && body[i] != '}'
                        && body[i] != ' ' && body[i] != '\n')
                        i++;
                }
                var val = body.Substring(valStart, i - valStart).Trim();
                pairs.Add(new string[] { key, val });
            }
            return pairs;
        }

        private static string ParseAccumulator(string accJson, Dictionary<string, string> unwindMap = null)
        {
            var s = accJson.Trim();
            if (!s.StartsWith("{") || !s.EndsWith("}"))
                throw new ArgumentException("Accumulator must be an object: " + accJson);

            var inner = s.Substring(1, s.Length - 2).Trim();
            int colonPos = inner.IndexOf(':');
            if (colonPos < 0)
                throw new ArgumentException("Invalid accumulator: " + accJson);

            var op = inner.Substring(0, colonPos).Trim().Trim('"');
            var arg = inner.Substring(colonPos + 1).Trim();

            switch (op)
            {
                case "$sum":
                    if (arg == "1")
                        return "COUNT(*)";
                    else
                    {
                        var field = ExtractFieldRef(arg);
                        var resolved = ResolveField(field, unwindMap);
                        if (unwindMap != null && unwindMap.ContainsKey(field))
                            return "SUM(" + resolved + "::numeric)";
                        return "SUM((" + resolved + ")::numeric)";
                    }
                case "$avg":
                {
                    var field = ExtractFieldRef(arg);
                    var resolved = ResolveField(field, unwindMap);
                    if (unwindMap != null && unwindMap.ContainsKey(field))
                        return "AVG(" + resolved + "::numeric)";
                    return "AVG((" + resolved + ")::numeric)";
                }
                case "$min":
                {
                    var field = ExtractFieldRef(arg);
                    var resolved = ResolveField(field, unwindMap);
                    if (unwindMap != null && unwindMap.ContainsKey(field))
                        return "MIN(" + resolved + "::numeric)";
                    return "MIN((" + resolved + ")::numeric)";
                }
                case "$max":
                {
                    var field = ExtractFieldRef(arg);
                    var resolved = ResolveField(field, unwindMap);
                    if (unwindMap != null && unwindMap.ContainsKey(field))
                        return "MAX(" + resolved + "::numeric)";
                    return "MAX((" + resolved + ")::numeric)";
                }
                case "$count":
                    return "COUNT(*)";
                case "$push":
                {
                    var field = ExtractFieldRef(arg);
                    var resolved = ResolveField(field, unwindMap);
                    return "array_agg(" + resolved + ")";
                }
                case "$addToSet":
                {
                    var field = ExtractFieldRef(arg);
                    var resolved = ResolveField(field, unwindMap);
                    return "array_agg(DISTINCT " + resolved + ")";
                }
                default:
                    throw new ArgumentException("Unsupported accumulator: " + op);
            }
        }

        private static string ExtractFieldRef(string arg)
        {
            var s = arg.Trim().Trim('"');
            if (!s.StartsWith("$"))
                throw new ArgumentException("Accumulator field must be a $reference: " + arg);
            var field = s.Substring(1);
            ValidateFieldName(field);
            return field;
        }

        private static string ResolveField(string field, Dictionary<string, string> unwindMap)
        {
            if (unwindMap != null && unwindMap.ContainsKey(field))
                return unwindMap[field];
            return FieldPath(field);
        }

        private static void ValidateFieldName(string name)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentException("Field name must be a non-empty string");
            var parts = name.Split('.');
            foreach (var part in parts)
            {
                if (!IdentifierPattern.IsMatch(part))
                    throw new ArgumentException("Invalid field name: " + name);
            }
        }

        private static string ParseAliasSortClause(string sortJson)
        {
            if (string.IsNullOrEmpty(sortJson))
                return null;
            var body = sortJson.Trim();
            if (!body.StartsWith("{") || !body.EndsWith("}"))
                throw new ArgumentException("Sort must be a JSON object");
            body = body.Substring(1, body.Length - 2).Trim();
            if (body.Length == 0)
                return null;

            var parts = new List<string>();
            var pairs = body.Split(',');
            foreach (var pair in pairs)
            {
                var kv = pair.Split(':');
                if (kv.Length != 2)
                    throw new ArgumentException("Invalid sort entry: " + pair.Trim());
                var key = kv[0].Trim().Trim('"');
                ValidateIdentifier(key);
                int dir = int.Parse(kv[1].Trim());
                parts.Add(key + (dir == 1 ? " ASC" : " DESC"));
            }
            return string.Join(", ", parts);
        }

        private static string ParseDataSortClause(string sortJson)
        {
            if (string.IsNullOrEmpty(sortJson))
                return null;
            var body = sortJson.Trim();
            if (!body.StartsWith("{") || !body.EndsWith("}"))
                throw new ArgumentException("Sort must be a JSON object");
            body = body.Substring(1, body.Length - 2).Trim();
            if (body.Length == 0)
                return null;

            var parts = new List<string>();
            var pairs = body.Split(',');
            foreach (var pair in pairs)
            {
                var kv = pair.Split(':');
                if (kv.Length != 2)
                    throw new ArgumentException("Invalid sort entry: " + pair.Trim());
                var key = kv[0].Trim().Trim('"');
                ValidateIdentifier(key);
                int dir = int.Parse(kv[1].Trim());
                parts.Add("data->>'" + key + "' " + (dir == 1 ? "ASC" : "DESC"));
            }
            return string.Join(", ", parts);
        }

        private static readonly Regex IdentifierPattern = new Regex(@"^[a-zA-Z_][a-zA-Z0-9_]*$");
        private static readonly Regex FieldPartPattern = new Regex(@"^[a-zA-Z_][a-zA-Z0-9_]*$");

        private static readonly Dictionary<string, string> ComparisonOps = new Dictionary<string, string>
        {
            { "$gt", ">" }, { "$gte", ">=" }, { "$lt", "<" }, { "$lte", "<=" },
            { "$eq", "=" }, { "$ne", "!=" }
        };

        private static readonly HashSet<string> SupportedFilterOps = new HashSet<string>
        {
            "$gt", "$gte", "$lt", "$lte", "$eq", "$ne", "$in", "$nin", "$exists", "$regex",
            "$elemMatch", "$text"
        };

        private static readonly HashSet<string> LogicalOps = new HashSet<string>
        {
            "$or", "$and", "$not"
        };

        private static readonly HashSet<string> UpdateOps = new HashSet<string>
        {
            "$set", "$unset", "$inc", "$mul", "$rename", "$push", "$pull", "$addToSet"
        };

        internal class FilterResult
        {
            public string WhereClause { get; }
            public List<object> Params { get; }
            public int ParamOffset { get; }

            public FilterResult(string whereClause, List<object> parameters, int paramOffset = 0)
            {
                WhereClause = whereClause;
                Params = parameters;
                ParamOffset = paramOffset;
            }
        }

        internal static string FieldPath(string key)
        {
            var parts = key.Split('.');
            foreach (var part in parts)
            {
                if (!FieldPartPattern.IsMatch(part))
                    throw new ArgumentException("Invalid filter key: " + key);
            }
            if (parts.Length == 1)
                return "data->>'" + parts[0] + "'";

            var sb = new System.Text.StringBuilder("data");
            for (int i = 0; i < parts.Length - 1; i++)
                sb.Append("->'").Append(parts[i]).Append("'");
            sb.Append("->>'").Append(parts[parts.Length - 1]).Append("'");
            return sb.ToString();
        }

        internal static string FieldPathJson(string key)
        {
            var parts = key.Split('.');
            foreach (var part in parts)
            {
                if (!FieldPartPattern.IsMatch(part))
                    throw new ArgumentException("Invalid field key: " + key);
            }
            var sb = new System.Text.StringBuilder("data");
            for (int i = 0; i < parts.Length; i++)
                sb.Append("->'").Append(parts[i]).Append("'");
            return sb.ToString();
        }

        internal static string JsonbPath(string key)
        {
            var parts = key.Split('.');
            foreach (var part in parts)
            {
                if (!FieldPartPattern.IsMatch(part))
                    throw new ArgumentException("Invalid field key: " + key);
            }
            return "{" + string.Join(",", parts) + "}";
        }

        internal static string ToJsonbExpr(string value, ref int paramIdx)
        {
            var trimmed = value.Trim();
            var pName = "@p" + paramIdx++;
            if (trimmed == "true" || trimmed == "false")
                return "to_jsonb(" + pName + "::boolean)";
            if (IsNumericLiteral(trimmed))
                return "to_jsonb(" + pName + "::numeric)";
            if (trimmed.StartsWith("\"") && trimmed.EndsWith("\""))
                return "to_jsonb(" + pName + "::text)";
            // JSON object or array
            return pName + "::jsonb";
        }

        internal static object ToJsonbValue(string value)
        {
            var trimmed = value.Trim();
            if (trimmed == "true") return true;
            if (trimmed == "false") return false;
            if (IsNumericLiteral(trimmed))
                return double.Parse(trimmed, CultureInfo.InvariantCulture);
            if (trimmed.StartsWith("\"") && trimmed.EndsWith("\""))
                return Unquote(trimmed);
            return trimmed; // raw JSON
        }

        internal class UpdateResult
        {
            public string Expression { get; }
            public List<object> Params { get; }

            public UpdateResult(string expression, List<object> parameters)
            {
                Expression = expression;
                Params = parameters;
            }
        }

        internal static UpdateResult BuildUpdate(string updateJson)
        {
            int paramIdx = 0;
            return BuildUpdate(updateJson, ref paramIdx);
        }

        internal static UpdateResult BuildUpdate(string updateJson, ref int paramIdx)
        {
            if (string.IsNullOrWhiteSpace(updateJson))
                throw new ArgumentException("Update must be a JSON object");

            var s = updateJson.Trim();
            if (!s.StartsWith("{") || !s.EndsWith("}"))
                throw new ArgumentException("Update must be a JSON object");

            var body = s.Substring(1, s.Length - 2).Trim();
            if (body.Length == 0)
                throw new ArgumentException("Update must be a non-empty JSON object");

            var pairs = SplitKeyValuePairs(body);

            // Check if any top-level key starts with $
            bool hasOps = false;
            foreach (var kv in pairs)
            {
                var key = kv[0].Trim().Trim('"');
                if (key.StartsWith("$")) { hasOps = true; break; }
            }

            if (!hasOps)
            {
                // Plain merge update: data || @pN::jsonb
                var pName = "@p" + paramIdx++;
                return new UpdateResult("data || " + pName + "::jsonb", new List<object> { s });
            }

            var expr = "data";
            var allParams = new List<object>();

            foreach (var kv in pairs)
            {
                var op = kv[0].Trim().Trim('"');
                var val = kv[1].Trim();

                if (op == "$set")
                {
                    var pName = "@p" + paramIdx++;
                    expr = "(" + expr + " || " + pName + "::jsonb)";
                    allParams.Add(val);
                }
                else if (op == "$unset")
                {
                    var unsetBody = val.Substring(1, val.Length - 2).Trim();
                    var unsetPairs = SplitKeyValuePairs(unsetBody);
                    foreach (var up in unsetPairs)
                    {
                        var field = up[0].Trim().Trim('"');
                        var parts = field.Split('.');
                        foreach (var part in parts)
                        {
                            if (!FieldPartPattern.IsMatch(part))
                                throw new ArgumentException("Invalid field key: " + field);
                        }
                        if (parts.Length == 1)
                        {
                            var pName = "@p" + paramIdx++;
                            expr = "(" + expr + " - " + pName + ")";
                            allParams.Add(field);
                        }
                        else
                        {
                            var path = "{" + string.Join(",", parts) + "}";
                            var pName = "@p" + paramIdx++;
                            expr = "(" + expr + " #- " + pName + "::text[])";
                            allParams.Add(path);
                        }
                    }
                }
                else if (op == "$inc")
                {
                    var incBody = val.Substring(1, val.Length - 2).Trim();
                    var incPairs = SplitKeyValuePairs(incBody);
                    foreach (var ip in incPairs)
                    {
                        var field = ip[0].Trim().Trim('"');
                        var amount = ip[1].Trim();
                        var jp = JsonbPath(field);
                        var fp = FieldPath(field);
                        var pJp = "@p" + paramIdx++;
                        var pAmt = "@p" + paramIdx++;
                        expr = "jsonb_set(" + expr + ", " + pJp + "::text[], to_jsonb(COALESCE((" + fp + ")::numeric, 0) + " + pAmt + "))";
                        allParams.Add(jp);
                        allParams.Add(double.Parse(amount, CultureInfo.InvariantCulture));
                    }
                }
                else if (op == "$mul")
                {
                    var mulBody = val.Substring(1, val.Length - 2).Trim();
                    var mulPairs = SplitKeyValuePairs(mulBody);
                    foreach (var mp in mulPairs)
                    {
                        var field = mp[0].Trim().Trim('"');
                        var factor = mp[1].Trim();
                        var jp = JsonbPath(field);
                        var fp = FieldPath(field);
                        var pJp = "@p" + paramIdx++;
                        var pFact = "@p" + paramIdx++;
                        expr = "jsonb_set(" + expr + ", " + pJp + "::text[], to_jsonb(COALESCE((" + fp + ")::numeric, 0) * " + pFact + "))";
                        allParams.Add(jp);
                        allParams.Add(double.Parse(factor, CultureInfo.InvariantCulture));
                    }
                }
                else if (op == "$rename")
                {
                    var renBody = val.Substring(1, val.Length - 2).Trim();
                    var renPairs = SplitKeyValuePairs(renBody);
                    foreach (var rp in renPairs)
                    {
                        var oldName = rp[0].Trim().Trim('"');
                        var newName = Unquote(rp[1].Trim());
                        foreach (var part in oldName.Split('.'))
                        {
                            if (!FieldPartPattern.IsMatch(part))
                                throw new ArgumentException("Invalid field key: " + oldName);
                        }
                        foreach (var part in newName.Split('.'))
                        {
                            if (!FieldPartPattern.IsMatch(part))
                                throw new ArgumentException("Invalid field key: " + newName);
                        }
                        var oldJson = FieldPathJson(oldName);
                        var newJp = JsonbPath(newName);
                        if (oldName.Contains("."))
                        {
                            var oldPath = "{" + string.Join(",", oldName.Split('.')) + "}";
                            var pOld = "@p" + paramIdx++;
                            var pNew = "@p" + paramIdx++;
                            expr = "jsonb_set((" + expr + " #- " + pOld + "::text[]), " + pNew + "::text[], " + oldJson + ")";
                            allParams.Add(oldPath);
                            allParams.Add(newJp);
                        }
                        else
                        {
                            var pOld = "@p" + paramIdx++;
                            var pNew = "@p" + paramIdx++;
                            expr = "jsonb_set((" + expr + " - " + pOld + "), " + pNew + "::text[], " + oldJson + ")";
                            allParams.Add(oldName);
                            allParams.Add(newJp);
                        }
                    }
                }
                else if (op == "$push")
                {
                    var pushBody = val.Substring(1, val.Length - 2).Trim();
                    var pushPairs = SplitKeyValuePairs(pushBody);
                    foreach (var pp in pushPairs)
                    {
                        var field = pp[0].Trim().Trim('"');
                        var pushVal = pp[1].Trim();
                        var jp = JsonbPath(field);
                        var fj = FieldPathJson(field);
                        var pJp = "@p" + paramIdx++;
                        var valExpr = ToJsonbExpr(pushVal, ref paramIdx);
                        expr = "jsonb_set(" + expr + ", " + pJp + "::text[], COALESCE(" + fj + ", '[]'::jsonb) || " + valExpr + ")";
                        allParams.Add(jp);
                        allParams.Add(ToJsonbValue(pushVal));
                    }
                }
                else if (op == "$pull")
                {
                    var pullBody = val.Substring(1, val.Length - 2).Trim();
                    var pullPairs = SplitKeyValuePairs(pullBody);
                    foreach (var pp in pullPairs)
                    {
                        var field = pp[0].Trim().Trim('"');
                        var pullVal = pp[1].Trim();
                        var jp = JsonbPath(field);
                        var fj = FieldPathJson(field);
                        var pJp = "@p" + paramIdx++;
                        var valExpr = ToJsonbExpr(pullVal, ref paramIdx);
                        expr = "jsonb_set(" + expr + ", " + pJp + "::text[], " +
                            "COALESCE((SELECT jsonb_agg(elem) FROM jsonb_array_elements(" + fj + ") AS elem " +
                            "WHERE elem != " + valExpr + "), '[]'::jsonb))";
                        allParams.Add(jp);
                        allParams.Add(ToJsonbValue(pullVal));
                    }
                }
                else if (op == "$addToSet")
                {
                    var atsBody = val.Substring(1, val.Length - 2).Trim();
                    var atsPairs = SplitKeyValuePairs(atsBody);
                    foreach (var ap in atsPairs)
                    {
                        var field = ap[0].Trim().Trim('"');
                        var atsVal = ap[1].Trim();
                        var jp = JsonbPath(field);
                        var fj = FieldPathJson(field);
                        var pJp = "@p" + paramIdx++;
                        // $addToSet uses the value in 2 places (containment check + append)
                        var valExpr1 = ToJsonbExpr(atsVal, ref paramIdx);
                        var valExpr2 = ToJsonbExpr(atsVal, ref paramIdx);
                        expr = "jsonb_set(" + expr + ", " + pJp + "::text[], " +
                            "CASE WHEN COALESCE(" + fj + ", '[]'::jsonb) @> " + valExpr1 + " " +
                            "THEN " + fj + " " +
                            "ELSE COALESCE(" + fj + ", '[]'::jsonb) || " + valExpr2 + " END)";
                        allParams.Add(jp);
                        var v = ToJsonbValue(atsVal);
                        allParams.Add(v);
                        allParams.Add(v);
                    }
                }
            }

            return new UpdateResult(expr, allParams);
        }

        internal static FilterResult BuildFilter(string filterJson)
        {
            int paramIdx = 0;
            return BuildFilter(filterJson, ref paramIdx);
        }

        internal static FilterResult BuildFilter(string filterJson, ref int paramIdx)
        {
            int startIdx = paramIdx;
            if (string.IsNullOrWhiteSpace(filterJson))
                return new FilterResult("", new List<object>(), startIdx);

            var s = filterJson.Trim();
            if (!s.StartsWith("{") || !s.EndsWith("}"))
                throw new ArgumentException("Filter must be a JSON object");

            var body = s.Substring(1, s.Length - 2).Trim();
            if (body.Length == 0)
                return new FilterResult("", new List<object>(), startIdx);

            var pairs = SplitKeyValuePairs(body);

            // Check for logical operators ($or, $and, $not), $text, and field operators
            bool hasLogicalOps = false;
            bool hasFieldOperators = false;
            bool hasTopLevelText = false;
            foreach (var kv in pairs)
            {
                var key = kv[0].Trim().Trim('"');
                if (LogicalOps.Contains(key))
                {
                    hasLogicalOps = true;
                }
                else if (key == "$text")
                {
                    hasTopLevelText = true;
                }
                var val = kv[1].Trim();
                if (val.StartsWith("{") && HasOperatorKeys(val))
                {
                    hasFieldOperators = true;
                }
            }

            // Fast path: no operator keys, no logical ops, no $text => pure containment
            if (!hasFieldOperators && !hasLogicalOps && !hasTopLevelText)
            {
                var parms = new List<object>();
                bool hasDot = false;
                foreach (var kv in pairs)
                {
                    var key = kv[0].Trim().Trim('"');
                    if (key.Contains(".")) { hasDot = true; break; }
                }
                var pName = "@p" + paramIdx++;
                if (hasDot)
                {
                    var flat = new Dictionary<string, string>();
                    foreach (var kv in pairs)
                        flat[kv[0].Trim().Trim('"')] = kv[1].Trim();
                    parms.Add(ExpandDotKeys(flat));
                }
                else
                {
                    parms.Add(s);
                }
                return new FilterResult("data @> " + pName + "::jsonb", parms, startIdx);
            }

            var containment = new Dictionary<string, string>();
            var allClauses = new List<string>();
            var allParams = new List<object>();

            // Separate containment keys from operator/logical/$text keys
            var operatorKeys = new List<string[]>();
            var logicalKeys = new List<string[]>();
            var textKeys = new List<string[]>();
            foreach (var kv in pairs)
            {
                var key = kv[0].Trim().Trim('"');
                var val = kv[1].Trim();
                if (LogicalOps.Contains(key))
                    logicalKeys.Add(kv);
                else if (key == "$text")
                    textKeys.Add(kv);
                else if (val.StartsWith("{") && HasOperatorKeys(val))
                    operatorKeys.Add(kv);
                else
                    containment[key] = val;
            }

            // Containment clause first
            if (containment.Count > 0)
            {
                var pName = "@p" + paramIdx++;
                allClauses.Add("data @> " + pName + "::jsonb");
                bool hasDot = false;
                foreach (var key in containment.Keys)
                {
                    if (key.Contains(".")) { hasDot = true; break; }
                }
                allParams.Add(hasDot ? ExpandDotKeys(containment) : RebuildJsonObject(containment));
            }

            // Operator clauses
            foreach (var kv in operatorKeys)
            {
                var key = kv[0].Trim().Trim('"');
                var val = kv[1].Trim();
                var fieldExpr = FieldPath(key);
                var opPairs = SplitKeyValuePairs(
                    val.Substring(1, val.Length - 2).Trim());

                foreach (var opKv in opPairs)
                {
                    var op = opKv[0].Trim().Trim('"');
                    var operand = opKv[1].Trim();

                    if (!SupportedFilterOps.Contains(op))
                        throw new ArgumentException("Unsupported filter operator: " + op);

                    if (ComparisonOps.ContainsKey(op))
                    {
                        var sqlOp = ComparisonOps[op];
                        var pName = "@p" + paramIdx++;
                        if (IsNumericLiteral(operand))
                        {
                            allClauses.Add("(" + fieldExpr + ")::numeric " + sqlOp + " " + pName);
                            allParams.Add(double.Parse(operand, CultureInfo.InvariantCulture));
                        }
                        else
                        {
                            var strVal = Unquote(operand);
                            allClauses.Add(fieldExpr + " " + sqlOp + " " + pName);
                            allParams.Add(strVal);
                        }
                    }
                    else if (op == "$in" || op == "$nin")
                    {
                        var elements = ParseJsonArray(operand);
                        if (elements.Count == 0)
                        {
                            if (op == "$in")
                                allClauses.Add("FALSE");
                        }
                        else
                        {
                            var placeholders = new List<string>();
                            foreach (var elem in elements)
                            {
                                var pName = "@p" + paramIdx++;
                                placeholders.Add(pName);
                                allParams.Add(Unquote(elem.Trim()));
                            }
                            var notPrefix = op == "$nin" ? "NOT " : "";
                            allClauses.Add(fieldExpr + " " + notPrefix + "IN (" +
                                string.Join(", ", placeholders) + ")");
                        }
                    }
                    else if (op == "$exists")
                    {
                        var topKey = key.Split('.')[0];
                        var pName = "@p" + paramIdx++;
                        bool exists = operand == "true";
                        if (exists)
                            allClauses.Add("data ?? " + pName);
                        else
                            allClauses.Add("NOT (data ?? " + pName + ")");
                        allParams.Add(topKey);
                    }
                    else if (op == "$regex")
                    {
                        var pattern = Unquote(operand);
                        var pName = "@p" + paramIdx++;
                        allClauses.Add(fieldExpr + " ~ " + pName);
                        allParams.Add(pattern);
                    }
                    else if (op == "$elemMatch")
                    {
                        if (!operand.StartsWith("{") || !operand.EndsWith("}"))
                            throw new ArgumentException("$elemMatch value must be an object");
                        var fieldJson = FieldPathJson(key);
                        var elemBody = operand.Substring(1, operand.Length - 2).Trim();
                        var subPairs = SplitKeyValuePairs(elemBody);
                        var elemClauses = new List<string>();
                        foreach (var sp in subPairs)
                        {
                            var subOp = sp[0].Trim().Trim('"');
                            var subVal = sp[1].Trim();
                            if (ComparisonOps.ContainsKey(subOp))
                            {
                                var sqlOp = ComparisonOps[subOp];
                                var pName = "@p" + paramIdx++;
                                if (IsNumericLiteral(subVal))
                                {
                                    elemClauses.Add("(elem#>>'{}')::numeric " + sqlOp + " " + pName);
                                    allParams.Add(double.Parse(subVal, CultureInfo.InvariantCulture));
                                }
                                else
                                {
                                    elemClauses.Add("elem#>>'{}' " + sqlOp + " " + pName);
                                    allParams.Add(Unquote(subVal));
                                }
                            }
                            else if (subOp == "$regex")
                            {
                                var pName = "@p" + paramIdx++;
                                elemClauses.Add("elem#>>'{}' ~ " + pName);
                                allParams.Add(Unquote(subVal));
                            }
                            else
                            {
                                throw new ArgumentException("Unsupported $elemMatch operator: " + subOp);
                            }
                        }
                        if (elemClauses.Count > 0)
                        {
                            allClauses.Add(
                                "EXISTS (SELECT 1 FROM jsonb_array_elements(" + fieldJson +
                                ") AS elem WHERE " + string.Join(" AND ", elemClauses) + ")");
                        }
                    }
                    else if (op == "$text")
                    {
                        if (!operand.StartsWith("{") || !operand.EndsWith("}"))
                            throw new ArgumentException("$text requires {$search: 'query'}");
                        var textBody = operand.Substring(1, operand.Length - 2).Trim();
                        var textPairs = SplitKeyValuePairs(textBody);
                        string searchVal = null;
                        string langVal = "english";
                        foreach (var tp in textPairs)
                        {
                            var tk = tp[0].Trim().Trim('"');
                            var tv = tp[1].Trim();
                            if (tk == "$search") searchVal = Unquote(tv);
                            else if (tk == "$language") langVal = Unquote(tv);
                        }
                        if (searchVal == null)
                            throw new ArgumentException("$text requires {$search: 'query'}");
                        var pLang = "@p" + paramIdx++;
                        var pLang2 = "@p" + paramIdx++;
                        var pSearch = "@p" + paramIdx++;
                        allClauses.Add("to_tsvector(" + pLang + ", " + fieldExpr + ") @@ plainto_tsquery(" + pLang2 + ", " + pSearch + ")");
                        allParams.Add(langVal);
                        allParams.Add(langVal);
                        allParams.Add(searchVal);
                    }
                }
            }

            // Logical operator clauses
            foreach (var kv in logicalKeys)
            {
                var key = kv[0].Trim().Trim('"');
                var val = kv[1].Trim();

                if (key == "$not")
                {
                    if (!val.StartsWith("{") || !val.EndsWith("}"))
                        throw new ArgumentException("$not value must be a filter object");
                    var subResult = BuildFilter(val, ref paramIdx);
                    if (!string.IsNullOrEmpty(subResult.WhereClause))
                    {
                        allClauses.Add("NOT (" + subResult.WhereClause + ")");
                        allParams.AddRange(subResult.Params);
                    }
                }
                else if (key == "$or" || key == "$and")
                {
                    if (!val.StartsWith("[") || !val.EndsWith("]"))
                        throw new ArgumentException(key + " value must be a non-empty array");
                    var elements = ParseJsonArray(val);
                    if (elements.Count == 0)
                        throw new ArgumentException(key + " value must be a non-empty array");

                    var joiner = key == "$or" ? " OR " : " AND ";
                    var subClauses = new List<string>();
                    foreach (var elem in elements)
                    {
                        var subResult = BuildFilter(elem.Trim(), ref paramIdx);
                        if (!string.IsNullOrEmpty(subResult.WhereClause))
                        {
                            subClauses.Add(subResult.WhereClause);
                            allParams.AddRange(subResult.Params);
                        }
                    }
                    if (subClauses.Count > 0)
                        allClauses.Add("(" + string.Join(joiner, subClauses) + ")");
                }
            }

            // Top-level $text clauses
            foreach (var kv in textKeys)
            {
                var val = kv[1].Trim();
                if (!val.StartsWith("{") || !val.EndsWith("}"))
                    throw new ArgumentException("$text requires {$search: 'query'}");
                var textBody = val.Substring(1, val.Length - 2).Trim();
                var textPairs = SplitKeyValuePairs(textBody);
                string searchVal = null;
                string langVal = "english";
                foreach (var tp in textPairs)
                {
                    var tk = tp[0].Trim().Trim('"');
                    var tv = tp[1].Trim();
                    if (tk == "$search") searchVal = Unquote(tv);
                    else if (tk == "$language") langVal = Unquote(tv);
                }
                if (searchVal == null)
                    throw new ArgumentException("$text requires {$search: 'query'}");
                var pLang = "@p" + paramIdx++;
                var pLang2 = "@p" + paramIdx++;
                var pSearch = "@p" + paramIdx++;
                allClauses.Add("to_tsvector(" + pLang + ", data::text) @@ plainto_tsquery(" + pLang2 + ", " + pSearch + ")");
                allParams.Add(langVal);
                allParams.Add(langVal);
                allParams.Add(searchVal);
            }

            if (allClauses.Count == 0)
                return new FilterResult("", allParams, startIdx);
            return new FilterResult(string.Join(" AND ", allClauses), allParams, startIdx);
        }

        private static bool HasOperatorKeys(string objStr)
        {
            var inner = objStr.Substring(1, objStr.Length - 2).Trim();
            if (inner.Length == 0) return false;
            string firstKey;
            if (inner[0] == '"')
            {
                int end = inner.IndexOf('"', 1);
                if (end < 0) return false;
                firstKey = inner.Substring(1, end - 1);
            }
            else
            {
                int colon = inner.IndexOf(':');
                if (colon < 0) return false;
                firstKey = inner.Substring(0, colon).Trim();
            }
            return firstKey.StartsWith("$");
        }

        private static bool IsNumericLiteral(string s)
        {
            if (string.IsNullOrEmpty(s)) return false;
            double _;
            return double.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out _);
        }

        private static string Unquote(string s)
        {
            if (s.Length >= 2 && s.StartsWith("\"") && s.EndsWith("\""))
                return s.Substring(1, s.Length - 2);
            return s;
        }

        private static List<string> ParseJsonArray(string s)
        {
            s = s.Trim();
            if (!s.StartsWith("[") || !s.EndsWith("]"))
                throw new ArgumentException("Expected JSON array: " + s);

            var inner = s.Substring(1, s.Length - 2).Trim();
            if (inner.Length == 0)
                return new List<string>();

            var elements = new List<string>();
            int depth = 0;
            int start = 0;
            bool inString = false;
            for (int i = 0; i < inner.Length; i++)
            {
                char c = inner[i];
                if (c == '"' && (i == 0 || inner[i - 1] != '\\'))
                    inString = !inString;
                else if (!inString)
                {
                    if (c == '{' || c == '[') depth++;
                    else if (c == '}' || c == ']') depth--;
                    else if (c == ',' && depth == 0)
                    {
                        elements.Add(inner.Substring(start, i - start).Trim());
                        start = i + 1;
                    }
                }
            }
            elements.Add(inner.Substring(start).Trim());
            return elements;
        }

        private static string RebuildJsonObject(Dictionary<string, string> pairs)
        {
            var sb = new System.Text.StringBuilder("{");
            bool first = true;
            foreach (var entry in pairs)
            {
                if (!first) sb.Append(", ");
                first = false;
                if (!entry.Key.StartsWith("\""))
                    sb.Append("\"").Append(entry.Key).Append("\"");
                else
                    sb.Append(entry.Key);
                sb.Append(": ").Append(entry.Value);
            }
            sb.Append("}");
            return sb.ToString();
        }

        private static string ExpandDotKeys(Dictionary<string, string> pairs)
        {
            // Tree node: either a leaf (raw JSON value) or a branch (children dict)
            // We use Dictionary<string, object> where object is either:
            //   string => leaf (raw JSON literal like "\"NY\"", "42", "true")
            //   Dictionary<string, object> => branch (nested object)
            var root = new Dictionary<string, object>();

            foreach (var entry in pairs)
            {
                var parts = entry.Key.Split('.');
                var current = root;
                for (int i = 0; i < parts.Length - 1; i++)
                {
                    object child;
                    if (!current.TryGetValue(parts[i], out child))
                    {
                        var next = new Dictionary<string, object>();
                        current[parts[i]] = next;
                        current = next;
                    }
                    else
                    {
                        current = (Dictionary<string, object>)child;
                    }
                }
                current[parts[parts.Length - 1]] = entry.Value;
            }

            return SerializeTree(root);
        }

        private static string SerializeTree(Dictionary<string, object> node)
        {
            var sb = new System.Text.StringBuilder("{");
            bool first = true;
            foreach (var entry in node)
            {
                if (!first) sb.Append(", ");
                first = false;
                sb.Append("\"").Append(entry.Key).Append("\": ");
                if (entry.Value is Dictionary<string, object>)
                    sb.Append(SerializeTree((Dictionary<string, object>)entry.Value));
                else
                    sb.Append((string)entry.Value);
            }
            sb.Append("}");
            return sb.ToString();
        }

        private static void ApplyFilterParams(DbCommand cmd, FilterResult filter)
        {
            for (int i = 0; i < filter.Params.Count; i++)
            {
                AddParameter(cmd, "@p" + (filter.ParamOffset + i), filter.Params[i]);
            }
        }

        private static void ApplyUpdateParams(DbCommand cmd, UpdateResult update, int startIdx)
        {
            for (int i = 0; i < update.Params.Count; i++)
            {
                AddParameter(cmd, "@p" + (startIdx + i), update.Params[i]);
            }
        }

        private static void ValidateIdentifier(string name)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentException("Identifier must be a non-empty string");
            if (!IdentifierPattern.IsMatch(name))
                throw new ArgumentException("Invalid identifier: " + name);
        }

        // ── Change streams (DocWatch / DocUnwatch) ─────────────────

        /// <summary>
        /// Watch a collection for changes via triggers + pg_notify.
        /// Like MongoDB change streams. Creates an AFTER INSERT/UPDATE/DELETE trigger
        /// that notifies on each row change. The callback receives the channel name
        /// and a JSON payload with operationType, _id, and fullDocument.
        /// </summary>
        public static void DocWatch(DbConnection conn, string collection,
            Action<string, string> callback, bool blocking = true)
        {
            ValidateIdentifier(collection);

            var funcName = "_gl_watch_" + collection;
            var triggerName = funcName + "_trigger";
            var channel = "_gl_changes_" + collection;

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "CREATE OR REPLACE FUNCTION " + funcName + "() RETURNS TRIGGER AS $$ " +
                    "BEGIN " +
                    "PERFORM pg_notify('" + channel + "', json_build_object(" +
                    "'operationType', lower(TG_OP), " +
                    "'_id', COALESCE(NEW._id, OLD._id)::text, " +
                    "'fullDocument', CASE WHEN TG_OP = 'DELETE' THEN NULL ELSE NEW.data END" +
                    ")::text); " +
                    "RETURN COALESCE(NEW, OLD); " +
                    "END; " +
                    "$$ LANGUAGE plpgsql";
                cmd.ExecuteNonQuery();
            }

            // CREATE OR REPLACE TRIGGER (Postgres 14+) is atomic — no
            // window where the trigger is missing, and a redefinition
            // cleanly replaces the old one instead of being silently
            // swallowed by `EXCEPTION WHEN duplicate_object`. GL targets
            // PG14+, so this is safe and matches the Go wrapper.
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "CREATE OR REPLACE TRIGGER " + triggerName +
                    " AFTER INSERT OR UPDATE OR DELETE ON " + collection +
                    " FOR EACH ROW EXECUTE FUNCTION " + funcName + "()";
                cmd.ExecuteNonQuery();
            }

            // Use the same Npgsql notification pattern as Subscribe
            if (blocking)
            {
                ListenLoop(conn, channel, callback);
            }
            else
            {
                var thread = new Thread(() => ListenLoop(conn, channel, callback));
                thread.IsBackground = true;
                thread.Start();
            }
        }

        /// <summary>
        /// Remove change stream trigger from a collection.
        /// </summary>
        public static void DocUnwatch(DbConnection conn, string collection)
        {
            ValidateIdentifier(collection);

            var funcName = "_gl_watch_" + collection;
            var triggerName = funcName + "_trigger";

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "DROP TRIGGER IF EXISTS " + triggerName + " ON " + collection;
                cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "DROP FUNCTION IF EXISTS " + funcName + "()";
                cmd.ExecuteNonQuery();
            }
        }

        // ── TTL indexes (DocCreateTtlIndex / DocRemoveTtlIndex) ──────

        /// <summary>
        /// Create a TTL index that deletes expired rows on each INSERT.
        /// Like MongoDB TTL indexes. Uses a BEFORE INSERT trigger.
        /// </summary>
        public static void DocCreateTtlIndex(DbConnection conn, string collection,
            int expireAfterSeconds, string field = "created_at")
        {
            ValidateIdentifier(collection);
            ValidateIdentifier(field);

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "CREATE INDEX IF NOT EXISTS idx_" + collection + "_ttl ON " +
                    collection + " (" + field + ")";
                cmd.ExecuteNonQuery();
            }

            var funcName = "_gl_ttl_" + collection;

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "CREATE OR REPLACE FUNCTION " + funcName + "() RETURNS TRIGGER AS $$ " +
                    "BEGIN " +
                    "DELETE FROM " + collection + " WHERE " + field +
                    " < NOW() - INTERVAL '" + expireAfterSeconds + " seconds'; " +
                    "RETURN NEW; " +
                    "END; " +
                    "$$ LANGUAGE plpgsql";
                cmd.ExecuteNonQuery();
            }

            // CREATE OR REPLACE TRIGGER (Postgres 14+): atomic and
            // redefinable. See DocWatch for rationale.
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "CREATE OR REPLACE TRIGGER " + funcName + "_trigger" +
                    " BEFORE INSERT ON " + collection +
                    " FOR EACH STATEMENT EXECUTE FUNCTION " + funcName + "()";
                cmd.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Remove TTL trigger, function, and index from a collection.
        /// </summary>
        public static void DocRemoveTtlIndex(DbConnection conn, string collection)
        {
            ValidateIdentifier(collection);

            var funcName = "_gl_ttl_" + collection;

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "DROP TRIGGER IF EXISTS " + funcName + "_trigger ON " + collection;
                cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "DROP FUNCTION IF EXISTS " + funcName + "()";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "DROP INDEX IF EXISTS idx_" + collection + "_ttl";
                cmd.ExecuteNonQuery();
            }
        }

        // ── Capped collections (DocCreateCapped / DocRemoveCap) ──────

        /// <summary>
        /// Create a capped collection that auto-deletes oldest rows.
        /// Like MongoDB capped collections. Uses an AFTER INSERT trigger.
        /// </summary>
        public static void DocCreateCapped(DbConnection conn, string collection, int maxDocuments)
        {
            ValidateIdentifier(collection);

            EnsureCollection(conn, collection);

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "CREATE INDEX IF NOT EXISTS idx_" + collection +
                    "_created_at ON " + collection + " (created_at ASC)";
                cmd.ExecuteNonQuery();
            }

            var funcName = "_gl_cap_" + collection;

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "CREATE OR REPLACE FUNCTION " + funcName + "() RETURNS TRIGGER AS $$ " +
                    "DECLARE excess INTEGER; " +
                    "BEGIN " +
                    "SELECT COUNT(*) - " + maxDocuments + " INTO excess FROM " + collection + "; " +
                    "IF excess > 0 THEN " +
                    "DELETE FROM " + collection + " WHERE _id IN (" +
                    "SELECT _id FROM " + collection + " ORDER BY created_at ASC LIMIT excess" +
                    "); " +
                    "END IF; " +
                    "RETURN NULL; " +
                    "END; " +
                    "$$ LANGUAGE plpgsql";
                cmd.ExecuteNonQuery();
            }

            // CREATE OR REPLACE TRIGGER (Postgres 14+): atomic and
            // redefinable. See DocWatch for rationale.
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "CREATE OR REPLACE TRIGGER " + funcName + "_trigger" +
                    " AFTER INSERT ON " + collection +
                    " FOR EACH STATEMENT EXECUTE FUNCTION " + funcName + "()";
                cmd.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Remove capped collection trigger and function.
        /// </summary>
        public static void DocRemoveCap(DbConnection conn, string collection)
        {
            ValidateIdentifier(collection);

            var funcName = "_gl_cap_" + collection;

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "DROP TRIGGER IF EXISTS " + funcName + "_trigger ON " + collection;
                cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "DROP FUNCTION IF EXISTS " + funcName + "()";
                cmd.ExecuteNonQuery();
            }
        }

        private static void AddParameter(DbCommand cmd, string name, object value)
        {
            var param = cmd.CreateParameter();
            param.ParameterName = name;
            param.Value = value;
            cmd.Parameters.Add(param);
        }
    }
}
