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
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "DELETE FROM " + table + " WHERE member = @member";
                AddParameter(cmd, "@member", member);
                return cmd.ExecuteNonQuery() > 0;
            }
        }

        public static long CountDistinct(DbConnection conn, string table, string column)
        {
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

        public static long StreamAdd(DbConnection conn, string stream, string payload)
        {
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "CREATE TABLE IF NOT EXISTS " + stream + " (" +
                    "id BIGSERIAL PRIMARY KEY, " +
                    "payload JSONB NOT NULL, " +
                    "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "INSERT INTO " + stream + " (payload) VALUES (@payload::jsonb) RETURNING id";
                AddParameter(cmd, "@payload", payload);
                return (long)cmd.ExecuteScalar();
            }
        }

        public static void StreamCreateGroup(DbConnection conn, string stream, string group)
        {
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "CREATE TABLE IF NOT EXISTS " + stream + "_groups (" +
                    "group_name TEXT NOT NULL, " +
                    "consumer TEXT NOT NULL DEFAULT '', " +
                    "message_id BIGINT NOT NULL, " +
                    "acked BOOLEAN NOT NULL DEFAULT FALSE, " +
                    "claimed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), " +
                    "PRIMARY KEY (group_name, message_id))";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "CREATE TABLE IF NOT EXISTS " + stream + "_cursors (" +
                    "group_name TEXT PRIMARY KEY, " +
                    "last_id BIGINT NOT NULL DEFAULT 0)";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "INSERT INTO " + stream + "_cursors (group_name, last_id) VALUES (@group, 0) " +
                    "ON CONFLICT (group_name) DO NOTHING";
                AddParameter(cmd, "@group", group);
                cmd.ExecuteNonQuery();
            }
        }

        public static List<Dictionary<string, object>> StreamRead(DbConnection conn, string stream,
            string group, string consumer, int count = 1)
        {
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "WITH cursor AS (" +
                    "SELECT last_id FROM " + stream + "_cursors WHERE group_name = @group1 FOR UPDATE" +
                    "), new_msgs AS (" +
                    "SELECT id, payload, created_at FROM " + stream +
                    " WHERE id > (SELECT last_id FROM cursor)" +
                    " ORDER BY id LIMIT @count" +
                    "), updated_cursor AS (" +
                    "UPDATE " + stream + "_cursors SET last_id = COALESCE((SELECT MAX(id) FROM new_msgs), last_id)" +
                    " WHERE group_name = @group2" +
                    "), inserted AS (" +
                    "INSERT INTO " + stream + "_groups (group_name, consumer, message_id)" +
                    " SELECT @group3, @consumer, id FROM new_msgs" +
                    " ON CONFLICT (group_name, message_id) DO NOTHING" +
                    ") SELECT id, payload, created_at FROM new_msgs ORDER BY id";
                AddParameter(cmd, "@group1", group);
                AddParameter(cmd, "@count", count);
                AddParameter(cmd, "@group2", group);
                AddParameter(cmd, "@group3", group);
                AddParameter(cmd, "@consumer", consumer);

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

        public static bool StreamAck(DbConnection conn, string stream, string group, long messageId)
        {
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "UPDATE " + stream + "_groups SET acked = TRUE " +
                    "WHERE group_name = @group AND message_id = @messageId AND acked = FALSE";
                AddParameter(cmd, "@group", group);
                AddParameter(cmd, "@messageId", messageId);
                return cmd.ExecuteNonQuery() > 0;
            }
        }

        public static List<Dictionary<string, object>> StreamClaim(DbConnection conn, string stream,
            string group, string consumer, long minIdleMs = 60000)
        {
            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "WITH claimed AS (" +
                    "UPDATE " + stream + "_groups SET consumer = @consumer, claimed_at = NOW()" +
                    " WHERE group_name = @group AND acked = FALSE" +
                    " AND claimed_at < NOW() - (@minIdleMs || ' milliseconds')::interval" +
                    " RETURNING message_id" +
                    ") SELECT s.id, s.payload, s.created_at FROM " + stream + " s" +
                    " INNER JOIN claimed c ON c.message_id = s.id ORDER BY s.id";
                AddParameter(cmd, "@consumer", consumer);
                AddParameter(cmd, "@group", group);
                AddParameter(cmd, "@minIdleMs", minIdleMs.ToString());

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
                cmd.CommandText = "CREATE EXTENSION IF NOT EXISTS pg_trgm";
                cmd.ExecuteNonQuery();
            }

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
                cmd.CommandText = "CREATE EXTENSION IF NOT EXISTS fuzzystrmatch";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "CREATE EXTENSION IF NOT EXISTS pg_trgm";
                cmd.ExecuteNonQuery();
            }

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

        public static List<Dictionary<string, object>> Similar(DbConnection conn, string table,
            string column, double[] vector, int limit = 10)
        {
            ValidateIdentifier(table);
            ValidateIdentifier(column);

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "CREATE EXTENSION IF NOT EXISTS vector";
                cmd.ExecuteNonQuery();
            }

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
                cmd.CommandText = "CREATE EXTENSION IF NOT EXISTS pg_trgm";
                cmd.ExecuteNonQuery();
            }

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
            string column, int limit = 50, string query = null, string[] queryColumn = null,
            string lang = "english")
        {
            ValidateIdentifier(table);
            ValidateIdentifier(column);

            var hasQuery = query != null && queryColumn != null && queryColumn.Length > 0;

            if (hasQuery)
            {
                foreach (var col in queryColumn)
                    ValidateIdentifier(col);
            }

            using (var cmd = conn.CreateCommand())
            {
                if (hasQuery)
                {
                    var tsvParts = string.Join(" || ' ' || ",
                        queryColumn.Select(c => "coalesce(" + c + ", '')"));
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

        private static readonly Regex IdentifierPattern = new Regex(@"^[a-zA-Z_][a-zA-Z0-9_]*$");

        private static void ValidateIdentifier(string name)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentException("Identifier must be a non-empty string");
            if (!IdentifierPattern.IsMatch(name))
                throw new ArgumentException("Invalid identifier: " + name);
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
