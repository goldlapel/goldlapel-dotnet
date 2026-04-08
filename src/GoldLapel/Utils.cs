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
                    " USING GIN (tsquery)";
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

        private static void EnsureCollection(DbConnection conn, string collection)
        {
            ValidateIdentifier(collection);

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "CREATE TABLE IF NOT EXISTS " + collection + " (" +
                    "id BIGSERIAL PRIMARY KEY, " +
                    "data JSONB NOT NULL, " +
                    "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), " +
                    "updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())";
                cmd.ExecuteNonQuery();
            }
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
                    "RETURNING id, data, created_at, updated_at";
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
                        "RETURNING id, data, created_at, updated_at";
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

            using (var cmd = conn.CreateCommand())
            {
                var sql = "SELECT id, data, created_at, updated_at FROM " + collection;

                if (filterJson != null)
                {
                    sql += " WHERE data @> @filter::jsonb";
                    AddParameter(cmd, "@filter", filterJson);
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

        public static Dictionary<string, object> DocFindOne(DbConnection conn, string collection, string filterJson = null)
        {
            ValidateIdentifier(collection);

            using (var cmd = conn.CreateCommand())
            {
                var sql = "SELECT id, data, created_at, updated_at FROM " + collection;

                if (filterJson != null)
                {
                    sql += " WHERE data @> @filter::jsonb";
                    AddParameter(cmd, "@filter", filterJson);
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

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "UPDATE " + collection + " SET data = data || @update::jsonb, updated_at = NOW() " +
                    "WHERE data @> @filter::jsonb";
                AddParameter(cmd, "@update", updateJson);
                AddParameter(cmd, "@filter", filterJson);
                return cmd.ExecuteNonQuery();
            }
        }

        public static int DocUpdateOne(DbConnection conn, string collection, string filterJson, string updateJson)
        {
            ValidateIdentifier(collection);

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "UPDATE " + collection + " SET data = data || @update::jsonb, updated_at = NOW() " +
                    "WHERE id = (SELECT id FROM " + collection + " WHERE data @> @filter::jsonb LIMIT 1)";
                AddParameter(cmd, "@update", updateJson);
                AddParameter(cmd, "@filter", filterJson);
                return cmd.ExecuteNonQuery();
            }
        }

        public static int DocDelete(DbConnection conn, string collection, string filterJson)
        {
            ValidateIdentifier(collection);

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "DELETE FROM " + collection + " WHERE data @> @filter::jsonb";
                AddParameter(cmd, "@filter", filterJson);
                return cmd.ExecuteNonQuery();
            }
        }

        public static int DocDeleteOne(DbConnection conn, string collection, string filterJson)
        {
            ValidateIdentifier(collection);

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText =
                    "DELETE FROM " + collection + " WHERE id = (" +
                    "SELECT id FROM " + collection + " WHERE data @> @filter::jsonb LIMIT 1)";
                AddParameter(cmd, "@filter", filterJson);
                return cmd.ExecuteNonQuery();
            }
        }

        public static long DocCount(DbConnection conn, string collection, string filterJson = null)
        {
            ValidateIdentifier(collection);

            using (var cmd = conn.CreateCommand())
            {
                var sql = "SELECT COUNT(*) FROM " + collection;

                if (filterJson != null)
                {
                    sql += " WHERE data @> @filter::jsonb";
                    AddParameter(cmd, "@filter", filterJson);
                }

                cmd.CommandText = sql;
                return (long)cmd.ExecuteScalar();
            }
        }

        public static List<Dictionary<string, object>> DocAggregate(DbConnection conn, string collection, string pipelineJson)
        {
            ValidateIdentifier(collection);
            if (pipelineJson == null)
                throw new ArgumentException("Pipeline must not be null");

            var stages = ParsePipeline(pipelineJson);

            string matchFilter = null;
            bool hasGroup = false;
            string groupIdField = null;
            var selectExprs = new List<string>();
            var groupByExprs = new List<string>();
            string sortClause = null;
            bool sortAfterGroup = false;
            int? limitVal = null;
            int? skipVal = null;

            foreach (var stage in stages)
            {
                string stageType = stage["_type"];
                switch (stageType)
                {
                    case "$match":
                        matchFilter = stage["_body"];
                        break;
                    case "$group":
                        hasGroup = true;
                        if (stage.ContainsKey("_id"))
                        {
                            var idRaw = stage["_id"];
                            if (idRaw != null && idRaw != "null")
                            {
                                if (idRaw.StartsWith("\"$") && idRaw.EndsWith("\""))
                                    groupIdField = idRaw.Substring(2, idRaw.Length - 3);
                                else if (idRaw.StartsWith("$"))
                                    groupIdField = idRaw.Substring(1);
                                else
                                    groupIdField = idRaw.Trim('"');

                                ValidateIdentifier(groupIdField);
                                selectExprs.Add("data->>'" + groupIdField + "' AS _id");
                                groupByExprs.Add("data->>'" + groupIdField + "'");
                            }
                        }
                        foreach (var kv in stage)
                        {
                            if (kv.Key == "_type" || kv.Key == "_id" || kv.Key == "_body")
                                continue;
                            ValidateIdentifier(kv.Key);
                            var accExpr = ParseAccumulator(kv.Value);
                            selectExprs.Add(accExpr + " AS " + kv.Key);
                        }
                        break;
                    case "$sort":
                        sortAfterGroup = hasGroup;
                        if (sortAfterGroup)
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
                    default:
                        throw new ArgumentException("Unsupported pipeline stage: " + stageType);
                }
            }

            using (var cmd = conn.CreateCommand())
            {
                var sql = "";
                if (hasGroup && selectExprs.Count > 0)
                    sql = "SELECT " + string.Join(", ", selectExprs);
                else
                    sql = "SELECT id, data, created_at, updated_at";

                sql += " FROM " + collection;

                if (matchFilter != null)
                {
                    sql += " WHERE data @> @filter::jsonb";
                    AddParameter(cmd, "@filter", matchFilter);
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
                    while (i < body.Length && body[i] != '"') i++;
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

                // Find value (could be object, string, number, null)
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

        private static string ParseAccumulator(string accJson)
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
                        return "SUM((data->>'" + field + "')::numeric)";
                    }
                case "$avg":
                {
                    var field = ExtractFieldRef(arg);
                    return "AVG((data->>'" + field + "')::numeric)";
                }
                case "$min":
                {
                    var field = ExtractFieldRef(arg);
                    return "MIN((data->>'" + field + "')::numeric)";
                }
                case "$max":
                {
                    var field = ExtractFieldRef(arg);
                    return "MAX((data->>'" + field + "')::numeric)";
                }
                case "$count":
                    return "COUNT(*)";
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
            ValidateIdentifier(field);
            return field;
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
