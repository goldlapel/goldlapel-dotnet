using System;
using System.Collections.Generic;
using System.Data.Common;

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

        private static void AddParameter(DbCommand cmd, string name, object value)
        {
            var param = cmd.CreateParameter();
            param.ParameterName = name;
            param.Value = value;
            cmd.Parameters.Add(param);
        }
    }
}
