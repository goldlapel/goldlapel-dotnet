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

        private static void AddParameter(DbCommand cmd, string name, object value)
        {
            var param = cmd.CreateParameter();
            param.ParameterName = name;
            param.Value = value;
            cmd.Parameters.Add(param);
        }
    }
}
