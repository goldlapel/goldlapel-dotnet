using System;
using Xunit;

namespace GoldLapel.Tests
{
    /// <summary>
    /// Regression: identifier validation must reject injection-shaped names
    /// before any SQL is issued. See v0.2 security review finding C1.
    ///
    /// Phase 5 retired the legacy flat redis-compat methods (Enqueue / Dequeue
    /// / Incr / GetCounter / Hset / Hget / Hgetall / Hdel / Zadd / Zincrby /
    /// Zrange / Zrank / Zscore / Zrem / Geoadd / Georadius / Geodist) — these
    /// are now covered by the per-family namespace tests
    /// (CountersNamespaceTests, ZsetsNamespaceTests, HashesNamespaceTests,
    /// QueuesNamespaceTests, GeosNamespaceTests). The only flat methods that
    /// survived are pub/sub (no DDL family) and stream verbs (covered below).
    /// </summary>
    public class RedisCompatValidationTests
    {
        private const string Bad = "foo; DROP TABLE users--";

        [Fact] public void PublishRejectsBad()
            => Assert.Throws<ArgumentException>(() => Utils.Publish(new SpyConnection(), Bad, "m"));

        [Fact] public void SubscribeRejectsBad()
            => Assert.Throws<ArgumentException>(() => Utils.Subscribe(new SpyConnection(), Bad, (a, b) => { }, blocking: false));

        [Fact] public void CountDistinctRejectsBadTable()
            => Assert.Throws<ArgumentException>(() => Utils.CountDistinct(new SpyConnection(), Bad, "col"));

        [Fact] public void CountDistinctRejectsBadColumn()
            => Assert.Throws<ArgumentException>(() => Utils.CountDistinct(new SpyConnection(), "tbl", Bad));

        // Stream validation runs before the DDL pattern check, so a null
        // DdlEntry is fine here — the ArgumentException fires first.
        [Fact] public void StreamAddRejectsBad()
            => Assert.Throws<ArgumentException>(() => Utils.StreamAdd(new SpyConnection(), Bad, "{}", null));

        [Fact] public void StreamCreateGroupRejectsBad()
            => Assert.Throws<ArgumentException>(() => Utils.StreamCreateGroup(new SpyConnection(), Bad, "g", null));

        [Fact] public void StreamReadRejectsBad()
            => Assert.Throws<ArgumentException>(() => Utils.StreamRead(new SpyConnection(), Bad, "g", "c", 1, null));

        [Fact] public void StreamAckRejectsBad()
            => Assert.Throws<ArgumentException>(() => Utils.StreamAck(new SpyConnection(), Bad, "g", 1L, null));

        [Fact] public void StreamClaimRejectsBad()
            => Assert.Throws<ArgumentException>(() => Utils.StreamClaim(new SpyConnection(), Bad, "g", "c", 60000L, null));

        // Phase 5 family helpers — same validate-first contract.
        [Fact] public void CounterIncrRejectsBad()
            => Assert.Throws<ArgumentException>(() => Utils.CounterIncr(new SpyConnection(), Bad, "k", 1, null));

        [Fact] public void ZsetAddRejectsBad()
            => Assert.Throws<ArgumentException>(() => Utils.ZsetAdd(new SpyConnection(), Bad, "z", "m", 1.0, null));

        [Fact] public void HashSetRejectsBad()
            => Assert.Throws<ArgumentException>(() => Utils.HashSet(new SpyConnection(), Bad, "k", "f", "v", null));

        [Fact] public void QueueEnqueueRejectsBad()
            => Assert.Throws<ArgumentException>(() => Utils.QueueEnqueue(new SpyConnection(), Bad, "{}", null));

        [Fact] public void GeoAddRejectsBad()
            => Assert.Throws<ArgumentException>(() => Utils.GeoAdd(new SpyConnection(), Bad, "m", 0.0, 0.0, null));
    }
}
