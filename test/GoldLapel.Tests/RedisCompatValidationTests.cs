using System;
using Xunit;

namespace GoldLapel.Tests
{
    /// <summary>
    /// Regression: Redis-compat helpers must reject injection-shaped identifier args.
    /// See v0.2 security review finding C1.
    /// </summary>
    public class RedisCompatValidationTests
    {
        private const string Bad = "foo; DROP TABLE users--";

        [Fact] public void PublishRejectsBad()
            => Assert.Throws<ArgumentException>(() => Utils.Publish(new SpyConnection(), Bad, "m"));

        [Fact] public void SubscribeRejectsBad()
            => Assert.Throws<ArgumentException>(() => Utils.Subscribe(new SpyConnection(), Bad, (a, b) => { }, blocking: false));

        [Fact] public void EnqueueRejectsBad()
            => Assert.Throws<ArgumentException>(() => Utils.Enqueue(new SpyConnection(), Bad, "{}"));

        [Fact] public void DequeueRejectsBad()
            => Assert.Throws<ArgumentException>(() => Utils.Dequeue(new SpyConnection(), Bad));

        [Fact] public void IncrRejectsBad()
            => Assert.Throws<ArgumentException>(() => Utils.Incr(new SpyConnection(), Bad, "k", 1));

        [Fact] public void GetCounterRejectsBad()
            => Assert.Throws<ArgumentException>(() => Utils.GetCounter(new SpyConnection(), Bad, "k"));

        [Fact] public void ZaddRejectsBad()
            => Assert.Throws<ArgumentException>(() => Utils.Zadd(new SpyConnection(), Bad, "m", 1.0));

        [Fact] public void ZincrbyRejectsBad()
            => Assert.Throws<ArgumentException>(() => Utils.Zincrby(new SpyConnection(), Bad, "m", 1.0));

        [Fact] public void ZrangeRejectsBad()
            => Assert.Throws<ArgumentException>(() => Utils.Zrange(new SpyConnection(), Bad));

        [Fact] public void ZrankRejectsBad()
            => Assert.Throws<ArgumentException>(() => Utils.Zrank(new SpyConnection(), Bad, "m"));

        [Fact] public void ZscoreRejectsBad()
            => Assert.Throws<ArgumentException>(() => Utils.Zscore(new SpyConnection(), Bad, "m"));

        [Fact] public void ZremRejectsBad()
            => Assert.Throws<ArgumentException>(() => Utils.Zrem(new SpyConnection(), Bad, "m"));

        [Fact] public void HsetRejectsBad()
            => Assert.Throws<ArgumentException>(() => Utils.Hset(new SpyConnection(), Bad, "k", "f", "\"v\""));

        [Fact] public void HgetRejectsBad()
            => Assert.Throws<ArgumentException>(() => Utils.Hget(new SpyConnection(), Bad, "k", "f"));

        [Fact] public void HgetallRejectsBad()
            => Assert.Throws<ArgumentException>(() => Utils.Hgetall(new SpyConnection(), Bad, "k"));

        [Fact] public void HdelRejectsBad()
            => Assert.Throws<ArgumentException>(() => Utils.Hdel(new SpyConnection(), Bad, "k", "f"));

        [Fact] public void CountDistinctRejectsBadTable()
            => Assert.Throws<ArgumentException>(() => Utils.CountDistinct(new SpyConnection(), Bad, "col"));

        [Fact] public void CountDistinctRejectsBadColumn()
            => Assert.Throws<ArgumentException>(() => Utils.CountDistinct(new SpyConnection(), "tbl", Bad));

        [Fact] public void GeoaddRejectsBadTable()
            => Assert.Throws<ArgumentException>(() => Utils.Geoadd(new SpyConnection(), Bad, "name", "geom", "x", 0.0, 0.0));

        [Fact] public void GeoaddRejectsBadNameColumn()
            => Assert.Throws<ArgumentException>(() => Utils.Geoadd(new SpyConnection(), "tbl", Bad, "geom", "x", 0.0, 0.0));

        [Fact] public void GeoaddRejectsBadGeomColumn()
            => Assert.Throws<ArgumentException>(() => Utils.Geoadd(new SpyConnection(), "tbl", "name", Bad, "x", 0.0, 0.0));

        [Fact] public void GeoradiusRejectsBadTable()
            => Assert.Throws<ArgumentException>(() => Utils.Georadius(new SpyConnection(), Bad, "geom", 0.0, 0.0, 100.0));

        [Fact] public void GeoradiusRejectsBadGeomColumn()
            => Assert.Throws<ArgumentException>(() => Utils.Georadius(new SpyConnection(), "tbl", Bad, 0.0, 0.0, 100.0));

        [Fact] public void GeodistRejectsBadTable()
            => Assert.Throws<ArgumentException>(() => Utils.Geodist(new SpyConnection(), Bad, "geom", "name", "a", "b"));

        [Fact] public void StreamAddRejectsBad()
            => Assert.Throws<ArgumentException>(() => Utils.StreamAdd(new SpyConnection(), Bad, "{}"));

        [Fact] public void StreamCreateGroupRejectsBad()
            => Assert.Throws<ArgumentException>(() => Utils.StreamCreateGroup(new SpyConnection(), Bad, "g"));

        [Fact] public void StreamReadRejectsBad()
            => Assert.Throws<ArgumentException>(() => Utils.StreamRead(new SpyConnection(), Bad, "g", "c"));

        [Fact] public void StreamAckRejectsBad()
            => Assert.Throws<ArgumentException>(() => Utils.StreamAck(new SpyConnection(), Bad, "g", 1L));

        [Fact] public void StreamClaimRejectsBad()
            => Assert.Throws<ArgumentException>(() => Utils.StreamClaim(new SpyConnection(), Bad, "g", "c"));
    }
}
