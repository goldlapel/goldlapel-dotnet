using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading.Tasks;
using Xunit;
using GL = GoldLapel.GoldLapel;

namespace GoldLapel.Tests
{
    /// <summary>
    /// Unit tests for <see cref="QueuesApi"/> — the nested
    /// <c>gl.Queues</c> namespace introduced in Phase 5 of schema-to-core.
    /// Mirrors <c>tests/test_queues.py</c> in the Python wrapper.
    ///
    /// Phase 5 contract: at-least-once delivery with visibility-timeout. The
    /// breaking change is <c>DequeueAsync</c> (delete-on-fetch) →
    /// <see cref="QueuesApi.ClaimAsync"/> + <see cref="QueuesApi.AckAsync"/>.
    /// There is intentionally no <c>Dequeue</c> compat shim.
    /// </summary>
    public class QueuesNamespaceShapeTest
    {
        [Fact]
        public void QueuesIsAQueuesApi()
        {
            var gl = GL.CreateForTest("postgresql://localhost:5432/mydb");
            Assert.IsType<QueuesApi>(gl.Queues);
        }

        [Fact]
        public void NoLegacyQueueMethodsOnGl()
        {
            var t = typeof(GL);
            foreach (var legacy in new[] { "EnqueueAsync", "DequeueAsync" })
                Assert.Null(t.GetMethod(legacy, BindingFlags.Public | BindingFlags.Instance));
        }

        [Fact]
        public void NoDequeueAliasOnQueuesApi()
        {
            // The dispatcher considered shipping a Dequeue compat shim that
            // combined claim+ack. The master plan rejected that — there must
            // be no compat alias here.
            var t = typeof(QueuesApi);
            Assert.Null(t.GetMethod("DequeueAsync", BindingFlags.Public | BindingFlags.Instance));
        }
    }

    public class QueuesNamespaceVerbTest
    {
        private readonly GL _gl;
        private readonly SpyConnection _spy;

        public QueuesNamespaceVerbTest()
        {
            _gl = TestHelpers.MakeWithSpy(out _spy);
            TestHelpers.InjectQueuePatterns(_gl, "jobs");
        }

        [Fact]
        public async Task EnqueueReturnsAssignedId()
        {
            _spy.NextReaderFactory = () => new FakeDataReader(
                new object[][] { new object[] { 99L, DateTime.UtcNow } },
                new[] { "id", "created_at" });
            var id = await _gl.Queues.EnqueueAsync("jobs", new { x = 1 });
            Assert.Equal(99L, id);
            // The proxy's pattern serializes payload as JSONB.
            Assert.Contains("INSERT INTO _goldlapel.queue_jobs", _spy.LastCommandText);
            Assert.Contains("VALUES (@p1::jsonb)", _spy.LastCommandText);
        }

        [Fact]
        public async Task EnqueueJsonEncodesPayload()
        {
            _spy.NextReaderFactory = () => new FakeDataReader(
                new object[][] { new object[] { 1L, DateTime.UtcNow } },
                new[] { "id", "created_at" });
            await _gl.Queues.EnqueueAsync("jobs", new Dictionary<string, int> { ["x"] = 1 });
            // The wrapper JSON-encodes at the edge — the proxy stores JSONB.
            Assert.Equal("{\"x\":1}", _spy.LastCommand.ParamValue("@p1"));
        }

        [Fact]
        public async Task ClaimReturnsClaimedMessageWithIdAndPayload()
        {
            _spy.NextReaderFactory = () => new FakeDataReader(
                new object[][] {
                    new object[] { 7L, "{\"x\":1}", DateTime.UtcNow, DateTime.UtcNow },
                },
                new[] { "id", "payload", "visible_at", "created_at" });
            var msg = await _gl.Queues.ClaimAsync("jobs", visibilityTimeoutMs: 30000);
            Assert.NotNull(msg);
            Assert.Equal(7L, msg.Id);
            Assert.Equal(1, msg.Payload.GetProperty("x").GetInt32());
        }

        [Fact]
        public async Task ClaimReturnsNullWhenEmpty()
        {
            _spy.NextReaderFactory = () => new FakeDataReader(
                new object[0][], new[] { "id", "payload", "visible_at", "created_at" });
            var msg = await _gl.Queues.ClaimAsync("jobs", visibilityTimeoutMs: 30000);
            Assert.Null(msg);
        }

        [Fact]
        public async Task ClaimAndAckAreDistinctCalls()
        {
            // Phase 5 contract: claim leases the row (UPDATE SET status='claimed'),
            // ack DELETEs it. They are NOT bundled — that's the at-least-once
            // delivery guarantee.
            _spy.NextReaderFactory = () => new FakeDataReader(
                new object[][] {
                    new object[] { 7L, "{}", DateTime.UtcNow, DateTime.UtcNow },
                },
                new[] { "id", "payload", "visible_at", "created_at" });
            await _gl.Queues.ClaimAsync("jobs", visibilityTimeoutMs: 30000);
            // The claim SQL must NOT issue a DELETE — that's ack's job.
            var sql = _spy.LastCommandText;
            Assert.Contains("UPDATE _goldlapel.queue_jobs", sql);
            Assert.DoesNotContain("DELETE FROM _goldlapel.queue_jobs", sql);
        }

        [Fact]
        public async Task AckReturnsTrueWhenDeleted()
        {
            _spy.NextNonQueryResult = 1;
            Assert.True(await _gl.Queues.AckAsync("jobs", 42L));
            Assert.Contains("DELETE FROM _goldlapel.queue_jobs", _spy.LastCommandText);
        }

        [Fact]
        public async Task AckReturnsFalseWhenIdUnknown()
        {
            _spy.NextNonQueryResult = 0;
            Assert.False(await _gl.Queues.AckAsync("jobs", 999L));
        }

        [Fact]
        public async Task AbandonUsesNackPatternToReReady()
        {
            _spy.NextReaderFactory = () => new FakeDataReader(
                new object[][] { new object[] { 42L } },
                new[] { "id" });
            Assert.True(await _gl.Queues.AbandonAsync("jobs", 42L));
            // The nack pattern flips status back to 'ready' rather than deleting.
            Assert.Contains("status = 'ready'", _spy.LastCommandText);
        }

        [Fact]
        public async Task ExtendBindsIdAndAdditionalMs()
        {
            _spy.NextReaderFactory = () => new FakeDataReader(
                new object[][] { new object[] { DateTime.UtcNow.AddSeconds(5) } },
                new[] { "visible_at" });
            await _gl.Queues.ExtendAsync("jobs", 42L, 5000);
            // Proxy contract: WHERE id = $1 AND ... INTERVAL ... * $2.
            // Bind: ($1=id, $2=additional_ms).
            var cmd = _spy.LastCommand;
            Assert.Equal(42L, cmd.ParamValue("@p1"));
            Assert.Equal(5000L, cmd.ParamValue("@p2"));
        }

        [Fact]
        public async Task PeekReturnsDictionary()
        {
            _spy.NextReaderFactory = () => new FakeDataReader(
                new object[][] {
                    new object[] { 42L, "{\"work\":\"foo\"}", DateTime.UtcNow, "ready", DateTime.UtcNow },
                },
                new[] { "id", "payload", "visible_at", "status", "created_at" });
            var got = await _gl.Queues.PeekAsync("jobs");
            Assert.NotNull(got);
            Assert.Equal(42L, got["id"]);
            Assert.Equal("ready", got["status"]);
            // payload comes back as JsonElement.
            var payload = (JsonElement)got["payload"];
            Assert.Equal("foo", payload.GetProperty("work").GetString());
        }
    }

    public class QueuesBreakingChangeTest
    {
        [Fact]
        public void HelperRequiresPatternsArg()
        {
            Assert.Throws<InvalidOperationException>(() =>
                Utils.QueueEnqueue(new SpyConnection(), "jobs", "{}", null));
        }
    }
}
