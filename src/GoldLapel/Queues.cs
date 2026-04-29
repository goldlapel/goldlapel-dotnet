using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;

namespace GoldLapel
{
    /// <summary>
    /// Result of <see cref="QueuesApi.ClaimAsync"/>. The caller MUST
    /// <see cref="QueuesApi.AckAsync"/> the <see cref="Id"/> to commit, or
    /// <see cref="QueuesApi.AbandonAsync"/> it to release the claim. A consumer
    /// that crashes leaves the lease standing; the message becomes ready again
    /// after the visibility timeout and is redelivered to the next claim.
    /// </summary>
    public sealed class ClaimedMessage
    {
        public long Id { get; set; }
        public JsonElement Payload { get; set; }
    }

    /// <summary>
    /// The queues sub-API — accessible as <c>gl.Queues</c>.
    ///
    /// Phase 5 of schema-to-core. The proxy's v1 queue schema is at-least-once
    /// with visibility-timeout — NOT the legacy fire-and-forget shape. The
    /// breaking change:
    ///
    /// <code>
    /// // Before: payload = await gl.DequeueAsync("jobs"); // delete-on-fetch, may lose work
    /// // After :
    /// var msg = await gl.Queues.ClaimAsync("jobs");
    /// if (msg != null) {
    ///     // ... handle msg.Payload ...
    ///     await gl.Queues.AckAsync("jobs", msg.Id);
    /// }
    /// </code>
    /// </summary>
    public class QueuesApi
    {
        private readonly GoldLapel _gl;

        internal QueuesApi(GoldLapel gl) => _gl = gl;

        private Task<DdlEntry> PatternsAsync(string name)
        {
            Utils.ValidateIdentifier(name);
            var token = _gl._dashboardToken ?? Ddl.TokenFromEnvOrFile();
            return Ddl.FetchPatternsAsync(
                _gl._ddlCache, "queue", name,
                _gl.DashboardPort, token, options: null);
        }

        public async Task CreateAsync(string name)
        {
            await PatternsAsync(name).ConfigureAwait(false);
        }

        /// <summary>Add a message; returns its assigned <c>id</c>.</summary>
        public async Task<long?> EnqueueAsync(string name, object payload, NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(name).ConfigureAwait(false);
            return Utils.QueueEnqueue(_gl.ResolveActiveDb(connection), name, payload, patterns);
        }

        /// <summary>
        /// Claim the next ready message. Returns <c>null</c> if the queue is
        /// empty. Caller MUST <see cref="AckAsync"/> or <see cref="AbandonAsync"/>
        /// the id, or the message becomes visible again after
        /// <paramref name="visibilityTimeoutMs"/>.
        /// </summary>
        public async Task<ClaimedMessage> ClaimAsync(string name, long visibilityTimeoutMs = 30000L,
            NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(name).ConfigureAwait(false);
            return Utils.QueueClaim(_gl.ResolveActiveDb(connection), name, visibilityTimeoutMs, patterns);
        }

        /// <summary>
        /// Mark a claimed message done (DELETE). Returns <c>true</c> if the
        /// message existed and was removed.
        /// </summary>
        public async Task<bool> AckAsync(string name, long messageId, NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(name).ConfigureAwait(false);
            return Utils.QueueAck(_gl.ResolveActiveDb(connection), name, messageId, patterns);
        }

        /// <summary>
        /// Release a claimed message back to ready immediately so it's
        /// redelivered without waiting for the visibility timeout.
        /// </summary>
        public async Task<bool> AbandonAsync(string name, long messageId, NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(name).ConfigureAwait(false);
            return Utils.QueueAbandon(_gl.ResolveActiveDb(connection), name, messageId, patterns);
        }

        /// <summary>
        /// Push the visibility deadline forward by <paramref name="additionalMs"/>
        /// milliseconds. Returns the new <c>visible_at</c>, or <c>null</c> if
        /// the id wasn't a claimed message.
        /// </summary>
        public async Task<DateTime?> ExtendAsync(string name, long messageId, long additionalMs,
            NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(name).ConfigureAwait(false);
            return Utils.QueueExtend(_gl.ResolveActiveDb(connection), name, messageId, additionalMs, patterns);
        }

        /// <summary>Look at the next-ready message without claiming it.</summary>
        public async Task<Dictionary<string, object>> PeekAsync(string name, NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(name).ConfigureAwait(false);
            return Utils.QueuePeek(_gl.ResolveActiveDb(connection), name, patterns);
        }

        public async Task<long> CountReadyAsync(string name, NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(name).ConfigureAwait(false);
            return Utils.QueueCountReady(_gl.ResolveActiveDb(connection), name, patterns);
        }

        public async Task<long> CountClaimedAsync(string name, NpgsqlConnection connection = null)
        {
            var patterns = await PatternsAsync(name).ConfigureAwait(false);
            return Utils.QueueCountClaimed(_gl.ResolveActiveDb(connection), name, patterns);
        }
    }
}
