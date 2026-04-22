using System;

namespace GoldLapel.Tests
{
    /// <summary>
    /// Shared integration-test gating — standardized across all Gold Lapel wrappers.
    ///
    /// Convention:
    ///   - GOLDLAPEL_INTEGRATION=1  — explicit opt-in gate ("yes, really run these")
    ///   - GOLDLAPEL_TEST_UPSTREAM  — Postgres URL for the test upstream
    ///
    /// Both must be set. If GOLDLAPEL_INTEGRATION=1 is set but GOLDLAPEL_TEST_UPSTREAM
    /// is missing, the gate throws loudly — this prevents a half-configured CI from
    /// silently skipping integration tests and producing a false-green unit-only run.
    ///
    /// If GOLDLAPEL_INTEGRATION is unset, integration tests skip silently.
    /// </summary>
    internal static class IntegrationGate
    {
        /// <summary>
        /// Returns the upstream Postgres URL if integration tests should run,
        /// or null if they should skip. Throws InvalidOperationException if
        /// GOLDLAPEL_INTEGRATION=1 is set but GOLDLAPEL_TEST_UPSTREAM is missing
        /// (false-green prevention).
        /// </summary>
        public static string? Upstream()
        {
            var integration = Environment.GetEnvironmentVariable("GOLDLAPEL_INTEGRATION") == "1";
            var upstream = Environment.GetEnvironmentVariable("GOLDLAPEL_TEST_UPSTREAM");

            if (integration && string.IsNullOrEmpty(upstream))
            {
                throw new InvalidOperationException(
                    "GOLDLAPEL_INTEGRATION=1 is set but GOLDLAPEL_TEST_UPSTREAM is " +
                    "missing. Set GOLDLAPEL_TEST_UPSTREAM to a Postgres URL " +
                    "(e.g. postgresql://postgres@localhost/postgres) or unset " +
                    "GOLDLAPEL_INTEGRATION to skip integration tests.");
            }

            return integration ? upstream : null;
        }

        /// <summary>
        /// True iff integration tests should run (both env vars set). Throws if
        /// the half-configured case is detected.
        /// </summary>
        public static bool ShouldRun() => Upstream() != null;
    }
}
