using System.Collections.Generic;

namespace GoldLapel.Tests
{
    /// <summary>
    /// Test-only helpers for the doc-store namespace. Mirrors the Python
    /// fixture pattern: the proxy normally hands the wrapper a DdlEntry
    /// whose <c>main</c> table is <c>_goldlapel.doc_&lt;name&gt;</c>; for
    /// SQL-shape tests we supply the user-facing collection name as the
    /// canonical table so existing assertions like
    /// <c>"INSERT INTO users"</c> remain meaningful.
    /// </summary>
    internal static class DocTestHelpers
    {
        /// <summary>
        /// Build a fake <see cref="DdlEntry"/> whose <c>main</c> table is
        /// <paramref name="collection"/>. Used by SQL-shape unit tests to
        /// drive <c>Utils.Doc*</c> with the user-facing name in place of
        /// the canonical <c>_goldlapel.doc_*</c> table — keeps assertions
        /// readable.
        /// </summary>
        public static DdlEntry FakePatterns(string collection) => new DdlEntry
        {
            Tables = new Dictionary<string, string> { ["main"] = collection },
            QueryPatterns = new Dictionary<string, string>(),
        };
    }
}
