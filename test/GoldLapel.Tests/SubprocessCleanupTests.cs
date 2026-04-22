using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Xunit;
using GL = GoldLapel.GoldLapel;

namespace GoldLapel.Tests
{
    // Regression test: GoldLapel.StartAsync must clean up its subprocess if the
    // subsequent Npgsql connect fails after the child has been spawned and the
    // proxy port has opened.
    //
    // Historic bug class: Process.Start succeeded, proxy port bound, then the
    // eager NpgsqlConnection.OpenAsync() failed (bad upstream creds, unreachable
    // upstream, unrelated process listening on the port, etc.) — and the
    // goldlapel child kept running. Same shape as the bug already covered by
    // goldlapel-python's test_v02_subprocess_cleanup.py and goldlapel-ruby's
    // test_v02_subprocess_cleanup.rb (wrapper-v0.2 Tests Q6).
    //
    // Strategy: point GOLDLAPEL_BINARY at a test-only shim that binds the
    // requested proxy port and stays alive without speaking the Postgres wire
    // protocol. PollForPortAsync therefore succeeds (port is open, process is
    // alive), the wrapper proceeds to open the internal NpgsqlConnection, and
    // Npgsql's OpenAsync throws because nothing on the other end speaks
    // Postgres. At that point — and only at that point — the wrapper's
    // post-spawn cleanup path runs. We verify the shim PID was reaped.
    //
    // Shares the "EnvVarTests" collection with FindBinaryTest + IntegrationTests
    // because this test mutates the GOLDLAPEL_BINARY env var, which both of
    // those suites read via FindBinary(). Running them concurrently would cross-
    // contaminate binary discovery.
    [Collection("EnvVarTests")]
    public class SubprocessCleanupTests : IDisposable
    {
        private readonly string? _origBinary;
        private readonly List<string> _tempFiles = new List<string>();

        public SubprocessCleanupTests()
        {
            _origBinary = Environment.GetEnvironmentVariable("GOLDLAPEL_BINARY");
        }

        public void Dispose()
        {
            Environment.SetEnvironmentVariable("GOLDLAPEL_BINARY", _origBinary);
            foreach (var f in _tempFiles)
            {
                try { File.Delete(f); } catch { }
            }
        }

        // Create a shim binary that binds the given port, loops forever on
        // accept (draining each connection immediately without speaking PG
        // protocol), and exits cleanly on SIGTERM. This mimics a spawned
        // goldlapel whose proxy port opens but which a client can't complete
        // a PG handshake against — i.e. the exact pre-OpenAsync state the
        // wrapper's cleanup path was written to handle.
        //
        // Shim is a Python 3 script (the .NET test host on Linux/macOS CI
        // has Python 3 available; on Windows we skip the test). Returns null
        // if no shim can be built on this platform.
        private string? BuildShimOrNull(int port)
        {
            // Windows: the shim relies on a POSIX-style shebang + chmod +x to
            // make itself directly executable via Process.Start. Building the
            // same shape on Windows would mean shelling through python.exe
            // explicitly, which complicates GOLDLAPEL_BINARY wiring. Skip
            // cleanly — the Python and Ruby sibling tests already establish
            // the regression across platforms; this test's value is verifying
            // the .NET cleanup code path on Linux/macOS CI.
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                return null;

            var python = FindOnPath("python3") ?? FindOnPath("python");
            if (python == null) return null;

            var shimPath = Path.Combine(Path.GetTempPath(),
                "goldlapel-shim-" + Guid.NewGuid().ToString("N").Substring(0, 8));

            // Script binds 127.0.0.1:<port>, accepts+closes connections in a
            // loop (so the wrapper's TCP readiness probe succeeds), and exits
            // on SIGTERM. Writing its own PID to stderr is unused here — the
            // test uses Process.GetProcessesByName snapshotting — but kept
            // for debuggability when the test fails.
            var script = $@"#!{python}
import os, signal, socket, sys

print(f'shim pid={{os.getpid()}} port={port}', file=sys.stderr, flush=True)
signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(('127.0.0.1', {port}))
s.listen(16)
try:
    while True:
        conn, _ = s.accept()
        conn.close()
except (KeyboardInterrupt, SystemExit):
    pass
finally:
    s.close()
";
            File.WriteAllText(shimPath, script);
            Chmod755(shimPath);
            _tempFiles.Add(shimPath);
            return shimPath;
        }

        private static string? FindOnPath(string name)
        {
            var pathEnv = Environment.GetEnvironmentVariable("PATH");
            if (string.IsNullOrEmpty(pathEnv)) return null;
            foreach (var dir in pathEnv.Split(':'))
            {
                var full = Path.Combine(dir, name);
                if (File.Exists(full)) return full;
            }
            return null;
        }

        [DllImport("libc", SetLastError = true, EntryPoint = "chmod")]
        private static extern int sys_chmod(string path, int mode);

        private static void Chmod755(string path)
        {
            // 0o755 = rwxr-xr-x
            sys_chmod(path, 0x1ED);
        }

        // Snapshot all live PIDs whose ProcessName matches the shim's base name.
        // We use the shim's basename rather than "goldlapel" so the snapshot
        // isn't polluted by any real goldlapel instance running on the host.
        private static HashSet<int> SnapshotPidsByName(string processName)
        {
            var pids = new HashSet<int>();
            foreach (var p in Process.GetProcessesByName(processName))
            {
                try { pids.Add(p.Id); }
                catch { /* process may have exited between enumeration and Id read */ }
                finally { p.Dispose(); }
            }
            return pids;
        }

        private static bool IsProcessAlive(int pid)
        {
            try
            {
                using var proc = Process.GetProcessById(pid);
                return !proc.HasExited;
            }
            catch (ArgumentException)
            {
                // Canonical .NET signal for "process no longer exists".
                return false;
            }
            catch (InvalidOperationException)
            {
                return false;
            }
        }

        [Fact]
        public async Task StartAsync_CleansUpSubprocess_WhenConnectFails()
        {
            // Port far from the IntegrationTests range (17900-17999) and defaults.
            var port = 18900 + Random.Shared.Next(0, 100);

            var shimPath = BuildShimOrNull(port);
            if (shimPath == null) return; // platform without a usable shim; skip cleanly

            var shimName = Path.GetFileName(shimPath);
            Environment.SetEnvironmentVariable("GOLDLAPEL_BINARY", shimPath);

            // Upstream content is irrelevant — the shim never connects upstream.
            // Npgsql's OpenAsync will fail because the shim closes the socket
            // immediately and doesn't speak the Postgres wire protocol.
            var upstream = "postgresql://nobody:bad@127.0.0.1:1/nope";

            var before = SnapshotPidsByName(shimName);

            var exception = await Assert.ThrowsAnyAsync<Exception>(async () =>
            {
                await using var gl = await GL.StartAsync(
                    upstream,
                    opts => { opts.Port = port; opts.Silent = true; });
            });

            Assert.NotNull(exception);
            // Echo the exception shape so "why did this test pass" is inspectable
            // in test logs — useful when triaging a future regression.
            Console.Error.WriteLine(
                $"[SubprocessCleanupTests] StartAsync threw {exception.GetType().Name}: {exception.Message}");

            // Diff: PID(s) spawned by this test's StartAsync call.
            var after = SnapshotPidsByName(shimName);
            after.ExceptWith(before);

            if (after.Count == 0)
            {
                // Child already reaped between the exception and the snapshot —
                // happy path (cleanup was fast). Nothing to verify.
                return;
            }

            // Allow a small grace window for graceful shutdown (SIGTERM → wait
            // → kill) to finish. StartAsync's cleanup path blocks inside
            // StopProcessInternal until the child has exited or timed out,
            // but PID-reaping by the OS is inherently racy on Unix.
            var deadline = DateTime.UtcNow.AddMilliseconds(500);
            while (DateTime.UtcNow < deadline)
            {
                if (after.All(pid => !IsProcessAlive(pid)))
                    return; // all spawned children are gone — test passes

                await Task.Delay(20);
            }

            var stragglers = after.Where(IsProcessAlive).ToList();
            if (stragglers.Count > 0)
            {
                // Best-effort cleanup so a failing assertion doesn't leak
                // children into the next test run.
                foreach (var pid in stragglers)
                {
                    try { using var p = Process.GetProcessById(pid); p.Kill(); } catch { }
                }
                Assert.Fail(
                    $"Subprocess PID(s) [{string.Join(", ", stragglers)}] still alive " +
                    "after failed StartAsync — wrapper must kill the goldlapel child " +
                    "when the eager Npgsql connect fails.");
            }
        }
    }
}
