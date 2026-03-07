using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text.RegularExpressions;
using System.Threading;

namespace GoldLapel
{
    public class GoldLapelOptions
    {
        public int? Port { get; set; }
        public string[] ExtraArgs { get; set; }
    }

    public class GoldLapel : IDisposable
    {
        internal const int DefaultPort = 7932;
        internal const long StartupTimeoutMs = 10000;
        internal const long StartupPollIntervalMs = 50;

        private readonly string _upstream;
        private readonly int _port;
        private readonly string[] _extraArgs;
        private Process _process;
        private string _proxyUrl;
        private bool _disposed;

        public GoldLapel(string upstream) : this(upstream, null) { }

        public GoldLapel(string upstream, GoldLapelOptions options)
        {
            _upstream = upstream;
            _port = options?.Port ?? DefaultPort;
            _extraArgs = options?.ExtraArgs ?? Array.Empty<string>();
        }

        public string StartProxy()
        {
            if (_process != null && !_process.HasExited)
                return _proxyUrl;

            var binary = FindBinary();
            var args = new List<string> { "--upstream", _upstream, "--port", _port.ToString() };
            args.AddRange(_extraArgs);

            var psi = new ProcessStartInfo
            {
                FileName = binary,
                Arguments = JoinArgs(args),
                UseShellExecute = false,
                RedirectStandardInput = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true
            };

            try
            {
                _process = Process.Start(psi);
                _process.StandardInput.Close();
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Failed to start Gold Lapel process", e);
            }

            // Drain stderr to prevent pipe-buffer deadlock
            var stderrBuf = new System.Text.StringBuilder();
            var stderrThread = new Thread(() =>
            {
                try
                {
                    var buffer = new char[1024];
                    int n;
                    while ((n = _process.StandardError.Read(buffer, 0, buffer.Length)) > 0)
                        stderrBuf.Append(buffer, 0, n);
                }
                catch { }
            });
            stderrThread.IsBackground = true;
            stderrThread.Start();

            // Drain stdout
            var stdoutThread = new Thread(() =>
            {
                try
                {
                    var buffer = new char[1024];
                    while (_process.StandardOutput.Read(buffer, 0, buffer.Length) > 0) { }
                }
                catch { }
            });
            stdoutThread.IsBackground = true;
            stdoutThread.Start();

            // Poll for port readiness
            var sw = Stopwatch.StartNew();
            var ready = false;
            while (sw.ElapsedMilliseconds < StartupTimeoutMs)
            {
                if (_process.HasExited) break;
                if (WaitForPort("127.0.0.1", _port, 500))
                {
                    ready = true;
                    break;
                }
            }

            if (!ready)
            {
                try { _process.Kill(); } catch { }
                try { _process.WaitForExit(5000); } catch { }
                try { stderrThread.Join(2000); } catch { }
                throw new InvalidOperationException(
                    "Gold Lapel failed to start on port " + _port +
                    " within " + (StartupTimeoutMs / 1000) + "s.\nstderr: " + stderrBuf
                );
            }

            _proxyUrl = MakeProxyUrl(_upstream, _port);
            return _proxyUrl;
        }

        public void StopProxy()
        {
            var proc = _process;
            _process = null;
            _proxyUrl = null;
            if (proc != null && !proc.HasExited)
            {
                try { proc.Kill(); } catch { }
                try { proc.WaitForExit(5000); } catch { }
            }
        }

        public string Url => _proxyUrl;

        public int Port => _port;

        public bool IsRunning => _process != null && !_process.HasExited;

        public void Dispose()
        {
            if (!_disposed)
            {
                StopProxy();
                _disposed = true;
            }
        }

        // ── Singleton ─────────────────────────────────────────

        private static GoldLapel _instance;
        private static bool _cleanupRegistered;
        private static readonly object _lock = new object();

        public static string Start(string upstream)
        {
            return Start(upstream, null);
        }

        public static string Start(string upstream, GoldLapelOptions options)
        {
            lock (_lock)
            {
                if (_instance != null && _instance.IsRunning)
                {
                    if (_instance._upstream != upstream)
                    {
                        throw new InvalidOperationException(
                            "Gold Lapel is already running for a different upstream. " +
                            "Call GoldLapel.Stop() before starting with a new upstream."
                        );
                    }
                    return _instance.Url;
                }
                _instance = new GoldLapel(upstream, options);
                if (!_cleanupRegistered)
                {
                    AppDomain.CurrentDomain.ProcessExit += (s, e) =>
                    {
                        lock (_lock)
                        {
                            if (_instance != null)
                            {
                                _instance.StopProxy();
                                _instance = null;
                            }
                        }
                    };
                    _cleanupRegistered = true;
                }
                return _instance.StartProxy();
            }
        }

        public static void Stop()
        {
            lock (_lock)
            {
                if (_instance != null)
                {
                    _instance.StopProxy();
                    _instance = null;
                }
            }
        }

        public static string ProxyUrl
        {
            get
            {
                lock (_lock)
                {
                    return _instance?.Url;
                }
            }
        }

        // ── Internal methods ──────────────────────────────────

        private static readonly Regex WithPort =
            new Regex(@"^(postgres(?:ql)?://(?:.*@)?)([^:/?#]+):(\d+)(.*)$");

        private static readonly Regex NoPort =
            new Regex(@"^(postgres(?:ql)?://(?:.*@)?)([^:/?#]+)(.*)$");

        internal static string FindBinary()
        {
            // 1. Explicit override via env var
            var envPath = Environment.GetEnvironmentVariable("GOLDLAPEL_BINARY");
            if (!string.IsNullOrEmpty(envPath))
            {
                if (File.Exists(envPath)) return envPath;
                throw new InvalidOperationException(
                    "GOLDLAPEL_BINARY points to " + envPath + " but file not found"
                );
            }

            // 2. NuGet runtime asset (binary next to assembly or runtimes/<rid>/native/)
            var bundled = FindBundledBinary();
            if (bundled != null) return bundled;

            // 3. On PATH
            var onPath = FindOnPath();
            if (onPath != null) return onPath;

            throw new InvalidOperationException(
                "Gold Lapel binary not found. Set GOLDLAPEL_BINARY env var, " +
                "install the NuGet package with bundled binaries, or ensure 'goldlapel' is on PATH."
            );
        }

        internal static string MakeProxyUrl(string upstream, int port)
        {
            // Uses regex instead of System.Uri to avoid decoding percent-encoded
            // characters in passwords (e.g. %40 for @), which would corrupt the URL.

            var m = WithPort.Match(upstream);
            if (m.Success)
                return m.Groups[1].Value + "localhost:" + port + m.Groups[4].Value;

            m = NoPort.Match(upstream);
            if (m.Success)
                return m.Groups[1].Value + "localhost:" + port + m.Groups[3].Value;

            // bare host:port
            if (!upstream.Contains("://") && upstream.Contains(":"))
                return "localhost:" + port;

            // bare host
            return "localhost:" + port;
        }

        internal static bool WaitForPort(string host, int port, long timeoutMs)
        {
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < timeoutMs)
            {
                try
                {
                    using (var client = new TcpClient())
                    {
                        var result = client.BeginConnect(host, port, null, null);
                        var connected = result.AsyncWaitHandle.WaitOne(500);
                        if (connected && client.Connected)
                            return true;
                    }
                }
                catch { }
                Thread.Sleep((int)StartupPollIntervalMs);
            }
            return false;
        }

        private static string FindBundledBinary()
        {
            var isWindows = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
            var binaryName = isWindows ? "goldlapel.exe" : "goldlapel";

            // Determine RID
            string rid;
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                rid = RuntimeInformation.OSArchitecture == Architecture.Arm64
                    ? "linux-arm64" : "linux-x64";
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            {
                rid = "osx-arm64";
            }
            else if (isWindows)
            {
                rid = "win-x64";
            }
            else
            {
                return null;
            }

            // Check next to the assembly (NuGet copies runtime assets here)
            var assemblyDir = Path.GetDirectoryName(typeof(GoldLapel).Assembly.Location);
            if (!string.IsNullOrEmpty(assemblyDir))
            {
                var nextTo = Path.Combine(assemblyDir, binaryName);
                if (File.Exists(nextTo)) return nextTo;

                // Check runtimes/<rid>/native/ relative to assembly
                var runtimePath = Path.Combine(assemblyDir, "runtimes", rid, "native", binaryName);
                if (File.Exists(runtimePath)) return runtimePath;
            }

            return null;
        }

        private static string FindOnPath()
        {
            var isWindows = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
            var names = isWindows
                ? new[] { "goldlapel.exe", "goldlapel" }
                : new[] { "goldlapel" };

            var pathEnv = Environment.GetEnvironmentVariable("PATH");
            if (string.IsNullOrEmpty(pathEnv)) return null;

            var separator = isWindows ? ';' : ':';
            foreach (var dir in pathEnv.Split(separator))
            {
                foreach (var name in names)
                {
                    var full = Path.Combine(dir, name);
                    if (File.Exists(full)) return full;
                }
            }
            return null;
        }

        private static string JoinArgs(List<string> args)
        {
            var parts = new string[args.Count];
            for (int i = 0; i < args.Count; i++)
            {
                var a = args[i];
                if (a.Contains(" ") || a.Contains("\""))
                    parts[i] = "\"" + a.Replace("\"", "\\\"") + "\"";
                else
                    parts[i] = a;
            }
            return string.Join(" ", parts);
        }
    }
}
