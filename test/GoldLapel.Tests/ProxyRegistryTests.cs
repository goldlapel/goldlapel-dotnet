using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Xunit;
using GL = GoldLapel.GoldLapel;
using GoldLapel;

namespace GoldLapel.Tests
{
    // Port allocation and readiness, without a real proxy. The registry is
    // process-wide, so these share the "EnvVarTests" collection with every
    // test that starts a proxy.
    [Collection("EnvVarTests")]
    public class ProxyRegistryTests : IDisposable
    {
        private const string UpstreamA = "postgresql://alice:s3cret@db-a.example.com:5432/a";
        private const string UpstreamB = "postgresql://bob:hunter2@db-b.example.com:5432/b";

        private readonly string? _origBinary = Environment.GetEnvironmentVariable("GOLDLAPEL_BINARY");
        private readonly System.Collections.Generic.List<string> _tempFiles = new System.Collections.Generic.List<string>();

        public void Dispose()
        {
            GL.ForgetForTest(UpstreamA);
            GL.ForgetForTest(UpstreamB);
            Environment.SetEnvironmentVariable("GOLDLAPEL_BINARY", _origBinary);
            foreach (var f in _tempFiles)
            {
                try { File.Delete(f); } catch { }
            }
        }

        private static TcpListener Hold(int port)
        {
            var l = new TcpListener(IPAddress.Loopback, port);
            l.Start();
            return l;
        }

        [Fact]
        public void PicksDefaultPairWhenNothingIsClaimed()
        {
            var p = GL.PickProxyPort(null);
            Assert.True(p >= 7932);
            Assert.True(GL.CanBind(p));
            Assert.True(GL.CanBind(p + 1));
        }

        [Fact]
        public void SkipsPairsClaimedByThisProcess()
        {
            var first = GL.PickProxyPort(null);
            GL.ClaimForTest(UpstreamA, first, first + 1);
            var second = GL.PickProxyPort(null);
            Assert.NotEqual(first, second);
            Assert.NotEqual(first + 1, second);
            Assert.NotEqual(first, second + 1);
        }

        [Fact]
        public void SkipsAPairWhoseDashboardPortIsClaimed()
        {
            // Another proxy's explicit dashboard port sits on P+1.
            var first = GL.PickProxyPort(null);
            GL.ClaimForTest(UpstreamA, 40000, first + 1);
            var picked = GL.PickProxyPort(null);
            Assert.NotEqual(first, picked);
            Assert.NotEqual(first + 1, picked);
        }

        [SkippableFact]
        public void IgnoresClaimsOfAProxyThatHasExited()
        {
            Skip.If(RuntimeInformation.IsOSPlatform(OSPlatform.Windows), "POSIX-only");
            var first = GL.PickProxyPort(null);
            var dead = Process.Start(new ProcessStartInfo("true") { UseShellExecute = false })!;
            dead.WaitForExit();
            GL.ClaimForTest(UpstreamA, first, first + 1, dead);
            Assert.Equal(first, GL.PickProxyPort(null));
        }

        [Fact]
        public void SkipsPortsAnotherProcessHolds()
        {
            var first = GL.PickProxyPort(null);
            using (var held = new DisposableListener(Hold(first)))
            {
                Assert.False(GL.CanBind(first));
                var picked = GL.PickProxyPort(null);
                Assert.NotEqual(first, picked);
                Assert.NotEqual(first, picked + 1);
            }
            var dash = GL.PickProxyPort(null);
            using (var held = new DisposableListener(Hold(dash + 1)))
            {
                Assert.NotEqual(dash, GL.PickProxyPort(null));
            }
        }

        [SkippableFact]
        public void SkipsAPortHeldWithReusePortLikeTheProxy()
        {
            // The proxy listens with SO_REUSEPORT. The probe must not set it,
            // or it would co-bind the other proxy's port and pass.
            Skip.IfNot(RuntimeInformation.IsOSPlatform(OSPlatform.Linux), "SO_REUSEPORT value is Linux's");
            var port = GL.PickProxyPort(null);
            using var s = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            s.SetRawSocketOption(1 /* SOL_SOCKET */, 15 /* SO_REUSEPORT */, BitConverter.GetBytes(1));
            s.Bind(new IPEndPoint(IPAddress.Any, port));
            s.Listen(1);
            Assert.False(GL.CanBind(port));
            Assert.NotEqual(port, GL.PickProxyPort(null));
        }

        [Fact]
        public void ExplicitDashboardPortIsHonoured()
        {
            var first = GL.PickProxyPort(null);
            // With an explicit dashboard port, P+1 is not the dashboard, so
            // the pick only has to avoid that port itself.
            Assert.Equal(first, GL.PickProxyPort(first + 50));
            Assert.NotEqual(first, GL.PickProxyPort(first));
        }

        [Fact]
        public async Task ExplicitProxyPortHeldByAnotherUpstreamNamesItWithoutThePassword()
        {
            GL.ClaimForTest(UpstreamA, 41000, 41001);
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                GL.StartAsync(UpstreamB, o => { o.ProxyPort = 41000; o.Silent = true; }));
            Assert.Contains("41000", ex.Message);
            Assert.Contains("db-a.example.com", ex.Message);
            Assert.Contains("***", ex.Message);
            Assert.DoesNotContain("s3cret", ex.Message);
            Assert.False(GL.IsRegistered(UpstreamB));
        }

        [Theory]
        [InlineData("postgresql://u:p@ss@h:5432/db?x=a@b", "postgresql://u:***@h:5432/db?x=a@b")]
        [InlineData("postgresql://u:secret@h/db", "postgresql://u:***@h/db")]
        [InlineData("postgresql://u@h/db", "postgresql://u@h/db")]
        [InlineData("postgresql://h/db?x=a:b@c", "postgresql://h/db?x=a:b@c")]
        public void RedactPasswordMasksOnlyThePassword(string url, string expected)
        {
            Assert.Equal(expected, GL.RedactPassword(url));
        }

        [Fact]
        public async Task ExplicitPortOnAnotherProxysDashboardPortIsRefused()
        {
            GL.ClaimForTest(UpstreamA, 41000, 41001);
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                GL.StartAsync(UpstreamB, o => { o.ProxyPort = 41001; o.Silent = true; }));
            Assert.Contains("41001", ex.Message);
            Assert.Contains("dashboard", ex.Message);

            ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                GL.StartAsync(UpstreamB, o => { o.ProxyPort = 42000; o.DashboardPort = 41000; o.Silent = true; }));
            Assert.Contains("41000", ex.Message);
        }

        [Fact]
        public async Task FailedStartReleasesItsClaim()
        {
            Environment.SetEnvironmentVariable("GOLDLAPEL_BINARY", "/nonexistent/goldlapel");
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                GL.StartAsync(UpstreamA, o => o.Silent = true));
            Assert.False(GL.IsRegistered(UpstreamA));

            // Option validation fails before anything is claimed.
            await Assert.ThrowsAsync<ArgumentException>(() =>
                GL.StartAsync(UpstreamA, o => o.LogLevel = "chatty"));
            Assert.False(GL.IsRegistered(UpstreamA));
        }

        // ── Readiness (R2) ──────────────────────────────────────

        private string? Shim(string body)
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows)) return null;
            var path = Path.Combine(Path.GetTempPath(), "goldlapel-shim-" + Guid.NewGuid().ToString("N").Substring(0, 8));
            File.WriteAllText(path, "#!/bin/sh\n" + body + "\n");
            File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
            _tempFiles.Add(path);
            return path;
        }

        [SkippableFact]
        public async Task ChildThatExitsSurfacesStatusAndStderr()
        {
            var shim = Shim("echo \"I'm afraid port 7932, for the proxy, is already in use\" >&2; exit 1");
            Skip.If(shim == null, "POSIX-only shim");
            Environment.SetEnvironmentVariable("GOLDLAPEL_BINARY", shim);

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                GL.StartAsync(UpstreamA, o => o.Silent = true));
            Assert.Contains("status 1", ex.Message);
            Assert.Contains("already in use", ex.Message);
            Assert.False(GL.IsRegistered(UpstreamA));
        }

        [SkippableFact]
        public async Task BusyExplicitPortIsNotMistakenForReadiness()
        {
            // Something else answers on the port, so a bare TCP connect
            // passes. The child refuses the port and exits; that must win.
            var shim = Shim("sleep 0.3; echo \"I'm afraid port is already in use\" >&2; exit 1");
            Skip.If(shim == null, "POSIX-only shim");
            Environment.SetEnvironmentVariable("GOLDLAPEL_BINARY", shim);

            var port = GL.PickProxyPort(null);
            using var held = new DisposableListener(Hold(port));
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                GL.StartAsync(UpstreamA, o => { o.ProxyPort = port; o.Silent = true; }));
            Assert.Contains("already in use", ex.Message);
            Assert.Contains("status 1", ex.Message);
        }

        private sealed class DisposableListener : IDisposable
        {
            private readonly TcpListener _l;
            public DisposableListener(TcpListener l) { _l = l; }
            public void Dispose() => _l.Stop();
        }
    }
}
