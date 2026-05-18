using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OpenServiceBus.Amqp.Hosting;

namespace OpenServiceBus.Amqp.WebSockets;

/// <summary>
/// AMQP-over-WebSocket bridge (M21). Hosts an <see cref="HttpListener"/> on a dedicated port
/// that accepts WebSocket upgrades with the <c>amqp</c> sub-protocol, then tunnels every
/// incoming binary frame to a loopback TCP connection on the running AMQP listener - and the
/// upstream replies back, framed as binary WebSocket messages.
///
/// This avoids modifying the existing AMQP pipeline at all: AMQPNetLite keeps owning the
/// raw socket, the bridge looks like just another TCP client to it.
/// </summary>
public sealed class WebSocketBridgeService : IHostedService, IAsyncDisposable
{
    private const string AmqpSubProtocol = "amqp";

    private readonly WebSocketBridgeOptions _options;
    private readonly IOptions<AmqpListenerOptions> _amqp;
    private readonly ILogger<WebSocketBridgeService> _logger;

    private HttpListener? _listener;
    private CancellationTokenSource? _cts;
    private Task? _acceptLoop;

    public WebSocketBridgeService(
        IOptions<WebSocketBridgeOptions> options,
        IOptions<AmqpListenerOptions> amqp,
        ILogger<WebSocketBridgeService> logger)
    {
        _options = options.Value;
        _amqp = amqp;
        _logger = logger;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (!_options.Enabled)
        {
            _logger.LogDebug("AMQP-over-WebSocket bridge disabled.");
            return Task.CompletedTask;
        }

        var prefix = $"http://{_options.Host}:{_options.Port}{_options.Path}";
        _listener = new HttpListener();
        _listener.Prefixes.Add(prefix);
        _listener.Start();

        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _acceptLoop = AcceptLoopAsync(_cts.Token);

        _logger.LogInformation(
            "AMQP-over-WebSocket bridge listening on {Prefix} → upstream {Up}:{Port}",
            prefix, _options.UpstreamHost, UpstreamPort);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_listener is null) return;
        _cts?.Cancel();
        try { _listener.Stop(); } catch { /* shutting down */ }
        if (_acceptLoop is not null)
        {
            try { await _acceptLoop.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken).ConfigureAwait(false); }
            catch { /* shutdown best-effort */ }
        }
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync(CancellationToken.None).ConfigureAwait(false);
        _cts?.Dispose();
        _listener?.Close();
    }

    private int UpstreamPort => _options.UpstreamPort ?? _amqp.Value.Port;

    private async Task AcceptLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested && _listener!.IsListening)
        {
            HttpListenerContext context;
            try { context = await _listener.GetContextAsync().ConfigureAwait(false); }
            catch (HttpListenerException) { return; }      // listener stopped
            catch (ObjectDisposedException) { return; }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "WebSocket accept loop hiccup; continuing.");
                continue;
            }

            // Detach handling onto the threadpool so the next upgrade isn't blocked on this one.
            _ = HandleAsync(context, ct);
        }
    }

    private async Task HandleAsync(HttpListenerContext context, CancellationToken ct)
    {
        if (!context.Request.IsWebSocketRequest)
        {
            context.Response.StatusCode = (int)HttpStatusCode.BadRequest;
            context.Response.Close();
            return;
        }

        WebSocketContext wsCtx;
        try
        {
            // AcceptWebSocketAsync with subprotocol=amqp echoes back Sec-WebSocket-Protocol: amqp
            // which is what Microsoft.Azure.Amqp's WebSocket transport asserts on connect.
            wsCtx = await context.AcceptWebSocketAsync(subProtocol: AmqpSubProtocol).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "WebSocket upgrade failed for {Remote}", context.Request.RemoteEndPoint);
            try { context.Response.Close(); } catch { /* ignore */ }
            return;
        }

        var ws = wsCtx.WebSocket;
        using var pumpCts = CancellationTokenSource.CreateLinkedTokenSource(ct);

        try
        {
            using var tcp = new TcpClient();
            await tcp.ConnectAsync(_options.UpstreamHost, UpstreamPort, pumpCts.Token).ConfigureAwait(false);
            var stream = tcp.GetStream();

            // Two pumps in parallel - first one to finish (clean close OR error) cancels the other.
            var wsToTcp = Task.Run(() => PumpWebSocketToTcpAsync(ws, stream, pumpCts.Token), pumpCts.Token);
            var tcpToWs = Task.Run(() => PumpTcpToWebSocketAsync(stream, ws, pumpCts.Token), pumpCts.Token);

            var first = await Task.WhenAny(wsToTcp, tcpToWs).ConfigureAwait(false);
            pumpCts.Cancel();
            try { await Task.WhenAll(wsToTcp, tcpToWs).WaitAsync(TimeSpan.FromSeconds(2)).ConfigureAwait(false); }
            catch { /* shutting down a pump */ }

            // Surface pump errors at debug level - most are just "connection closed".
            if (first.IsFaulted) _logger.LogDebug(first.Exception, "Bridge pump terminated with exception.");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "WebSocket bridge session failed.");
        }
        finally
        {
            try
            {
                if (ws.State is WebSocketState.Open or WebSocketState.CloseReceived)
                {
                    await ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "bridge end", CancellationToken.None).ConfigureAwait(false);
                }
            }
            catch { /* best effort */ }
            ws.Dispose();
        }
    }

    private static async Task PumpWebSocketToTcpAsync(WebSocket ws, NetworkStream tcp, CancellationToken ct)
    {
        var buffer = new byte[8192];
        while (!ct.IsCancellationRequested && ws.State == WebSocketState.Open)
        {
            var result = await ws.ReceiveAsync(buffer, ct).ConfigureAwait(false);
            if (result.MessageType == WebSocketMessageType.Close) return;
            if (result.Count > 0)
            {
                await tcp.WriteAsync(buffer.AsMemory(0, result.Count), ct).ConfigureAwait(false);
            }
        }
    }

    private static async Task PumpTcpToWebSocketAsync(NetworkStream tcp, WebSocket ws, CancellationToken ct)
    {
        var buffer = new byte[8192];
        while (!ct.IsCancellationRequested)
        {
            var read = await tcp.ReadAsync(buffer.AsMemory(), ct).ConfigureAwait(false);
            if (read == 0) return; // upstream closed
            // AMQP framing flows transparently as opaque bytes - the broker reads them as if from
            // a regular TCP client, so we just forward each TCP read as a binary WS message.
            await ws.SendAsync(buffer.AsMemory(0, read), WebSocketMessageType.Binary, endOfMessage: true, ct).ConfigureAwait(false);
        }
    }
}
