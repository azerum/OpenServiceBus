namespace OpenServiceBus.Amqp.WebSockets;

/// <summary>
/// Configuration for the AMQP-over-WebSocket bridge (M21). When enabled, the broker exposes
/// <c>ws://&lt;host&gt;:&lt;port&gt;/$servicebus/websocket/</c> for clients that can't open a raw
/// TCP AMQP socket (browsers, restrictive corporate firewalls). Incoming WebSocket binary
/// frames are pumped to the local AMQP listener and vice versa.
/// </summary>
public sealed class WebSocketBridgeOptions
{
    /// <summary>
    /// Master switch. Off by default — set to <c>true</c> in <c>appsettings.json</c> under
    /// <c>OpenServiceBus:WebSockets:Enabled</c>, or pass <c>configure</c> to the DI extension.
    /// </summary>
    public bool Enabled { get; set; }

    /// <summary>
    /// Host prefix for <see cref="System.Net.HttpListener"/>. Defaults to <c>+</c> (all
    /// interfaces) which matches typical container hosting; tests prefer <c>127.0.0.1</c>.
    /// </summary>
    public string Host { get; set; } = "+";

    /// <summary>
    /// Port the WebSocket bridge listens on. Defaults to 5673 — one above the AMQP port — so
    /// the existing <c>5672</c> mapping stays untouched.
    /// </summary>
    public int Port { get; set; } = 5673;

    /// <summary>
    /// HTTP path the bridge responds on. Defaults to <c>/$servicebus/websocket/</c> — the
    /// path the Azure SDK appends when <c>TransportType=AmqpWebSockets</c>.
    /// </summary>
    public string Path { get; set; } = "/$servicebus/websocket/";

    /// <summary>
    /// TCP endpoint the upstream AMQP listener is on. Defaults to <c>127.0.0.1</c> + the
    /// broker's AMQP port — bridge and broker live in the same process so loopback is correct.
    /// </summary>
    public string UpstreamHost { get; set; } = "127.0.0.1";

    /// <summary>Upstream AMQP port. Falls back to the configured AMQP listener port if unset.</summary>
    public int? UpstreamPort { get; set; }
}
