namespace OpenServiceBus.Amqp.Hosting;

public sealed class AmqpListenerOptions
{
    public string Host { get; set; } = "0.0.0.0";

    public int Port { get; set; } = 5672;

    public string ContainerId { get; set; } = "OpenServiceBus";

    /// <summary>
    /// Idle timeout advertised on the outgoing AMQP Open frame, in milliseconds.
    /// AMQPNetLite issue #238 means the listener does not currently enforce dead-client detection
    /// purely from this value; advertising it still lets clients drive their own keep-alive logic.
    /// </summary>
    public uint IdleTimeoutMs { get; set; } = 30_000;

    public uint MaxFrameSize { get; set; } = 256 * 1024;

    /// <summary>
    /// Maximum message size advertised on link attach, in bytes. The Azure SDK reads this on
    /// link attach and rejects sends if it remains 0/unset (interprets as -1, refusing every message).
    /// Default 256 KB matches Azure Service Bus Standard tier.
    /// </summary>
    public ulong MaxMessageSize { get; set; } = 256 * 1024;

    /// <summary>
    /// Enable AMQPNetLite frame-level tracing routed through our logger at Debug level.
    /// Off by default — useful when diagnosing wire-protocol interop issues with new clients.
    /// </summary>
    public bool EnableFrameTracing { get; set; }
}
