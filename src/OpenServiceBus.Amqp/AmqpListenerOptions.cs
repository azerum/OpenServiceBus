namespace OpenServiceBus.Amqp;

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
}
