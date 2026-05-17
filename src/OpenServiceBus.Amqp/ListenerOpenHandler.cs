using Amqp.Framing;
using Amqp.Handler;

namespace OpenServiceBus.Amqp;

/// <summary>
/// Stamps idle-timeout and max-frame-size on outgoing AMQP Open frames so that
/// clients see broker-side limits at link attach. Workaround for AMQPNetLite #238
/// which means these are not configurable directly on <see cref="Amqp.Listener.ConnectionListener"/>.
/// </summary>
internal sealed class ListenerOpenHandler : IHandler
{
    private readonly uint _idleTimeoutMs;
    private readonly uint _maxFrameSize;
    private readonly string _containerId;

    public ListenerOpenHandler(string containerId, uint idleTimeoutMs, uint maxFrameSize)
    {
        _containerId = containerId;
        _idleTimeoutMs = idleTimeoutMs;
        _maxFrameSize = maxFrameSize;
    }

    public bool CanHandle(EventId id) => id == EventId.ConnectionLocalOpen;

    public void Handle(Event protocolEvent)
    {
        if (protocolEvent.Context is Open open)
        {
            open.ContainerId = _containerId;
            open.IdleTimeOut = _idleTimeoutMs;
            open.MaxFrameSize = _maxFrameSize;
        }
    }
}
