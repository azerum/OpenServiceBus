using System.Reflection;
using Amqp;
using Amqp.Framing;
using Amqp.Handler;
using Amqp.Listener;

namespace OpenServiceBus.Amqp.Hosting;

/// <summary>
/// Per-connection handler that hooks two points in the AMQPNetLite pipeline:
///
/// <para><b>ConnectionLocalOpen</b> — stamp container-id, idle-timeout, max-frame-size on the
/// outgoing AMQP Open frame. Workaround for AMQPNetLite #238 which means these are not
/// configurable directly on <see cref="ConnectionListener"/>.</para>
///
/// <para><b>SendDelivery</b> — when an outgoing delivery carries a <see cref="ReceiveContext"/>
/// in its UserToken (the IMessageSource path), copy its peek-lock token into the AMQP
/// <c>delivery-tag</c>. The Azure SDK's <c>ServiceBusReceiver.CompleteMessageAsync</c>
/// rejects messages with empty lock tokens — a non-Guid delivery-tag round-trips as
/// <see cref="Guid.Empty"/> and Complete throws InvalidOperationException.</para>
/// </summary>
internal sealed class ListenerEventHandler : IHandler
{
    // Delivery.Tag and Delivery.UserToken are internal in AMQPNetLite; reflect once at startup.
    private static readonly Type DeliveryType =
        typeof(Connection).Assembly.GetType("Amqp.Delivery", throwOnError: true)!;

    private static readonly PropertyInfo DeliveryTagProperty =
        DeliveryType.GetProperty("Tag", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
        ?? throw new InvalidOperationException("AMQPNetLite Delivery.Tag not found - upstream API changed.");

    private static readonly PropertyInfo DeliveryUserTokenProperty =
        DeliveryType.GetProperty("UserToken", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
        ?? throw new InvalidOperationException("AMQPNetLite Delivery.UserToken not found - upstream API changed.");

    private readonly uint _idleTimeoutMs;
    private readonly uint _maxFrameSize;
    private readonly string _containerId;

    public ListenerEventHandler(string containerId, uint idleTimeoutMs, uint maxFrameSize)
    {
        _containerId = containerId;
        _idleTimeoutMs = idleTimeoutMs;
        _maxFrameSize = maxFrameSize;
    }

    public bool CanHandle(EventId id) =>
        id == EventId.ConnectionLocalOpen || id == EventId.SendDelivery;

    public void Handle(Event protocolEvent)
    {
        if (protocolEvent.Id == EventId.ConnectionLocalOpen && protocolEvent.Context is Open open)
        {
            open.ContainerId = _containerId;
            open.IdleTimeOut = _idleTimeoutMs;
            open.MaxFrameSize = _maxFrameSize;
            return;
        }

        if (protocolEvent.Id == EventId.SendDelivery && protocolEvent.Context is { } delivery
            && DeliveryType.IsInstanceOfType(delivery))
        {
            var userToken = DeliveryUserTokenProperty.GetValue(delivery);
            if (userToken is ReceiveContext rc && rc.UserToken is Guid lockToken)
            {
                DeliveryTagProperty.SetValue(delivery, lockToken.ToByteArray());
            }
        }
    }
}
