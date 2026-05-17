using System.Collections.Concurrent;
using Azure.Messaging.ServiceBus;

namespace OpenServiceBus.Explorer.Sessions;

/// <summary>
/// Pools <see cref="ServiceBusClient"/> and per-queue <see cref="ServiceBusReceiver"/> instances
/// keyed by connection string. Peek-lock dispositions (Complete/Abandon/DeadLetter) require the
/// same receiver instance that originally received the message, so we keep receivers alive across
/// HTTP requests.
/// </summary>
public sealed class SessionManager : IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, Session> _sessions = new();

    public Session GetOrCreate(string connectionString)
    {
        return _sessions.GetOrAdd(connectionString, cs => new Session(cs));
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var session in _sessions.Values)
        {
            await session.DisposeAsync();
        }
        _sessions.Clear();
    }
}

public sealed class Session : IAsyncDisposable
{
    private readonly ServiceBusClient _client;
    private readonly ConcurrentDictionary<string, ServiceBusReceiver> _receivers = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, ServiceBusReceivedMessage> _lockedMessages = new(StringComparer.OrdinalIgnoreCase);

    internal Session(string connectionString)
    {
        _client = new ServiceBusClient(connectionString);
    }

    public ServiceBusSender Sender(string queue) => _client.CreateSender(queue);

    public ServiceBusReceiver Receiver(string queue) => _receivers.GetOrAdd(queue, q =>
        _client.CreateReceiver(q, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock,
            PrefetchCount = 0,
        }));

    /// <summary>Stash a received message so a later Complete/Abandon call can find it by lock-token.</summary>
    public void TrackLocked(ServiceBusReceivedMessage message)
    {
        _lockedMessages[message.LockToken] = message;
    }

    public bool TryTakeLocked(string lockToken, out ServiceBusReceivedMessage? message)
    {
        var found = _lockedMessages.TryRemove(lockToken, out var msg);
        message = msg;
        return found;
    }

    /// <summary>Look up a tracked locked message without removing it (used for RenewLock).</summary>
    public bool TryPeekLocked(string lockToken, out ServiceBusReceivedMessage? message)
    {
        var found = _lockedMessages.TryGetValue(lockToken, out var msg);
        message = msg;
        return found;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var receiver in _receivers.Values)
        {
            try { await receiver.DisposeAsync(); } catch { /* best effort */ }
        }
        _receivers.Clear();
        _lockedMessages.Clear();
        await _client.DisposeAsync();
    }
}
