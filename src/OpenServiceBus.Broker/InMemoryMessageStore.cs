using System.Collections.Concurrent;
using OpenServiceBus.Abstractions;

namespace OpenServiceBus.Broker;

/// <summary>
/// In-memory message store. Each queue gets its own ordered list and a monotonic sequence number.
/// Thread-safe for the operations exposed by <see cref="IMessageStore"/>.
/// </summary>
public sealed class InMemoryMessageStore : IMessageStore
{
    private readonly TimeProvider _timeProvider;
    private readonly ConcurrentDictionary<string, QueueState> _queues = new(StringComparer.OrdinalIgnoreCase);

    public InMemoryMessageStore() : this(TimeProvider.System) { }

    public InMemoryMessageStore(TimeProvider timeProvider)
    {
        _timeProvider = timeProvider;
    }

    public Task CreateQueueAsync(string queueName, CancellationToken cancellationToken = default)
    {
        _queues.TryAdd(queueName, new QueueState());
        return Task.CompletedTask;
    }

    public Task DeleteQueueAsync(string queueName, CancellationToken cancellationToken = default)
    {
        _queues.TryRemove(queueName, out _);
        return Task.CompletedTask;
    }

    public Task<StoredMessage> EnqueueAsync(
        string queueName,
        byte[] encodedMessage,
        CancellationToken cancellationToken = default)
    {
        var state = GetQueue(queueName);
        var seq = Interlocked.Increment(ref state.NextSequenceNumber);
        var message = new StoredMessage
        {
            SequenceNumber = seq,
            EnqueuedAt = _timeProvider.GetUtcNow(),
            EncodedMessage = encodedMessage,
        };
        lock (state.Lock)
        {
            state.Messages.Add(message);
        }
        return Task.FromResult(message);
    }

    public Task<long> CountAsync(string queueName, CancellationToken cancellationToken = default)
    {
        var state = GetQueue(queueName);
        lock (state.Lock)
        {
            return Task.FromResult((long)state.Messages.Count);
        }
    }

    private QueueState GetQueue(string queueName)
    {
        if (!_queues.TryGetValue(queueName, out var state))
        {
            throw new InvalidOperationException($"Queue '{queueName}' does not exist.");
        }
        return state;
    }

    private sealed class QueueState
    {
        public long NextSequenceNumber;
        public readonly object Lock = new();
        public readonly List<StoredMessage> Messages = [];
    }
}
