using System.Collections.Concurrent;
using System.Threading.Channels;
using OpenServiceBus.Abstractions;

namespace OpenServiceBus.Broker;

/// <summary>
/// In-memory message store with peek-lock semantics.
///
/// Per queue:
///  - <c>messages</c>: every accepted message keyed by sequence number (active OR locked, but not yet completed).
///  - <c>available</c>: a <see cref="Channel{T}"/> of sequence numbers ready for delivery (FIFO).
///    Enqueue writes, dequeue reads, abandon writes back.
///  - <c>locks</c>: outstanding peek-locks keyed by lock token.
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

    public IReadOnlyCollection<string> ListQueueNames() => _queues.Keys.ToArray();

    public Task CreateQueueAsync(string queueName, CancellationToken cancellationToken = default)
    {
        _queues.TryAdd(queueName, new QueueState());
        return Task.CompletedTask;
    }

    public Task DeleteQueueAsync(string queueName, CancellationToken cancellationToken = default)
    {
        if (_queues.TryRemove(queueName, out var state))
        {
            state.Available.Writer.TryComplete();
        }
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

        state.Messages[seq] = message;
        if (!state.Available.Writer.TryWrite(seq))
        {
            throw new InvalidOperationException($"Queue '{queueName}' is closed.");
        }

        return Task.FromResult(message);
    }

    public Task<long> CountAsync(string queueName, CancellationToken cancellationToken = default)
    {
        var state = GetQueue(queueName);
        return Task.FromResult((long)state.Messages.Count);
    }

    public async Task<LockedMessage?> TryDequeueAsync(
        string queueName,
        TimeSpan lockDuration,
        CancellationToken cancellationToken = default)
    {
        var state = GetQueue(queueName);

        long seq;
        try
        {
            seq = await state.Available.Reader.ReadAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return null;
        }
        catch (ChannelClosedException)
        {
            return null;
        }

        if (!state.Messages.TryGetValue(seq, out var stored))
        {
            // The message was completed/deleted out-of-band - skip and try the next.
            return await TryDequeueAsync(queueName, lockDuration, cancellationToken).ConfigureAwait(false);
        }

        var lockToken = Guid.NewGuid();
        var lockedUntil = _timeProvider.GetUtcNow() + lockDuration;
        state.Locks[lockToken] = new LockEntry
        {
            SequenceNumber = seq,
            LockedUntil = lockedUntil,
        };

        return new LockedMessage
        {
            Message = stored,
            LockToken = lockToken,
            LockedUntil = lockedUntil,
        };
    }

    public Task<bool> TryCompleteAsync(
        string queueName,
        Guid lockToken,
        CancellationToken cancellationToken = default)
    {
        var state = GetQueue(queueName);
        if (!state.Locks.TryRemove(lockToken, out var entry))
        {
            return Task.FromResult(false);
        }
        state.Messages.TryRemove(entry.SequenceNumber, out _);
        return Task.FromResult(true);
    }

    public Task<bool> TryAbandonAsync(
        string queueName,
        Guid lockToken,
        CancellationToken cancellationToken = default)
    {
        var state = GetQueue(queueName);
        if (!state.Locks.TryRemove(lockToken, out var entry))
        {
            return Task.FromResult(false);
        }

        // Make it available again. The encoded body and sequence number remain unchanged;
        // the next delivery attempt will get a fresh lock token.
        state.Available.Writer.TryWrite(entry.SequenceNumber);
        return Task.FromResult(true);
    }

    public int ExpireLocks(string queueName, DateTimeOffset now)
    {
        if (!_queues.TryGetValue(queueName, out var state)) return 0;

        var expired = 0;
        foreach (var (token, entry) in state.Locks)
        {
            if (entry.LockedUntil > now) continue;
            if (state.Locks.TryRemove(token, out _))
            {
                state.Available.Writer.TryWrite(entry.SequenceNumber);
                expired++;
            }
        }
        return expired;
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
        public readonly ConcurrentDictionary<long, StoredMessage> Messages = new();
        public readonly ConcurrentDictionary<Guid, LockEntry> Locks = new();
        public readonly Channel<long> Available = Channel.CreateUnbounded<long>(new UnboundedChannelOptions
        {
            SingleReader = false,
            SingleWriter = false,
        });
    }

    private sealed class LockEntry
    {
        public required long SequenceNumber { get; init; }
        public required DateTimeOffset LockedUntil { get; set; }
    }
}
