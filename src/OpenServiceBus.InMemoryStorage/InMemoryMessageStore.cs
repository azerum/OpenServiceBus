using System.Collections.Concurrent;
using System.Threading.Channels;
using OpenServiceBus.Core.Entities;
using OpenServiceBus.Core.Messaging;
using OpenServiceBus.Core.Storage;

namespace OpenServiceBus.InMemoryStorage;

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
        DateTimeOffset? expiresAt = null,
        DateTimeOffset? scheduledEnqueueTime = null,
        CancellationToken cancellationToken = default)
    {
        var state = GetQueue(queueName);
        var seq = Interlocked.Increment(ref state.NextSequenceNumber);
        var now = _timeProvider.GetUtcNow();
        // A scheduled time in the past is meaningless — treat it as available immediately.
        var effectiveSchedule = scheduledEnqueueTime is { } sched && sched > now ? scheduledEnqueueTime : null;
        var message = new StoredMessage
        {
            SequenceNumber = seq,
            EnqueuedAt = now,
            EncodedMessage = encodedMessage,
            ExpiresAt = expiresAt,
            ScheduledEnqueueTime = effectiveSchedule,
        };

        state.Messages[seq] = message;
        if (effectiveSchedule is null)
        {
            // Immediately available for delivery.
            if (!state.Available.Writer.TryWrite(seq))
            {
                throw new InvalidOperationException($"Queue '{queueName}' is closed.");
            }
        }
        // else: scheduled — waits in Messages until ActivateScheduled promotes it.

        return Task.FromResult(message);
    }

    public int ActivateScheduled(string queueName, DateTimeOffset now)
    {
        if (!_queues.TryGetValue(queueName, out var state)) return 0;

        var activated = 0;
        foreach (var (seq, msg) in state.Messages)
        {
            if (msg.ScheduledEnqueueTime is null) continue;       // not scheduled
            if (msg.ScheduledEnqueueTime > now) continue;          // not yet
            // Clear the scheduled marker atomically; only the winner of TryUpdate pushes to Available.
            var activated_ = msg with { ScheduledEnqueueTime = null };
            if (state.Messages.TryUpdate(seq, activated_, msg))
            {
                state.Available.Writer.TryWrite(seq);
                activated++;
            }
        }
        return activated;
    }

    public Task<bool> TryCancelScheduledAsync(
        string queueName,
        long sequenceNumber,
        CancellationToken cancellationToken = default)
    {
        if (!_queues.TryGetValue(queueName, out var state))
        {
            return Task.FromResult(false);
        }
        if (!state.Messages.TryGetValue(sequenceNumber, out var msg))
        {
            return Task.FromResult(false);
        }
        if (msg.ScheduledEnqueueTime is null)
        {
            // Already activated — caller should use normal disposition / lock-cancel paths.
            return Task.FromResult(false);
        }
        // TryRemove with comparison so we don't race with ActivateScheduled.
        return Task.FromResult(((ICollection<KeyValuePair<long, StoredMessage>>)state.Messages)
            .Remove(new KeyValuePair<long, StoredMessage>(sequenceNumber, msg)));
    }

    public IReadOnlyList<StoredMessage> ExpireMessages(string queueName, DateTimeOffset now)
    {
        if (!_queues.TryGetValue(queueName, out var state)) return Array.Empty<StoredMessage>();

        // Build a set of currently-locked sequence numbers — locked messages stay alive
        // even if they cross their TTL deadline; the lock holder gets the chance to settle them.
        var locked = new HashSet<long>();
        foreach (var entry in state.Locks.Values) locked.Add(entry.SequenceNumber);

        var expired = new List<StoredMessage>();
        foreach (var (seq, msg) in state.Messages)
        {
            if (!msg.IsExpired(now)) continue;
            if (locked.Contains(seq)) continue;
            if (state.Messages.TryRemove(seq, out var removed))
            {
                expired.Add(removed);
            }
        }
        return expired;
    }

    public Task<long> CountAsync(string queueName, CancellationToken cancellationToken = default)
    {
        var state = GetQueue(queueName);
        return Task.FromResult((long)state.Messages.Count);
    }

    public async Task<LockedMessage?> TryDequeueAsync(
        string queueName,
        TimeSpan lockDuration,
        string? associatedLinkName = null,
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
            return await TryDequeueAsync(queueName, lockDuration, associatedLinkName, cancellationToken).ConfigureAwait(false);
        }

        var lockToken = Guid.NewGuid();
        var lockedUntil = _timeProvider.GetUtcNow() + lockDuration;
        state.Locks[lockToken] = new LockEntry
        {
            SequenceNumber = seq,
            LockedUntil = lockedUntil,
            AssociatedLink = associatedLinkName,
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

        ReturnToAvailableWithIncrementedDeliveryCount(state, entry.SequenceNumber);
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
                ReturnToAvailableWithIncrementedDeliveryCount(state, entry.SequenceNumber);
                expired++;
            }
        }
        return expired;
    }

    /// <summary>
    /// Bump <see cref="StoredMessage.DeliveryCount"/> and re-queue. Because <c>StoredMessage</c>
    /// is immutable we write a new record with <c>DeliveryCount + 1</c>; the next delivery attempt
    /// will stamp it onto the AMQP header.
    /// </summary>
    private static void ReturnToAvailableWithIncrementedDeliveryCount(QueueState state, long sequenceNumber)
    {
        if (state.Messages.TryGetValue(sequenceNumber, out var existing))
        {
            state.Messages[sequenceNumber] = existing with { DeliveryCount = existing.DeliveryCount + 1 };
        }
        state.Available.Writer.TryWrite(sequenceNumber);
    }

    public Task<DateTimeOffset?> TryRenewLockAsync(
        string queueName,
        Guid lockToken,
        TimeSpan lockDuration,
        string? requestingLinkName = null,
        CancellationToken cancellationToken = default)
    {
        var state = GetQueue(queueName);
        if (!state.Locks.TryGetValue(lockToken, out var entry))
        {
            return Task.FromResult<DateTimeOffset?>(null);
        }

        // Link-affinity check: if the lock was taken from a specific link, only that link
        // can renew it. Matches Service Bus's lock-link scoping — a sneaky cross-link renew
        // attempt returns Gone.
        if (entry.AssociatedLink is not null
            && requestingLinkName is not null
            && !string.Equals(entry.AssociatedLink, requestingLinkName, StringComparison.Ordinal))
        {
            return Task.FromResult<DateTimeOffset?>(null);
        }

        var newUntil = _timeProvider.GetUtcNow() + lockDuration;
        entry.LockedUntil = newUntil;
        return Task.FromResult<DateTimeOffset?>(newUntil);
    }

    public Task<StoredMessage?> TryRemoveLockedAsync(
        string queueName,
        Guid lockToken,
        CancellationToken cancellationToken = default)
    {
        var state = GetQueue(queueName);
        if (!state.Locks.TryRemove(lockToken, out var entry))
        {
            return Task.FromResult<StoredMessage?>(null);
        }
        state.Messages.TryRemove(entry.SequenceNumber, out var msg);
        return Task.FromResult<StoredMessage?>(msg);
    }

    public IReadOnlyList<StoredMessage> Peek(string queueName, long fromSequenceNumber, int maxCount)
    {
        if (maxCount <= 0 || !_queues.TryGetValue(queueName, out var state))
        {
            return Array.Empty<StoredMessage>();
        }

        // Snapshot, filter by lower bound, sort, take.
        var result = new List<StoredMessage>(Math.Min(maxCount, state.Messages.Count));
        foreach (var (_, msg) in state.Messages)
        {
            if (msg.SequenceNumber >= fromSequenceNumber) result.Add(msg);
        }
        result.Sort((a, b) => a.SequenceNumber.CompareTo(b.SequenceNumber));
        if (result.Count > maxCount) result.RemoveRange(maxCount, result.Count - maxCount);
        return result;
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
        public string? AssociatedLink { get; init; }
    }
}
