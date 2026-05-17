using System.Collections.Concurrent;
using OpenServiceBus.Abstractions;

namespace OpenServiceBus.Broker;

public sealed class QueueManager : IQueueRegistry
{
    /// <summary>The suffix Service Bus uses for the dead-letter sub-entity of a queue.</summary>
    public const string DeadLetterSuffix = "/$DeadLetterQueue";

    private readonly IMessageStore _store;
    private readonly ConcurrentDictionary<string, QueueDescriptor> _queues = new(StringComparer.OrdinalIgnoreCase);

    public QueueManager(IMessageStore store)
    {
        _store = store;
    }

    public event EventHandler<QueueDescriptor>? QueueCreated;
    public event EventHandler<QueueDescriptor>? QueueDeleted;

    event EventHandler<QueueDescriptor> IQueueRegistry.QueueCreated
    {
        add => QueueCreated += value;
        remove => QueueCreated -= value;
    }

    event EventHandler<QueueDescriptor> IQueueRegistry.QueueDeleted
    {
        add => QueueDeleted += value;
        remove => QueueDeleted -= value;
    }

    /// <summary>
    /// Returns true when <paramref name="name"/> identifies a dead-letter sub-entity (e.g. <c>orders/$DeadLetterQueue</c>).
    /// DLQ entities do not themselves get DLQ siblings and have effectively unbounded redelivery to prevent loops.
    /// </summary>
    public static bool IsDeadLetterQueue(string name) =>
        name.EndsWith(DeadLetterSuffix, StringComparison.Ordinal);

    public async Task<QueueDescriptor> CreateAsync(QueueDescriptor descriptor, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(descriptor);
        ArgumentException.ThrowIfNullOrWhiteSpace(descriptor.Name);

        var existing = _queues.GetOrAdd(descriptor.Name, descriptor);
        if (!ReferenceEquals(existing, descriptor))
        {
            // Already existed - idempotent create.
            return existing;
        }

        await _store.CreateQueueAsync(descriptor.Name, cancellationToken).ConfigureAwait(false);
        QueueCreated?.Invoke(this, descriptor);

        // Every main queue gets an implicit dead-letter sibling — Azure Service Bus's contract.
        // DLQs themselves do NOT get DLQ siblings (no recursion).
        if (!IsDeadLetterQueue(descriptor.Name))
        {
            var dlq = new QueueDescriptor
            {
                Name = descriptor.Name + DeadLetterSuffix,
                LockDuration = descriptor.LockDuration,
                MaxDeliveryCount = int.MaxValue,
            };
            await CreateAsync(dlq, cancellationToken).ConfigureAwait(false);
        }

        return descriptor;
    }

    public Task<QueueDescriptor?> GetAsync(string name, CancellationToken cancellationToken = default)
    {
        _queues.TryGetValue(name, out var descriptor);
        return Task.FromResult(descriptor);
    }

    public Task<IReadOnlyList<QueueDescriptor>> ListAsync(CancellationToken cancellationToken = default)
    {
        IReadOnlyList<QueueDescriptor> snapshot = _queues.Values.ToArray();
        return Task.FromResult(snapshot);
    }

    public async Task<bool> DeleteAsync(string name, CancellationToken cancellationToken = default)
    {
        if (!_queues.TryRemove(name, out var descriptor))
        {
            return false;
        }
        await _store.DeleteQueueAsync(name, cancellationToken).ConfigureAwait(false);
        QueueDeleted?.Invoke(this, descriptor);

        // Tear down the DLQ sibling alongside the main queue.
        if (!IsDeadLetterQueue(name))
        {
            await DeleteAsync(name + DeadLetterSuffix, cancellationToken).ConfigureAwait(false);
        }

        return true;
    }
}
