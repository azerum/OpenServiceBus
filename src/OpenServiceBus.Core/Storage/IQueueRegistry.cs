using OpenServiceBus.Core.Entities;

namespace OpenServiceBus.Core.Storage;

/// <summary>
/// Tracks queue entities and emits lifecycle events the AMQP listener subscribes to
/// so it can register/unregister per-entity link handlers without coupling the broker
/// to AMQP types directly.
/// </summary>
public interface IQueueRegistry
{
    /// <summary>Create a queue. No-op if it already exists (caller decides on conflict policy).</summary>
    Task<QueueDescriptor> CreateAsync(QueueDescriptor descriptor, CancellationToken cancellationToken = default);

    Task<QueueDescriptor?> GetAsync(string name, CancellationToken cancellationToken = default);

    Task<IReadOnlyList<QueueDescriptor>> ListAsync(CancellationToken cancellationToken = default);

    Task<bool> DeleteAsync(string name, CancellationToken cancellationToken = default);

    /// <summary>Raised after a new queue has been created and its storage allocated.</summary>
    event EventHandler<QueueDescriptor> QueueCreated;

    /// <summary>Raised after a queue has been deleted.</summary>
    event EventHandler<QueueDescriptor> QueueDeleted;
}
