using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OpenServiceBus.Core.Entities;
using OpenServiceBus.Core.Storage;

namespace OpenServiceBus.Host;

/// <summary>
/// After config bootstrap, ensure the in-memory <see cref="IQueueRegistry"/> has a descriptor
/// for every queue the backing <see cref="IMessageStore"/> knows about. Only relevant with a
/// persistent store (M18 SQLite) — on restart the SQLite file still has the queue rows but
/// the registry is empty memory. Rehydrating with default descriptors means existing messages
/// are visible to receivers and counts surface in the management API; per-queue settings reset
/// to defaults though, so prefer bootstrapping settings via <c>config.json</c>.
/// </summary>
public sealed class QueueRehydrationHostedService : IHostedService
{
    private readonly IMessageStore _store;
    private readonly IQueueRegistry _queues;
    private readonly ILogger<QueueRehydrationHostedService> _logger;

    public QueueRehydrationHostedService(
        IMessageStore store,
        IQueueRegistry queues,
        ILogger<QueueRehydrationHostedService> logger)
    {
        _store = store;
        _queues = queues;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var known = (await _queues.ListAsync(cancellationToken).ConfigureAwait(false))
            .Select(q => q.Name)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var rehydrated = 0;
        foreach (var name in _store.ListQueueNames())
        {
            if (known.Contains(name)) continue;
            // The DLQ sibling is created automatically by QueueManager when the parent is added —
            // skip discovering it directly so we don't end up double-registering or fighting the auto-sibling logic.
            if (name.EndsWith("/$DeadLetterQueue", StringComparison.Ordinal)) continue;

            await _queues.CreateAsync(new QueueDescriptor { Name = name }, cancellationToken).ConfigureAwait(false);
            rehydrated++;
        }

        if (rehydrated > 0)
        {
            _logger.LogInformation("Rehydrated {Count} queue(s) from persistent store with default descriptors.", rehydrated);
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
