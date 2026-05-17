using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OpenServiceBus.Abstractions;

namespace OpenServiceBus.Broker;

/// <summary>
/// Periodically sweeps the store for expired peek-locks and abandons them, returning their
/// messages to the available pool. Tests can drive expiration directly via
/// <see cref="IMessageStore.ExpireLocks"/> with a FakeTimeProvider — this background loop
/// exists for runtime, not for tests.
/// </summary>
public sealed class LockManager : BackgroundService
{
    private static readonly TimeSpan SweepInterval = TimeSpan.FromMilliseconds(100);

    private readonly IMessageStore _store;
    private readonly IQueueRegistry _registry;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<LockManager> _logger;

    public LockManager(
        IMessageStore store,
        IQueueRegistry registry,
        TimeProvider timeProvider,
        ILogger<LockManager> logger)
    {
        _store = store;
        _registry = registry;
        _timeProvider = timeProvider;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(SweepInterval, _timeProvider);
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
            {
                await SweepOnceAsync(stoppingToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // Normal shutdown.
        }
    }

    private async Task SweepOnceAsync(CancellationToken cancellationToken)
    {
        var now = _timeProvider.GetUtcNow();
        var queues = await _registry.ListAsync(cancellationToken).ConfigureAwait(false);
        foreach (var q in queues)
        {
            try
            {
                var expired = _store.ExpireLocks(q.Name, now);
                if (expired > 0)
                {
                    _logger.LogDebug("Expired {Count} lock(s) on queue {Queue}", expired, q.Name);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Lock expiration sweep failed for queue {Queue}", q.Name);
            }
        }
    }
}
