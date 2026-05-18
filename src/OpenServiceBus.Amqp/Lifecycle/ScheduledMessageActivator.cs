using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OpenServiceBus.Core.Storage;

namespace OpenServiceBus.Amqp.Lifecycle;

/// <summary>
/// Periodic sweeper that promotes scheduled messages whose <c>ScheduledEnqueueTime</c> has
/// arrived from "scheduled" to "available" in the store. The dequeue side never sees
/// scheduled messages until this service moves them — so idle queues with future-dated
/// messages stay quiescent until their time.
/// </summary>
public sealed class ScheduledMessageActivator : BackgroundService
{
    private static readonly TimeSpan SweepInterval = TimeSpan.FromMilliseconds(500);

    private readonly IMessageStore _store;
    private readonly IQueueRegistry _registry;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<ScheduledMessageActivator> _logger;

    public ScheduledMessageActivator(
        IMessageStore store,
        IQueueRegistry registry,
        TimeProvider timeProvider,
        ILogger<ScheduledMessageActivator> logger)
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
        catch (OperationCanceledException) { /* normal shutdown */ }
    }

    private async Task SweepOnceAsync(CancellationToken cancellationToken)
    {
        var now = _timeProvider.GetUtcNow();
        var queues = await _registry.ListAsync(cancellationToken).ConfigureAwait(false);
        foreach (var queue in queues)
        {
            try
            {
                var activated = _store.ActivateScheduled(queue.Name, now);
                if (activated > 0)
                {
                    _logger.LogDebug("Activated {Count} scheduled message(s) on {Queue}", activated, queue.Name);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Scheduled-message sweep failed for queue {Queue}", queue.Name);
            }
        }
    }
}
