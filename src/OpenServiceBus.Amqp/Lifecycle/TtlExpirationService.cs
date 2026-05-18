using OpenServiceBus.Amqp.DeadLettering;
using OpenServiceBus.Amqp.Queues;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OpenServiceBus.Core.Entities;
using OpenServiceBus.Core.Messaging;
using OpenServiceBus.Core.Storage;

namespace OpenServiceBus.Amqp.Lifecycle;

/// <summary>
/// Periodic sweeper that removes (or dead-letters) messages past their TTL deadline.
/// Works alongside the dequeue-time check in <see cref="QueueReceiverSource"/> so that
/// idle queues with expired messages don't accumulate junk just because no consumer is reading.
/// </summary>
public sealed class TtlExpirationService : BackgroundService
{
    private static readonly TimeSpan SweepInterval = TimeSpan.FromMilliseconds(500);

    private readonly IMessageStore _store;
    private readonly IQueueRegistry _registry;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<TtlExpirationService> _logger;

    public TtlExpirationService(
        IMessageStore store,
        IQueueRegistry registry,
        TimeProvider timeProvider,
        ILogger<TtlExpirationService> logger)
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
                var expired = _store.ExpireMessages(queue.Name, now);
                if (expired.Count == 0) continue;

                var routeToDlq = !EntityNames.IsDeadLetterQueue(queue.Name)
                    && queue.DeadLetteringOnMessageExpiration;

                if (routeToDlq)
                {
                    var dlqName = queue.Name + EntityNames.DeadLetterSuffix;
                    foreach (var msg in expired)
                    {
                        var dlqBytes = DeadLetterEncoder.AppendDeadLetterHeaders(
                            msg.EncodedMessage,
                            queue.Name,
                            QueueReceiverSource.TtlExpiredReason,
                            QueueReceiverSource.TtlExpiredDescription);
                        await _store.EnqueueAsync(dlqName, dlqBytes, expiresAt: null, cancellationToken: cancellationToken).ConfigureAwait(false);
                    }
                    _logger.LogDebug("TTL-expired {Count} message(s) from {Queue} → {Dlq}", expired.Count, queue.Name, dlqName);
                }
                else
                {
                    _logger.LogDebug("TTL-dropped {Count} message(s) from {Queue}", expired.Count, queue.Name);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "TTL sweep failed for queue {Queue}", queue.Name);
            }
        }
    }
}
