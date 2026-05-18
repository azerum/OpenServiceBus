using System.Diagnostics.Metrics;
using Microsoft.Extensions.Hosting;
using OpenServiceBus.Core.Diagnostics;
using OpenServiceBus.Core.Storage;

namespace OpenServiceBus.Amqp.Diagnostics;

/// <summary>
/// Registers OpenServiceBus's <see cref="ObservableGauge{T}"/> instruments at startup.
/// The Meter SDK polls the callbacks on every metric collection cycle, so the broker
/// publishes a fresh queue-depth reading per queue every scrape without any background
/// work between scrapes.
/// </summary>
public sealed class DiagnosticsHostedService : IHostedService
{
    private readonly IMessageStore _store;
    private readonly IQueueRegistry _queues;

    public DiagnosticsHostedService(IMessageStore store, IQueueRegistry queues)
    {
        _store = store;
        _queues = queues;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        // Observable gauge for queue depth: one measurement per queue per collection cycle,
        // tagged with messaging.destination.name. Captures both main queues and DLQ siblings.
        OpenServiceBusDiagnostics.Meter.CreateObservableGauge(
            "osb.queue.depth",
            ObserveQueueDepths,
            unit: "{message}",
            description: "Current message count per queue (active + locked + deferred + scheduled).");
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    private IEnumerable<Measurement<long>> ObserveQueueDepths()
    {
        // Read names from the store so DLQ siblings and rehydrated queues are included even
        // when the registry hasn't been told about them yet.
        foreach (var name in _store.ListQueueNames())
        {
            long count;
            try { count = _store.CountAsync(name).GetAwaiter().GetResult(); }
            catch { continue; }
            yield return new Measurement<long>(count,
                new KeyValuePair<string, object?>(OpenServiceBusDiagnostics.TagDestination, name));
        }
    }
}
