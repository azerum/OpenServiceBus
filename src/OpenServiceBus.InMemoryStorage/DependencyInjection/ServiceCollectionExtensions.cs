using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using OpenServiceBus.Core.Storage;
using OpenServiceBus.InMemoryStorage.Lifecycle;
using OpenServiceBus.InMemoryStorage.Queues;

namespace OpenServiceBus.InMemoryStorage.DependencyInjection;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the in-memory storage adapter: <see cref="InMemoryMessageStore"/>, <see cref="QueueManager"/>,
    /// a singleton <see cref="TimeProvider"/> (system clock by default; tests can replace with FakeTimeProvider),
    /// and the <see cref="LockManager"/> background sweeper.
    /// </summary>
    public static IServiceCollection AddOpenServiceBusInMemoryStorage(this IServiceCollection services)
    {
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<InMemoryMessageStore>();
        services.TryAddSingleton<IMessageStore>(sp => sp.GetRequiredService<InMemoryMessageStore>());
        services.TryAddSingleton<QueueManager>();
        services.TryAddSingleton<IQueueRegistry>(sp => sp.GetRequiredService<QueueManager>());
        services.AddHostedService<LockManager>();
        return services;
    }
}
