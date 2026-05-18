using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using OpenServiceBus.Core.Storage;

namespace OpenServiceBus.SqliteStorage.DependencyInjection;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Register a SQLite-backed <see cref="IMessageStore"/>. Use this instead of
    /// <c>AddOpenServiceBusInMemoryStorage</c> when you want persistence across restarts —
    /// the rest of the broker stack (queue / topic registry, router, transactions, AMQP
    /// listener) is identical, so callers don't change.
    ///
    /// Pair with the in-memory <see cref="OpenServiceBus.InMemoryStorage.Queues.QueueManager"/>
    /// + <see cref="OpenServiceBus.InMemoryStorage.Topics.TopicManager"/> registrations for
    /// the registry layer (their data is currently small enough to keep in memory and
    /// reconstructed from <c>config.json</c> at startup).
    /// </summary>
    public static IServiceCollection AddOpenServiceBusSqliteStorage(
        this IServiceCollection services,
        Action<SqliteStorageOptions>? configure = null)
    {
        services.TryAddSingleton(TimeProvider.System);
        services.AddOptions<SqliteStorageOptions>();
        if (configure is not null)
        {
            services.Configure(configure);
        }
        services.TryAddSingleton<SqliteMessageStore>();
        services.TryAddSingleton<IMessageStore>(sp => sp.GetRequiredService<SqliteMessageStore>());
        return services;
    }
}
