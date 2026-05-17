using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace OpenServiceBus.Amqp;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the OpenServiceBus AMQP listener as a hosted service.
    /// Configure via <see cref="AmqpListenerOptions"/> (bound from the "OpenServiceBus:Amqp" section if using configuration).
    /// </summary>
    public static IServiceCollection AddOpenServiceBusAmqp(
        this IServiceCollection services,
        Action<AmqpListenerOptions>? configure = null)
    {
        services.AddOptions<AmqpListenerOptions>();
        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.TryAddSingleton<AmqpListenerHost>();
        services.AddHostedService(sp => sp.GetRequiredService<AmqpListenerHost>());

        return services;
    }
}
