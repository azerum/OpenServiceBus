using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OpenServiceBus.Core.Configuration;
using OpenServiceBus.Core.Entities;
using OpenServiceBus.Core.Storage;

namespace OpenServiceBus.Host;

/// <summary>
/// At startup, declares any queues listed in a Microsoft-emulator-compatible <c>config.json</c>.
/// Resolution order:
///   1. <c>--config &lt;path&gt;</c> CLI argument
///   2. <c>OPENSERVICEBUS_CONFIG</c> environment variable
///   3. <c>config.json</c> in the current working directory (only if present)
/// </summary>
public sealed class ConfigBootstrapHostedService : IHostedService
{
    private const string ConfigFlag = "--config";
    private const string ConfigEnv = "OPENSERVICEBUS_CONFIG";
    private const string DefaultFileName = "config.json";

    private readonly IQueueRegistry _queues;
    private readonly ILogger<ConfigBootstrapHostedService> _logger;
    private readonly IHostEnvironment _env;

    public ConfigBootstrapHostedService(IQueueRegistry queues, IHostEnvironment env, ILogger<ConfigBootstrapHostedService> logger)
    {
        _queues = queues;
        _env = env;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var path = ResolveConfigPath();
        if (path is null)
        {
            _logger.LogInformation("No bootstrap config found ({Flag}, env {Env}, or {Default} in cwd). Starting with no pre-declared queues.",
                ConfigFlag, ConfigEnv, DefaultFileName);
            return;
        }

        _logger.LogInformation("Loading bootstrap config from {Path}", path);

        EmulatorConfigLoader.LoadResult result;
        try
        {
            result = EmulatorConfigLoader.LoadFromFile(path);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to parse bootstrap config at {Path}. Continuing without bootstrapping.", path);
            return;
        }

        foreach (var warning in result.Warnings)
        {
            _logger.LogWarning("config.json: {Warning}", warning);
        }

        foreach (var descriptor in result.Queues)
        {
            await _queues.CreateAsync(descriptor);
            _logger.LogInformation("Bootstrapped queue '{Name}' (lockDuration={Lock}, maxDeliveryCount={Max}, ttl={Ttl}, dlqOnExpire={Dlq})",
                descriptor.Name, descriptor.LockDuration, descriptor.MaxDeliveryCount,
                descriptor.DefaultMessageTimeToLive?.ToString() ?? "(none)",
                descriptor.DeadLetteringOnMessageExpiration);
        }

        _logger.LogInformation("Bootstrap complete: {Count} queue(s) declared.", result.Queues.Count);
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    private string? ResolveConfigPath()
    {
        // 1. CLI: --config <path>
        var args = Environment.GetCommandLineArgs();
        for (var i = 0; i < args.Length - 1; i++)
        {
            if (string.Equals(args[i], ConfigFlag, StringComparison.Ordinal))
            {
                return args[i + 1];
            }
        }

        // 2. Env var
        var envValue = Environment.GetEnvironmentVariable(ConfigEnv);
        if (!string.IsNullOrWhiteSpace(envValue))
        {
            return envValue;
        }

        // 3. Default: config.json in cwd
        var cwdCandidate = Path.Combine(_env.ContentRootPath, DefaultFileName);
        if (File.Exists(cwdCandidate))
        {
            return cwdCandidate;
        }

        return null;
    }
}
