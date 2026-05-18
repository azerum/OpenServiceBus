namespace OpenServiceBus.Testing;

/// <summary>
/// User-configurable options for <see cref="OpenServiceBusTestHost"/>.
/// </summary>
public sealed class OpenServiceBusTestHostOptions
{
    /// <summary>
    /// Loopback host the AMQP listener binds to. Defaults to <c>127.0.0.1</c> so tests don't
    /// expose the broker on a non-loopback interface.
    /// </summary>
    public string Host { get; set; } = "127.0.0.1";

    /// <summary>
    /// Fixed port to bind. When <c>null</c> (default) a free ephemeral port is chosen, which is
    /// the right choice for unit tests so concurrent fixtures don't collide.
    /// </summary>
    public int? Port { get; set; }

    /// <summary>
    /// Container-id reported on the AMQP Open frame. Set this to identify a particular fixture
    /// in trace output.
    /// </summary>
    public string ContainerId { get; set; } = "OpenServiceBus.Testing";

    /// <summary>
    /// AMQP idle timeout (ms) advertised to clients.
    /// </summary>
    public uint IdleTimeoutMs { get; set; } = 30_000;

    /// <summary>
    /// Maximum message size (bytes) advertised on link attach. The Azure SDK rejects sends if
    /// this is left unset, so the default matches Service Bus Standard tier (256 KB).
    /// </summary>
    public ulong MaxMessageSize { get; set; } = 256 * 1024;

    /// <summary>
    /// SAS key name used in the auto-generated connection string. Matches what the Azure SDK
    /// extracts from <c>SharedAccessKeyName=</c>. Default mirrors the official Microsoft emulator.
    /// </summary>
    public string SasKeyName { get; set; } = "RootManageSharedAccessKey";

    /// <summary>
    /// SAS key value used in the auto-generated connection string. Default mirrors the official
    /// emulator key so test code can be ported across emulators without re-keying.
    /// </summary>
    public string SasKey { get; set; } = "SAS_KEY_VALUE";

    /// <summary>
    /// When true the broker enforces SAS validation at <c>$cbs put-token</c>. The
    /// <see cref="SasKeyName"/>/<see cref="SasKey"/> pair is added to the broker key store
    /// automatically; add more via <see cref="AdditionalSasKeys"/>.
    /// </summary>
    public bool RequireSasAuth { get; set; }

    /// <summary>
    /// Extra (name → key) pairs trusted by <c>$cbs</c> in addition to
    /// <see cref="SasKeyName"/>/<see cref="SasKey"/>.
    /// </summary>
    public Dictionary<string, string> AdditionalSasKeys { get; } = new(StringComparer.Ordinal);

    /// <summary>
    /// <see cref="System.TimeProvider"/> used by the broker. Defaults to
    /// <see cref="TimeProvider.System"/>; tests that exercise TTL/lock/schedule timing should
    /// pass a <c>FakeTimeProvider</c> here.
    /// </summary>
    public TimeProvider TimeProvider { get; set; } = TimeProvider.System;

    /// <summary>
    /// Enable AMQPNetLite frame-level tracing. Off by default — useful only when diagnosing
    /// wire-protocol issues.
    /// </summary>
    public bool EnableFrameTracing { get; set; }
}
