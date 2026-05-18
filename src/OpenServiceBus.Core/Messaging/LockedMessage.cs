namespace OpenServiceBus.Core.Messaging;

/// <summary>
/// A message handed to a consumer under peek-lock. The lock token uniquely identifies
/// this single delivery attempt - the same underlying message will get a fresh lock token
/// on each re-delivery so consumers can settle the precise attempt they received.
/// </summary>
public sealed record LockedMessage
{
    public required StoredMessage Message { get; init; }
    public required Guid LockToken { get; init; }
    public required DateTimeOffset LockedUntil { get; init; }
}
