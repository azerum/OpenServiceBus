using Microsoft.Extensions.Time.Testing;
using OpenServiceBus.Core.Messaging;

namespace OpenServiceBus.InMemoryStorage.Tests;

public class TtlTests
{
    [Fact]
    public async Task ExpireMessages_SomeMessagesPastDeadline_RemovesAndReturnsOnlyExpiredOnes()
    {
        // Arrange
        var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var store = new InMemoryMessageStore(time);
        await store.CreateQueueAsync("q");
        var soon = time.GetUtcNow() + TimeSpan.FromSeconds(10);
        var later = time.GetUtcNow() + TimeSpan.FromMinutes(5);
        await store.EnqueueAsync("q", [1], expiresAt: soon);
        await store.EnqueueAsync("q", [2], expiresAt: later);
        time.Advance(TimeSpan.FromSeconds(30));

        // Act
        var expired = store.ExpireMessages("q", time.GetUtcNow());

        // Assert
        expired.Count.ShouldBe(1);
        expired[0].EncodedMessage.ShouldBe(new byte[] { 1 });
        (await store.CountAsync("q")).ShouldBe(1L, "only the not-yet-expired message remains");
    }

    [Fact]
    public async Task ExpireMessages_LockedMessage_DoesNotExpireWhileLocked()
    {
        // Arrange
        var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var store = new InMemoryMessageStore(time);
        await store.CreateQueueAsync("q");
        var soon = time.GetUtcNow() + TimeSpan.FromSeconds(10);
        await store.EnqueueAsync("q", [1], expiresAt: soon);
        var locked = await store.TryDequeueAsync("q", TimeSpan.FromMinutes(5));
        locked.ShouldNotBeNull();
        time.Advance(TimeSpan.FromSeconds(30));

        // Act
        var expired = store.ExpireMessages("q", time.GetUtcNow());

        // Assert
        expired.Count.ShouldBe(0, "locked messages must NOT be expired by the sweeper - the lock holder owns them");
        (await store.CountAsync("q")).ShouldBe(1L);
    }

    [Fact]
    public async Task ExpireMessages_MessageHasNoTtl_ReturnsEmptyAndRetainsMessage()
    {
        // Arrange
        var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var store = new InMemoryMessageStore(time);
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", [1], expiresAt: null);
        time.Advance(TimeSpan.FromHours(24));

        // Act
        var expired = store.ExpireMessages("q", time.GetUtcNow());

        // Assert
        expired.Count.ShouldBe(0);
        (await store.CountAsync("q")).ShouldBe(1L);
    }

    [Fact]
    public void IsExpired_NoExpiresAt_ReturnsFalse()
    {
        // Arrange
        var msg = new StoredMessage
        {
            SequenceNumber = 1,
            EnqueuedAt = DateTimeOffset.UtcNow,
            EncodedMessage = [1],
            ExpiresAt = null,
        };

        // Act
        var expired = msg.IsExpired(DateTimeOffset.UtcNow.AddYears(10));

        // Assert
        expired.ShouldBeFalse();
    }

    [Fact]
    public void IsExpired_NowEqualsDeadline_ReturnsTrueInclusiveOfDeadline()
    {
        // Arrange
        var deadline = DateTimeOffset.UtcNow;
        var msg = new StoredMessage
        {
            SequenceNumber = 1,
            EnqueuedAt = deadline.AddMinutes(-1),
            EncodedMessage = [1],
            ExpiresAt = deadline,
        };

        // Act
        var atDeadline = msg.IsExpired(deadline);
        var oneSecondBefore = msg.IsExpired(deadline.AddSeconds(-1));

        // Assert
        atDeadline.ShouldBeTrue();
        oneSecondBefore.ShouldBeFalse();
    }
}
