using Microsoft.Extensions.Time.Testing;

namespace OpenServiceBus.InMemoryStorage.Tests;

public class PeekLockTests
{
    [Fact]
    public async Task TryDequeueAsync_MultipleEnqueuedMessages_ReturnsInFifoOrderWithUniqueLockTokens()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", new byte[] { 1 });
        await store.EnqueueAsync("q", new byte[] { 2 });

        // Act
        var first = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));
        var second = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));

        // Assert
        first.ShouldNotBeNull();
        second.ShouldNotBeNull();
        first.Message.SequenceNumber.ShouldBe(1L);
        second.Message.SequenceNumber.ShouldBe(2L);
        first.LockToken.ShouldNotBe(second.LockToken);
        first.LockToken.ShouldNotBe(Guid.Empty);
    }

    [Fact]
    public async Task TryCompleteAsync_OnLockedMessage_RemovesItFromStorage()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", new byte[] { 1 });
        var locked = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));
        locked.ShouldNotBeNull();

        // Act
        var completed = await store.TryCompleteAsync("q", locked.LockToken);

        // Assert
        completed.ShouldBeTrue();
        (await store.CountAsync("q")).ShouldBe(0L);
    }

    [Fact]
    public async Task TryCompleteAsync_SameTokenTwice_ReturnsFalseOnSecondCall()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", new byte[] { 1 });
        var locked = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));
        locked.ShouldNotBeNull();
        await store.TryCompleteAsync("q", locked.LockToken);

        // Act
        var secondCompletion = await store.TryCompleteAsync("q", locked.LockToken);

        // Assert
        secondCompletion.ShouldBeFalse();
    }

    [Fact]
    public async Task TryAbandonAsync_OnLockedMessage_ReturnsMessageToAvailablePoolWithFreshLockToken()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", new byte[] { 1 });
        var firstLock = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));
        firstLock.ShouldNotBeNull();

        // Act
        var abandoned = await store.TryAbandonAsync("q", firstLock.LockToken);
        var secondLock = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));

        // Assert
        abandoned.ShouldBeTrue();
        (await store.CountAsync("q")).ShouldBe(1L);
        secondLock.ShouldNotBeNull();
        secondLock.Message.SequenceNumber.ShouldBe(firstLock.Message.SequenceNumber);
        secondLock.LockToken.ShouldNotBe(firstLock.LockToken, "redelivery must use a fresh lock token");
    }

    [Fact]
    public async Task ExpireLocks_PastDeadline_ReleasesOnlyExpiredLocks()
    {
        // Arrange
        var fakeTime = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var store = new InMemoryMessageStore(fakeTime);
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", new byte[] { 1 });
        await store.EnqueueAsync("q", new byte[] { 2 });
        var shortLock = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(10));
        var longLock = await store.TryDequeueAsync("q", TimeSpan.FromMinutes(5));
        shortLock.ShouldNotBeNull();
        longLock.ShouldNotBeNull();
        fakeTime.Advance(TimeSpan.FromSeconds(30));

        // Act
        var expiredCount = store.ExpireLocks("q", fakeTime.GetUtcNow());
        var redelivered = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(10));

        // Assert
        expiredCount.ShouldBe(1, "only the 10s lock should have expired");
        redelivered.ShouldNotBeNull();
        redelivered.Message.SequenceNumber.ShouldBe(shortLock.Message.SequenceNumber);
        (await store.TryCompleteAsync("q", longLock.LockToken)).ShouldBeTrue("the 5-minute lock is still held and completable");
    }

    [Fact]
    public async Task ExpireLocks_QueueWithNoLocks_ReturnsZero()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");

        // Act
        var expiredCount = store.ExpireLocks("q", DateTimeOffset.UtcNow);

        // Assert
        expiredCount.ShouldBe(0);
    }
}
