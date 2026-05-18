namespace OpenServiceBus.InMemoryStorage.Tests;

public class DeferTests
{
    [Fact]
    public async Task TryDeferAsync_LockedMessage_HidesItFromNormalDequeueButKeepsItInStorage()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", [1]);
        var locked = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));
        locked.ShouldNotBeNull();
        (await store.TryDeferAsync("q", locked.LockToken)).ShouldBeTrue();
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));

        // Act
        var none = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30), cancellationToken: cts.Token);

        // Assert
        none.ShouldBeNull("normal dequeue must skip deferred messages");
        (await store.CountAsync("q")).ShouldBe(1L, "deferred message remains in storage");
    }

    [Fact]
    public async Task TryReceiveDeferredAsync_DeferredMessage_ReturnsItUnderFreshLockToken()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        var enqueued = await store.EnqueueAsync("q", [1, 2, 3]);
        var first = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));
        first.ShouldNotBeNull();
        await store.TryDeferAsync("q", first.LockToken);

        // Act
        var deferred = await store.TryReceiveDeferredAsync("q", enqueued.SequenceNumber, TimeSpan.FromSeconds(30));

        // Assert
        deferred.ShouldNotBeNull();
        deferred.Message.EncodedMessage.ShouldBe(new byte[] { 1, 2, 3 });
        deferred.LockToken.ShouldNotBe(first.LockToken, "receive-by-seq must issue a fresh lock token");
        (await store.TryCompleteAsync("q", deferred.LockToken)).ShouldBeTrue();
        (await store.CountAsync("q")).ShouldBe(0L);
    }

    [Fact]
    public async Task TryAbandonAsync_DeferredMessage_ReturnsItToDeferredStateNotActive()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        var enqueued = await store.EnqueueAsync("q", [1]);
        var locked = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));
        locked.ShouldNotBeNull();
        await store.TryDeferAsync("q", locked.LockToken);
        var retrieved = await store.TryReceiveDeferredAsync("q", enqueued.SequenceNumber, TimeSpan.FromSeconds(30));
        retrieved.ShouldNotBeNull();

        // Act
        (await store.TryAbandonAsync("q", retrieved.LockToken)).ShouldBeTrue();
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        var none = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30), cancellationToken: cts.Token);
        var again = await store.TryReceiveDeferredAsync("q", enqueued.SequenceNumber, TimeSpan.FromSeconds(30));

        // Assert
        none.ShouldBeNull("normal dequeue must still skip the message after abandon");
        again.ShouldNotBeNull("receive-by-seq still finds the message — it stays deferred");
    }

    [Fact]
    public async Task TryReceiveDeferredAsync_MessageIsNotDeferred_ReturnsNull()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        var enqueued = await store.EnqueueAsync("q", [1]);

        // Act
        var notDeferred = await store.TryReceiveDeferredAsync("q", enqueued.SequenceNumber, TimeSpan.FromSeconds(30));

        // Assert
        notDeferred.ShouldBeNull();
    }
}
