using Microsoft.Extensions.Time.Testing;

namespace OpenServiceBus.InMemoryStorage.Tests;

public class DeliveryCountTests
{
    [Fact]
    public async Task TryDequeueAsync_FirstDelivery_ReturnsDeliveryCountZero()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", [1]);

        // Act
        var locked = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));

        // Assert
        locked.ShouldNotBeNull();
        locked.Message.DeliveryCount.ShouldBe(0);
    }

    [Fact]
    public async Task TryAbandonAsync_RepeatedAbandons_IncrementsDeliveryCountEachAttempt()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", [1]);

        // Act + Assert (loop drives the act + assertion together for each attempt)
        for (var attempt = 0; attempt < 4; attempt++)
        {
            var locked = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));
            locked.ShouldNotBeNull();
            locked.Message.DeliveryCount.ShouldBe(attempt, $"attempt {attempt}");
            (await store.TryAbandonAsync("q", locked.LockToken)).ShouldBeTrue();
        }
    }

    [Fact]
    public async Task ExpireLocks_LockTimedOut_IncrementsDeliveryCountOnNextDelivery()
    {
        // Arrange
        var fakeTime = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var store = new InMemoryMessageStore(fakeTime);
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", [1]);
        var first = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(5));
        first.ShouldNotBeNull();
        first.Message.DeliveryCount.ShouldBe(0);
        fakeTime.Advance(TimeSpan.FromSeconds(30));

        // Act
        store.ExpireLocks("q", fakeTime.GetUtcNow()).ShouldBe(1);
        var second = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(5));

        // Assert
        second.ShouldNotBeNull();
        second.Message.DeliveryCount.ShouldBe(1, "redelivery after lock expiry must bump delivery-count");
    }

    [Fact]
    public async Task TryDequeueAsync_DistinctMessages_DoNotShareDeliveryCount()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", [1]);
        await store.EnqueueAsync("q", [2]);
        var first = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));
        first.ShouldNotBeNull();
        await store.TryCompleteAsync("q", first.LockToken);

        // Act
        var second = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));

        // Assert
        second.ShouldNotBeNull();
        second.Message.DeliveryCount.ShouldBe(0, "an independent message must start at delivery-count 0");
    }
}
