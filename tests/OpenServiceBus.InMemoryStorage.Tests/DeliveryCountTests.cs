using Microsoft.Extensions.Time.Testing;
using OpenServiceBus.InMemoryStorage.DependencyInjection;
using OpenServiceBus.InMemoryStorage.Lifecycle;
using OpenServiceBus.InMemoryStorage.Queues;

namespace OpenServiceBus.InMemoryStorage.Tests;

public class DeliveryCountTests
{
    [Fact]
    public async Task First_delivery_carries_delivery_count_zero()
    {
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", [1]);

        var locked = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));
        locked.ShouldNotBeNull();
        locked.Message.DeliveryCount.ShouldBe(0);
    }

    [Fact]
    public async Task Abandon_increments_delivery_count_for_the_next_delivery()
    {
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", [1]);

        for (var attempt = 0; attempt < 4; attempt++)
        {
            var locked = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));
            locked.ShouldNotBeNull();
            locked.Message.DeliveryCount.ShouldBe(attempt, $"attempt {attempt}");
            (await store.TryAbandonAsync("q", locked.LockToken)).ShouldBeTrue();
        }
    }

    [Fact]
    public async Task Lock_expiration_increments_delivery_count()
    {
        var fakeTime = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var store = new InMemoryMessageStore(fakeTime);
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", [1]);

        var first = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(5));
        first.ShouldNotBeNull();
        first.Message.DeliveryCount.ShouldBe(0);

        fakeTime.Advance(TimeSpan.FromSeconds(30));
        store.ExpireLocks("q", fakeTime.GetUtcNow()).ShouldBe(1);

        var second = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(5));
        second.ShouldNotBeNull();
        second.Message.DeliveryCount.ShouldBe(1, "redelivery after lock expiry must bump delivery-count");
    }

    [Fact]
    public async Task Complete_does_not_carry_delivery_count_forward()
    {
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", [1]);
        await store.EnqueueAsync("q", [2]);

        var first = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));
        first.ShouldNotBeNull();
        (await store.TryCompleteAsync("q", first.LockToken)).ShouldBeTrue();

        var second = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));
        second.ShouldNotBeNull();
        second.Message.DeliveryCount.ShouldBe(0, "an independent message should start at delivery-count 0");
    }
}
