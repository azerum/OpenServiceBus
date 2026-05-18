using Microsoft.Extensions.Time.Testing;

namespace OpenServiceBus.InMemoryStorage.Tests;

public class ScheduledMessagesTests
{
    [Fact]
    public async Task TryDequeueAsync_MessageScheduledInFuture_ReturnsNullUntilScheduledTime()
    {
        // Arrange
        var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var store = new InMemoryMessageStore(time);
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", [1], expiresAt: null, scheduledEnqueueTime: time.GetUtcNow() + TimeSpan.FromMinutes(5));
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));

        // Act
        var locked = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30), cancellationToken: cts.Token);

        // Assert
        locked.ShouldBeNull("scheduled message must not be delivered before its time");
        (await store.CountAsync("q")).ShouldBe(1L, "count still reflects the scheduled message in storage");
    }

    [Fact]
    public async Task ActivateScheduled_TimeAdvancedPastSomeButNotAll_PromotesOnlyMessagesPastTheirTime()
    {
        // Arrange
        var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var store = new InMemoryMessageStore(time);
        await store.CreateQueueAsync("q");
        var earlySched = time.GetUtcNow() + TimeSpan.FromSeconds(10);
        var lateSched = time.GetUtcNow() + TimeSpan.FromMinutes(5);
        await store.EnqueueAsync("q", [1], scheduledEnqueueTime: earlySched);
        await store.EnqueueAsync("q", [2], scheduledEnqueueTime: lateSched);
        time.Advance(TimeSpan.FromSeconds(30));

        // Act
        var activated = store.ActivateScheduled("q", time.GetUtcNow());
        var locked = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));

        // Assert
        activated.ShouldBe(1, "only the early-scheduled message has reached its time");
        locked.ShouldNotBeNull();
        locked.Message.EncodedMessage.ShouldBe(new byte[] { 1 });
    }

    [Fact]
    public async Task ActivateScheduled_CalledTwice_SecondCallReturnsZero()
    {
        // Arrange
        var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var store = new InMemoryMessageStore(time);
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", [1], scheduledEnqueueTime: time.GetUtcNow() + TimeSpan.FromSeconds(5));
        time.Advance(TimeSpan.FromSeconds(10));

        // Act
        var firstSweep = store.ActivateScheduled("q", time.GetUtcNow());
        var secondSweep = store.ActivateScheduled("q", time.GetUtcNow());

        // Assert
        firstSweep.ShouldBe(1);
        secondSweep.ShouldBe(0, "second sweep finds nothing to do");
    }

    [Fact]
    public async Task TryCancelScheduledAsync_BeforeActivation_RemovesMessageFromStorage()
    {
        // Arrange
        var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var store = new InMemoryMessageStore(time);
        await store.CreateQueueAsync("q");
        var stored = await store.EnqueueAsync("q", [1], scheduledEnqueueTime: time.GetUtcNow() + TimeSpan.FromMinutes(5));
        (await store.CountAsync("q")).ShouldBe(1L);

        // Act
        var cancelled = await store.TryCancelScheduledAsync("q", stored.SequenceNumber);

        // Assert
        cancelled.ShouldBeTrue();
        (await store.CountAsync("q")).ShouldBe(0L);
    }

    [Fact]
    public async Task TryCancelScheduledAsync_AfterActivation_ReturnsFalse()
    {
        // Arrange
        var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var store = new InMemoryMessageStore(time);
        await store.CreateQueueAsync("q");
        var stored = await store.EnqueueAsync("q", [1], scheduledEnqueueTime: time.GetUtcNow() + TimeSpan.FromSeconds(5));
        time.Advance(TimeSpan.FromSeconds(10));
        store.ActivateScheduled("q", time.GetUtcNow()).ShouldBe(1);

        // Act
        var cancelled = await store.TryCancelScheduledAsync("q", stored.SequenceNumber);

        // Assert
        cancelled.ShouldBeFalse("activated messages must be settled via normal disposition");
    }

    [Fact]
    public async Task TryCancelScheduledAsync_UnknownSequenceNumber_ReturnsFalse()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");

        // Act
        var cancelled = await store.TryCancelScheduledAsync("q", 999L);

        // Assert
        cancelled.ShouldBeFalse();
    }

    [Fact]
    public async Task EnqueueAsync_ScheduledTimeInPast_TreatsMessageAsImmediatelyAvailable()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", [1], scheduledEnqueueTime: DateTimeOffset.UtcNow - TimeSpan.FromMinutes(5));

        // Act
        var locked = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));

        // Assert
        locked.ShouldNotBeNull("past-dated schedule must be treated as immediate");
    }
}
