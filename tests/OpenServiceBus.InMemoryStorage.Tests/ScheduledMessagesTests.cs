using Microsoft.Extensions.Time.Testing;
using OpenServiceBus.Core.Messaging;
using OpenServiceBus.Core.Storage;
using OpenServiceBus.InMemoryStorage;

namespace OpenServiceBus.InMemoryStorage.Tests;

public class ScheduledMessagesTests
{
    [Fact]
    public async Task Scheduled_messages_are_not_immediately_dequeueable()
    {
        var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var store = new InMemoryMessageStore(time);
        await store.CreateQueueAsync("q");

        await store.EnqueueAsync("q", [1], expiresAt: null, scheduledEnqueueTime: time.GetUtcNow() + TimeSpan.FromMinutes(5));

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        var locked = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30), cancellationToken: cts.Token);
        locked.ShouldBeNull("scheduled message must not be delivered before its time");

        // Count still reflects the scheduled message — it exists in storage.
        (await store.CountAsync("q")).ShouldBe(1L);
    }

    [Fact]
    public async Task ActivateScheduled_promotes_messages_whose_time_has_arrived()
    {
        var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var store = new InMemoryMessageStore(time);
        await store.CreateQueueAsync("q");

        var earlySched = time.GetUtcNow() + TimeSpan.FromSeconds(10);
        var lateSched = time.GetUtcNow() + TimeSpan.FromMinutes(5);
        await store.EnqueueAsync("q", [1], scheduledEnqueueTime: earlySched);
        await store.EnqueueAsync("q", [2], scheduledEnqueueTime: lateSched);

        time.Advance(TimeSpan.FromSeconds(30));

        store.ActivateScheduled("q", time.GetUtcNow()).ShouldBe(1, "only the early-scheduled message has reached its time");

        var locked = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));
        locked.ShouldNotBeNull();
        locked.Message.EncodedMessage.ShouldBe(new byte[] { 1 });
    }

    [Fact]
    public async Task ActivateScheduled_is_idempotent()
    {
        var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var store = new InMemoryMessageStore(time);
        await store.CreateQueueAsync("q");

        await store.EnqueueAsync("q", [1], scheduledEnqueueTime: time.GetUtcNow() + TimeSpan.FromSeconds(5));
        time.Advance(TimeSpan.FromSeconds(10));

        store.ActivateScheduled("q", time.GetUtcNow()).ShouldBe(1);
        store.ActivateScheduled("q", time.GetUtcNow()).ShouldBe(0, "second sweep finds nothing to do");
    }

    [Fact]
    public async Task TryCancelScheduled_removes_a_message_before_its_time_arrives()
    {
        var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var store = new InMemoryMessageStore(time);
        await store.CreateQueueAsync("q");

        var stored = await store.EnqueueAsync("q", [1], scheduledEnqueueTime: time.GetUtcNow() + TimeSpan.FromMinutes(5));
        (await store.CountAsync("q")).ShouldBe(1L);

        (await store.TryCancelScheduledAsync("q", stored.SequenceNumber)).ShouldBeTrue();
        (await store.CountAsync("q")).ShouldBe(0L);
    }

    [Fact]
    public async Task TryCancelScheduled_returns_false_for_already_activated_messages()
    {
        var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var store = new InMemoryMessageStore(time);
        await store.CreateQueueAsync("q");

        var stored = await store.EnqueueAsync("q", [1], scheduledEnqueueTime: time.GetUtcNow() + TimeSpan.FromSeconds(5));
        time.Advance(TimeSpan.FromSeconds(10));
        store.ActivateScheduled("q", time.GetUtcNow()).ShouldBe(1);

        (await store.TryCancelScheduledAsync("q", stored.SequenceNumber)).ShouldBeFalse("activated messages must be settled via normal disposition");
    }

    [Fact]
    public async Task TryCancelScheduled_returns_false_for_unknown_sequence_numbers()
    {
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        (await store.TryCancelScheduledAsync("q", 999L)).ShouldBeFalse();
    }

    [Fact]
    public async Task Scheduled_time_in_the_past_makes_message_immediately_available()
    {
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", [1], scheduledEnqueueTime: DateTimeOffset.UtcNow - TimeSpan.FromMinutes(5));

        var locked = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));
        locked.ShouldNotBeNull("past-dated 'schedule' must be treated as immediate");
    }
}
