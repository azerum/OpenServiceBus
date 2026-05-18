using OpenServiceBus.InMemoryStorage;

namespace OpenServiceBus.InMemoryStorage.Tests;

public class DeferTests
{
    [Fact]
    public async Task TryDefer_parks_a_locked_message_and_hides_it_from_normal_dequeue()
    {
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", [1]);

        var locked = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));
        locked.ShouldNotBeNull();
        (await store.TryDeferAsync("q", locked.LockToken)).ShouldBeTrue();

        // Normal dequeue must NOT return a deferred message — it should return null on cancel.
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        var none = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30), cancellationToken: cts.Token);
        none.ShouldBeNull();

        // The message is still in storage.
        (await store.CountAsync("q")).ShouldBe(1L);
    }

    [Fact]
    public async Task TryReceiveDeferred_returns_a_deferred_message_under_a_fresh_lock()
    {
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        var enqueued = await store.EnqueueAsync("q", [1, 2, 3]);

        var first = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));
        first.ShouldNotBeNull();
        await store.TryDeferAsync("q", first.LockToken);

        var deferred = await store.TryReceiveDeferredAsync("q", enqueued.SequenceNumber, TimeSpan.FromSeconds(30));
        deferred.ShouldNotBeNull();
        deferred.Message.EncodedMessage.ShouldBe(new byte[] { 1, 2, 3 });
        deferred.LockToken.ShouldNotBe(first.LockToken, "receive-by-seq must issue a fresh lock token");

        (await store.TryCompleteAsync("q", deferred.LockToken)).ShouldBeTrue();
        (await store.CountAsync("q")).ShouldBe(0L);
    }

    [Fact]
    public async Task Abandoning_a_deferred_message_returns_it_to_Deferred_state()
    {
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        var enqueued = await store.EnqueueAsync("q", [1]);

        var locked = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));
        locked.ShouldNotBeNull();
        await store.TryDeferAsync("q", locked.LockToken);

        var retrieved = await store.TryReceiveDeferredAsync("q", enqueued.SequenceNumber, TimeSpan.FromSeconds(30));
        retrieved.ShouldNotBeNull();
        (await store.TryAbandonAsync("q", retrieved.LockToken)).ShouldBeTrue();

        // After abandon, the message goes back to Deferred (NOT Active) — normal dequeue still skips it.
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        var none = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30), cancellationToken: cts.Token);
        none.ShouldBeNull();

        // But receive-by-seq finds it again.
        var again = await store.TryReceiveDeferredAsync("q", enqueued.SequenceNumber, TimeSpan.FromSeconds(30));
        again.ShouldNotBeNull();
    }

    [Fact]
    public async Task TryReceiveDeferred_returns_null_for_a_message_that_is_not_deferred()
    {
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        var enqueued = await store.EnqueueAsync("q", [1]);

        var notDeferred = await store.TryReceiveDeferredAsync("q", enqueued.SequenceNumber, TimeSpan.FromSeconds(30));
        notDeferred.ShouldBeNull();
    }
}
