using Microsoft.Extensions.Time.Testing;
using OpenServiceBus.InMemoryStorage.DependencyInjection;
using OpenServiceBus.InMemoryStorage.Lifecycle;
using OpenServiceBus.InMemoryStorage.Queues;

namespace OpenServiceBus.InMemoryStorage.Tests;

public class PeekLockTests
{
    [Fact]
    public async Task TryDequeue_returns_message_in_FIFO_order_and_assigns_a_lock_token()
    {
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");

        await store.EnqueueAsync("q", new byte[] { 1 });
        await store.EnqueueAsync("q", new byte[] { 2 });

        var a = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));
        var b = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));

        a.ShouldNotBeNull();
        b.ShouldNotBeNull();
        a.Message.SequenceNumber.ShouldBe(1L);
        b.Message.SequenceNumber.ShouldBe(2L);
        a.LockToken.ShouldNotBe(b.LockToken);
        a.LockToken.ShouldNotBe(Guid.Empty);
    }

    [Fact]
    public async Task TryComplete_removes_the_locked_message_from_storage()
    {
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", new byte[] { 1 });

        var locked = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));
        locked.ShouldNotBeNull();

        (await store.TryCompleteAsync("q", locked.LockToken)).ShouldBeTrue();
        (await store.CountAsync("q")).ShouldBe(0L);

        // Completing the same token twice is a no-op.
        (await store.TryCompleteAsync("q", locked.LockToken)).ShouldBeFalse();
    }

    [Fact]
    public async Task TryAbandon_returns_the_message_to_the_available_pool()
    {
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", new byte[] { 1 });

        var locked1 = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));
        locked1.ShouldNotBeNull();

        (await store.TryAbandonAsync("q", locked1.LockToken)).ShouldBeTrue();
        (await store.CountAsync("q")).ShouldBe(1L);

        var locked2 = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));
        locked2.ShouldNotBeNull();
        locked2.Message.SequenceNumber.ShouldBe(locked1.Message.SequenceNumber);
        locked2.LockToken.ShouldNotBe(locked1.LockToken, "redelivery must use a fresh lock token");
    }

    [Fact]
    public async Task ExpireLocks_releases_locks_whose_deadlines_have_passed()
    {
        var fakeTime = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var store = new InMemoryMessageStore(fakeTime);
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", new byte[] { 1 });
        await store.EnqueueAsync("q", new byte[] { 2 });

        var a = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(10));
        var b = await store.TryDequeueAsync("q", TimeSpan.FromMinutes(5));

        a.ShouldNotBeNull();
        b.ShouldNotBeNull();

        fakeTime.Advance(TimeSpan.FromSeconds(30));

        store.ExpireLocks("q", fakeTime.GetUtcNow()).ShouldBe(1, "only the 10s lock should have expired");

        // The 10s-locked message comes back; the 5min one stays locked.
        var redelivered = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(10));
        redelivered.ShouldNotBeNull();
        redelivered.Message.SequenceNumber.ShouldBe(a.Message.SequenceNumber);

        // b is still locked - completing it works.
        (await store.TryCompleteAsync("q", b.LockToken)).ShouldBeTrue();
    }

    [Fact]
    public async Task ExpireLocks_is_a_no_op_for_a_queue_with_no_locks()
    {
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        store.ExpireLocks("q", DateTimeOffset.UtcNow).ShouldBe(0);
    }
}
