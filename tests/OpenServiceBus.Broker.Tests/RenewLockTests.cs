using Microsoft.Extensions.Time.Testing;
using OpenServiceBus.Broker;

namespace OpenServiceBus.Broker.Tests;

public class RenewLockTests
{
    [Fact]
    public async Task TryRenewLock_extends_an_existing_lock_to_now_plus_duration()
    {
        var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var store = new InMemoryMessageStore(time);
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", [1]);

        var locked = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));
        locked.ShouldNotBeNull();
        var originalUntil = locked.LockedUntil;

        time.Advance(TimeSpan.FromSeconds(10));

        var newUntil = await store.TryRenewLockAsync("q", locked.LockToken, TimeSpan.FromSeconds(60));
        newUntil.ShouldNotBeNull();
        newUntil!.Value.ShouldBeGreaterThan(originalUntil, "renew must push the deadline beyond the original");
        newUntil.Value.ShouldBe(time.GetUtcNow() + TimeSpan.FromSeconds(60));
    }

    [Fact]
    public async Task TryRenewLock_returns_null_for_an_unknown_lock_token()
    {
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        (await store.TryRenewLockAsync("q", Guid.NewGuid(), TimeSpan.FromSeconds(30))).ShouldBeNull();
    }

    [Fact]
    public async Task TryRemoveLocked_releases_lock_and_returns_the_stored_message()
    {
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", [1, 2, 3]);

        var locked = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));
        locked.ShouldNotBeNull();

        var removed = await store.TryRemoveLockedAsync("q", locked.LockToken);
        removed.ShouldNotBeNull();
        removed!.SequenceNumber.ShouldBe(locked.Message.SequenceNumber);
        removed.EncodedMessage.ShouldBe(new byte[] { 1, 2, 3 });

        (await store.CountAsync("q")).ShouldBe(0L, "the message must be gone from the queue");
        (await store.TryRemoveLockedAsync("q", locked.LockToken)).ShouldBeNull("the lock token is now invalid");
    }

    [Fact]
    public async Task Expired_lock_can_no_longer_be_renewed()
    {
        var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var store = new InMemoryMessageStore(time);
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", [1]);

        var locked = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(5));
        locked.ShouldNotBeNull();

        time.Advance(TimeSpan.FromSeconds(30));
        store.ExpireLocks("q", time.GetUtcNow()).ShouldBe(1);

        (await store.TryRenewLockAsync("q", locked.LockToken, TimeSpan.FromSeconds(60))).ShouldBeNull();
    }
}
