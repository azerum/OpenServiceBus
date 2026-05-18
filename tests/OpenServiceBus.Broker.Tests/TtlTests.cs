using Microsoft.Extensions.Time.Testing;
using OpenServiceBus.Abstractions;
using OpenServiceBus.Broker;

namespace OpenServiceBus.Broker.Tests;

public class TtlTests
{
    [Fact]
    public async Task ExpireMessages_returns_and_removes_only_messages_past_their_deadline()
    {
        var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var store = new InMemoryMessageStore(time);
        await store.CreateQueueAsync("q");

        // Two messages: one expires in 10s, one in 5min.
        var soon = time.GetUtcNow() + TimeSpan.FromSeconds(10);
        var later = time.GetUtcNow() + TimeSpan.FromMinutes(5);
        await store.EnqueueAsync("q", [1], expiresAt: soon);
        await store.EnqueueAsync("q", [2], expiresAt: later);

        time.Advance(TimeSpan.FromSeconds(30));

        var expired = store.ExpireMessages("q", time.GetUtcNow());
        expired.Count.ShouldBe(1);
        expired[0].EncodedMessage.ShouldBe(new byte[] { 1 });

        (await store.CountAsync("q")).ShouldBe(1L, "only the not-yet-expired message remains");
    }

    [Fact]
    public async Task ExpireMessages_never_touches_a_currently_locked_message()
    {
        var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var store = new InMemoryMessageStore(time);
        await store.CreateQueueAsync("q");

        var soon = time.GetUtcNow() + TimeSpan.FromSeconds(10);
        await store.EnqueueAsync("q", [1], expiresAt: soon);

        // Lock it before TTL passes.
        var locked = await store.TryDequeueAsync("q", TimeSpan.FromMinutes(5));
        locked.ShouldNotBeNull();

        // Now advance past the TTL deadline.
        time.Advance(TimeSpan.FromSeconds(30));

        var expired = store.ExpireMessages("q", time.GetUtcNow());
        expired.Count.ShouldBe(0, "locked messages must NOT be expired by the sweeper - the lock holder owns them");
        (await store.CountAsync("q")).ShouldBe(1L);
    }

    [Fact]
    public async Task ExpireMessages_is_a_noop_when_no_TTL_was_set()
    {
        var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var store = new InMemoryMessageStore(time);
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", [1], expiresAt: null);

        time.Advance(TimeSpan.FromHours(24));

        var expired = store.ExpireMessages("q", time.GetUtcNow());
        expired.Count.ShouldBe(0);
        (await store.CountAsync("q")).ShouldBe(1L);
    }

    [Fact]
    public void IsExpired_returns_false_for_messages_with_no_deadline()
    {
        var msg = new StoredMessage
        {
            SequenceNumber = 1,
            EnqueuedAt = DateTimeOffset.UtcNow,
            EncodedMessage = [1],
            ExpiresAt = null,
        };
        msg.IsExpired(DateTimeOffset.UtcNow.AddYears(10)).ShouldBeFalse();
    }

    [Fact]
    public void IsExpired_is_inclusive_of_the_exact_deadline()
    {
        var deadline = DateTimeOffset.UtcNow;
        var msg = new StoredMessage
        {
            SequenceNumber = 1,
            EnqueuedAt = deadline.AddMinutes(-1),
            EncodedMessage = [1],
            ExpiresAt = deadline,
        };
        msg.IsExpired(deadline).ShouldBeTrue();
        msg.IsExpired(deadline.AddSeconds(-1)).ShouldBeFalse();
    }
}
