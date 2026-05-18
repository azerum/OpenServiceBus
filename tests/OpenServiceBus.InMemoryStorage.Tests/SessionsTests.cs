using Microsoft.Extensions.Time.Testing;

namespace OpenServiceBus.InMemoryStorage.Tests;

public class SessionsTests
{
    [Fact]
    public async Task EnqueueAsync_WithSessionId_DoesNotMakeMessageVisibleToNonSessionDequeue()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", [1], sessionId: "s1");
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));

        // Act
        var locked = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30), cancellationToken: cts.Token);

        // Assert
        locked.ShouldBeNull("a session message must not surface through the regular dequeue path");
        (await store.CountAsync("q")).ShouldBe(1L);
    }

    [Fact]
    public async Task TryAcceptSessionAsync_ThenDequeueFromSession_DeliversTheSessionMessage()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", [42], sessionId: "s1");

        // Act
        var sessionLock = await store.TryAcceptSessionAsync("q", "s1", TimeSpan.FromSeconds(30));
        var locked = await store.TryDequeueFromSessionAsync("q", "s1", TimeSpan.FromSeconds(30));

        // Assert
        sessionLock.ShouldNotBeNull();
        sessionLock.SessionId.ShouldBe("s1");
        locked.ShouldNotBeNull();
        locked.Message.EncodedMessage.ShouldBe(new byte[] { 42 });
        locked.Message.SessionId.ShouldBe("s1");
    }

    [Fact]
    public async Task TryAcceptSessionAsync_WhenAlreadyLockedByAnotherReceiver_ReturnsNull()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", [1], sessionId: "s1");

        // Act
        var first = await store.TryAcceptSessionAsync("q", "s1", TimeSpan.FromSeconds(30), linkName: "r1");
        var second = await store.TryAcceptSessionAsync("q", "s1", TimeSpan.FromSeconds(30), linkName: "r2");

        // Assert
        first.ShouldNotBeNull();
        second.ShouldBeNull("only one receiver may hold the session lock at a time");
    }

    [Fact]
    public async Task TryAcceptNextSessionAsync_ReturnsSessionWithMessages_AndSkipsLockedOnes()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", [1], sessionId: "s1");
        await store.EnqueueAsync("q", [2], sessionId: "s2");
        await store.TryAcceptSessionAsync("q", "s1", TimeSpan.FromSeconds(30), linkName: "first-holder");

        // Act
        var next = await store.TryAcceptNextSessionAsync("q", TimeSpan.FromSeconds(30), linkName: "second-receiver");

        // Assert
        next.ShouldNotBeNull();
        next.SessionId.ShouldBe("s2", "s1 is already locked so the broker hands out s2");
    }

    [Fact]
    public async Task TryDequeueFromSessionAsync_RepeatedReceives_DeliversInEnqueueOrderWithinSession()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", [1], sessionId: "s");
        await store.EnqueueAsync("q", [2], sessionId: "s");
        await store.EnqueueAsync("q", [3], sessionId: "s");
        await store.TryAcceptSessionAsync("q", "s", TimeSpan.FromSeconds(30));

        // Act
        var bodies = new List<byte>();
        for (var i = 0; i < 3; i++)
        {
            var l = await store.TryDequeueFromSessionAsync("q", "s", TimeSpan.FromSeconds(30));
            bodies.Add(l!.Message.EncodedMessage[0]);
            await store.TryCompleteAsync("q", l.LockToken);
        }

        // Assert
        bodies.ShouldBe(new byte[] { 1, 2, 3 });
    }

    [Fact]
    public async Task SetSessionStateAsync_ThenGet_RoundTripsStateBlob()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", [1], sessionId: "s");

        // Act
        await store.SetSessionStateAsync("q", "s", new byte[] { 7, 7, 7 });
        var read = await store.GetSessionStateAsync("q", "s");

        // Assert
        read.ShouldBe(new byte[] { 7, 7, 7 });
    }

    [Fact]
    public async Task SetSessionStateAsync_Null_ClearsExistingState()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        await store.SetSessionStateAsync("q", "s", new byte[] { 1 });

        // Act
        await store.SetSessionStateAsync("q", "s", null);

        // Assert
        (await store.GetSessionStateAsync("q", "s")).ShouldBeNull();
    }

    [Fact]
    public async Task TryRenewSessionLockAsync_AfterAccept_ExtendsLockedUntilDeadline()
    {
        // Arrange
        var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var store = new InMemoryMessageStore(time);
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", [1], sessionId: "s");
        var lock1 = await store.TryAcceptSessionAsync("q", "s", TimeSpan.FromSeconds(30));
        time.Advance(TimeSpan.FromSeconds(10));

        // Act
        var newUntil = await store.TryRenewSessionLockAsync("q", "s", TimeSpan.FromSeconds(60));

        // Assert
        newUntil.ShouldNotBeNull();
        newUntil!.Value.ShouldBe(time.GetUtcNow() + TimeSpan.FromSeconds(60));
        newUntil.Value.ShouldBeGreaterThan(lock1!.LockedUntil);
    }

    [Fact]
    public async Task TryRenewSessionLockAsync_FromDifferentLink_ReturnsNull()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", [1], sessionId: "s");
        await store.TryAcceptSessionAsync("q", "s", TimeSpan.FromSeconds(30), linkName: "r1");

        // Act
        var renewed = await store.TryRenewSessionLockAsync("q", "s", TimeSpan.FromSeconds(60), requestingLinkName: "r2");

        // Assert
        renewed.ShouldBeNull("session lock affinity matches message-lock affinity - cross-link renew is refused");
    }

    [Fact]
    public async Task ReleaseSessionAsync_AfterAccept_PermitsAnotherReceiverToTakeIt()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", [1], sessionId: "s");
        await store.TryAcceptSessionAsync("q", "s", TimeSpan.FromSeconds(30), linkName: "r1");

        // Act
        await store.ReleaseSessionAsync("q", "s");
        var second = await store.TryAcceptSessionAsync("q", "s", TimeSpan.FromSeconds(30), linkName: "r2");

        // Assert
        second.ShouldNotBeNull();
    }

    [Fact]
    public async Task TryAbandonAsync_OnSessionLockedMessage_ReturnsItToTheSameSessionForRedelivery()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", [1], sessionId: "s");
        await store.TryAcceptSessionAsync("q", "s", TimeSpan.FromSeconds(30));
        var first = await store.TryDequeueFromSessionAsync("q", "s", TimeSpan.FromSeconds(30));

        // Act
        await store.TryAbandonAsync("q", first!.LockToken);
        var second = await store.TryDequeueFromSessionAsync("q", "s", TimeSpan.FromSeconds(30));

        // Assert
        second.ShouldNotBeNull();
        second.Message.SessionId.ShouldBe("s");
        second.Message.DeliveryCount.ShouldBe(1, "abandon bumped delivery-count");
    }

    [Fact]
    public async Task ListSessions_ReturnsSessionsWithMessagesOrState()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", [1], sessionId: "with-messages");
        await store.SetSessionStateAsync("q", "with-state-only", new byte[] { 9 });

        // Act
        var ids = store.ListSessions("q");

        // Assert
        ids.OrderBy(s => s).ShouldBe(new[] { "with-messages", "with-state-only" });
    }
}
