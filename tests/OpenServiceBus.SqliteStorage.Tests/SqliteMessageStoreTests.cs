using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using OpenServiceBus.Core.Messaging;
using OpenServiceBus.SqliteStorage;

namespace OpenServiceBus.SqliteStorage.Tests;

/// <summary>
/// Parity smoke-tests for <see cref="SqliteMessageStore"/>. The bigger SDK proof-of-life
/// runs in a separate file via <c>OpenServiceBusTestHost</c>. These check that every method
/// we ship has the same observable contract as the in-memory store on the path most likely
/// to break first (locking, deferred state, dedup, sessions).
/// </summary>
public class SqliteMessageStoreTests
{
    private static SqliteMessageStore NewStore(TimeProvider? tp = null) =>
        new(new SqliteStorageOptions { DataSource = ":memory:" },
            tp ?? TimeProvider.System,
            NullLogger<SqliteMessageStore>.Instance);

    [Fact]
    public async Task EnqueueAndDequeue_RoundTripsBytesAndAssignsSequenceNumber()
    {
        // Arrange
        await using var store = NewStore();
        await store.CreateQueueAsync("q");

        // Act
        var stored = await store.EnqueueAsync("q", new byte[] { 0xCA, 0xFE });
        var locked = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));

        // Assert
        stored.SequenceNumber.ShouldBe(1L);
        locked.ShouldNotBeNull();
        locked.Message.SequenceNumber.ShouldBe(1L);
        locked.Message.EncodedMessage.ShouldBe(new byte[] { 0xCA, 0xFE });
        locked.LockToken.ShouldNotBe(Guid.Empty);
    }

    [Fact]
    public async Task TryDequeue_TwoMessages_PreservesFifoOrder()
    {
        await using var store = NewStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", new byte[] { 1 });
        await store.EnqueueAsync("q", new byte[] { 2 });

        var first = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));
        var second = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));

        first!.Message.SequenceNumber.ShouldBe(1L);
        second!.Message.SequenceNumber.ShouldBe(2L);
    }

    [Fact]
    public async Task TryComplete_RemovesMessage_CountGoesToZero()
    {
        await using var store = NewStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", new byte[] { 1 });
        var locked = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));

        var ok = await store.TryCompleteAsync("q", locked!.LockToken);

        ok.ShouldBeTrue();
        (await store.CountAsync("q")).ShouldBe(0L);
    }

    [Fact]
    public async Task TryComplete_TwiceOnSameToken_SecondReturnsFalse()
    {
        await using var store = NewStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", new byte[] { 1 });
        var locked = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));
        await store.TryCompleteAsync("q", locked!.LockToken);

        (await store.TryCompleteAsync("q", locked.LockToken)).ShouldBeFalse();
    }

    [Fact]
    public async Task TryAbandon_BumpsDeliveryCountAndReturnsMessageToAvailable()
    {
        await using var store = NewStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", new byte[] { 1 });
        var locked = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));

        await store.TryAbandonAsync("q", locked!.LockToken);
        var redelivered = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));

        redelivered.ShouldNotBeNull();
        redelivered.Message.DeliveryCount.ShouldBe(1);
    }

    [Fact]
    public async Task ExpireLocks_LockPastDeadline_ReleasesAndIncrementsDeliveryCount()
    {
        var fake = new FakeTimeProvider();
        await using var store = NewStore(fake);
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", new byte[] { 1 });

        var locked = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));
        fake.Advance(TimeSpan.FromMinutes(1));

        var released = store.ExpireLocks("q", fake.GetUtcNow());
        released.ShouldBe(1);

        var redelivered = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));
        redelivered.ShouldNotBeNull();
        redelivered.Message.DeliveryCount.ShouldBe(1);
    }

    [Fact]
    public async Task TryRenewLock_LinkAffinityMismatch_ReturnsNullAndDoesNotExtend()
    {
        await using var store = NewStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", new byte[] { 1 });
        var locked = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30), associatedLinkName: "linkA");

        var renewed = await store.TryRenewLockAsync("q", locked!.LockToken, TimeSpan.FromMinutes(5), requestingLinkName: "linkB");

        renewed.ShouldBeNull();
    }

    [Fact]
    public async Task Defer_ThenReceiveBySeq_ReturnsMessageWithIsDeferredFalseAndLocked()
    {
        await using var store = NewStore();
        await store.CreateQueueAsync("q");
        var enq = await store.EnqueueAsync("q", new byte[] { 1 });
        var locked = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));

        await store.TryDeferAsync("q", locked!.LockToken);
        // Deferred messages are invisible to TryDequeueAsync.
        var nothing = await store.TryDequeueAsync("q", TimeSpan.FromMilliseconds(100), cancellationToken: new CancellationTokenSource(200).Token);
        nothing.ShouldBeNull();

        var revived = await store.TryReceiveDeferredAsync("q", enq.SequenceNumber, TimeSpan.FromSeconds(30));
        revived.ShouldNotBeNull();
        revived.Message.IsDeferred.ShouldBeFalse();
    }

    [Fact]
    public async Task DeferredThenAbandon_ReturnsToDeferredNotActive()
    {
        await using var store = NewStore();
        await store.CreateQueueAsync("q");
        var enq = await store.EnqueueAsync("q", new byte[] { 1 });
        var locked = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));
        await store.TryDeferAsync("q", locked!.LockToken);
        var revived = await store.TryReceiveDeferredAsync("q", enq.SequenceNumber, TimeSpan.FromSeconds(30));

        await store.TryAbandonAsync("q", revived!.LockToken);

        // Still deferred → invisible to dequeue.
        var nothing = await store.TryDequeueAsync("q", TimeSpan.FromMilliseconds(100), cancellationToken: new CancellationTokenSource(200).Token);
        nothing.ShouldBeNull();
        // Still retrievable by sequence number.
        var receivedAgain = await store.TryReceiveDeferredAsync("q", enq.SequenceNumber, TimeSpan.FromSeconds(30));
        receivedAgain.ShouldNotBeNull();
    }

    [Fact]
    public async Task Schedule_MessageNotVisibleUntilActivated()
    {
        var fake = new FakeTimeProvider();
        await using var store = NewStore(fake);
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", new byte[] { 1 }, scheduledEnqueueTime: fake.GetUtcNow() + TimeSpan.FromMinutes(5));

        var early = await store.TryDequeueAsync("q", TimeSpan.FromMilliseconds(50), cancellationToken: new CancellationTokenSource(200).Token);
        early.ShouldBeNull("scheduled-for-future messages are invisible to dequeue");

        fake.Advance(TimeSpan.FromMinutes(6));
        store.ActivateScheduled("q", fake.GetUtcNow()).ShouldBe(1);

        var visible = await store.TryDequeueAsync("q", TimeSpan.FromSeconds(30));
        visible.ShouldNotBeNull();
    }

    [Fact]
    public async Task TryCancelScheduled_BeforeActivation_RemovesMessage()
    {
        var fake = new FakeTimeProvider();
        await using var store = NewStore(fake);
        await store.CreateQueueAsync("q");
        var enq = await store.EnqueueAsync("q", new byte[] { 1 }, scheduledEnqueueTime: fake.GetUtcNow() + TimeSpan.FromMinutes(5));

        var canceled = await store.TryCancelScheduledAsync("q", enq.SequenceNumber);

        canceled.ShouldBeTrue();
        (await store.CountAsync("q")).ShouldBe(0L);
    }

    [Fact]
    public async Task DuplicateDetection_SecondSendSameMessageIdWithinWindow_ReturnsOriginal()
    {
        await using var store = NewStore();
        await store.CreateQueueAsync("q");
        var first = await store.EnqueueAsync("q", new byte[] { 1 },
            messageId: "same", duplicateDetectionWindow: TimeSpan.FromMinutes(5));
        var second = await store.EnqueueAsync("q", new byte[] { 2 },
            messageId: "same", duplicateDetectionWindow: TimeSpan.FromMinutes(5));

        // The second send returns the *original* StoredMessage and doesn't insert a new row.
        second.SequenceNumber.ShouldBe(first.SequenceNumber);
        (await store.CountAsync("q")).ShouldBe(1L);
    }

    [Fact]
    public async Task DuplicateDetection_AfterWindowExpires_NextSendIsTreatedAsNew()
    {
        var fake = new FakeTimeProvider();
        await using var store = NewStore(fake);
        await store.CreateQueueAsync("q");
        var first = await store.EnqueueAsync("q", new byte[] { 1 },
            messageId: "same", duplicateDetectionWindow: TimeSpan.FromMinutes(5));

        fake.Advance(TimeSpan.FromMinutes(6));
        var second = await store.EnqueueAsync("q", new byte[] { 2 },
            messageId: "same", duplicateDetectionWindow: TimeSpan.FromMinutes(5));

        second.SequenceNumber.ShouldBeGreaterThan(first.SequenceNumber);
        (await store.CountAsync("q")).ShouldBe(2L);
    }

    [Fact]
    public async Task TtlExpire_UnlockedMessagesPastDeadline_AreRemovedAndReturned()
    {
        var fake = new FakeTimeProvider();
        await using var store = NewStore(fake);
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", new byte[] { 1 }, expiresAt: fake.GetUtcNow() + TimeSpan.FromMinutes(1));
        await store.EnqueueAsync("q", new byte[] { 2 });

        fake.Advance(TimeSpan.FromMinutes(2));
        var expired = store.ExpireMessages("q", fake.GetUtcNow());

        expired.Count.ShouldBe(1);
        expired[0].SequenceNumber.ShouldBe(1L);
        (await store.CountAsync("q")).ShouldBe(1L, "the un-expired message is still there");
    }

    [Fact]
    public async Task Sessions_AcceptThenDequeue_OnlyVisibleToLockHolder()
    {
        await using var store = NewStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", new byte[] { 1 }, sessionId: "A");
        await store.EnqueueAsync("q", new byte[] { 2 }, sessionId: "B");

        var lockA = await store.TryAcceptSessionAsync("q", "A", TimeSpan.FromMinutes(1));
        lockA.ShouldNotBeNull();

        var msgA = await store.TryDequeueFromSessionAsync("q", "A", TimeSpan.FromSeconds(30));
        msgA.ShouldNotBeNull();
        msgA.Message.SessionId.ShouldBe("A");
        msgA.Message.EncodedMessage[0].ShouldBe((byte)1);
    }

    [Fact]
    public async Task Sessions_TryAcceptAlreadyLockedSession_ReturnsNull()
    {
        await using var store = NewStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", new byte[] { 1 }, sessionId: "A");
        var first = await store.TryAcceptSessionAsync("q", "A", TimeSpan.FromMinutes(1));
        first.ShouldNotBeNull();

        var second = await store.TryAcceptSessionAsync("q", "A", TimeSpan.FromMinutes(1));

        second.ShouldBeNull();
    }

    [Fact]
    public async Task Sessions_TryAcceptNext_PicksTheSessionWithLowestSequence()
    {
        await using var store = NewStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", new byte[] { 1 }, sessionId: "B");
        await store.EnqueueAsync("q", new byte[] { 2 }, sessionId: "A"); // arrived second but lower seq for A
        await store.EnqueueAsync("q", new byte[] { 3 }, sessionId: "B");

        var first = await store.TryAcceptNextSessionAsync("q", TimeSpan.FromMinutes(1));

        first.ShouldNotBeNull();
        first.SessionId.ShouldBe("B", "B's first message arrived before A's");
    }

    [Fact]
    public async Task Peek_FromSequenceNumber_ReturnsOrderedSubset()
    {
        await using var store = NewStore();
        await store.CreateQueueAsync("q");
        await store.EnqueueAsync("q", new byte[] { 1 });
        await store.EnqueueAsync("q", new byte[] { 2 });
        await store.EnqueueAsync("q", new byte[] { 3 });

        var peeked = store.Peek("q", fromSequenceNumber: 2, maxCount: 10);

        peeked.Count.ShouldBe(2);
        peeked[0].SequenceNumber.ShouldBe(2L);
        peeked[1].SequenceNumber.ShouldBe(3L);
    }

    [Fact]
    public async Task PersistAcrossInstance_SameFile_MessagesSurviveRestart()
    {
        // Use a real file (not :memory:) to prove on-disk durability.
        var path = Path.Combine(Path.GetTempPath(), $"osb-test-{Guid.NewGuid():N}.db");
        try
        {
            await using (var store1 = new SqliteMessageStore(
                new SqliteStorageOptions { DataSource = path },
                TimeProvider.System,
                NullLogger<SqliteMessageStore>.Instance))
            {
                await store1.CreateQueueAsync("durable");
                await store1.EnqueueAsync("durable", new byte[] { 0xAA });
            }

            await using var store2 = new SqliteMessageStore(
                new SqliteStorageOptions { DataSource = path },
                TimeProvider.System,
                NullLogger<SqliteMessageStore>.Instance);

            (await store2.CountAsync("durable")).ShouldBe(1L);
            var locked = await store2.TryDequeueAsync("durable", TimeSpan.FromSeconds(30));
            locked.ShouldNotBeNull();
            locked.Message.EncodedMessage[0].ShouldBe((byte)0xAA);
        }
        finally
        {
            try { File.Delete(path); File.Delete(path + "-wal"); File.Delete(path + "-shm"); } catch { /* ignore */ }
        }
    }
}
