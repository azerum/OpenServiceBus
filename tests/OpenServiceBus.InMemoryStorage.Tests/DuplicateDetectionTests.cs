using Microsoft.Extensions.Time.Testing;

namespace OpenServiceBus.InMemoryStorage.Tests;

public class DuplicateDetectionTests
{
    [Fact]
    public async Task EnqueueAsync_SecondSendWithSameMessageIdInWindow_IsSilentlyDroppedAndReturnsOriginal()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        var window = TimeSpan.FromMinutes(5);

        // Act
        var first = await store.EnqueueAsync("q", [1], messageId: "id-1", duplicateDetectionWindow: window);
        var second = await store.EnqueueAsync("q", [9, 9, 9], messageId: "id-1", duplicateDetectionWindow: window);

        // Assert
        second.SequenceNumber.ShouldBe(first.SequenceNumber, "the duplicate should return the original");
        (await store.CountAsync("q")).ShouldBe(1L, "the duplicate must not be stored");
    }

    [Fact]
    public async Task EnqueueAsync_SameMessageIdAfterWindowExpires_IsTreatedAsNewMessage()
    {
        // Arrange
        var time = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var store = new InMemoryMessageStore(time);
        await store.CreateQueueAsync("q");
        var window = TimeSpan.FromMinutes(5);
        var first = await store.EnqueueAsync("q", [1], messageId: "id-1", duplicateDetectionWindow: window);
        time.Advance(TimeSpan.FromMinutes(6));

        // Act
        var second = await store.EnqueueAsync("q", [2], messageId: "id-1", duplicateDetectionWindow: window);

        // Assert
        second.SequenceNumber.ShouldNotBe(first.SequenceNumber, "after the window expires the same id is a fresh enqueue");
        (await store.CountAsync("q")).ShouldBe(2L);
    }

    [Fact]
    public async Task EnqueueAsync_DifferentMessageIds_AreAllStoredEvenWithDedupOn()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");

        // Act
        await store.EnqueueAsync("q", [1], messageId: "id-1", duplicateDetectionWindow: TimeSpan.FromMinutes(5));
        await store.EnqueueAsync("q", [2], messageId: "id-2", duplicateDetectionWindow: TimeSpan.FromMinutes(5));
        await store.EnqueueAsync("q", [3], messageId: "id-3", duplicateDetectionWindow: TimeSpan.FromMinutes(5));

        // Assert
        (await store.CountAsync("q")).ShouldBe(3L);
    }

    [Fact]
    public async Task EnqueueAsync_NoDedupWindowConfigured_AlwaysEnqueuesEvenForSameMessageId()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");

        // Act
        await store.EnqueueAsync("q", [1], messageId: "id-1");
        await store.EnqueueAsync("q", [2], messageId: "id-1");

        // Assert
        (await store.CountAsync("q")).ShouldBe(2L, "dedup is opt-in; without a window the same id is fine");
    }

    [Fact]
    public async Task EnqueueAsync_EmptyMessageId_DedupCheckIsSkipped()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");
        var window = TimeSpan.FromMinutes(5);

        // Act - both sends have a null/empty messageId; can't dedup without an id.
        await store.EnqueueAsync("q", [1], messageId: null, duplicateDetectionWindow: window);
        await store.EnqueueAsync("q", [2], messageId: "", duplicateDetectionWindow: window);

        // Assert
        (await store.CountAsync("q")).ShouldBe(2L);
    }
}
