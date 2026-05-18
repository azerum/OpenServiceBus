namespace OpenServiceBus.InMemoryStorage.Tests;

public class InMemoryMessageStoreTests
{
    [Fact]
    public async Task EnqueueAsync_ThreeMessagesInOrder_AssignsMonotonicSequenceNumbersStartingAtOne()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");

        // Act
        var first = await store.EnqueueAsync("q", new byte[] { 1 });
        var second = await store.EnqueueAsync("q", new byte[] { 2 });
        var third = await store.EnqueueAsync("q", new byte[] { 3 });

        // Assert
        first.SequenceNumber.ShouldBe(1L);
        second.SequenceNumber.ShouldBe(2L);
        third.SequenceNumber.ShouldBe(3L);
        (await store.CountAsync("q")).ShouldBe(3L);
    }

    [Fact]
    public async Task EnqueueAsync_QueueDoesNotExist_ThrowsInvalidOperationException()
    {
        // Arrange
        var store = new InMemoryMessageStore();

        // Act
        var enqueue = () => store.EnqueueAsync("missing", new byte[] { 0 });

        // Assert
        await Should.ThrowAsync<InvalidOperationException>(enqueue);
    }

    [Fact]
    public async Task DeleteQueueAsync_QueueHasMessages_DiscardsAllStoredMessages()
    {
        // Arrange
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("temp");
        await store.EnqueueAsync("temp", new byte[] { 1 });

        // Act
        await store.DeleteQueueAsync("temp");
        await store.CreateQueueAsync("temp");

        // Assert
        (await store.CountAsync("temp")).ShouldBe(0L);
    }
}
