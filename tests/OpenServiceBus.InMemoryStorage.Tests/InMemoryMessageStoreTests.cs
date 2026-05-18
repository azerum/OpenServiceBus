using OpenServiceBus.Core.Entities;
using OpenServiceBus.Core.Messaging;
using OpenServiceBus.Core.Storage;
using OpenServiceBus.InMemoryStorage.DependencyInjection;
using OpenServiceBus.InMemoryStorage.Lifecycle;
using OpenServiceBus.InMemoryStorage.Queues;

namespace OpenServiceBus.InMemoryStorage.Tests;

public class InMemoryMessageStoreTests
{
    [Fact]
    public async Task Enqueue_assigns_monotonic_sequence_numbers_starting_at_1()
    {
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("q");

        var a = await store.EnqueueAsync("q", new byte[] { 1 });
        var b = await store.EnqueueAsync("q", new byte[] { 2 });
        var c = await store.EnqueueAsync("q", new byte[] { 3 });

        a.SequenceNumber.ShouldBe(1L);
        b.SequenceNumber.ShouldBe(2L);
        c.SequenceNumber.ShouldBe(3L);
        (await store.CountAsync("q")).ShouldBe(3L);
    }

    [Fact]
    public async Task Enqueue_to_unknown_queue_throws()
    {
        var store = new InMemoryMessageStore();
        await Should.ThrowAsync<InvalidOperationException>(
            () => store.EnqueueAsync("missing", new byte[] { 0 }));
    }

    [Fact]
    public async Task DeleteQueue_removes_messages()
    {
        var store = new InMemoryMessageStore();
        await store.CreateQueueAsync("temp");
        await store.EnqueueAsync("temp", new byte[] { 1 });
        await store.DeleteQueueAsync("temp");
        await store.CreateQueueAsync("temp");
        (await store.CountAsync("temp")).ShouldBe(0L);
    }
}
