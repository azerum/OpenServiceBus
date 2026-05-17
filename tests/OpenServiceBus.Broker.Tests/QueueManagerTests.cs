using OpenServiceBus.Abstractions;
using OpenServiceBus.Broker;

namespace OpenServiceBus.Broker.Tests;

public class QueueManagerTests
{
    [Fact]
    public async Task Create_then_Get_returns_the_descriptor_and_raises_QueueCreated()
    {
        var store = new InMemoryMessageStore();
        var manager = new QueueManager(store);

        var raised = new List<string>();
        manager.QueueCreated += (_, q) => raised.Add(q.Name);

        var created = await manager.CreateAsync(new QueueDescriptor { Name = "orders", MaxDeliveryCount = 5 });
        var fetched = await manager.GetAsync("orders");

        created.MaxDeliveryCount.ShouldBe(5);
        fetched.ShouldBe(created);
        raised.ShouldContain("orders");
        raised.ShouldContain("orders/$DeadLetterQueue", "creating a main queue also raises QueueCreated for its DLQ sibling");
    }

    [Fact]
    public async Task Create_is_idempotent_for_same_name()
    {
        var manager = new QueueManager(new InMemoryMessageStore());
        var a = await manager.CreateAsync(new QueueDescriptor { Name = "dup", MaxDeliveryCount = 3 });
        var b = await manager.CreateAsync(new QueueDescriptor { Name = "dup", MaxDeliveryCount = 99 });
        b.MaxDeliveryCount.ShouldBe(3, "second create should not overwrite the first");
        a.ShouldBe(b);
    }

    [Fact]
    public async Task Delete_returns_true_then_false_and_raises_QueueDeleted()
    {
        var manager = new QueueManager(new InMemoryMessageStore());
        await manager.CreateAsync(new QueueDescriptor { Name = "gone" });

        var raised = new List<string>();
        manager.QueueDeleted += (_, q) => raised.Add(q.Name);

        (await manager.DeleteAsync("gone")).ShouldBeTrue();
        (await manager.DeleteAsync("gone")).ShouldBeFalse();
        raised.ShouldContain("gone");
        raised.ShouldContain("gone/$DeadLetterQueue", "deleting a main queue cascades to its DLQ sibling");
    }

    [Fact]
    public async Task List_returns_main_queues_and_their_DLQ_siblings()
    {
        var manager = new QueueManager(new InMemoryMessageStore());
        await manager.CreateAsync(new QueueDescriptor { Name = "a" });
        await manager.CreateAsync(new QueueDescriptor { Name = "b" });

        var all = await manager.ListAsync();
        all.Select(q => q.Name).OrderBy(n => n).ShouldBe(new[]
        {
            "a", "a/$DeadLetterQueue",
            "b", "b/$DeadLetterQueue",
        });
    }

    [Fact]
    public async Task Creating_a_main_queue_implicitly_creates_its_DLQ_sibling()
    {
        var manager = new QueueManager(new InMemoryMessageStore());
        await manager.CreateAsync(new QueueDescriptor { Name = "orders", MaxDeliveryCount = 3 });

        var dlq = await manager.GetAsync("orders/$DeadLetterQueue");
        dlq.ShouldNotBeNull();
        dlq!.MaxDeliveryCount.ShouldBe(int.MaxValue, "the DLQ should not itself dead-letter further");
    }

    [Fact]
    public async Task Creating_a_DLQ_directly_does_not_create_a_DLQ_for_the_DLQ()
    {
        var manager = new QueueManager(new InMemoryMessageStore());
        await manager.CreateAsync(new QueueDescriptor { Name = "raw/$DeadLetterQueue" });

        (await manager.GetAsync("raw/$DeadLetterQueue/$DeadLetterQueue")).ShouldBeNull();
    }

    [Fact]
    public async Task Deleting_a_main_queue_also_deletes_its_DLQ()
    {
        var manager = new QueueManager(new InMemoryMessageStore());
        await manager.CreateAsync(new QueueDescriptor { Name = "ephemeral" });
        (await manager.GetAsync("ephemeral/$DeadLetterQueue")).ShouldNotBeNull();

        (await manager.DeleteAsync("ephemeral")).ShouldBeTrue();
        (await manager.GetAsync("ephemeral/$DeadLetterQueue")).ShouldBeNull();
    }
}
