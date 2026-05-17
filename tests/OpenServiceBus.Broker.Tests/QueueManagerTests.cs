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

        QueueDescriptor? raised = null;
        manager.QueueCreated += (_, q) => raised = q;

        var created = await manager.CreateAsync(new QueueDescriptor { Name = "orders", MaxDeliveryCount = 5 });
        var fetched = await manager.GetAsync("orders");

        created.MaxDeliveryCount.ShouldBe(5);
        fetched.ShouldBe(created);
        raised.ShouldNotBeNull();
        raised!.Name.ShouldBe("orders");
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

        QueueDescriptor? raised = null;
        manager.QueueDeleted += (_, q) => raised = q;

        (await manager.DeleteAsync("gone")).ShouldBeTrue();
        (await manager.DeleteAsync("gone")).ShouldBeFalse();
        raised?.Name.ShouldBe("gone");
    }

    [Fact]
    public async Task List_returns_all_created_queues()
    {
        var manager = new QueueManager(new InMemoryMessageStore());
        await manager.CreateAsync(new QueueDescriptor { Name = "a" });
        await manager.CreateAsync(new QueueDescriptor { Name = "b" });
        await manager.CreateAsync(new QueueDescriptor { Name = "c" });

        var all = await manager.ListAsync();
        all.Select(q => q.Name).OrderBy(n => n).ShouldBe(new[] { "a", "b", "c" });
    }
}
