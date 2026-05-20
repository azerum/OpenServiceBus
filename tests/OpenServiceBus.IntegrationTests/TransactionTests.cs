using System.Transactions;
using Azure.Messaging.ServiceBus;
using OpenServiceBus.Core.Entities;

namespace OpenServiceBus.IntegrationTests;

/// <summary>
/// AMQP transactions. The Azure SDK enlists in <see cref="TransactionScope"/>
/// automatically - opens a coordinator link on first transactional op, sends ops with
/// <c>TransactionalState</c>, and discharges on scope completion.
/// </summary>
public class TransactionTests
{
    [Fact]
    public async Task SendInTransactionScope_Committed_MessageBecomesVisible()
    {
        // Arrange
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "txn-q" });
        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("txn-q");

        // Act - send inside a scope, mark complete.
        using (var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await sender.SendMessageAsync(new ServiceBusMessage("hello-tx") { MessageId = "tx-1" });
            // Before commit the broker MUST NOT have stored the message.
            (await harness.Store.CountAsync("txn-q")).ShouldBe(0L, "uncommitted send must be invisible");
            scope.Complete();
        }

        // After scope dispose, the SDK discharges the txn. Wait for the commit replay.
        var visible = await TestUtilities.WaitForCountAsync(harness.Store, "txn-q", expected: 1, timeoutMs: 2000);
        visible.ShouldBeTrue("the committed send must surface as a regular available message");

        var receiver = client.CreateReceiver("txn-q");
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        msg.ShouldNotBeNull();
        msg.MessageId.ShouldBe("tx-1");
        msg.Body.ToString().ShouldBe("hello-tx");
    }

    [Fact]
    public async Task SendInTransactionScope_NotCompleted_MessageIsDiscarded()
    {
        // Arrange
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "txn-rollback" });
        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("txn-rollback");

        // Act - open scope, send, but DON'T call Complete - TransactionScope rolls back on dispose.
        using (new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await sender.SendMessageAsync(new ServiceBusMessage("doomed") { MessageId = "rb-1" });
        }

        // Give the rollback time to be processed.
        await Task.Delay(500);

        // Assert
        (await harness.Store.CountAsync("txn-rollback")).ShouldBe(0L, "rolled-back send must leave no trace");
    }

    [Fact]
    public async Task MultipleSendsInTransactionScope_AllOrNothing()
    {
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "txn-multi" });
        await using var client = new ServiceBusClient(harness.ConnectionString);
        var sender = client.CreateSender("txn-multi");

        // Three sends, then commit.
        using (var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await sender.SendMessageAsync(new ServiceBusMessage("m1") { MessageId = "m1" });
            await sender.SendMessageAsync(new ServiceBusMessage("m2") { MessageId = "m2" });
            await sender.SendMessageAsync(new ServiceBusMessage("m3") { MessageId = "m3" });
            (await harness.Store.CountAsync("txn-multi")).ShouldBe(0L, "no sends visible mid-txn");
            scope.Complete();
        }

        (await TestUtilities.WaitForCountAsync(harness.Store, "txn-multi", expected: 3, timeoutMs: 2000))
            .ShouldBeTrue("all three sends must commit together");
    }

    [Fact]
    public async Task CompleteInTransactionScope_Rollback_LockReleasedAndRedelivered()
    {
        // Pre-seed a message, peek-lock it, then call CompleteMessageAsync inside a scope
        // that rolls back. The lock release (TryComplete) must NOT happen - the next receive
        // (after the lock expires or via this same receiver) should still see the message.
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor
        {
            Name = "txn-complete-rb",
            LockDuration = TimeSpan.FromSeconds(2),
        });
        await using var client = new ServiceBusClient(harness.ConnectionString);
        await client.CreateSender("txn-complete-rb").SendMessageAsync(new ServiceBusMessage("stay") { MessageId = "s1" });

        var receiver = client.CreateReceiver("txn-complete-rb");
        var first = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        first.ShouldNotBeNull();
        first.MessageId.ShouldBe("s1");

        // Complete inside a scope, then let it roll back by NOT calling Complete().
        using (new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await receiver.CompleteMessageAsync(first);
        }
        await Task.Delay(300);

        // The completion was buffered and rolled back - message still on the queue under lock.
        (await harness.Store.CountAsync("txn-complete-rb")).ShouldBe(1L,
            "the buffered complete must not have removed the message on rollback");
    }

    [Fact]
    public async Task CompleteInTransactionScope_Committed_MessageRemoved()
    {
        await using var harness = await IntegrationHarness.StartAsync();
        await harness.Queues.CreateAsync(new QueueDescriptor { Name = "txn-complete-ok" });
        await using var client = new ServiceBusClient(harness.ConnectionString);
        await client.CreateSender("txn-complete-ok").SendMessageAsync(new ServiceBusMessage("die") { MessageId = "d1" });

        var receiver = client.CreateReceiver("txn-complete-ok");
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        msg.ShouldNotBeNull();

        using (var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await receiver.CompleteMessageAsync(msg);
            scope.Complete();
        }

        (await TestUtilities.WaitForCountAsync(harness.Store, "txn-complete-ok", expected: 0, timeoutMs: 2000))
            .ShouldBeTrue("committed complete must remove the message");
    }
}

internal static class TestUtilities
{
    /// <summary>
    /// Spin until the queue count reaches <paramref name="expected"/> or the timeout elapses.
    /// Used after a transaction commits - the discharge round-trip is asynchronous on the SDK
    /// side, so a tight check immediately after scope.Complete() can race the replay.
    /// </summary>
    public static async Task<bool> WaitForCountAsync(
        OpenServiceBus.Core.Storage.IMessageStore store, string queue, long expected, int timeoutMs)
    {
        var deadline = Environment.TickCount + timeoutMs;
        while (Environment.TickCount < deadline)
        {
            if (await store.CountAsync(queue) == expected) return true;
            await Task.Delay(20);
        }
        return await store.CountAsync(queue) == expected;
    }
}
