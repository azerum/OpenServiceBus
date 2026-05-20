using Amqp;
using Amqp.Framing;
using Amqp.Listener;
using Amqp.Transactions;
using Amqp.Types;
using Microsoft.Extensions.Logging;
using OpenServiceBus.Core.Transactions;

namespace OpenServiceBus.Amqp.Transactions;

/// <summary>
/// AMQP transaction coordinator. Handles attaches whose <c>Target</c> is a
/// <see cref="Coordinator"/> - the SDK's <c>TransactionScope</c> integration opens such a
/// link automatically. Two message bodies are recognised:
///
///   <c>Declare</c>  - the broker mints a fresh transaction id and replies with
///                     <see cref="Declared"/> as the disposition outcome.
///   <c>Discharge</c> - the broker commits (replay buffered ops) when <c>Fail=false</c>
///                     or rolls back (discard ops) when <c>Fail=true</c>, then replies
///                     <see cref="Accepted"/>.
///
/// Any other body shape is rejected. The processor itself is stateless beyond what the
/// <see cref="ITransactionManager"/> tracks.
/// </summary>
public sealed class CoordinatorProcessor : IMessageProcessor
{
    /// <summary>The pseudo-address used in <see cref="AddressResolver"/> for coordinator attaches.</summary>
    public const string Address = "$coordinator";

    private readonly ITransactionManager _transactions;
    private readonly ILogger<CoordinatorProcessor> _logger;

    public CoordinatorProcessor(ITransactionManager transactions, ILogger<CoordinatorProcessor> logger)
    {
        _transactions = transactions;
        _logger = logger;
    }

    public int Credit => 100;

    public void Process(MessageContext messageContext)
    {
        var body = messageContext.Message.Body;
        try
        {
            switch (body)
            {
                case Declare:
                    {
                        var txnId = _transactions.Declare();
                        // The disposition outcome carries the new txn id back to the client.
                        var declared = new Declared { TxnId = txnId };
                        messageContext.Link.DisposeMessage(messageContext.Message, declared, settled: true);
                        _logger.LogDebug("Declared txn {TxnId}", Convert.ToHexString(txnId));
                        return;
                    }
                case Discharge discharge:
                    {
                        if (discharge.Fail)
                        {
                            _transactions.Rollback(discharge.TxnId);
                            _logger.LogDebug("Rolled back txn {TxnId}", Convert.ToHexString(discharge.TxnId));
                        }
                        else
                        {
                            // Fire-and-forget the commit replay - the SDK awaits the disposition,
                            // not individual op completions, and the manager already drops ops
                            // synchronously into the dictionary.
                            _ = _transactions.CommitAsync(discharge.TxnId);
                            _logger.LogDebug("Committed txn {TxnId}", Convert.ToHexString(discharge.TxnId));
                        }
                        messageContext.Complete();
                        return;
                    }
                default:
                    messageContext.Complete(new Error(new Symbol(ErrorCode.NotImplemented))
                    {
                        Description = $"Unknown coordinator body: {body?.GetType().Name ?? "<null>"}",
                    });
                    return;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Coordinator processing failed for body {Body}", body?.GetType().Name);
            messageContext.Complete(new Error(new Symbol(ErrorCode.InternalError)) { Description = ex.Message });
        }
    }
}
