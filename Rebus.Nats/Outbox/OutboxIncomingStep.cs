using System;
using System.Threading.Tasks;
using Rebus.Pipeline;
using Rebus.Transport;

namespace Rebus.Nats.Outbox;

[StepDocumentation(
    "Adds a new NATS transaction context and registers committing it with the current Rebus transaction context."
)]
internal class OutboxIncomingStep : IIncomingStep
{
    private readonly NatsProvider _provider;

    public OutboxIncomingStep(NatsProvider provider)
    {
        _provider = provider ?? throw new ArgumentNullException(nameof(provider));
    }

    public Task Process(IncomingStepContext context, Func<Task> next)
    {
        var outboxConnection = _provider.GetWithTransaction();
        var transactionContext = context.Load<ITransactionContext>();

        transactionContext.Items[NatsProvider.CurrentOutboxConnectionKey] = outboxConnection;

        transactionContext.OnCommit(async _ => await outboxConnection.Commit());

        return next();
    }
}
