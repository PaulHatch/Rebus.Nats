using System.Collections.Generic;
using System.Threading.Tasks;
using Rebus.Transport;

namespace Rebus.Nats.Outbox;

/// <summary>Defines storage operations for the transactional outbox pattern using NATS JetStream.</summary>
public interface IOutboxStorage
{
    /// <summary>Retrieves the next batch of messages to be dispatched from the outbox.</summary>
    Task<IEnumerable<OutboxMessage>?> GetNextMessageBatch();

    /// <summary>Marks a message as successfully dispatched and removes it from the outbox.</summary>
    /// <param name="message">The message that has been dispatched.</param>
    Task MarkAsDispatched(OutboxMessage message);

    /// <summary>Trims the outbox stream to maintain a maximum size.</summary>
    Task TrimQueue();

    /// <summary>Removes idle consumers that have not processed messages within the timeout period.</summary>
    Task CleanupIdleConsumers();

    /// <summary>Retrieves messages that were claimed by consumers but not acknowledged within the timeout.</summary>
    Task<IEnumerable<OutboxMessage>?> GetOrphanedMessageBatch();
}

/// <summary>Internal interface for saving messages to the outbox within a transaction context.</summary>
internal interface IOutboxQueueStorage
{
    /// <summary>Saves an outgoing message to the outbox as part of a transaction.</summary>
    /// <param name="message">The message to save.</param>
    /// <param name="transaction">The transaction context to add the save operation to.</param>
    Task Save(OutgoingTransportMessage message, NatsTransactionContext transaction);
}
