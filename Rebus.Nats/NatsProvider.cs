using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.KeyValueStore;
using Rebus.Pipeline;
using Rebus.Transport;

namespace Rebus.Nats;

/// <summary>
/// Provider for <see cref="NatsTransactionContext" />, responsible for managing NATS connections
/// and providing scoped transactional support using JetStream.
/// </summary>
public class NatsProvider
{
    internal const string CurrentOutboxConnectionKey = "nats-outbox-context";
    private readonly INatsConnection _connection;
    private readonly INatsJSContext _jetStream;
    private readonly INatsKVContext _keyValue;

    /// <summary>Creates a new instance of the <see cref="NatsProvider" /> class.</summary>
    /// <param name="connection">The NATS connection to use.</param>
    /// <param name="jetStream">The JetStream context for reliable messaging.</param>
    /// <param name="keyValue">The Key-Value context for accessing NATS Key-Value stores. If not provided, a new context will be created.</param>
    public NatsProvider(INatsConnection connection, INatsJSContext jetStream, INatsKVContext? keyValue = null)
    {
        _connection = connection;
        _jetStream = jetStream;
        _keyValue = keyValue ?? new NatsKVContext(jetStream);
    }

    /// <summary>Gets the NATS connection for direct messaging operations.</summary>
    public INatsConnection Connection => _connection;

    /// <summary>Gets the JetStream context for reliable, persistent messaging operations.</summary>
    public INatsJSContext JetStream => _jetStream;

    /// <summary>Gets the Key-Value context for accessing NATS Key-Value stores.</summary>
    public INatsKVContext KeyValue => _keyValue;

    /// <summary>
    /// Creates a <see cref="NatsTransactionContext" /> with JetStream support. Note that this is the only place
    /// where new NATS transaction contexts are created for transactional operations.
    /// </summary>
    public NatsTransactionContext GetWithTransaction()
    {
        return new NatsTransactionContext(_jetStream);
    }

    /// <summary>
    /// Creates a <see cref="NatsTransactionContext" /> without transactional semantics, which can be used to run
    /// operations outside the context of a Rebus transaction.
    /// </summary>
    public NatsTransactionContext GetWithoutTransaction()
    {
        return new NatsTransactionContext(_jetStream);
    }

    /// <summary>
    /// Gets an existing <see cref="NatsTransactionContext" /> from the current Rebus transaction scope if one exists,
    /// or creates a new non-transactional <see cref="NatsTransactionContext" /> if one does not exist.
    /// </summary>
    /// <param name="context">
    /// The Rebus <see cref="ITransactionContext" /> to check for a transaction. If none is provided, the current
    /// "MessageContext.Current.TransactionContext" is used.
    /// </param>
    public NatsTransactionContext GetForScope(ITransactionContext? context = null)
    {
        var currentContext = context ?? MessageContext.Current?.TransactionContext;

        if (currentContext?.Items.TryGetValue(CurrentOutboxConnectionKey, out var result) == true &&
            result is NatsTransactionContext natsContext)
        {
            return natsContext;
        }

        return GetWithoutTransaction();
    }
}
