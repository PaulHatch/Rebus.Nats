using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.KeyValueStore;

namespace Rebus.Nats;

/// <summary>
/// Provider for <see cref="NatsTransactionContext" />, responsible for managing NATS connections
/// and providing scoped transactional support using JetStream.
/// </summary>
public class NatsProvider
{
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

}
