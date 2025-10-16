using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NATS.Client.JetStream;

namespace Rebus.Nats;

/// <summary>
/// Represents a transactional context for NATS operations, collecting operations to be executed as a batch
/// when committed, providing consistency guarantees for message publishing and storage operations.
/// </summary>
public class NatsTransactionContext
{
    private readonly INatsJSContext _jetStream;
    private readonly List<Func<Task>> _operations = [];

    /// <summary>Creates a new instance of the <see cref="NatsTransactionContext" /> class.</summary>
    /// <param name="jetStream">The JetStream context for executing operations.</param>
    public NatsTransactionContext(INatsJSContext jetStream)
    {
        _jetStream = jetStream;
    }

    /// <summary>Gets the JetStream context for direct JetStream operations.</summary>
    public INatsJSContext JetStream => _jetStream;

    /// <summary>Gets a task that completes when all queued operations have been executed.</summary>
    public Task Task => Task.WhenAll(_operations.ConvertAll(op => op()));

    /// <summary>Adds an operation to be executed when the transaction is committed.</summary>
    /// <param name="operation">The async operation to queue for execution.</param>
    public void AddOperation(Func<Task> operation)
    {
        _operations.Add(operation);
    }

    /// <summary>Executes all queued operations in sequence, effectively committing the transaction.</summary>
    public async Task Commit()
    {
        await Task.WhenAll(_operations.ConvertAll(op => op()));
    }
}
