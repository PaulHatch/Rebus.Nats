using System.Collections.Generic;
using Rebus.Messages;

namespace Rebus.Nats.Outbox;

/// <summary>
/// Represents a message stored in the NATS outbox, containing all necessary information to forward the message
/// to its destination after the transaction commits.
/// </summary>
public class OutboxMessage
{
    /// <summary>Creates a new instance of the <see cref="OutboxMessage" /> class.</summary>
    /// <param name="sequence">The JetStream sequence number identifying this message in the stream.</param>
    public OutboxMessage(ulong sequence)
    {
        Sequence = sequence;
    }

    /// <summary>Gets the JetStream sequence number identifying this message in the stream.</summary>
    public ulong Sequence { get; }

    /// <summary>Gets or sets the destination address where the message should be forwarded.</summary>
    public string? DestinationAddress { get; set; }

    /// <summary>Gets the message headers to include when forwarding.</summary>
    public Dictionary<string, string> Headers { get; } = new();

    /// <summary>Gets or sets the serialized message body.</summary>
    public byte[]? Body { get; set; }

    /// <summary>Converts this outbox message to a Rebus transport message for forwarding.</summary>
    public TransportMessage ToTransportMessage()
    {
        return new TransportMessage(Headers, Body);
    }
}
