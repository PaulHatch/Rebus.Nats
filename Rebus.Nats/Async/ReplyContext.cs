using System;

namespace Rebus.Nats.Async;

/// <summary>
/// Contains information needed to send a reply to an async request, including the NATS subject for publishing
/// the reply and identifying information about the original request.
/// </summary>
public class ReplyContext
{
    /// <summary>Creates a new instance of the <see cref="ReplyContext" /> class.</summary>
    /// <param name="senderAddress">The address of the service that sent the request.</param>
    /// <param name="replySubject">The NATS subject where the reply should be published.</param>
    /// <param name="messageID">The unique identifier of the request message.</param>
    public ReplyContext(string senderAddress, string replySubject, string messageID)
    {
        SenderAddress = senderAddress ?? throw new ArgumentNullException(nameof(senderAddress));
        ReplySubject = replySubject ?? throw new ArgumentNullException(nameof(replySubject));
        MessageID = messageID ?? throw new ArgumentNullException(nameof(messageID));
    }

    /// <summary>Gets the address of the service that sent the request.</summary>
    public string SenderAddress { get; }

    /// <summary>Gets the NATS subject where the reply should be published.</summary>
    public string ReplySubject { get; }

    /// <summary>Gets the unique identifier of the request message.</summary>
    public string MessageID { get; }
}
