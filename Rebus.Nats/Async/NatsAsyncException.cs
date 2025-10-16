using System;

namespace Rebus.Nats.Async;

/// <summary>Exception thrown when an error occurs during NATS async request-reply operations.</summary>
public class NatsAsyncException : Exception
{
    /// <summary>Creates a new instance of the <see cref="NatsAsyncException" /> class.</summary>
    /// <param name="message">The error message describing the failure.</param>
    /// <param name="messageId">The unique identifier of the message that caused the exception.</param>
    /// <param name="innerException">The exception that is the cause of the current exception, if any.</param>
    public NatsAsyncException(string message, string messageId, Exception? innerException = null)
        : base(message, innerException)
    {
        MessageId = messageId;
    }

    /// <summary>Gets the unique identifier of the message that caused the exception.</summary>
    public string MessageId { get; }
}
