namespace Rebus.Nats.Async;

internal static class AsyncHeaders
{
    /// <summary>The maximum time in milliseconds the caller has chosen to wait for a response.</summary>
    internal const string Timeout = "async-timeout";

    /// <summary>A prefix for message ID header value indicating it was sent requesting a NATS response.</summary>
    internal const string MessageIDPrefix = "nats-async:";

    /// <summary>The NATS subject where the reply should be published.</summary>
    internal const string ReplySubject = "nats-reply-subject";
}
