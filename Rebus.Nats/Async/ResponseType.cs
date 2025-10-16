namespace Rebus.Nats.Async;

/// <summary>Indicates the type of response received from an async request-reply operation.</summary>
internal enum ResponseType
{
    /// <summary>The operation completed successfully.</summary>
    Success,

    /// <summary>The operation failed with an error.</summary>
    Error,

    /// <summary>The operation was cancelled.</summary>
    Cancelled
}
