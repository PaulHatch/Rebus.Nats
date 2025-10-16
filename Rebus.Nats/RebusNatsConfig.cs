using System;

namespace Rebus.Nats;

/// <summary>Configuration helper for configuring NATS support.</summary>
public class RebusNatsConfig
{
    internal bool ClientAsyncEnabled { get; private set; }
    internal bool HostAsyncEnabled { get; private set; }

    /// <summary>
    /// Enables async support for the NATS transport. This will allow the transport to send and receive messages, or
    /// client-only (sending messages and awaiting replies) or host-only (receiving messages and sending replies) mode
    /// can be enabled.
    /// </summary>
    /// <param name="mode">
    /// Indicates the async mode to enable, defaults to <see cref="AsyncMode.Both" /> if not specified.
    /// </param>
    public RebusNatsConfig EnableAsync(AsyncMode mode = AsyncMode.Both)
    {
        (ClientAsyncEnabled, HostAsyncEnabled) = mode switch
        {
            AsyncMode.Both => (true, true),
            AsyncMode.Client => (true, false),
            AsyncMode.Host => (false, true),
            _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null)
        };

        return this;
    }
}
