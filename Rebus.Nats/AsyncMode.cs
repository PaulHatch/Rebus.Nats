namespace Rebus.Nats;

/// <summary>Specifies which async capabilities to enable for NATS communication.</summary>
public enum AsyncMode
{
    /// <summary>Enable both client and host async capabilities for bidirectional request-reply patterns.</summary>
    Both,

    /// <summary>Enable only client async capabilities for sending requests and receiving replies.</summary>
    Client,

    /// <summary>Enable only host async capabilities for receiving requests and sending replies.</summary>
    Host
}
