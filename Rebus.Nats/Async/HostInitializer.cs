using NATS.Client.Core;
using Rebus.Bus;

namespace Rebus.Nats.Async;

/// <summary>Initializes host-side async support by registering the NATS connection for publishing replies.</summary>
internal class HostInitializer : IInitializable
{
    private readonly INatsConnection _connection;

    public HostInitializer(INatsConnection connection)
    {
        _connection = connection;
    }

    public void Initialize()
    {
        _connection.ConnectAsync().ConfigureAwait(false).GetAwaiter().GetResult();
        AsyncHostNatsExtensions.RegisterHost(_connection);
    }
}
