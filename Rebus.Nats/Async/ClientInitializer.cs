using NATS.Client.Core;
using Rebus.Bus;

namespace Rebus.Nats.Async;

/// <summary>Initializes client-side async support by registering the NATS connection for sending requests.</summary>
internal class ClientInitializer : IInitializable
{
    private readonly INatsConnection _connection;

    public ClientInitializer(INatsConnection connection)
    {
        _connection = connection;
    }

    public void Initialize()
    {
        _connection.ConnectAsync().ConfigureAwait(false).GetAwaiter().GetResult();
        AsyncClientNatsExtensions.RegisterClient(_connection);
    }
}
