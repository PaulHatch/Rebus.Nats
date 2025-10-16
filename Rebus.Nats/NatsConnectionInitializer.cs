using NATS.Client.Core;
using Rebus.Bus;

namespace Rebus.Nats;

/// <summary>Ensures the NATS connection is established during bus initialization.</summary>
internal class NatsConnectionInitializer : IInitializable
{
    private readonly INatsConnection _connection;

    public NatsConnectionInitializer(INatsConnection connection)
    {
        _connection = connection;
    }

    public void Initialize()
    {
        _connection.ConnectAsync().ConfigureAwait(false).GetAwaiter().GetResult();
    }
}
