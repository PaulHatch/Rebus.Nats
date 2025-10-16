using DotNet.Testcontainers.Builders;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.KeyValueStore;
using Testcontainers.Nats;
using Xunit;

namespace Rebus.Nats.Tests.Fixtures;

public class NatsTestFixture : IAsyncLifetime
{
    private readonly NatsContainer _natsContainer;
    private INatsConnection? _connection;
    private INatsJSContext? _jetStream;

    public NatsTestFixture()
    {
        _natsContainer = new NatsBuilder()
            .WithImage("nats:latest")
            .WithCommand("-js")
            .WithPortBinding(4222, true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Server is ready"))
            .Build();
    }

    public string ConnectionString => _natsContainer.GetConnectionString();

    public INatsConnection Connection
    {
        get
        {
            if (_connection == null)
                throw new InvalidOperationException("NATS connection not initialized. Did you forget to call InitializeAsync?");
            return _connection;
        }
    }

    public INatsJSContext JetStream
    {
        get
        {
            if (_jetStream == null)
                throw new InvalidOperationException("NATS JetStream not initialized. Did you forget to call InitializeAsync?");
            return _jetStream;
        }
    }

    public INatsKVContext KeyValue => new NatsKVContext(JetStream);

    public async Task InitializeAsync()
    {
        await _natsContainer.StartAsync();
        var opts = new NatsOpts { Url = ConnectionString };
        _connection = new NatsConnection(opts);
        await _connection.ConnectAsync();
        _jetStream = new NatsJSContext(_connection);
    }

    public async Task DisposeAsync()
    {
        if (_connection != null)
        {
            await _connection.DisposeAsync();
        }

        await _natsContainer.DisposeAsync();
    }

    public async Task FlushDatabaseAsync()
    {
        if (_jetStream == null)
            return;

        var streamNames = new List<string>();
        try
        {
            await foreach (var stream in _jetStream.ListStreamsAsync())
            {
                if (stream.Info?.Config?.Name != null)
                {
                    streamNames.Add(stream.Info.Config.Name);
                }
            }

            foreach (var streamName in streamNames)
            {
                try
                {
                    await _jetStream.DeleteStreamAsync(streamName);
                }
                catch
                {
                }
            }
        }
        catch
        {
        }
    }
}
