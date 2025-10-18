using NUnit.Framework;
using Rebus.Logging;
using Rebus.Nats.Tests.Fixtures;
using Rebus.Nats.Transport;
using Rebus.Subscriptions;
using Rebus.Tests.Contracts.Subscriptions;

namespace Rebus.Nats.Tests.Contracts;

public class NatsSubscriptionStorageFactory : ISubscriptionStorageFactory
{
    private NatsTestFixture? _fixture;
    private NatsTransport? _transport;

    public ISubscriptionStorage Create()
    {
        _fixture = new NatsTestFixture();
        _fixture.InitializeAsync().GetAwaiter().GetResult();
        _fixture.FlushDatabaseAsync().GetAwaiter().GetResult();

        var natsProvider = new NatsProvider(_fixture.Connection, _fixture.JetStream);
        var loggerFactory = new ConsoleLoggerFactory(false);

        var options = new NatsTransportOptions
        {
            StreamName = "test-subscriptions-stream"
        };

        _transport = new NatsTransport(
            natsProvider,
            inputQueueName: "test-subscription-queue",
            loggerFactory,
            options);

        _transport.Initialize();

        return _transport;
    }

    public void Cleanup()
    {
        if (_transport != null)
        {
            _transport.Dispose();
            _transport = null;
        }

        if (_fixture != null)
        {
            _fixture.FlushDatabaseAsync().GetAwaiter().GetResult();
            _fixture.DisposeAsync().GetAwaiter().GetResult();
            _fixture = null;
        }
    }
}

[TestFixture]
public class NatsSubscriptionStorageBasicSubscriptionOperations : BasicSubscriptionOperations<NatsSubscriptionStorageFactory>
{
}
