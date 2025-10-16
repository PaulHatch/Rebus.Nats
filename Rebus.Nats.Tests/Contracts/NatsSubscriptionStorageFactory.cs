using NUnit.Framework;
using Rebus.Nats.Subscriptions;
using Rebus.Nats.Tests.Fixtures;
using Rebus.Subscriptions;
using Rebus.Tests.Contracts.Subscriptions;

namespace Rebus.Nats.Tests.Contracts;

public class NatsSubscriptionStorageFactory : ISubscriptionStorageFactory
{
    private NatsTestFixture? _fixture;

    public ISubscriptionStorage Create()
    {
        _fixture = new NatsTestFixture();
        _fixture.InitializeAsync().GetAwaiter().GetResult();
        _fixture.FlushDatabaseAsync().GetAwaiter().GetResult();

        var natsProvider = new NatsProvider(_fixture.Connection, _fixture.JetStream);
        var storage = new NatsSubscriptionStorage(
            natsProvider,
            "test-subscriptions-contract",
            isCentralized: false);

        return storage;
    }

    public void Cleanup()
    {
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
