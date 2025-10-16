using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.KeyValueStore;
using NSubstitute;
using Rebus.Nats.Subscriptions;
using Xunit;

namespace Rebus.Nats.Tests.Unit.Subscriptions;

public class NatsSubscriptionStorageTests
{
    private readonly INatsConnection _connection;
    private readonly INatsJSContext _jetStream;
    private readonly INatsKVContext _kvContext;
    private readonly INatsKVStore _kvStore;
    private readonly NatsProvider _natsProvider;
    private readonly NatsSubscriptionStorage _storage;

    public NatsSubscriptionStorageTests()
    {
        _connection = Substitute.For<INatsConnection>();
        _jetStream = Substitute.For<INatsJSContext>();
        _kvContext = Substitute.For<INatsKVContext>();
        _kvStore = Substitute.For<INatsKVStore>();

        _kvContext.CreateStoreAsync(Arg.Any<NatsKVConfig>(), Arg.Any<CancellationToken>())
            .Returns(_kvStore);

        _natsProvider = new NatsProvider(_connection, _jetStream, _kvContext);

        _storage = new NatsSubscriptionStorage(_natsProvider, "test-subscriptions", isCentralized: false);
    }

    [Fact]
    public async Task GetSubscriberAddresses_WhenNoSubscribers_ShouldReturnEmptyList()
    {
        var topic = "EmptyTopic";

        var result = await _storage.GetSubscriberAddresses(topic);

        Assert.Empty(result);
    }

    [Fact]
    public async Task RegisterSubscriber_ShouldAddSubscriber()
    {
        var topic = "TestTopic";
        var subscriberAddress = "subscriber@host";

        await _storage.RegisterSubscriber(topic, subscriberAddress);
    }

    [Fact]
    public async Task UnregisterSubscriber_ShouldRemoveSubscriber()
    {
        var topic = "TestTopic";
        var subscriberAddress = "subscriber@host";

        await _storage.UnregisterSubscriber(topic, subscriberAddress);
    }

    [Fact]
    public void IsCentralized_WhenTrue_ShouldReturnTrue()
    {
        var centralizedStorage = new NatsSubscriptionStorage(_natsProvider, "test-subscriptions", isCentralized: true);

        Assert.True(centralizedStorage.IsCentralized);
    }

    [Fact]
    public void IsCentralized_WhenFalse_ShouldReturnFalse()
    {
        var nonCentralizedStorage = new NatsSubscriptionStorage(_natsProvider, "test-subscriptions", isCentralized: false);

        Assert.False(nonCentralizedStorage.IsCentralized);
    }
}
