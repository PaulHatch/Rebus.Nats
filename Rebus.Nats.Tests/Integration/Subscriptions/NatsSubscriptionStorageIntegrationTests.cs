using Rebus.Nats.Subscriptions;
using Rebus.Nats.Tests.Fixtures;
using Xunit;

namespace Rebus.Nats.Tests.Integration.Subscriptions;

public class NatsSubscriptionStorageIntegrationTests : IClassFixture<NatsTestFixture>
{
    private readonly NatsTestFixture _natsFixture;

    public NatsSubscriptionStorageIntegrationTests(NatsTestFixture natsFixture)
    {
        _natsFixture = natsFixture;
    }

    [Fact]
    public async Task SubscriptionStorage_ShouldStoreAndRetrieveSubscriptions()
    {
        await _natsFixture.FlushDatabaseAsync();

        var natsProvider = new NatsProvider(_natsFixture.Connection, _natsFixture.JetStream);
        var storage = new NatsSubscriptionStorage(
            natsProvider,
            "test-subscriptions-1",
            isCentralized: false);

        var topic = "test-topic";
        var subscribers = new[]
        {
            "subscriber1@queue1",
            "subscriber2@queue2",
            "subscriber3@queue3"
        };

        foreach (var subscriber in subscribers)
        {
            await storage.RegisterSubscriber(topic, subscriber);
        }

        var retrieved = await storage.GetSubscriberAddresses(topic);

        Assert.Equal(3, retrieved.Count);
        Assert.Equal(new HashSet<string>(subscribers), new HashSet<string>(retrieved));
    }

    [Fact]
    public async Task SubscriptionStorage_ShouldHandleUnregistration()
    {
        await _natsFixture.FlushDatabaseAsync();

        var natsProvider = new NatsProvider(_natsFixture.Connection, _natsFixture.JetStream);
        var storage = new NatsSubscriptionStorage(
            natsProvider,
            "test-subscriptions-2",
            isCentralized: false);

        var topic = "unregister-test";
        var subscriber1 = "subscriber1@queue";
        var subscriber2 = "subscriber2@queue";

        await storage.RegisterSubscriber(topic, subscriber1);
        await storage.RegisterSubscriber(topic, subscriber2);

        var before = await storage.GetSubscriberAddresses(topic);
        Assert.Equal(2, before.Count);

        await storage.UnregisterSubscriber(topic, subscriber1);

        var after = await storage.GetSubscriberAddresses(topic);
        Assert.Single(after);
        Assert.Contains(subscriber2, after);
        Assert.DoesNotContain(subscriber1, after);
    }

    [Fact]
    public async Task SubscriptionStorage_ShouldHandleMultipleTopics()
    {
        await _natsFixture.FlushDatabaseAsync();

        var natsProvider = new NatsProvider(_natsFixture.Connection, _natsFixture.JetStream);
        var storage = new NatsSubscriptionStorage(
            natsProvider,
            "test-subscriptions-3",
            isCentralized: false);

        var topic1 = "topic1";
        var topic2 = "topic2";
        var subscriber1 = "subscriber1@queue";
        var subscriber2 = "subscriber2@queue";
        var subscriber3 = "subscriber3@queue";

        await storage.RegisterSubscriber(topic1, subscriber1);
        await storage.RegisterSubscriber(topic1, subscriber2);
        await storage.RegisterSubscriber(topic2, subscriber2);
        await storage.RegisterSubscriber(topic2, subscriber3);

        var topic1Subscribers = await storage.GetSubscriberAddresses(topic1);
        Assert.Equal(2, topic1Subscribers.Count);
        Assert.Contains(subscriber1, topic1Subscribers);
        Assert.Contains(subscriber2, topic1Subscribers);

        var topic2Subscribers = await storage.GetSubscriberAddresses(topic2);
        Assert.Equal(2, topic2Subscribers.Count);
        Assert.Contains(subscriber2, topic2Subscribers);
        Assert.Contains(subscriber3, topic2Subscribers);
    }

    [Fact]
    public async Task SubscriptionStorage_CentralizedMode_ShouldShareAcrossBuses()
    {
        await _natsFixture.FlushDatabaseAsync();

        var natsProvider = new NatsProvider(_natsFixture.Connection, _natsFixture.JetStream);
        var storage1 = new NatsSubscriptionStorage(
            natsProvider,
            "shared-subscriptions",
            isCentralized: true);

        var storage2 = new NatsSubscriptionStorage(
            natsProvider,
            "shared-subscriptions",
            isCentralized: true);

        var topic = "shared-topic";
        var subscriber1 = "bus1-subscriber@queue";
        var subscriber2 = "bus2-subscriber@queue";

        await storage1.RegisterSubscriber(topic, subscriber1);
        await storage2.RegisterSubscriber(topic, subscriber2);

        var fromStorage1 = await storage1.GetSubscriberAddresses(topic);
        var fromStorage2 = await storage2.GetSubscriberAddresses(topic);

        Assert.Equal(2, fromStorage1.Count);
        Assert.Equal(2, fromStorage2.Count);
        Assert.Equal(new HashSet<string>(fromStorage2), new HashSet<string>(fromStorage1));
        Assert.Contains(subscriber1, fromStorage1);
        Assert.Contains(subscriber2, fromStorage1);
    }

    [Fact]
    public async Task SubscriptionStorage_NonCentralizedMode_ShouldIsolateByBucket()
    {
        await _natsFixture.FlushDatabaseAsync();

        var natsProvider = new NatsProvider(_natsFixture.Connection, _natsFixture.JetStream);
        var storage1 = new NatsSubscriptionStorage(
            natsProvider,
            "bus1-subscriptions",
            isCentralized: false);

        var storage2 = new NatsSubscriptionStorage(
            natsProvider,
            "bus2-subscriptions",
            isCentralized: false);

        var topic = "isolated-topic";
        var subscriber1 = "bus1-subscriber@queue";
        var subscriber2 = "bus2-subscriber@queue";

        await storage1.RegisterSubscriber(topic, subscriber1);
        await storage2.RegisterSubscriber(topic, subscriber2);

        var fromStorage1 = await storage1.GetSubscriberAddresses(topic);
        var fromStorage2 = await storage2.GetSubscriberAddresses(topic);

        Assert.Single(fromStorage1);
        Assert.Single(fromStorage2);
        Assert.Contains(subscriber1, fromStorage1);
        Assert.Contains(subscriber2, fromStorage2);
        Assert.DoesNotContain(subscriber2, fromStorage1);
        Assert.DoesNotContain(subscriber1, fromStorage2);
    }

    [Fact]
    public async Task SubscriptionStorage_DuplicateRegistration_ShouldBeIdempotent()
    {
        await _natsFixture.FlushDatabaseAsync();

        var natsProvider = new NatsProvider(_natsFixture.Connection, _natsFixture.JetStream);
        var storage = new NatsSubscriptionStorage(
            natsProvider,
            "test-subscriptions-4",
            isCentralized: false);

        var topic = "duplicate-test";
        var subscriber = "subscriber@queue";

        await storage.RegisterSubscriber(topic, subscriber);
        await storage.RegisterSubscriber(topic, subscriber);
        await storage.RegisterSubscriber(topic, subscriber);

        var subscribers = await storage.GetSubscriberAddresses(topic);
        Assert.Single(subscribers);
        Assert.Contains(subscriber, subscribers);
    }
}
