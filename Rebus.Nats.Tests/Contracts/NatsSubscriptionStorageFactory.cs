using System.Text;
using NUnit.Framework;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Nats.Tests.Fixtures;
using Rebus.Nats.Transport;
using Rebus.Transport;

namespace Rebus.Nats.Tests.Contracts;

[TestFixture]
public class NatsNativePubSubTests
{
    private NatsTestFixture? _fixture;
    private NatsTransport? _transport;

    [SetUp]
    public async Task SetUp()
    {
        _fixture = new NatsTestFixture();
        await _fixture.InitializeAsync();
        await _fixture.FlushDatabaseAsync();

        var natsProvider = new NatsProvider(_fixture.Connection, _fixture.JetStream);
        var loggerFactory = new ConsoleLoggerFactory(false);

        var options = new NatsTransportOptions
        {
            StreamName = "test-pubsub-stream"
        };

        _transport = new NatsTransport(
            natsProvider,
            inputQueueName: "test-subscriber-queue",
            loggerFactory,
            options);

        _transport.Initialize();
    }

    [TearDown]
    public async Task TearDown()
    {
        _transport?.Dispose();
        _transport = null;

        if (_fixture != null)
        {
            await _fixture.FlushDatabaseAsync();
            await _fixture.DisposeAsync();
            _fixture = null;
        }
    }

    [Test]
    public async Task GetSubscriberAddresses_ReturnsTopicAddress()
    {
        var addresses = await _transport!.GetSubscriberAddresses("MyNamespace.MyEvent");

        Assert.That(addresses, Has.Count.EqualTo(1));
        Assert.That(addresses[0], Does.StartWith("topic:"));
    }

    [Test]
    public async Task GetSubscriberAddresses_ReturnsSameAddressRegardlessOfSubscribers()
    {
        var addressesBefore = await _transport!.GetSubscriberAddresses("MyTopic");

        await _transport.RegisterSubscriber("MyTopic", "subscriber1");
        await _transport.RegisterSubscriber("MyTopic", "subscriber2");

        var addressesAfter = await _transport!.GetSubscriberAddresses("MyTopic");

        Assert.That(addressesBefore, Is.EqualTo(addressesAfter));
    }

    [Test]
    public void IsCentralized_ReturnsTrue()
    {
        Assert.That(_transport!.IsCentralized, Is.True);
    }
}

[TestFixture]
public class NatsPubSubIntegrationTests
{
    private NatsTestFixture? _fixture;

    [SetUp]
    public async Task SetUp()
    {
        _fixture = new NatsTestFixture();
        await _fixture.InitializeAsync();
        await _fixture.FlushDatabaseAsync();
    }

    [TearDown]
    public async Task TearDown()
    {
        if (_fixture != null)
        {
            await _fixture.FlushDatabaseAsync();
            await _fixture.DisposeAsync();
            _fixture = null;
        }
    }

    [Test]
    public async Task PublishToTopic_DeliversToAllSubscribers()
    {
        var loggerFactory = new ConsoleLoggerFactory(false);
        var options = new NatsTransportOptions { StreamName = "test-pubsub-integration" };
        var natsProvider = new NatsProvider(_fixture!.Connection, _fixture.JetStream);

        // Create publisher (one-way client)
        using var publisher = new NatsTransport(natsProvider, null, loggerFactory, options);
        publisher.Initialize();

        // Create two subscribers
        using var subscriber1 = new NatsTransport(natsProvider, "subscriber1", loggerFactory, options);
        subscriber1.Initialize();

        using var subscriber2 = new NatsTransport(natsProvider, "subscriber2", loggerFactory, options);
        subscriber2.Initialize();

        const string topic = "TestEvents.OrderCreated";

        // Both subscribers register for the same topic
        await subscriber1.RegisterSubscriber(topic, "subscriber1");
        await subscriber2.RegisterSubscriber(topic, "subscriber2");

        // Get the topic address for publishing
        var topicAddresses = await publisher.GetSubscriberAddresses(topic);
        Assert.That(topicAddresses, Has.Count.EqualTo(1));

        // Create and publish a message
        var messageId = Guid.NewGuid().ToString();
        var headers = new Dictionary<string, string>
        {
            [Headers.MessageId] = messageId,
            [Headers.Type] = "TestEvents.OrderCreated"
        };
        var body = Encoding.UTF8.GetBytes("{\"OrderId\": 123}");
        var transportMessage = new TransportMessage(headers, body);

        // Send to the topic address
        using var scope = new RebusTransactionScope();
        await publisher.Send(topicAddresses[0], transportMessage, scope.TransactionContext);
        await scope.CompleteAsync();

        // Allow time for message to be published
        await Task.Delay(500);

        // Both subscribers should receive the message
        using var receiveScope1 = new RebusTransactionScope();
        var received1 = await subscriber1.Receive(receiveScope1.TransactionContext, CancellationToken.None);
        await receiveScope1.CompleteAsync();

        using var receiveScope2 = new RebusTransactionScope();
        var received2 = await subscriber2.Receive(receiveScope2.TransactionContext, CancellationToken.None);
        await receiveScope2.CompleteAsync();

        Assert.That(received1, Is.Not.Null, "Subscriber 1 should receive the message");
        Assert.That(received2, Is.Not.Null, "Subscriber 2 should receive the message");
        Assert.That(received1!.Headers[Headers.MessageId], Is.EqualTo(messageId));
        Assert.That(received2!.Headers[Headers.MessageId], Is.EqualTo(messageId));
    }

    [Test]
    public async Task Subscriptions_AreDurable_SurvivesConsumerRecreation()
    {
        var loggerFactory = new ConsoleLoggerFactory(false);
        var options = new NatsTransportOptions { StreamName = "test-durable-subs" };
        var natsProvider = new NatsProvider(_fixture!.Connection, _fixture.JetStream);

        const string topic = "TestEvents.UserCreated";

        // Create subscriber and register subscription
        using (var subscriber = new NatsTransport(natsProvider, "durable-subscriber", loggerFactory, options))
        {
            subscriber.Initialize();
            await subscriber.RegisterSubscriber(topic, "durable-subscriber");
        }

        // Create a new transport instance with the same queue name - subscription should persist
        using var newSubscriber = new NatsTransport(natsProvider, "durable-subscriber", loggerFactory, options);
        newSubscriber.Initialize();

        // Publish a message
        using var publisher = new NatsTransport(natsProvider, null, loggerFactory, options);
        publisher.Initialize();

        var topicAddresses = await publisher.GetSubscriberAddresses(topic);
        var messageId = Guid.NewGuid().ToString();
        var headers = new Dictionary<string, string>
        {
            [Headers.MessageId] = messageId,
            [Headers.Type] = topic
        };
        var body = Encoding.UTF8.GetBytes("{}");
        var transportMessage = new TransportMessage(headers, body);

        using var sendScope = new RebusTransactionScope();
        await publisher.Send(topicAddresses[0], transportMessage, sendScope.TransactionContext);
        await sendScope.CompleteAsync();

        await Task.Delay(500);

        // New subscriber instance should receive the message because subscription is durable
        using var receiveScope = new RebusTransactionScope();
        var received = await newSubscriber.Receive(receiveScope.TransactionContext, CancellationToken.None);
        await receiveScope.CompleteAsync();

        Assert.That(received, Is.Not.Null, "New subscriber instance should receive message via durable subscription");
        Assert.That(received!.Headers[Headers.MessageId], Is.EqualTo(messageId));
    }

    [Test]
    public async Task DirectQueueMessages_AreNotAffectedByTopicSubscriptions()
    {
        var loggerFactory = new ConsoleLoggerFactory(false);
        var options = new NatsTransportOptions { StreamName = "test-direct-queue" };
        var natsProvider = new NatsProvider(_fixture!.Connection, _fixture.JetStream);

        using var receiver = new NatsTransport(natsProvider, "direct-receiver", loggerFactory, options);
        receiver.Initialize();

        using var sender = new NatsTransport(natsProvider, null, loggerFactory, options);
        sender.Initialize();

        // Send directly to queue (not via topic)
        var messageId = Guid.NewGuid().ToString();
        var headers = new Dictionary<string, string>
        {
            [Headers.MessageId] = messageId,
            [Headers.Type] = "DirectMessage"
        };
        var body = Encoding.UTF8.GetBytes("direct");
        var transportMessage = new TransportMessage(headers, body);

        using var sendScope = new RebusTransactionScope();
        await sender.Send("direct-receiver", transportMessage, sendScope.TransactionContext);
        await sendScope.CompleteAsync();

        await Task.Delay(500);

        using var receiveScope = new RebusTransactionScope();
        var received = await receiver.Receive(receiveScope.TransactionContext, CancellationToken.None);
        await receiveScope.CompleteAsync();

        Assert.That(received, Is.Not.Null);
        Assert.That(received!.Headers[Headers.MessageId], Is.EqualTo(messageId));
    }
}
