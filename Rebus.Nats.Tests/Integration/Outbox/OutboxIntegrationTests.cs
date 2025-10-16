using System.Collections.Concurrent;
using Rebus.Activation;
using Rebus.Config;
using Rebus.Nats.Outbox;
using Rebus.Nats.Tests.Fixtures;
using Rebus.Transport.InMem;
using Xunit;

namespace Rebus.Nats.Tests.Integration.Outbox;

public class OutboxIntegrationTests : IClassFixture<NatsTestFixture>, IAsyncLifetime
{
    private readonly NatsTestFixture _natsFixture;
    private readonly InMemNetwork _network;
    private readonly BuiltinHandlerActivator _activator;

    public OutboxIntegrationTests(NatsTestFixture natsFixture)
    {
        _natsFixture = natsFixture;
        _network = new InMemNetwork();
        _activator = new BuiltinHandlerActivator();
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        _activator.Dispose();
        await Task.CompletedTask;
    }

    [Fact]
    public async Task Outbox_ShouldForwardMessagesReliably()
    {
        await _natsFixture.FlushDatabaseAsync();

        var receivedMessages = new ConcurrentBag<string>();
        var resetEvent = new ManualResetEventSlim(false);
        var expectedCount = 3;

        _activator.Handle<TestMessage>(async message =>
        {
            receivedMessages.Add(message.Content);
            if (receivedMessages.Count >= expectedCount)
            {
                resetEvent.Set();
            }
            await Task.CompletedTask;
        });

        using var bus = Configure.With(_activator)
            .Transport(t => t.UseInMemoryTransport(_network, "outbox-test"))
            .Options(o =>
            {
                o.SetBusName("outbox-test");
                o.EnableNats(_natsFixture.ConnectionString);
            })
            .NatsOutbox(o => o.StoreInNats(config =>
            {
                config.SetForwardingInterval(TimeSpan.FromSeconds(1));
            }))
            .Start();

        await bus.SendLocal(new TestMessage { Content = "Message 1" });
        await bus.SendLocal(new TestMessage { Content = "Message 2" });
        await bus.SendLocal(new TestMessage { Content = "Message 3" });

        var received = resetEvent.Wait(TimeSpan.FromSeconds(10));

        Assert.True(received, "messages should be received within timeout");
        Assert.Equal(expectedCount, receivedMessages.Count);
        Assert.Contains("Message 1", receivedMessages);
        Assert.Contains("Message 2", receivedMessages);
        Assert.Contains("Message 3", receivedMessages);
    }

    [Fact]
    public async Task Outbox_ShouldHandleMultipleBuses()
    {
        await _natsFixture.FlushDatabaseAsync();

        var bus1Messages = new ConcurrentBag<string>();
        var bus2Messages = new ConcurrentBag<string>();

        using var activator1 = new BuiltinHandlerActivator();
        activator1.Handle<TestMessage>(async message =>
        {
            bus1Messages.Add(message.Content);
            await Task.CompletedTask;
        });

        using var activator2 = new BuiltinHandlerActivator();
        activator2.Handle<TestMessage>(async message =>
        {
            bus2Messages.Add(message.Content);
            await Task.CompletedTask;
        });

        using var bus1 = Configure.With(activator1)
            .Transport(t => t.UseInMemoryTransport(_network, "bus1"))
            .Options(o =>
            {
                o.SetBusName("bus1");
                o.EnableNats(_natsFixture.ConnectionString);
            })
            .NatsOutbox(o => o.StoreInNats(config =>
            {
                config.SetForwardingInterval(TimeSpan.FromSeconds(1));
            }))
            .Start();

        using var bus2 = Configure.With(activator2)
            .Transport(t => t.UseInMemoryTransport(_network, "bus2"))
            .Options(o =>
            {
                o.SetBusName("bus2");
                o.EnableNats(_natsFixture.ConnectionString);
            })
            .NatsOutbox(o => o.StoreInNats(config =>
            {
                config.SetForwardingInterval(TimeSpan.FromSeconds(1));
            }))
            .Start();

        await bus1.SendLocal(new TestMessage { Content = "Bus1 Message" });
        await bus2.SendLocal(new TestMessage { Content = "Bus2 Message" });

        await Task.Delay(3000);

        Assert.Single(bus1Messages);
        Assert.Contains("Bus1 Message", bus1Messages);

        Assert.Single(bus2Messages);
        Assert.Contains("Bus2 Message", bus2Messages);
    }

    [Fact]
    public async Task Outbox_ShouldHandleBasicProcessing()
    {
        await _natsFixture.FlushDatabaseAsync();

        var receivedMessages = new ConcurrentBag<string>();

        _activator.Handle<TestMessage>(async message =>
        {
            receivedMessages.Add(message.Content);
            await Task.CompletedTask;
        });

        using var bus = Configure.With(_activator)
            .Transport(t => t.UseInMemoryTransport(_network, "basic-test"))
            .Options(o =>
            {
                o.SetBusName("basic-test");
                o.EnableNats(_natsFixture.ConnectionString);
            })
            .NatsOutbox(o => o.StoreInNats(config =>
            {
                config.SetForwardingInterval(TimeSpan.FromSeconds(1));
            }))
            .Start();

        await bus.SendLocal(new TestMessage { Content = "Test Message" });

        await Task.Delay(3000);

        Assert.Contains("Test Message", receivedMessages);
    }

    [Fact]
    public async Task Outbox_ShouldMaintainMessageOrder()
    {
        await _natsFixture.FlushDatabaseAsync();

        var receivedMessages = new ConcurrentBag<int>();
        var resetEvent = new ManualResetEventSlim(false);
        var messageCount = 10;

        _activator.Handle<OrderedMessage>(async message =>
        {
            receivedMessages.Add(message.Order);
            if (receivedMessages.Count >= messageCount)
            {
                resetEvent.Set();
            }
            await Task.CompletedTask;
        });

        using var bus = Configure.With(_activator)
            .Transport(t => t.UseInMemoryTransport(_network, "order-test"))
            .Options(o =>
            {
                o.SetBusName("order-test");
                o.EnableNats(_natsFixture.ConnectionString);
            })
            .NatsOutbox(o => o.StoreInNats(config =>
            {
                config.SetForwardingInterval(TimeSpan.FromSeconds(1));
            }))
            .Start();

        for (int i = 0; i < messageCount; i++)
        {
            await bus.SendLocal(new OrderedMessage { Order = i });
        }

        var received = resetEvent.Wait(TimeSpan.FromSeconds(10));

        Assert.True(received, "all messages should be received within timeout");
        Assert.Equal(messageCount, receivedMessages.Count);

        var orderedList = receivedMessages.OrderBy(x => x).ToList();
        Assert.Equal(Enumerable.Range(0, messageCount), orderedList);
    }

    private class TestMessage
    {
        public string Content { get; set; } = string.Empty;
    }

    private class OrderedMessage
    {
        public int Order { get; set; }
    }
}
