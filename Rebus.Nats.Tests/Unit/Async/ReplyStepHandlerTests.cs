using NSubstitute;
using Rebus.Messages;
using Rebus.Nats.Async;
using Rebus.Pipeline;
using Rebus.Pipeline.Send;
using Rebus.Transport;
using Xunit;

namespace Rebus.Nats.Tests.Unit.Async;

public class ReplyStepHandlerTests
{
    private readonly ReplyStepHandler _handler;
    private readonly ITransactionContext _transactionContext;

    public ReplyStepHandlerTests()
    {
        _handler = new ReplyStepHandler();
        _transactionContext = Substitute.For<ITransactionContext>();
    }

    private OutgoingStepContext CreateContext(TransportMessage transportMessage)
    {
        var logicalMessage = new Message(transportMessage.Headers, new object());
        var destinationAddresses = new DestinationAddresses(["test-destination"]);
        var context = new OutgoingStepContext(logicalMessage, _transactionContext, destinationAddresses);
        context.Save(transportMessage);
        return context;
    }

    [Fact]
    public async Task Process_WithoutReplyContext_ShouldCallNext()
    {
        var message = new TransportMessage(
            new Dictionary<string, string> {["test-header"] = "test-value"},
            [1, 2, 3]);
        var context = CreateContext(message);

        var nextCalled = false;

        await _handler.Process(context, () =>
        {
            nextCalled = true;
            return Task.CompletedTask;
        });

        Assert.True(nextCalled);
    }

    [Fact]
    public async Task Process_WithException_ShouldPropagateException()
    {
        var message = new TransportMessage(
            new Dictionary<string, string>(),
            [1, 2, 3]);
        var context = CreateContext(message);

        var expectedException = new InvalidOperationException("Test exception");
        Func<Task> next = () => throw expectedException;

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () => await _handler.Process(context, next));
        Assert.Equal("Test exception", ex.Message);
    }

    [Fact]
    public async Task Process_WithMultipleMessages_ShouldProcessEachCorrectly()
    {
        var processCount = 0;
        Func<Task> next = () =>
        {
            processCount++;
            return Task.CompletedTask;
        };

        var message1 = new TransportMessage(new Dictionary<string, string>(), []);
        var context1 = CreateContext(message1);
        await _handler.Process(context1, next);

        var message2 = new TransportMessage(new Dictionary<string, string>(), []);
        var context2 = CreateContext(message2);
        await _handler.Process(context2, next);

        Assert.Equal(2, processCount);
    }

    [Fact]
    public async Task Process_AlwaysCallsNextOrNatsReply()
    {
        var message = new TransportMessage(
            new Dictionary<string, string> {[Headers.MessageId] = Guid.NewGuid().ToString()},
            [1, 2, 3]);
        var context = CreateContext(message);

        var nextCalled = false;
        Func<Task> next = () =>
        {
            nextCalled = true;
            return Task.CompletedTask;
        };

        await _handler.Process(context, next);

        Assert.True(nextCalled);
    }
}
