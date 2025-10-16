using Rebus.Nats.Async;
using Xunit;

namespace Rebus.Nats.Tests.Unit.Async;

public class ReplyContextTests
{
    [Fact]
    public void Constructor_ShouldInitializeProperties()
    {
        var senderAddress = "test-sender";
        var replySubject = "reply.subject.123";
        var messageId = "message-456";

        var context = new ReplyContext(senderAddress, replySubject, messageId);

        Assert.NotNull(context);
        Assert.Equal(senderAddress, context.SenderAddress);
        Assert.Equal(replySubject, context.ReplySubject);
        Assert.Equal(messageId, context.MessageID);
    }

    [Fact]
    public void Constructor_WithNullSenderAddress_ShouldThrowArgumentNullException()
    {
        var ex = Assert.Throws<ArgumentNullException>(() => new ReplyContext(null!, "reply.subject.123", "message-456"));
        Assert.Equal("senderAddress", ex.ParamName);
    }

    [Fact]
    public void Constructor_WithNullReplySubject_ShouldThrowArgumentNullException()
    {
        var ex = Assert.Throws<ArgumentNullException>(() => new ReplyContext("test-sender", null!, "message-456"));
        Assert.Equal("replySubject", ex.ParamName);
    }

    [Fact]
    public void Constructor_WithNullMessageId_ShouldThrowArgumentNullException()
    {
        var ex = Assert.Throws<ArgumentNullException>(() => new ReplyContext("test-sender", "reply.subject.123", null!));
        Assert.Equal("messageID", ex.ParamName);
    }

    [Fact]
    public void Properties_ShouldBeReadOnly()
    {
        var context = new ReplyContext("test-sender", "reply.subject.123", "message-456");

        Assert.Equal("test-sender", context.SenderAddress);
        Assert.Equal("reply.subject.123", context.ReplySubject);
        Assert.Equal("message-456", context.MessageID);
    }

    [Fact]
    public void MultipleContexts_WithSameValues_ShouldBeDistinct()
    {
        var context1 = new ReplyContext("sender", "reply.subject", "message");
        var context2 = new ReplyContext("sender", "reply.subject", "message");

        Assert.NotSame(context2, context1);
        Assert.Equal(context2.SenderAddress, context1.SenderAddress);
        Assert.Equal(context2.ReplySubject, context1.ReplySubject);
        Assert.Equal(context2.MessageID, context1.MessageID);
    }

    [Fact]
    public void Context_ShouldHandleSpecialCharacters()
    {
        var senderAddress = "test-sender:with:colons";
        var replySubject = "reply.subject.with.dots";
        var messageId = "message/with/slashes";

        var context = new ReplyContext(senderAddress, replySubject, messageId);

        Assert.Equal(senderAddress, context.SenderAddress);
        Assert.Equal(replySubject, context.ReplySubject);
        Assert.Equal(messageId, context.MessageID);
    }
}
