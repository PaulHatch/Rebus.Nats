using System;
using System.Threading.Tasks;
using Rebus.Config;
using Rebus.Messages;
using Rebus.Pipeline;

namespace Rebus.Nats.Async;

/// <summary>
/// Pipeline step that intercepts outgoing messages to detect async replies and publish them directly to NATS
/// instead of going through the normal transport, enabling request-reply patterns.
/// </summary>
internal class ReplyStepHandler : IOutgoingStep
{
    public async Task Process(OutgoingStepContext context, Func<Task> next)
    {
        var message = context.Load<TransportMessage>();
        var replyContext = message.GetReplyToContext();
        if (replyContext is not null)
        {
            await replyContext.NatsReplyAsync(message);
        }
        else
        {
            await next();
        }
    }
}
