using System;
using System.Threading;
using System.Threading.Tasks;
using Rebus.Messages;
using Rebus.Transport;

namespace Rebus.Nats.Outbox;

/// <summary>Decorator for ITransport that intercepts outgoing messages and stores them in the NATS outbox.</summary>
internal class OutboxClientTransportDecorator : ITransport
{
    private readonly IOutboxQueueStorage _outboxQueueStorage;
    private readonly ITransport _transport;

    public OutboxClientTransportDecorator(ITransport transport, IOutboxQueueStorage outboxQueueStorage)
    {
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));
        _outboxQueueStorage = outboxQueueStorage ?? throw new ArgumentNullException(nameof(outboxQueueStorage));
    }

    public void CreateQueue(string address)
    {
        _transport.CreateQueue(address);
    }

    public Task Send(string destinationAddress, TransportMessage message, ITransactionContext context)
    {
        var natsTransaction = context.GetOrNull<NatsTransactionContext>(NatsProvider.CurrentOutboxConnectionKey);

        return natsTransaction == null
            ? _transport.Send(destinationAddress, message, context)
            : _outboxQueueStorage.Save(new OutgoingTransportMessage(message, destinationAddress), natsTransaction);
    }

    public Task<TransportMessage> Receive(ITransactionContext context, CancellationToken cancellationToken)
    {
        return _transport.Receive(context, cancellationToken);
    }

    public string Address => _transport.Address;
}
