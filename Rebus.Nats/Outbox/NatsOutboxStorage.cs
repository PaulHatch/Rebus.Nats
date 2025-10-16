using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Rebus.Bus;
using Rebus.Logging;
using Rebus.Transport;

namespace Rebus.Nats.Outbox;

/// <summary>
/// Implements the transactional outbox pattern using NATS JetStream, ensuring reliable message delivery
/// by persisting messages before committing transactions and forwarding them asynchronously.
/// </summary>
internal class NatsOutboxStorage : IOutboxStorage, IInitializable, IDisposable
{
    private readonly NatsOutboxConfiguration _config;
    private readonly NatsProvider _natsProvider;
    private readonly ILog _log;
    private readonly Dictionary<ulong, NatsJSMsg<byte[]>> _pendingMessages = new();
    private INatsJSStream? _stream;
    private INatsJSConsumer? _consumer;

    public NatsOutboxStorage(
        NatsProvider natsProvider,
        NatsOutboxConfiguration config,
        IRebusLoggerFactory loggerFactory)
    {
        _natsProvider = natsProvider;
        _config = config;
        _log = loggerFactory.GetLogger<NatsOutboxStorage>();
    }

    public void Dispose()
    {
    }

    public void Initialize()
    {
        var streamConfig = new StreamConfig(_config.StreamName!, [$"{_config.StreamName}.>"])
        {
            Storage = StreamConfigStorage.File,
            MaxAge = TimeSpan.FromDays(7)
        };

        _stream = _natsProvider.JetStream.CreateStreamAsync(streamConfig).ConfigureAwait(false).GetAwaiter().GetResult();

        var consumerConfig = new ConsumerConfig(_config.ConsumerGroupName!)
        {
            AckPolicy = ConsumerConfigAckPolicy.Explicit,
            AckWait = _config.OrphanedMessageTimeout,
            MaxDeliver = 10
        };

        _consumer = _stream.CreateOrUpdateConsumerAsync(consumerConfig).ConfigureAwait(false).GetAwaiter().GetResult();
        _log.Info("Created consumer {ConsumerName} for stream {StreamName}", _config.ConsumerName, _config.StreamName);
    }

    public async Task TrimQueue()
    {
        if (_stream != null)
        {
            await _stream.PurgeAsync(new StreamPurgeRequest { Keep = (ulong)_config.TrimSize });
        }
    }

    public async Task CleanupIdleConsumers()
    {
        await Task.CompletedTask;
    }

    public async Task<IEnumerable<OutboxMessage>?> GetOrphanedMessageBatch()
    {
        return await GetNextMessageBatch();
    }

    public async Task<IEnumerable<OutboxMessage>?> GetNextMessageBatch()
    {
        var messages = new List<OutboxMessage>();
        var fetchOpts = new NatsJSFetchOpts
        {
            MaxMsgs = _config.MessageBatchSize,
            Expires = _config.ForwardingInterval
        };

        await foreach (var msg in _consumer!.FetchAsync<byte[]>(opts: fetchOpts))
        {
            var outboxMessage = DeserializeMessage(msg);
            if (outboxMessage != null)
            {
                _pendingMessages[outboxMessage.Sequence] = msg;
                messages.Add(outboxMessage);
            }
        }

        return messages.Count > 0 ? messages : null;
    }

    public async Task MarkAsDispatched(OutboxMessage message)
    {
        if (_pendingMessages.TryGetValue(message.Sequence, out var natsMsg))
        {
            await natsMsg.AckAsync();
            _pendingMessages.Remove(message.Sequence);
        }
    }

    private OutboxMessage? DeserializeMessage(NatsJSMsg<byte[]> msg)
    {
        try
        {
            var json = Encoding.UTF8.GetString(msg.Data ?? []);
            var envelope = JsonSerializer.Deserialize<MessageEnvelope>(json);
            if (envelope == null)
            {
                return null;
            }

            var outboxMessage = new OutboxMessage(msg.Metadata?.Sequence.Stream ?? 0)
            {
                DestinationAddress = envelope.DestinationAddress,
                Body = envelope.Body
            };

            foreach (var header in envelope.Headers ?? [])
            {
                outboxMessage.Headers[header.Key] = header.Value;
            }

            return outboxMessage;
        }
        catch (Exception ex)
        {
            _log.Error(ex, "Failed to deserialize outbox message");
            return null;
        }
    }
}

/// <summary>Internal implementation for queuing messages to the NATS outbox stream.</summary>
internal class NatsOutboxQueueStorage : IOutboxQueueStorage
{
    private readonly string _streamName;

    public NatsOutboxQueueStorage(string streamName)
    {
        _streamName = streamName;
    }

    public Task Save(OutgoingTransportMessage message, NatsTransactionContext transaction)
    {
        var envelope = new MessageEnvelope
        {
            DestinationAddress = message.DestinationAddress,
            Headers = message.TransportMessage.Headers,
            Body = message.TransportMessage.Body
        };

        var json = JsonSerializer.Serialize(envelope);
        var data = Encoding.UTF8.GetBytes(json);

        transaction.AddOperation(async () =>
        {
            await transaction.JetStream.PublishAsync($"{_streamName}.msg", data);
        });

        return Task.CompletedTask;
    }
}

internal record MessageEnvelope
{
    public string? DestinationAddress { get; init; }
    public Dictionary<string, string>? Headers { get; init; }
    public byte[]? Body { get; init; }
}
