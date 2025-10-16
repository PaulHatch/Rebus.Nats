using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Rebus.Bus;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Nats.Outbox;
using Rebus.Transport;

namespace Rebus.Nats.Transport;

/// <summary>
/// NATS JetStream transport implementation for Rebus, providing durable message delivery using NATS JetStream
/// streams and consumers with support for acknowledgment, retries, and message expiration.
/// </summary>
public class NatsTransport : AbstractRebusTransport, IInitializable, IDisposable
{
    private static readonly RetryUtility _sendRetryUtility = new([
        TimeSpan.FromMilliseconds(100),
        TimeSpan.FromMilliseconds(200),
        TimeSpan.FromMilliseconds(500)
    ]);

    private readonly NatsProvider _natsProvider;
    private readonly NatsTransportOptions _options;
    private readonly ILog _log;
    private readonly string? _inputQueueName;

    private INatsJSStream? _stream;
    private INatsJSConsumer? _consumer;
    private readonly SemaphoreSlim _initializationLock = new(1, 1);
    private bool _initialized;

    /// <summary>Creates a new instance of the <see cref="NatsTransport"/> class.</summary>
    /// <param name="natsProvider">The NATS provider for connection management.</param>
    /// <param name="inputQueueName">The name of the input queue (null for one-way client).</param>
    /// <param name="loggerFactory">The logger factory for creating loggers.</param>
    /// <param name="options">Configuration options for the transport.</param>
    public NatsTransport(
        NatsProvider natsProvider,
        string? inputQueueName,
        IRebusLoggerFactory loggerFactory,
        NatsTransportOptions options)
        : base(inputQueueName)
    {
        _natsProvider = natsProvider ?? throw new ArgumentNullException(nameof(natsProvider));
        _inputQueueName = inputQueueName;
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _log = loggerFactory.GetLogger<NatsTransport>() ?? throw new ArgumentNullException(nameof(loggerFactory));
    }

    /// <summary>Initializes the transport by creating the stream and consumer infrastructure.</summary>
    public void Initialize()
    {
        if (_initialized)
        {
            return;
        }

        _initializationLock.Wait();
        try
        {
            if (_initialized)
            {
                return;
            }

            _log.Info("Initializing NATS transport with stream: {StreamName}", _options.StreamName);

            // Create the main transport stream
            var streamConfig = new StreamConfig(
                name: _options.StreamName,
                subjects: [$"{_options.SubjectPrefix}.>", $"{_options.TopicSubjectPrefix}.>"])
            {
                Storage = _options.Storage,
                MaxAge = _options.MaxAge,
                Retention = _options.Retention,
                NumReplicas = _options.Replicas
            };

            if (_options.MaxMessages > 0)
            {
                streamConfig.MaxMsgs = _options.MaxMessages;
            }

            if (_options.MaxBytes > 0)
            {
                streamConfig.MaxBytes = _options.MaxBytes;
            }

            _stream = _natsProvider.JetStream.CreateStreamAsync(streamConfig)
                .ConfigureAwait(false).GetAwaiter().GetResult();

            _log.Info("Created/verified stream: {StreamName}", _options.StreamName);

            // Create consumer for input queue if this is not a one-way client
            if (_inputQueueName != null)
            {
                CreateQueue(_inputQueueName);
            }

            _initialized = true;
        }
        catch (Exception ex)
        {
            _log.Error(ex, "Failed to initialize NATS transport");
            throw;
        }
        finally
        {
            _initializationLock.Release();
        }
    }

    /// <summary>Creates a queue (consumer) for the specified address.</summary>
    /// <param name="address">The queue address/name.</param>
    public override void CreateQueue(string address)
    {
        if (string.IsNullOrWhiteSpace(address))
        {
            throw new ArgumentException("Address cannot be null or whitespace", nameof(address));
        }

        try
        {
            _log.Info("Creating queue (consumer) for address: {Address}", address);

            var consumerName = GetConsumerName(address);
            var filterSubject = GetSubjectForAddress(address);

            var consumerConfig = new ConsumerConfig(consumerName)
            {
                AckPolicy = _options.AckPolicy,
                AckWait = _options.AckWait,
                MaxDeliver = _options.MaxDeliver,
                FilterSubject = filterSubject,
                DeliverPolicy = ConsumerConfigDeliverPolicy.All
            };

            if (_stream == null)
            {
                throw new InvalidOperationException("Stream not initialized. Call Initialize() first.");
            }

            var consumer = _stream.CreateOrUpdateConsumerAsync(consumerConfig)
                .ConfigureAwait(false).GetAwaiter().GetResult();

            // If this is our input queue, save the consumer reference
            if (address == _inputQueueName)
            {
                _consumer = consumer;
            }

            _log.Info("Created/updated consumer: {ConsumerName} for subject: {Subject}", consumerName, filterSubject);
        }
        catch (Exception ex)
        {
            _log.Error(ex, "Failed to create queue for address: {Address}", address);
            throw;
        }
    }

    /// <summary>Receives the next available message from the input queue.</summary>
    /// <param name="context">The transaction context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The received transport message, or null if no message is available.</returns>
    public override async Task<TransportMessage?> Receive(
        ITransactionContext context,
        CancellationToken cancellationToken)
    {
        if (_consumer == null)
        {
            throw new InvalidOperationException("Consumer not initialized. This is not a valid receiving endpoint.");
        }

        try
        {
            var fetchOpts = new NatsJSFetchOpts
            {
                MaxMsgs = 1,
                Expires = _options.FetchExpires
            };

            await foreach (var msg in _consumer.FetchAsync<byte[]>(opts: fetchOpts, cancellationToken: cancellationToken))
            {
                if (msg.Data == null)
                {
                    continue;
                }

                // Extract headers
                var headers = ExtractRebusHeaders(msg.Headers);

                // Check for message expiration
                if (IsMessageExpired(headers))
                {
                    var messageId = headers.TryGetValue(Headers.MessageId, out var id) ? id : "unknown";
                    _log.Debug("Message {MessageId} has expired, acknowledging and skipping", messageId);

                    await msg.AckAsync(cancellationToken: cancellationToken);
                    continue;
                }

                // Add delivery count from JetStream metadata
                // Note: NATS JetStream tracks delivery attempts automatically
                // We'll extract this from metadata when available
                // For now, we omit delivery count - it can be added later when we verify the API
                // TODO: Add delivery count tracking once NATS client API is verified

                // Register acknowledgment callbacks
                context.OnAck(async _ =>
                {
                    await msg.AckAsync(cancellationToken: cancellationToken);
                    var messageId = headers.TryGetValue(Headers.MessageId, out var id) ? id : "unknown";
                    _log.Debug("Message acknowledged: {MessageId}", messageId);
                });

                context.OnNack(async _ =>
                {
                    await msg.NakAsync(cancellationToken: cancellationToken);
                    var messageId = headers.TryGetValue(Headers.MessageId, out var id) ? id : "unknown";
                    _log.Debug("Message negatively acknowledged for redelivery: {MessageId}", messageId);
                });

                return new TransportMessage(headers, msg.Data);
            }

            // No messages available
            return null;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Expected during shutdown
            return null;
        }
        catch (Exception ex)
        {
            _log.Error(ex, "Error receiving message from NATS");
            throw;
        }
    }

    /// <summary>Sends outgoing messages to their destinations.</summary>
    /// <param name="outgoingMessages">The messages to send.</param>
    /// <param name="context">The transaction context.</param>
    protected override async Task SendOutgoingMessages(
        IEnumerable<OutgoingTransportMessage> outgoingMessages,
        ITransactionContext context)
    {
        // Check if outbox is active for this transaction
        var natsTransaction = context.GetOrNull<NatsTransactionContext>(NatsProvider.CurrentOutboxConnectionKey);

        if (natsTransaction != null)
        {
            // Outbox is active - messages are already queued by OutboxClientTransportDecorator
            // The outbox will forward them later, so we skip sending here
            _log.Debug("Outbox active, messages will be forwarded by outbox");
            return;
        }

        // Normal send path - no outbox active
        foreach (var outgoingMessage in outgoingMessages)
        {
            try
            {
                var destinationAddress = outgoingMessage.DestinationAddress;
                var transportMessage = outgoingMessage.TransportMessage;

                // Determine subject based on destination
                var subject = GetSubjectForAddress(destinationAddress);

                // Convert headers to NATS headers
                var natsHeaders = CreateNatsHeaders(transportMessage.Headers);

                // Check for message expiration
                if (transportMessage.Headers.TryGetValue(Headers.TimeToBeReceived, out var ttlString))
                {
                    // Parse the time-to-be-received header
                    if (TimeSpan.TryParse(ttlString, out var ttl))
                    {
                        // Note: NATS doesn't support per-message TTL directly
                        // We can use the Nats-Ttl header which some implementations respect
                        natsHeaders.Add("Nats-Ttl", ttl.TotalSeconds.ToString(CultureInfo.InvariantCulture));
                    }
                }

                // Add message ID for deduplication if available
                if (transportMessage.Headers.TryGetValue(Headers.MessageId, out var messageId))
                {
                    natsHeaders.Add("Nats-Msg-Id", messageId);
                }

                // Publish the message with retry logic for transient failures
                await _sendRetryUtility.ExecuteAsync(async () =>
                {
                    await _natsProvider.JetStream.PublishAsync(
                        subject: subject,
                        data: transportMessage.Body,
                        headers: natsHeaders);
                });

                _log.Debug("Published message to subject: {Subject}, MessageId: {MessageId}",
                    subject, messageId ?? "none");
            }
            catch (Exception ex)
            {
                _log.Error(ex, "Failed to send message to: {Destination}", outgoingMessage.DestinationAddress);
                throw;
            }
        }
    }

    /// <summary>Gets the NATS subject for a given address.</summary>
    private string GetSubjectForAddress(string address)
    {
        // Check if this looks like a topic (contains dots or wildcards)
        if (address.Contains(".") || address.Contains("*") || address.Contains(">"))
        {
            return $"{_options.TopicSubjectPrefix}.{address}";
        }

        return $"{_options.SubjectPrefix}.{address}";
    }

    /// <summary>Gets the consumer name for a given queue address.</summary>
    private string GetConsumerName(string address)
    {
        // Sanitize the address to create a valid consumer name
        return $"rebus-{address.Replace(".", "-").Replace("*", "").Replace(">", "")}";
    }

    /// <summary>Creates NATS headers from Rebus headers.</summary>
    private NatsHeaders CreateNatsHeaders(Dictionary<string, string> rebusHeaders)
    {
        var natsHeaders = new NatsHeaders();

        foreach (var kvp in rebusHeaders)
        {
            // NATS headers use string values
            natsHeaders.Add(kvp.Key, kvp.Value);
        }

        return natsHeaders;
    }

    /// <summary>Extracts Rebus headers from NATS headers.</summary>
    private Dictionary<string, string> ExtractRebusHeaders(NatsHeaders? natsHeaders)
    {
        var rebusHeaders = new Dictionary<string, string>();

        if (natsHeaders != null)
        {
            foreach (var key in natsHeaders.Keys)
            {
                // Get the first value for each header
                if (natsHeaders.TryGetValue(key, out var values) && values.Count > 0)
                {
                    rebusHeaders[key] = values[0];
                }
            }
        }

        return rebusHeaders;
    }

    /// <summary>Checks if a message has expired based on TimeToBeReceived and SentTime headers.</summary>
    private static bool IsMessageExpired(Dictionary<string, string> headers)
    {
        if (!headers.TryGetValue(Headers.TimeToBeReceived, out var timeToBeReceivedString))
        {
            return false;
        }

        if (!TimeSpan.TryParse(timeToBeReceivedString, out var timeToBeReceived))
        {
            return false;
        }

        if (!headers.TryGetValue(Headers.SentTime, out var sentTimeString))
        {
            return false;
        }

        if (!DateTimeOffset.TryParse(sentTimeString, out var sentTime))
        {
            return false;
        }

        var age = DateTimeOffset.UtcNow - sentTime.ToUniversalTime();
        return age > timeToBeReceived;
    }

    /// <summary>Disposes the transport and releases resources.</summary>
    public void Dispose()
    {
        _initializationLock.Dispose();
    }
}
