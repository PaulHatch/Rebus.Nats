using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using Rebus.Bus;
using Rebus.Logging;
using Rebus.Messages;
using Rebus.Subscriptions;
using Rebus.Transport;

namespace Rebus.Nats.Transport;

/// <summary>
/// NATS JetStream transport implementation for Rebus, providing durable message delivery using NATS JetStream
/// streams and consumers with support for acknowledgment, retries, and message expiration.
/// </summary>
public class NatsTransport : AbstractRebusTransport, IInitializable, IDisposable, ISubscriptionStorage
{
    private const string QueueSubjectsegment = "queue";
    private const string TopicSubjectSegment = "topic";
    private const string TopicDestinationPrefix = "topic:";

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

            var streamConfig = new StreamConfig(
                name: _options.StreamName,
                subjects: [$"{_options.SubjectPrefix}.{QueueSubjectsegment}.>", $"{_options.SubjectPrefix}.{TopicSubjectSegment}.>"])
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

    /// <summary>Creates or updates a queue (consumer) for the specified address.</summary>
    /// <param name="address">The queue address/name.</param>
    public override void CreateQueue(string address)
    {
        if (string.IsNullOrWhiteSpace(address))
        {
            throw new ArgumentException("Address cannot be null or whitespace", nameof(address));
        }

        var existingTopics = GetCurrentTopicSubscriptionsAsync(address).ConfigureAwait(false).GetAwaiter().GetResult();
        CreateOrUpdateConsumerAsync(address, existingTopics).ConfigureAwait(false).GetAwaiter().GetResult();
    }

    private async Task<INatsJSConsumer> CreateOrUpdateConsumerAsync(string address, IReadOnlyList<string> topicSubjects)
    {
        _log.Info("Creating/updating consumer for address: {Address} with {TopicCount} topic subscriptions", address, topicSubjects.Count);

        var consumerName = GetConsumerNameForQueue(address);
        var queueSubject = GetSubjectForQueue(address);

        var allSubjects = new List<string> { queueSubject };
        allSubjects.AddRange(topicSubjects);

        var consumerConfig = new ConsumerConfig(consumerName)
        {
            AckPolicy = _options.AckPolicy,
            AckWait = _options.AckWait,
            MaxDeliver = _options.MaxDeliver,
            DeliverPolicy = ConsumerConfigDeliverPolicy.All,
            FilterSubjects = allSubjects
        };

        if (_stream == null)
        {
            throw new InvalidOperationException("Stream not initialized. Call Initialize() first.");
        }

        var consumer = await _stream.CreateOrUpdateConsumerAsync(consumerConfig);

        // If this is our input queue, save the consumer reference
        if (address == _inputQueueName)
        {
            _consumer = consumer;
        }

        _log.Info("Created/updated consumer: {ConsumerName} with subjects: [{Subjects}]",
            consumerName, string.Join(", ", allSubjects));

        return consumer;
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

                // Register acknowledgment callbacks
                context.OnAck(async _ =>
                {
                    await msg.AckAsync();
                    var messageId = headers.TryGetValue(Headers.MessageId, out var id) ? id : "unknown";
                    _log.Debug("Message acknowledged: {MessageId}", messageId);
                });

                context.OnNack(async _ =>
                {
                    await msg.NakAsync();
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
        foreach (var outgoingMessage in outgoingMessages)
        {
            try
            {
                var destinationAddress = outgoingMessage.DestinationAddress;
                var transportMessage = outgoingMessage.TransportMessage;

                var subject = destinationAddress.StartsWith(TopicDestinationPrefix, StringComparison.Ordinal)
                    ? GetSubjectForTopic(destinationAddress[TopicDestinationPrefix.Length..])
                    : GetSubjectForQueue(destinationAddress);

                var natsHeaders = CreateNatsHeaders(transportMessage.Headers);

                if (transportMessage.Headers.TryGetValue(Headers.TimeToBeReceived, out var ttlString))
                {
                    if (TimeSpan.TryParse(ttlString, out var ttl))
                    {
                        natsHeaders.Add("Nats-Ttl", ttl.TotalSeconds.ToString(CultureInfo.InvariantCulture));
                    }
                }

                if (transportMessage.Headers.TryGetValue(Headers.MessageId, out var messageId))
                {
                    natsHeaders.Add("Nats-Msg-Id", messageId);
                }

                await _natsProvider.JetStream.PublishAsync(
                    subject: subject,
                    data: transportMessage.Body,
                    headers: natsHeaders);

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

    private string GetSubjectForQueue(string address) => $"{_options.SubjectPrefix}.{QueueSubjectsegment}.{address}";

    private string GetSubjectForTopic(string topic) => $"{_options.SubjectPrefix}.{TopicSubjectSegment}.{SanitizeForSubject(topic)}";

    private string GetConsumerNameForQueue(string address) => $"rebus-queue-{SanitizeForConsumerName(address)}";

    private static string SanitizeForConsumerName(string value) => value.Replace(".", "-").Replace("*", "").Replace(">", "");

    private static string SanitizeForSubject(string value) => value.Replace(".", "-").Replace(" ", "-");

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

    /// <summary>Gets whether this transport is centralized (it always is, as NATS JetStream handles pub/sub routing).</summary>
    public bool IsCentralized => true;

    /// <summary>Gets the destination address for publishing to a topic.</summary>
    public Task<IReadOnlyList<string>> GetSubscriberAddresses(string topic)
    {
        var topicAddress = $"{TopicDestinationPrefix}{topic}";
        return Task.FromResult<IReadOnlyList<string>>([topicAddress]);
    }

    /// <summary>Registers a subscriber to a topic.</summary>
    public async Task RegisterSubscriber(string topic, string subscriberAddress)
    {
        if (_stream == null)
        {
            throw new InvalidOperationException("Stream not initialized. Call Initialize() first.");
        }

        _log.Info("Registering subscriber {QueueName} for topic {Topic}", subscriberAddress, topic);

        var topicSubject = GetSubjectForTopic(topic);
        var currentTopics = await GetCurrentTopicSubscriptionsAsync(subscriberAddress);

        if (!currentTopics.Contains(topicSubject))
        {
            currentTopics.Add(topicSubject);
            await CreateOrUpdateConsumerAsync(subscriberAddress, currentTopics);
        }
    }

    /// <summary>Unregisters a subscriber from a topic.</summary>
    public async Task UnregisterSubscriber(string topic, string subscriberAddress)
    {
        if (_stream == null)
        {
            throw new InvalidOperationException("Stream not initialized. Call Initialize() first.");
        }

        _log.Info("Unregistering subscriber {QueueName} from topic {Topic}", subscriberAddress, topic);

        var topicSubject = GetSubjectForTopic(topic);
        var currentTopics = await GetCurrentTopicSubscriptionsAsync(subscriberAddress);

        if (currentTopics.Remove(topicSubject))
        {
            await CreateOrUpdateConsumerAsync(subscriberAddress, currentTopics);
        }
    }

    private async Task<List<string>> GetCurrentTopicSubscriptionsAsync(string queueAddress)
    {
        if (_stream == null)
        {
            throw new InvalidOperationException("Stream not initialized. Call Initialize() first.");
        }

        var consumerName = GetConsumerNameForQueue(queueAddress);
        var queueSubject = GetSubjectForQueue(queueAddress);
        var topicPrefix = $"{_options.SubjectPrefix}.{TopicSubjectSegment}.";

        try
        {
            var consumer = await _stream.GetConsumerAsync(consumerName);
            var filterSubjects = consumer.Info.Config.FilterSubjects ?? [];

            // Return only topic subjects (exclude the queue's own subject)
            return filterSubjects
                .Where(s => s.StartsWith(topicPrefix, StringComparison.Ordinal))
                .ToList();
        }
        catch (NatsJSApiException ex) when (ex.Error.Code == 404)
        {
            // Consumer doesn't exist yet - no subscriptions
            return [];
        }
    }
}
