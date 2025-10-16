using System;
using NATS.Client.JetStream.Models;

namespace Rebus.Nats.Transport;

/// <summary>Configuration options for the NATS JetStream transport.</summary>
public class NatsTransportOptions
{
    /// <summary>Gets or sets the stream name for the transport. Default is "rebus-transport".</summary>
    public string StreamName { get; set; } = "rebus-transport";

    /// <summary>Gets or sets the maximum number of delivery attempts before moving to dead letter. Default is 5.</summary>
    public int MaxDeliver { get; set; } = 5;

    /// <summary>Gets or sets the acknowledgment wait time before redelivery. Default is 1 minute.</summary>
    public TimeSpan AckWait { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>Gets or sets the acknowledgment policy for consumers. Default is Explicit.</summary>
    public ConsumerConfigAckPolicy AckPolicy { get; set; } = ConsumerConfigAckPolicy.Explicit;

    /// <summary>Gets or sets the storage type for the stream. Default is File.</summary>
    public StreamConfigStorage Storage { get; set; } = StreamConfigStorage.File;

    /// <summary>Gets or sets the maximum age for messages in the stream. Default is 7 days.</summary>
    public TimeSpan MaxAge { get; set; } = TimeSpan.FromDays(7);

    /// <summary>Gets or sets the maximum number of messages in the stream. Default is -1 (unlimited).</summary>
    public long MaxMessages { get; set; } = -1;

    /// <summary>Gets or sets the maximum bytes for the stream. Default is -1 (unlimited).</summary>
    public long MaxBytes { get; set; } = -1;

    /// <summary>Gets or sets the retention policy for the stream. Default is WorkQueue.</summary>
    public StreamConfigRetention Retention { get; set; } = StreamConfigRetention.Workqueue;

    /// <summary>Gets or sets the subject prefix for transport messages. Default is "rebus.queue".</summary>
    public string SubjectPrefix { get; set; } = "rebus.queue";

    /// <summary>Gets or sets the subject prefix for topic messages. Default is "rebus.topic".</summary>
    public string TopicSubjectPrefix { get; set; } = "rebus.topic";

    /// <summary>Gets or sets the maximum number of messages to fetch in a single pull operation. Default is 100.</summary>
    public int FetchBatchSize { get; set; } = 100;

    /// <summary>Gets or sets the time to wait for messages when fetching. Default is 5 seconds.</summary>
    public TimeSpan FetchExpires { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Gets or sets the number of replicas for the stream. Default is 1.</summary>
    public int Replicas { get; set; } = 1;

    /// <summary>Gets or sets whether to use explicit consumer creation per queue. Default is true.</summary>
    public bool UseExplicitConsumers { get; set; } = true;
}
