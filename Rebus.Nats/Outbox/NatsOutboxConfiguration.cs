using System;

namespace Rebus.Nats.Outbox;

/// <summary>Configuration settings for the NATS outbox implementation using JetStream.</summary>
public class NatsOutboxConfiguration
{
    internal bool CleanupEnabled { get; private set; } = true;
    internal bool ForwardingEnabled { get; private set; } = true;
    internal bool OrphanedForwardingEnabled { get; private set; } = true;

    internal int TrimSize { get; private set; } = 10000;
    internal TimeSpan CleanupInterval { get; private set; } = TimeSpan.FromMinutes(1);
    internal TimeSpan ForwardingInterval { get; private set; } = TimeSpan.FromMilliseconds(100);
    internal TimeSpan OrphanedForwardingInterval { get; private set; } = TimeSpan.FromSeconds(1);
    internal string? StreamName { get; private set; }
    internal string? ConsumerGroupName { get; private set; } = "rebus-outbox";
    internal string? ConsumerName { get; private set; } = $"rebus-{Environment.MachineName}-{Guid.NewGuid():N}";
    internal TimeSpan IdleConsumerTimeout { get; private set; } = TimeSpan.FromHours(1);
    internal int MessageBatchSize { get; private set; } = 100;
    internal TimeSpan OrphanedMessageTimeout { get; private set; } = TimeSpan.FromMinutes(5);

    /// <summary>Specifies the name of the JetStream stream used for outbox storage.</summary>
    public NatsOutboxConfiguration UseStreamName(string streamName)
    {
        if (string.IsNullOrWhiteSpace(streamName))
        {
            throw new ArgumentException("Stream name cannot be null or whitespace.", nameof(streamName));
        }

        StreamName = streamName;
        return this;
    }

    /// <summary>Sets a consumer group name for the outbox consumer, defaults to "rebus-outbox".</summary>
    public NatsOutboxConfiguration UseConsumerGroupName(string consumerGroupName)
    {
        if (string.IsNullOrWhiteSpace(consumerGroupName))
        {
            throw new ArgumentException("Consumer group name cannot be null or whitespace.", nameof(consumerGroupName));
        }

        ConsumerGroupName = consumerGroupName;
        return this;
    }

    /// <summary>Sets a consumer name for the outbox consumer. If not set, a unique name will be generated based on machine name and GUID.</summary>
    public NatsOutboxConfiguration UseConsumerName(string consumerName)
    {
        if (string.IsNullOrWhiteSpace(consumerName))
        {
            throw new ArgumentException("Consumer name cannot be null or whitespace.", nameof(consumerName));
        }

        ConsumerName = consumerName;
        return this;
    }

    /// <summary>Sets the minimum time before an idle consumer will be removed. Defaults to 1 hour. Must be at least 1 second.</summary>
    public NatsOutboxConfiguration SetIdleConsumerTimeout(TimeSpan idleConsumerTimeout)
    {
        if (idleConsumerTimeout < TimeSpan.FromSeconds(1))
        {
            throw new ArgumentOutOfRangeException(nameof(idleConsumerTimeout));
        }

        IdleConsumerTimeout = idleConsumerTimeout;
        return this;
    }

    /// <summary>Sets the interval between cleanup runs. Defaults to 1 minute. Must be at least 1 second.</summary>
    public NatsOutboxConfiguration SetCleanupInterval(TimeSpan cleanupInterval)
    {
        if (cleanupInterval < TimeSpan.FromSeconds(1))
        {
            throw new ArgumentOutOfRangeException(nameof(cleanupInterval));
        }

        CleanupInterval = cleanupInterval;
        return this;
    }

    /// <summary>Sets the polling interval to check for new messages. Defaults to 100 milliseconds. Must be at least 1 millisecond.</summary>
    public NatsOutboxConfiguration SetForwardingInterval(TimeSpan forwardingInterval)
    {
        if (forwardingInterval < TimeSpan.FromMilliseconds(1))
        {
            throw new ArgumentOutOfRangeException(nameof(forwardingInterval));
        }

        ForwardingInterval = forwardingInterval;
        return this;
    }

    /// <summary>Sets the interval for checking orphaned messages. Defaults to 1 second. Must be at least 1 second.</summary>
    public NatsOutboxConfiguration SetOrphanedForwardingInterval(TimeSpan orphanedForwardingInterval)
    {
        if (orphanedForwardingInterval < TimeSpan.FromSeconds(1))
        {
            throw new ArgumentOutOfRangeException(nameof(orphanedForwardingInterval));
        }

        OrphanedForwardingInterval = orphanedForwardingInterval;
        return this;
    }

    /// <summary>Sets the number of messages to trim the stream to. Defaults to 10000.</summary>
    public NatsOutboxConfiguration SetTrimSize(int trimSize)
    {
        if (trimSize < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(trimSize));
        }

        TrimSize = trimSize;
        return this;
    }

    /// <summary>Sets the number of messages to read in a single batch. Defaults to 100.</summary>
    public NatsOutboxConfiguration SetMessageBatchSize(int messageBatchSize)
    {
        if (messageBatchSize < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(messageBatchSize));
        }

        MessageBatchSize = messageBatchSize;
        return this;
    }

    /// <summary>Sets the time before a message is considered orphaned. Defaults to 5 minutes, must be at least 1 second.</summary>
    public NatsOutboxConfiguration SetOrphanedMessageTimeout(TimeSpan orphanedMessageTimeout)
    {
        if (orphanedMessageTimeout < TimeSpan.FromSeconds(1))
        {
            throw new ArgumentOutOfRangeException(nameof(orphanedMessageTimeout));
        }

        OrphanedMessageTimeout = orphanedMessageTimeout;
        return this;
    }

    /// <summary>Enables (default) or disables the cleanup task.</summary>
    public NatsOutboxConfiguration EnableCleanup(bool enableCleanup = true)
    {
        CleanupEnabled = enableCleanup;
        return this;
    }

    /// <summary>Enables (default) or disables forwarding of outgoing messages.</summary>
    public NatsOutboxConfiguration EnableForwarding(bool enableForwarding = true)
    {
        ForwardingEnabled = enableForwarding;
        return this;
    }

    /// <summary>Enables (default) or disables forwarding of orphaned messages.</summary>
    public NatsOutboxConfiguration EnableOrphanedForwarding(bool enableOrphanedForwarding = true)
    {
        OrphanedForwardingEnabled = enableOrphanedForwarding;
        return this;
    }
}
