using System;
using Rebus.Config;
using Rebus.Exceptions;

namespace Rebus.Nats.Outbox;

/// <summary>Configuration extensions for storing outbox messages in NATS JetStream.</summary>
public static class OutboxExtensions
{
    /// <summary>Configures Rebus to use NATS JetStream for transactional outbox storage.</summary>
    /// <param name="configurer">The outbox storage configurer.</param>
    /// <param name="configure">Optional configuration delegate to customize outbox settings.</param>
    public static void StoreInNats(this StandardConfigurer<IOutboxStorage> configurer, Action<NatsOutboxConfiguration>? configure = null)
    {
        if (configurer == null)
        {
            throw new ArgumentNullException(nameof(configurer));
        }

        var outboxConfig = new NatsOutboxConfiguration();
        configure?.Invoke(outboxConfig);

        configurer.OtherService<NatsOutboxConfiguration>()
            .Register(_ => outboxConfig);

        configurer.OtherService<IOutboxStorage>()
            .Register(r =>
            {
                var config = r.Get<NatsOutboxConfiguration>();
                var options = r.Get<Options>();

                if (config.StreamName == null)
                {
                    var derivedStreamName = options.OptionalBusName.AddSuffix() ??
                                           throw new RebusConfigurationException(
                                               "Either a stream name or a bus name must be specified");
                    config.UseStreamName(derivedStreamName);
                }

                return new NatsOutboxStorage(
                    r.Get<NatsProvider>(),
                    config,
                    r.Get<Logging.IRebusLoggerFactory>());
            }, "NATS Outbox Storage");

        configurer.OtherService<IOutboxQueueStorage>()
            .Register(r =>
            {
                var config = r.Get<NatsOutboxConfiguration>();
                var options = r.Get<Options>();
                var streamName = config.StreamName ??
                                 options.OptionalBusName.AddSuffix() ??
                                 throw new RebusConfigurationException(
                                     "Either a stream name or a bus name must be specified");

                return new NatsOutboxQueueStorage(streamName);
            });
    }

    private static string? AddSuffix(this string? name)
    {
        return name is null ? null : $"{name}-outbox";
    }
}
