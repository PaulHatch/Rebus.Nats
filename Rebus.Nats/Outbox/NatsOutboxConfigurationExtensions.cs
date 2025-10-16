using System;
using Rebus.Logging;
using Rebus.Pipeline;
using Rebus.Retry.Simple;
using Rebus.Threading;
using Rebus.Transport;

// ReSharper disable once CheckNamespace
namespace Rebus.Config;

/// <summary>Configuration extensions for the NATS outbox support.</summary>
public static class NatsOutboxConfigurationExtensions
{
    /// <summary>
    /// Configures Rebus to use a NATS outbox.
    /// This will store a (message ID, source queue) tuple for all processed messages, and under this tuple any messages
    /// sent/published will also be stored in NATS JetStream, thus enabling truly idempotent message processing.
    /// </summary>
    public static RebusConfigurer NatsOutbox(this RebusConfigurer configurer,
        Action<StandardConfigurer<Nats.Outbox.IOutboxStorage>> configure)
    {
        if (configurer == null)
        {
            throw new ArgumentNullException(nameof(configurer));
        }

        if (configure == null)
        {
            throw new ArgumentNullException(nameof(configure));
        }

        configurer.Options(o =>
        {
            configure(StandardConfigurer<Nats.Outbox.IOutboxStorage>.GetConfigurerFrom(o));

            if (!o.Has<Nats.Outbox.IOutboxStorage>())
            {
                return;
            }

            o.Decorate<ITransport>(
                c => new Nats.Outbox.OutboxClientTransportDecorator(c.Get<ITransport>(), c.Get<Nats.Outbox.IOutboxQueueStorage>()));

            o.Register(c =>
            {
                var asyncTaskFactory = c.Get<IAsyncTaskFactory>();
                var rebusLoggerFactory = c.Get<IRebusLoggerFactory>();
                var outboxStorage = c.Get<Nats.Outbox.IOutboxStorage>();
                var transport = c.Get<ITransport>();
                var config = c.Get<Nats.Outbox.NatsOutboxConfiguration>();

                return new Nats.Outbox.OutboxForwarder(
                    asyncTaskFactory,
                    rebusLoggerFactory,
                    outboxStorage,
                    transport,
                    config);
            });

            o.Decorate(c =>
            {
                _ = c.Get<Nats.Outbox.OutboxForwarder>();
                return c.Get<Options>();
            });

            o.Decorate<IPipeline>(c =>
            {
                var pipeline = c.Get<IPipeline>();
                var natsProvider = c.Get<Nats.NatsProvider>();
                var step = new Nats.Outbox.OutboxIncomingStep(natsProvider);
                return new PipelineStepInjector(pipeline)
                    .OnReceive(step, PipelineRelativePosition.After, typeof(DefaultRetryStep));
            });
        });

        return configurer;
    }
}
