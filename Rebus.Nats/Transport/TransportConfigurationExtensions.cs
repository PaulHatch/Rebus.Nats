using System;
using Rebus.Config;
using Rebus.Logging;
using Rebus.Subscriptions;
using Rebus.Transport;

namespace Rebus.Nats.Transport;

/// <summary>Configuration extensions for the NATS JetStream transport.</summary>
public static class TransportConfigurationExtensions
{
    /// <summary>
    /// Configures Rebus to use NATS JetStream as the message transport, enabling durable message delivery
    /// with support for acknowledgments, retries, and message expiration.
    /// </summary>
    /// <param name="configurer">The transport configurer to extend.</param>
    /// <param name="inputQueueName">
    /// The name of the input queue for this endpoint. Use null for one-way clients that only send messages.
    /// </param>
    /// <param name="configure">
    /// Optional configuration delegate to customize NATS transport settings such as stream name, retry policies,
    /// and acknowledgment timeouts.
    /// </param>
    /// <exception cref="ArgumentNullException">Thrown when configurer is null.</exception>
    /// <remarks>
    /// This method must be called after EnableNats() has been called on the OptionsConfigurer to establish
    /// the NATS connection. The transport will use the shared NatsProvider from the dependency injection
    /// container.
    ///
    /// The NATS transport has native support for pub/sub messaging, so it will automatically be registered
    /// as the subscription storage implementation.
    ///
    /// Example usage:
    /// <code>
    /// Configure.With(activator)
    ///     .Options(o => o.EnableNats("nats://localhost:4222"))
    ///     .Transport(t => t.UseNatsJetStream("my-queue", opt =>
    ///     {
    ///         opt.MaxDeliver = 5;
    ///         opt.StreamName = "rebus-transport";
    ///         opt.AckWait = TimeSpan.FromMinutes(2);
    ///     }))
    ///     .Start();
    /// </code>
    /// </remarks>
    public static void UseNatsJetStream(
        this StandardConfigurer<ITransport> configurer,
        string? inputQueueName,
        Action<NatsTransportOptions>? configure = null)
    {
        if (configurer == null)
        {
            throw new ArgumentNullException(nameof(configurer));
        }

        const string natsSubText = "The NATS transport was inserted as the subscriptions storage because it has native support for pub/sub messaging";

        configurer.Register(context =>
        {
            var natsProvider = context.Get<NatsProvider>();
            var loggerFactory = context.Get<IRebusLoggerFactory>();

            var options = new NatsTransportOptions();
            configure?.Invoke(options);

            var transport = new NatsTransport(
                natsProvider,
                inputQueueName,
                loggerFactory,
                options);

            return transport;
        });

        configurer
            .OtherService<ISubscriptionStorage>()
            .Register(c => (ISubscriptionStorage)c.Get<ITransport>(), description: natsSubText);
    }

    /// <summary>
    /// Configures Rebus to use NATS JetStream as a one-way transport client that only sends messages
    /// without receiving them.
    /// </summary>
    /// <param name="configurer">The transport configurer to extend.</param>
    /// <param name="configure">
    /// Optional configuration delegate to customize NATS transport settings such as stream name and
    /// subject prefixes.
    /// </param>
    /// <remarks>
    /// This is a convenience method for creating a one-way client. It is equivalent to calling
    /// UseNatsJetStream with inputQueueName set to null.
    /// </remarks>
    public static void UseNatsJetStreamAsOneWayClient(
        this StandardConfigurer<ITransport> configurer,
        Action<NatsTransportOptions>? configure = null)
    {
        UseNatsJetStream(configurer, null, configure);
    }
}
