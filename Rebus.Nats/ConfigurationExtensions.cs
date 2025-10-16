using System;
using NATS.Client.Core;
using NATS.Client.JetStream;
using Rebus.Config;
using Rebus.Exceptions;
using Rebus.Nats.Async;
using Rebus.Pipeline;
using Rebus.Pipeline.Send;

namespace Rebus.Nats;

/// <summary>Configuration helper for adding NATS support.</summary>
public static class ConfigurationExtensions
{
    /// <summary>
    /// Configures Rebus to use NATS for asynchronous request-reply messaging, enabling distributed communication across service boundaries.
    /// </summary>
    /// <param name="configurer">The Rebus options configurer to extend with NATS support.</param>
    /// <param name="url">The NATS server URL for establishing connections to the NATS server.</param>
    /// <param name="configure">Optional configuration delegate to customize NATS-specific settings such as async modes.</param>
    public static void EnableNats(
        this OptionsConfigurer configurer,
        string url,
        Action<RebusNatsConfig>? configure = null)
    {
        if (configurer == null)
        {
            throw new ArgumentNullException(nameof(configurer));
        }

        var config = new RebusNatsConfig();
        (configure ?? DefaultConfig).Invoke(config);

        try
        {
            var opts = new NatsOpts
            {
                Url = url,
                Name = "rebus-nats"
            };

            var connection = new NatsConnection(opts);
            var jetStream = new NatsJSContext(connection);

            configurer.Register<INatsConnection>(_ => connection, "nats-connection");
            configurer.Register<INatsJSContext>(_ => jetStream, "nats-jetstream");
        }
        catch (Exception e)
        {
            throw new RebusConfigurationException(e, "Could not connect to NATS");
        }

        configurer.Register<NatsProvider>(r =>
            new NatsProvider(
                r.Get<INatsConnection>(),
                r.Get<INatsJSContext>()),
            "Connection Provider");

        configurer.Register(r =>
                new NatsConnectionInitializer(r.Get<INatsConnection>()),
            "NATS connection initializer");

        configurer.Decorate<IPipeline>(r =>
        {
            r.Get<NatsConnectionInitializer>();
            return r.Get<IPipeline>();
        }, "Ensure NATS connection is established");

        if (config.HostAsyncEnabled)
        {
            configurer.Register(_ =>
            {
                var step = new ReplyStepHandler();
                return step;
            });

            configurer.Decorate<IPipeline>(c =>
            {
                var pipeline = c.Get<IPipeline>();
                var step = c.Get<ReplyStepHandler>();
                return new PipelineStepInjector(pipeline)
                    .OnSend(step, PipelineRelativePosition.Before, typeof(SendOutgoingMessageStep));
            });

            configurer.Register(r =>
                    new HostInitializer(r.Get<INatsConnection>()),
                "Host initializer");

            configurer.Decorate<IPipeline>(r =>
            {
                r.Get<HostInitializer>();
                return r.Get<IPipeline>();
            }, "Trigger host initialization");
        }

        if (config.ClientAsyncEnabled)
        {
            configurer.Register(r =>
                    new ClientInitializer(r.Get<INatsConnection>()),
                "Client initializer");

            configurer.Decorate<IPipeline>(r =>
            {
                r.Get<ClientInitializer>();
                return r.Get<IPipeline>();
            }, "Trigger client initialization");
        }
    }

    private static void DefaultConfig(RebusNatsConfig config)
    {
        config.EnableAsync();
    }
}
