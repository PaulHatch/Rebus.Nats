using System;
using Rebus.Logging;
using Rebus.Nats;
using Rebus.Nats.Sagas;
using Rebus.Sagas;

// ReSharper disable once CheckNamespace
namespace Rebus.Config;

/// <summary>Configuration extensions for storing saga data in NATS Key-Value stores.</summary>
public static class SagaConfig
{
    /// <summary>Configures Rebus to store saga data in a NATS Key-Value store.</summary>
    /// <param name="configurer">The saga storage configurer.</param>
    /// <param name="bucketName">The name of the Key-Value bucket to use for saga storage.</param>
    public static void StoreInNats(this StandardConfigurer<ISagaStorage> configurer, string bucketName = "rebus-sagas")
    {
        if (configurer == null)
        {
            throw new ArgumentNullException(nameof(configurer));
        }

        configurer.Register(r =>
        {
            var provider = r.Get<NatsProvider>();
            var loggerFactory = r.Get<IRebusLoggerFactory>();
            return new NatsSagaStorage(provider, bucketName, loggerFactory);
        });
    }
}
