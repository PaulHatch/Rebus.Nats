using System;
using Rebus.Config;
using Rebus.Sagas;

namespace Rebus.Nats.Sagas;

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
            return new NatsSagaStorage(provider, bucketName);
        });
    }
}
