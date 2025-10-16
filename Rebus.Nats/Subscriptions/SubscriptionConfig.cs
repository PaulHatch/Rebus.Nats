using System;
using Rebus.Config;
using Rebus.Subscriptions;

namespace Rebus.Nats.Subscriptions;

/// <summary>Configuration extensions for storing subscription data in NATS Key-Value stores.</summary>
public static class SubscriptionConfig
{
    /// <summary>Configures Rebus to store subscription data in a NATS Key-Value store.</summary>
    /// <param name="configurer">The subscription storage configurer.</param>
    /// <param name="bucketName">The name of the Key-Value bucket to use for subscription storage.</param>
    /// <param name="isCentralized">True if a single NATS instance is used for all buses.</param>
    public static void StoreInNats(
        this StandardConfigurer<ISubscriptionStorage> configurer,
        string bucketName = "rebus-subscriptions",
        bool isCentralized = true)
    {
        if (configurer == null)
        {
            throw new ArgumentNullException(nameof(configurer));
        }

        configurer.Register(r =>
        {
            var provider = r.Get<NatsProvider>();
            return new NatsSubscriptionStorage(provider, bucketName, isCentralized);
        });
    }
}
