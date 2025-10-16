using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NATS.Client.KeyValueStore;
using Rebus.Subscriptions;

namespace Rebus.Nats.Subscriptions;

/// <summary>Stores subscriptions in NATS Key-Value stores.</summary>
public class NatsSubscriptionStorage : ISubscriptionStorage
{
    private readonly NatsProvider _natsProvider;
    private readonly string _bucketName;
    private readonly bool _isCentralized;
    private INatsKVStore? _kvStore;

    /// <summary>Creates a new instance of the NATS subscription storage.</summary>
    /// <param name="natsProvider">NATS provider to use.</param>
    /// <param name="bucketName">Key-Value bucket name.</param>
    /// <param name="isCentralized">True if a single NATS instance is used for all buses.</param>
    public NatsSubscriptionStorage(
        NatsProvider natsProvider,
        string bucketName,
        bool isCentralized)
    {
        _natsProvider = natsProvider;
        _bucketName = bucketName;
        _isCentralized = isCentralized;
    }

    private async Task<INatsKVStore> GetOrCreateKVStore()
    {
        if (_kvStore != null)
        {
            return _kvStore;
        }

        var config = new NatsKVConfig(_bucketName)
        {
            History = 1,
            Storage = NatsKVStorageType.File
        };

        _kvStore = await _natsProvider.KeyValue.CreateStoreAsync(config);
        return _kvStore;
    }

    public async Task<IReadOnlyList<string>> GetSubscriberAddresses(string topic)
    {
        var store = await GetOrCreateKVStore();
        var key = GetKey(topic);

        try
        {
            var entry = await store.GetEntryAsync<string>(key);
            return string.IsNullOrEmpty(entry.Value) ? [] : entry.Value!.Split(',').ToList();
        }
        catch (NatsKVKeyNotFoundException)
        {
            return [];
        }
    }

    public async Task RegisterSubscriber(string topic, string subscriberAddress)
    {
        var store = await GetOrCreateKVStore();
        var key = GetKey(topic);

        var addresses = (await GetSubscriberAddresses(topic)).ToList();
        if (!addresses.Contains(subscriberAddress))
        {
            addresses.Add(subscriberAddress);
            await store.PutAsync(key, string.Join(",", addresses));
        }
    }

    public async Task UnregisterSubscriber(string topic, string subscriberAddress)
    {
        var store = await GetOrCreateKVStore();
        var key = GetKey(topic);

        var addresses = (await GetSubscriberAddresses(topic)).ToList();
        if (addresses.Remove(subscriberAddress))
        {
            if (addresses.Count > 0)
            {
                await store.PutAsync(key, string.Join(",", addresses));
            }
            else
            {
                await store.DeleteAsync(key);
            }
        }
    }

    public bool IsCentralized => _isCentralized;

    private string GetKey(string topic)
    {
        return $"subscription.{topic}";
    }
}
