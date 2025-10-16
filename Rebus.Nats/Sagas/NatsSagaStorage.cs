using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading.Tasks;
using NATS.Client.KeyValueStore;
using Rebus.Exceptions;
using Rebus.Sagas;

namespace Rebus.Nats.Sagas;

/// <summary>
/// Stores saga data in NATS Key-Value stores, providing persistence and retrieval capabilities with optimistic
/// concurrency control using Key-Value store revisions.
/// </summary>
internal class NatsSagaStorage : ISagaStorage
{
    private static readonly JsonSerializerOptions _serializeOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    private readonly NatsProvider _natsProvider;
    private readonly string _bucketName;
    private INatsKVStore? _kvStore;

    public NatsSagaStorage(NatsProvider natsProvider, string bucketName)
    {
        _natsProvider = natsProvider;
        _bucketName = bucketName;
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

    public async Task<ISagaData> Find(Type sagaDataType, string propertyName, object propertyValue)
    {
        var store = await GetOrCreateKVStore();

        if (propertyName.Equals(nameof(ISagaData.Id), StringComparison.OrdinalIgnoreCase))
        {
            var key = GetKey(sagaDataType, propertyValue.ToString());

            try
            {
                var entry = await store.GetEntryAsync<string>(key);
                if (string.IsNullOrEmpty(entry.Value))
                {
                    return null!;
                }

                return (ISagaData)JsonSerializer.Deserialize(entry.Value, sagaDataType, _serializeOptions)!;
            }
            catch (NatsKVKeyNotFoundException)
            {
                return null!;
            }
            catch (NatsKVKeyDeletedException)
            {
                return null!;
            }
        }

        var propertyValueString = propertyValue?.ToString();
        if (string.IsNullOrEmpty(propertyValueString))
        {
            return null!;
        }

        var indexKey = GetIndexKey(sagaDataType, propertyName, propertyValueString);

        try
        {
            var indexEntry = await store.GetEntryAsync<string>(indexKey);
            if (string.IsNullOrEmpty(indexEntry.Value))
            {
                return null!;
            }

            if (!Guid.TryParse(indexEntry.Value, out var sagaId))
            {
                return null!;
            }

            return await Find(sagaDataType, nameof(ISagaData.Id), sagaId);
        }
        catch (NatsKVKeyNotFoundException)
        {
            return null!;
        }
        catch (NatsKVKeyDeletedException)
        {
            return null!;
        }
    }

    public async Task Insert(ISagaData data, IEnumerable<ISagaCorrelationProperty> correlationProperties)
    {
        if (data.Id == Guid.Empty)
        {
            throw new InvalidOperationException($"Saga data {data.GetType()} has an uninitialized Id property!");
        }

        if (data.Revision != 0)
        {
            throw new InvalidOperationException(
                $"Attempted to insert saga data with ID {data.Id} and revision {data.Revision}, but revision must be 0 on first insert!");
        }

        var store = await GetOrCreateKVStore();

        await CreateIndexEntries(data, correlationProperties);

        data.Revision++;
        var key = GetKey(data);
        var value = JsonSerializer.Serialize(data, data.GetType(), _serializeOptions);

        try
        {
            await store.PutAsync(key, value);
        }
        catch (Exception ex)
        {
            await DeleteIndexEntries(data, correlationProperties);
            throw new ConcurrencyException(ex, $"Saga data for {data.GetType().Name} with ID {data.Id} already exists");
        }
    }

    public async Task Update(ISagaData data, IEnumerable<ISagaCorrelationProperty> correlationProperties)
    {
        if (data.Id == Guid.Empty)
        {
            throw new InvalidOperationException($"Saga data {data.GetType()} has an uninitialized Id property!");
        }

        var store = await GetOrCreateKVStore();
        var key = GetKey(data);

        ISagaData? oldData = null;
        ulong natsRevision = 0;
        try
        {
            var oldEntry = await store.GetEntryAsync<string>(key);
            if (!string.IsNullOrEmpty(oldEntry.Value))
            {
                oldData = (ISagaData)JsonSerializer.Deserialize(oldEntry.Value, data.GetType(), _serializeOptions)!;
                natsRevision = oldEntry.Revision;

                if (oldData.Revision != data.Revision)
                {
                    throw new ConcurrencyException(
                        $"Update failed for saga data for {data.GetType().Name} with ID {data.Id} due to revision mismatch, another update has been made to this data since it was loaded for the current operation.");
                }
            }
        }
        catch (ConcurrencyException)
        {
            throw;
        }
        catch (NatsKVKeyNotFoundException ex)
        {
            throw new ConcurrencyException(ex,
                $"Saga data for {data.GetType().Name} with ID {data.Id} does not exist");
        }
        catch (NatsKVKeyDeletedException ex)
        {
            throw new ConcurrencyException(ex,
                $"Saga data for {data.GetType().Name} with ID {data.Id} does not exist");
        }

        if (oldData != null)
        {
            await DeleteIndexEntries(oldData, correlationProperties);
        }

        await CreateIndexEntries(data, correlationProperties);

        data.Revision++;
        var value = JsonSerializer.Serialize(data, data.GetType(), _serializeOptions);

        try
        {
            await store.UpdateAsync(key, value, natsRevision);
        }
        catch (NatsKVWrongLastRevisionException ex)
        {
            if (oldData != null)
            {
                await CreateIndexEntries(oldData, correlationProperties);
            }
            await DeleteIndexEntries(data, correlationProperties);
            throw new ConcurrencyException(ex,
                $"Update failed for saga data for {data.GetType().Name} with ID {data.Id} due to revision mismatch, another update has been made to this data since it was loaded for the current operation.");
        }
    }

    public async Task Delete(ISagaData sagaData)
    {
        var key = GetKey(sagaData);
        var store = await GetOrCreateKVStore();

        ISagaData? dataToDelete = sagaData;

        if (dataToDelete.Revision == 0)
        {
            try
            {
                var entry = await store.GetEntryAsync<string>(key);
                if (!string.IsNullOrEmpty(entry.Value))
                {
                    dataToDelete = (ISagaData)JsonSerializer.Deserialize(entry.Value, sagaData.GetType(), _serializeOptions)!;
                }
            }
            catch (NatsKVKeyNotFoundException)
            {
            }
            catch (NatsKVKeyDeletedException)
            {
            }
        }

        var correlationProperties = GetCorrelationPropertiesForType(sagaData.GetType());
        await DeleteIndexEntries(dataToDelete, correlationProperties);

        try
        {
            await store.DeleteAsync(key);
        }
        catch (NatsKVKeyNotFoundException)
        {
        }
    }

    private static IEnumerable<ISagaCorrelationProperty> GetCorrelationPropertiesForType(Type sagaDataType)
    {
        var properties = sagaDataType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.Name != nameof(ISagaData.Id) && p.Name != nameof(ISagaData.Revision));

        foreach (var property in properties)
        {
            yield return new SimpleSagaCorrelationProperty(property.Name, sagaDataType);
        }
    }

    private class SimpleSagaCorrelationProperty : ISagaCorrelationProperty
    {
        public SimpleSagaCorrelationProperty(string propertyName, Type sagaDataType)
        {
            PropertyName = propertyName;
            SagaDataType = sagaDataType;
        }

        public string PropertyName { get; }
        public Type SagaDataType { get; }
    }

    private static string GetKey(ISagaData data)
    {
        return GetKey(data.GetType(), data.Id);
    }

    private static string GetKey(Type type, object? id)
    {
        return $"saga.{type.Name.ToKebabCase()}.{id ?? throw new ArgumentNullException(nameof(id))}";
    }

    private static string GetIndexKey(Type sagaDataType, string propertyName, string propertyValue)
    {
        var sanitizedValue = SanitizeKeyValue(propertyValue);
        return $"saga-index.{sagaDataType.Name.ToKebabCase()}.{propertyName.ToKebabCase()}.{sanitizedValue}";
    }

    private static string SanitizeKeyValue(string value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return value;
        }

        var sanitized = new char[value.Length];
        for (int i = 0; i < value.Length; i++)
        {
            var c = value[i];
            sanitized[i] = char.IsLetterOrDigit(c) || c == '-' || c == '_' ? c : '_';
        }

        return new string(sanitized);
    }

    private static object? GetPropertyValue(object obj, string propertyName)
    {
        if (string.IsNullOrEmpty(propertyName))
        {
            throw new ArgumentException("Property name cannot be null or empty", nameof(propertyName));
        }

        var parts = propertyName.Split('.');
        object? currentValue = obj;

        foreach (var part in parts)
        {
            if (currentValue == null)
            {
                return null;
            }

            var propertyInfo = currentValue.GetType().GetProperty(part, BindingFlags.Public | BindingFlags.Instance);
            if (propertyInfo == null)
            {
                throw new ArgumentException($"Property '{part}' not found on type '{currentValue.GetType().Name}'");
            }

            currentValue = propertyInfo.GetValue(currentValue);
        }

        return currentValue;
    }

    private async Task CreateIndexEntries(ISagaData sagaData, IEnumerable<ISagaCorrelationProperty> correlationProperties)
    {
        var store = await GetOrCreateKVStore();
        var sagaDataType = sagaData.GetType();

        foreach (var correlationProperty in correlationProperties)
        {
            var propertyValue = GetPropertyValue(sagaData, correlationProperty.PropertyName);
            if (propertyValue == null)
            {
                continue;
            }

            var propertyValueString = propertyValue.ToString();
            if (string.IsNullOrEmpty(propertyValueString))
            {
                continue;
            }

            var indexKey = GetIndexKey(sagaDataType, correlationProperty.PropertyName, propertyValueString);

            try
            {
                var existingEntry = await store.GetEntryAsync<string>(indexKey);
                if (existingEntry.Value != null && existingEntry.Value != sagaData.Id.ToString())
                {
                    throw new ConcurrencyException(
                        $"Correlation property '{correlationProperty.PropertyName}' with value '{propertyValueString}' is already used by another saga instance");
                }
            }
            catch (NatsKVKeyNotFoundException)
            {
            }
            catch (NatsKVKeyDeletedException)
            {
            }

            await store.PutAsync(indexKey, sagaData.Id.ToString());
        }
    }

    private async Task DeleteIndexEntries(ISagaData sagaData, IEnumerable<ISagaCorrelationProperty> correlationProperties)
    {
        var store = await GetOrCreateKVStore();
        var sagaDataType = sagaData.GetType();

        foreach (var correlationProperty in correlationProperties)
        {
            var propertyValue = GetPropertyValue(sagaData, correlationProperty.PropertyName);
            if (propertyValue == null)
            {
                continue;
            }

            var propertyValueString = propertyValue.ToString();
            if (string.IsNullOrEmpty(propertyValueString))
            {
                continue;
            }

            var indexKey = GetIndexKey(sagaDataType, correlationProperty.PropertyName, propertyValueString);

            try
            {
                await store.DeleteAsync(indexKey);
            }
            catch (NatsKVKeyNotFoundException)
            {
            }
        }
    }
}
