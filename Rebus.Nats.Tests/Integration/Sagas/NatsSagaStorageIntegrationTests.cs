using Rebus.Exceptions;
using Rebus.Logging;
using Rebus.Nats.Sagas;
using Rebus.Nats.Tests.Fixtures;
using Rebus.Sagas;
using Xunit;

namespace Rebus.Nats.Tests.Integration.Sagas;

public class NatsSagaStorageIntegrationTests : IClassFixture<NatsTestFixture>
{
    private readonly NatsTestFixture _natsFixture;

    public NatsSagaStorageIntegrationTests(NatsTestFixture natsFixture)
    {
        _natsFixture = natsFixture;
    }

    private class TestCorrelationProperty : ISagaCorrelationProperty
    {
        public TestCorrelationProperty(string propertyName, Type sagaDataType)
        {
            PropertyName = propertyName;
            SagaDataType = sagaDataType;
        }

        public string PropertyName { get; }
        public Type SagaDataType { get; }
    }

    [Fact]
    public async Task SagaStorage_ShouldPersistAndRetrieveSagaData()
    {
        await _natsFixture.FlushDatabaseAsync();

        var natsProvider = new NatsProvider(_natsFixture.Connection, _natsFixture.JetStream);
        var loggerFactory = new ConsoleLoggerFactory(colored: false);
        var storage = new NatsSagaStorage(natsProvider, "test-sagas-1", loggerFactory);

        var sagaData = SagaTestFixture.CreateSagaData("test-correlation-123");
        var correlationProperties = new[]
        {
            new TestCorrelationProperty(nameof(TestSagaData.CorrelationId), typeof(TestSagaData))
        };

        await storage.Insert(sagaData, correlationProperties);

        var retrievedById = await storage.Find(typeof(TestSagaData), nameof(ISagaData.Id), sagaData.Id);

        Assert.NotNull(retrievedById);
        Assert.Equal(sagaData.Id, retrievedById.Id);
        Assert.Equal("test-correlation-123", ((TestSagaData) retrievedById).CorrelationId);
    }

    [Fact]
    public async Task SagaStorage_ShouldHandleConcurrentUpdates()
    {
        await _natsFixture.FlushDatabaseAsync();

        var natsProvider = new NatsProvider(_natsFixture.Connection, _natsFixture.JetStream);
        var loggerFactory = new ConsoleLoggerFactory(colored: false);
        var storage = new NatsSagaStorage(natsProvider, "test-sagas-2", loggerFactory);

        var sagaData = SagaTestFixture.CreateSagaData("concurrent-test");
        var correlationProperties = new[]
        {
            new TestCorrelationProperty(nameof(TestSagaData.CorrelationId), typeof(TestSagaData))
        };

        await storage.Insert(sagaData, correlationProperties);

        var saga1 = await storage.Find(typeof(TestSagaData), nameof(ISagaData.Id), sagaData.Id);
        var saga2 = await storage.Find(typeof(TestSagaData), nameof(ISagaData.Id), sagaData.Id);

        Assert.NotNull(saga1);
        Assert.NotNull(saga2);

        ((TestSagaData) saga1).ProcessingCount = 10;
        await storage.Update(saga1, correlationProperties);

        ((TestSagaData) saga2).ProcessingCount = 20;
        await Assert.ThrowsAsync<ConcurrencyException>(async () =>
            await storage.Update(saga2, correlationProperties));
    }

    [Fact]
    public async Task SagaStorage_ShouldDeleteSagaAndIndexes()
    {
        await _natsFixture.FlushDatabaseAsync();

        var natsProvider = new NatsProvider(_natsFixture.Connection, _natsFixture.JetStream);
        var loggerFactory = new ConsoleLoggerFactory(colored: false);
        var storage = new NatsSagaStorage(natsProvider, "test-sagas-3", loggerFactory);

        var sagaData = SagaTestFixture.CreateSagaData("delete-test");
        var correlationProperties = new[]
        {
            new TestCorrelationProperty(nameof(TestSagaData.CorrelationId), typeof(TestSagaData))
        };

        await storage.Insert(sagaData, correlationProperties);

        var retrieved = await storage.Find(typeof(TestSagaData), nameof(ISagaData.Id), sagaData.Id);
        Assert.NotNull(retrieved);

        await storage.Delete(retrieved);

        var afterDelete = await storage.Find(typeof(TestSagaData), nameof(ISagaData.Id), sagaData.Id);
        Assert.Null(afterDelete);
    }

    [Fact]
    public async Task SagaStorage_ShouldUpdateSagaData()
    {
        await _natsFixture.FlushDatabaseAsync();

        var natsProvider = new NatsProvider(_natsFixture.Connection, _natsFixture.JetStream);
        var loggerFactory = new ConsoleLoggerFactory(colored: false);
        var storage = new NatsSagaStorage(natsProvider, "test-sagas-4", loggerFactory);

        var sagaData = SagaTestFixture.CreateSagaData("update-test");
        var correlationProperties = new[]
        {
            new TestCorrelationProperty(nameof(TestSagaData.CorrelationId), typeof(TestSagaData))
        };

        await storage.Insert(sagaData, correlationProperties);
        Assert.Equal(1, sagaData.Revision);

        sagaData.ProcessingCount = 42;
        sagaData.Description = "Updated description";
        await storage.Update(sagaData, correlationProperties);
        Assert.Equal(2, sagaData.Revision);

        var retrieved = await storage.Find(typeof(TestSagaData), nameof(ISagaData.Id), sagaData.Id);
        Assert.NotNull(retrieved);
        Assert.Equal(42, ((TestSagaData) retrieved).ProcessingCount);
        Assert.Equal("Updated description", ((TestSagaData) retrieved).Description);
        Assert.Equal(2, retrieved.Revision);
    }

    [Fact]
    public async Task SagaStorage_MultipleInserts_ShouldPersistAllSagas()
    {
        await _natsFixture.FlushDatabaseAsync();

        var natsProvider = new NatsProvider(_natsFixture.Connection, _natsFixture.JetStream);
        var loggerFactory = new ConsoleLoggerFactory(colored: false);
        var storage = new NatsSagaStorage(natsProvider, "test-sagas-5", loggerFactory);

        var correlationProperties = new[]
        {
            new TestCorrelationProperty(nameof(TestSagaData.CorrelationId), typeof(TestSagaData))
        };

        var sagas = Enumerable.Range(0, 5)
            .Select(i => SagaTestFixture.CreateSagaData($"correlation-{i}"))
            .ToList();

        foreach (var saga in sagas)
        {
            await storage.Insert(saga, correlationProperties);
        }

        foreach (var saga in sagas)
        {
            var retrieved = await storage.Find(typeof(TestSagaData), nameof(ISagaData.Id), saga.Id);

            Assert.NotNull(retrieved);
            Assert.Equal(saga.Id, retrieved.Id);
        }
    }
}
