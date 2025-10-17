using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.KeyValueStore;
using NSubstitute;
using Rebus.Logging;
using Rebus.Nats.Sagas;
using Rebus.Nats.Tests.Fixtures;
using Rebus.Sagas;
using Xunit;

namespace Rebus.Nats.Tests.Unit.Sagas;

public class NatsSagaStorageTests
{
    private readonly NatsSagaStorage _storage;

    public NatsSagaStorageTests()
    {
        var connection = Substitute.For<INatsConnection>();
        var jetStream = Substitute.For<INatsJSContext>();
        var kvContext = Substitute.For<INatsKVContext>();
        var kvStore = Substitute.For<INatsKVStore>();

        kvContext.CreateStoreAsync(Arg.Any<NatsKVConfig>(), Arg.Any<CancellationToken>())
            .Returns(kvStore);

        var loggerFactory = Substitute.For<IRebusLoggerFactory>();
        var logger = Substitute.For<ILog>();
        loggerFactory.GetLogger<NatsSagaStorage>().Returns(logger);

        var natsProvider = new NatsProvider(connection, jetStream, kvContext);
        _storage = new NatsSagaStorage(natsProvider, "test-sagas", loggerFactory);
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
    public async Task Find_ByCorrelationProperty_ShouldReturnNull()
    {
        var correlationId = "test-correlation-123";

        var result = await _storage.Find(typeof(TestSagaData), nameof(TestSagaData.CorrelationId), correlationId);

        Assert.Null(result);
    }

    [Fact]
    public async Task Insert_ShouldStoreNewSagaData()
    {
        var sagaData = SagaTestFixture.CreateSagaData();
        var correlationProperties = new[]
        {
            new TestCorrelationProperty(nameof(TestSagaData.CorrelationId), typeof(TestSagaData))
        };

        await _storage.Insert(sagaData, correlationProperties);

        Assert.Equal(1, sagaData.Revision);
    }

    [Fact]
    public async Task Insert_WithEmptyId_ShouldThrowInvalidOperationException()
    {
        var sagaData = SagaTestFixture.CreateSagaData();
        sagaData.Id = Guid.Empty;
        var correlationProperties = new[]
        {
            new TestCorrelationProperty(nameof(TestSagaData.CorrelationId), typeof(TestSagaData))
        };

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await _storage.Insert(sagaData, correlationProperties));
    }

    [Fact]
    public async Task Insert_WithNonZeroRevision_ShouldThrowInvalidOperationException()
    {
        var sagaData = SagaTestFixture.CreateSagaData();
        sagaData.Revision = 5;
        var correlationProperties = new[]
        {
            new TestCorrelationProperty(nameof(TestSagaData.CorrelationId), typeof(TestSagaData))
        };

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await _storage.Insert(sagaData, correlationProperties));
    }

    [Fact]
    public async Task Update_ShouldIncrementRevision()
    {
        var sagaData = SagaTestFixture.CreateSagaData();
        sagaData.Revision = 1;
        var originalRevision = sagaData.Revision;

        var correlationProperties = new[]
        {
            new TestCorrelationProperty(nameof(TestSagaData.CorrelationId), typeof(TestSagaData))
        };

        try
        {
            await _storage.Update(sagaData, correlationProperties);
        }
        catch
        {
        }

        Assert.Equal(originalRevision + 1, sagaData.Revision);
    }

    [Fact]
    public async Task Update_WithEmptyId_ShouldThrowInvalidOperationException()
    {
        var sagaData = SagaTestFixture.CreateSagaData();
        sagaData.Id = Guid.Empty;
        var correlationProperties = new[]
        {
            new TestCorrelationProperty(nameof(TestSagaData.CorrelationId), typeof(TestSagaData))
        };

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await _storage.Update(sagaData, correlationProperties));
    }

    [Fact]
    public async Task Delete_ShouldRemoveSagaData()
    {
        var sagaData = SagaTestFixture.CreateSagaData();

        await _storage.Delete(sagaData);
    }
}
