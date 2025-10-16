using NUnit.Framework;
using Rebus.Logging;
using Rebus.Nats.Tests.Fixtures;
using Rebus.Nats.Transport;
using Rebus.Tests.Contracts;
using Rebus.Tests.Contracts.Transports;
using Rebus.Threading.TaskParallelLibrary;
using Rebus.Transport;

namespace Rebus.Nats.Tests.Contracts;

public class NatsTransportFactory : ITransportFactory, IDisposable
{
    private readonly List<IDisposable> _disposables = new();
    private readonly HashSet<string> _queuesToCleanup = new();
    private readonly NatsTestFixture _fixture;

    public NatsTransportFactory()
    {
        _fixture = new NatsTestFixture();
        _fixture.InitializeAsync().GetAwaiter().GetResult();
    }

    public ITransport CreateOneWayClient()
    {
        var options = new NatsTransportOptions
        {
            StreamName = $"rebus-transport-{TestConfig.Suffix}",
            SubjectPrefix = "rebus.queue",
            TopicSubjectPrefix = "rebus.topic"
        };

        var natsProvider = new NatsProvider(_fixture.Connection, _fixture.JetStream);
        var loggerFactory = new ConsoleLoggerFactory(false);
        var transport = new NatsTransport(natsProvider, null, loggerFactory, options);

        _disposables.Add(transport);

        transport.Initialize();

        return transport;
    }

    public ITransport Create(string inputQueueAddress)
    {
        var options = new NatsTransportOptions
        {
            StreamName = $"rebus-transport-{TestConfig.Suffix}",
            SubjectPrefix = "rebus.queue",
            TopicSubjectPrefix = "rebus.topic"
        };

        var natsProvider = new NatsProvider(_fixture.Connection, _fixture.JetStream);
        var loggerFactory = new ConsoleLoggerFactory(false);
        var transport = new NatsTransport(natsProvider, inputQueueAddress, loggerFactory, options);

        _disposables.Add(transport);
        _queuesToCleanup.Add(inputQueueAddress);

        transport.Initialize();

        return transport;
    }

    public void CleanUp()
    {
        foreach (var disposable in _disposables)
        {
            disposable.Dispose();
        }
        _disposables.Clear();

        _fixture.FlushDatabaseAsync().GetAwaiter().GetResult();

        _queuesToCleanup.Clear();
    }

    public void Dispose()
    {
        CleanUp();
        _fixture.DisposeAsync().GetAwaiter().GetResult();
    }
}

[TestFixture]
public class NatsTransportBasicSendReceive : BasicSendReceive<NatsTransportFactory>
{
}

[TestFixture]
public class NatsTransportMessageExpiration : MessageExpiration<NatsTransportFactory>
{
}
