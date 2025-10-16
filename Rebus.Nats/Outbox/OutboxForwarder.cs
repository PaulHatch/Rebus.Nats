using System;
using System.Threading;
using System.Threading.Tasks;
using Rebus.Bus;
using Rebus.Logging;
using Rebus.Threading;
using Rebus.Transport;

namespace Rebus.Nats.Outbox;

/// <summary>Background worker that forwards messages from the NATS outbox to their destinations.</summary>
internal class OutboxForwarder : IDisposable, IInitializable
{
    private static readonly RetryUtility _sendRetryUtility = new([
        TimeSpan.FromSeconds(0.1),
        TimeSpan.FromSeconds(0.1),
        TimeSpan.FromSeconds(0.1),
        TimeSpan.FromSeconds(0.1),
        TimeSpan.FromSeconds(0.1),
        TimeSpan.FromSeconds(0.5),
        TimeSpan.FromSeconds(0.5),
        TimeSpan.FromSeconds(0.5),
        TimeSpan.FromSeconds(0.5),
        TimeSpan.FromSeconds(0.5),
        TimeSpan.FromSeconds(1),
        TimeSpan.FromSeconds(1),
        TimeSpan.FromSeconds(1),
        TimeSpan.FromSeconds(1),
        TimeSpan.FromSeconds(1)
    ]);

    private readonly CancellationToken _cancellationToken;
    private readonly CancellationTokenSource _cancellationTokenSource;
    private readonly IAsyncTask? _cleanup;
    private readonly IAsyncTask? _forwarder;
    private readonly IAsyncTask? _orphanedForwarder;
    private readonly IOutboxStorage _outboxStorage;
    private readonly ITransport _transport;

    public OutboxForwarder(
        IAsyncTaskFactory asyncTaskFactory,
        IRebusLoggerFactory rebusLoggerFactory,
        IOutboxStorage outboxStorage,
        ITransport transport,
        NatsOutboxConfiguration config)
    {
        if (asyncTaskFactory == null)
        {
            throw new ArgumentNullException(nameof(asyncTaskFactory));
        }

        _outboxStorage = outboxStorage;
        _transport = transport;
        var logger = rebusLoggerFactory.GetLogger<OutboxForwarder>();

        _cancellationTokenSource = new CancellationTokenSource();
        _cancellationToken = _cancellationTokenSource.Token;

        if (config.ForwardingEnabled)
        {
            _forwarder = asyncTaskFactory.Create("NATS Outbox Forwarder",
                RunForwarder,
                intervalSeconds: (int) config.ForwardingInterval.TotalSeconds);
        }
        else
        {
            logger.Info("NATS outbox forwarding is disabled");
        }

        if (config.CleanupEnabled)
        {
            _cleanup = asyncTaskFactory.Create("NATS Outbox Cleanup", RunCleanup,
                intervalSeconds: (int) config.CleanupInterval.TotalSeconds);
        }
        else
        {
            logger.Info("NATS outbox cleanup is disabled");
        }

        if (config.OrphanedForwardingEnabled)
        {
            _orphanedForwarder = asyncTaskFactory.Create("NATS Orphaned Forwarder", RunOrphanedForwarder,
                intervalSeconds: (int) config.OrphanedForwardingInterval.TotalSeconds);
        }
        else
        {
            logger.Info("NATS orphaned forwarding is disabled");
        }
    }

    private bool IsRunning => !_cancellationToken.IsCancellationRequested;

    public void Dispose()
    {
        _cancellationTokenSource.Cancel();
        _forwarder?.Dispose();
        _cleanup?.Dispose();
        _orphanedForwarder?.Dispose();
        _cancellationTokenSource.Dispose();
    }

    public void Initialize()
    {
        _forwarder?.Start();
        _cleanup?.Start();
        _orphanedForwarder?.Start();
    }

    private async Task RunCleanup()
    {
        await _outboxStorage.CleanupIdleConsumers();
        await _outboxStorage.TrimQueue();
    }

    private async Task RunForwarder()
    {
        bool anySent;

        do
        {
            anySent = false;
            var messages = await _outboxStorage.GetNextMessageBatch();

            if (messages is null)
            {
                continue;
            }

            using var scope = new RebusTransactionScope();
            foreach (var message in messages)
            {
                anySent = true;
                var destinationAddress = message.DestinationAddress;
                var transportMessage = message.ToTransportMessage();
                var transactionContext = scope.TransactionContext;

                await _sendRetryUtility.ExecuteAsync(() =>
                    _transport.Send(destinationAddress, transportMessage, transactionContext), _cancellationToken);

                await _outboxStorage.MarkAsDispatched(message);
            }

            await scope.CompleteAsync();
        } while (anySent);
    }

    private async Task RunOrphanedForwarder()
    {
        bool anySent;

        do
        {
            anySent = false;
            var messages = await _outboxStorage.GetOrphanedMessageBatch();

            if (messages is null)
            {
                continue;
            }

            using var scope = new RebusTransactionScope();
            foreach (var message in messages)
            {
                anySent = true;
                var destinationAddress = message.DestinationAddress;
                var transportMessage = message.ToTransportMessage();
                var transactionContext = scope.TransactionContext;

                await _sendRetryUtility.ExecuteAsync(() =>
                    _transport.Send(destinationAddress, transportMessage, transactionContext), _cancellationToken);

                await _outboxStorage.MarkAsDispatched(message);
            }

            await scope.CompleteAsync();
        } while (anySent && IsRunning);
    }
}
