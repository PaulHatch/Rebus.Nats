using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using NATS.Client.Core;
using Rebus.Bus;
using Rebus.Messages;

namespace Rebus.Nats.Async;

/// <summary>Helper class for sending NATS async requests and receiving responses.</summary>
public static class AsyncClientNatsExtensions
{
    private static INatsConnection? _connection;

    internal static void RegisterClient(INatsConnection connection)
    {
        _connection = connection;
    }

    /// <summary>
    /// Extension method on <see cref="IBus" /> that allows for asynchronously sending a request and dispatching
    /// the received reply to the continuation.
    /// </summary>
    /// <typeparam name="TReply">
    /// Specifies the expected type of the reply. Can be any type compatible with the actually
    /// received reply
    /// </typeparam>
    /// <param name="bus">The bus API to use when sending the request</param>
    /// <param name="request">The request message</param>
    /// <param name="optionalHeaders">Headers to be included in the request message</param>
    /// <param name="timeout">
    /// Optionally specifies the max time to wait for a reply. If this time is exceeded, a
    /// <see cref="TimeoutException" /> is thrown
    /// </param>
    /// <param name="externalCancellationToken">
    /// An external cancellation token from some outer context that cancels waiting for
    /// a reply
    /// </param>
    public static async Task<TReply> SendRequest<TReply>(
        this IBus bus,
        object request,
        IDictionary<string, string>? optionalHeaders = null,
        TimeSpan? timeout = null,
        CancellationToken externalCancellationToken = default)
    {
        if (_connection == null)
        {
            throw new InvalidOperationException("The NATS client has not been initialized.");
        }

        if (bus == null)
        {
            throw new ArgumentNullException(nameof(bus));
        }

        if (request == null)
        {
            throw new ArgumentNullException(nameof(request));
        }

        var maxWaitTime = timeout ?? TimeSpan.FromSeconds(15);
        var replySubject =  _connection.NewInbox();
        

        if (optionalHeaders?.TryGetValue(Headers.MessageId, out var messageID) is not true)
        {
            messageID = Guid.NewGuid().ToString();
        }

        var headers = optionalHeaders ?? new Dictionary<string, string>();
        headers[AsyncHeaders.Timeout] = maxWaitTime.TotalMilliseconds.ToString(CultureInfo.InvariantCulture);
        headers[AsyncHeaders.ReplySubject] = replySubject;
        headers[Headers.MessageId] = string.Concat(AsyncHeaders.MessageIDPrefix, messageID);

        using var timeoutCts = new CancellationTokenSource(maxWaitTime);
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(timeoutCts.Token, externalCancellationToken);

        var replyTask = _connection.SubscribeAsync<string>(replySubject, cancellationToken: linkedCts.Token);

        await bus.Send(request, headers);

        try
        {
            await foreach (var msg in replyTask.ConfigureAwait(false))
            {
                if (msg.Data == null)
                {
                    throw new InvalidOperationException("Received null response from NATS");
                }

                var response = JsonSerializer.Deserialize<AsyncResponse>(msg.Data);
                if (response == null)
                {
                    throw new InvalidOperationException("Could not deserialize NATS response");
                }

                switch (response.ResponseType)
                {
                    case ResponseType.Success:
                        if (response.Data != null && typeof(TReply) != typeof(object))
                        {
                            var result = JsonSerializer.Deserialize<TReply>(response.Data);
                            if (result != null)
                            {
                                return result;
                            }
                        }
                        return default!;

                    case ResponseType.Error:
                        throw new NatsAsyncException(response.Data ?? "Unknown error", messageID);

                    case ResponseType.Cancelled:
                        throw new OperationCanceledException();

                    default:
                        throw new NatsAsyncException($"Unknown response type: {response.ResponseType}", messageID);
                }
            }

            throw new TimeoutException($"Did not receive reply for request with ID '{messageID}' within {maxWaitTime} timeout");
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested)
        {
            throw new TimeoutException($"Did not receive reply for request with ID '{messageID}' within {maxWaitTime} timeout");
        }
    }
}

internal record AsyncResponse(ResponseType ResponseType, string? Data);
