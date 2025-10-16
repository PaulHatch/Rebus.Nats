using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using NATS.Client.Core;
using Rebus.Messages;
using Rebus.Pipeline;

namespace Rebus.Nats.Async;

/// <summary>Extension methods for host-side async request-reply support using NATS.</summary>
public static class AsyncHostNatsExtensions
{
    private static INatsConnection? _connection;

    internal static void RegisterHost(INatsConnection connection)
    {
        _connection = connection;
    }

    /// <summary>Extracts reply context from a message context for responding to async requests.</summary>
    /// <param name="context">The message context to extract reply information from.</param>
    public static ReplyContext? GetReplyContext(this IMessageContext context)
    {
        return GetContextFromHeaders(context.Headers, false);
    }

    /// <summary>Extracts reply context from a transport message for responding to async requests.</summary>
    /// <param name="message">The transport message to extract reply information from.</param>
    public static ReplyContext? GetReplyContext(this TransportMessage message)
    {
        return GetContextFromHeaders(message.Headers, false);
    }

    internal static ReplyContext? GetReplyToContext(this TransportMessage message)
    {
        return GetContextFromHeaders(message.Headers, true);
    }

    /// <summary>Publishes a successful response to an async request via NATS.</summary>
    /// <typeparam name="TResponse">The type of the response payload.</typeparam>
    /// <param name="context">The reply context containing routing information.</param>
    /// <param name="response">The response payload to send.</param>
    public static async Task NatsReplyAsync<TResponse>(this ReplyContext context, TResponse response)
    {
        if (_connection == null)
        {
            throw new InvalidOperationException("NATS host support has not been initialized");
        }

        var data = response != null ? JsonSerializer.Serialize(response) : null;
        var asyncResponse = new AsyncResponse(ResponseType.Success, data);
        var json = JsonSerializer.Serialize(asyncResponse);

        await _connection.PublishAsync(context.ReplySubject, json);
    }

    /// <summary>Publishes an error response to an async request via NATS.</summary>
    /// <param name="context">The reply context containing routing information.</param>
    /// <param name="message">The error message to send.</param>
    public static async Task NatsFailAsync(this ReplyContext context, string message)
    {
        if (_connection == null)
        {
            throw new InvalidOperationException("NATS host support has not been initialized");
        }

        var asyncResponse = new AsyncResponse(ResponseType.Error, message);
        var json = JsonSerializer.Serialize(asyncResponse);

        await _connection.PublishAsync(context.ReplySubject, json);
    }

    /// <summary>Publishes a cancellation response to an async request via NATS.</summary>
    /// <param name="context">The reply context containing routing information.</param>
    public static async Task NatsCancelAsync(this ReplyContext context)
    {
        if (_connection == null)
        {
            throw new InvalidOperationException("NATS host support has not been initialized");
        }

        var asyncResponse = new AsyncResponse(ResponseType.Cancelled, null);
        var json = JsonSerializer.Serialize(asyncResponse);

        await _connection.PublishAsync(context.ReplySubject, json);
    }

    /// <summary>Publishes a successful response to an async request from a message context.</summary>
    /// <typeparam name="TResponse">The type of the response payload.</typeparam>
    /// <param name="context">The message context to extract reply information and send response.</param>
    /// <param name="response">The response payload to send.</param>
    public static async Task NatsReplyAsync<TResponse>(this IMessageContext context, TResponse response)
    {
        var replyContext = context.GetReplyContext();
        if (replyContext is not null)
        {
            await replyContext.NatsReplyAsync(response);
        }
    }

    /// <summary>Publishes an error response to an async request from a message context.</summary>
    /// <param name="context">The message context to extract reply information and send error.</param>
    /// <param name="message">The error message to send.</param>
    public static async Task NatsFailAsync(this IMessageContext context, string message)
    {
        var replyContext = context.GetReplyContext();
        if (replyContext is not null)
        {
            await replyContext.NatsFailAsync(message);
        }
    }

    /// <summary>Publishes a cancellation response to an async request from a message context.</summary>
    /// <param name="context">The message context to extract reply information and send cancellation.</param>
    public static async Task NatsCancelAsync(this IMessageContext context)
    {
        var replyContext = context.GetReplyContext();
        if (replyContext is not null)
        {
            await replyContext.NatsCancelAsync();
        }
    }

    /// <summary>Extracts the timeout duration specified by the requestor from message headers.</summary>
    /// <param name="context">The message context containing timeout information.</param>
    public static TimeSpan? GetReplyTimeout(this IMessageContext context)
    {
        if (!context.Headers.ContainsKey(AsyncHeaders.Timeout) ||
            !long.TryParse(context.Headers[AsyncHeaders.Timeout], out var timeout))
        {
            return null;
        }

        return TimeSpan.FromMilliseconds(timeout);
    }

    private static ReplyContext? GetContextFromHeaders(IDictionary<string, string> headers, bool fromReplyTo)
    {
        var messageIDHeader = fromReplyTo ? Headers.InReplyTo : Headers.MessageId;
        if (!headers.ContainsKey(messageIDHeader) || !headers[messageIDHeader].StartsWith(AsyncHeaders.MessageIDPrefix))
        {
            return null;
        }

        if (!headers.ContainsKey(AsyncHeaders.ReplySubject))
        {
            return null;
        }

        var headerValue = headers[messageIDHeader];
        var messageID = headerValue.Substring(AsyncHeaders.MessageIDPrefix.Length);
        var replySubject = headers[AsyncHeaders.ReplySubject];
        var senderAddress = headers.TryGetValue(Headers.SenderAddress, out var header) ? header : "default";

        return new ReplyContext(senderAddress, replySubject, messageID);
    }
}
