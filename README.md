# Rebus NATS

This is a port of the [Rebus.Redis](https://github.com/PaulHatch/Rebus.Redis) library to use NATS, supporting saga persistence, outbox, and async messaging, as well as a transport implementation which was not present in the original Rebus.Redis implementation.

## Async Support

There is [an existing async library for Rebus](https://github.com/rebus-org/Rebus.Async) which uses the normal Rebus transport to send a reply, however it is marked experimental and the reasoning given for this is quite a reasonable one that [durable messages are not suitable for the ephemeral state of an async request](https://github.com/rebus-org/Rebus.Async/issues/19#issuecomment-1243273692). A pending async await is by nature ephemeral, using a persistent queue to send a reply is undesirable. In place of using the normal Rebus transport, this library uses NATS publish/subscribe to send the reply, only currently subscribed listeners will receive the reply making it well suited for this use case.

Using async you can make a call like this on the client:

```csharp
var response = await bus.SendRequest<ReplyMessage>(request);
```

This will send the message to the server as normal, as well as add a task to the NATS subscription. On the server you can reply to the pending task like this:

```csharp
await bus.ReplyAsync(replyMessage);
```

There are some additional methods to allow flexibility in cases like sagas where the handler is not ready to reply until some further action is taken. Calling `GetReplyContext` in the context of a NATS async request will return a context to allow you to send a reply at some later date. This context is just an identifier, it is safe to store and can be added to a saga state, allowing a future message to reply to the original request.

```csharp
var replyContext = messageContext.GetReplyContext();
await replyContext.ReplyAsync(replyMessage);
```

In addition, the timeout for the caller is sent along with the request so that the recipient of a message can determine how long the caller will be waiting for a response, which may be useful for cancelling a task or determining whether to send a response to the caller.

```csharp
// in the client (the default timeout is 15 seconds if not specified)
var response = await bus.SendRequest<ReplyMessage>(request, timeout: TimeSpan.FromSeconds(30));
// on the handler
var timeout = messageContext.GetReplyTimeout();
```

## Async Configuration

To configure async messaging, you need to enable NATS and configure the async messaging. This can be done as follows:

```csharp
Configure.With(activationHandler)
    .Options(o =>
    {
        o.SetBusName("main");
        o.EnableNats("nats://localhost:4222", r => r.EnableAsync());
    })
    // ...
```

By default, both client and server mode will be active. This means that a listener will be started to listen for replies from dispatched requests and that a step handler will be registered to redirect replies sent from a NATS request to the NATS publish channel. If you only want to use async messaging in one direction, you can disable the other mode as follows:

```csharp
Configure.With(activationHandler)
    .Options(o =>
    {
        o.SetBusName("main");
        o.EnableNats("nats://localhost:4222", r => r.EnableAsync(AsyncMode.Client)); // or AsyncMode.Host
    })
    // ...
```

Typically only one service would use NATS async messaging, e.g. a client facing service. If however you need to send replies via NATS from one service to another and if each one has its own NATS server, you can configure the replies to be routed based on the sender address. This can be done using the `RouteRepliesTo` method on the NATS configuration:

```csharp
Configure.With(activationHandler)
    .Options(o =>
    {
        o.SetBusName("main");
        o.EnableNats("nats://main-nats:4222", r => r.EnableAsync()
            .RouteRepliesTo("other-service", "nats://other-nats:4222"));
    })
    // ...
```

Note that this impacts only the reply routing, all other NATS components will use the main NATS connection configured when calling `EnableNats`.

## Saga Storage and Outbox

This library also provides a NATS implementation of the saga storage and an outbox implementation modeled after the Postgres implementation in Rebus. The outbox is implemented using NATS JetStream, and saga data is stored using NATS key-value stores.

Subscriptions (pub/sub) are handled natively by the NATS transport using JetStream subjects and consumers, so no separate subscription storage configuration is needed.

Basic configuration for the saga storage and outbox is as follows:

```csharp
using var activationHandler = new BuiltinHandlerActivator();
Configure.With(activationHandler)
    .Options(o =>
    {
        o.SetBusName("main");
        o.EnableNats("nats://localhost:4222", r => r.EnableAsync());
    })
    .Transport(t => t.UseNatsJetStream("my-queue"))  // Subscriptions handled automatically
    .Outbox(o => o.StoreInNats())
    .Sagas(s => s.StoreInNats());
```

This outbox only makes sense to use when the activity being performed is also stored in NATS, e.g. for sagas that use
NATS storage.

Internally the outbox is implemented using NATS JetStream using the NATS .NET client. Messages are consumed using push or pull consumers depending on the configuration, with automatic acknowledgment handling to ensure reliable delivery.