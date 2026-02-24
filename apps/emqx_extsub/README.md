# MQTT External Subscription

MQTT External Subscription is an auxiliary app used to deliver messages to MQTT channels from external sources other than regular MQTT messaging.
Such sources may include:
* Retained messages
* Offline Message Plugin messages
* Message Queues
* Message Streams

## Design

The application is integrated into the EMQX core via hooks.

* message-consumption hooks, like:
    - `delivery.completed`
    - `message.delivered`
    - `message.nack`

* session-lifecycle hooks, like:
    - `session.created`
    - `session.subscribed`
    - `session.unsubscribed`
    - `session.resumed`
    - `session.disconnected`

* hook for handling generic messages:
    - `client.handle_info`

Using the hooks, the application attaches to each client some state (`st` record in the `emqx_extsub` module) that is
responsible for receiving messages from external sources and delivering them to the client's inflight.

This ExtSub state contains
* states of individual external sources, _ExtSub handlers_ (see the `emqx_extsub_handler` module);
* a common buffer of messages received from external sources;
* some other data, like timers, inflight messages, etc.

## Usage

### General usage

To use the `extsub` for delivering non-standard messages (that is, not originating from regular MQTT messaging), you need to do the following.

* Implement an `emqx_extsub_handler` callback module.
* Register the callback module in the `extsub` application via `emqx_extsub_handler_registry:register/2`.
* On application stop, unregister the callback module via `emqx_extsub_handler_registry:unregister/1`.

### `emqx_extsub_handler` behavior

An ExtSub handler represents a single external source of messages for a particular client. All callbacks are executed in the context of the channel's process.

ExtSub handlers may be registered as single-topic or multi-topic. When registered as single-topic, for each topic
on which the client is subscribed, a separate ExtSub handler is created. When registered as multi-topic, a single ExtSub handler is created for all topics.

Also, ExtSub handlers may be registered as handling generic messages. In this case, the handler will receive all generic (OTP) messages that the client process receives and that were not handled by the channel itself.

#### `handle_subscribe` callback

```erlang
-callback handle_subscribe(
    subscribe_type(), subscribe_ctx(), state() | undefined, emqx_extsub_types:topic_filter()
) ->
    {ok, state()} | ignore.
```

If the handler wants to handle the topic, then it should return `{ok, state()}`. If the handler is not interested in the topic, it should return `ignore`.

The `subscribe_ctx()` contains callback functions that the handler may use to send messages to self (optionally with delay).

`handle_subscribe` callback is called when the client subscribes to a topic. For a single-topic handler, state() is always `undefined`. For a multi-topic handler, `state()` is either `undefined` if no topic has been handled yet, or the state of the handler for the previous topics.

#### `handle_unsubscribe` callback

```erlang
-callback handle_unsubscribe(unsubscribe_type(), state(), emqx_extsub_types:topic_filter()) ->
    state().
```

`handle_unsubscribe` callback is called when the client unsubscribes from a topic that has been handled by the handler.

#### `handle_terminate` callback

```erlang
-callback handle_terminate(state()) -> ok.
```

`handle_terminate` callback is called when the handler is about to be destroyed: either the client disconnects or it has unsubscribed from all topics.

For single-topic handlers, unsubscribe and terminate may be used interchangeably.

#### `handle_delivered` callback

```erlang
-callback handle_delivered(state(), ack_ctx(), emqx_types:message(), emqx_extsub_types:ack()) ->
    {ok, state()} | {destroy, [emqx_extsub_types:topic_filter()]}.
```

`handle_delivered` callback is called when a message provided by the handler has been delivered to the client.
For QoS 0 messages this happens when the message is pushed to the inflight. For QoS 1 and 2 messages, this happens when the message is acknowledged by the client (if the underlying session supports acknowledgements; otherwise the callback is called when the message was sent to the outcoming connection buffer).

If the `ack_ctx()` the handler is informed about the desired number of messages that the ExtSub application wants the handler to provide.

If the handler returns `{destroy, [emqx_extsub_types:topic_filter()]}`, it signals that it is done and should be removed from the state to free up resources.

#### `handle_info` callback

```erlang
-callback handle_info(state(), info_ctx(), term()) ->
      {ok, state()}
    | {ok, state(), [emqx_types:message()]}
    | {destroy, [emqx_extsub_types:topic_filter()]}
    | recreate.
```

`handle_info` callback is called when the handler receives a generic message or a self-sent message. After handling the message, the callback may
* provide a new state for the handler;
* provide a new state and a list of messages to be delivered to the client;
* or request the handler to be recreated, e.g. if it encounters an error it cannot recover from.

If the `info_ctx()` the handler is informed about the desired number of messages that the ExtSub application wants the handler to provide.

If the handler returns `{destroy, [emqx_extsub_types:topic_filter()]}`, it signals that it is done and should be removed from the state to free up resources.

### How the callbacks of the `emqx_extsub_handler` behaviour are coordinated

The general pattern is the following.

* On `handle_subscribe` callback, the handler initializes and subscribes to receive messages from the source it handles, e.g. from some database.
* When receiving messages from the source (via the `handle_info` callback), the handler provides them to the ExtSub immediately.
* It may happen after some iterations that the ExtSub does not want any more messages from the handler. In this case,
`handle_info` context will contain `desired_message_count` set to 0. Then the handler should block its subscription to its message source.
* As messages are delivered to the client, the `handle_delivered` callback is called. Eventually, all the messages from the source will be delivered to the client, and the context of `handle_delivered` will ask for more messages (`ack_ctx()` will contain `desired_message_count > 0`). Then the handler should unblock its subscription to its message source and continue delivering messages.

If the handler fetches messages synchronously, the pattern is even simpler. In this case, the handler does not need
even to track the blocking status.

* On `handle_subscribe` callback, the handler initializes and fetches the initial batch of messages from the source it handles, e.g. from some database.
* The handler sends the obtained batch to self.
* When the handler receives info from self with the batch of messages, it delivers them to the client.
* When `handle_delivered` callback is called with the desired number of messages > 0, the handler fetches the next batch of messages from the source and sends it to self.

In both cases, the handler should avoid any kind of buffering of messages in the handler state. It should only be responsible for fetching messages and reacting to the backpressure from the ExtSub application.
