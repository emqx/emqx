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
    - `delivery.dropped`
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

#### Session takeover

When an in-memory session moves to another channel, the session replays its inflight messages with their original packet identifiers, as MQTT requires.
The new channel creates new handlers with new handler references.
A handler source can therefore send a replayed message again, and the client can receive two copies.
ExtSub ignores acknowledgements for replayed messages because their handler references do not match the new handlers.

#### `handle_subscribe` callback

```erlang
-callback handle_subscribe(
    subscribe_type(), subscribe_ctx(), state() | undefined, emqx_extsub_types:topic_filter()
) ->
    {ok, state()} | ignore | destroy.
```

The result applies to the topic filter of the call, for single-topic and multi-topic handlers:

* `{ok, state()}`: handle the topic with the returned state.
* `ignore`: change nothing. An existing handler keeps its state and its topics. When no handler handles the topic, ExtSub does not create one.
* `destroy`: stop handling the topic. When a handler handles the topic, ExtSub removes the topic from it, as for an unsubscribe. A handler without topics is terminated. When no handler handles the topic, `destroy` has the same effect as `ignore`.

The `subscribe_ctx()` contains a `message_target` value that identifies the owning channel and handler.
Use `emqx_extsub_handler:send/2` or `emqx_extsub_handler:send_after/3` to send a message to this target.
Store the target in handler state, not a callback function. A callback function can retain old code after a module reload.

To track another process, call `emqx_extsub_handler:monitor/2` with the target and the process PID.
When the process exits, the handler receives `{'DOWN', MRef, process, Pid, Reason}` in `handle_info`.
Only the channel that owns the handler can call `monitor/2` and `demonitor/2`, so call them from handler callbacks.
The handler must call `emqx_extsub_handler:demonitor/2` before it is destroyed, for example in `handle_terminate`.
A monitor that is not removed stays in place until the channel exits.
ExtSub drops a down message for a handler that no longer exists.

`handle_subscribe` callback is called when the client subscribes to a topic. For a single-topic handler, state() is `undefined` for a new subscription. For a multi-topic handler, `state()` is either `undefined` if no topic has been handled yet, or the state of the handler for the previous topics.

When the client subscribes again to a topic that the handler already handles, the `ignore_resubscribe` option controls the result.
With `ignore_resubscribe => true` (the default), ExtSub does not call the handler.
With `ignore_resubscribe => false`, ExtSub calls `handle_subscribe` with the existing handler state, and the result applies as described above.

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
      {ok, state()}
    | {destroy, state(), [emqx_extsub_types:topic_filter()]}
    | {destroy, state()}.
```

`handle_delivered` callback is called when a message provided by the handler has been delivered to the client.
For QoS 0 messages this happens when the message is pushed to the inflight. For QoS 1 and 2 messages, this happens when the message is acknowledged by the client (if the underlying session supports acknowledgements; otherwise the callback is called when the message was sent to the outcoming connection buffer).

When the session drops a message and runs `delivery.dropped`, the callback receives `{dropped, Reason}` as its acknowledgement value.
For an in-memory session, the reasons are `no_local` and `expired`.
The session does not put ExtSub messages in its mqueue. When inflight is full or the delivery rate limit refuses a message, the session returns the message to the ExtSub buffer with a NACK.
The session does not deliver the message later, and ExtSub does not put it back in the buffer.
Handlers must accept this value. Each handler decides what its source does with the message, for example acknowledge it or send it again.
ExtSub checks the handler reference and the sequence number of an acknowledgement, a drop or a NACK before it changes its state.
When the check fails, ExtSub does not call the handler and does not change its delivery counters.

If the `ack_ctx()` the handler is informed about the desired number of messages that the ExtSub application wants the handler to provide.

The destroy results are described in [Destroy and recreate results](#destroy-and-recreate-results).

#### `handle_info` callback

```erlang
-callback handle_info(state(), info_ctx(), term()) ->
      {ok, state()}
    | {ok, state(), [emqx_types:message()]}
    | {destroy, state(), [emqx_extsub_types:topic_filter()]}
    | {destroy, state()}
    | {recreate, state()}.
```

`handle_info` callback is called when the handler receives a generic message or a self-sent message. After handling the message, the callback may
* provide a new state for the handler;
* provide a new state and a list of messages to be delivered to the client;
* or request the handler to be recreated, e.g. if it encounters an error it cannot recover from.

If the `info_ctx()` the handler is informed about the desired number of messages that the ExtSub application wants the handler to provide.

The destroy and recreate results are described in [Destroy and recreate results](#destroy-and-recreate-results).

#### Destroy and recreate results

`handle_delivered` and `handle_info` return the handler state with each result. ExtSub stores the state, then applies the result:

* `{destroy, state(), [emqx_extsub_types:topic_filter()]}`: the handler is done with the listed topic filters. ExtSub removes them from the handler, as for an unsubscribe. A handler without topics left is terminated. A multi-topic handler keeps the returned state for its remaining topics.
* `{destroy, state()}`: the handler is done with all its topic filters. ExtSub removes all of them and terminates the handler.
* `{recreate, state()}` (`handle_info` only): ExtSub terminates the handler and subscribes it again to the same topic filters.

ExtSub calls `handle_unsubscribe` and `handle_terminate` with the returned state. So the handler can release the resources of the returned state there, including resources that the same callback created.

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
