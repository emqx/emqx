%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams).

-include("emqx_streams_internal.hrl").

-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").

-export([register_hooks/0, unregister_hooks/0]).
-export([register_sdisp_hooks/0, unregister_sdisp_hooks/0]).

-export([
    on_message_publish_sdisp/1,
    on_message_publish_stream/1,
    on_message_puback/4,
    on_session_subscribed/3,
    on_session_unsubscribed/3,
    on_client_handle_info/3
]).

%%

-spec register_hooks() -> ok.
register_hooks() ->
    ok = register_stream_hooks(),
    ok.

-spec unregister_hooks() -> ok.
unregister_hooks() ->
    ok = unregister_stream_hooks(),
    ok.

-spec register_sdisp_hooks() -> ok.
register_sdisp_hooks() ->
    %% FIXME: prios
    ok = emqx_hooks:add('message.publish', {?MODULE, on_message_publish_sdisp, []}, ?HP_HIGHEST),
    ok = emqx_hooks:add('message.puback', {?MODULE, on_message_puback, []}, ?HP_HIGHEST),
    ok = emqx_hooks:add('session.subscribed', {?MODULE, on_session_subscribed, []}, ?HP_LOWEST),
    ok = emqx_hooks:add('session.unsubscribed', {?MODULE, on_session_unsubscribed, []}, ?HP_LOWEST),
    ok = emqx_hooks:add('client.handle_info', {?MODULE, on_client_handle_info, []}, ?HP_LOWEST).

-spec register_stream_hooks() -> ok.
register_stream_hooks() ->
    ok = emqx_hooks:add('message.publish', {?MODULE, on_message_publish_stream, []}, ?HP_HIGHEST),
    ok = emqx_extsub_handler_registry:register(emqx_streams_extsub_handler, #{
        handle_generic_messages => true,
        multi_topic => true
    }).

-spec unregister_sdisp_hooks() -> ok.
unregister_sdisp_hooks() ->
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish_sdisp}),
    emqx_hooks:del('message.puback', {?MODULE, on_message_puback}),
    emqx_hooks:del('session.subscribed', {?MODULE, on_session_subscribed}),
    emqx_hooks:del('session.unsubscribed', {?MODULE, on_session_unsubscribed}),
    emqx_hooks:del('client.handle_info', {?MODULE, on_client_handle_info}).

-spec unregister_stream_hooks() -> ok.
unregister_stream_hooks() ->
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish_stream}),
    emqx_extsub_handler_registry:unregister(emqx_streams_extsub_handler).

%%

on_message_publish_sdisp(#message{topic = <<"$sdisp/", _/binary>>} = Message) ->
    ?tp_debug("streams_on_message_publish", #{topic => Message#message.topic}),
    St = shard_dispatch_state(),
    Ret = emqx_streams_shard_dispatch:on_publish(Message, St),
    shard_dispatch_handle_ret(?FUNCTION_NAME, Ret);
on_message_publish_sdisp(Message) ->
    {ok, Message}.

on_message_publish_stream(#message{topic = Topic} = Message) ->
    ?tp_debug(streams_on_message_publish_stream, #{topic => Topic}),
    Streams = emqx_streams_registry:match(Topic),
    ok = lists:foreach(
        fun(Stream) ->
            {Time, Result} = timer:tc(fun() -> publish_to_stream(Stream, Message) end),
            case Result of
                ok ->
                    emqx_streams_metrics:inc(ds, inserted_messages),
                    ?tp_debug(streams_on_message_publish_to_queue, #{
                        topic_filter => emqx_streams_prop:topic_filter(Stream),
                        message_topic => emqx_message:topic(Message),
                        time_us => Time,
                        result => ok
                    });
                {error, Reason} ->
                    ?tp(error, streams_on_message_publish_queue_error, #{
                        topic_filter => emqx_streams_prop:topic_filter(Stream),
                        message_topic => emqx_message:topic(Message),
                        time_us => Time,
                        reason => Reason
                    })
            end
        end,
        Streams
    ),
    {ok, Message}.

on_message_puback(PacketId, #message{topic = <<"$sdisp/", _/binary>>} = Message, _Res, _RC) ->
    ?tp_debug("streams_on_message_puback", #{topic => Message#message.topic}),
    St = shard_dispatch_state(),
    Ret = emqx_streams_shard_dispatch:on_puback(PacketId, Message, St),
    shard_dispatch_handle_ret(?FUNCTION_NAME, Ret);
on_message_puback(_PacketId, _Message, _Res, _RC) ->
    ok.

on_client_handle_info(_ClientInfo, ?ds_tx_commit_reply(Ref, _) = Reply, Acc) ->
    ?tp_debug("on_client_handle_info", #{message => Reply}),
    St = shard_dispatch_state(),
    Ret = emqx_streams_shard_dispatch:on_tx_commit(Ref, Reply, Acc, St),
    shard_dispatch_handle_ret(?FUNCTION_NAME, Ret);
on_client_handle_info(_ClientInfo, #shard_dispatch_command{} = Command, Acc) ->
    ?tp_debug("on_client_handle_info", #{command => Command}),
    St = shard_dispatch_state(),
    Ret = emqx_streams_shard_dispatch:on_command(Command, Acc, St),
    shard_dispatch_handle_ret(?FUNCTION_NAME, Ret);
on_client_handle_info(_ClientInfo, _Info, _Acc) ->
    ok.

on_session_subscribed(ClientInfo, Topic = <<"$sdisp/", _/binary>>, _SubOpts) ->
    ?tp_debug("streams_on_session_subscribed", #{topic => Topic, subopts => _SubOpts}),
    St = shard_dispatch_state(),
    Ret = emqx_streams_shard_dispatch:on_subscription(ClientInfo, Topic, St),
    shard_dispatch_handle_ret(?FUNCTION_NAME, Ret);
on_session_subscribed(_ClientInfo, _Topic, _SubOpts) ->
    ok.

on_session_unsubscribed(ClientInfo, Topic = <<"$sdisp/", _/binary>>, _SubOpts) ->
    ?tp_debug("streams_on_session_unsubscribed", #{topic => Topic, subopts => _SubOpts}),
    St = shard_dispatch_state(),
    Ret = emqx_streams_shard_dispatch:on_unsubscription(ClientInfo, Topic, St),
    shard_dispatch_handle_ret(?FUNCTION_NAME, Ret);
on_session_unsubscribed(_ClientInfo, _Topic, _SubOpts) ->
    ok.

%%------------------------------------------------------------------------------
%% Shard Dispatch
%%------------------------------------------------------------------------------
%%
%% === Protocol
%%
%% The goal of the protocol is to:
%%  * Distribute shards of a stream among consumers.
%%  * Redistribute shards when existing consumers go away and new ones come in.
%%  * Ensure eventually well-balanced distribution of shards.
%%    - According to implementation-defined metric, e.g.: consumer load, locality,
%%      client responsiveness.
%%
%% Stream consumers are protocol participants. Shard dispatch protocol is separate
%% from stream consumption. Participants are expected to cooperate, there's no
%% measures against subversion. Each consumer must specify which Group it is
%% part of.
%%
%% Shard dispatch is facilitated through MQTT subscriptions and messaging to special
%% topics rooted at `$sdisp/`.
%%
%% 1) Stream consumer announces itself by subscribing to a special topic.
%%    Topic: `$sdisp/consume/<group>/<stream>`.
%%     Opts: QoS=0/1
%%    Broker registers stream consumer by its ClientID. If Broker already knows
%%    this consumer (even if there are shards already allocated to it), Broker
%%    always assumes that the consumer starts anew.
%%    Such minimal SUBCRIBE packet essentially means Consumer speaks current, 1.0
%%    version of the protocol. Similarly, if Broker recognize and speaks this
%%    version, it replies with a minimal `SUBACK(SUCCESS)` packet.
%%    Protocol discovery is currently not defined in this protocol, may be later
%%    added for example as a separate flow through subscription to `$sdisp/hello`.
%%
%% 2) Each time broker has a shard to dispatch to this consumer, a message is sent.
%%      Message Topic: `$sdisp/consume/<group>/lease/<shard>/<last-offset>/<stream>`
%%    Message Payload: <none>
%%    This message represents _proposal_, shard distribution does not change yet.
%%    Note that message topic does not match the subcription "topic filter",
%%    middlewares might need to be aware of this.
%%
%% 3) Stream consumer accepts the proposal by publishing a message.
%%    Message Topic: `$sdisp/progress/<group>/<shard>/<offset>/<stream>`
%%      Message QoS: 1
%%    Broker allocates specified shard to this consumer. Consumer should expect to
%%    receive `PUBACK(SUCCESS)`, before that shard should still be considered
%%    unallocated. If `PUBACK(ERROR)` is received, the allocation was refused.
%%    Strictly speaking, Consumer may attempt to publish such message w/o receiving
%%    proposal first, it's up to Broker to allow that.
%%
%% 4) Broker may attempt to redistribute shards, in this case a message is sent.
%%      Message Topic: `$sdisp/consumer/<group>/release/<shard>/<stream>`
%%    Message Payload: <none>
%%
%% 5) Stream consumer respects such request by publishing another message.
%%    Message Topic: `$sdisp/release/<group>/<shard>/<new-offset>/<stream>`
%%      Message QoS: 1
%%    Similarly, only `PUBACK(SUCCESS)` means specified shard is unallocated.
%%    It's almost identical to a _progress_ message in purpose: progress reporting.
%%    Broker should handle such messages idempotently. However, Broker may treat an
%%    attempt to release already released shard _and_ advance its offset as a logic
%%    error.
%%    Only when Broker receives _release_ message, shard is considered unallocated.
%%    Obviously, a misbehaving consumer may hold onto shards indefinitely. In this
%%    case one option Broker has is to forcefully shut down consumer's connection.
%%
%% *) Moreover, Consumers must publish same _progress_ messages each time another
%%    batch of messages in the stream shard was processed.
%%    Message Topic: `$sdisp/progress/<group>/<shard>/<new-offset>/<stream>`
%%      Message QoS: 1
%%    Once again, `PUBACK(SUCCESS)` means progress is saved. `PUBACK(ERROR)`s are
%%    possible and should be handled accordingly.
%%
%% For _progress_ messages, `PUBACK`s communicate logic error conditions: invalid
%% stream / shard, offset going backwards, shard allocation conflicts. Broker should
%% handle _progress_ messages idempontently.
%%
%% Progress messages also serve as heartbeats: consumers are expected to
%% periodically report progress when no new stream messages are received and
%% processed; those that fail to do so are considered dead, and their shard are
%% released. This protocol requirement may be relaxed in the near future.

-define(pd_sdisp_state, emqx_streams_shard_dispatch_state).

shard_dispatch_handle_ret(on_message_publish, {Cont, #message{} = Message}) when is_atom(Cont) ->
    {Cont, emqx_message:set_header(allow_publish, false, Message)};
shard_dispatch_handle_ret(_Hook, Ret) ->
    case Ret of
        {stop, Acc, StNext} ->
            shard_dispatch_update_state(StNext),
            {stop, Acc};
        {stop, Acc} ->
            {stop, Acc};
        {ok, StNext} ->
            shard_dispatch_update_state(StNext),
            ok;
        ok ->
            ok
    end.

shard_dispatch_state() ->
    erlang:get(?pd_sdisp_state).

shard_dispatch_update_state(St) ->
    erlang:put(?pd_sdisp_state, St).

publish_to_stream(Stream, #message{} = Message) ->
    emqx_streams_message_db:insert(Stream, Message).
