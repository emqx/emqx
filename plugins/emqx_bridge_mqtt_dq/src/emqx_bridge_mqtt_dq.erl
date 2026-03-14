%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_mqtt_dq).

-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").
-include("emqx_bridge_mqtt_dq.hrl").

-export([hook/0, unhook/0, on_message_publish/1]).

-spec hook() -> ok.
hook() ->
    emqx_hooks:put('message.publish', {?MODULE, on_message_publish, []}, ?HP_LOWEST).

-spec unhook() -> ok.
unhook() ->
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish, []}).

-spec on_message_publish(emqx_types:message()) -> {ok, emqx_types:message()}.
on_message_publish(#message{} = Message) ->
    forward_to_bridges(Message),
    {ok, Message}.

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

forward_to_bridges(#message{
    topic = Topic,
    payload = Payload,
    qos = QoS,
    flags = Flags,
    headers = Headers,
    timestamp = Ts
}) ->
    Bridges = emqx_bridge_mqtt_dq_config:get_bridges(),
    Retain = maps:get(retain, Flags, false),
    PayloadBin = iolist_to_binary(Payload),
    Props = pub_props(Headers),
    Item = #{
        topic => Topic,
        payload => PayloadBin,
        qos => QoS,
        retain => Retain,
        timestamp => Ts,
        properties => Props
    },
    lists:foreach(fun(Bridge) -> maybe_forward(Bridge, Topic, QoS, Item) end, Bridges).

pub_props(Headers) ->
    maps:get(properties, Headers, #{}).

maybe_forward(#{name := Name, enable := true, filter_topic := Filter} = Bridge, Topic, QoS, Item) ->
    case emqx_topic:match(Topic, Filter) of
        true -> enqueue_to_buffer(Name, Topic, QoS, Bridge, Item);
        false -> ok
    end;
maybe_forward(_Bridge, _Topic, _QoS, _Item) ->
    ok.

enqueue_to_buffer(BridgeName, Topic, QoS, Bridge, Item) ->
    BufferPoolSize = maps:get(buffer_pool_size, Bridge, 4),
    BufferIndex = erlang:phash2(Topic, BufferPoolSize),
    try emqx_bridge_mqtt_dq_buffer:get_pid(BridgeName, BufferIndex) of
        Pid ->
            ok = emqx_bridge_mqtt_dq_metrics:incr_bridge_enqueue(BridgeName),
            do_enqueue(Pid, Item, QoS, Bridge, BridgeName, BufferIndex)
    catch
        error:badarg ->
            ?LOG(error, #{
                msg => "mqtt_dq_buffer_not_ready",
                bridge => BridgeName,
                index => BufferIndex
            }),
            ok
    end.

do_enqueue(Pid, Item, 0, _Bridge, _BridgeName, _BufferIndex) ->
    emqx_bridge_mqtt_dq_buffer:enqueue(Pid, Item, no_ack);
do_enqueue(Pid, Item, _QoS, Bridge, BridgeName, BufferIndex) ->
    Alias = alias([reply]),
    emqx_bridge_mqtt_dq_buffer:enqueue(Pid, Item, Alias),
    Timeout = maps:get(enqueue_timeout_ms, Bridge, 5000),
    case wait_enqueue_ack(Alias, Timeout) of
        ok ->
            ok;
        timeout ->
            ?LOG(error, #{
                msg => "mqtt_dq_enqueue_timeout",
                bridge => BridgeName,
                index => BufferIndex
            }),
            ok
    end.

wait_enqueue_ack(Alias, Timeout) ->
    receive
        {Alias, ok} ->
            ok
    after Timeout ->
        unalias(Alias),
        receive
            {Alias, ok} ->
                ok
        after 0 ->
            timeout
        end
    end.
