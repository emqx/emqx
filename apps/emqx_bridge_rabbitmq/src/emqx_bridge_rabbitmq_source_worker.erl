%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_rabbitmq_source_worker).

-behaviour(gen_server).

-export([start_link/1]).
-export([
    init/1,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-include_lib("amqp_client/include/amqp_client.hrl").

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

init({_RabbitChannel, _InstanceId, _Params} = State) ->
    {ok, State, {continue, confirm_ok}}.

handle_continue(confirm_ok, State) ->
    receive
        #'basic.consume_ok'{} -> {noreply, State}
    end.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(
    {#'basic.deliver'{delivery_tag = Tag} = BasicDeliver, #amqp_msg{
        payload = Payload,
        props = PBasic
    }},
    {Channel, InstanceId, Params} = State
) ->
    Message = to_map(BasicDeliver, PBasic, Params, Payload),
    #{hookpoints := Hooks, no_ack := NoAck} = Params,
    lists:foreach(fun(Hook) -> emqx_hooks:run(Hook, [Message]) end, Hooks),
    (NoAck =:= false) andalso
        amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}),
    emqx_resource_metrics:received_inc(InstanceId),
    {noreply, State};
handle_info(#'basic.cancel_ok'{}, State) ->
    {stop, normal, State};
handle_info(_Info, State) ->
    {noreply, State}.

to_map(BasicDeliver, PBasic, Params, Payload) ->
    #'basic.deliver'{exchange = Exchange, routing_key = RoutingKey} = BasicDeliver,

    #'P_basic'{
        content_type = ContentType,
        content_encoding = ContentEncoding,
        headers = Headers,
        delivery_mode = DeliveryMode,
        priority = Priority,
        correlation_id = CorrelationId,
        reply_to = ReplyTo,
        expiration = Expiration,
        message_id = MessageId,
        timestamp = Timestamp,
        type = Type,
        user_id = UserId,
        app_id = AppId,
        cluster_id = ClusterId
    } = PBasic,

    #{queue := Queue} = Params,

    Message = #{
        <<"payload">> => make_payload(Payload),
        <<"content_type">> => ContentType,
        <<"content_encoding">> => ContentEncoding,
        <<"headers">> => make_headers(Headers),
        <<"delivery_mode">> => DeliveryMode,
        <<"priority">> => Priority,
        <<"correlation_id">> => CorrelationId,
        <<"reply_to">> => ReplyTo,
        <<"expiration">> => Expiration,
        <<"message_id">> => MessageId,
        <<"timestamp">> => Timestamp,
        <<"type">> => Type,
        <<"user_id">> => UserId,
        <<"app_id">> => AppId,
        <<"cluster_id">> => ClusterId,
        <<"exchange">> => Exchange,
        <<"routing_key">> => RoutingKey,
        <<"queue">> => Queue
    },
    maps:filtermap(fun(_K, V) -> V =/= undefined andalso V =/= <<"undefined">> end, Message).

terminate(_Reason, _State) ->
    ok.

make_headers(undefined) ->
    undefined;
make_headers(Headers) when is_list(Headers) ->
    maps:from_list([{Key, Value} || {Key, _Type, Value} <- Headers]).

make_payload(Payload) ->
    case emqx_utils_json:safe_decode(Payload, [return_maps]) of
        {ok, Map} -> Map;
        {error, _} -> Payload
    end.
