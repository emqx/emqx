%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_rabbitmq_source_worker).

-behaviour(gen_server).

%% API
-export([
    start_link/1
]).

%% `gen_server' API
-export([
    init/1,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%%-------------------------------------------------------------------------------------------------
%% API
%%-------------------------------------------------------------------------------------------------

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-define(chan_pid, chan_pid).
-define(source_res_id, source_res_id).
-define(subscription_params, subscription_params).
-define(id, id).

-record(subscribe, {}).

%%-------------------------------------------------------------------------------------------------
%% API
%%-------------------------------------------------------------------------------------------------

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

%%-------------------------------------------------------------------------------------------------
%% `gen_server' API
%%-------------------------------------------------------------------------------------------------

init(Opts) ->
    #{
        id := Id,
        chan_pid := ChanPid,
        source_res_id := SourceResId,
        subscription_params := SubParams
    } = Opts,
    State = #{
        ?id => Id,
        ?chan_pid => ChanPid,
        ?source_res_id => SourceResId,
        ?subscription_params => SubParams
    },
    {ok, State, {continue, confirm_ok}}.

handle_continue(confirm_ok, State) ->
    receive
        #'basic.consume_ok'{} ->
            set_subscription_status(?status_connected, State),
            {noreply, State}
    end;
handle_continue(#subscribe{}, State) ->
    handle_subscribe(State).

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(
    {#'basic.deliver'{delivery_tag = Tag} = BasicDeliver, #amqp_msg{
        payload = Payload,
        props = PBasic
    }},
    State
) ->
    #{
        ?chan_pid := Chan,
        ?source_res_id := SourceResId,
        ?subscription_params := SubParams
    } = State,
    Message = to_map(BasicDeliver, PBasic, SubParams, Payload),
    #{
        hookpoints := Hooks,
        namespace := Namespace,
        no_ack := NoAck
    } = SubParams,
    lists:foreach(fun(Hook) -> emqx_hooks:run(Hook, [Message, Namespace]) end, Hooks),
    (NoAck =:= false) andalso
        amqp_channel:cast(Chan, #'basic.ack'{delivery_tag = Tag}),
    emqx_resource_metrics:received_inc(SourceResId),
    {noreply, State};
handle_info(#'basic.cancel_ok'{}, State) ->
    {stop, normal, State};
handle_info(#'basic.cancel'{}, State) ->
    set_subscription_status(
        {?status_disconnected, ~"subscription cancelled by remote server"}, State
    ),
    {noreply, State, {continue, #subscribe{}}};
handle_info(#subscribe{}, State) ->
    handle_subscribe(State);
handle_info(_Info, State) ->
    {noreply, State}.

%%-------------------------------------------------------------------------------------------------
%% Internal fns
%%-------------------------------------------------------------------------------------------------

handle_subscribe(State) ->
    #{
        ?chan_pid := Chan,
        ?source_res_id := SourceResId,
        ?subscription_params := SubParams
    } = State,
    #{queue := Queue, no_ack := NoAck} = SubParams,
    BasicConsume = #'basic.consume'{queue = Queue, no_ack = NoAck},
    try
        #'basic.consume_ok'{consumer_tag = _} =
            amqp_channel:subscribe(Chan, BasicConsume, self()),
        {noreply, State, {continue, confirm_ok}}
    catch
        exit:{Reason, _Stack} ->
            ?SLOG(warning, #{
                msg => "rabbitmq_source_worker_failed_to_subscribe",
                source_resource_id => SourceResId,
                reason => Reason
            }),
            case is_process_alive(Chan) of
                false ->
                    %% no point continuing; connector will be marked as disconencted and
                    %% restart.
                    {stop, {shutdown, failed_to_subscribe}, State};
                true ->
                    erlang:send_after(1_000, self(), #subscribe{}),
                    {noreply, State}
            end
    end.

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

make_headers(undefined) ->
    undefined;
make_headers(Headers) when is_list(Headers) ->
    maps:from_list([{Key, Value} || {Key, _Type, Value} <- Headers]).

make_payload(Payload) ->
    case emqx_utils_json:safe_decode(Payload) of
        {ok, Map} -> Map;
        {error, _} -> Payload
    end.

set_subscription_status(Status, State) ->
    #{?id := Id, ?source_res_id := SourceResId} = State,
    emqx_bridge_rabbitmq_connector:set_health(SourceResId, Id, Status).
