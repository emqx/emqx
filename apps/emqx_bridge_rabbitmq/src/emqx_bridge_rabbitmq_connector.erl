%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_rabbitmq_connector).

-feature(maybe_expr, enable).
-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_trace.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% Needed to create RabbitMQ connection
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("credentials_obfuscation/include/credentials_obfuscation.hrl").

-behaviour(emqx_resource).
-behaviour(hocon_schema).
-behaviour(ecpool_worker).

%% hocon_schema callbacks
-export([namespace/0, roots/0, fields/1]).

%% emqx_resource callbacks
-export([
    on_start/2,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channels/1,
    on_stop/2,
    resource_type/0,
    callback_mode/0,
    on_get_status/2,
    on_get_channel_status/3,
    on_query/3,
    on_batch_query/3
]).

%% callbacks for ecpool_worker
-export([connect/1]).

%% internal exports
-export([set_health/3]).

%% Internal callbacks
-export([publish_messages/5]).

namespace() -> "rabbitmq".

roots() ->
    [].

%% bridge v1 called by emqx_bridge_rabbitmq
fields(config) ->
    emqx_bridge_rabbitmq_connector_schema:fields(connector) ++
        emqx_bridge_rabbitmq_pubsub_schema:fields(action_parameters).

-define(GET_STATUS_TIMEOUT, 10_000).
%% Less than ?T_OPERATION of emqx_resource_manager
-define(CHANNEL_CLOSE_TIMEOUT, 4_000).

-define(AUTO_RECONNECT_INTERVAL_S, 2).

-define(OPTVAR_CHAN_HEALTH(CHANRESID, IDX), {?MODULE, chan_health, CHANRESID, IDX}).

%% Allocatable resources
-define(optvars(CHANRESID), {optvars, CHANRESID}).

%% ===================================================================
%% Callbacks defined in emqx_resource
%% ===================================================================

%% emqx_resource callback
resource_type() -> rabbitmq.

callback_mode() -> always_sync.

on_start(InstanceId, Config) ->
    ?SLOG(info, #{
        msg => "starting_rabbitmq_connector",
        connector => InstanceId,
        config => emqx_utils:redact(Config)
    }),
    init_secret(),
    #{pool_size := PoolSize} = Config,
    Options = [
        {config, Config},
        {pool_size, PoolSize},
        {pool, InstanceId},
        {auto_reconnect, ?AUTO_RECONNECT_INTERVAL_S}
    ],
    case emqx_resource_pool:start(InstanceId, ?MODULE, Options) of
        ok ->
            {ok, #{
                pool_size => PoolSize,
                channels => #{}
            }};
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "rabbitmq_connector_start_failed",
                reason => Reason,
                config => emqx_utils:redact(Config)
            }),
            {error, Reason}
    end.

on_add_channel(
    ConnResId,
    #{channels := Channels0} = ConnState,
    ChanResId,
    ChanConfig
) ->
    case maps:is_key(ChanResId, Channels0) of
        true ->
            {error, already_exists};
        false ->
            #{resource_opts := #{health_check_timeout := HCTimeout}} = ChanConfig,
            ProcParam = preproc_parameter(ChanResId, ChanConfig),
            #{config_root := Kind0} = ProcParam,
            Kind =
                case Kind0 of
                    actions -> action;
                    sources -> source
                end,
            #{pool_size := PoolSize} = ConnState,
            clear_health(ChanResId, PoolSize),
            allocate(ConnResId, ?optvars(ChanResId), optvar_keys(ChanResId, PoolSize)),
            case make_channel(ConnResId, ChanResId, ProcParam) of
                {ok, RabbitChannels} ->
                    ChanState = #{
                        kind => Kind,
                        param => ProcParam,
                        rabbitmq => RabbitChannels,
                        health_check_timeout => HCTimeout
                    },
                    Channels = maps:put(ChanResId, ChanState, Channels0),
                    {ok, ConnState#{channels := Channels}};
                {{error, Error}, PartialRabbitChannels} ->
                    ?SLOG(error, #{
                        msg => "failed_to_start_rabbitmq_channel",
                        instance_id => ConnResId,
                        params => emqx_utils:redact(ChanConfig),
                        error => Error
                    }),
                    close_channels(maps:values(PartialRabbitChannels)),
                    {error, Error}
            end
    end.

on_remove_channel(ConnResId, ConnState, ChanResId) ->
    #{channels := Channels} = ConnState,
    clear_health_allocated(ConnResId, ChanResId),
    try_unsubscribe(ChanResId, Channels),
    {ok, ConnState#{channels := maps:remove(ChanResId, Channels)}}.

on_get_channels(ConnResId) ->
    emqx_bridge_v2:get_channels_for_connector(ConnResId).

on_stop(ConnResId, _ConnState) ->
    ?SLOG(info, #{
        msg => "stopping_rabbitmq_connector",
        connector => ConnResId
    }),
    clear_health_allocated(ConnResId),
    lists:foreach(
        fun({_Name, Worker}) ->
            case ecpool_worker:client(Worker) of
                {ok, Conn} -> amqp_connection:close(Conn);
                _ -> ok
            end
        end,
        ecpool:workers(ConnResId)
    ),
    Res = emqx_resource_pool:stop(ConnResId),
    ?tp("rabbitmq_connector_stop", #{instance_id => ConnResId}),
    Res.

%% internal export
set_health(ChanResId, Idx, HealthStatus) ->
    optvar:set(?OPTVAR_CHAN_HEALTH(ChanResId, Idx), HealthStatus),
    ok.

clear_health(ChanResId, PoolSize) ->
    lists:foreach(
        fun(Idx) -> optvar:unset(?OPTVAR_CHAN_HEALTH(ChanResId, Idx)) end,
        lists:seq(1, PoolSize)
    ).

clear_health_allocated(ConnResId) ->
    clear_health_allocated(ConnResId, _ChanResIdPat = '_').

clear_health_allocated(ConnResId, ChanResIdPat) ->
    maps:foreach(
        fun
            (?optvars(Id) = Key, Keys) when
                ChanResIdPat == '_' orelse Id == ChanResIdPat
            ->
                lists:foreach(fun optvar:unset/1, Keys),
                deallocate(ConnResId, Key);
            (_, _) ->
                ok
        end,
        emqx_resource:get_allocated_resources(ConnResId)
    ).

%% This is the callback function that is called by ecpool
-spec connect(term()) -> {ok, {pid(), pid()}, map()} | {error, term()}.
connect(Options) ->
    Config = proplists:get_value(config, Options),
    WorkerId = proplists:get_value(ecpool_worker_id, Options, 1),
    #{
        username := Username,
        password := WrappedPassword,
        timeout := Timeout,
        virtual_host := VirtualHost,
        heartbeat := Heartbeat
    } = Config,
    %% TODO: teach `amqp` to accept 0-arity closures as passwords.
    Password = emqx_secret:unwrap(WrappedPassword),
    Servers0 = emqx_bridge_rabbitmq_client:servers_from_config(Config),
    Servers = emqx_bridge_rabbitmq_client:rotate_servers(Servers0, WorkerId),
    AmqpParamsBase = #amqp_params_network{
        ssl_options = to_ssl_options(Config),
        username = Username,
        password = Password,
        connection_timeout = Timeout,
        virtual_host = VirtualHost,
        heartbeat = Heartbeat
    },
    case emqx_bridge_rabbitmq_client:start_connection(Servers, AmqpParamsBase) of
        {ok, RabbitMQConn} ->
            {ok, RabbitMQConn};
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "rabbitmq_connector_connection_failed",
                reason => Reason,
                config => emqx_utils:redact(Config)
            }),
            {error, Reason}
    end.

-spec on_get_status(resource_id(), term()) ->
    ?status_connected | {?status_disconnected, binary()}.
on_get_status(PoolName, #{channels := Channels}) ->
    ?tp("rabbitmq_on_get_status_enter0", #{}),
    ?tp("rabbitmq_on_get_status_enter1", #{}),
    ChannelNum = maps:size(Channels),
    Conns = get_rabbitmq_connections(PoolName),
    %% TODO: should disentangle channel and connector health checks; channels should be
    %% restarted independently, instead of relying on full connector restart...
    try actual_channel_nums(Conns) of
        ActualNums ->
            Check = lists:all(fun(ActualNum) -> ActualNum >= ChannelNum end, ActualNums),
            case Check of
                true when Conns =/= [] ->
                    ?tp_span(
                        "rabbitmq_on_get_status_connected",
                        #{},
                        ?status_connected
                    );
                _ ->
                    {?status_disconnected, <<"not_connected">>}
            end
    catch
        Error:Reason ->
            ?SLOG(error, #{
                msg => "rabbitmq_connector_get_status_failed",
                connector => PoolName,
                error => Error,
                reason => Reason
            }),
            {?status_disconnected, <<"not_connected">>}
    end.

actual_channel_nums(Conns) ->
    emqx_utils:pmap(
        fun(Conn) ->
            [{num_channels, ActualNum}] = amqp_connection:info(Conn, [num_channels]),
            ActualNum
        end,
        Conns,
        ?GET_STATUS_TIMEOUT
    ).

on_get_channel_status(_ConnResId, ChanResId, #{channels := Channels} = ConnState) when
    is_map_key(ChanResId, Channels)
->
    ?tp("rabbitmq_on_get_channel_status_enter", #{channel_id => ChanResId}),
    #{ChanResId := ChanState} = Channels,
    #{rabbitmq := RabbitMQ} = ChanState,
    case lists:all(fun is_process_alive/1, maps:values(RabbitMQ)) of
        true ->
            channel_health_check_cont(ChanResId, ChanState, ConnState);
        false ->
            {?status_disconnected, not_connected}
    end;
on_get_channel_status(_ConnResId, _ChanResId, _ConnState) ->
    %% impossible
    ?status_disconnected.

on_query(ResourceID, {ChannelId, Data} = MsgReq, State) ->
    ?SLOG(debug, #{
        msg => "rabbitmq_connector_received_query",
        connector => ResourceID,
        channel => ChannelId,
        data => Data,
        state => emqx_utils:redact(State)
    }),
    #{channels := Channels} = State,
    case maps:find(ChannelId, Channels) of
        {ok, #{param := ProcParam, rabbitmq := RabbitMQ}} ->
            TraceRenderedCTX = emqx_trace:make_rendered_action_template_trace_context(ChannelId),
            Res = ecpool:pick_and_do(
                ResourceID,
                {?MODULE, publish_messages, [RabbitMQ, ProcParam, [MsgReq], TraceRenderedCTX]},
                no_handover
            ),
            handle_result(Res);
        error ->
            {error, {unrecoverable_error, {invalid_message_tag, ChannelId}}}
    end.

on_batch_query(ResourceID, [{ChannelId, _Data} | _] = Batch, State) ->
    ?SLOG(debug, #{
        msg => "rabbitmq_connector_received_batch_query",
        connector => ResourceID,
        data => Batch,
        state => emqx_utils:redact(State)
    }),
    #{channels := Channels} = State,
    case maps:find(ChannelId, Channels) of
        {ok, #{param := ProcParam, rabbitmq := RabbitMQ}} ->
            TraceRenderedCTX = emqx_trace:make_rendered_action_template_trace_context(ChannelId),
            Res = ecpool:pick_and_do(
                ResourceID,
                {?MODULE, publish_messages, [RabbitMQ, ProcParam, Batch, TraceRenderedCTX]},
                no_handover
            ),
            handle_result(Res);
        error ->
            {error, {unrecoverable_error, {invalid_message_tag, ChannelId}}}
    end.

publish_messages(
    Conn,
    RabbitMQ,
    ChanState,
    Messages,
    TraceRenderedCTX
) ->
    try
        do_publish_messages(
            Conn,
            RabbitMQ,
            ChanState,
            Messages,
            TraceRenderedCTX
        )
    catch
        error:?EMQX_TRACE_STOP_ACTION_MATCH = Reason ->
            {error, Reason};
        %% if send a message to a non-existent exchange, RabbitMQ client will crash
        %% {shutdown,{server_initiated_close,404,<<"NOT_FOUND - no exchange 'xyz' in vhost '/'">>}
        %% so we catch and return a more user friendly message in that case.
        %% This seems to happen sometimes when the exchange does not exists.
        exit:{{shutdown, {server_initiated_close, Code, Msg}}, _InternalReason} ->
            ?tp(emqx_bridge_rabbitmq_connector_rabbit_publish_failed_with_msg, #{}),
            {error,
                {recoverable_error, #{
                    msg => <<"rabbitmq_publish_failed">>,
                    explain => Msg,
                    exchange => maps:get(exchange, ChanState),
                    routing_key => maps:get(routing_key, ChanState),
                    rabbit_mq_error_code => Code
                }}};
        %% This probably happens when the RabbitMQ driver is restarting the connection process
        exit:{noproc, _} = InternalError ->
            ?tp(emqx_bridge_rabbitmq_connector_rabbit_publish_failed_con_not_ready, #{}),
            {error,
                {recoverable_error, #{
                    msg => <<"rabbitmq_publish_failed">>,
                    explain => "Connection is establishing",
                    exchange => maps:get(exchange, ChanState),
                    routing_key => maps:get(routing_key, ChanState),
                    internal_error => InternalError
                }}};
        _Type:Reason ->
            ?tp(emqx_bridge_rabbitmq_connector_rabbit_publish_failed_other, #{}),
            Msg = iolist_to_binary(io_lib:format("RabbitMQ: publish_failed: ~p", [Reason])),
            {error, {recoverable_error, Msg}}
    end.

do_publish_messages(
    Conn,
    RabbitMQ,
    ChanState,
    Messages,
    TraceRenderedCTX
) ->
    #{
        delivery_mode := DeliveryMode,
        payload_template := _PayloadTmpl,
        headers_template := _HeadersTemplate,
        properties_template := _PropsTemplate,
        routing_key := RoutingKey0,
        exchange := Exchange0,
        wait_for_publish_confirmations := WaitForPublishConfirmations,
        publish_confirmation_timeout := PublishConfirmationTimeout
    } = ChanState,
    case maps:find(Conn, RabbitMQ) of
        {ok, Channel} ->
            Exchange = render_template(Exchange0, Messages),
            RoutingKey = render_template(RoutingKey0, Messages),
            Method = #'basic.publish'{
                exchange = Exchange,
                routing_key = RoutingKey
            },

            BaseProps = #'P_basic'{
                delivery_mode = DeliveryMode
            },
            FormattedMsgs = lists:map(
                fun({_ChanId, Msg}) ->
                    render_data(Msg, BaseProps, ChanState)
                end,
                Messages
            ),

            emqx_trace:rendered_action_template_with_ctx(TraceRenderedCTX, #{
                messages => FormattedMsgs,
                properties => #{
                    delivery_mode => DeliveryMode
                },
                method => #{
                    exchange => Exchange,
                    routing_key => RoutingKey
                }
            }),
            lists:foreach(
                fun({Payload, Props}) ->
                    amqp_channel:cast(
                        Channel,
                        Method,
                        #amqp_msg{
                            payload = Payload,
                            props = Props
                        }
                    )
                end,
                FormattedMsgs
            ),
            case WaitForPublishConfirmations of
                true ->
                    case
                        amqp_channel:wait_for_confirms(
                            Channel,
                            {PublishConfirmationTimeout, millisecond}
                        )
                    of
                        true ->
                            ok;
                        false ->
                            erlang:error(
                                {recoverable_error,
                                    <<"RabbitMQ: Got NACK when waiting for message acknowledgment.">>}
                            );
                        timeout ->
                            erlang:error(
                                {recoverable_error,
                                    <<"RabbitMQ: Timeout when waiting for message acknowledgment.">>}
                            )
                    end;
                false ->
                    ok
            end;
        error ->
            erlang:error(
                {recoverable_error, {<<"RabbitMQ: channel_not_found">>, Conn, RabbitMQ}}
            )
    end.

format_data([], Msg) ->
    emqx_utils_json:encode(Msg);
format_data(Tokens, Msg) ->
    emqx_placeholder:proc_tmpl(Tokens, Msg).

render_data(Msg, BaseProps, ChanState) ->
    #{
        payload_template := PayloadTemplate,
        headers_template := HeadersTemplate,
        properties_template := PropsTemplate
    } = ChanState,
    Payload = format_data(PayloadTemplate, Msg),
    Props1 = add_rendered_props(PropsTemplate, Msg, BaseProps),
    Headers = lists:map(
        fun(Tpl) ->
            {K, V} = render_kv_template(Tpl, Msg),
            %% should we support other types?
            {K, binary, V}
        end,
        HeadersTemplate
    ),
    Props = Props1#'P_basic'{headers = Headers},
    {Payload, Props}.

%% Dynamic `exchange` and `routing_key` are restricted in batch mode,
%% we assume these two values ​​are the same in a batch.
render_template({fixed, Data}, _) ->
    Data;
render_template(Template, [Req | _]) ->
    render_template(Template, Req);
render_template({dynamic, Template}, {_, Message}) ->
    try
        erlang:iolist_to_binary(emqx_template:render_strict(Template, {emqx_jsonish, Message}))
    catch
        error:_Errors ->
            erlang:throw(bad_template)
    end.

handle_result({error, ecpool_empty}) ->
    {error, {recoverable_error, ecpool_empty}};
handle_result(Res) ->
    Res.

make_channel(PoolName, ChannelId, Params) ->
    Conns = get_rabbitmq_connections(PoolName),
    IndexedConns = lists:enumerate(Conns),
    make_channel(IndexedConns, ChannelId, Params, #{}).

make_channel([], _ChannelId, _Param, Acc) ->
    {ok, Acc};
make_channel([{Idx, Conn} | Conns], ChannelId, Params, Acc) ->
    maybe
        ?tp("rabbitmq_will_make_channel", #{}),
        {ok, RabbitMQChannel} ?= open_channel(Conn),
        make_channel_cont([{Idx, Conn} | Conns], RabbitMQChannel, ChannelId, Params, Acc)
    else
        Error ->
            {Error, Acc}
    end.

make_channel_cont([{Idx, Conn} | Conns], RabbitMQChannel, ChannelId, Params, Acc) ->
    NewAcc = Acc#{Conn => RabbitMQChannel},
    maybe
        ok ?= try_confirm_channel(Params, RabbitMQChannel),
        ok ?= try_subscribe(Params, RabbitMQChannel, Idx, ChannelId),
        make_channel(Conns, ChannelId, Params, NewAcc)
    else
        Error ->
            {Error, NewAcc}
    end.

%% We need to enable confirmations if we want to wait for them

try_confirm_channel(#{wait_for_publish_confirmations := true}, Channel) ->
    try
        ?tp("rabbitmq_will_confirm_channel", #{}),
        case amqp_channel:call(Channel, #'confirm.select'{}) of
            #'confirm.select_ok'{} ->
                ok;
            Error ->
                Reason =
                    iolist_to_binary(
                        io_lib:format(
                            "Could not enable RabbitMQ confirmation mode ~p",
                            [Error]
                        )
                    ),
                {error, Reason}
        end
    catch
        Kind:Reason0:Stacktrace ->
            {error, #{kind => Kind, reason => Reason0, stacktrace => Stacktrace}}
    end;
try_confirm_channel(#{wait_for_publish_confirmations := false}, _Channel) ->
    ok.

%% Initialize Rabbitmq's secret library so that the password is encrypted
%% in the log files.
init_secret() ->
    case credentials_obfuscation:secret() of
        ?PENDING_SECRET ->
            Bytes = crypto:strong_rand_bytes(128),
            %% The password can appear in log files if we don't do this
            credentials_obfuscation:set_secret(Bytes);
        _ ->
            %% Already initialized
            ok
    end.

preproc_parameter(_ActionResId, #{config_root := actions, parameters := Parameter}) ->
    #{
        payload_template := PayloadTemplate,
        delivery_mode := InitialDeliveryMode,
        exchange := Exchange,
        routing_key := RoutingKey,
        headers_template := HeadersTemplate,
        properties_template := PropsTemplate
    } = Parameter,
    Parameter#{
        delivery_mode => delivery_mode(InitialDeliveryMode),
        payload_template => emqx_placeholder:preproc_tmpl(PayloadTemplate),
        config_root => actions,
        exchange := preproc_template(Exchange),
        routing_key := preproc_template(RoutingKey),
        headers_template := lists:map(fun parse_kv_template/1, HeadersTemplate),
        properties_template := lists:map(fun parse_v_template/1, PropsTemplate)
    };
preproc_parameter(SourceResId, #{
    config_root := sources, parameters := Parameter, hookpoints := Hooks
}) ->
    #{namespace := Namespace} = emqx_resource:parse_channel_id(SourceResId),
    Parameter#{
        hookpoints => Hooks,
        namespace => Namespace,
        config_root => sources
    }.

preproc_template(Template0) ->
    Template = emqx_template:parse(Template0),
    case emqx_template:placeholders(Template) of
        [] ->
            {fixed, emqx_utils_conv:bin(Template0)};
        [_ | _] ->
            {dynamic, Template}
    end.

parse_v_template(#{key := K, value := VTpl}) ->
    {K, emqx_template:parse(VTpl)}.

parse_kv_template(#{key := KTpl, value := VTpl}) ->
    #{
        key => emqx_template:parse(KTpl),
        value => emqx_template:parse(VTpl)
    }.

render_kv_template(#{key := KTpl, value := VTpl}, Msg) ->
    Key = render_lax(KTpl, Msg),
    Value = render_lax(VTpl, Msg),
    {Key, Value}.

render_lax(Template, Data) ->
    %% Unknown/undefined values are rendered as empty strings
    VarTrans = fun
        (undefined) -> <<"">>;
        (X) -> X
    end,
    {Rendered, _} = emqx_template:render(Template, {emqx_jsonish, Data}, #{var_trans => VarTrans}),
    case Rendered of
        [Value] -> Value;
        _ -> iolist_to_binary(Rendered)
    end.

add_rendered_props(PropsTemplate, Data, PBasic) ->
    lists:foldl(
        fun
            ({app_id, Tpl}, Acc) ->
                Val = render_lax(Tpl, Data),
                Acc#'P_basic'{app_id = Val};
            ({cluster_id, Tpl}, Acc) ->
                Val = render_lax(Tpl, Data),
                Acc#'P_basic'{cluster_id = Val};
            ({content_encoding, Tpl}, Acc) ->
                Val = render_lax(Tpl, Data),
                Acc#'P_basic'{content_encoding = Val};
            ({content_type, Tpl}, Acc) ->
                Val = render_lax(Tpl, Data),
                Acc#'P_basic'{content_type = Val};
            ({correlation_id, Tpl}, Acc) ->
                Val = render_lax(Tpl, Data),
                Acc#'P_basic'{correlation_id = Val};
            ({expiration, Tpl}, Acc) ->
                Val = render_lax(Tpl, Data),
                Acc#'P_basic'{expiration = Val};
            ({message_id, Tpl}, Acc) ->
                Val = render_lax(Tpl, Data),
                Acc#'P_basic'{message_id = Val};
            ({reply_to, Tpl}, Acc) ->
                Val = render_lax(Tpl, Data),
                Acc#'P_basic'{reply_to = Val};
            ({timestamp, Tpl}, Acc) ->
                Val0 = render_lax(Tpl, Data),
                Val = try_cast_timestamp_to_integer(Val0),
                Acc#'P_basic'{timestamp = Val};
            ({type, Tpl}, Acc) ->
                Val = render_lax(Tpl, Data),
                Acc#'P_basic'{type = Val};
            ({user_id, Tpl}, Acc) ->
                Val = render_lax(Tpl, Data),
                Acc#'P_basic'{user_id = Val}
        end,
        PBasic,
        PropsTemplate
    ).

try_cast_timestamp_to_integer(I) when is_integer(I) ->
    I;
try_cast_timestamp_to_integer(Bin) when is_binary(Bin) ->
    try
        binary_to_integer(Bin)
    catch
        error:badarg ->
            throw({unrecoverable_error, {bad_timestamp_value, Bin}})
    end.

delivery_mode(non_persistent) -> 1;
delivery_mode(persistent) -> 2.

to_ssl_options(#{ssl := #{enable := true} = SSLOpts}) ->
    emqx_tls_lib:to_client_opts(SSLOpts);
to_ssl_options(_) ->
    none.

get_rabbitmq_connections(PoolName) ->
    lists:filtermap(
        fun({_Name, Worker}) ->
            case ecpool_worker:client(Worker) of
                {ok, Conn} -> {true, Conn};
                _ -> false
            end
        end,
        ecpool:workers(PoolName)
    ).

open_channel(Conn) ->
    try
        amqp_connection:open_channel(Conn)
    catch
        Kind:Reason0:Stacktrace ->
            {error, #{kind => Kind, reason => Reason0, stacktrace => Stacktrace}}
    end.

try_subscribe(
    #{queue := Queue, no_ack := NoAck, config_root := sources} = Params,
    RabbitChan,
    Idx,
    ChannelId
) ->
    try
        Opts = #{
            id => Idx,
            chan_pid => RabbitChan,
            source_res_id => ChannelId,
            subscription_params => Params
        },
        {ok, ConsumePid} = emqx_bridge_rabbitmq_sup:ensure_started(ChannelId, Opts),
        BasicConsume = #'basic.consume'{queue = Queue, no_ack = NoAck},
        #'basic.consume_ok'{consumer_tag = _} =
            amqp_channel:subscribe(RabbitChan, BasicConsume, ConsumePid),
        ok
    catch
        Kind:Reason0:Stacktrace ->
            {error, #{kind => Kind, reason => Reason0, stacktrace => Stacktrace}}
    end;
try_subscribe(#{config_root := actions}, _RabbitChan, _Idx, _ChannelId) ->
    ok.

try_unsubscribe(ChannelId, Channels) ->
    case emqx_utils_maps:deep_find([ChannelId, rabbitmq], Channels) of
        {ok, RabbitMQ} ->
            Pids = maps:values(RabbitMQ),
            ok = close_channels(Pids),
            _ = emqx_bridge_rabbitmq_sup:ensure_deleted(ChannelId),
            ok;
        _ ->
            ok
    end.

close_channels(Pids) ->
    try
        emqx_utils:pforeach(
            fun(Pid) -> amqp_channel:close(Pid) end, Pids, ?CHANNEL_CLOSE_TIMEOUT
        )
    catch
        _:_ ->
            ok
    end.

channel_health_check_cont(_ChanResId, #{kind := action} = _ChanState, _ConnState) ->
    ?status_connected;
channel_health_check_cont(ChanResId, #{kind := source} = ChanState, ConnState) ->
    #{pool_size := PoolSize} = ConnState,
    #{health_check_timeout := HCTimeout} = ChanState,
    Keys = optvar_keys(ChanResId, PoolSize),
    case optvar:wait_vars(Keys, HCTimeout) of
        ok ->
            do_fold_optvar_results(Keys);
        {timeout, _} ->
            {?status_disconnected, ~"rabbitmq_source_workers_health_check_timeout"}
    end.

optvar_keys(ChanResId, PoolSize) ->
    [?OPTVAR_CHAN_HEALTH(ChanResId, Idx) || Idx <- lists:seq(1, PoolSize)].

do_fold_optvar_results(Keys) ->
    Results = [optvar:peek(Key) || Key <- Keys],
    Errors0 = lists:filtermap(
        fun
            ({ok, ?status_connected}) ->
                false;
            ({ok, {?status_connected, _}}) ->
                false;
            ({ok, {Status, Reason}}) when
                ?IS_HEALTH_CHECK_STATUS(Status),
                Status /= ?status_connected
            ->
                {true, {Status, Reason}};
            ({ok, Status}) when
                ?IS_HEALTH_CHECK_STATUS(Status),
                Status /= ?status_connected
            ->
                {true, Status};
            ({ok, Reason}) ->
                %% shouldn't happen; it's a code bug.
                {true, {?status_disconnected, Reason}};
            (_) ->
                %% not ok should be a very rare race condition, since we wait for all
                %% optvars before peeking.
                false
        end,
        Results
    ),
    case sort_status(Errors0) of
        [{Status, Reason} | _] ->
            {Status, Reason};
        [Status | _] ->
            Status;
        [] ->
            ?status_connected
    end.

%% {disconnected, unhealthy_target} > disconnected > connecting > connected
sort_status(Errors0) ->
    ToStatus = fun
        ({S, {unhealthy_target, _}}) -> {S, unhealthy_target};
        ({S, _Reason}) -> S;
        (S) -> S
    end,
    CompareFn = fun(S1A, S2A) ->
        S1 = ToStatus(S1A),
        S2 = ToStatus(S2A),
        case S1 == S2 of
            true ->
                case {S1A, S2A} of
                    {{_, _}, _} when not is_tuple(S2A) ->
                        %% prefer those with reason
                        true;
                    _ ->
                        false
                end;
            false ->
                S1 > S2
        end
    end,
    lists:sort(CompareFn, Errors0).

allocate(ConnResId, Key, Value) ->
    ok = emqx_resource:allocate_resource(ConnResId, ?MODULE, Key, Value).

deallocate(ConnResId, Key) ->
    ok = emqx_resource:deallocate_resource(ConnResId, Key).
