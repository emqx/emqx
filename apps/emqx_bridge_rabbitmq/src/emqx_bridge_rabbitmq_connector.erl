%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Internal callbacks
-export([publish_messages/5]).

namespace() -> "rabbitmq".

%% bridge v1
roots() ->
    [{config, #{type => hoconsc:ref(?MODULE, config)}}].

%% bridge v1 called by emqx_bridge_rabbitmq
fields(config) ->
    emqx_bridge_rabbitmq_connector_schema:fields(connector) ++
        emqx_bridge_rabbitmq_pubsub_schema:fields(action_parameters).

-define(GET_STATUS_TIMEOUT, 10_000).
%% Less than ?T_OPERATION of emqx_resource_manager
-define(CHANNEL_CLOSE_TIMEOUT, 4_000).

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
    Options = [
        {config, Config},
        {pool_size, maps:get(pool_size, Config)},
        {pool, InstanceId}
    ],
    case emqx_resource_pool:start(InstanceId, ?MODULE, Options) of
        ok ->
            {ok, #{channels => #{}}};
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "rabbitmq_connector_start_failed",
                reason => Reason,
                config => emqx_utils:redact(Config)
            }),
            {error, Reason}
    end.

on_add_channel(
    InstanceId,
    #{channels := Channels} = State,
    ChannelId,
    Config
) ->
    case maps:is_key(ChannelId, Channels) of
        true ->
            {error, already_exists};
        false ->
            ProcParam = preproc_parameter(ChannelId, Config),
            case make_channel(InstanceId, ChannelId, ProcParam) of
                {ok, RabbitChannels} ->
                    Channel = #{param => ProcParam, rabbitmq => RabbitChannels},
                    NewChannels = maps:put(ChannelId, Channel, Channels),
                    {ok, State#{channels => NewChannels}};
                {error, Error} ->
                    ?SLOG(error, #{
                        msg => "failed_to_start_rabbitmq_channel",
                        instance_id => InstanceId,
                        params => emqx_utils:redact(Config),
                        error => Error
                    }),
                    {error, Error}
            end
    end.

on_remove_channel(_InstanceId, #{channels := Channels} = State, ChannelId) ->
    try_unsubscribe(ChannelId, Channels),
    {ok, State#{channels => maps:remove(ChannelId, Channels)}}.

on_get_channels(InstanceId) ->
    emqx_bridge_v2:get_channels_for_connector(InstanceId).

on_stop(ResourceID, _State) ->
    ?SLOG(info, #{
        msg => "stopping_rabbitmq_connector",
        connector => ResourceID
    }),
    lists:foreach(
        fun({_Name, Worker}) ->
            case ecpool_worker:client(Worker) of
                {ok, Conn} -> amqp_connection:close(Conn);
                _ -> ok
            end
        end,
        ecpool:workers(ResourceID)
    ),
    emqx_resource_pool:stop(ResourceID).

%% This is the callback function that is called by ecpool
-spec connect(term()) -> {ok, {pid(), pid()}, map()} | {error, term()}.
connect(Options) ->
    Config = proplists:get_value(config, Options),
    #{
        server := Host,
        port := Port,
        username := Username,
        password := WrappedPassword,
        timeout := Timeout,
        virtual_host := VirtualHost,
        heartbeat := Heartbeat
    } = Config,
    %% TODO: teach `amqp` to accept 0-arity closures as passwords.
    Password = emqx_secret:unwrap(WrappedPassword),
    RabbitMQConnOptions =
        #amqp_params_network{
            host = Host,
            port = Port,
            ssl_options = to_ssl_options(Config),
            username = Username,
            password = Password,
            connection_timeout = Timeout,
            virtual_host = VirtualHost,
            heartbeat = Heartbeat
        },
    case amqp_connection:start(RabbitMQConnOptions) of
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
    ChannelNum = maps:size(Channels),
    Conns = get_rabbitmq_connections(PoolName),
    try actual_channel_nums(Conns) of
        ActualNums ->
            Check = lists:all(fun(ActualNum) -> ActualNum >= ChannelNum end, ActualNums),
            case Check of
                true when Conns =/= [] ->
                    ?status_connected;
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

on_get_channel_status(_InstanceId, ChannelId, #{channels := Channels}) ->
    case emqx_utils_maps:deep_find([ChannelId, rabbitmq], Channels) of
        {ok, RabbitMQ} ->
            case lists:all(fun is_process_alive/1, maps:values(RabbitMQ)) of
                true -> ?status_connected;
                false -> {?status_disconnected, not_connected}
            end;
        _ ->
            ?status_disconnected
    end.

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
    make_channel(Conns, ChannelId, Params, #{}).

make_channel([], _ChannelId, _Param, Acc) ->
    {ok, Acc};
make_channel([Conn | Conns], ChannelId, Params, Acc) ->
    maybe
        {ok, RabbitMQChannel} ?= amqp_connection:open_channel(Conn),
        ok ?= try_confirm_channel(Params, RabbitMQChannel),
        ok ?= try_subscribe(Params, RabbitMQChannel, ChannelId),
        NewAcc = Acc#{Conn => RabbitMQChannel},
        make_channel(Conns, ChannelId, Params, NewAcc)
    end.

%% We need to enable confirmations if we want to wait for them
try_confirm_channel(#{wait_for_publish_confirmations := true}, Channel) ->
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

try_subscribe(
    #{queue := Queue, no_ack := NoAck, config_root := sources} = Params,
    RabbitChan,
    ChannelId
) ->
    WorkState = {RabbitChan, ChannelId, Params},
    {ok, ConsumePid} = emqx_bridge_rabbitmq_sup:ensure_started(ChannelId, WorkState),
    BasicConsume = #'basic.consume'{queue = Queue, no_ack = NoAck},
    #'basic.consume_ok'{consumer_tag = _} =
        amqp_channel:subscribe(RabbitChan, BasicConsume, ConsumePid),
    ok;
try_subscribe(#{config_root := actions}, _RabbitChan, _ChannelId) ->
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
