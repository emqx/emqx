%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_rabbitmq_connector).

-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% Needed to create RabbitMQ connection
-include_lib("amqp_client/include/amqp_client.hrl").

-behaviour(emqx_resource).
-behaviour(hocon_schema).
-behaviour(ecpool_worker).

%% hocon_schema callbacks
-export([roots/0, fields/1]).

%% HTTP API callbacks
-export([values/1]).

%% emqx_resource callbacks
-export([
    %% Required callbacks
    on_start/2,
    on_stop/2,
    callback_mode/0,
    %% Optional callbacks
    on_get_status/2,
    on_query/3,
    is_buffer_supported/0,
    on_batch_query/3
]).

%% callbacks for ecpool_worker
-export([connect/1]).

%% Internal callbacks
-export([publish_messages/3]).

roots() ->
    [{config, #{type => hoconsc:ref(?MODULE, config)}}].

fields(config) ->
    [
        {server,
            hoconsc:mk(
                typerefl:binary(),
                #{
                    default => <<"localhost">>,
                    desc => ?DESC("server")
                }
            )},
        {port,
            hoconsc:mk(
                emqx_schema:port_number(),
                #{
                    default => 5672,
                    desc => ?DESC("server")
                }
            )},
        {username,
            hoconsc:mk(
                typerefl:binary(),
                #{
                    required => true,
                    desc => ?DESC("username")
                }
            )},
        {password,
            hoconsc:mk(
                typerefl:binary(),
                #{
                    required => true,
                    desc => ?DESC("password")
                }
            )},
        {pool_size,
            hoconsc:mk(
                typerefl:pos_integer(),
                #{
                    default => 8,
                    desc => ?DESC("pool_size")
                }
            )},
        {timeout,
            hoconsc:mk(
                emqx_schema:duration_ms(),
                #{
                    default => <<"5s">>,
                    desc => ?DESC("timeout")
                }
            )},
        {wait_for_publish_confirmations,
            hoconsc:mk(
                boolean(),
                #{
                    default => true,
                    desc => ?DESC("wait_for_publish_confirmations")
                }
            )},
        {publish_confirmation_timeout,
            hoconsc:mk(
                emqx_schema:duration_ms(),
                #{
                    default => <<"30s">>,
                    desc => ?DESC("timeout")
                }
            )},

        {virtual_host,
            hoconsc:mk(
                typerefl:binary(),
                #{
                    default => <<"/">>,
                    desc => ?DESC("virtual_host")
                }
            )},
        {heartbeat,
            hoconsc:mk(
                emqx_schema:duration_ms(),
                #{
                    default => <<"30s">>,
                    desc => ?DESC("heartbeat")
                }
            )},
        {auto_reconnect,
            hoconsc:mk(
                emqx_schema:duration_ms(),
                #{
                    default => <<"2s">>,
                    desc => ?DESC("auto_reconnect")
                }
            )},
        %% Things related to sending messages to RabbitMQ
        {exchange,
            hoconsc:mk(
                typerefl:binary(),
                #{
                    required => true,
                    desc => ?DESC("exchange")
                }
            )},
        {routing_key,
            hoconsc:mk(
                typerefl:binary(),
                #{
                    required => true,
                    desc => ?DESC("routing_key")
                }
            )},
        {delivery_mode,
            hoconsc:mk(
                hoconsc:enum([non_persistent, persistent]),
                #{
                    default => non_persistent,
                    desc => ?DESC("delivery_mode")
                }
            )},
        {payload_template,
            hoconsc:mk(
                binary(),
                #{
                    default => <<"${.}">>,
                    desc => ?DESC("payload_template")
                }
            )}
    ].

values(post) ->
    maps:merge(values(put), #{name => <<"connector">>});
values(get) ->
    values(post);
values(put) ->
    #{
        server => <<"localhost">>,
        port => 5672,
        enable => true,
        pool_size => 8,
        type => rabbitmq,
        username => <<"guest">>,
        password => <<"******">>,
        routing_key => <<"my_routing_key">>,
        payload_template => <<"">>
    };
values(_) ->
    #{}.

%% ===================================================================
%% Callbacks defined in emqx_resource
%% ===================================================================

%% emqx_resource callback

callback_mode() -> always_sync.

%% emqx_resource callback

-spec is_buffer_supported() -> boolean().
is_buffer_supported() ->
    %% We want to make use of EMQX's buffer mechanism
    false.

%% emqx_resource callback called when the resource is started

-spec on_start(resource_id(), term()) -> {ok, resource_state()} | {error, _}.
on_start(
    InstanceID,
    #{
        pool_size := PoolSize,
        payload_template := PayloadTemplate,
        password := Password,
        delivery_mode := InitialDeliveryMode
    } = InitialConfig
) ->
    DeliveryMode =
        case InitialDeliveryMode of
            non_persistent -> 1;
            persistent -> 2
        end,
    Config = InitialConfig#{
        password => emqx_secret:wrap(Password),
        delivery_mode => DeliveryMode
    },
    ?SLOG(info, #{
        msg => "starting_rabbitmq_connector",
        connector => InstanceID,
        config => emqx_utils:redact(Config)
    }),
    Options = [
        {config, Config},
        %% The pool_size is read by ecpool and decides the number of workers in
        %% the pool
        {pool_size, PoolSize},
        {pool, InstanceID}
    ],
    ProcessedTemplate = emqx_plugin_libs_rule:preproc_tmpl(PayloadTemplate),
    State = #{
        poolname => InstanceID,
        processed_payload_template => ProcessedTemplate,
        config => Config
    },
    case emqx_resource_pool:start(InstanceID, ?MODULE, Options) of
        ok ->
            {ok, State};
        {error, Reason} ->
            LogMessage =
                #{
                    msg => "rabbitmq_connector_start_failed",
                    error_reason => Reason,
                    config => emqx_utils:redact(Config)
                },
            ?SLOG(info, LogMessage),
            {error, Reason}
    end.

%% emqx_resource callback called when the resource is stopped

-spec on_stop(resource_id(), resource_state()) -> term().
on_stop(
    ResourceID,
    #{poolname := PoolName} = _State
) ->
    ?SLOG(info, #{
        msg => "stopping RabbitMQ connector",
        connector => ResourceID
    }),
    Workers = [Worker || {_WorkerName, Worker} <- ecpool:workers(PoolName)],
    Clients = [
        begin
            {ok, Client} = ecpool_worker:client(Worker),
            Client
        end
     || Worker <- Workers
    ],
    %% We need to stop the pool before stopping the workers as the pool monitors the workers
    StopResult = emqx_resource_pool:stop(PoolName),
    lists:foreach(fun stop_worker/1, Clients),
    StopResult.

stop_worker({Channel, Connection}) ->
    amqp_channel:close(Channel),
    amqp_connection:close(Connection).

%% This is the callback function that is called by ecpool when the pool is
%% started

-spec connect(term()) -> {ok, {pid(), pid()}, map()} | {error, term()}.
connect(Options) ->
    Config = proplists:get_value(config, Options),
    try
        create_rabbitmq_connection_and_channel(Config)
    catch
        _:{error, Reason} ->
            ?SLOG(error, #{
                msg => "rabbitmq_connector_connection_failed",
                error_type => error,
                error_reason => Reason,
                config => emqx_utils:redact(Config)
            }),
            {error, Reason};
        Type:Reason ->
            ?SLOG(error, #{
                msg => "rabbitmq_connector_connection_failed",
                error_type => Type,
                error_reason => Reason,
                config => emqx_utils:redact(Config)
            }),
            {error, Reason}
    end.

create_rabbitmq_connection_and_channel(Config) ->
    #{
        server := Host,
        port := Port,
        username := Username,
        password := WrappedPassword,
        timeout := Timeout,
        virtual_host := VirtualHost,
        heartbeat := Heartbeat,
        wait_for_publish_confirmations := WaitForPublishConfirmations
    } = Config,
    Password = emqx_secret:unwrap(WrappedPassword),
    RabbitMQConnectionOptions =
        #amqp_params_network{
            host = erlang:binary_to_list(Host),
            port = Port,
            username = Username,
            password = Password,
            connection_timeout = Timeout,
            virtual_host = VirtualHost,
            heartbeat = Heartbeat
        },
    {ok, RabbitMQConnection} =
        case amqp_connection:start(RabbitMQConnectionOptions) of
            {ok, Connection} ->
                {ok, Connection};
            {error, Reason} ->
                erlang:error({error, Reason})
        end,
    {ok, RabbitMQChannel} =
        case amqp_connection:open_channel(RabbitMQConnection) of
            {ok, Channel} ->
                {ok, Channel};
            {error, OpenChannelErrorReason} ->
                erlang:error({error, OpenChannelErrorReason})
        end,
    %% We need to enable confirmations if we want to wait for them
    case WaitForPublishConfirmations of
        true ->
            case amqp_channel:call(RabbitMQChannel, #'confirm.select'{}) of
                #'confirm.select_ok'{} ->
                    ok;
                Error ->
                    ConfirmModeErrorReason =
                        erlang:iolist_to_binary(
                            io_lib:format(
                                "Could not enable RabbitMQ confirmation mode ~p",
                                [Error]
                            )
                        ),
                    erlang:error({error, ConfirmModeErrorReason})
            end;
        false ->
            ok
    end,
    {ok, {RabbitMQConnection, RabbitMQChannel}, #{
        supervisees => [RabbitMQConnection, RabbitMQChannel]
    }}.

%% emqx_resource callback called to check the status of the resource

-spec on_get_status(resource_id(), term()) ->
    {connected, resource_state()} | {disconnected, resource_state(), binary()}.
on_get_status(
    _InstId,
    #{
        poolname := PoolName
    } = State
) ->
    Workers = [Worker || {_WorkerName, Worker} <- ecpool:workers(PoolName)],
    Clients = [
        begin
            {ok, Client} = ecpool_worker:client(Worker),
            Client
        end
     || Worker <- Workers
    ],
    CheckResults = [
        check_worker(Client)
     || Client <- Clients
    ],
    Connected = length(CheckResults) > 0 andalso lists:all(fun(R) -> R end, CheckResults),
    case Connected of
        true ->
            {connected, State};
        false ->
            {disconnected, State, <<"not_connected">>}
    end;
on_get_status(
    _InstId,
    State
) ->
    {disconnect, State, <<"not_connected: no connection pool in state">>}.

check_worker({Channel, Connection}) ->
    erlang:is_process_alive(Channel) andalso erlang:is_process_alive(Connection).

%% emqx_resource callback that is called when a non-batch query is received

-spec on_query(resource_id(), Request, resource_state()) -> query_result() when
    Request :: {RequestType, Data},
    RequestType :: send_message,
    Data :: map().
on_query(
    ResourceID,
    {RequestType, Data},
    #{
        poolname := PoolName,
        processed_payload_template := PayloadTemplate,
        config := Config
    } = State
) ->
    ?SLOG(debug, #{
        msg => "RabbitMQ connector received query",
        connector => ResourceID,
        type => RequestType,
        data => Data,
        state => emqx_utils:redact(State)
    }),
    MessageData = format_data(PayloadTemplate, Data),
    ecpool:pick_and_do(
        PoolName,
        {?MODULE, publish_messages, [Config, [MessageData]]},
        no_handover
    ).

%% emqx_resource callback that is called when a batch query is received

-spec on_batch_query(resource_id(), BatchReq, resource_state()) -> query_result() when
    BatchReq :: nonempty_list({'send_message', map()}).
on_batch_query(
    ResourceID,
    BatchReq,
    State
) ->
    ?SLOG(debug, #{
        msg => "RabbitMQ connector received batch query",
        connector => ResourceID,
        data => BatchReq,
        state => emqx_utils:redact(State)
    }),
    %% Currently we only support batch requests with the send_message key
    {Keys, MessagesToInsert} = lists:unzip(BatchReq),
    ensure_keys_are_of_type_send_message(Keys),
    %% Pick out the payload template
    #{
        processed_payload_template := PayloadTemplate,
        poolname := PoolName,
        config := Config
    } = State,
    %% Create batch payload
    FormattedMessages = [
        format_data(PayloadTemplate, Data)
     || Data <- MessagesToInsert
    ],
    %% Publish the messages
    ecpool:pick_and_do(
        PoolName,
        {?MODULE, publish_messages, [Config, FormattedMessages]},
        no_handover
    ).

publish_messages(
    {_Connection, Channel},
    #{
        delivery_mode := DeliveryMode,
        routing_key := RoutingKey,
        exchange := Exchange,
        wait_for_publish_confirmations := WaitForPublishConfirmations,
        publish_confirmation_timeout := PublishConfirmationTimeout
    } = _Config,
    Messages
) ->
    MessageProperties = #'P_basic'{
        headers = [],
        delivery_mode = DeliveryMode
    },
    Method = #'basic.publish'{
        exchange = Exchange,
        routing_key = RoutingKey
    },
    _ = [
        amqp_channel:cast(
            Channel,
            Method,
            #amqp_msg{
                payload = Message,
                props = MessageProperties
            }
        )
     || Message <- Messages
    ],
    case WaitForPublishConfirmations of
        true ->
            case amqp_channel:wait_for_confirms(Channel, PublishConfirmationTimeout) of
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
    end.

ensure_keys_are_of_type_send_message(Keys) ->
    case lists:all(fun is_send_message_atom/1, Keys) of
        true ->
            ok;
        false ->
            erlang:error(
                {unrecoverable_error,
                    <<"Unexpected type for batch message (Expected send_message)">>}
            )
    end.

is_send_message_atom(send_message) ->
    true;
is_send_message_atom(_) ->
    false.

format_data([], Msg) ->
    emqx_utils_json:encode(Msg);
format_data(Tokens, Msg) ->
    emqx_plugin_libs_rule:proc_tmpl(Tokens, Msg).
