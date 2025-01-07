%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_pulsar_connector).

-include("emqx_bridge_pulsar.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% `emqx_resource' API
-export([
    resource_type/0,
    callback_mode/0,
    query_mode/1,
    query_opts/1,
    on_start/2,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channels/1,
    on_stop/2,
    on_get_status/2,
    on_get_channel_status/3,
    on_query/3,
    on_query_async/4,
    on_format_query_result/1
]).

-export([on_pulsar_ack/2]).
-export([handle_telemetry_event/4]).

-type pulsar_client_id() :: atom().
-type state() :: #{
    client_id := pulsar_client_id(),
    channels := map(),
    client_opts := map()
}.

-type message_template_raw() :: #{
    key := binary(),
    value := binary()
}.
-type message_template() :: #{
    key := emqx_placeholder:tmpl_token(),
    value := emqx_placeholder:tmpl_token()
}.
-type config() :: #{
    authentication := _,
    bridge_name := atom(),
    servers := binary(),
    ssl := _
}.

%% Allocatable resources
-define(pulsar_client_id, pulsar_client_id).
-define(pulsar_producers, pulsar_producers).
-define(telemetry_handler_id, telemetry_handler_id).

-define(HEALTH_CHECK_RETRY_TIMEOUT, 4_000).

%%-------------------------------------------------------------------------------------
%% `emqx_resource' API
%%-------------------------------------------------------------------------------------
resource_type() -> pulsar.

callback_mode() -> async_if_possible.

query_mode(#{resource_opts := #{query_mode := sync}}) ->
    simple_sync_internal_buffer;
query_mode(_Config) ->
    simple_async_internal_buffer.

query_opts(#{resource_opts := #{query_mode := sync}, parameters := #{sync_timeout := Timeout}}) ->
    #{timeout => Timeout};
query_opts(_) ->
    #{}.

-spec on_start(connector_resource_id(), config()) -> {ok, state()}.
on_start(ConnResId, Config) ->
    #{servers := Servers0, ssl := SSL} = Config,
    Servers = format_servers(Servers0),
    ClientId = make_client_id(ConnResId),
    ok = emqx_resource:allocate_resource(ConnResId, ?pulsar_client_id, ClientId),
    SSLOpts = emqx_tls_lib:to_client_opts(SSL),
    ConnectTimeout = maps:get(connect_timeout, Config, timer:seconds(10)),
    ClientOpts = #{
        connect_timeout => ConnectTimeout,
        ssl_opts => SSLOpts,
        conn_opts => conn_opts(Config)
    },
    case pulsar:ensure_supervised_client(ClientId, Servers, ClientOpts) of
        {ok, _} ->
            ?tp(
                info,
                "pulsar_client_started",
                #{
                    instance_id => ConnResId,
                    pulsar_hosts => Servers
                }
            );
        {error, Reason} ->
            RedactedReason = emqx_utils:redact(Reason, fun is_sensitive_key/1),
            ?SLOG(error, #{
                msg => "failed_to_start_pulsar_client",
                instance_id => ConnResId,
                pulsar_hosts => Servers,
                reason => RedactedReason
            }),
            Message =
                case get_error_message(RedactedReason) of
                    {ok, Msg} -> Msg;
                    error -> failed_to_start_pulsar_client
                end,
            throw(Message)
    end,
    {ok, #{channels => #{}, client_id => ClientId, client_opts => ClientOpts}}.

on_add_channel(
    ConnResId,
    #{channels := Channels, client_id := ClientId, client_opts := ClientOpts} = State,
    ActionResId,
    #{parameters := #{message := Message, sync_timeout := SyncTimeout} = Params}
) ->
    case maps:is_key(ActionResId, Channels) of
        true ->
            {error, channel_already_exists};
        false ->
            {ok, Producers} = start_producer(ConnResId, ActionResId, ClientId, ClientOpts, Params),
            Parameters = #{
                message => compile_message_template(Message),
                sync_timeout => SyncTimeout,
                producers => Producers
            },
            NewChannels = maps:put(ActionResId, Parameters, Channels),
            {ok, State#{channels => NewChannels}}
    end.

on_remove_channel(ConnResId, State, ActionResId) ->
    #{channels := Channels} = State,
    case maps:find(ActionResId, Channels) of
        {ok, #{producers := Producers}} ->
            stop_producers(ActionResId, Producers),
            emqx_resource:deallocate_resource(ConnResId, {?pulsar_producers, ActionResId}),
            deallocate_telemetry_handlers(ConnResId, ActionResId),
            {ok, State#{channels => maps:remove(ActionResId, Channels)}};
        error ->
            {ok, State}
    end.

on_get_channels(ConnResId) ->
    emqx_bridge_v2:get_channels_for_connector(ConnResId).

-spec on_stop(resource_id(), state()) -> ok.
on_stop(ConnResId, _State) ->
    Resources = emqx_resource:get_allocated_resources(ConnResId),
    maps:foreach(
        fun
            ({?pulsar_producers, ActionResId}, Producers) ->
                stop_producers(ActionResId, Producers);
            (_, _) ->
                ok
        end,
        Resources
    ),
    maps:foreach(
        fun
            ({?telemetry_handler_id, _ActionResId}, TelemetryId) ->
                deallocate_telemetry_handlers(ConnResId, TelemetryId);
            (_, _) ->
                ok
        end,
        Resources
    ),
    maps:foreach(
        fun
            (?pulsar_client_id, ClientId) ->
                stop_client(ClientId);
            (_, _) ->
                ok
        end,
        Resources
    ),
    ?tp(pulsar_bridge_stopped, #{instance_id => ConnResId}),
    ok.

%% Note: since Pulsar client has its own replayq that is not managed by
%% `emqx_resource_buffer_worker', we must avoid returning `disconnected' here.  Otherwise,
%% `emqx_resource_manager' will kill the Pulsar producers and messages might be lost.
-spec on_get_status(resource_id(), state()) -> connected | connecting.
on_get_status(_ConnResId, State = #{}) ->
    #{client_id := ClientId} = State,
    try pulsar_client_manager:get_status(ClientId, 5_000) of
        true -> ?status_connected;
        false -> ?status_connecting
    catch
        exit:{timeout, _} ->
            ?status_connecting;
        exit:{noproc, _} ->
            ?status_connecting
    end;
on_get_status(_ConnResId, _State) ->
    %% If a health check happens just after a concurrent request to
    %% create the bridge is not quite finished, `State = undefined'.
    ?status_connecting.

on_get_channel_status(_ConnResId, ActionResId, #{channels := Channels}) ->
    case maps:find(ActionResId, Channels) of
        {ok, #{producers := Producers}} ->
            get_producer_status(Producers);
        error ->
            {error, channel_not_found}
    end.

-spec on_query(resource_id(), tuple(), state()) ->
    {ok, term()}
    | {error, timeout}
    | {error, term()}.
on_query(_ConnResId, {ActionResId, Message}, State) ->
    #{channels := Channels} = State,
    case maps:find(ActionResId, Channels) of
        error ->
            {error, channel_not_found};
        {ok, #{message := MessageTmpl, sync_timeout := SyncTimeout, producers := Producers}} ->
            PulsarMessage = render_message(Message, MessageTmpl),
            emqx_trace:rendered_action_template(ActionResId, #{
                message => PulsarMessage,
                sync_timeout => SyncTimeout,
                is_async => false
            }),
            ?tp_span(
                "pulsar_producer_query_enter",
                #{instance_id => _ConnResId, message => Message, mode => sync},
                try
                    ?tp("pulsar_producer_send", #{msg => PulsarMessage, mode => sync}),
                    pulsar:send_sync(Producers, [PulsarMessage], SyncTimeout)
                catch
                    error:timeout ->
                        {error, timeout}
                end
            )
    end.

-spec on_query_async(
    resource_id(), tuple(), {ReplyFun :: function(), Args :: list()}, state()
) ->
    {ok, pid()}.
on_query_async(_ConnResId, {ActionResId, Message}, AsyncReplyFn, State) ->
    #{channels := Channels} = State,
    case maps:find(ActionResId, Channels) of
        error ->
            {error, {unrecoverable_error, channel_not_found}};
        {ok, #{message := MessageTmpl, producers := Producers}} ->
            ?tp_span(
                "pulsar_producer_query_enter",
                #{instance_id => _ConnResId, message => Message, mode => async},
                on_query_async2(ActionResId, Producers, Message, MessageTmpl, AsyncReplyFn)
            )
    end.

on_query_async2(ActionResId, Producers, Message, MessageTmpl, AsyncReplyFn) ->
    PulsarMessage = render_message(Message, MessageTmpl),
    emqx_trace:rendered_action_template(ActionResId, #{
        message => PulsarMessage,
        is_async => true
    }),
    CallbackFn = {fun ?MODULE:on_pulsar_ack/2, [AsyncReplyFn]},
    ?tp("pulsar_producer_send", #{msg => PulsarMessage, mode => async}),
    pulsar:send(Producers, [PulsarMessage], #{callback_fn => CallbackFn}).

on_format_query_result({ok, Info}) ->
    #{result => ok, info => Info};
on_format_query_result(Result) ->
    Result.

on_pulsar_ack(_ReplyFnAndArgs, {error, Reason}) when
    Reason =:= expired;
    Reason =:= overflow
->
    %% We already bumped the dropped counter in `handle_telemetry_event/4', so no need to
    %% call the wrapping callback here (it would bump the failure counter).
    ok;
on_pulsar_ack(ReplyFnAndArgs, Result) ->
    emqx_resource:apply_reply_fun(ReplyFnAndArgs, Result).

%%-------------------------------------------------------------------------------------
%% `telemetry' API
%%-------------------------------------------------------------------------------------

%% we *must* match the bridge id in the event metadata with that in
%% the handler config; otherwise, multiple pulsar producer bridges will
%% install multiple handlers to the same pulsar events, multiplying the
handle_telemetry_event(
    [pulsar, dropped],
    #{counter_inc := Val, reason := queue_full},
    #{action_id := ID},
    #{action_id := ID}
) when is_integer(Val) ->
    emqx_resource_metrics:dropped_queue_full_inc(ID, Val);
handle_telemetry_event(
    [pulsar, dropped],
    #{counter_inc := Val, reason := expired},
    #{action_id := ID},
    #{action_id := ID}
) when is_integer(Val) ->
    emqx_resource_metrics:dropped_expired_inc(ID, Val);
handle_telemetry_event(
    [pulsar, queuing],
    #{gauge_set := Val},
    #{action_id := ID, partition_topic := PartitionTopic},
    #{action_id := ID}
) when is_integer(Val) ->
    emqx_resource_metrics:queuing_set(ID, PartitionTopic, Val);
handle_telemetry_event(
    [pulsar, queuing_bytes],
    #{gauge_set := Val},
    #{action_id := ID, partition_topic := PartitionTopic},
    #{action_id := ID}
) when is_integer(Val) ->
    emqx_resource_metrics:queuing_bytes_set(ID, PartitionTopic, Val);
handle_telemetry_event(
    [pulsar, retried],
    #{counter_inc := Val},
    #{action_id := ID},
    #{action_id := ID}
) when is_integer(Val) ->
    emqx_resource_metrics:retried_inc(ID, Val);
handle_telemetry_event(
    [pulsar, inflight],
    #{gauge_set := Val},
    #{action_id := ID, partition_topic := PartitionTopic},
    #{action_id := ID}
) when is_integer(Val) ->
    emqx_resource_metrics:inflight_set(ID, PartitionTopic, Val);
handle_telemetry_event(_EventId, _Metrics, _Metadata, _HandlerConfig) ->
    %% Event that we do not handle
    ok.

%%-------------------------------------------------------------------------------------
%% Internal fns
%%-------------------------------------------------------------------------------------

-spec format_servers(binary()) -> [string()].
format_servers(Servers0) ->
    lists:map(
        fun(#{scheme := Scheme, hostname := Host, port := Port}) ->
            Scheme ++ "://" ++ Host ++ ":" ++ integer_to_list(Port)
        end,
        emqx_schema:parse_servers(Servers0, ?PULSAR_HOST_OPTIONS)
    ).

-spec make_client_id(resource_id()) -> pulsar_client_id().
make_client_id(ConnResId) ->
    case emqx_resource:is_dry_run(ConnResId) of
        true ->
            pulsar_producer_probe;
        false ->
            {pulsar, Name} = emqx_connector_resource:parse_connector_id(ConnResId),
            ClientIdBin = iolist_to_binary([
                <<"pulsar:">>,
                emqx_utils_conv:bin(Name),
                <<":">>,
                emqx_utils_conv:bin(node())
            ]),
            binary_to_atom(ClientIdBin)
    end.

conn_opts(#{authentication := none}) ->
    #{};
conn_opts(#{authentication := #{username := Username, password := Password}}) ->
    #{
        %% TODO: teach `pulsar` to accept 0-arity closures as passwords.
        auth_data => iolist_to_binary([Username, <<":">>, emqx_secret:unwrap(Password)]),
        auth_method_name => <<"basic">>
    };
conn_opts(#{authentication := #{jwt := JWT}}) ->
    #{
        %% TODO: teach `pulsar` to accept 0-arity closures as passwords.
        auth_data => emqx_secret:unwrap(JWT),
        auth_method_name => <<"token">>
    }.

-spec replayq_dir(pulsar_client_id()) -> string().
replayq_dir(ClientId) ->
    filename:join([emqx:data_dir(), "pulsar", emqx_utils_conv:bin(ClientId)]).

producer_name(ConnResId, ActionResId) ->
    case emqx_resource:is_dry_run(ConnResId) of
        %% do not create more atom
        true ->
            pulsar_producer_probe_worker;
        false ->
            ActionResIdBin = emqx_utils_conv:bin(ActionResId),
            binary_to_atom(
                iolist_to_binary([
                    <<"producer-">>,
                    ActionResIdBin
                ])
            )
    end.

start_producer(ConnResId, ActionResId, ClientId, ClientOpts, Params) ->
    #{
        conn_opts := ConnOpts,
        ssl_opts := SSLOpts
    } = ClientOpts,
    #{
        batch_size := BatchSize,
        buffer := #{
            mode := BufferMode,
            per_partition_limit := PerPartitionLimit,
            segment_bytes := SegmentBytes,
            memory_overload_protection := MemOLP0
        },
        compression := Compression,
        max_batch_bytes := MaxBatchBytes,
        pulsar_topic := PulsarTopic,
        retention_period := RetentionPeriod,
        send_buffer := SendBuffer,
        strategy := Strategy
    } = Params,
    {OffloadMode, ReplayQDir} =
        case BufferMode of
            memory -> {false, false};
            disk -> {false, replayq_dir(ActionResId)};
            hybrid -> {true, replayq_dir(ActionResId)}
        end,
    MemOLP =
        case os:type() of
            {unix, linux} -> MemOLP0;
            _ -> false
        end,
    ReplayQOpts = #{
        replayq_dir => ReplayQDir,
        replayq_offload_mode => OffloadMode,
        replayq_max_total_bytes => PerPartitionLimit,
        replayq_seg_bytes => SegmentBytes,
        drop_if_highmem => MemOLP
    },
    ProducerName = producer_name(ConnResId, ActionResId),
    ?tp(pulsar_producer_capture_name, #{producer_name => ProducerName}),
    ProducerOpts0 =
        #{
            batch_size => BatchSize,
            compression => Compression,
            conn_opts => ConnOpts,
            max_batch_bytes => MaxBatchBytes,
            name => ProducerName,
            retention_period => RetentionPeriod,
            ssl_opts => SSLOpts,
            strategy => partition_strategy(Strategy),
            tcp_opts => [{sndbuf, SendBuffer}],
            telemetry_metadata => #{action_id => ActionResId}
        },
    ProducerOpts = maps:merge(ReplayQOpts, ProducerOpts0),
    ?tp(pulsar_producer_about_to_start_producers, #{producer_name => ProducerName}),
    ok = emqx_resource:allocate_resource(
        ConnResId,
        {?telemetry_handler_id, ActionResId},
        ActionResId
    ),
    _ = maybe_install_telemetry_handlers(ActionResId),
    try pulsar:ensure_supervised_producers(ClientId, PulsarTopic, ProducerOpts) of
        {ok, Producers} ->
            ok = emqx_resource:allocate_resource(
                ConnResId,
                {?pulsar_producers, ActionResId},
                Producers
            ),
            ?tp(pulsar_producer_producers_allocated, #{}),
            ?tp(pulsar_producer_bridge_started, #{}),
            {ok, Producers}
    catch
        Kind:Error:Stacktrace ->
            ?tp(
                error,
                "failed_to_start_pulsar_producer",
                #{
                    instance_id => ConnResId,
                    kind => Kind,
                    reason => emqx_utils:redact(Error, fun is_sensitive_key/1),
                    stacktrace => Stacktrace
                }
            ),
            ?tp(pulsar_bridge_producer_stopped, #{
                pulsar_client_id => ClientId,
                producers => undefined
            }),
            _ = uninstall_telemetry_handlers(ActionResId),
            throw(failed_to_start_pulsar_producer)
    end.

-spec stop_client(pulsar_client_id()) -> ok.
stop_client(ClientId) ->
    _ = log_when_error(
        fun() ->
            ok = pulsar:stop_and_delete_supervised_client(ClientId),
            ?tp(pulsar_bridge_client_stopped, #{pulsar_client_id => ClientId}),
            ok
        end,
        #{
            msg => "failed_to_delete_pulsar_client",
            pulsar_client_id => ClientId
        }
    ),
    ok.

-spec stop_producers(action_resource_id(), pulsar_producers:producers()) -> ok.
stop_producers(ActionResId, Producers) ->
    _ = log_when_error(
        fun() ->
            ok = pulsar:stop_and_delete_supervised_producers(Producers),
            ?tp(pulsar_bridge_producer_stopped, #{
                action_id => ActionResId,
                producers => Producers
            }),
            ok
        end,
        #{
            msg => "failed_to_delete_pulsar_producer",
            action_id => ActionResId
        }
    ),
    ok.

log_when_error(Fun, Log) ->
    try
        Fun()
    catch
        C:E ->
            ?SLOG(error, Log#{
                exception => C,
                reason => E
            })
    end.

-spec compile_message_template(message_template_raw()) -> message_template().
compile_message_template(TemplateOpts) ->
    KeyTemplate = maps:get(key, TemplateOpts, <<"${.clientid}">>),
    ValueTemplate = maps:get(value, TemplateOpts, <<"${.}">>),
    #{
        key => preproc_tmpl(KeyTemplate),
        value => preproc_tmpl(ValueTemplate)
    }.

preproc_tmpl(Template) ->
    emqx_placeholder:preproc_tmpl(Template).

render_message(
    Message, #{key := KeyTemplate, value := ValueTemplate}
) ->
    #{
        key => render(Message, KeyTemplate),
        value => render(Message, ValueTemplate)
    }.

render(Message, Template) ->
    Opts = #{
        var_trans => fun
            (undefined) -> <<"">>;
            (X) -> emqx_utils_conv:bin(X)
        end,
        return => full_binary
    },
    emqx_placeholder:proc_tmpl(Template, Message, Opts).

get_producer_status(Producers) ->
    do_get_producer_status(Producers, 0).

do_get_producer_status(_Producers, TimeSpent) when TimeSpent > ?HEALTH_CHECK_RETRY_TIMEOUT ->
    ?status_connecting;
do_get_producer_status(Producers, TimeSpent) ->
    try pulsar_producers:all_connected(Producers) of
        true ->
            ?status_connected;
        false ->
            Sleep = 200,
            timer:sleep(Sleep),
            do_get_producer_status(Producers, TimeSpent + Sleep)
        %% producer crashed with badarg. will recover later
    catch
        error:badarg ->
            ?status_connecting
    end.

partition_strategy(key_dispatch) -> first_key_dispatch;
partition_strategy(Strategy) -> Strategy.

is_sensitive_key(auth_data) -> true;
is_sensitive_key(_) -> false.

get_error_message({BrokerErrorMap, _}) when is_map(BrokerErrorMap) ->
    Iterator = maps:iterator(BrokerErrorMap),
    do_get_error_message(Iterator);
get_error_message(_Error) ->
    error.

do_get_error_message(Iterator) ->
    case maps:next(Iterator) of
        {{_Broker, _Port}, #{message := Message}, _NIterator} ->
            {ok, Message};
        {_K, _V, NIterator} ->
            do_get_error_message(NIterator);
        none ->
            error
    end.

maybe_install_telemetry_handlers(ActionResId) ->
    %% Attach event handlers for telemetry events. If a handler with the
    %% handler id already exists, the attach_many function does nothing
    telemetry:attach_many(
        %% unique handler id
        ActionResId,
        [
            [pulsar, dropped],
            [pulsar, queuing],
            [pulsar, queuing_bytes],
            [pulsar, retried],
            [pulsar, inflight]
        ],
        fun ?MODULE:handle_telemetry_event/4,
        %% we *must* keep track of the same id that is handed down to
        %% wolff producers; otherwise, multiple kafka producer bridges
        %% will install multiple handlers to the same wolff events,
        %% multiplying the metric counts...
        #{action_id => ActionResId}
    ).

with_log_at_error(Fun, Log) ->
    try
        Fun()
    catch
        C:E ->
            ?SLOG(error, Log#{
                exception => C,
                reason => E
            })
    end.

uninstall_telemetry_handlers(TelemetryId) ->
    telemetry:detach(TelemetryId).

deallocate_telemetry_handlers(ConnResId, ActionResId) ->
    _ = with_log_at_error(
        fun() ->
            _ = uninstall_telemetry_handlers(ActionResId),
            emqx_resource:deallocate_resource(ConnResId, {?telemetry_handler_id, ActionResId})
        end,
        #{
            msg => "failed_to_uninstall_telemetry_handlers",
            action_id => ActionResId
        }
    ).
