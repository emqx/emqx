%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_pulsar_impl_producer).

-include("emqx_bridge_pulsar.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% `emqx_resource' API
-export([
    callback_mode/0,
    query_mode/1,
    on_start/2,
    on_stop/2,
    on_get_status/2,
    on_query/3,
    on_query_async/4
]).

-type pulsar_client_id() :: atom().
-type state() :: #{
    pulsar_client_id := pulsar_client_id(),
    producers := pulsar_producers:producers(),
    sync_timeout := infinity | time:time(),
    message_template := message_template()
}.
-type buffer_mode() :: memory | disk | hybrid.
-type compression_mode() :: no_compression | snappy | zlib.
-type partition_strategy() :: random | roundrobin | key_dispatch.
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
    batch_size := pos_integer(),
    bridge_name := atom(),
    buffer := #{
        mode := buffer_mode(),
        per_partition_limit := emqx_schema:byte_size(),
        segment_bytes := emqx_schema:byte_size(),
        memory_overload_protection := boolean()
    },
    compression := compression_mode(),
    connect_timeout := emqx_schema:duration_ms(),
    max_batch_bytes := emqx_schema:bytesize(),
    message := message_template_raw(),
    pulsar_topic := binary(),
    retention_period := infinity | emqx_schema:duration_ms(),
    send_buffer := emqx_schema:bytesize(),
    servers := binary(),
    ssl := _,
    strategy := partition_strategy(),
    sync_timeout := emqx_schema:duration_ms()
}.

%% Allocatable resources
-define(pulsar_client_id, pulsar_client_id).
-define(pulsar_producers, pulsar_producers).

-define(HEALTH_CHECK_RETRY_TIMEOUT, 4_000).

%%-------------------------------------------------------------------------------------
%% `emqx_resource' API
%%-------------------------------------------------------------------------------------

callback_mode() -> async_if_possible.

query_mode(_Config) ->
    simple_async_internal_buffer.

-spec on_start(resource_id(), config()) -> {ok, state()}.
on_start(InstanceId, Config) ->
    #{
        authentication := _Auth,
        bridge_name := BridgeName,
        servers := Servers0,
        ssl := SSL
    } = Config,
    Servers = format_servers(Servers0),
    ClientId = make_client_id(InstanceId, BridgeName),
    ok = emqx_resource:allocate_resource(InstanceId, ?pulsar_client_id, ClientId),
    SSLOpts = emqx_tls_lib:to_client_opts(SSL),
    ConnectTimeout = maps:get(connect_timeout, Config, timer:seconds(5)),
    ClientOpts = #{
        connect_timeout => ConnectTimeout,
        ssl_opts => SSLOpts,
        conn_opts => conn_opts(Config)
    },
    case pulsar:ensure_supervised_client(ClientId, Servers, ClientOpts) of
        {ok, _Pid} ->
            ?tp(
                info,
                "pulsar_client_started",
                #{
                    instance_id => InstanceId,
                    pulsar_hosts => Servers
                }
            );
        {error, Reason} ->
            RedactedReason = emqx_utils:redact(Reason, fun is_sensitive_key/1),
            ?SLOG(error, #{
                msg => "failed_to_start_pulsar_client",
                instance_id => InstanceId,
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
    start_producer(Config, InstanceId, ClientId, ClientOpts).

-spec on_stop(resource_id(), state()) -> ok.
on_stop(InstanceId, _State) ->
    case emqx_resource:get_allocated_resources(InstanceId) of
        #{?pulsar_client_id := ClientId, ?pulsar_producers := Producers} ->
            stop_producers(ClientId, Producers),
            stop_client(ClientId),
            ?tp(pulsar_bridge_stopped, #{
                instance_id => InstanceId,
                pulsar_client_id => ClientId,
                pulsar_producers => Producers
            }),
            ok;
        #{?pulsar_client_id := ClientId} ->
            stop_client(ClientId),
            ?tp(pulsar_bridge_stopped, #{
                instance_id => InstanceId,
                pulsar_client_id => ClientId,
                pulsar_producers => undefined
            }),
            ok;
        _ ->
            ?tp(pulsar_bridge_stopped, #{instance_id => InstanceId}),
            ok
    end.

%% Note: since Pulsar client has its own replayq that is not managed by
%% `emqx_resource_buffer_worker', we must avoid returning `disconnected' here.  Otherwise,
%% `emqx_resource_manager' will kill the Pulsar producers and messages might be lost.
-spec on_get_status(resource_id(), state()) -> connected | connecting.
on_get_status(_InstanceId, State = #{}) ->
    #{
        pulsar_client_id := ClientId,
        producers := Producers
    } = State,
    case pulsar_client_sup:find_client(ClientId) of
        {ok, Pid} ->
            try pulsar_client:get_status(Pid) of
                true ->
                    get_producer_status(Producers);
                false ->
                    connecting
            catch
                error:timeout ->
                    connecting;
                exit:{noproc, _} ->
                    connecting
            end;
        {error, _} ->
            connecting
    end;
on_get_status(_InstanceId, _State) ->
    %% If a health check happens just after a concurrent request to
    %% create the bridge is not quite finished, `State = undefined'.
    connecting.

-spec on_query(resource_id(), {send_message, map()}, state()) ->
    {ok, term()}
    | {error, timeout}
    | {error, term()}.
on_query(_InstanceId, {send_message, Message}, State) ->
    #{
        producers := Producers,
        sync_timeout := SyncTimeout,
        message_template := MessageTemplate
    } = State,
    PulsarMessage = render_message(Message, MessageTemplate),
    try
        pulsar:send_sync(Producers, [PulsarMessage], SyncTimeout)
    catch
        error:timeout ->
            {error, timeout}
    end.

-spec on_query_async(
    resource_id(), {send_message, map()}, {ReplyFun :: function(), Args :: list()}, state()
) ->
    {ok, pid()}.
on_query_async(_InstanceId, {send_message, Message}, AsyncReplyFn, State) ->
    ?tp_span(
        pulsar_producer_on_query_async,
        #{instance_id => _InstanceId, message => Message},
        do_on_query_async(Message, AsyncReplyFn, State)
    ).

do_on_query_async(Message, AsyncReplyFn, State) ->
    #{
        producers := Producers,
        message_template := MessageTemplate
    } = State,
    PulsarMessage = render_message(Message, MessageTemplate),
    pulsar:send(Producers, [PulsarMessage], #{callback_fn => AsyncReplyFn}).

%%-------------------------------------------------------------------------------------
%% Internal fns
%%-------------------------------------------------------------------------------------

-spec to_bin(atom() | string() | binary()) -> binary().
to_bin(A) when is_atom(A) ->
    atom_to_binary(A);
to_bin(L) when is_list(L) ->
    list_to_binary(L);
to_bin(B) when is_binary(B) ->
    B.

-spec format_servers(binary()) -> [string()].
format_servers(Servers0) ->
    Servers1 = emqx_schema:parse_servers(Servers0, ?PULSAR_HOST_OPTIONS),
    lists:map(
        fun(#{scheme := Scheme, hostname := Host, port := Port}) ->
            Scheme ++ "://" ++ Host ++ ":" ++ integer_to_list(Port)
        end,
        Servers1
    ).

-spec make_client_id(resource_id(), atom() | binary()) -> pulsar_client_id().
make_client_id(InstanceId, BridgeName) ->
    case is_dry_run(InstanceId) of
        true ->
            pulsar_producer_probe;
        false ->
            ClientIdBin = iolist_to_binary([
                <<"pulsar_producer:">>,
                to_bin(BridgeName),
                <<":">>,
                to_bin(node())
            ]),
            binary_to_atom(ClientIdBin)
    end.

-spec is_dry_run(resource_id()) -> boolean().
is_dry_run(InstanceId) ->
    TestIdStart = string:find(InstanceId, ?TEST_ID_PREFIX),
    case TestIdStart of
        nomatch ->
            false;
        _ ->
            string:equal(TestIdStart, InstanceId)
    end.

conn_opts(#{authentication := none}) ->
    #{};
conn_opts(#{authentication := #{username := Username, password := Password}}) ->
    #{
        auth_data => iolist_to_binary([Username, <<":">>, Password]),
        auth_method_name => <<"basic">>
    };
conn_opts(#{authentication := #{jwt := JWT}}) ->
    #{
        auth_data => JWT,
        auth_method_name => <<"token">>
    }.

-spec replayq_dir(pulsar_client_id()) -> string().
replayq_dir(ClientId) ->
    filename:join([emqx:data_dir(), "pulsar", to_bin(ClientId)]).

-spec producer_name(pulsar_client_id()) -> atom().
producer_name(ClientId) ->
    ClientIdBin = to_bin(ClientId),
    binary_to_atom(
        iolist_to_binary([
            <<"producer-">>,
            ClientIdBin
        ])
    ).

-spec start_producer(config(), resource_id(), pulsar_client_id(), map()) -> {ok, state()}.
start_producer(Config, InstanceId, ClientId, ClientOpts) ->
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
        message := MessageTemplateOpts,
        pulsar_topic := PulsarTopic0,
        retention_period := RetentionPeriod,
        send_buffer := SendBuffer,
        strategy := Strategy,
        sync_timeout := SyncTimeout
    } = Config,
    {OffloadMode, ReplayQDir} =
        case BufferMode of
            memory -> {false, false};
            disk -> {false, replayq_dir(ClientId)};
            hybrid -> {true, replayq_dir(ClientId)}
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
    ProducerName = producer_name(ClientId),
    ?tp(pulsar_producer_capture_name, #{producer_name => ProducerName}),
    MessageTemplate = compile_message_template(MessageTemplateOpts),
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
            tcp_opts => [{sndbuf, SendBuffer}]
        },
    ProducerOpts = maps:merge(ReplayQOpts, ProducerOpts0),
    PulsarTopic = binary_to_list(PulsarTopic0),
    ?tp(pulsar_producer_about_to_start_producers, #{producer_name => ProducerName}),
    try pulsar:ensure_supervised_producers(ClientId, PulsarTopic, ProducerOpts) of
        {ok, Producers} ->
            ok = emqx_resource:allocate_resource(InstanceId, ?pulsar_producers, Producers),
            ?tp(pulsar_producer_producers_allocated, #{}),
            State = #{
                pulsar_client_id => ClientId,
                producers => Producers,
                sync_timeout => SyncTimeout,
                message_template => MessageTemplate
            },
            ?tp(pulsar_producer_bridge_started, #{}),
            {ok, State}
    catch
        Kind:Error:Stacktrace ->
            ?tp(
                error,
                "failed_to_start_pulsar_producer",
                #{
                    instance_id => InstanceId,
                    kind => Kind,
                    reason => emqx_utils:redact(Error, fun is_sensitive_key/1),
                    stacktrace => Stacktrace
                }
            ),
            stop_client(ClientId),
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

-spec stop_producers(pulsar_client_id(), pulsar_producers:producers()) -> ok.
stop_producers(ClientId, Producers) ->
    _ = log_when_error(
        fun() ->
            ok = pulsar:stop_and_delete_supervised_producers(Producers),
            ?tp(pulsar_bridge_producer_stopped, #{pulsar_client_id => ClientId}),
            ok
        end,
        #{
            msg => "failed_to_delete_pulsar_producer",
            pulsar_client_id => ClientId
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
    connecting;
do_get_producer_status(Producers, TimeSpent) ->
    case pulsar_producers:all_connected(Producers) of
        true ->
            connected;
        false ->
            Sleep = 200,
            timer:sleep(Sleep),
            do_get_producer_status(Producers, TimeSpent + Sleep)
    end.

partition_strategy(key_dispatch) -> first_key_dispatch;
partition_strategy(Strategy) -> Strategy.

is_sensitive_key(auth_data) -> true;
is_sensitive_key(_) -> false.

get_error_message({BrokerErrorMap, _}) when is_map(BrokerErrorMap) ->
    Iter = maps:iterator(BrokerErrorMap),
    do_get_error_message(Iter);
get_error_message(_Error) ->
    error.

do_get_error_message(Iter) ->
    case maps:next(Iter) of
        {{_Broker, _Port}, #{message := Message}, _NIter} ->
            {ok, Message};
        {_K, _V, NIter} ->
            do_get_error_message(NIter);
        none ->
            error
    end.
