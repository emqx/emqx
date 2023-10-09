%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_kafka_impl_producer).

-behaviour(emqx_resource).

-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%% callbacks of behaviour emqx_resource
-export([
    query_mode/1,
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_query_async/4,
    on_get_status/2
]).

-export([
    on_kafka_ack/3,
    handle_telemetry_event/4
]).

-include_lib("emqx/include/logger.hrl").

%% Allocatable resources
-define(kafka_resource_id, kafka_resource_id).
-define(kafka_client_id, kafka_client_id).
-define(kafka_producers, kafka_producers).

query_mode(#{kafka := #{query_mode := sync}}) ->
    simple_sync_internal_buffer;
query_mode(_) ->
    simple_async_internal_buffer.

callback_mode() -> async_if_possible.

%% @doc Config schema is defined in emqx_bridge_kafka.
on_start(InstId, Config) ->
    #{
        authentication := Auth,
        bootstrap_hosts := Hosts0,
        bridge_name := BridgeName,
        bridge_type := BridgeType,
        connect_timeout := ConnTimeout,
        kafka := KafkaConfig = #{
            message := MessageTemplate,
            topic := KafkaTopic,
            sync_query_timeout := SyncQueryTimeout
        },
        metadata_request_timeout := MetaReqTimeout,
        min_metadata_refresh_interval := MinMetaRefreshInterval,
        socket_opts := SocketOpts,
        ssl := SSL
    } = Config,
    KafkaHeadersTokens = preproc_kafka_headers(maps:get(kafka_headers, KafkaConfig, undefined)),
    KafkaExtHeadersTokens = preproc_ext_headers(maps:get(kafka_ext_headers, KafkaConfig, [])),
    KafkaHeadersValEncodeMode = maps:get(kafka_header_value_encode_mode, KafkaConfig, none),
    ResourceId = emqx_bridge_resource:resource_id(BridgeType, BridgeName),
    ok = emqx_resource:allocate_resource(InstId, ?kafka_resource_id, ResourceId),
    _ = maybe_install_wolff_telemetry_handlers(ResourceId),
    Hosts = emqx_bridge_kafka_impl:hosts(Hosts0),
    ClientId = emqx_bridge_kafka_impl:make_client_id(BridgeType, BridgeName),
    ok = emqx_resource:allocate_resource(InstId, ?kafka_client_id, ClientId),
    ClientConfig = #{
        min_metadata_refresh_interval => MinMetaRefreshInterval,
        connect_timeout => ConnTimeout,
        client_id => ClientId,
        request_timeout => MetaReqTimeout,
        extra_sock_opts => emqx_bridge_kafka_impl:socket_opts(SocketOpts),
        sasl => emqx_bridge_kafka_impl:sasl(Auth),
        ssl => ssl(SSL)
    },
    case do_get_topic_status(Hosts, KafkaConfig, KafkaTopic) of
        unhealthy_target ->
            throw(unhealthy_target);
        _ ->
            ok
    end,
    case wolff:ensure_supervised_client(ClientId, Hosts, ClientConfig) of
        {ok, _} ->
            ?SLOG(info, #{
                msg => "kafka_client_started",
                instance_id => InstId,
                kafka_hosts => Hosts
            });
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "failed_to_start_kafka_client",
                instance_id => InstId,
                kafka_hosts => Hosts,
                reason => Reason
            }),
            throw(failed_to_start_kafka_client)
    end,
    %% Check if this is a dry run
    TestIdStart = string:find(InstId, ?TEST_ID_PREFIX),
    IsDryRun =
        case TestIdStart of
            nomatch ->
                false;
            _ ->
                string:equal(TestIdStart, InstId)
        end,
    WolffProducerConfig = producers_config(BridgeType, BridgeName, ClientId, KafkaConfig, IsDryRun),
    case wolff:ensure_supervised_producers(ClientId, KafkaTopic, WolffProducerConfig) of
        {ok, Producers} ->
            ok = emqx_resource:allocate_resource(InstId, ?kafka_producers, Producers),
            {ok, #{
                message_template => compile_message_template(MessageTemplate),
                client_id => ClientId,
                kafka_topic => KafkaTopic,
                producers => Producers,
                resource_id => ResourceId,
                sync_query_timeout => SyncQueryTimeout,
                hosts => Hosts,
                kafka_config => KafkaConfig,
                headers_tokens => KafkaHeadersTokens,
                ext_headers_tokens => KafkaExtHeadersTokens,
                headers_val_encode_mode => KafkaHeadersValEncodeMode
            }};
        {error, Reason2} ->
            ?SLOG(error, #{
                msg => "failed_to_start_kafka_producer",
                instance_id => InstId,
                kafka_hosts => Hosts,
                kafka_topic => KafkaTopic,
                reason => Reason2
            }),
            %% Need to stop the already running client; otherwise, the
            %% next `on_start' call will try to ensure the client
            %% exists and it will be already present and using the old
            %% config.  This is specially bad if the original crash
            %% was due to misconfiguration and we are trying to fix
            %% it...
            _ = with_log_at_error(
                fun() -> wolff:stop_and_delete_supervised_client(ClientId) end,
                #{
                    msg => "failed_to_delete_kafka_client",
                    client_id => ClientId
                }
            ),

            throw(
                "Failed to start Kafka client. Please check the logs for errors and check"
                " the connection parameters."
            )
    end.

on_stop(InstanceId, _State) ->
    case emqx_resource:get_allocated_resources(InstanceId) of
        #{
            ?kafka_client_id := ClientId,
            ?kafka_producers := Producers,
            ?kafka_resource_id := ResourceId
        } ->
            _ = with_log_at_error(
                fun() -> wolff:stop_and_delete_supervised_producers(Producers) end,
                #{
                    msg => "failed_to_delete_kafka_producer",
                    client_id => ClientId
                }
            ),
            _ = with_log_at_error(
                fun() -> wolff:stop_and_delete_supervised_client(ClientId) end,
                #{
                    msg => "failed_to_delete_kafka_client",
                    client_id => ClientId
                }
            ),
            _ = with_log_at_error(
                fun() -> uninstall_telemetry_handlers(ResourceId) end,
                #{
                    msg => "failed_to_uninstall_telemetry_handlers",
                    resource_id => ResourceId
                }
            ),
            ok;
        #{?kafka_client_id := ClientId, ?kafka_resource_id := ResourceId} ->
            _ = with_log_at_error(
                fun() -> wolff:stop_and_delete_supervised_client(ClientId) end,
                #{
                    msg => "failed_to_delete_kafka_client",
                    client_id => ClientId
                }
            ),
            _ = with_log_at_error(
                fun() -> uninstall_telemetry_handlers(ResourceId) end,
                #{
                    msg => "failed_to_uninstall_telemetry_handlers",
                    resource_id => ResourceId
                }
            ),
            ok;
        #{?kafka_resource_id := ResourceId} ->
            _ = with_log_at_error(
                fun() -> uninstall_telemetry_handlers(ResourceId) end,
                #{
                    msg => "failed_to_uninstall_telemetry_handlers",
                    resource_id => ResourceId
                }
            ),
            ok;
        _ ->
            ok
    end,
    ?tp(kafka_producer_stopped, #{instance_id => InstanceId}),
    ok.

on_query(
    InstId,
    {send_message, Message},
    #{
        message_template := Template,
        producers := Producers,
        sync_query_timeout := SyncTimeout,
        headers_tokens := KafkaHeadersTokens,
        ext_headers_tokens := KafkaExtHeadersTokens,
        headers_val_encode_mode := KafkaHeadersValEncodeMode
    }
) ->
    KafkaHeaders = #{
        headers_tokens => KafkaHeadersTokens,
        ext_headers_tokens => KafkaExtHeadersTokens,
        headers_val_encode_mode => KafkaHeadersValEncodeMode
    },
    try
        KafkaMessage = render_message(Template, KafkaHeaders, Message),
        ?tp(
            emqx_bridge_kafka_impl_producer_sync_query,
            #{headers_config => KafkaHeaders, instance_id => InstId}
        ),
        do_send_msg(sync, KafkaMessage, Producers, SyncTimeout)
    catch
        throw:{bad_kafka_header, _} = Error ->
            ?tp(
                emqx_bridge_kafka_impl_producer_sync_query_failed,
                #{
                    headers_config => KafkaHeaders,
                    instance_id => InstId,
                    reason => Error
                }
            ),
            {error, {unrecoverable_error, Error}};
        throw:{bad_kafka_headers, _} = Error ->
            ?tp(
                emqx_bridge_kafka_impl_producer_sync_query_failed,
                #{
                    headers_config => KafkaHeaders,
                    instance_id => InstId,
                    reason => Error
                }
            ),
            {error, {unrecoverable_error, Error}}
    end.

%% @doc The callback API for rule-engine (or bridge without rules)
%% The input argument `Message' is an enriched format (as a map())
%% of the original #message{} record.
%% The enrichment is done by rule-engine or by the data bridge framework.
%% E.g. the output of rule-engine process chain
%% or the direct mapping from an MQTT message.
on_query_async(
    InstId,
    {send_message, Message},
    AsyncReplyFn,
    #{
        message_template := Template,
        producers := Producers,
        headers_tokens := KafkaHeadersTokens,
        ext_headers_tokens := KafkaExtHeadersTokens,
        headers_val_encode_mode := KafkaHeadersValEncodeMode
    }
) ->
    KafkaHeaders = #{
        headers_tokens => KafkaHeadersTokens,
        ext_headers_tokens => KafkaExtHeadersTokens,
        headers_val_encode_mode => KafkaHeadersValEncodeMode
    },
    try
        KafkaMessage = render_message(Template, KafkaHeaders, Message),
        ?tp(
            emqx_bridge_kafka_impl_producer_async_query,
            #{headers_config => KafkaHeaders, instance_id => InstId}
        ),
        do_send_msg(async, KafkaMessage, Producers, AsyncReplyFn)
    catch
        throw:{bad_kafka_header, _} = Error ->
            ?tp(
                emqx_bridge_kafka_impl_producer_async_query_failed,
                #{
                    headers_config => KafkaHeaders,
                    instance_id => InstId,
                    reason => Error
                }
            ),
            {error, {unrecoverable_error, Error}};
        throw:{bad_kafka_headers, _} = Error ->
            ?tp(
                emqx_bridge_kafka_impl_producer_async_query_failed,
                #{
                    headers_config => KafkaHeaders,
                    instance_id => InstId,
                    reason => Error
                }
            ),
            {error, {unrecoverable_error, Error}}
    end.

compile_message_template(T) ->
    KeyTemplate = maps:get(key, T, <<"${.clientid}">>),
    ValueTemplate = maps:get(value, T, <<"${.}">>),
    TimestampTemplate = maps:get(timestamp, T, <<"${.timestamp}">>),
    #{
        key => preproc_tmpl(KeyTemplate),
        value => preproc_tmpl(ValueTemplate),
        timestamp => preproc_tmpl(TimestampTemplate)
    }.

preproc_tmpl(Tmpl) ->
    emqx_placeholder:preproc_tmpl(Tmpl).

render_message(
    #{key := KeyTemplate, value := ValueTemplate, timestamp := TimestampTemplate},
    #{
        headers_tokens := KafkaHeadersTokens,
        ext_headers_tokens := KafkaExtHeadersTokens,
        headers_val_encode_mode := KafkaHeadersValEncodeMode
    },
    Message
) ->
    ExtHeaders = proc_ext_headers(KafkaExtHeadersTokens, Message),
    KafkaHeaders =
        case KafkaHeadersTokens of
            undefined -> ExtHeaders;
            HeadersTks -> merge_kafka_headers(HeadersTks, ExtHeaders, Message)
        end,
    Headers = formalize_kafka_headers(KafkaHeaders, KafkaHeadersValEncodeMode),
    #{
        key => render(KeyTemplate, Message),
        value => render(ValueTemplate, Message),
        headers => Headers,
        ts => render_timestamp(TimestampTemplate, Message)
    }.

render(Template, Message) ->
    Opts = #{
        var_trans => fun
            (undefined) -> <<"">>;
            (X) -> emqx_utils_conv:bin(X)
        end,
        return => full_binary
    },
    emqx_placeholder:proc_tmpl(Template, Message, Opts).

render_timestamp(Template, Message) ->
    try
        binary_to_integer(render(Template, Message))
    catch
        _:_ ->
            erlang:system_time(millisecond)
    end.

do_send_msg(sync, KafkaMessage, Producers, SyncTimeout) ->
    try
        {_Partition, _Offset} = wolff:send_sync(Producers, [KafkaMessage], SyncTimeout),
        ok
    catch
        error:{producer_down, _} = Reason ->
            {error, Reason};
        error:timeout ->
            {error, timeout}
    end;
do_send_msg(async, KafkaMessage, Producers, AsyncReplyFn) ->
    %% * Must be a batch because wolff:send and wolff:send_sync are batch APIs
    %% * Must be a single element batch because wolff books calls, but not batch sizes
    %%   for counters and gauges.
    Batch = [KafkaMessage],
    %% The retuned information is discarded here.
    %% If the producer process is down when sending, this function would
    %% raise an error exception which is to be caught by the caller of this callback
    {_Partition, Pid} = wolff:send(Producers, Batch, {fun ?MODULE:on_kafka_ack/3, [AsyncReplyFn]}),
    %% this Pid is so far never used because Kafka producer is by-passing the buffer worker
    {ok, Pid}.

%% Wolff producer never gives up retrying
%% so there can only be 'ok' results.
on_kafka_ack(_Partition, Offset, {ReplyFn, Args}) when is_integer(Offset) ->
    %% the ReplyFn is emqx_resource_buffer_worker:handle_async_reply/2
    apply(ReplyFn, Args ++ [ok]);
on_kafka_ack(_Partition, buffer_overflow_discarded, _Callback) ->
    %% wolff should bump the dropped_queue_full counter
    %% do not apply the callback (which is basically to bump success or fail counter)
    ok.

%% Note: since wolff client has its own replayq that is not managed by
%% `emqx_resource_buffer_worker', we must avoid returning `disconnected' here.  Otherwise,
%% `emqx_resource_manager' will kill the wolff producers and messages might be lost.
on_get_status(_InstId, #{client_id := ClientId} = State) ->
    case wolff_client_sup:find_client(ClientId) of
        {ok, Pid} ->
            case do_get_status(Pid, State) of
                ok -> connected;
                unhealthy_target -> {disconnected, State, unhealthy_target};
                error -> connecting
            end;
        {error, _Reason} ->
            connecting
    end.

do_get_status(Client, #{kafka_topic := KafkaTopic, hosts := Hosts, kafka_config := KafkaConfig}) ->
    case do_get_topic_status(Hosts, KafkaConfig, KafkaTopic) of
        unhealthy_target ->
            unhealthy_target;
        _ ->
            case do_get_healthy_leaders(Client, KafkaTopic) of
                [] -> error;
                _ -> ok
            end
    end.

do_get_healthy_leaders(Client, KafkaTopic) ->
    case wolff_client:get_leader_connections(Client, KafkaTopic) of
        {ok, Leaders} ->
            %% Kafka is considered healthy as long as any of the partition leader is reachable.
            lists:filtermap(
                fun({_Partition, Pid}) ->
                    case is_pid(Pid) andalso erlang:is_process_alive(Pid) of
                        true -> {true, Pid};
                        _ -> false
                    end
                end,
                Leaders
            );
        {error, _} ->
            []
    end.

do_get_topic_status(Hosts, KafkaConfig, KafkaTopic) ->
    CheckTopicFun =
        fun() ->
            wolff_client:check_if_topic_exists(Hosts, KafkaConfig, KafkaTopic)
        end,
    try
        case emqx_utils:nolink_apply(CheckTopicFun, 5_000) of
            ok -> ok;
            {error, unknown_topic_or_partition} -> unhealthy_target;
            _ -> error
        end
    catch
        _:_ ->
            error
    end.

ssl(#{enable := true} = SSL) ->
    emqx_tls_lib:to_client_opts(SSL);
ssl(_) ->
    [].

producers_config(BridgeType, BridgeName, ClientId, Input, IsDryRun) ->
    #{
        max_batch_bytes := MaxBatchBytes,
        compression := Compression,
        partition_strategy := PartitionStrategy,
        required_acks := RequiredAcks,
        partition_count_refresh_interval := PCntRefreshInterval,
        max_inflight := MaxInflight,
        buffer := #{
            mode := BufferMode,
            per_partition_limit := PerPartitionLimit,
            segment_bytes := SegmentBytes,
            memory_overload_protection := MemOLP0
        }
    } = Input,
    MemOLP =
        case os:type() of
            {unix, linux} -> MemOLP0;
            _ -> false
        end,
    {OffloadMode, ReplayqDir} =
        case BufferMode of
            memory -> {false, false};
            disk -> {false, replayq_dir(ClientId)};
            hybrid -> {true, replayq_dir(ClientId)}
        end,
    ResourceID = emqx_bridge_resource:resource_id(BridgeType, BridgeName),
    #{
        name => make_producer_name(BridgeType, BridgeName, IsDryRun),
        partitioner => partitioner(PartitionStrategy),
        partition_count_refresh_interval_seconds => PCntRefreshInterval,
        replayq_dir => ReplayqDir,
        replayq_offload_mode => OffloadMode,
        replayq_max_total_bytes => PerPartitionLimit,
        replayq_seg_bytes => SegmentBytes,
        drop_if_highmem => MemOLP,
        required_acks => RequiredAcks,
        max_batch_bytes => MaxBatchBytes,
        max_send_ahead => MaxInflight - 1,
        compression => Compression,
        telemetry_meta_data => #{bridge_id => ResourceID}
    }.

%% Wolff API is a batch API.
%% key_dispatch only looks at the first element, so it's named 'first_key_dispatch'
partitioner(random) -> random;
partitioner(key_dispatch) -> first_key_dispatch.

replayq_dir(ClientId) ->
    filename:join([emqx:data_dir(), "kafka", ClientId]).

%% Producer name must be an atom which will be used as a ETS table name for
%% partition worker lookup.
make_producer_name(_BridgeType, _BridgeName, true = _IsDryRun) ->
    %% It is a dry run and we don't want to leak too many atoms
    %% so we use the default producer name instead of creating
    %% an unique name.
    probing_wolff_producers;
make_producer_name(BridgeType, BridgeName, _IsDryRun) ->
    %% Woff needs an atom for ets table name registration. The assumption here is
    %% that bridges with new names are not often created.
    binary_to_atom(iolist_to_binary([BridgeType, "_", bin(BridgeName)])).

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

%% we *must* match the bridge id in the event metadata with that in
%% the handler config; otherwise, multiple kafka producer bridges will
%% install multiple handlers to the same wolff events, multiplying the
handle_telemetry_event(
    [wolff, dropped_queue_full],
    #{counter_inc := Val},
    #{bridge_id := ID},
    #{bridge_id := ID}
) when is_integer(Val) ->
    emqx_resource_metrics:dropped_queue_full_inc(ID, Val);
handle_telemetry_event(
    [wolff, queuing],
    #{gauge_set := Val},
    #{bridge_id := ID, partition_id := PartitionID},
    #{bridge_id := ID}
) when is_integer(Val) ->
    emqx_resource_metrics:queuing_set(ID, PartitionID, Val);
handle_telemetry_event(
    [wolff, retried],
    #{counter_inc := Val},
    #{bridge_id := ID},
    #{bridge_id := ID}
) when is_integer(Val) ->
    emqx_resource_metrics:retried_inc(ID, Val);
handle_telemetry_event(
    [wolff, inflight],
    #{gauge_set := Val},
    #{bridge_id := ID, partition_id := PartitionID},
    #{bridge_id := ID}
) when is_integer(Val) ->
    emqx_resource_metrics:inflight_set(ID, PartitionID, Val);
handle_telemetry_event(_EventId, _Metrics, _MetaData, _HandlerConfig) ->
    %% Event that we do not handle
    ok.

%% Note: don't use the instance/manager ID, as that changes everytime
%% the bridge is recreated, and will lead to multiplication of
%% metrics.
-spec telemetry_handler_id(resource_id()) -> binary().
telemetry_handler_id(ResourceID) ->
    <<"emqx-bridge-kafka-producer-", ResourceID/binary>>.

uninstall_telemetry_handlers(ResourceID) ->
    HandlerID = telemetry_handler_id(ResourceID),
    telemetry:detach(HandlerID).

maybe_install_wolff_telemetry_handlers(ResourceID) ->
    %% Attach event handlers for Kafka telemetry events. If a handler with the
    %% handler id already exists, the attach_many function does nothing
    telemetry:attach_many(
        %% unique handler id
        telemetry_handler_id(ResourceID),
        [
            [wolff, dropped_queue_full],
            [wolff, queuing],
            [wolff, retried],
            [wolff, inflight]
        ],
        fun ?MODULE:handle_telemetry_event/4,
        %% we *must* keep track of the same id that is handed down to
        %% wolff producers; otherwise, multiple kafka producer bridges
        %% will install multiple handlers to the same wolff events,
        %% multiplying the metric counts...
        #{bridge_id => ResourceID}
    ).

preproc_kafka_headers(HeadersTmpl) when HeadersTmpl =:= <<>>; HeadersTmpl =:= undefined ->
    undefined;
preproc_kafka_headers(HeadersTmpl) ->
    %% the value is already validated by schema, so we do not validate it again.
    emqx_placeholder:preproc_tmpl(HeadersTmpl).

preproc_ext_headers(Headers) ->
    [
        {emqx_placeholder:preproc_tmpl(K), preproc_ext_headers_value(V)}
     || #{kafka_ext_header_key := K, kafka_ext_header_value := V} <- Headers
    ].

preproc_ext_headers_value(ValTmpl) ->
    %% the value is already validated by schema, so we do not validate it again.
    emqx_placeholder:preproc_tmpl(ValTmpl).

proc_ext_headers(ExtHeaders, Msg) ->
    lists:filtermap(
        fun({KTks, VTks}) ->
            try
                Key = proc_ext_headers_key(KTks, Msg),
                Val = proc_ext_headers_value(VTks, Msg),
                {true, {Key, Val}}
            catch
                throw:placeholder_not_found -> false
            end
        end,
        ExtHeaders
    ).

proc_ext_headers_key(KeyTks, Msg) ->
    RawList = emqx_placeholder:proc_tmpl(KeyTks, Msg, #{return => rawlist}),
    list_to_binary(
        lists:map(
            fun
                (undefined) -> throw(placeholder_not_found);
                (Key) -> bin(Key)
            end,
            RawList
        )
    ).

proc_ext_headers_value(ValTks, Msg) ->
    case emqx_placeholder:proc_tmpl(ValTks, Msg, #{return => rawlist}) of
        [undefined] -> throw(placeholder_not_found);
        [Value] -> Value
    end.

kvlist_headers([], Acc) ->
    lists:reverse(Acc);
kvlist_headers([#{key := K, value := V} | Headers], Acc) ->
    kvlist_headers(Headers, [{K, V} | Acc]);
kvlist_headers([#{<<"key">> := K, <<"value">> := V} | Headers], Acc) ->
    kvlist_headers(Headers, [{K, V} | Acc]);
kvlist_headers([{K, V} | Headers], Acc) ->
    kvlist_headers(Headers, [{K, V} | Acc]);
kvlist_headers([KVList | Headers], Acc) when is_list(KVList) ->
    %% for instance, when user sets a json list as headers like '[{"foo":"bar"}, {"foo2":"bar2"}]'.
    kvlist_headers(KVList ++ Headers, Acc);
kvlist_headers([BadHeader | _], _) ->
    throw({bad_kafka_header, BadHeader}).

-define(IS_STR_KEY(K), (is_list(K) orelse is_atom(K) orelse is_binary(K))).
formalize_kafka_headers(Headers, none) ->
    %% Note that we will drop all the non-binary values in the NONE mode
    [{bin(K), V} || {K, V} <- Headers, is_binary(V) andalso ?IS_STR_KEY(K)];
formalize_kafka_headers(Headers, json) ->
    lists:filtermap(
        fun({K, V}) ->
            try
                {true, {bin(K), emqx_utils_json:encode(V)}}
            catch
                _:_ -> false
            end
        end,
        Headers
    ).

merge_kafka_headers(HeadersTks, ExtHeaders, Msg) ->
    case emqx_placeholder:proc_tmpl(HeadersTks, Msg, #{return => rawlist}) of
        % Headers by map object.
        [Map] when is_map(Map) ->
            maps:to_list(Map) ++ ExtHeaders;
        [KVList] when is_list(KVList) ->
            kvlist_headers(KVList, []) ++ ExtHeaders;
        %% the placeholder cannot be found in Msg
        [undefined] ->
            ExtHeaders;
        [MaybeJson] when is_binary(MaybeJson) ->
            case emqx_utils_json:safe_decode(MaybeJson, [return_maps]) of
                {ok, JsonTerm} when is_map(JsonTerm) ->
                    maps:to_list(JsonTerm) ++ ExtHeaders;
                {ok, JsonTerm} when is_list(JsonTerm) ->
                    kvlist_headers(JsonTerm, []) ++ ExtHeaders;
                _ ->
                    throw({bad_kafka_headers, MaybeJson})
            end;
        BadHeaders ->
            throw({bad_kafka_headers, BadHeaders})
    end.

bin(B) when is_binary(B) -> B;
bin(L) when is_list(L) -> erlang:list_to_binary(L);
bin(A) when is_atom(A) -> erlang:atom_to_binary(A, utf8).
