%%--------------------------------------------------------------------
%% Copyright (c) 2022-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    on_get_status/2,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channels/1,
    on_get_channel_status/3
]).

-export([
    on_kafka_ack/3,
    handle_telemetry_event/4
]).

-include_lib("emqx/include/logger.hrl").

%% Allocatable resources
-define(kafka_telemetry_id, kafka_telemetry_id).
-define(kafka_client_id, kafka_client_id).
-define(kafka_producers, kafka_producers).

query_mode(#{parameters := #{query_mode := sync}}) ->
    simple_sync_internal_buffer;
query_mode(_) ->
    simple_async_internal_buffer.

callback_mode() -> async_if_possible.

check_config(Key, Config) when is_map_key(Key, Config) ->
    tr_config(Key, maps:get(Key, Config));
check_config(Key, _Config) ->
    throw(#{
        reason => missing_required_config,
        missing_config => Key
    }).

tr_config(bootstrap_hosts, Hosts) ->
    emqx_bridge_kafka_impl:hosts(Hosts);
tr_config(authentication, Auth) ->
    emqx_bridge_kafka_impl:sasl(Auth);
tr_config(ssl, Ssl) ->
    ssl(Ssl);
tr_config(socket_opts, Opts) ->
    emqx_bridge_kafka_impl:socket_opts(Opts);
tr_config(_Key, Value) ->
    Value.

%% @doc Config schema is defined in emqx_bridge_kafka.
on_start(InstId, Config) ->
    ?SLOG(debug, #{
        msg => "kafka_client_starting",
        instance_id => InstId,
        config => emqx_utils:redact(Config)
    }),
    C = fun(Key) -> check_config(Key, Config) end,
    Hosts = C(bootstrap_hosts),
    ClientConfig = #{
        min_metadata_refresh_interval => C(min_metadata_refresh_interval),
        connect_timeout => C(connect_timeout),
        request_timeout => C(metadata_request_timeout),
        extra_sock_opts => C(socket_opts),
        sasl => C(authentication),
        ssl => C(ssl)
    },
    ClientId = InstId,
    emqx_resource:allocate_resource(InstId, ?kafka_client_id, ClientId),
    ok = ensure_client(ClientId, Hosts, ClientConfig),
    %% Note: we must return `{error, _}' here if the client cannot connect so that the
    %% connector will immediately enter the `?status_disconnected' state, and then avoid
    %% giving the impression that channels/actions may be added immediately and start
    %% buffering, which won't happen if it's `?status_connecting'.  That would lead to
    %% data loss, since Kafka Producer uses wolff's internal buffering, which is started
    %% only when its producers start.
    case check_client_connectivity(ClientId) of
        ok ->
            HealthCheckTopic = maps:get(health_check_topic, Config, undefined),
            ConnectorState = #{
                client_id => ClientId,
                health_check_topic => HealthCheckTopic,
                installed_bridge_v2s => #{}
            },
            {ok, ConnectorState};
        {error, {find_client, Reason}} ->
            %% Race condition?  Crash?  We just checked it with `ensure_client'...
            {error, Reason};
        {error, {connectivity, Reason}} ->
            {error, Reason}
    end.

on_add_channel(
    InstId,
    #{
        client_id := ClientId,
        installed_bridge_v2s := InstalledBridgeV2s
    } = OldState,
    BridgeV2Id,
    BridgeV2Config
) ->
    %% The following will throw an exception if the bridge producers fails to start
    {ok, BridgeV2State} = create_producers_for_bridge_v2(
        InstId, BridgeV2Id, ClientId, BridgeV2Config
    ),
    NewInstalledBridgeV2s = maps:put(BridgeV2Id, BridgeV2State, InstalledBridgeV2s),
    %% Update state
    NewState = OldState#{installed_bridge_v2s => NewInstalledBridgeV2s},
    {ok, NewState}.

create_producers_for_bridge_v2(
    InstId,
    BridgeV2Id,
    ClientId,
    #{
        bridge_type := BridgeType,
        parameters := KafkaConfig
    }
) ->
    #{
        message := MessageTemplate,
        topic := KafkaTopic,
        sync_query_timeout := SyncQueryTimeout
    } = KafkaConfig,
    KafkaHeadersTokens = preproc_kafka_headers(maps:get(kafka_headers, KafkaConfig, undefined)),
    KafkaExtHeadersTokens = preproc_ext_headers(maps:get(kafka_ext_headers, KafkaConfig, [])),
    KafkaHeadersValEncodeMode = maps:get(kafka_header_value_encode_mode, KafkaConfig, none),
    MaxPartitions = maps:get(partitions_limit, KafkaConfig, all_partitions),
    #{name := BridgeName} = emqx_bridge_v2:parse_id(BridgeV2Id),
    TestIdStart = string:find(BridgeV2Id, ?TEST_ID_PREFIX),
    IsDryRun =
        case TestIdStart of
            nomatch ->
                false;
            _ ->
                string:equal(TestIdStart, InstId)
        end,
    ok = check_topic_and_leader_connections(ClientId, KafkaTopic, MaxPartitions),
    WolffProducerConfig = producers_config(
        BridgeType, BridgeName, KafkaConfig, IsDryRun, BridgeV2Id
    ),
    case wolff:ensure_supervised_producers(ClientId, KafkaTopic, WolffProducerConfig) of
        {ok, Producers} ->
            ok = emqx_resource:allocate_resource(InstId, {?kafka_producers, BridgeV2Id}, Producers),
            ok = emqx_resource:allocate_resource(
                InstId, {?kafka_telemetry_id, BridgeV2Id}, BridgeV2Id
            ),
            _ = maybe_install_wolff_telemetry_handlers(BridgeV2Id),
            {ok, #{
                message_template => compile_message_template(MessageTemplate),
                kafka_client_id => ClientId,
                kafka_topic => KafkaTopic,
                producers => Producers,
                resource_id => BridgeV2Id,
                connector_resource_id => InstId,
                sync_query_timeout => SyncQueryTimeout,
                kafka_config => KafkaConfig,
                headers_tokens => KafkaHeadersTokens,
                ext_headers_tokens => KafkaExtHeadersTokens,
                headers_val_encode_mode => KafkaHeadersValEncodeMode,
                partitions_limit => MaxPartitions
            }};
        {error, Reason2} ->
            ?SLOG(error, #{
                msg => "failed_to_start_kafka_producer",
                instance_id => InstId,
                kafka_client_id => ClientId,
                kafka_topic => KafkaTopic,
                reason => Reason2
            }),
            throw(
                "Failed to start Kafka client. Please check the logs for errors and check"
                " the connection parameters."
            )
    end.

on_stop(InstanceId, _State) ->
    AllocatedResources = emqx_resource:get_allocated_resources(InstanceId),
    ClientId = maps:get(?kafka_client_id, AllocatedResources, undefined),
    case ClientId of
        undefined ->
            ok;
        ClientId ->
            deallocate_client(ClientId)
    end,
    maps:foreach(
        fun
            ({?kafka_producers, _BridgeV2Id}, Producers) ->
                deallocate_producers(ClientId, Producers);
            ({?kafka_telemetry_id, _BridgeV2Id}, TelemetryId) ->
                deallocate_telemetry_handlers(TelemetryId);
            (_, _) ->
                ok
        end,
        AllocatedResources
    ),
    ?tp(kafka_producer_stopped, #{instance_id => InstanceId}),
    ok.

ensure_client(ClientId, Hosts, ClientConfig) ->
    case wolff_client_sup:find_client(ClientId) of
        {ok, _Pid} ->
            ok;
        {error, no_such_client} ->
            case wolff:ensure_supervised_client(ClientId, Hosts, ClientConfig) of
                {ok, _} ->
                    ?SLOG(info, #{
                        msg => "kafka_client_started",
                        client_id => ClientId,
                        kafka_hosts => Hosts
                    });
                {error, Reason} ->
                    ?SLOG(error, #{
                        msg => failed_to_start_kafka_client,
                        client_id => ClientId,
                        kafka_hosts => Hosts,
                        reason => Reason
                    }),
                    throw(failed_to_start_kafka_client)
            end;
        {error, Reason} ->
            deallocate_client(ClientId),
            throw({failed_to_find_created_client, Reason})
    end.

deallocate_client(ClientId) ->
    _ = with_log_at_error(
        fun() -> wolff:stop_and_delete_supervised_client(ClientId) end,
        #{
            msg => "failed_to_delete_kafka_client",
            client_id => ClientId
        }
    ),
    ok.

deallocate_producers(ClientId, Producers) ->
    _ = with_log_at_error(
        fun() -> wolff:stop_and_delete_supervised_producers(Producers) end,
        #{
            msg => "failed_to_delete_kafka_producer",
            client_id => ClientId
        }
    ).

deallocate_telemetry_handlers(TelemetryId) ->
    _ = with_log_at_error(
        fun() -> uninstall_telemetry_handlers(TelemetryId) end,
        #{
            msg => "failed_to_uninstall_telemetry_handlers",
            resource_id => TelemetryId
        }
    ).

remove_producers_for_bridge_v2(
    InstId, BridgeV2Id
) ->
    AllocatedResources = emqx_resource:get_allocated_resources(InstId),
    ClientId = maps:get(?kafka_client_id, AllocatedResources, no_client_id),
    maps:foreach(
        fun
            ({?kafka_producers, BridgeV2IdCheck}, Producers) when BridgeV2IdCheck =:= BridgeV2Id ->
                deallocate_producers(ClientId, Producers);
            ({?kafka_telemetry_id, BridgeV2IdCheck}, TelemetryId) when
                BridgeV2IdCheck =:= BridgeV2Id
            ->
                deallocate_telemetry_handlers(TelemetryId);
            (_, _) ->
                ok
        end,
        AllocatedResources
    ),
    ok.

on_remove_channel(
    InstId,
    #{
        client_id := _ClientId,
        installed_bridge_v2s := InstalledBridgeV2s
    } = OldState,
    BridgeV2Id
) ->
    ok = remove_producers_for_bridge_v2(InstId, BridgeV2Id),
    NewInstalledBridgeV2s = maps:remove(BridgeV2Id, InstalledBridgeV2s),
    %% Update state
    NewState = OldState#{installed_bridge_v2s => NewInstalledBridgeV2s},
    {ok, NewState}.

on_query(
    InstId,
    {MessageTag, Message},
    #{installed_bridge_v2s := BridgeV2Configs} = _ConnectorState
) ->
    #{
        message_template := Template,
        producers := Producers,
        sync_query_timeout := SyncTimeout,
        headers_tokens := KafkaHeadersTokens,
        ext_headers_tokens := KafkaExtHeadersTokens,
        headers_val_encode_mode := KafkaHeadersValEncodeMode
    } = maps:get(MessageTag, BridgeV2Configs),
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
        emqx_trace:rendered_action_template(MessageTag, #{
            message => KafkaMessage
        }),
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

on_get_channels(ResId) ->
    emqx_bridge_v2:get_channels_for_connector(ResId).

%% @doc The callback API for rule-engine (or bridge without rules)
%% The input argument `Message' is an enriched format (as a map())
%% of the original #message{} record.
%% The enrichment is done by rule-engine or by the data bridge framework.
%% E.g. the output of rule-engine process chain
%% or the direct mapping from an MQTT message.
on_query_async(
    InstId,
    {MessageTag, Message},
    AsyncReplyFn,
    #{installed_bridge_v2s := BridgeV2Configs} = _ConnectorState
) ->
    #{
        message_template := Template,
        producers := Producers,
        headers_tokens := KafkaHeadersTokens,
        ext_headers_tokens := KafkaExtHeadersTokens,
        headers_val_encode_mode := KafkaHeadersValEncodeMode
    } = maps:get(MessageTag, BridgeV2Configs),
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
        emqx_trace:rendered_action_template(MessageTag, #{
            message => KafkaMessage
        }),
        do_send_msg(async, KafkaMessage, Producers, AsyncReplyFn)
    catch
        error:{invalid_partition_count, _Count, _Partitioner} ->
            {error, invalid_partition_count};
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
on_get_status(
    _InstId,
    #{client_id := ClientId} = State
) ->
    %% Note: we must avoid returning `?status_disconnected' here if the connector ever was
    %% connected.  If the connector ever connected, wolff producers might have been
    %% successfully started, and returning `?status_disconnected' will make resource
    %% manager try to restart the producers / connector, thus potentially dropping data
    %% held in wolff producer's replayq.
    case check_client_connectivity(ClientId) of
        ok ->
            maybe_check_health_check_topic(State);
        {error, {find_client, _Error}} ->
            ?status_connecting;
        {error, {connectivity, Error}} ->
            {?status_connecting, State, Error}
    end.

on_get_channel_status(
    _ResId,
    ChannelId,
    #{
        client_id := ClientId,
        installed_bridge_v2s := Channels
    } = _State
) ->
    %% Note: we must avoid returning `?status_disconnected' here. Returning
    %% `?status_disconnected' will make resource manager try to restart the producers /
    %% connector, thus potentially dropping data held in wolff producer's replayq.  The
    %% only exception is if the topic does not exist ("unhealthy target").
    #{kafka_topic := KafkaTopic, partitions_limit := MaxPartitions} = maps:get(ChannelId, Channels),
    try
        ok = check_topic_and_leader_connections(ClientId, KafkaTopic, MaxPartitions),
        ?status_connected
    catch
        throw:{unhealthy_target, Msg} ->
            throw({unhealthy_target, Msg});
        K:E ->
            {?status_connecting, {K, E}}
    end.

check_topic_and_leader_connections(ClientId, KafkaTopic, MaxPartitions) ->
    case wolff_client_sup:find_client(ClientId) of
        {ok, Pid} ->
            ok = check_topic_status(ClientId, Pid, KafkaTopic),
            ok = check_if_healthy_leaders(ClientId, Pid, KafkaTopic, MaxPartitions);
        {error, no_such_client} ->
            throw(#{
                reason => cannot_find_kafka_client,
                kafka_client => ClientId,
                kafka_topic => KafkaTopic
            });
        {error, client_supervisor_not_initialized} ->
            throw(#{
                reason => restarting,
                kafka_client => ClientId,
                kafka_topic => KafkaTopic
            })
    end.

-spec check_client_connectivity(wolff:client_id()) ->
    ok | {error, {connectivity | find_client, term()}}.
check_client_connectivity(ClientId) ->
    case wolff_client_sup:find_client(ClientId) of
        {ok, Pid} ->
            case wolff_client:check_connectivity(Pid) of
                ok ->
                    ok;
                {error, Error} ->
                    {error, {connectivity, Error}}
            end;
        {error, Reason} ->
            {error, {find_client, Reason}}
    end.

maybe_check_health_check_topic(#{health_check_topic := Topic} = ConnectorState) when
    is_binary(Topic)
->
    #{client_id := ClientId} = ConnectorState,
    MaxPartitions = all_partitions,
    try check_topic_and_leader_connections(ClientId, Topic, MaxPartitions) of
        ok ->
            ?status_connected
    catch
        throw:#{reason := {connection_down, _} = Reason} ->
            {?status_disconnected, ConnectorState, Reason};
        throw:#{reason := Reason} ->
            {?status_connecting, ConnectorState, Reason}
    end;
maybe_check_health_check_topic(_) ->
    %% Cannot infer further information.  Maybe upgraded from older version.
    ?status_connected.

check_if_healthy_leaders(ClientId, ClientPid, KafkaTopic, MaxPartitions) when is_pid(ClientPid) ->
    Leaders =
        case wolff_client:get_leader_connections(ClientPid, KafkaTopic, MaxPartitions) of
            {ok, LeadersToCheck} ->
                %% Kafka is considered healthy as long as any of the partition leader is reachable.
                lists:filtermap(
                    fun({_Partition, Pid}) ->
                        case is_pid(Pid) andalso erlang:is_process_alive(Pid) of
                            true -> {true, Pid};
                            _ -> false
                        end
                    end,
                    LeadersToCheck
                );
            {error, _} ->
                []
        end,
    case Leaders of
        [] ->
            throw(#{
                error => no_connected_partition_leader,
                kafka_client => ClientId,
                kafka_topic => KafkaTopic,
                partitions_limit => MaxPartitions
            });
        _ ->
            ok
    end.

check_topic_status(ClientId, WolffClientPid, KafkaTopic) ->
    case wolff_client:check_topic_exists_with_client_pid(WolffClientPid, KafkaTopic) of
        ok ->
            ok;
        {error, unknown_topic_or_partition} ->
            Msg = iolist_to_binary([<<"Unknown topic or partition: ">>, KafkaTopic]),
            throw({unhealthy_target, Msg});
        {error, Reason} ->
            throw(#{
                error => failed_to_check_topic_status,
                kafka_client_id => ClientId,
                reason => Reason,
                kafka_topic => KafkaTopic
            })
    end.

ssl(#{enable := true} = SSL) ->
    emqx_tls_lib:to_client_opts(SSL);
ssl(_) ->
    false.

producers_config(BridgeType, BridgeName, Input, IsDryRun, BridgeV2Id) ->
    #{
        max_batch_bytes := MaxBatchBytes,
        compression := Compression,
        partition_strategy := PartitionStrategy,
        required_acks := RequiredAcks,
        partition_count_refresh_interval := PCntRefreshInterval,
        max_inflight := MaxInflight,
        partitions_limit := MaxPartitions,
        buffer := #{
            mode := BufferMode0,
            per_partition_limit := PerPartitionLimit,
            segment_bytes := SegmentBytes,
            memory_overload_protection := MemOLP
        }
    } = Input,
    %% avoid creating dirs for probing producers
    BufferMode =
        case IsDryRun of
            true ->
                memory;
            false ->
                BufferMode0
        end,
    {OffloadMode, ReplayqDir} =
        case BufferMode of
            memory -> {false, false};
            disk -> {false, replayq_dir(BridgeType, BridgeName)};
            hybrid -> {true, replayq_dir(BridgeType, BridgeName)}
        end,
    #{
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
        telemetry_meta_data => #{bridge_id => BridgeV2Id},
        max_partitions => MaxPartitions
    }.

%% Wolff API is a batch API.
%% key_dispatch only looks at the first element, so it's named 'first_key_dispatch'
partitioner(random) -> random;
partitioner(key_dispatch) -> first_key_dispatch.

replayq_dir(BridgeType, BridgeName) ->
    DirName = iolist_to_binary([
        maybe_v1_type_name(BridgeType),
        ":",
        BridgeName,
        ":",
        atom_to_list(node())
    ]),
    filename:join([emqx:data_dir(), "kafka", DirName]).

%% To avoid losing queued data on disk, we must use the same directory as the old v1
%% bridges, if any.  Among the Kafka-based bridges that exist since v1, only Kafka changed
%% its type name.  Other bridges are either unchanged, or v2-only, and should use their v2
%% type names.
maybe_v1_type_name(Type) when is_atom(Type) -> maybe_v1_type_name(atom_to_binary(Type));
maybe_v1_type_name(<<"kafka_producer">>) -> <<"kafka">>;
maybe_v1_type_name(Type) -> Type.

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
