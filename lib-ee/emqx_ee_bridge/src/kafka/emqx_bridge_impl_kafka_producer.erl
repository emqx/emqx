%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_impl_kafka_producer).

%% callbacks of behaviour emqx_resource
-export([
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_get_status/2
]).

-export([
    on_kafka_ack/3,
    handle_telemetry_event/4
]).

-include_lib("emqx/include/logger.hrl").

callback_mode() -> async_if_possible.

%% @doc Config schema is defined in emqx_ee_bridge_kafka.
on_start(InstId, Config) ->
    #{
        bridge_name := BridgeName,
        bootstrap_hosts := Hosts0,
        connect_timeout := ConnTimeout,
        metadata_request_timeout := MetaReqTimeout,
        min_metadata_refresh_interval := MinMetaRefreshInterval,
        socket_opts := SocketOpts,
        authentication := Auth,
        ssl := SSL
    } = Config,
    _ = maybe_install_wolff_telemetry_handlers(InstId),
    %% it's a bug if producer config is not found
    %% the caller should not try to start a producer if
    %% there is no producer config
    ProducerConfigWrapper = get_required(producer, Config, no_kafka_producer_config),
    ProducerConfig = get_required(kafka, ProducerConfigWrapper, no_kafka_producer_parameters),
    MessageTemplate = get_required(message, ProducerConfig, no_kafka_message_template),
    Hosts = hosts(Hosts0),
    ClientId = make_client_id(BridgeName),
    ClientConfig = #{
        min_metadata_refresh_interval => MinMetaRefreshInterval,
        connect_timeout => ConnTimeout,
        client_id => ClientId,
        request_timeout => MetaReqTimeout,
        extra_sock_opts => socket_opts(SocketOpts),
        sasl => sasl(Auth),
        ssl => ssl(SSL)
    },
    #{
        topic := KafkaTopic
    } = ProducerConfig,
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
    WolffProducerConfig = producers_config(BridgeName, ClientId, ProducerConfig),
    case wolff:ensure_supervised_producers(ClientId, KafkaTopic, WolffProducerConfig) of
        {ok, Producers} ->
            {ok, #{
                message_template => compile_message_template(MessageTemplate),
                client_id => ClientId,
                producers => Producers
            }};
        {error, Reason2} ->
            ?SLOG(error, #{
                msg => "failed_to_start_kafka_producer",
                instance_id => InstId,
                kafka_hosts => Hosts,
                kafka_topic => KafkaTopic,
                reason => Reason2
            }),
            throw(failed_to_start_kafka_producer)
    end.

on_stop(InstanceID, #{client_id := ClientID, producers := Producers}) ->
    _ = with_log_at_error(
        fun() -> wolff:stop_and_delete_supervised_producers(Producers) end,
        #{
            msg => "failed_to_delete_kafka_producer",
            client_id => ClientID
        }
    ),
    _ = with_log_at_error(
        fun() -> wolff:stop_and_delete_supervised_client(ClientID) end,
        #{
            msg => "failed_to_delete_kafka_client",
            client_id => ClientID
        }
    ),
    with_log_at_error(
        fun() -> uninstall_telemetry_handlers(InstanceID) end,
        #{
            msg => "failed_to_uninstall_telemetry_handlers",
            client_id => ClientID
        }
    ).

%% @doc The callback API for rule-engine (or bridge without rules)
%% The input argument `Message' is an enriched format (as a map())
%% of the original #message{} record.
%% The enrichment is done by rule-engine or by the data bridge framework.
%% E.g. the output of rule-engine process chain
%% or the direct mapping from an MQTT message.
on_query(_InstId, {send_message, Message}, #{message_template := Template, producers := Producers}) ->
    KafkaMessage = render_message(Template, Message),
    %% The retuned information is discarded here.
    %% If the producer process is down when sending, this function would
    %% raise an error exception which is to be caught by the caller of this callback
    {_Partition, _Pid} = wolff:send(Producers, [KafkaMessage], {fun ?MODULE:on_kafka_ack/3, [#{}]}),
    ok.

compile_message_template(#{
    key := KeyTemplate, value := ValueTemplate, timestamp := TimestampTemplate
}) ->
    #{
        key => emqx_plugin_libs_rule:preproc_tmpl(KeyTemplate),
        value => emqx_plugin_libs_rule:preproc_tmpl(ValueTemplate),
        timestamp => emqx_plugin_libs_rule:preproc_tmpl(TimestampTemplate)
    }.

render_message(
    #{key := KeyTemplate, value := ValueTemplate, timestamp := TimestampTemplate}, Message
) ->
    #{
        key => render(KeyTemplate, Message),
        value => render(ValueTemplate, Message),
        ts => render_timestamp(TimestampTemplate, Message)
    }.

render(Template, Message) ->
    emqx_plugin_libs_rule:proc_tmpl(Template, Message).

render_timestamp(Template, Message) ->
    try
        binary_to_integer(render(Template, Message))
    catch
        _:_ ->
            erlang:system_time(millisecond)
    end.

on_kafka_ack(_Partition, _Offset, _Extra) ->
    %% Do nothing so far.
    %% Maybe need to bump some counters?
    ok.

on_get_status(_InstId, _State) ->
    connected.

%% Parse comma separated host:port list into a [{Host,Port}] list
hosts(Hosts) when is_binary(Hosts) ->
    hosts(binary_to_list(Hosts));
hosts(Hosts) when is_list(Hosts) ->
    kpro:parse_endpoints(Hosts).

%% Extra socket options, such as sndbuf size etc.
socket_opts(Opts) when is_map(Opts) ->
    socket_opts(maps:to_list(Opts));
socket_opts(Opts) when is_list(Opts) ->
    socket_opts_loop(Opts, []).

socket_opts_loop([], Acc) ->
    lists:reverse(Acc);
socket_opts_loop([{T, Bytes} | Rest], Acc) when
    T =:= sndbuf orelse T =:= recbuf orelse T =:= buffer
->
    Acc1 = [{T, Bytes} | adjust_socket_buffer(Bytes, Acc)],
    socket_opts_loop(Rest, Acc1);
socket_opts_loop([Other | Rest], Acc) ->
    socket_opts_loop(Rest, [Other | Acc]).

%% https://www.erlang.org/doc/man/inet.html
%% For TCP it is recommended to have val(buffer) >= val(recbuf)
%% to avoid performance issues because of unnecessary copying.
adjust_socket_buffer(Bytes, Opts) ->
    case lists:keytake(buffer, 1, Opts) of
        false ->
            [{buffer, Bytes} | Opts];
        {value, {buffer, Bytes1}, Acc1} ->
            [{buffer, max(Bytes1, Bytes)} | Acc1]
    end.

sasl(none) ->
    undefined;
sasl(#{mechanism := Mechanism, username := Username, password := Password}) ->
    {Mechanism, Username, emqx_secret:wrap(Password)};
sasl(#{
    kerberos_principal := Principal,
    kerberos_keytab_file := KeyTabFile
}) ->
    {callback, brod_gssapi, {gssapi, KeyTabFile, Principal}}.

ssl(#{enable := true} = SSL) ->
    emqx_tls_lib:to_client_opts(SSL);
ssl(_) ->
    [].

producers_config(BridgeName, ClientId, Input) ->
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
            memory_overload_protection := MemOLP
        }
    } = Input,

    {OffloadMode, ReplayqDir} =
        case BufferMode of
            memory -> {false, false};
            disk -> {false, replayq_dir(ClientId)};
            hybrid -> {true, replayq_dir(ClientId)}
        end,
    %% TODO: change this once we add kafka source
    BridgeType = kafka,
    ResourceID = emqx_bridge_resource:resource_id(BridgeType, BridgeName),
    #{
        name => make_producer_name(BridgeName),
        partitioner => PartitionStrategy,
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

replayq_dir(ClientId) ->
    filename:join([emqx:data_dir(), "kafka", ClientId]).

%% Client ID is better to be unique to make it easier for Kafka side trouble shooting.
make_client_id(BridgeName) when is_atom(BridgeName) ->
    make_client_id(atom_to_list(BridgeName));
make_client_id(BridgeName) ->
    iolist_to_binary([BridgeName, ":", atom_to_list(node())]).

%% Producer name must be an atom which will be used as a ETS table name for
%% partition worker lookup.
make_producer_name(BridgeName) when is_atom(BridgeName) ->
    make_producer_name(atom_to_list(BridgeName));
make_producer_name(BridgeName) ->
    %% Woff needs atom for ets table name registration
    %% The assumption here is bridge is not often re-created
    binary_to_atom(iolist_to_binary(["kafka_producer_", BridgeName])).

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

get_required(Field, Config, Throw) ->
    Value = maps:get(Field, Config, none),
    Value =:= none andalso throw(Throw),
    Value.

handle_telemetry_event(
    [wolff, dropped],
    #{counter_inc := Val},
    #{bridge_id := ID},
    _HandlerConfig
) when is_integer(Val) ->
    emqx_resource_metrics:dropped_inc(ID, Val);
handle_telemetry_event(
    [wolff, dropped_queue_full],
    #{counter_inc := Val},
    #{bridge_id := ID},
    _HandlerConfig
) when is_integer(Val) ->
    emqx_resource_metrics:dropped_queue_full_inc(ID, Val);
handle_telemetry_event(
    [wolff, queuing],
    #{counter_inc := Val},
    #{bridge_id := ID},
    _HandlerConfig
) when is_integer(Val) ->
    emqx_resource_metrics:queuing_change(ID, Val);
handle_telemetry_event(
    [wolff, retried],
    #{counter_inc := Val},
    #{bridge_id := ID},
    _HandlerConfig
) when is_integer(Val) ->
    emqx_resource_metrics:retried_inc(ID, Val);
handle_telemetry_event(
    [wolff, failed],
    #{counter_inc := Val},
    #{bridge_id := ID},
    _HandlerConfig
) when is_integer(Val) ->
    emqx_resource_metrics:failed_inc(ID, Val);
handle_telemetry_event(
    [wolff, inflight],
    #{counter_inc := Val},
    #{bridge_id := ID},
    _HandlerConfig
) when is_integer(Val) ->
    emqx_resource_metrics:inflight_change(ID, Val);
handle_telemetry_event(
    [wolff, retried_failed],
    #{counter_inc := Val},
    #{bridge_id := ID},
    _HandlerConfig
) when is_integer(Val) ->
    emqx_resource_metrics:retried_failed_inc(ID, Val);
handle_telemetry_event(
    [wolff, retried_success],
    #{counter_inc := Val},
    #{bridge_id := ID},
    _HandlerConfig
) when is_integer(Val) ->
    emqx_resource_metrics:retried_success_inc(ID, Val);
handle_telemetry_event(_EventId, _Metrics, _MetaData, _HandlerConfig) ->
    %% Event that we do not handle
    ok.

-spec telemetry_handler_id(emqx_resource:resource_id()) -> binary().
telemetry_handler_id(InstanceID) ->
    <<"emqx-bridge-kafka-producer-", InstanceID/binary>>.

uninstall_telemetry_handlers(InstanceID) ->
    HandlerID = telemetry_handler_id(InstanceID),
    telemetry:detach(HandlerID).

maybe_install_wolff_telemetry_handlers(InstanceID) ->
    %% Attach event handlers for Kafka telemetry events. If a handler with the
    %% handler id already exists, the attach_many function does nothing
    telemetry:attach_many(
        %% unique handler id
        telemetry_handler_id(InstanceID),
        %% Note: we don't handle `[wolff, success]' because,
        %% currently, we already increment the success counter for
        %% this resource at `emqx_rule_runtime:handle_action' when
        %% the response is `ok' and we would double increment it
        %% here.
        [
            [wolff, dropped],
            [wolff, dropped_queue_full],
            [wolff, queuing],
            [wolff, retried],
            [wolff, failed],
            [wolff, inflight],
            [wolff, retried_failed],
            [wolff, retried_success]
        ],
        fun ?MODULE:handle_telemetry_event/4,
        []
    ).
