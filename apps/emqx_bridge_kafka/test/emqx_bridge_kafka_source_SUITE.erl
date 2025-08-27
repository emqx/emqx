%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_kafka_source_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx/include/emqx_config.hrl").
-include_lib("brod/include/brod.hrl").
-include_lib("kafka_protocol/include/kpro.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-define(CONNECTOR_TYPE, kafka_consumer).
-define(CONNECTOR_TYPE_BIN, <<"kafka_consumer">>).
-define(SOURCE_TYPE, kafka_consumer).
-define(SOURCE_TYPE_BIN, <<"kafka_consumer">>).

-define(PROXY_HOST, "toxiproxy").
-define(PROXY_PORT, 8474).

-define(local, local).
-define(cluster, cluster).

-define(tcp_plain, tcp_plain).
-define(tcp_sasl, tcp_sasl).
-define(tls_plain, tls_plain).
-define(tls_sasl, tls_sasl).

-define(no_auth, no_auth).
-define(plain_auth, plain_auth).
-define(scram_sha256, scram_sha256).
-define(scram_sha512, scram_sha512).
-define(kerberos, kerberos).

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

-define(tpal(MSG), begin
    ct:pal(MSG),
    ?tp(notice, MSG, #{})
end).

-define(N_PARTITIONS, 3).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [
        {group, ?cluster},
        {group, ?local}
    ].

groups() ->
    AllTCs0 = emqx_common_test_helpers:all_with_matrix(?MODULE),
    AllTCs = lists:filter(
        fun
            ({group, _}) -> false;
            (_) -> true
        end,
        AllTCs0
    ),
    CustomMatrix0 = emqx_common_test_helpers:groups_with_matrix(?MODULE),
    CustomMatrix = lists:filter(
        fun
            (Spec) when element(1, Spec) == ?cluster ->
                false;
            (_) ->
                true
        end,
        CustomMatrix0
    ),
    ClusterTCs0 = cluster_testcases(),
    LocalTCs = merge_custom_groups(?local, (AllTCs ++ CustomMatrix) -- ClusterTCs0, CustomMatrix),
    ClusterTCs = merge_custom_groups(?cluster, ClusterTCs0, CustomMatrix),
    [
        {?cluster, ClusterTCs},
        {?local, LocalTCs}
    ].

merge_custom_groups(RootGroup, GroupTCs, CustomMatrix0) ->
    CustomMatrix =
        lists:flatmap(
            fun
                ({G, _, SubGroup}) when G == RootGroup ->
                    SubGroup;
                (_) ->
                    []
            end,
            CustomMatrix0
        ),
    CustomMatrix ++ GroupTCs.

cluster_testcases() ->
    Key = ?cluster,
    lists:filter(
        fun
            ({testcase, TestCase, _Opts}) ->
                emqx_common_test_helpers:get_tc_prop(?MODULE, TestCase, Key, false);
            (TestCase) ->
                emqx_common_test_helpers:get_tc_prop(?MODULE, TestCase, Key, false)
        end,
        emqx_common_test_helpers:all(?MODULE)
    ).

init_per_suite(TCConfig) ->
    TCConfig.

end_per_suite(_TCConfig) ->
    ok.

init_per_group(?cluster, TCConfig) ->
    reset_proxy(),
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_bridge_kafka,
            emqx_bridge,
            emqx_rule_engine
        ],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    emqx_bridge_kafka_testlib:wait_until_kafka_is_up(),
    %% Apparently, Kafka in 2+ brokers needs a moment to replicate kafka creation...  We
    %% need to wait or else starting producers to quickly will crash...
    %% Otherwise, publishers might get flaky and fail with:
    %% `throw(... cause => invalid_partition_count,count => -1 ...)`
    ct:pal("starting producers"),
    ProducersConfigs = start_producers(),
    [
        {apps, Apps},
        {kafka_producers, ProducersConfigs}
        | TCConfig
    ];
init_per_group(?local, TCConfig) ->
    reset_proxy(),
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_bridge_kafka,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    emqx_bridge_kafka_testlib:wait_until_kafka_is_up(),
    %% Apparently, Kafka in 2+ brokers needs a moment to replicate kafka creation...  We
    %% need to wait or else starting producers to quickly will crash...
    %% Otherwise, publishers might get flaky and fail with:
    %% `throw(... cause => invalid_partition_count,count => -1 ...)`
    ct:pal("starting producers"),
    ProducersConfigs = start_producers(),
    [
        {apps, Apps},
        {kafka_producers, ProducersConfigs}
        | TCConfig
    ];
init_per_group(?tcp_plain, TCConfig) ->
    Host = os:getenv("KAFKA_PLAIN_HOST", "toxiproxy.emqx.net"),
    Port = list_to_integer(os:getenv("KAFKA_PLAIN_PORT", "9292")),
    [
        {kafka_host, Host},
        {kafka_port, Port},
        {enable_tls, false},
        {proxy_name, "kafka_plain"},
        {proxy_name_2, "kafka_2_plain"},
        {proxy_host, ?PROXY_HOST},
        {proxy_port, ?PROXY_PORT}
        | TCConfig
    ];
init_per_group(?tcp_sasl, TCConfig) ->
    Host = os:getenv("KAFKA_SASL_PLAIN_HOST", "toxiproxy.emqx.net"),
    Port = list_to_integer(os:getenv("KAFKA_SASL_PORT", "9293")),
    [
        {kafka_host, Host},
        {kafka_port, Port},
        {enable_tls, false},
        {tls_config, no_tls()}
        | TCConfig
    ];
init_per_group(?tls_plain, TCConfig) ->
    Host = os:getenv("KAFKA_SSL_HOST", "toxiproxy.emqx.net"),
    Port = list_to_integer(os:getenv("KAFKA_SSL_PORT", "9294")),
    [
        {kafka_host, Host},
        {kafka_port, Port},
        {enable_tls, true},
        {tls_config, emqx_bridge_kafka_testlib:valid_ssl_settings()}
        | TCConfig
    ];
init_per_group(?tls_sasl, TCConfig) ->
    Host = os:getenv("KAFKA_SSL_SASL_HOST", "toxiproxy.emqx.net"),
    Port = list_to_integer(os:getenv("KAFKA_SSL_SASL_PORT", "9295")),
    [
        {kafka_host, Host},
        {kafka_port, Port},
        {enable_tls, true},
        {tls_config, emqx_bridge_kafka_testlib:valid_ssl_settings()}
        | TCConfig
    ];
init_per_group(?no_auth, TCConfig) ->
    [{auth, no_auth()} | TCConfig];
init_per_group(?plain_auth, TCConfig) ->
    [{auth, plain_auth()} | TCConfig];
init_per_group(?scram_sha256, TCConfig) ->
    [{auth, scram_sha256_auth()} | TCConfig];
init_per_group(?scram_sha512, TCConfig) ->
    [{auth, scram_sha512_auth()} | TCConfig];
init_per_group(?kerberos, TCConfig) ->
    %% Kerberos auth doesn't work with toxiproxy
    %% If one tries, errors like:
    %% ```
    %% SASL(-1): generic failure: GSSAPI Error: Unspecified GSS failure.  Minor code may
    %% provide more information (Server krbtgt/EMQX.NET@KDC.EMQX.NET not found in Kerberos
    %% database)
    %% ```
    %% ... may appear
    Host = "kafka-1.emqx.net",
    Port = 9095,
    [
        {kafka_host, Host},
        {kafka_port, Port},
        {enable_tls, true},
        {tls_config, emqx_bridge_kafka_testlib:valid_ssl_settings()},
        {auth, kerberos_auth()}
        | TCConfig
    ];
init_per_group(_Group, TCConfig) ->
    TCConfig.

end_per_group(?local, TCConfig) ->
    reset_proxy(),
    stop_producers(TCConfig),
    Apps = get_config(apps, TCConfig),
    emqx_cth_suite:stop(Apps),
    ok;
end_per_group(?cluster, TCConfig) ->
    reset_proxy(),
    stop_producers(TCConfig),
    Apps = get_config(apps, TCConfig),
    emqx_cth_suite:stop(Apps),
    ok;
end_per_group(_Group, _TCConfig) ->
    ok.

init_per_testcase(TestCase, TCConfig) ->
    reset_proxy(),
    Path = group_path(TCConfig, no_groups),
    ct:pal(asciiart:visible($%, "~p - ~s", [Path, TestCase])),
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    Name = <<(atom_to_binary(TestCase))/binary, UniqueNum/binary>>,
    ConnectorName = Name,
    ConnectorConfig = connector_config(#{
        <<"authentication">> => get_config(auth, TCConfig, no_auth()),
        <<"bootstrap_hosts">> => bootstrap_hosts_of(TCConfig),
        <<"ssl">> => get_config(tls_config, TCConfig, no_tls())
    }),
    SourceName = ConnectorName,
    Topic = Name,
    SourceConfig = source_config(#{
        <<"connector">> => ConnectorName,
        <<"parameters">> => #{<<"topic">> => Topic}
    }),
    ct:pal("creating kafka topic"),
    ensure_kafka_topic(Topic),
    snabbkaffe:start_trace(),
    [
        {bridge_kind, source},
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, ConnectorName},
        {connector_config, ConnectorConfig},
        {source_type, ?SOURCE_TYPE},
        {source_name, SourceName},
        {source_config, SourceConfig}
        | TCConfig
    ].

end_per_testcase(_TestCase, _TCConfig) ->
    reset_proxy(),
    snabbkaffe:stop(),
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

bootstrap_hosts_of(TCConfig) ->
    Host = get_config(kafka_host, TCConfig, <<"toxiproxy.emqx.net">>),
    Port = get_config(kafka_port, TCConfig, 9292),
    emqx_bridge_v2_testlib:fmt(<<"${h}:${p}">>, #{h => Host, p => Port}).

no_tls() ->
    #{<<"enable">> => false}.

connector_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my connector">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"bootstrap_hosts">> => <<"kafka-1.emqx.net:9092">>,
        <<"authentication">> => no_auth(),
        <<"connect_timeout">> => <<"5s">>,
        <<"metadata_request_timeout">> => <<"5s">>,
        <<"min_metadata_refresh_interval">> => <<"3s">>,
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_connector_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check_connector(?CONNECTOR_TYPE_BIN, <<"x">>, InnerConfigMap).

source_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my action">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"parameters">> => #{
            <<"key_encoding_mode">> => <<"none">>,
            <<"max_batch_bytes">> => <<"896KB">>,
            <<"max_wait_time">> => <<"500ms">>,
            <<"max_rejoin_attempts">> => <<"5">>,
            <<"offset_reset_policy">> => <<"earliest">>,
            <<"topic">> => <<"please override">>,
            <<"value_encoding_mode">> => <<"none">>
        },
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_source_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(source, ?SOURCE_TYPE_BIN, <<"x">>, InnerConfigMap).

no_auth() ->
    emqx_bridge_kafka_testlib:no_auth().

plain_auth() ->
    emqx_bridge_kafka_testlib:plain_auth().

scram_sha256_auth() ->
    emqx_bridge_kafka_testlib:scram_sha256_auth().

scram_sha512_auth() ->
    emqx_bridge_kafka_testlib:scram_sha512_auth().

kerberos_auth() ->
    emqx_bridge_kafka_testlib:kerberos_auth().

msk_iam_auth() ->
    emqx_bridge_kafka_testlib:msk_iam_auth().

get_config(K, TCConfig) -> emqx_bridge_v2_testlib:get_value(K, TCConfig).
get_config(K, TCConfig, Default) -> proplists:get_value(K, TCConfig, Default).

group_path(TCConfig, Default) ->
    case emqx_common_test_helpers:group_path(TCConfig) of
        [] -> Default;
        Path -> Path
    end.

get_tc_prop(TestCase, Key, Default) ->
    maybe
        true ?= erlang:function_exported(?MODULE, TestCase, 0),
        {Key, Val} ?= proplists:lookup(Key, ?MODULE:TestCase()),
        Val
    else
        _ -> Default
    end.

reset_proxy() ->
    emqx_common_test_helpers:reset_proxy(?PROXY_HOST, ?PROXY_PORT).

%% N.B.: Only works with plain TCP endpoint
with_brokers_down(TCConfig, Fun) ->
    ProxyName1 = get_config(proxy_name, TCConfig),
    ProxyName2 = get_config(proxy_name_2, TCConfig),
    ProxyHost = get_config(proxy_host, TCConfig),
    ProxyPort = get_config(proxy_port, TCConfig),
    emqx_common_test_helpers:with_failure(
        down, ProxyName1, ProxyHost, ProxyPort, fun() ->
            emqx_common_test_helpers:with_failure(down, ProxyName2, ProxyHost, ProxyPort, Fun)
        end
    ).

ensure_kafka_topic(KafkaTopic) ->
    emqx_bridge_kafka_testlib:ensure_kafka_topic(KafkaTopic, #{num_partitions => ?N_PARTITIONS}).

delete_kafka_topic(KafkaTopic) ->
    emqx_bridge_kafka_testlib:delete_kafka_topic(KafkaTopic).

start_producers() ->
    KafkaClientId =
        <<"test-client-", (atom_to_binary(?MODULE))/binary,
            (integer_to_binary(erlang:unique_integer()))/binary>>,
    Hosts = emqx_bridge_kafka_testlib:kafka_hosts_direct(),
    ClientConfig = #{
        min_metadata_refresh_interval => 5_000,
        connect_timeout => 5_000,
        client_id => KafkaClientId,
        request_timeout => 1_000,
        sasl => undefined,
        ssl => []
    },
    {ok, Clients} = wolff:ensure_supervised_client(KafkaClientId, Hosts, ClientConfig),
    ProducerConfig =
        #{
            name => atom_to_binary(?MODULE),
            partitioner => random,
            partition_count_refresh_interval_seconds => 1_000,
            replayq_max_total_bytes => 10_000,
            replayq_seg_bytes => 9_000,
            drop_if_highmem => false,
            required_acks => leader_only,
            max_batch_bytes => 900_000,
            max_send_ahead => 0,
            compression => no_compression,
            telemetry_meta_data => #{}
        },
    {ok, Producers} = wolff:ensure_supervised_dynamic_producers(KafkaClientId, ProducerConfig),
    #{
        clients => Clients,
        clientid => KafkaClientId,
        producers => Producers
    }.

stop_producers(TCConfig) ->
    #{
        clientid := KafkaClientId,
        producers := Producers
    } = get_config(kafka_producers, TCConfig),
    ok = wolff:stop_and_delete_supervised_producers(Producers),
    ok = wolff:stop_and_delete_supervised_client(KafkaClientId),
    ok.

consumer_group_id(TCConfig) ->
    #{name := Name} = emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_kafka_impl_consumer:consumer_group_id(#{}, Name).

consumer_clientid(TCConfig) ->
    ConnResId = emqx_bridge_v2_testlib:connector_resource_id(TCConfig),
    emqx_bridge_kafka_impl_consumer:make_client_id(ConnResId).

get_client_connection(TCConfig) ->
    KafkaHost = get_config(kafka_host, TCConfig, "kafka-1.emqx.net"),
    KafkaPort = get_config(kafka_port, TCConfig, 9092),
    ClientID = consumer_clientid(TCConfig),
    brod_client:get_connection(ClientID, KafkaHost, KafkaPort).

get_subscriber_workers() ->
    [{_, SubscriberPid, _, _}] = supervisor:which_children(emqx_bridge_kafka_consumer_sup),
    brod_group_subscriber_v2:get_workers(SubscriberPid).

wait_downs(Refs, _Timeout) when map_size(Refs) =:= 0 ->
    ok;
wait_downs(Refs0, Timeout) ->
    receive
        {'DOWN', Ref, process, _Pid, _Reason} when is_map_key(Ref, Refs0) ->
            Refs = maps:remove(Ref, Refs0),
            wait_downs(Refs, Timeout)
    after Timeout ->
        ct:fail("processes didn't die; remaining: ~p", [map_size(Refs0)])
    end.

publish_kafka(TCConfig, Messages) ->
    #{<<"parameters">> := #{<<"topic">> := Topic}} =
        get_config(source_config, TCConfig),
    publish_kafka(TCConfig, Topic, Messages).

publish_kafka(TCConfig, KafkaTopic, Messages) ->
    #{producers := Producers} = get_config(kafka_producers, TCConfig),
    ct:pal("publishing to ~p:\n ~p", [KafkaTopic, Messages]),
    {_Partition, _OffsetReply} = wolff:send_sync2(Producers, KafkaTopic, Messages, 10_000).

create_connector_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:create_connector_api2(TCConfig, Overrides).

update_connector_api(TCConfig, Overrides) ->
    #{
        connector_type := Type,
        connector_name := Name,
        connector_config := Cfg0
    } =
        emqx_bridge_v2_testlib:get_common_values_with_configs(TCConfig),
    Cfg = emqx_utils_maps:deep_merge(Cfg0, Overrides),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:update_connector_api(Name, Type, Cfg)
    ).

create_source_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:create_source_api(TCConfig, Overrides).

get_source_api(TCConfig) ->
    #{type := Type, name := Name} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_source_api(Type, Name)
    ).

delete_source_api(TCConfig) ->
    #{type := Type, name := Name} = emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:delete_kind_api(source, Type, Name).

probe_connector_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:probe_connector_api2(TCConfig, Overrides).

probe_source_api(TCConfig) ->
    probe_source_api(TCConfig, _Overrides = #{}).

probe_source_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:probe_bridge_api(TCConfig, Overrides)
    ).

get_source_metrics_api(Config) ->
    emqx_bridge_v2_testlib:get_source_metrics_api(
        Config
    ).

simple_create_rule_api(TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(TCConfig).

simple_create_rule_api(Opts, TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(Opts, TCConfig).

start_client() ->
    start_client(_Opts = #{}).

start_client(Opts) ->
    {ok, C} = emqtt:start_link(Opts),
    on_exit(fun() -> emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    C.

unique_payload() ->
    emqx_guid:to_hexstr(emqx_guid:gen()).

wait_until_group_is_balanced(KafkaTopic, NPartitions, Nodes, Timeout) ->
    do_wait_until_group_is_balanced(KafkaTopic, NPartitions, Nodes, Timeout, #{}).

do_wait_until_group_is_balanced(KafkaTopic, NPartitions, Nodes, Timeout, Acc0) ->
    AllPartitionsCovered = map_size(Acc0) =:= NPartitions,
    PresentNodes = lists:usort([N || {_Partition, {N, _MemberId}} <- maps:to_list(Acc0)]),
    AllNodesCovered = PresentNodes =:= lists:usort(Nodes),
    case AllPartitionsCovered andalso AllNodesCovered of
        true ->
            ct:pal("group balanced: ~p", [Acc0]),
            {ok, Acc0};
        false ->
            receive
                {kafka_assignment, Node, {Pid, MemberId, GenerationId, TopicAssignments}} ->
                    Event = #{
                        node => Node,
                        pid => Pid,
                        member_id => MemberId,
                        generation_id => GenerationId,
                        topic_assignments => TopicAssignments
                    },
                    Acc = reconstruct_assignments_from_events(KafkaTopic, [Event], Acc0),
                    do_wait_until_group_is_balanced(KafkaTopic, NPartitions, Nodes, Timeout, Acc)
            after Timeout ->
                {timeout, Acc0}
            end
    end.

wait_for_expected_published_messages(Messages0, Timeout) ->
    Messages = maps:from_list([{K, Msg} || Msg = #{key := K} <- Messages0]),
    do_wait_for_expected_published_messages(Messages, [], Timeout).

do_wait_for_expected_published_messages(Messages, Acc, _Timeout) when map_size(Messages) =:= 0 ->
    lists:reverse(Acc);
do_wait_for_expected_published_messages(Messages0, Acc0, Timeout) ->
    receive
        {publish, Msg0 = #{payload := Payload}} ->
            case emqx_utils_json:safe_decode(Payload) of
                {error, _} ->
                    ct:pal("unexpected message: ~p; discarding", [Msg0]),
                    do_wait_for_expected_published_messages(Messages0, Acc0, Timeout);
                {ok, Decoded = #{<<"key">> := K}} when is_map_key(K, Messages0) ->
                    Msg = Msg0#{payload := Decoded},
                    ct:pal("received expected message: ~p", [Msg]),
                    Acc = [Msg | Acc0],
                    Messages = maps:remove(K, Messages0),
                    do_wait_for_expected_published_messages(Messages, Acc, Timeout);
                {ok, Decoded} ->
                    ct:pal("unexpected message: ~p; discarding", [Msg0#{payload := Decoded}]),
                    do_wait_for_expected_published_messages(Messages0, Acc0, Timeout)
            end
    after Timeout ->
        error(
            {timed_out_waiting_for_published_messages, #{
                so_far => Acc0,
                remaining => Messages0,
                mailbox => process_info(self(), messages)
            }}
        )
    end.

receive_published() ->
    receive_published(#{}).

receive_published(Opts0) ->
    Default = #{n => 1, timeout => 10_000},
    Opts = maps:merge(Default, Opts0),
    receive_published(Opts, []).

receive_published(#{n := N, timeout := _Timeout}, Acc) when N =< 0 ->
    Res = lists:reverse(Acc),
    ct:pal("received:\n  ~p", [Res]),
    Res;
receive_published(#{n := N, timeout := Timeout} = Opts, Acc) ->
    receive
        {publish, Msg} ->
            receive_published(Opts#{n := N - 1}, [Msg | Acc])
    after Timeout ->
        error(
            {timeout, #{
                msgs_so_far => Acc,
                mailbox => process_info(self(), messages),
                expected_remaining => N
            }}
        )
    end.

receive_published_payloads(Opts0) ->
    Default = #{n => 1, timeout => 10_000},
    #{expected := Expected0} = Opts = maps:merge(Default, Opts0),
    Expected = maps:from_keys(Expected0, true),
    receive_published_payloads(Expected, Opts, []).

receive_published_payloads(Expected, _Opts, Acc) when map_size(Expected) == 0 ->
    lists:reverse(Acc);
receive_published_payloads(Expected0, Opts, Acc) ->
    #{timeout := Timeout} = Opts,
    receive
        {publish, #{payload := Payload0} = Msg} ->
            case emqx_utils_json:decode(Payload0) of
                #{<<"value">> := V} when is_map_key(V, Expected0) ->
                    Expected = maps:remove(V, Expected0),
                    receive_published_payloads(Expected, Opts, [Msg | Acc]);
                #{} ->
                    ct:pal("unexpected publish:\n  ~p", [Msg]),
                    receive_published_payloads(Expected0, Opts, Acc)
            end
    after Timeout ->
        error(
            {timeout, #{
                msgs_so_far => lists:reverse(Acc),
                mailbox => process_info(self(), messages),
                expected_remaining => Expected0
            }}
        )
    end.

create_bridge_wait_for_balance(TCConfig) ->
    setup_group_subscriber_spy(self()),
    try
        {201, #{<<"status">> := <<"connected">>}} = create_connector_api(TCConfig, #{}),
        {201, #{<<"status">> := <<"connected">>}} = create_source_api(TCConfig, #{}),
        receive
            {kafka_assignment, _, _} ->
                ok
        after 20_000 ->
            ct:fail("timed out waiting for kafka assignment")
        end
    after
        kill_group_subscriber_spy()
    end.

reconstruct_assignments_from_events(KafkaTopic, Events) ->
    reconstruct_assignments_from_events(KafkaTopic, Events, #{}).

reconstruct_assignments_from_events(KafkaTopic, Events0, Acc0) ->
    %% when running the test multiple times with the same kafka
    %% cluster, kafka will send assignments from old test topics that
    %% we must discard.
    Assignments = [
        {MemberId, Node, P}
     || #{
            node := Node,
            member_id := MemberId,
            topic_assignments := Assignments
        } <- Events0,
        #brod_received_assignment{topic = T, partition = P} <- Assignments,
        T =:= KafkaTopic
    ],
    ct:pal("assignments for topic ~p:\n ~p", [KafkaTopic, Assignments]),
    lists:foldl(
        fun({MemberId, Node, Partition}, Acc) ->
            Acc#{Partition => {Node, MemberId}}
        end,
        Acc0,
        Assignments
    ).

setup_group_subscriber_spy_fn() ->
    TestPid = self(),
    fun() ->
        setup_group_subscriber_spy(TestPid)
    end.

setup_group_subscriber_spy(TestPid) ->
    ok = meck:new(brod_group_subscriber_v2, [
        passthrough, no_link, no_history
    ]),
    ok = meck:expect(
        brod_group_subscriber_v2,
        assignments_received,
        fun(Pid, MemberId, GenerationId, TopicAssignments) ->
            ?tp(
                kafka_assignment,
                #{
                    node => node(),
                    pid => Pid,
                    member_id => MemberId,
                    generation_id => GenerationId,
                    topic_assignments => TopicAssignments
                }
            ),
            TestPid !
                {kafka_assignment, node(), {Pid, MemberId, GenerationId, TopicAssignments}},
            meck:passthrough([Pid, MemberId, GenerationId, TopicAssignments])
        end
    ),
    ok.

kill_group_subscriber_spy() ->
    meck:unload(brod_group_subscriber_v2).

get_root_configs_for_cluster(TCConfig) ->
    #{
        name := Name,
        config := SourceConfig,
        connector_name := ConnectorName,
        connector_config := ConnectorConfig
    } =
        emqx_bridge_v2_testlib:get_common_values_with_configs(TCConfig),
    ConnectorRootConfig = #{
        <<"connectors">> => #{?CONNECTOR_TYPE_BIN => #{ConnectorName => ConnectorConfig}}
    },
    BridgeRootConfig = #{
        <<"sources">> => #{?SOURCE_TYPE_BIN => #{Name => SourceConfig}}
    },
    #{
        connector_root_config => ConnectorRootConfig,
        bridge_root_config => BridgeRootConfig
    }.

cluster(TestCase, TCConfig) ->
    cluster(TestCase, _Opts = #{}, TCConfig).

cluster(TestCase, Opts, TCConfig) ->
    ConnectorRootConfig = maps:get(connector_root_config, Opts, #{}),
    BridgeRootConfig = maps:get(bridge_root_config, Opts, #{}),
    AppSpecs = [
        emqx_conf,
        {emqx_bridge_kafka, #{after_start => setup_group_subscriber_spy_fn()}},
        {emqx_connector, #{config => ConnectorRootConfig}},
        {emqx_bridge, #{
            schema_mod => emqx_bridge_v2_schema,
            config => BridgeRootConfig
        }},
        emqx_rule_engine,
        emqx_management
    ],
    NodeSpecs = emqx_cth_cluster:mk_nodespecs(
        [
            {node_name(TestCase, 1), #{
                apps => AppSpecs ++
                    [emqx_mgmt_api_test_util:emqx_dashboard()]
            }},
            {node_name(TestCase, 2), #{
                apps => AppSpecs
            }}
        ],
        #{work_dir => emqx_cth_suite:work_dir(TestCase, TCConfig)}
    ),
    ct:pal("cluster: ~p", [NodeSpecs]),
    NodeSpecs.

node_name(TestCase, N) ->
    binary_to_atom(iolist_to_binary(io_lib:format("~s_~b", [TestCase, N]))).

get_mqtt_port(Node) ->
    {_IP, Port} = ?ON(Node, emqx_config:get([listeners, tcp, default, bind])),
    Port.

start_async_publisher(TCConfig, KafkaTopic) ->
    TId = ets:new(kafka_payloads, [public, ordered_set]),
    Loop = fun Go() ->
        receive
            stop -> ok
        after 0 ->
            Payload = emqx_guid:to_hexstr(emqx_guid:gen()),
            publish_kafka(TCConfig, KafkaTopic, [#{key => Payload, value => Payload}]),
            ets:insert(TId, {Payload}),
            timer:sleep(400),
            Go()
        end
    end,
    Pid = spawn_link(Loop),
    {TId, Pid}.

stop_async_publisher(Pid) ->
    MRef = monitor(process, Pid),
    Pid ! stop,
    receive
        {'DOWN', MRef, process, Pid, _} ->
            ok
    after 1_000 ->
        ct:fail("publisher didn't die")
    end,
    ok.

kill_resource_managers() ->
    ct:pal("gonna kill resource managers"),
    lists:foreach(
        fun({_, Pid, _, _}) ->
            ct:pal("terminating resource manager ~p", [Pid]),
            Ref = monitor(process, Pid),
            exit(Pid, kill),
            receive
                {'DOWN', Ref, process, Pid, killed} ->
                    ok
            after 500 ->
                ct:fail("pid ~p didn't die!", [Pid])
            end,
            ok
        end,
        supervisor:which_children(emqx_resource_manager_sup)
    ).

%% For things like listing groups, apparently different brokers return different
%% responses, and we need to query more than one...
all_bootstrap_hosts() ->
    [
        {<<"kafka-1.emqx.net">>, 9092},
        {<<"kafka-2.emqx.net">>, 9092}
    ].

get_groups() ->
    lists:foldl(
        fun(Endpoint, Acc) ->
            {ok, Groups} = brod:list_groups(Endpoint, _ConnOpts = #{}),
            Groups ++ Acc
        end,
        [],
        all_bootstrap_hosts()
    ).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop() ->
    [{matrix, true}].
t_start_stop(matrix) ->
    [
        [?tcp_plain, ?no_auth],
        [?tcp_sasl, ?plain_auth],
        [?tcp_sasl, ?scram_sha256],
        [?tcp_sasl, ?scram_sha512],
        [?tcp_sasl, ?kerberos],
        [?tls_plain, ?no_auth],
        [?tls_sasl, ?plain_auth]
    ];
t_start_stop(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_start_stop(TCConfig, "kafka_consumer_stopped").

t_on_get_status() ->
    [{matrix, true}].
t_on_get_status(matrix) ->
    [[?tcp_plain, ?no_auth]];
t_on_get_status(TCConfig) ->
    Opts = #{
        connector_overrides => #{
            <<"resource_opts">> => #{<<"health_check_interval">> => <<"200ms">>}
        },
        source_overrides => #{
            <<"resource_opts">> => #{<<"health_check_interval">> => <<"200ms">>}
        }
    },
    emqx_bridge_v2_testlib:t_on_get_status(TCConfig, Opts).

t_consume() ->
    [{matrix, true}].
t_consume(matrix) ->
    [
        [?tcp_plain, ?no_auth],
        [?tcp_sasl, ?plain_auth],
        [?tcp_sasl, ?scram_sha256],
        [?tcp_sasl, ?scram_sha512],
        [?tcp_sasl, ?kerberos],
        [?tls_plain, ?no_auth],
        [?tls_sasl, ?plain_auth]
    ];
t_consume(TCConfig) ->
    NumPartitions = 1,
    Key = <<"mykey">>,
    Payload = #{<<"key">> => <<"value">>},
    Encoded = emqx_utils_json:encode(Payload),
    Headers = [{<<"hkey">>, <<"hvalue">>}],
    HeadersMap = maps:from_list(Headers),
    ProduceFn = fun() ->
        publish_kafka(
            TCConfig,
            [
                #{
                    key => Key,
                    value => Encoded,
                    headers => Headers
                }
            ]
        )
    end,
    CheckFn = fun(Message) ->
        ?assertMatch(
            #{
                headers := HeadersMap,
                key := Key,
                offset := _,
                topic := _Topic,
                ts := _,
                ts_type := _,
                value := Encoded
            },
            Message
        )
    end,
    Opts = #{
        test_timeout => timer:seconds(20),
        consumer_ready_tracepoint => ?match_n_events(
            NumPartitions,
            #{?snk_kind := kafka_consumer_subscriber_init}
        ),
        produce_fn => ProduceFn,
        check_fn => CheckFn,
        produce_tracepoint => ?match_event(
            #{
                ?snk_kind := kafka_consumer_handle_message,
                ?snk_span := {complete, _}
            }
        )
    },
    ok = emqx_bridge_v2_testlib:t_consume(TCConfig, Opts),
    ok.

%% ensure that we can create and use the bridge successfully after
%% creating it with bad config.
t_failed_creation_then_fixed() ->
    [{matrix, true}].
t_failed_creation_then_fixed(matrix) ->
    [[?tcp_sasl, ?plain_auth]];
t_failed_creation_then_fixed(TCConfig) ->
    ct:timetrap({seconds, 180}),
    {201, _} = create_connector_api(TCConfig, #{
        <<"authentication">> => #{<<"password">> => <<"wrong password">>}
    }),
    {201, _} = create_source_api(TCConfig, #{}),
    ?retry(
        _Interval0 = 200,
        _Attempts0 = 10,
        begin
            ClientConn0 = get_client_connection(TCConfig),
            case ClientConn0 of
                {error, client_down} ->
                    ok;
                {error, {client_down, _Stacktrace}} ->
                    ok;
                _ ->
                    error({client_should_be_down, ClientConn0})
            end
        end
    ),
    %% now, update with the correct configuration
    ?assertMatch(
        {{200, #{<<"status">> := <<"connected">>}}, {ok, _}},
        ?wait_async_action(
            update_connector_api(TCConfig, #{
                <<"resource_opts">> =>
                    #{<<"health_check_interval">> => <<"1s">>}
            }),
            #{?snk_kind := kafka_consumer_subscriber_started},
            60_000
        )
    ),
    #{topic := MQTTTopic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    {ok, _, [0]} = emqtt:subscribe(C, MQTTTopic),
    Payload = unique_payload(),
    {_, {ok, _}} =
        ?wait_async_action(
            publish_kafka(TCConfig, [
                #{
                    key => <<"mykey">>,
                    value => Payload,
                    headers => [{<<"hkey">>, <<"hvalue">>}]
                }
            ]),
            #{?snk_kind := kafka_consumer_handle_message, ?snk_span := {complete, _}},
            20_000
        ),
    Published = receive_published(),
    ?assertMatch(
        [
            #{
                qos := _,
                topic := MQTTTopic,
                payload := _
            }
        ],
        Published
    ),
    [#{payload := PayloadBin}] = Published,
    ?assertMatch(
        #{
            <<"value">> := Payload,
            <<"key">> := <<"mykey">>,
            <<"topic">> := _KafkaTopic,
            <<"offset">> := _,
            <<"headers">> := #{<<"hkey">> := <<"hvalue">>}
        },
        emqx_utils_json:decode(PayloadBin),
        #{
            payload => Payload
        }
    ),
    ok.

%% check that we commit the offsets so that restarting an emqx node or
%% recovering from a network partition will make the subscribers
%% consume the messages produced during the down time.
t_receive_after_recovery() ->
    [{matrix, true}].
t_receive_after_recovery(matrix) ->
    [[?tcp_plain, ?no_auth]];
t_receive_after_recovery(TCConfig) ->
    _ = get_config(proxy_name, TCConfig),
    ct:timetrap(120_000),
    KafkaClientId = consumer_clientid(TCConfig),
    NPartitions = ?N_PARTITIONS,
    ?check_trace(
        begin
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, #{<<"status">> := <<"connected">>}} = create_source_api(TCConfig, #{}),
            %% 0) ensure each partition commits its offset so it can
            %% recover later.
            Messages0 = [
                #{
                    key => <<"commit", (integer_to_binary(N))/binary>>,
                    value => <<"commit", (integer_to_binary(N))/binary>>
                }
             || N <- lists:seq(1, NPartitions * 10)
            ],
            %% we do distinct passes over this producing part so that
            %% wolff won't batch everything together.
            lists:foreach(
                fun(Msg) ->
                    {_, {ok, _}} =
                        ?wait_async_action(
                            publish_kafka(TCConfig, [Msg]),
                            #{
                                ?snk_kind := kafka_consumer_handle_message,
                                ?snk_span := {complete, {ok, commit, _}}
                            },
                            _Timeout1 = 2_000
                        )
                end,
                Messages0
            ),
            ?retry(
                _Interval = 200,
                _NAttempts = 20,
                begin
                    GroupId = consumer_group_id(TCConfig),
                    {ok, [#{partitions := Partitions}]} = brod:fetch_committed_offsets(
                        KafkaClientId, GroupId
                    ),
                    NPartitions = length(Partitions)
                end
            ),
            %% we need some time to avoid flakiness due to the
            %% subscription happening while the consumers are still
            %% publishing messages...
            ct:sleep(500),

            %% 1) cut the connection with kafka.
            WorkerRefs = maps:from_list([
                {monitor(process, Pid), Pid}
             || Pid <- maps:values(get_subscriber_workers())
            ]),
            NumMsgs = 50,
            Messages1 = [
                begin
                    X = emqx_guid:to_hexstr(emqx_guid:gen()),
                    #{
                        key => X,
                        value => X
                    }
                end
             || _ <- lists:seq(1, NumMsgs)
            ],
            #{topic := MQTTTopic} = simple_create_rule_api(TCConfig),
            C = start_client(),
            {ok, _, [0]} = emqtt:subscribe(C, MQTTTopic),
            with_brokers_down(TCConfig, fun() ->
                wait_downs(WorkerRefs, _Timeout2 = 1_000),
                %% 2) publish messages while the consumer is down.
                %% we use `pmap' to avoid wolff sending the whole
                %% batch to a single partition.
                emqx_utils:pmap(fun(Msg) -> publish_kafka(TCConfig, [Msg]) end, Messages1),
                ok
            end),
            %% 3) restore and consume messages
            {ok, SRef1} = snabbkaffe:subscribe(
                ?match_event(#{
                    ?snk_kind := kafka_consumer_handle_message,
                    ?snk_span := {complete, _}
                }),
                NumMsgs,
                _Timeout3 = 60_000
            ),
            {ok, _} = snabbkaffe:receive_events(SRef1),
            #{num_msgs => NumMsgs, msgs => lists:sort(Messages1)}
        end,
        fun(#{num_msgs := NumMsgs, msgs := ExpectedMsgs}, Trace) ->
            Received0 = wait_for_expected_published_messages(ExpectedMsgs, _Timeout4 = 2_000),
            Received1 =
                lists:map(
                    fun(#{payload := #{<<"key">> := K, <<"value">> := V}}) ->
                        #{key => K, value => V}
                    end,
                    Received0
                ),
            Received = lists:sort(Received1),
            ?assertEqual(ExpectedMsgs, Received),
            ?assert(length(?of_kind(kafka_consumer_handle_message, Trace)) > NumMsgs * 2),
            ok
        end
    ),
    ok.

t_dynamic_mqtt_topic(TCConfig) ->
    #{<<"parameters">> := #{<<"topic">> := KafkaTopic}} =
        get_config(source_config, TCConfig),
    Payload = unique_payload(),
    ?check_trace(
        begin
            ?assertMatch(ok, create_bridge_wait_for_balance(TCConfig)),
            Opts = #{
                sql => auto,
                republish_overrides => #{
                    <<"topic">> => <<"${.topic}/${.value}/${.headers.hkey}">>
                }
            },
            #{} = simple_create_rule_api(Opts, TCConfig),
            C = start_client(),
            {ok, _, [0]} = emqtt:subscribe(C, <<"#">>),
            ct:pal("subscribed to mqtt topic filter"),

            {ok, SRef0} = snabbkaffe:subscribe(
                ?match_event(#{
                    ?snk_kind := kafka_consumer_handle_message, ?snk_span := {complete, _}
                }),
                _NumMsgs = 3,
                20_000
            ),
            {_Partition, _OffsetReply} =
                publish_kafka(TCConfig, [
                    %% this will have the last segment defined
                    #{
                        key => <<"mykey">>,
                        value => Payload,
                        headers => [{<<"hkey">>, <<"hvalue">>}]
                    },
                    %% this will not
                    #{
                        key => <<"mykey">>,
                        value => Payload
                    },
                    %% will inject an invalid topic segment
                    #{
                        key => <<"mykey">>,
                        value => <<"+">>
                    }
                ]),
            {ok, _} = snabbkaffe:receive_events(SRef0),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([_Enter, _Complete | _], ?of_kind(kafka_consumer_handle_message, Trace)),
            %% the message with invalid topic will fail to be published
            Published = receive_published(#{n => 2}),
            ExpectedMQTTTopic0 = emqx_topic:join([KafkaTopic, Payload, <<"hvalue">>]),
            %% Note: when topic mapping was supported, this would be <<>>
            %% Apparently, cannot be reproduced by rule engine republish action...
            ExpectedMQTTTopic1 = emqx_topic:join([KafkaTopic, Payload, <<"undefined">>]),
            ?assertMatch(
                [
                    #{
                        topic := ExpectedMQTTTopic0
                    },
                    #{
                        topic := ExpectedMQTTTopic1
                    }
                ],
                Published
            ),
            ?retry(
                200,
                10,
                ?assertMatch(
                    {200, #{
                        <<"metrics">> := #{<<"received">> := 3}
                    }},
                    get_source_metrics_api(TCConfig)
                )
            ),
            ?assertError({timeout, _}, receive_published(#{timeout => 500})),
            ok
        end
    ),
    ok.

%% checks that an existing cluster can be configured with a kafka
%% consumer bridge and that the consumers will distribute over the two
%% nodes.
t_cluster_group() ->
    [{?cluster, true}].
t_cluster_group(TCConfig) ->
    ct:timetrap({seconds, 150}),
    NPartitions = ?N_PARTITIONS,
    #{type := Type, name := Name} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    #{<<"parameters">> := #{<<"topic">> := KafkaTopic}} =
        get_config(source_config, TCConfig),
    Cluster = cluster(?FUNCTION_NAME, TCConfig),
    ?check_trace(
        begin
            Nodes = [_N1, N2 | _] = emqx_cth_cluster:start(Cluster),
            on_exit(fun() -> emqx_cth_cluster:stop(Nodes) end),
            Fun = fun() -> ?ON(N2, emqx_mgmt_api_test_util:auth_header_()) end,
            emqx_bridge_v2_testlib:set_auth_header_getter(Fun),
            {ok, SRef0} = snabbkaffe:subscribe(
                ?match_event(#{?snk_kind := kafka_consumer_subscriber_started}),
                length(Nodes),
                15_000
            ),
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, _} = create_source_api(TCConfig, #{}),
            {ok, _} = snabbkaffe:receive_events(SRef0),
            lists:foreach(
                fun(N) ->
                    ?assertMatch(
                        {ok, _},
                        ?ON(N, emqx_bridge_v2:lookup(?global_ns, sources, Type, Name)),
                        #{node => N}
                    )
                end,
                Nodes
            ),

            %% give kafka some time to rebalance the group; we need to
            %% sleep so that the two nodes have time to distribute the
            %% subscribers, rather than just one node containing all
            %% of them.
            {ok, _} = wait_until_group_is_balanced(KafkaTopic, NPartitions, Nodes, 30_000),
            lists:foreach(
                fun(N) ->
                    ?assertMatch(
                        #{status := ?status_connected},
                        ?ON(
                            N,
                            emqx_bridge_v2_testlib:force_health_check(
                                emqx_bridge_v2_testlib:get_common_values(TCConfig)
                            )
                        ),
                        #{node => N}
                    )
                end,
                Nodes
            ),

            #{nodes => Nodes}
        end,
        fun(Res, Trace0) ->
            #{nodes := Nodes} = Res,
            Trace1 = ?of_kind(kafka_assignment, Trace0),
            Assignments = reconstruct_assignments_from_events(KafkaTopic, Trace1),
            ?assertEqual(
                lists:usort(Nodes),
                lists:usort([
                    N
                 || {_Partition, {N, _MemberId}} <-
                        maps:to_list(Assignments)
                ])
            ),
            ?assertEqual(NPartitions, map_size(Assignments)),
            ok
        end
    ),
    ok.

%% test that the kafka consumer group rebalances correctly if a bridge
%% already exists when a new EMQX node joins the cluster.
t_node_joins_existing_cluster() ->
    [{?cluster, true}].
t_node_joins_existing_cluster(TCConfig) ->
    ct:timetrap({seconds, 150}),
    #{
        type := Type,
        name := Name,
        config := SourceConfig
    } = emqx_bridge_v2_testlib:get_common_values_with_configs(TCConfig),
    #{<<"parameters">> := #{<<"topic">> := KafkaTopic}} = SourceConfig,
    NPartitions = ?N_PARTITIONS,
    ClusterOpts = get_root_configs_for_cluster(TCConfig),
    Cluster = cluster(?FUNCTION_NAME, ClusterOpts, TCConfig),
    ?check_trace(
        begin
            [NodeSpec1, NodeSpec2 | _] = Cluster,
            on_exit(fun() ->
                Ns = [N || #{name := N} <- Cluster],
                ok = emqx_cth_cluster:stop(Ns)
            end),
            ct:pal("starting ~p", [NodeSpec1]),
            {[N1], {ok, _}} =
                ?wait_async_action(
                    emqx_cth_cluster:start([NodeSpec1]),
                    #{?snk_kind := kafka_consumer_subscriber_started},
                    15_000
                ),
            Fun = fun() -> ?ON(N1, emqx_mgmt_api_test_util:auth_header_()) end,
            emqx_bridge_v2_testlib:set_auth_header_getter(Fun),
            ?assertMatch({ok, _}, ?ON(N1, emqx_bridge_v2:lookup(?global_ns, sources, Type, Name))),
            {ok, _} = wait_until_group_is_balanced(KafkaTopic, NPartitions, [N1], 30_000),

            #{topic := MQTTTopic} = simple_create_rule_api(TCConfig),
            TCPPort1 = get_mqtt_port(N1),
            C1 = start_client(#{port => TCPPort1, proto_ver => v5}),
            {ok, _, [2]} = emqtt:subscribe(C1, MQTTTopic, 2),

            %% Now, we start the second node and have it join the cluster.
            {ok, SRef0} = snabbkaffe:subscribe(
                ?match_event(#{?snk_kind := kafka_consumer_subscriber_started}),
                1,
                30_000
            ),
            ?tpal("starting second node"),
            [N2] = emqx_cth_cluster:start([NodeSpec2]),
            Nodes = [N1, N2],

            ?tpal("waiting for source to be ready on second node"),
            {ok, _} = snabbkaffe:receive_events(SRef0),
            ?retry(
                _Sleep1 = 100,
                _Attempts1 = 50,
                ?assertMatch(
                    {ok, _},
                    ?ON(N2, emqx_bridge_v2:lookup(?global_ns, sources, Type, Name))
                )
            ),

            %% Give some time for the consumers in both nodes to
            %% rebalance.
            {ok, _} = wait_until_group_is_balanced(KafkaTopic, NPartitions, Nodes, 30_000),
            %% Publish some messages so we can check they came from each node.
            NumMsgs = 50 * NPartitions,
            {ok, SRef1} =
                snabbkaffe:subscribe(
                    ?match_event(#{
                        ?snk_kind := kafka_consumer_handle_message,
                        ?snk_span := {complete, _}
                    }),
                    NumMsgs,
                    20_000
                ),
            lists:foreach(
                fun(N) ->
                    Key = <<"k", (integer_to_binary(N))/binary>>,
                    Val = <<"v", (integer_to_binary(N))/binary>>,
                    publish_kafka(TCConfig, KafkaTopic, [#{key => Key, value => Val}])
                end,
                lists:seq(1, NumMsgs)
            ),
            {ok, _} = snabbkaffe:receive_events(SRef1),

            #{nodes => Nodes}
        end,
        fun(Res, Trace0) ->
            #{nodes := Nodes} = Res,
            Trace1 = ?of_kind(kafka_assignment, Trace0),
            Assignments = reconstruct_assignments_from_events(KafkaTopic, Trace1),
            NodeAssignments = lists:usort([
                N
             || {_Partition, {N, _MemberId}} <-
                    maps:to_list(Assignments)
            ]),
            ?assertEqual(lists:usort(Nodes), NodeAssignments),
            ?assertEqual(NPartitions, map_size(Assignments)),
            Published = receive_published(#{n => NPartitions, timeout => 3_000}),
            ct:pal("published:\n  ~p", [Published]),
            PublishingNodesFromTrace =
                [
                    N
                 || #{
                        ?snk_kind := kafka_consumer_handle_message,
                        ?snk_span := start,
                        ?snk_meta := #{node := N}
                    } <- Trace0
                ],
            ?assertEqual(lists:usort(Nodes), lists:usort(PublishingNodesFromTrace)),
            ok
        end
    ),
    ok.

%% Checks that the consumers get rebalanced after an EMQX nodes goes
%% down.
t_cluster_node_down() ->
    [{?cluster, true}].
t_cluster_node_down(TCConfig) ->
    ct:timetrap({seconds, 150}),
    #{
        type := Type,
        name := Name,
        config := SourceConfig
    } = emqx_bridge_v2_testlib:get_common_values_with_configs(TCConfig),
    #{<<"parameters">> := #{<<"topic">> := KafkaTopic}} = SourceConfig,
    NPartitions = ?N_PARTITIONS,
    ClusterOpts = get_root_configs_for_cluster(TCConfig),
    Cluster = cluster(?FUNCTION_NAME, ClusterOpts, TCConfig),
    ?check_trace(
        begin
            {ok, SRef0} = snabbkaffe:subscribe(
                ?match_event(#{?snk_kind := kafka_consumer_subscriber_started}),
                length(Cluster),
                15_000
            ),
            Nodes = [N1, N2 | _] = emqx_cth_cluster:start(Cluster),
            on_exit(fun() -> emqx_cth_cluster:stop(Nodes) end),
            {ok, _} = snabbkaffe:receive_events(SRef0),
            lists:foreach(
                fun(N) ->
                    ?retry(
                        _Sleep1 = 100,
                        _Attempts1 = 50,
                        ?assertMatch(
                            {ok, _},
                            ?ON(N, emqx_bridge_v2:lookup(?global_ns, sources, Type, Name)),
                            #{node => N}
                        )
                    )
                end,
                Nodes
            ),
            {ok, _} = wait_until_group_is_balanced(KafkaTopic, NPartitions, Nodes, 30_000),
            Fun = fun() -> ?ON(N1, emqx_mgmt_api_test_util:auth_header_()) end,
            emqx_bridge_v2_testlib:set_auth_header_getter(Fun),
            #{topic := MQTTTopic} = simple_create_rule_api(TCConfig),

            %% Now, we stop one of the nodes and watch the group
            %% rebalance.
            TCPPort = get_mqtt_port(N2),
            C = start_client(#{port => TCPPort, proto_ver => v5}),
            {ok, _, [2]} = emqtt:subscribe(C, MQTTTopic, 2),
            {TId, Pid} = start_async_publisher(TCConfig, KafkaTopic),

            ct:pal("stopping node ~p", [N1]),
            ok = emqx_cth_cluster:stop([N1]),

            %% Give some time for the consumers in remaining node to
            %% rebalance.
            {ok, _} = wait_until_group_is_balanced(KafkaTopic, NPartitions, [N2], 60_000),

            ok = stop_async_publisher(Pid),

            #{nodes => Nodes, payloads_tid => TId}
        end,
        fun(Res, Trace0) ->
            #{nodes := Nodes, payloads_tid := TId} = Res,
            [_N1, N2 | _] = Nodes,
            Trace1 = ?of_kind(kafka_assignment, Trace0),
            Assignments = reconstruct_assignments_from_events(KafkaTopic, Trace1),
            NodeAssignments = lists:usort([
                N
             || {_Partition, {N, _MemberId}} <-
                    maps:to_list(Assignments)
            ]),
            %% The surviving node has all the partitions assigned to
            %% it.
            ?assertEqual([N2], NodeAssignments),
            ?assertEqual(NPartitions, map_size(Assignments)),
            %% All published messages are eventually received.
            Payloads = [P || {P} <- ets:tab2list(TId)],
            Published = receive_published_payloads(#{expected => Payloads, timeout => 10_000}),
            ct:pal("published:\n  ~p", [Published]),
            ok
        end
    ),
    ok.

t_resource_manager_crash_after_subscriber_started(TCConfig) ->
    ?check_trace(
        begin
            ?force_ordering(
                #{?snk_kind := kafka_consumer_subscriber_allocated},
                #{?snk_kind := will_kill_resource_manager}
            ),
            ?force_ordering(
                #{?snk_kind := resource_manager_killed},
                #{?snk_kind := kafka_consumer_subscriber_started}
            ),
            spawn_link(fun() ->
                ?tp(will_kill_resource_manager, #{}),
                kill_resource_managers(),
                ?tp(resource_manager_killed, #{}),
                ok
            end),

            %% even if the resource manager is dead, we can still
            %% clear the allocated resources.

            %% We avoid asserting only the `config_update_crashed'
            %% error here because there's a race condition (just a
            %% problem for the test assertion below) in which the
            %% `emqx_resource_manager:create/5' call returns a failure
            %% (not checked) and then `lookup' in that module is
            %% delayed enough so that the manager supervisor has time
            %% to restart the manager process and for the latter to
            %% startup successfully.  Occurs frequently in CI...

            Overrides = #{<<"resource_opts">> => #{<<"health_check_interval">> => <<"1s">>}},
            {_, {ok, _}} =
                ?wait_async_action(
                    begin
                        create_connector_api(TCConfig, Overrides),
                        create_source_api(TCConfig, Overrides)
                    end,
                    #{?snk_kind := kafka_consumer_subcriber_and_client_stopped},
                    10_000
                ),
            ok
        end,
        []
    ),
    ok.

t_resource_manager_crash_before_subscriber_started(TCConfig) ->
    ?check_trace(
        begin
            ?force_ordering(
                #{?snk_kind := kafka_consumer_sup_started},
                #{?snk_kind := will_kill_resource_manager}
            ),
            ?force_ordering(
                #{?snk_kind := resource_manager_killed},
                #{?snk_kind := kafka_consumer_about_to_start_subscriber}
            ),
            spawn_link(fun() ->
                ?tp(will_kill_resource_manager, #{}),
                kill_resource_managers(),
                ?tp(resource_manager_killed, #{}),
                ok
            end),

            %% even if the resource manager is dead, we can still
            %% clear the allocated resources.

            %% We avoid asserting only the `config_update_crashed'
            %% error here because there's a race condition (just a
            %% problem for the test assertion below) in which the
            %% `emqx_resource_manager:create/5' call returns a failure
            %% (not checked) and then `lookup' in that module is
            %% delayed enough so that the manager supervisor has time
            %% to restart the manager process and for the latter to
            %% startup successfully.  Occurs frequently in CI...
            {_, {ok, _}} =
                ?wait_async_action(
                    begin
                        create_connector_api(TCConfig, #{}),
                        create_source_api(TCConfig, #{})
                    end,
                    #{?snk_kind := kafka_consumer_just_client_stopped},
                    10_000
                ),
            %% the new manager may have had time to startup
            %% before the resource status cache is read...
            {204, _} = delete_source_api(TCConfig),
            ?retry(
                _Sleep = 50,
                _Attempts = 50,
                ?assertEqual([], supervisor:which_children(emqx_bridge_kafka_consumer_sup))
            ),
            ok
        end,
        []
    ),
    ok.

t_bad_bootstrap_host(TCConfig) ->
    ?assertMatch(
        {400, _},
        probe_connector_api(
            TCConfig,
            #{<<"bootstrap_hosts">> => <<"bad_host:9999">>}
        )
    ),
    ok.

%% Checks that a group id is automatically generated if a custom one is not provided in
%% the config.
t_absent_group_id(TCConfig) ->
    ?check_trace(
        begin
            SourceConfig = ?config(source_config, TCConfig),
            SourceName = ?config(source_name, TCConfig),
            ?assertEqual(
                undefined,
                emqx_utils_maps:deep_get(
                    [<<"parameters">>, <<"group_id">>],
                    SourceConfig,
                    undefined
                )
            ),
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, _} = create_source_api(TCConfig, #{}),
            ?retry(
                1_000,
                10,
                ?assertMatch(
                    {200, #{<<"status">> := <<"connected">>}},
                    get_source_api(TCConfig)
                )
            ),
            GroupId = emqx_bridge_kafka_impl_consumer:consumer_group_id(#{}, SourceName),
            ct:pal("generated group id: ~p", [GroupId]),
            ?retry(1_000, 10, begin
                Groups = get_groups(),
                ?assertMatch(
                    [_],
                    [Group || Group = {_, Id, _} <- Groups, Id == GroupId],
                    #{groups => Groups}
                )
            end),
            ok
        end,
        []
    ),
    ok.

%% Checks that a group id is automatically generated if an empty string is provided in the
%% config.
t_empty_group_id(TCConfig) ->
    ?check_trace(
        begin
            SourceName = ?config(source_name, TCConfig),
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, _} = create_source_api(TCConfig, #{
                <<"parameters">> => #{<<"group_id">> => <<"">>}
            }),
            ?retry(
                1_000,
                10,
                ?assertMatch(
                    {200, #{<<"status">> := <<"connected">>}},
                    get_source_api(TCConfig)
                )
            ),
            GroupId = emqx_bridge_kafka_impl_consumer:consumer_group_id(#{}, SourceName),
            ct:pal("generated group id: ~p", [GroupId]),
            ?retry(1_000, 10, begin
                Groups = get_groups(),
                ?assertMatch(
                    [_],
                    [Group || Group = {_, Id, _} <- Groups, Id == GroupId],
                    #{groups => Groups}
                )
            end),
            ok
        end,
        []
    ),
    ok.

t_custom_group_id(TCConfig) ->
    ?check_trace(
        begin
            CustomGroupId = <<"my_group_id">>,
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, _} = create_source_api(TCConfig, #{
                <<"parameters">> => #{<<"group_id">> => CustomGroupId}
            }),
            ?retry(
                1_000,
                10,
                ?assertMatch(
                    {200, #{<<"status">> := <<"connected">>}},
                    get_source_api(TCConfig)
                )
            ),
            ?retry(1_000, 10, begin
                Groups = get_groups(),
                ?assertMatch(
                    [_],
                    [Group || Group = {_, Id, _} <- Groups, Id == CustomGroupId],
                    #{groups => Groups}
                )
            end),
            ok
        end,
        []
    ),
    ok.

%% Currently, brod treats a consumer process to a specific topic as a singleton (per
%% client id / connector), meaning that the first subscriber to a given topic will define
%% the consumer options for all other consumers, and those options persist even after the
%% original consumer group is terminated.  We enforce that, if the user wants to consume
%% multiple times from the same topic, then they must create a different connector.
t_repeated_topics(TCConfig) ->
    ?check_trace(
        begin
            %% first source is fine
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, _} = create_source_api(TCConfig, #{}),
            %% second source fails to create
            Name2 = <<"duplicated">>,
            {201, #{<<"error">> := Error}} = create_source_api(
                [{source_name, Name2} | TCConfig], #{}
            ),
            ?assertEqual(
                match,
                re:run(Error, <<"Topics .* already exist in other sources">>, [{capture, none}]),
                #{error => Error}
            ),
            ok
        end,
        []
    ),
    ok.

%% Verifies that we return an error containing information to debug connection issues when
%% one of the partition leaders is unreachable.
t_pretty_api_dry_run_reason(TCConfig) ->
    ProxyName = "kafka_2_plain",
    ?check_trace(
        begin
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, _} = create_source_api(TCConfig, #{}),
            emqx_common_test_helpers:with_failure(
                down, ProxyName, ?PROXY_HOST, ?PROXY_PORT, fun() ->
                    Res = probe_source_api(
                        TCConfig,
                        #{<<"parameters">> => #{<<"topic">> => <<"test-topic-three-partitions">>}}
                    ),
                    ?assertMatch({400, _}, Res),
                    {400, #{<<"message">> := Msg}} = Res,
                    LeaderUnavailable =
                        match ==
                            re:run(
                                Msg,
                                <<"Leader for partition . unavailable; reason: ">>,
                                [{capture, none}]
                            ),
                    %% In CI, if this tests runs soon enough, Kafka may not be stable yet, and
                    %% this failure might occur.
                    CoordinatorFailure =
                        match ==
                            re:run(
                                Msg,
                                <<"shutdown,coordinator_failure">>,
                                [{capture, none}]
                            ),
                    ?assert(LeaderUnavailable or CoordinatorFailure, #{message => Msg})
                end
            ),
            %% Wait for recovery; avoids affecting other test cases due to Kafka restabilizing...
            ?retry(
                1_000,
                10,
                ?assertMatch(
                    {200, #{<<"status">> := <<"connected">>}},
                    get_source_api(TCConfig)
                )
            ),
            ok
        end,
        []
    ),
    ok.

%% Exercises the code path where we use MSK IAM authentication.
%% Unfortunately, there seems to be no good way to accurately test this as it would
%% require running this in an EC2 instance to pass, and also to have a Kafka cluster
%% configured to use MSK IAM authentication.
t_msk_iam_authn(TCConfig) ->
    %% We mock the innermost call with the SASL authentication made by the OAuth plugin to
    %% try and exercise most of the code.
    emqx_bridge_kafka_action_SUITE:mock_iam_metadata_v2_calls(),
    ok = meck:new(emqx_bridge_kafka_msk_iam_authn, [passthrough]),
    emqx_common_test_helpers:with_mock(
        kpro_lib,
        send_and_recv,
        fun(Req, Sock, Mod, ClientId, Timeout) ->
            case Req of
                #kpro_req{api = sasl_handshake} ->
                    #{error_code => no_error};
                #kpro_req{api = sasl_authenticate} ->
                    #{error_code => no_error, session_lifetime_ms => 99999};
                _ ->
                    meck:passthrough([Req, Sock, Mod, ClientId, Timeout])
            end
        end,
        fun() ->
            ?assertMatch(
                {201, #{<<"status">> := <<"connected">>}},
                create_connector_api(
                    TCConfig,
                    #{<<"authentication">> => <<"msk_iam">>}
                )
            ),
            %% Must have called our callback
            ?assertMatch(
                [{_, {_, token_callback, _}, {ok, #{token := <<_/binary>>}}}],
                meck:history(emqx_bridge_kafka_msk_iam_authn)
            )
        end
    ),
    ok.
