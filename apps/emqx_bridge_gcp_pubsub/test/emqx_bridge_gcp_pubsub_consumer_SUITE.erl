%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_gcp_pubsub_consumer_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-define(local, local).
-define(custom_cluster, custom_cluster).

-define(CONNECTOR_TYPE, gcp_pubsub_consumer).
-define(CONNECTOR_TYPE_BIN, <<"gcp_pubsub_consumer">>).
-define(SOURCE_TYPE, gcp_pubsub_consumer).
-define(SOURCE_TYPE_BIN, <<"gcp_pubsub_consumer">>).

-define(PROXY_NAME, "gcp_emulator").
-define(PROXY_HOST, "toxiproxy").
-define(PROXY_PORT, 8474).

-define(PREPARED_REQUEST(METHOD, PATH, BODY),
    {prepared_request, {METHOD, PATH, BODY}, #{request_ttl => 1_000}}
).
-define(PREPARED_REQUEST_PAT(METHOD, PATH, BODY),
    {prepared_request, {METHOD, PATH, BODY}, _}
).

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [
        {group, ?local},
        {group, ?custom_cluster}
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
    CustomMatrix = emqx_common_test_helpers:groups_with_matrix(?MODULE),
    LocalTCs = merge_custom_groups(?local, AllTCs, CustomMatrix),
    ClusterTCs = merge_custom_groups(?custom_cluster, [], CustomMatrix),
    [
        {?local, LocalTCs},
        {?custom_cluster, ClusterTCs}
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

init_per_suite(TCConfig) ->
    TCConfig.

end_per_suite(_TCConfig) ->
    ok.

init_per_group(?local = Group, TCConfig) ->
    reset_proxy(),
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_bridge_gcp_pubsub,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Group, TCConfig)}
    ),
    HostPort = "toxiproxy:8085",
    true = os:putenv("PUBSUB_EMULATOR_HOST", HostPort),
    Client = start_control_client("toxiproxy", 8085),
    [
        {apps, Apps},
        {proxy_host, ?PROXY_HOST},
        {proxy_port, ?PROXY_PORT},
        {proxy_name, ?PROXY_NAME},
        {client, Client}
        | TCConfig
    ];
init_per_group(?custom_cluster = Group, TCConfig) ->
    Apps = emqx_cth_suite:start(
        [
            ehttpc,
            emqx_conf,
            emqx_connector_jwt,
            emqx_bridge,
            emqx_rule_engine
        ],
        #{work_dir => emqx_cth_suite:work_dir(Group, TCConfig)}
    ),
    HostPort = "toxiproxy:8085",
    true = os:putenv("PUBSUB_EMULATOR_HOST", HostPort),
    Client = start_control_client("toxiproxy", 8085),
    [
        {apps, Apps},
        {proxy_host, ?PROXY_HOST},
        {proxy_port, ?PROXY_PORT},
        {proxy_name, ?PROXY_NAME},
        {client, Client}
        | TCConfig
    ].

end_per_group(_Group, TCConfig) ->
    reset_proxy(),
    Client = get_config(client, TCConfig),
    stop_control_client(Client),
    Apps = get_config(apps, TCConfig),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(TestCase, TCConfig0) ->
    reset_proxy(),
    Path = group_path(TCConfig0, no_groups),
    ct:pal(asciiart:visible($%, "~p - ~s", [Path, TestCase])),
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    Name = <<(atom_to_binary(TestCase))/binary, UniqueNum/binary>>,
    ConnectorName = atom_to_binary(TestCase),
    ServiceAccountJSON =
        #{<<"project_id">> := ProjectId} =
        emqx_bridge_gcp_pubsub_utils:generate_service_account_json(),
    ConnectorConfig = connector_config(#{
        <<"service_account_json">> => emqx_utils_json:encode(ServiceAccountJSON)
    }),
    SourceName = ConnectorName,
    PubSubTopic = Name,
    SourceConfig = source_config(#{
        <<"connector">> => ConnectorName,
        <<"parameters">> => #{
            <<"topic">> => PubSubTopic
        }
    }),
    TCConfig1 = [{project_id, ProjectId} | TCConfig0],
    ensure_topic(TCConfig1, PubSubTopic),
    snabbkaffe:start_trace(),
    [
        {bridge_kind, source},
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, ConnectorName},
        {connector_config, ConnectorConfig},
        {source_type, ?SOURCE_TYPE},
        {source_name, SourceName},
        {source_config, SourceConfig},
        {pubsub_topic, PubSubTopic}
        | TCConfig1
    ].

end_per_testcase(_TestCase, _TCConfig) ->
    snabbkaffe:stop(),
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Trace properties
%%------------------------------------------------------------------------------

prop_handled_only_once() ->
    {"all pulled message are processed only once", fun ?MODULE:prop_handled_only_once/1}.
prop_handled_only_once(Trace) ->
    HandledIds =
        [
            MsgId
         || #{?snk_span := start, message_id := MsgId} <-
                ?of_kind("gcp_pubsub_consumer_worker_handle_message", Trace)
        ],
    UniqueHandledIds = lists:usort(HandledIds),
    NumHandled = length(HandledIds),
    NumUniqueHandled = length(UniqueHandledIds),
    ?assertEqual(NumHandled, NumUniqueHandled, #{handled_ids => HandledIds}),
    ok.

prop_all_pulled_are_acked() ->
    {"all pulled msg ids are acked", fun ?MODULE:prop_all_pulled_are_acked/1}.
prop_all_pulled_are_acked(Trace) ->
    PulledMsgIds =
        [
            MsgId
         || #{messages := Msgs} <- ?of_kind(gcp_pubsub_consumer_worker_decoded_messages, Trace),
            #{<<"message">> := #{<<"messageId">> := MsgId}} <- Msgs
        ],
    %% we just need to check that it _tries_ to ack each id; the result itself doesn't
    %% matter, as it might timeout.
    AckedMsgIds0 = ?projection(acks, ?of_kind(gcp_pubsub_consumer_worker_will_acknowledge, Trace)),
    AckedMsgIds1 = [
        MsgId
     || PendingAcks <- AckedMsgIds0, {MsgId, _AckId} <- maps:to_list(PendingAcks)
    ],
    AckedMsgIds = sets:from_list(AckedMsgIds1, [{version, 2}]),
    ?assertEqual(
        sets:from_list(PulledMsgIds, [{version, 2}]),
        AckedMsgIds,
        #{
            decoded_msgs => ?of_kind(gcp_pubsub_consumer_worker_decoded_messages, Trace),
            acknlowledged => ?of_kind(gcp_pubsub_consumer_worker_acknowledged, Trace)
        }
    ),
    ok.

prop_client_stopped() ->
    {"client is stopped", fun ?MODULE:prop_client_stopped/1}.
prop_client_stopped(Trace) ->
    ?assert(
        ?strict_causality(
            #{?snk_kind := gcp_ehttpc_pool_started, pool_name := _P1},
            #{?snk_kind := gcp_client_stop, resource_id := _P2},
            _P1 =:= _P2,
            Trace
        )
    ),
    ok.

prop_workers_stopped(Topic) ->
    {"workers are stopped", fun(Trace) -> ?MODULE:prop_workers_stopped(Trace, Topic) end}.
prop_workers_stopped(Trace0, Topic) ->
    %% no assert because they might not start in the first place
    Trace = [Event || Event = #{topic := T} <- Trace0, T =:= Topic],
    ?strict_causality(
        #{?snk_kind := gcp_pubsub_consumer_worker_init, ?snk_meta := #{pid := _P1}},
        #{?snk_kind := gcp_pubsub_consumer_worker_terminate, ?snk_meta := #{pid := _P2}},
        _P1 =:= _P2,
        Trace
    ),
    ok.

prop_acked_ids_eventually_forgotten() ->
    {"all acked message ids are eventually forgotten",
        fun ?MODULE:prop_acked_ids_eventually_forgotten/1}.
prop_acked_ids_eventually_forgotten(Trace) ->
    AckedMsgIds0 =
        [
            MsgId
         || #{acks := PendingAcks} <- ?of_kind(gcp_pubsub_consumer_worker_acknowledged, Trace),
            {MsgId, _AckId} <- maps:to_list(PendingAcks)
        ],
    AckedMsgIds = sets:from_list(AckedMsgIds0, [{version, 2}]),
    ForgottenMsgIds = sets:union(
        ?projection(
            message_ids,
            ?of_kind(gcp_pubsub_consumer_worker_message_ids_forgotten, Trace)
        )
    ),
    EmptySet = sets:new([{version, 2}]),
    ?assertEqual(
        EmptySet,
        sets:subtract(AckedMsgIds, ForgottenMsgIds),
        #{
            forgotten => ForgottenMsgIds,
            acked => AckedMsgIds
        }
    ),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

connector_config() ->
    connector_config(_Overrides = #{}).

connector_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my connector">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"max_retries">> => 2,
        <<"pool_size">> => 8,
        <<"pipelining">> => 1,
        <<"connect_timeout">> => <<"5s">>,
        <<"max_inactive">> => <<"10s">>,
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
            <<"topic">> => <<"please override">>,
            <<"ack_deadline">> => <<"10s">>,
            <<"ack_retry_interval">> => <<"1s">>,
            <<"pull_max_messages">> => 10,
            <<"consumer_workers_per_topic">> => 1
        },
        <<"resource_opts">> =>
            emqx_utils_maps:deep_merge(
                emqx_bridge_v2_testlib:common_source_resource_opts(),
                #{
                    %% to fail and retry pulling faster
                    <<"request_ttl">> => <<"1s">>
                }
            )
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(source, ?SOURCE_TYPE_BIN, <<"x">>, InnerConfigMap).

get_config(K, TCConfig) -> emqx_bridge_v2_testlib:get_value(K, TCConfig).

group_path(Config, Default) ->
    case emqx_common_test_helpers:group_path(Config) of
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

with_failure(FailureType, Fn) ->
    emqx_common_test_helpers:with_failure(FailureType, ?PROXY_NAME, ?PROXY_HOST, ?PROXY_PORT, Fn).

enable_failure(FailureType) ->
    emqx_common_test_helpers:enable_failure(
        FailureType, ?PROXY_NAME, ?PROXY_HOST, ?PROXY_PORT
    ).

heal_failure(FailureType) ->
    emqx_common_test_helpers:heal_failure(
        FailureType, ?PROXY_NAME, ?PROXY_HOST, ?PROXY_PORT
    ).

create_connector_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(TCConfig, Overrides)
    ).

create_source_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:create_source_api(TCConfig, Overrides).

delete_connector_api(TCConfig) ->
    emqx_bridge_v2_testlib:delete_connector_api(TCConfig).

delete_source_api(TCConfig) ->
    #{type := Type, name := Name} = emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:delete_kind_api(source, Type, Name).

get_connector_api(TCConfig) ->
    #{connector_type := Type, connector_name := Name} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_connector_api(Type, Name)
    ).

get_source_api(TCConfig) ->
    #{type := Type, name := Name} = emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_source_api(Type, Name)
    ).

disable_source_api(TCConfig) ->
    #{type := Type, name := Name} = emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:disable_kind_api(source, Type, Name).

enable_source_api(TCConfig) ->
    #{type := Type, name := Name} = emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:enable_kind_api(source, Type, Name).

probe_source_api(TCConfig) ->
    probe_source_api(TCConfig, _Overrides = #{}).

probe_source_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:probe_bridge_api(TCConfig, Overrides)
    ).

ensure_topic(TCConfig, Topic) ->
    ProjectId = get_config(project_id, TCConfig),
    Client = get_config(client, TCConfig),
    Method = put,
    Path = <<"/v1/projects/", ProjectId/binary, "/topics/", Topic/binary>>,
    Body = <<"{}">>,
    Res = emqx_bridge_gcp_pubsub_client:query_sync(
        ?PREPARED_REQUEST(Method, Path, Body),
        Client
    ),
    case Res of
        {ok, _} ->
            ok;
        {error, #{status_code := 409}} ->
            %% already exists
            ok
    end,
    ok.

delete_topic(TCConfig, Topic) ->
    Client = get_config(client, TCConfig),
    ProjectId = get_config(project_id, TCConfig),
    Method = delete,
    Path = <<"/v1/projects/", ProjectId/binary, "/topics/", Topic/binary>>,
    Body = <<>>,
    {ok, _} = emqx_bridge_gcp_pubsub_client:query_sync(
        ?PREPARED_REQUEST(Method, Path, Body),
        Client
    ),
    ok.

delete_subscription(TCConfig, SubscriptionId) ->
    Client = get_config(client, TCConfig),
    ProjectId = get_config(project_id, TCConfig),
    Method = delete,
    Path = <<"/v1/projects/", ProjectId/binary, "/subscriptions/", SubscriptionId/binary>>,
    Body = <<>>,
    {ok, _} = emqx_bridge_gcp_pubsub_client:query_sync(
        ?PREPARED_REQUEST(Method, Path, Body),
        Client
    ),
    ok.

start_control_client(GCPEmulatorHost, GCPEmulatorPort) ->
    RawServiceAccount = emqx_bridge_gcp_pubsub_utils:generate_service_account_json(),
    ClientConfig =
        #{
            connect_timeout => 5_000,
            max_retries => 0,
            pool_size => 1,
            service_account_json => RawServiceAccount,
            jwt_opts => #{aud => <<"https://pubsub.googleapis.com/">>},
            transport => tcp,
            host => GCPEmulatorHost,
            port => GCPEmulatorPort
        },
    PoolName = <<"control_connector">>,
    {ok, Client} = emqx_bridge_gcp_pubsub_client:start(PoolName, ClientConfig),
    Client.

stop_control_client(Client) ->
    ok = emqx_bridge_gcp_pubsub_client:stop(Client),
    ok.

pubsub_publish(TCConfig, Topic, Messages0) ->
    Client = get_config(client, TCConfig),
    ProjectId = get_config(project_id, TCConfig),
    Method = post,
    Path = <<"/v1/projects/", ProjectId/binary, "/topics/", Topic/binary, ":publish">>,
    Messages =
        lists:map(
            fun(Msg) ->
                emqx_utils_maps:update_if_present(
                    <<"data">>,
                    fun
                        (D) when is_binary(D) -> base64:encode(D);
                        (M) when is_map(M) -> base64:encode(emqx_utils_json:encode(M))
                    end,
                    Msg
                )
            end,
            Messages0
        ),
    Body = emqx_utils_json:encode(#{<<"messages">> => Messages}),
    {ok, _} = emqx_bridge_gcp_pubsub_client:query_sync(
        ?PREPARED_REQUEST(Method, Path, Body),
        Client
    ),
    ok.

receive_published() ->
    receive_published(#{}).

receive_published(Opts0) ->
    Default = #{n => 1, timeout => 20_000},
    Opts = maps:merge(Default, Opts0),
    receive_published(Opts, []).

receive_published(#{n := N, timeout := _Timeout}, Acc) when N =< 0 ->
    {ok, lists:reverse(Acc)};
receive_published(#{n := N, timeout := Timeout} = Opts, Acc) ->
    receive
        {publish, Msg0 = #{payload := Payload}} ->
            Msg =
                case emqx_utils_json:safe_decode(Payload) of
                    {ok, Decoded} -> Msg0#{payload := Decoded};
                    {error, _} -> Msg0
                end,
            receive_published(Opts#{n := N - 1}, [Msg | Acc])
    after Timeout ->
        {timeout, #{
            msgs_so_far => Acc,
            mailbox => process_info(self(), messages),
            expected_remaining => N
        }}
    end.

wait_acked(Opts) ->
    N = maps:get(n, Opts),
    Timeout = maps:get(timeout, Opts, 30_000),
    %% no need to check return value; we check the property in
    %% the check phase.  this is just to give it a chance to do
    %% so and avoid flakiness.  should be fast.
    ct:pal("waiting ~b ms until acked...", [Timeout]),
    Res = snabbkaffe:block_until(
        ?match_n_events(N, #{?snk_kind := gcp_pubsub_consumer_worker_acknowledged}),
        Timeout
    ),
    case Res of
        {ok, _} ->
            ok;
        {timeout, Evts} ->
            %% Fixme: apparently, snabbkaffe may timeout but still return the expected
            %% events here.
            case length(Evts) >= N of
                true ->
                    ok;
                false ->
                    ct:pal("timed out waiting for acks;\n expected: ~b\n received:\n  ~p", [N, Evts])
            end
    end,
    ok.

wait_forgotten() ->
    wait_forgotten(_Opts = #{}).

wait_forgotten(Opts0) ->
    Timeout = maps:get(timeout, Opts0, 15_000),
    %% no need to check return value; we check the property in
    %% the check phase.  this is just to give it a chance to do
    %% so and avoid flakiness.
    ?block_until(
        #{?snk_kind := gcp_pubsub_consumer_worker_message_ids_forgotten},
        Timeout
    ),
    ok.

dedup([]) ->
    [];
dedup([X]) ->
    [X];
dedup([X | Rest]) ->
    [X | dedup(X, Rest)].

dedup(X, [X | Rest]) ->
    dedup(X, Rest);
dedup(_X, [Y | Rest]) ->
    [Y | dedup(Y, Rest)];
dedup(_X, []) ->
    [].

projection_optional_span(Trace) ->
    [
        case maps:get(?snk_span, Evt, undefined) of
            undefined ->
                K;
            start ->
                {K, start};
            {complete, _} ->
                {K, complete}
        end
     || #{?snk_kind := K} = Evt <- Trace
    ].

assert_non_received_metrics(TCConfig) ->
    #{type := Type, name := Name} = emqx_bridge_v2_testlib:get_common_values(TCConfig),
    Metrics = emqx_bridge:get_metrics(Type, Name),
    #{counters := Counters0, gauges := Gauges} = Metrics,
    Counters = maps:remove(received, Counters0),
    ?assert(lists:all(fun(V) -> V == 0 end, maps:values(Counters)), #{metrics => Metrics}),
    ?assert(lists:all(fun(V) -> V == 0 end, maps:values(Gauges)), #{metrics => Metrics}),
    ok.

get_pull_worker_pids(Config) ->
    ResourceId = emqx_bridge_v2_testlib:resource_id(Config),
    Pids =
        [
            PullWorkerPid
         || {_WorkerName, PoolWorkerPid} <- ecpool:workers(ResourceId),
            {ok, PullWorkerPid} <- [ecpool_worker:client(PoolWorkerPid)]
        ],
    %% assert
    [_ | _] = Pids,
    Pids.

get_async_worker_pids(TCConfig) ->
    ResourceId = emqx_bridge_v2_testlib:connector_resource_id(TCConfig),
    Pids =
        [
            AsyncWorkerPid
         || {_WorkerName, AsyncWorkerPid} <- gproc_pool:active_workers(ehttpc:name(ResourceId))
        ],
    %% assert
    [_ | _] = Pids,
    Pids.

start_client() ->
    start_client(_Opts = #{}).

start_client(Opts) ->
    {ok, C} = emqtt:start_link(Opts),
    on_exit(fun() -> emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    C.

simple_create_rule_api(TCConfig) ->
    RepublishTopic = <<"republish/topic">>,
    {201, #{<<"id">> := RuleId}} = emqx_bridge_v2_testlib:create_rule_api2(
        #{
            <<"sql">> =>
                emqx_bridge_v2_testlib:fmt(
                    <<"select * from \"${bhp}\" ">>,
                    #{bhp => emqx_bridge_v2_testlib:bridge_hookpoint(TCConfig)}
                ),
            <<"actions">> => [
                #{
                    <<"args">> => #{
                        <<"topic">> => <<"republish/topic">>,
                        <<"mqtt_properties">> => #{},
                        <<"payload">> => <<"">>,
                        <<"qos">> => 2,
                        <<"retain">> => false,
                        <<"user_properties">> => [],
                        <<"direct_dispatch">> => false
                    },
                    <<"function">> => <<"republish">>
                }
            ],
            <<"description">> => <<"">>
        }
    ),
    #{topic => RepublishTopic, id => RuleId}.

permission_denied_response() ->
    Link =
        <<"https://console.developers.google.com/project/9999/apiui/credential">>,
    {error, #{
        status_code => 403,
        headers =>
            [
                {<<"vary">>, <<"X-Origin">>},
                {<<"vary">>, <<"Referer">>},
                {<<"content-type">>, <<"application/json; charset=UTF-8">>},
                {<<"date">>, <<"Tue, 15 Aug 2023 13:59:09 GMT">>},
                {<<"server">>, <<"ESF">>},
                {<<"cache-control">>, <<"private">>},
                {<<"x-xss-protection">>, <<"0">>},
                {<<"x-frame-options">>, <<"SAMEORIGIN">>},
                {<<"x-content-type-options">>, <<"nosniff">>},
                {<<"alt-svc">>, <<"h3=\":443\"; ma=2592000,h3-29=\":443\"; ma=2592000">>},
                {<<"accept-ranges">>, <<"none">>},
                {<<"vary">>, <<"Origin,Accept-Encoding">>},
                {<<"transfer-encoding">>, <<"chunked">>}
            ],
        body => emqx_utils_json:encode(
            #{
                <<"error">> =>
                    #{
                        <<"code">> => 403,
                        <<"details">> =>
                            [
                                #{
                                    <<"@type">> => <<"type.googleapis.com/google.rpc.Help">>,
                                    <<"links">> =>
                                        [
                                            #{
                                                <<"description">> =>
                                                    <<"Google developer console API key">>,
                                                <<"url">> =>
                                                    Link
                                            }
                                        ]
                                },
                                #{
                                    <<"@type">> => <<"type.googleapis.com/google.rpc.ErrorInfo">>,
                                    <<"domain">> => <<"googleapis.com">>,
                                    <<"metadata">> =>
                                        #{
                                            <<"consumer">> => <<"projects/9999">>,
                                            <<"service">> => <<"pubsub.googleapis.com">>
                                        },
                                    <<"reason">> => <<"CONSUMER_INVALID">>
                                }
                            ],
                        <<"message">> => <<"Project #9999 has been deleted.">>,
                        <<"status">> => <<"PERMISSION_DENIED">>
                    }
            }
        )
    }}.

unauthenticated_response() ->
    Msg = <<
        "Request had invalid authentication credentials. Expected OAuth 2 access token,"
        " login cookie or other valid authentication credential. "
        "See https://developers.google.com/identity/sign-in/web/devconsole-project."
    >>,
    {error, #{
        body =>
            #{
                <<"error">> =>
                    #{
                        <<"code">> => 401,
                        <<"details">> =>
                            [
                                #{
                                    <<"@type">> =>
                                        <<"type.googleapis.com/google.rpc.ErrorInfo">>,
                                    <<"domain">> => <<"googleapis.com">>,
                                    <<"metadata">> =>
                                        #{
                                            <<"email">> =>
                                                <<"test-516@emqx-cloud-pubsub.iam.gserviceaccount.com">>,
                                            <<"method">> =>
                                                <<"google.pubsub.v1.Publisher.CreateTopic">>,
                                            <<"service">> =>
                                                <<"pubsub.googleapis.com">>
                                        },
                                    <<"reason">> => <<"ACCOUNT_STATE_INVALID">>
                                }
                            ],
                        <<"message">> => Msg,

                        <<"status">> => <<"UNAUTHENTICATED">>
                    }
            },
        headers =>
            [
                {<<"www-authenticate">>, <<"Bearer realm=\"https://accounts.google.com/\"">>},
                {<<"vary">>, <<"X-Origin">>},
                {<<"vary">>, <<"Referer">>},
                {<<"content-type">>, <<"application/json; charset=UTF-8">>},
                {<<"date">>, <<"Wed, 23 Aug 2023 12:41:40 GMT">>},
                {<<"server">>, <<"ESF">>},
                {<<"cache-control">>, <<"private">>},
                {<<"x-xss-protection">>, <<"0">>},
                {<<"x-frame-options">>, <<"SAMEORIGIN">>},
                {<<"x-content-type-options">>, <<"nosniff">>},
                {<<"alt-svc">>, <<"h3=\":443\"; ma=2592000,h3-29=\":443\"; ma=2592000">>},
                {<<"accept-ranges">>, <<"none">>},
                {<<"vary">>, <<"Origin,Accept-Encoding">>},
                {<<"transfer-encoding">>, <<"chunked">>}
            ],
        status_code => 401
    }}.

get_mqtt_port(Node) ->
    {_IP, Port} = erpc:call(Node, emqx_config, get, [[listeners, tcp, default, bind]]),
    Port.

assert_persisted_service_account_json_is_binary(TCConfig) ->
    #{connector_name := ConnectorName} = emqx_bridge_v2_testlib:get_common_values(TCConfig),
    %% ensure cluster.hocon has a binary encoded json string as the value
    {ok, Hocon} = hocon:files([application:get_env(emqx, cluster_hocon_file, undefined)]),
    ?assertMatch(
        Bin when is_binary(Bin),
        emqx_utils_maps:deep_get(
            [
                <<"connectors">>,
                <<"gcp_pubsub_consumer">>,
                ConnectorName,
                <<"service_account_json">>
            ],
            Hocon
        )
    ),
    ok.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop(TCConfig) ->
    PubSubTopic = get_config(pubsub_topic, TCConfig),
    ?check_trace(
        begin
            {ok, SRef0} =
                snabbkaffe:subscribe(
                    ?match_event(#{?snk_kind := "gcp_pubsub_consumer_worker_subscription_ready"}),
                    40_000
                ),
            ?assertMatch(
                {201, #{<<"status">> := <<"connected">>}},
                create_connector_api(TCConfig, #{})
            ),
            ?assertMatch(
                {201, #{<<"status">> := <<"connected">>}},
                create_source_api(TCConfig, #{})
            ),
            {ok, _} = snabbkaffe:receive_events(SRef0),
            ?assertMatch({204, _}, delete_source_api(TCConfig)),
            ?assertMatch({204, _}, delete_connector_api(TCConfig)),
            ok
        end,
        [
            prop_client_stopped(),
            prop_workers_stopped(PubSubTopic),
            fun(Trace) ->
                ?assertMatch([_], ?of_kind(gcp_pubsub_consumer_clear_unhealthy, Trace)),
                ok
            end
        ]
    ),
    ok.

t_on_get_status(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_on_get_status(TCConfig, #{failure_status => ?status_connecting}),
    %% no workers alive
    ?retry(
        _Interval0 = 200,
        _NAttempts0 = 20,
        ?assertMatch(
            {200, #{<<"status">> := <<"connected">>}},
            get_source_api(TCConfig)
        )
    ),
    WorkerPids = get_pull_worker_pids(TCConfig),
    emqx_utils:pmap(
        fun(Pid) ->
            Ref = monitor(process, Pid),
            exit(Pid, kill),
            receive
                {'DOWN', Ref, process, Pid, killed} ->
                    ok
            end
        end,
        WorkerPids
    ),
    ?assertMatch(
        #{status := ?status_connecting},
        emqx_bridge_v2_testlib:health_check_channel(TCConfig)
    ),
    ok.

t_consume_ok(TCConfig) ->
    PubSubTopic = get_config(pubsub_topic, TCConfig),
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        begin
            {201, _} = create_connector_api(TCConfig, #{}),
            ?assertMatch(
                {{201, _}, {ok, _}},
                ?wait_async_action(
                    create_source_api(TCConfig, #{}),
                    #{?snk_kind := "gcp_pubsub_consumer_worker_subscription_ready"},
                    40_000
                )
            ),
            #{topic := RepublishTopic} = simple_create_rule_api(TCConfig),
            C = start_client(),
            QoS = 2,
            {ok, _, [_]} = emqtt:subscribe(C, RepublishTopic, [{qos, QoS}]),

            Payload0 = emqx_guid:to_hexstr(emqx_guid:gen()),
            Messages0 = [
                #{
                    <<"data">> => Data0 = #{<<"value">> => Payload0},
                    <<"attributes">> => Attributes0 = #{<<"key">> => <<"value">>},
                    <<"orderingKey">> => <<"some_ordering_key">>
                }
            ],
            pubsub_publish(TCConfig, PubSubTopic, Messages0),
            {ok, Published0} = receive_published(),
            EncodedData0 = emqx_utils_json:encode(Data0),
            ?assertMatch(
                [
                    #{
                        qos := QoS,
                        topic := RepublishTopic,
                        payload :=
                            #{
                                <<"attributes">> := Attributes0,
                                <<"message_id">> := MsgId,
                                <<"ordering_key">> := <<"some_ordering_key">>,
                                <<"publish_time">> := PubTime,
                                <<"topic">> := PubSubTopic,
                                <<"value">> := EncodedData0
                            }
                    }
                ] when is_binary(MsgId) andalso is_binary(PubTime),
                Published0
            ),
            wait_acked(#{n => 1}),
            ?retry(
                _Interval = 200,
                _NAttempts = 20,
                ?assertMatch(
                    {200, #{
                        <<"metrics">> := #{<<"received">> := 1}
                    }},
                    emqx_bridge_v2_testlib:get_source_metrics_api(TCConfig)
                )
            ),

            %% Batch with only data and only attributes
            Payload1 = emqx_guid:to_hexstr(emqx_guid:gen()),
            Messages1 = [
                #{<<"data">> => Data1 = #{<<"val">> => Payload1}},
                #{<<"attributes">> => Attributes1 = #{<<"other_key">> => <<"other_value">>}}
            ],
            pubsub_publish(TCConfig, PubSubTopic, Messages1),
            {ok, Published1} = receive_published(#{n => 2}),
            EncodedData1 = emqx_utils_json:encode(Data1),
            ?assertMatch(
                [
                    #{
                        qos := QoS,
                        topic := RepublishTopic,
                        payload :=
                            #{
                                <<"message_id">> := _,
                                <<"publish_time">> := _,
                                <<"topic">> := Topic,
                                <<"value">> := EncodedData1
                            }
                    },
                    #{
                        qos := QoS,
                        topic := RepublishTopic,
                        payload :=
                            #{
                                <<"attributes">> := Attributes1,
                                <<"message_id">> := _,
                                <<"publish_time">> := _,
                                <<"topic">> := Topic
                            }
                    }
                ],
                Published1
            ),
            ?assertNotMatch(
                [
                    #{payload := #{<<"attributes">> := _, <<"ordering_key">> := _}},
                    #{payload := #{<<"value">> := _, <<"ordering_key">> := _}}
                ],
                Published1
            ),
            %% no need to check return value; we check the property in
            %% the check phase.  this is just to give it a chance to do
            %% so and avoid flakiness.  should be fast.
            ?block_until(
                #{?snk_kind := gcp_pubsub_consumer_worker_acknowledged, acks := Acks} when
                    map_size(Acks) =:= 2,
                5_000
            ),
            ?retry(
                _Interval = 200,
                _NAttempts = 20,
                ?assertMatch(
                    {200, #{
                        <<"metrics">> := #{<<"received">> := 3}
                    }},
                    emqx_bridge_v2_testlib:get_source_metrics_api(TCConfig)
                )
            ),

            %% Check that the bridge probe API doesn't leak atoms.
            ?assertMatch({204, _}, probe_source_api(TCConfig)),
            ?assertMatch({204, _}, probe_source_api(TCConfig)),
            AtomsBefore = erlang:system_info(atom_count),
            %% Probe again; shouldn't have created more atoms.
            ProbeRes1 = probe_source_api(TCConfig),
            ?assertMatch({204, _}, ProbeRes1),
            AtomsAfter = erlang:system_info(atom_count),
            ?assertEqual(AtomsBefore, AtomsAfter),

            assert_non_received_metrics(TCConfig),
            ?block_until(
                #{?snk_kind := gcp_pubsub_consumer_worker_message_ids_forgotten, message_ids := Ids} when
                    map_size(Ids) =:= 2,
                30_000
            ),

            ok
        end,
        [
            prop_all_pulled_are_acked(),
            prop_handled_only_once(),
            prop_acked_ids_eventually_forgotten()
        ]
    ),
    ok.

t_nonexistent_topic(TCConfig) when is_list(TCConfig) ->
    PubSubTopic = <<"nonexistent-", (emqx_guid:to_hexstr(emqx_guid:gen()))/binary>>,
    ?check_trace(
        begin
            {201, _} = create_connector_api(TCConfig, #{}),
            ?assertMatch(
                {201, #{
                    <<"status">> := <<"disconnected">>,
                    <<"status_reason">> :=
                        <<"{unhealthy_target,\"GCP PubSub topics are invalid.", _/binary>>
                }},
                create_source_api(TCConfig, #{
                    <<"parameters">> => #{<<"topic">> => PubSubTopic}
                })
            ),

            %% now create the topic and restart the bridge
            ensure_topic(TCConfig, PubSubTopic),
            ?retry(
                _Interval0 = 500,
                _NAttempts0 = 20,
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

t_topic_deleted_while_consumer_is_running(TCConfig) ->
    PubSubTopic = get_config(pubsub_topic, TCConfig),
    ?check_trace(
        begin
            {ok, SRef0} =
                snabbkaffe:subscribe(
                    ?match_event(#{?snk_kind := "gcp_pubsub_consumer_worker_subscription_ready"}),
                    40_000
                ),
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, #{<<"status">> := <<"connected">>}} = create_source_api(TCConfig, #{}),
            {ok, _} = snabbkaffe:receive_events(SRef0),

            %% curiously, gcp pubsub doesn't seem to return any errors from the
            %% subscription if the topic is deleted while the subscription still exists...
            {ok, SRef1} =
                snabbkaffe:subscribe(
                    ?match_event(#{
                        ?snk_kind := gcp_pubsub_consumer_worker_pull_async,
                        ?snk_span := start
                    }),
                    2,
                    40_000
                ),
            delete_topic(TCConfig, PubSubTopic),
            {ok, _} = snabbkaffe:receive_events(SRef1),

            ok
        end,
        []
    ),
    ok.

t_connection_down_before_starting(TCConfig) ->
    ?check_trace(
        begin
            ?force_ordering(
                #{?snk_kind := gcp_pubsub_consumer_worker_about_to_spawn},
                #{?snk_kind := will_cut_connection}
            ),
            ?force_ordering(
                #{?snk_kind := connection_down},
                #{?snk_kind := gcp_pubsub_consumer_worker_create_subscription_enter}
            ),
            spawn_link(fun() ->
                ?tp(notice, will_cut_connection, #{}),
                enable_failure(down),
                ?tp(notice, connection_down, #{})
            end),
            %% check retries
            {ok, SRef0} =
                snabbkaffe:subscribe(
                    ?match_event(#{?snk_kind := "gcp_pubsub_consumer_worker_subscription_error"}),
                    _NEvents0 = 2,
                    10_000
                ),
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, _} = create_source_api(TCConfig, #{}),
            {ok, _} = snabbkaffe:receive_events(SRef0),
            ?retry(
                500,
                20,
                ?assertMatch(
                    {200, #{<<"status">> := <<"connecting">>}},
                    get_source_api(TCConfig)
                )
            ),

            ?assertMatch(
                {200, #{<<"status">> := <<"connecting">>}},
                get_connector_api(TCConfig)
            ),

            heal_failure(down),
            ?retry(
                _Interval0 = 200,
                _NAttempts0 = 20,
                ?assertMatch(
                    {200, #{<<"status">> := <<"connected">>}},
                    get_source_api(TCConfig)
                )
            ),
            ?retry(
                _Interval0 = 200,
                _NAttempts0 = 20,
                ?assertMatch(
                    {200, #{<<"status">> := <<"connected">>}},
                    get_connector_api(TCConfig)
                )
            ),

            ok
        end,
        []
    ),
    ok.

t_connection_timeout_before_starting(TCConfig) ->
    ?check_trace(
        begin
            with_failure(timeout, fun() ->
                {201, _} = create_connector_api(TCConfig, #{}),
                ?assertMatch(
                    {201, #{<<"status">> := Status}} when
                        Status == <<"connecting">> orelse Status == <<"disconnected">>,
                    create_source_api(TCConfig, #{})
                )
            end),
            ?retry(
                _Interval0 = 200,
                _NAttempts0 = 20,
                ?assertMatch(
                    {200, #{<<"status">> := <<"connected">>}},
                    get_connector_api(TCConfig)
                )
            ),
            ok
        end,
        []
    ),
    ok.

t_pull_worker_death(TCConfig) ->
    ?check_trace(
        begin
            {201, _} = create_connector_api(TCConfig, #{}),
            ?assertMatch(
                {{201, _}, {ok, _}},
                ?wait_async_action(
                    create_source_api(TCConfig, #{}),
                    #{?snk_kind := gcp_pubsub_consumer_worker_init},
                    10_000
                )
            ),

            [PullWorkerPid | _] = get_pull_worker_pids(TCConfig),
            Ref = monitor(process, PullWorkerPid),
            sys:terminate(PullWorkerPid, die, 20_000),
            receive
                {'DOWN', Ref, process, PullWorkerPid, _} ->
                    ok
            after 500 -> ct:fail("pull worker didn't die")
            end,
            ?assertMatch(
                #{status := ?status_connecting},
                emqx_bridge_v2_testlib:health_check_channel(TCConfig)
            ),

            %% recovery
            ?retry(
                _Interval0 = 200,
                _NAttempts0 = 20,
                ?assertMatch(
                    {200, #{<<"status">> := <<"connected">>}},
                    get_connector_api(TCConfig)
                )
            ),

            ok
        end,
        []
    ),
    ok.

t_async_worker_death_mid_pull(TCConfig) ->
    ct:timetrap({seconds, 122}),
    PubSubTopic = get_config(pubsub_topic, TCConfig),
    Payload = emqx_guid:to_hexstr(emqx_guid:gen()),
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        begin
            ?force_ordering(
                #{
                    ?snk_kind := gcp_pubsub_consumer_worker_pull_async,
                    ?snk_span := {complete, _}
                },
                #{?snk_kind := kill_async_worker, ?snk_span := start}
            ),
            ?force_ordering(
                #{?snk_kind := kill_async_worker, ?snk_span := {complete, _}},
                #{?snk_kind := gcp_pubsub_consumer_worker_reply_delegator}
            ),
            spawn_link(fun() ->
                ct:pal("will kill async workers"),
                ?tp_span(
                    kill_async_worker,
                    #{},
                    begin
                        %% produce a message while worker is being killed
                        Messages = [#{<<"data">> => Payload}],
                        ct:pal("publishing message"),
                        pubsub_publish(TCConfig, PubSubTopic, Messages),
                        ct:pal("published message"),

                        AsyncWorkerPids = get_async_worker_pids(TCConfig),
                        Timeout = 20_000,
                        emqx_utils:pmap(
                            fun(AsyncWorkerPid) ->
                                Ref = monitor(process, AsyncWorkerPid),
                                ct:pal("killing pid ~p", [AsyncWorkerPid]),
                                exit(AsyncWorkerPid, kill),
                                receive
                                    {'DOWN', Ref, process, AsyncWorkerPid, _} ->
                                        ct:pal("killed pid ~p", [AsyncWorkerPid]),
                                        ok
                                after 500 -> ct:fail("async worker ~p didn't die", [AsyncWorkerPid])
                                end,
                                ok
                            end,
                            AsyncWorkerPids,
                            Timeout + 2_000
                        ),

                        ok
                    end
                ),
                ct:pal("killed async workers")
            end),

            {201, _} = create_connector_api(TCConfig, #{}),
            ?assertMatch(
                {{201, _}, {ok, _}},
                ?wait_async_action(
                    create_source_api(TCConfig, #{}),
                    #{?snk_kind := gcp_pubsub_consumer_worker_init},
                    10_000
                )
            ),
            #{topic := RepublishTopic} = simple_create_rule_api(TCConfig),
            C = start_client(),
            {ok, _, [_]} = emqtt:subscribe(C, RepublishTopic, [{qos, 2}]),

            ct:pal("waiting for consumer workers to be down"),
            {ok, _} =
                ?block_until(
                    #{?snk_kind := gcp_pubsub_consumer_worker_handled_async_worker_down},
                    30_000
                ),

            %% check that we eventually received the message.
            %% for some reason, this can take forever in ci...
            ct:pal("waiting for message to eventually be received"),
            {ok, Published} = receive_published(#{timeout => 60_000}),
            ?assertMatch([#{payload := #{<<"value">> := Payload}}], Published),

            ok
        end,
        [
            prop_handled_only_once(),
            fun(Trace) ->
                %% expected order of events; reply delegator called only once
                SubTrace = ?of_kind(
                    [
                        gcp_pubsub_consumer_worker_handled_async_worker_down,
                        gcp_pubsub_consumer_worker_pull_response_received,
                        gcp_pubsub_consumer_worker_reply_delegator
                    ],
                    Trace
                ),
                SubTraceEvts = ?projection(?snk_kind, SubTrace),
                ?assertMatch(
                    [
                        gcp_pubsub_consumer_worker_handled_async_worker_down,
                        gcp_pubsub_consumer_worker_reply_delegator
                        | _
                    ],
                    dedup(SubTraceEvts),
                    #{sub_trace => projection_optional_span(SubTrace)}
                ),
                ?assertMatch(
                    gcp_pubsub_consumer_worker_pull_response_received,
                    lists:last(SubTraceEvts)
                ),
                ok
            end
        ]
    ),
    ok.

t_connection_error_while_creating_subscription(TCConfig) ->
    ?check_trace(
        begin
            ?force_ordering(
                #{?snk_kind := gcp_pubsub_consumer_worker_init},
                #{?snk_kind := will_cut_connection}
            ),
            ?force_ordering(
                #{?snk_kind := connection_down},
                #{?snk_kind := gcp_pubsub_consumer_worker_create_subscription_enter}
            ),
            spawn_link(fun() ->
                ?tp(notice, will_cut_connection, #{}),
                emqx_common_test_helpers:enable_failure(
                    down, ?PROXY_NAME, ?PROXY_HOST, ?PROXY_PORT
                ),
                ?tp(notice, connection_down, #{})
            end),
            %% check retries
            {ok, SRef0} =
                snabbkaffe:subscribe(
                    ?match_event(#{?snk_kind := "gcp_pubsub_consumer_worker_subscription_error"}),
                    _NEvents0 = 2,
                    10_000
                ),
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, _} = create_source_api(TCConfig, #{}),
            {ok, _} = snabbkaffe:receive_events(SRef0),
            emqx_common_test_helpers:heal_failure(
                down, ?PROXY_NAME, ?PROXY_HOST, ?PROXY_PORT
            ),

            %% should eventually succeed
            ?tp(notice, "waiting for recovery", #{}),
            {ok, _} =
                ?block_until(
                    #{?snk_kind := "gcp_pubsub_consumer_worker_subscription_created"},
                    10_000
                ),
            ok
        end,
        []
    ),
    ok.

t_subscription_already_exists(TCConfig) ->
    ?check_trace(
        begin
            {201, _} = create_connector_api(TCConfig, #{}),
            {{201, _}, {ok, _}} =
                ?wait_async_action(
                    create_source_api(TCConfig, #{}),
                    #{?snk_kind := "gcp_pubsub_consumer_worker_subscription_created"},
                    10_000
                ),
            %% now restart the same bridge
            {204, _} = disable_source_api(TCConfig),

            {{204, _}, {ok, _}} =
                ?wait_async_action(
                    enable_source_api(TCConfig),
                    #{?snk_kind := "gcp_pubsub_consumer_worker_subscription_ready"},
                    10_000
                ),

            ok
        end,
        fun(Trace) ->
            ?assertMatch(
                [
                    "gcp_pubsub_consumer_worker_subscription_already_exists",
                    "gcp_pubsub_consumer_worker_subscription_patched"
                ],
                ?projection(
                    ?snk_kind,
                    ?of_kind(
                        [
                            "gcp_pubsub_consumer_worker_subscription_already_exists",
                            "gcp_pubsub_consumer_worker_subscription_patched"
                        ],
                        Trace
                    )
                )
            ),
            ok
        end
    ),
    ok.

t_subscription_patch_error(TCConfig) ->
    ?check_trace(
        begin
            {201, _} = create_connector_api(TCConfig, #{}),
            {{201, _}, {ok, _}} =
                ?wait_async_action(
                    create_source_api(TCConfig, #{}),
                    #{?snk_kind := "gcp_pubsub_consumer_worker_subscription_created"},
                    10_000
                ),
            %% now restart the same bridge
            {204, _} = disable_source_api(TCConfig),

            ?force_ordering(
                #{?snk_kind := "gcp_pubsub_consumer_worker_subscription_already_exists"},
                #{?snk_kind := cut_connection, ?snk_span := start}
            ),
            ?force_ordering(
                #{?snk_kind := cut_connection, ?snk_span := {complete, _}},
                #{?snk_kind := gcp_pubsub_consumer_worker_patch_subscription_enter}
            ),
            spawn_link(fun() ->
                ?tp_span(
                    cut_connection,
                    #{},
                    enable_failure(down)
                )
            end),

            {{204, _}, {ok, _}} =
                ?wait_async_action(
                    enable_source_api(TCConfig),
                    #{?snk_kind := "gcp_pubsub_consumer_worker_subscription_patch_error"},
                    10_000
                ),

            {{ok, _}, {ok, _}} =
                ?wait_async_action(
                    heal_failure(down),
                    #{?snk_kind := "gcp_pubsub_consumer_worker_subscription_ready"},
                    10_000
                ),

            ok
        end,
        []
    ),
    ok.

t_topic_deleted_while_creating_subscription(TCConfig) ->
    PubSubTopic = get_config(pubsub_topic, TCConfig),
    ?check_trace(
        begin
            ?force_ordering(
                #{?snk_kind := gcp_pubsub_consumer_worker_init},
                #{?snk_kind := delete_topic, ?snk_span := start}
            ),
            ?force_ordering(
                #{?snk_kind := delete_topic, ?snk_span := {complete, _}},
                #{?snk_kind := gcp_pubsub_consumer_worker_create_subscription_enter}
            ),
            spawn_link(fun() ->
                ?tp_span(
                    delete_topic,
                    #{},
                    delete_topic(TCConfig, PubSubTopic)
                )
            end),
            {201, _} = create_connector_api(TCConfig, #{}),
            {{201, #{<<"status">> := <<"disconnected">>}}, {ok, _}} =
                ?wait_async_action(
                    create_source_api(TCConfig, #{}),
                    #{?snk_kind := gcp_pubsub_consumer_worker_terminate},
                    10_000
                ),
            ok
        end,
        []
    ),
    ok.

t_topic_deleted_while_patching_subscription(TCConfig) ->
    PubSubTopic = get_config(pubsub_topic, TCConfig),
    ?check_trace(
        begin
            {201, _} = create_connector_api(TCConfig, #{}),
            {{201, _}, {ok, _}} =
                ?wait_async_action(
                    create_source_api(TCConfig, #{}),
                    #{?snk_kind := "gcp_pubsub_consumer_worker_subscription_created"},
                    10_000
                ),
            %% now restart the same bridge
            {204, _} = disable_source_api(TCConfig),

            ?force_ordering(
                #{?snk_kind := "gcp_pubsub_consumer_worker_subscription_already_exists"},
                #{?snk_kind := delete_topic, ?snk_span := start}
            ),
            ?force_ordering(
                #{?snk_kind := delete_topic, ?snk_span := {complete, _}},
                #{?snk_kind := gcp_pubsub_consumer_worker_patch_subscription_enter}
            ),
            spawn_link(fun() ->
                ?tp_span(
                    delete_topic,
                    #{},
                    delete_topic(TCConfig, PubSubTopic)
                )
            end),
            %% as with deleting the topic of an existing subscription, patching after the
            %% topic does not exist anymore doesn't return errors either...
            {{204, _}, {ok, _}} =
                ?wait_async_action(
                    enable_source_api(TCConfig),
                    #{?snk_kind := "gcp_pubsub_consumer_worker_subscription_ready"},
                    10_000
                ),
            ?retry(
                500,
                20,
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

t_subscription_deleted_while_consumer_is_running(TCConfig) ->
    ?check_trace(
        begin
            {201, _} = create_connector_api(TCConfig, #{}),
            {{201, _}, {ok, #{subscription_id := SubscriptionId}}} =
                ?wait_async_action(
                    create_source_api(TCConfig, #{}),
                    #{
                        ?snk_kind := gcp_pubsub_consumer_worker_pull_async,
                        ?snk_span := {complete, _}
                    },
                    10_000
                ),
            {ok, SRef0} =
                snabbkaffe:subscribe(
                    ?match_event(
                        #{?snk_kind := "gcp_pubsub_consumer_worker_pull_error"}
                    ),
                    30_000
                ),
            {ok, SRef1} =
                snabbkaffe:subscribe(
                    ?match_event(
                        #{?snk_kind := "gcp_pubsub_consumer_worker_subscription_ready"}
                    ),
                    30_000
                ),
            delete_subscription(TCConfig, SubscriptionId),
            {ok, _} = snabbkaffe:receive_events(SRef0),
            {ok, _} = snabbkaffe:receive_events(SRef1),

            ?assertMatch(
                {200, #{<<"status">> := <<"connected">>}},
                get_source_api(TCConfig)
            ),
            ok
        end,
        []
    ),
    ok.

t_subscription_and_topic_deleted_while_consumer_is_running(TCConfig) ->
    ct:timetrap({seconds, 90}),
    PubSubTopic = get_config(pubsub_topic, TCConfig),
    ?check_trace(
        begin
            {201, _} = create_connector_api(TCConfig, #{}),
            {{201, _}, {ok, #{subscription_id := SubscriptionId}}} =
                ?wait_async_action(
                    create_source_api(TCConfig, #{}),
                    #{
                        ?snk_kind := gcp_pubsub_consumer_worker_pull_async,
                        ?snk_span := {complete, _}
                    },
                    10_000
                ),
            delete_topic(TCConfig, PubSubTopic),
            delete_subscription(TCConfig, SubscriptionId),
            {ok, _} = ?block_until(#{?snk_kind := gcp_pubsub_consumer_worker_terminate}, 60_000),

            ?retry(
                _Sleep0 = 100,
                _Retries = 20,
                ?assertMatch(
                    {200, #{<<"status">> := <<"disconnected">>}},
                    get_source_api(TCConfig)
                )
            ),
            ok
        end,
        []
    ),
    ok.

t_connection_down_during_ack(TCConfig) ->
    PubSubTopic = get_config(pubsub_topic, TCConfig),
    ?check_trace(
        begin
            {201, _} = create_connector_api(TCConfig, #{}),
            {{201, _}, {ok, _}} =
                ?wait_async_action(
                    create_source_api(TCConfig, #{}),
                    #{?snk_kind := "gcp_pubsub_consumer_worker_subscription_ready"},
                    10_000
                ),
            #{topic := RepublishTopic} = simple_create_rule_api(TCConfig),
            C = start_client(),
            {ok, _, [_]} = emqtt:subscribe(C, RepublishTopic, [{qos, 2}]),

            ?force_ordering(
                #{
                    ?snk_kind := "gcp_pubsub_consumer_worker_handle_message",
                    ?snk_span := {complete, _}
                },
                #{?snk_kind := cut_connection, ?snk_span := start}
            ),
            ?force_ordering(
                #{?snk_kind := cut_connection, ?snk_span := {complete, _}},
                #{?snk_kind := gcp_pubsub_consumer_worker_acknowledge_enter}
            ),
            spawn_link(fun() ->
                ?tp_span(
                    cut_connection,
                    #{},
                    enable_failure(down)
                )
            end),

            Payload = emqx_guid:to_hexstr(emqx_guid:gen()),
            Messages = [#{<<"data">> => Payload}],
            pubsub_publish(TCConfig, PubSubTopic, Messages),
            {ok, _} = ?block_until(#{?snk_kind := "gcp_pubsub_consumer_worker_ack_error"}, 10_000),

            {{ok, _}, {ok, _}} =
                ?wait_async_action(
                    heal_failure(down),
                    #{?snk_kind := gcp_pubsub_consumer_worker_acknowledged},
                    30_000
                ),

            {ok, _Published} = receive_published(),

            ok
        end,
        [
            prop_all_pulled_are_acked(),
            prop_handled_only_once(),
            {"message is processed only once", fun(Trace) ->
                ?assertMatch({timeout, _}, receive_published(#{timeout => 5_000})),
                ?assertMatch(
                    [#{?snk_span := start}, #{?snk_span := {complete, _}}],
                    ?of_kind("gcp_pubsub_consumer_worker_handle_message", Trace)
                ),
                ok
            end}
        ]
    ),
    ok.

t_connection_down_during_ack_redeliver(TCConfig) ->
    ct:timetrap({seconds, 120}),
    PubSubTopic = get_config(pubsub_topic, TCConfig),
    ?check_trace(
        begin
            {201, _} = create_connector_api(TCConfig, #{}),
            {{201, _}, {ok, _}} =
                ?wait_async_action(
                    create_source_api(
                        TCConfig,
                        #{
                            <<"parameters">> => #{
                                <<"ack_deadline">> => <<"12s">>,
                                <<"ack_retry_interval">> => <<"1s">>
                            },
                            <<"resource_opts">> => #{
                                <<"request_ttl">> => <<"11s">>
                            }
                        }
                    ),
                    #{?snk_kind := "gcp_pubsub_consumer_worker_subscription_ready"},
                    10_000
                ),
            #{topic := RepublishTopic} = simple_create_rule_api(TCConfig),
            C = start_client(),
            {ok, _, [_]} = emqtt:subscribe(C, RepublishTopic, [{qos, 2}]),

            emqx_common_test_helpers:with_mock(
                emqx_bridge_gcp_pubsub_client,
                query_sync,
                fun(PreparedRequest = ?PREPARED_REQUEST_PAT(_Method, Path, _Body), Client) ->
                    case re:run(Path, <<":acknowledge$">>) of
                        {match, _} ->
                            ct:sleep(800),
                            {error, timeout};
                        nomatch ->
                            meck:passthrough([PreparedRequest, Client])
                    end
                end,
                fun() ->
                    Payload = emqx_guid:to_hexstr(emqx_guid:gen()),
                    Messages = [#{<<"data">> => Payload}],
                    pubsub_publish(TCConfig, PubSubTopic, Messages),
                    {ok, _} = snabbkaffe:block_until(
                        ?match_n_events(2, #{?snk_kind := "gcp_pubsub_consumer_worker_ack_error"}),
                        20_000
                    ),
                    %% The minimum deadline pubsub does is 10 s.
                    {ok, _} = ?block_until(#{?snk_kind := message_redelivered}, 30_000),
                    ok
                end
            ),

            {ok, Published} = receive_published(),
            ct:pal("received: ~p", [Published]),

            wait_forgotten(#{timeout => 60_000}),

            %% should be processed only once
            Res = receive_published(#{timeout => 5_000}),

            Res
        end,
        [
            prop_acked_ids_eventually_forgotten(),
            prop_all_pulled_are_acked(),
            prop_handled_only_once(),
            {"message is processed only once", fun(Res, Trace) ->
                ?assertMatch({timeout, _}, Res),
                ?assertMatch(
                    [#{?snk_span := start}, #{?snk_span := {complete, _}}],
                    ?of_kind("gcp_pubsub_consumer_worker_handle_message", Trace)
                ),
                ok
            end}
        ]
    ),
    ok.

t_connection_down_during_pull(TCConfig) ->
    ct:timetrap({seconds, 90}),
    PubSubTopic = get_config(pubsub_topic, TCConfig),
    FailureType = timeout,
    ?check_trace(
        begin
            {201, _} = create_connector_api(TCConfig, #{}),
            {{201, _}, {ok, _}} =
                ?wait_async_action(
                    create_source_api(
                        TCConfig,
                        #{
                            <<"parameters">> => #{<<"ack_retry_interval">> => <<"1s">>},
                            <<"resource_opts">> => #{<<"request_ttl">> => <<"11s">>}
                        }
                    ),
                    #{?snk_kind := "gcp_pubsub_consumer_worker_subscription_ready"},
                    10_000
                ),
            #{topic := RepublishTopic} = simple_create_rule_api(TCConfig),
            C = start_client(),
            {ok, _, [_]} = emqtt:subscribe(C, RepublishTopic, [{qos, 2}]),

            ?force_ordering(
                #{
                    ?snk_kind := gcp_pubsub_consumer_worker_pull_async,
                    ?snk_span := start
                },
                #{?snk_kind := cut_connection, ?snk_span := start}
            ),
            ?force_ordering(
                #{?snk_kind := cut_connection, ?snk_span := {complete, _}},
                #{
                    ?snk_kind := gcp_pubsub_consumer_worker_pull_async,
                    ?snk_span := {complete, _}
                }
            ),
            spawn_link(fun() ->
                ?tp_span(
                    cut_connection,
                    #{},
                    begin
                        Payload = emqx_guid:to_hexstr(emqx_guid:gen()),
                        Messages = [#{<<"data">> => Payload}],
                        pubsub_publish(TCConfig, PubSubTopic, Messages),
                        enable_failure(FailureType),
                        ok
                    end
                )
            end),

            ?block_until("gcp_pubsub_consumer_worker_pull_error", 10_000),
            heal_failure(FailureType),

            {ok, _Published} = receive_published(),

            Res = receive_published(#{timeout => 5_000}),

            wait_forgotten(#{timeout => 60_000}),

            Res
        end,
        [
            prop_acked_ids_eventually_forgotten(),
            prop_all_pulled_are_acked(),
            prop_handled_only_once(),
            {"message is processed only once", fun(Res, Trace) ->
                ?assertMatch({timeout, _}, Res),
                ?assertMatch(
                    [#{?snk_span := start}, #{?snk_span := {complete, _}}],
                    ?of_kind("gcp_pubsub_consumer_worker_handle_message", Trace)
                ),
                ok
            end}
        ]
    ),
    ok.

%% debugging api
t_get_subscription(TCConfig) ->
    ?check_trace(
        begin
            {201, _} = create_connector_api(TCConfig, #{}),
            ?assertMatch(
                {{201, _}, {ok, _}},
                ?wait_async_action(
                    create_source_api(TCConfig, #{}),
                    #{?snk_kind := "gcp_pubsub_consumer_worker_subscription_ready"},
                    10_000
                )
            ),

            [PullWorkerPid | _] = get_pull_worker_pids(TCConfig),
            ?retry(
                _Interval0 = 200,
                _NAttempts0 = 20,
                ?assertMatch(
                    {ok, #{}},
                    emqx_bridge_gcp_pubsub_consumer_worker:get_subscription(PullWorkerPid)
                )
            ),

            ok
        end,
        []
    ),
    ok.

t_permission_denied_topic_check(TCConfig) ->
    PubSubTopic = get_config(pubsub_topic, TCConfig),
    ?check_trace(
        begin
            %% the emulator does not check any credentials
            emqx_common_test_helpers:with_mock(
                emqx_bridge_gcp_pubsub_client,
                query_sync,
                fun(PreparedRequest = ?PREPARED_REQUEST_PAT(Method, Path, _Body), Client) ->
                    RE = iolist_to_binary(["/topics/", PubSubTopic, "$"]),
                    case {Method =:= get, re:run(Path, RE)} of
                        {true, {match, _}} ->
                            permission_denied_response();
                        _ ->
                            meck:passthrough([PreparedRequest, Client])
                    end
                end,
                fun() ->
                    {201, _} = create_connector_api(TCConfig, #{}),
                    ?assertMatch(
                        {201, #{
                            <<"status">> := <<"disconnected">>,
                            <<"status_reason">> :=
                                <<"{unhealthy_target,\"Permission denied", _/binary>>
                        }},
                        create_source_api(TCConfig, #{})
                    ),
                    ok
                end
            ),
            ok
        end,
        []
    ),
    ok.

t_permission_denied_worker(TCConfig) ->
    ?check_trace(
        begin
            emqx_common_test_helpers:with_mock(
                emqx_bridge_gcp_pubsub_client,
                query_sync,
                fun(PreparedRequest = ?PREPARED_REQUEST_PAT(Method, _Path, _Body), Client) ->
                    case Method =:= put of
                        true ->
                            permission_denied_response();
                        false ->
                            meck:passthrough([PreparedRequest, Client])
                    end
                end,
                fun() ->
                    {201, _} = create_connector_api(TCConfig, #{}),
                    {{201, _}, {ok, _}} =
                        ?wait_async_action(
                            create_source_api(TCConfig, #{}),
                            #{?snk_kind := gcp_pubsub_consumer_worker_terminate},
                            10_000
                        ),

                    ok
                end
            ),
            ok
        end,
        []
    ),
    ok.

t_unauthenticated_topic_check(TCConfig) ->
    PubSubTopic = get_config(pubsub_topic, TCConfig),
    ?check_trace(
        begin
            %% the emulator does not check any credentials
            emqx_common_test_helpers:with_mock(
                emqx_bridge_gcp_pubsub_client,
                query_sync,
                fun(PreparedRequest = ?PREPARED_REQUEST_PAT(Method, Path, _Body), Client) ->
                    RE = iolist_to_binary(["/topics/", PubSubTopic, "$"]),
                    case {Method =:= get, re:run(Path, RE)} of
                        {true, {match, _}} ->
                            unauthenticated_response();
                        _ ->
                            meck:passthrough([PreparedRequest, Client])
                    end
                end,
                fun() ->
                    {201, _} = create_connector_api(TCConfig, #{}),
                    ?assertMatch(
                        {201, #{
                            <<"status">> := <<"disconnected">>,
                            <<"status_reason">> :=
                                <<"{unhealthy_target,\"Permission denied", _/binary>>
                        }},
                        create_source_api(TCConfig, #{})
                    ),
                    ok
                end
            ),
            ok
        end,
        []
    ),
    ok.

t_unauthenticated_worker(TCConfig) ->
    ?check_trace(
        begin
            emqx_common_test_helpers:with_mock(
                emqx_bridge_gcp_pubsub_client,
                query_sync,
                fun(PreparedRequest = ?PREPARED_REQUEST_PAT(Method, _Path, _Body), Client) ->
                    case Method =:= put of
                        true ->
                            unauthenticated_response();
                        false ->
                            meck:passthrough([PreparedRequest, Client])
                    end
                end,
                fun() ->
                    {201, _} = create_connector_api(TCConfig, #{}),
                    {{201, _}, {ok, _}} =
                        ?wait_async_action(
                            create_source_api(TCConfig, #{}),
                            #{?snk_kind := gcp_pubsub_consumer_worker_terminate},
                            10_000
                        ),
                    ok
                end
            ),
            ok
        end,
        []
    ),
    ok.

t_cluster_subscription() ->
    [{matrix, true}].
t_cluster_subscription(matrix) ->
    [[?custom_cluster]];
t_cluster_subscription(TCConfig) when is_list(TCConfig) ->
    PubSubTopic = get_config(pubsub_topic, TCConfig),
    AppSpecs = [
        emqx_conf,
        emqx_bridge_gcp_pubsub,
        emqx_bridge,
        emqx_rule_engine,
        emqx_management
    ],
    ?check_trace(
        begin
            Nodes =
                [N1, _N2] = emqx_cth_cluster:start(
                    [
                        {gcp_pubsub_consumer_subscription1, #{apps => AppSpecs}},
                        {gcp_pubsub_consumer_subscription2, #{
                            apps => AppSpecs ++
                                [emqx_mgmt_api_test_util:emqx_dashboard()]
                        }}
                    ],
                    #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, TCConfig)}
                ),
            on_exit(fun() -> emqx_cth_cluster:stop(Nodes) end),
            Fun = fun() -> ?ON(N1, emqx_mgmt_api_test_util:auth_header_()) end,
            emqx_bridge_v2_testlib:set_auth_header_getter(Fun),
            NumNodes = length(Nodes),
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, _} = create_source_api(TCConfig, #{}),
            #{topic := RepublishTopic} = simple_create_rule_api(TCConfig),
            {ok, _} = snabbkaffe:block_until(
                ?match_n_events(
                    NumNodes,
                    #{?snk_kind := "gcp_pubsub_consumer_worker_subscription_ready"}
                ),
                10_000
            ),

            TCPPort1 = get_mqtt_port(N1),
            {ok, C1} = emqtt:start_link([{port, TCPPort1}, {proto_ver, v5}]),
            on_exit(fun() -> catch emqtt:stop(C1) end),
            {ok, _} = emqtt:connect(C1),
            {ok, _, [2]} = emqtt:subscribe(C1, RepublishTopic, 2),

            Payload = emqx_guid:to_hexstr(emqx_guid:gen()),
            Messages = [#{<<"data">> => Payload}],
            pubsub_publish(TCConfig, PubSubTopic, Messages),

            ?assertMatch({ok, _Published}, receive_published()),

            ok
        end,
        [prop_handled_only_once()]
    ),
    ok.

t_consume(TCConfig) ->
    PubSubTopic = get_config(pubsub_topic, TCConfig),
    Payload = #{<<"key">> => <<"value">>},
    Attributes = #{<<"hkey">> => <<"hval">>},
    ProduceFn = fun() ->
        pubsub_publish(
            TCConfig,
            PubSubTopic,
            [
                #{
                    <<"data">> => Payload,
                    <<"orderingKey">> => <<"ok">>,
                    <<"attributes">> => Attributes
                }
            ]
        )
    end,
    Encoded = emqx_utils_json:encode(Payload),
    CheckFn = fun(Message) ->
        ?assertMatch(
            #{
                attributes := Attributes,
                message_id := _,
                ordering_key := <<"ok">>,
                publish_time := _,
                topic := PubSubTopic,
                value := Encoded
            },
            Message
        )
    end,
    ok = emqx_bridge_v2_testlib:t_consume(
        TCConfig,
        #{
            consumer_ready_tracepoint => ?match_event(
                #{?snk_kind := "gcp_pubsub_consumer_worker_subscription_ready"}
            ),
            produce_fn => ProduceFn,
            check_fn => CheckFn,
            produce_tracepoint => ?match_event(
                #{
                    ?snk_kind := "gcp_pubsub_consumer_worker_handle_message",
                    ?snk_span := {complete, _}
                }
            )
        }
    ),
    ok.

t_create_via_http_json_object_service_account(TCConfig) ->
    %% After the config goes through the roundtrip with `hocon_tconf:check_plain', service
    %% account json comes back as a binary even if the input is a json object.
    ServiceAccountJSON = emqx_bridge_gcp_pubsub_utils:generate_service_account_json(),
    true = is_map(ServiceAccountJSON),
    {201, _} = create_connector_api(TCConfig, #{
        <<"service_account_json">> => ServiceAccountJSON
    }),
    assert_persisted_service_account_json_is_binary(TCConfig),
    ok.
