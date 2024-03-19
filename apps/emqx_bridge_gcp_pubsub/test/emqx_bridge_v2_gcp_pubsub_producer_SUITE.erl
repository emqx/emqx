%%--------------------------------------------------------------------
%% Copyright (c) 2022-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_v2_gcp_pubsub_producer_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(CONNECTOR_TYPE_BIN, <<"gcp_pubsub_producer">>).
-define(ACTION_TYPE_BIN, <<"gcp_pubsub_producer">>).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_common_test_helpers:clear_screen(),
    emqx_bridge_gcp_pubsub_consumer_SUITE:init_per_suite(Config).

end_per_suite(Config) ->
    emqx_bridge_gcp_pubsub_consumer_SUITE:end_per_suite(Config).

init_per_testcase(TestCase, Config) ->
    common_init_per_testcase(TestCase, Config).

common_init_per_testcase(TestCase, Config0) ->
    ct:timetrap(timer:seconds(60)),
    ServiceAccountJSON =
        #{<<"project_id">> := ProjectId} =
        emqx_bridge_gcp_pubsub_utils:generate_service_account_json(),
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    Name = <<(atom_to_binary(TestCase))/binary, UniqueNum/binary>>,
    ConnectorConfig = connector_config(Name, ServiceAccountJSON),
    PubsubTopic = Name,
    ActionConfig = action_config(#{
        connector => Name,
        parameters => #{pubsub_topic => PubsubTopic}
    }),
    Config = [
        {bridge_kind, action},
        {action_type, ?ACTION_TYPE_BIN},
        {action_name, Name},
        {action_config, ActionConfig},
        {connector_name, Name},
        {connector_type, ?CONNECTOR_TYPE_BIN},
        {connector_config, ConnectorConfig},
        {service_account_json, ServiceAccountJSON},
        {project_id, ProjectId},
        {pubsub_topic, PubsubTopic}
        | Config0
    ],
    ok = emqx_bridge_gcp_pubsub_consumer_SUITE:ensure_topic(Config, PubsubTopic),
    Config.

end_per_testcase(_Testcase, Config) ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(60_000),
    ok = snabbkaffe:stop(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

connector_config(Name, ServiceAccountJSON) ->
    InnerConfigMap0 =
        #{
            <<"enable">> => true,
            <<"tags">> => [<<"bridge">>],
            <<"description">> => <<"my cool bridge">>,
            <<"connect_timeout">> => <<"5s">>,
            <<"pool_size">> => 8,
            <<"pipelining">> => <<"100">>,
            <<"max_retries">> => <<"2">>,
            <<"service_account_json">> => ServiceAccountJSON,
            <<"resource_opts">> =>
                #{
                    <<"health_check_interval">> => <<"1s">>,
                    <<"start_after_created">> => true,
                    <<"start_timeout">> => <<"5s">>
                }
        },
    emqx_bridge_v2_testlib:parse_and_check_connector(?ACTION_TYPE_BIN, Name, InnerConfigMap0).

action_config(Overrides0) ->
    Overrides = emqx_utils_maps:binary_key_map(Overrides0),
    CommonConfig =
        #{
            <<"enable">> => true,
            <<"connector">> => <<"please override">>,
            <<"parameters">> =>
                #{
                    <<"pubsub_topic">> => <<"please override">>
                },
            <<"resource_opts">> => #{
                <<"batch_size">> => 1,
                <<"batch_time">> => <<"0ms">>,
                <<"buffer_mode">> => <<"memory_only">>,
                <<"buffer_seg_bytes">> => <<"10MB">>,
                <<"health_check_interval">> => <<"15s">>,
                <<"inflight_window">> => 100,
                <<"max_buffer_bytes">> => <<"256MB">>,
                <<"metrics_flush_interval">> => <<"1s">>,
                <<"query_mode">> => <<"sync">>,
                <<"request_ttl">> => <<"45s">>,
                <<"resume_interval">> => <<"15s">>,
                <<"worker_pool_size">> => <<"1">>
            }
        },
    maps:merge(CommonConfig, Overrides).

assert_persisted_service_account_json_is_binary(ConnectorName) ->
    %% ensure cluster.hocon has a binary encoded json string as the value
    {ok, Hocon} = hocon:files([application:get_env(emqx, cluster_hocon_file, undefined)]),
    ?assertMatch(
        Bin when is_binary(Bin),
        emqx_utils_maps:deep_get(
            [
                <<"connectors">>,
                <<"gcp_pubsub_producer">>,
                ConnectorName,
                <<"service_account_json">>
            ],
            Hocon
        )
    ),
    ok.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_start_stop(Config) ->
    ok = emqx_bridge_v2_testlib:t_start_stop(Config, gcp_pubsub_stop),
    ok.

t_create_via_http(Config) ->
    ok = emqx_bridge_v2_testlib:t_create_via_http(Config),
    ok.

t_create_via_http_json_object_service_account(Config0) ->
    %% After the config goes through the roundtrip with `hocon_tconf:check_plain', service
    %% account json comes back as a binary even if the input is a json object.
    ConnectorName = ?config(connector_name, Config0),
    ConnConfig0 = ?config(connector_config, Config0),
    Config1 = proplists:delete(connector_config, Config0),
    ConnConfig1 = maps:update_with(
        <<"service_account_json">>,
        fun(X) ->
            ?assert(is_binary(X), #{json => X}),
            JSON = emqx_utils_json:decode(X, [return_maps]),
            ?assert(is_map(JSON)),
            JSON
        end,
        ConnConfig0
    ),
    Config = [{connector_config, ConnConfig1} | Config1],
    ok = emqx_bridge_v2_testlib:t_create_via_http(Config),
    assert_persisted_service_account_json_is_binary(ConnectorName),
    ok.

%% Check that creating an action (V2) with a non-existent topic leads returns an error.
t_bad_topic(Config) ->
    ?check_trace(
        begin
            %% Should it really be 201 here?
            ?assertMatch(
                {ok, {{_, 201, _}, _, #{}}},
                emqx_bridge_v2_testlib:create_bridge_api(
                    Config,
                    #{<<"parameters">> => #{<<"pubsub_topic">> => <<"i-dont-exist">>}}
                )
            ),
            #{
                kind := Kind,
                type := Type,
                name := Name
            } = emqx_bridge_v2_testlib:get_common_values(Config),
            ActionConfig0 = emqx_bridge_v2_testlib:get_value(action_config, Config),
            ProbeRes = emqx_bridge_v2_testlib:probe_bridge_api(
                Kind,
                Type,
                Name,
                emqx_utils_maps:deep_merge(
                    ActionConfig0,
                    #{<<"parameters">> => #{<<"pubsub_topic">> => <<"i-dont-exist">>}}
                )
            ),
            ?assertMatch(
                {error, {{_, 400, _}, _, _}},
                ProbeRes
            ),
            {error, {{_, 400, _}, _, #{<<"message">> := Msg}}} = ProbeRes,
            ?assertMatch(match, re:run(Msg, <<"unhealthy_target">>, [{capture, none}]), #{
                msg => Msg
            }),
            ?assertMatch(match, re:run(Msg, <<"Topic does not exist">>, [{capture, none}]), #{
                msg => Msg
            }),
            ok
        end,
        []
    ),
    ok.
