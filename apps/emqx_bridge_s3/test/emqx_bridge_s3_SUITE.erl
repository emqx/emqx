%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_s3_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/test_macros.hrl").

%% See `emqx_bridge_s3.hrl`.
-define(BRIDGE_TYPE, <<"s3">>).
-define(CONNECTOR_TYPE, <<"s3">>).

-define(PROXY_NAME, "minio_tcp").
-define(CONTENT_TYPE, "application/x-emqx-payload").

%% CT Setup

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    % Setup toxiproxy
    ProxyHost = os:getenv("PROXY_HOST", "toxiproxy"),
    ProxyPort = list_to_integer(os:getenv("PROXY_PORT", "8474")),
    _ = emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_connector,
            emqx_bridge_s3,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    {ok, _} = emqx_common_test_http:create_default_app(),
    [
        {apps, Apps},
        {proxy_host, ProxyHost},
        {proxy_port, ProxyPort},
        {proxy_name, ?PROXY_NAME}
        | Config
    ].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)).

%% Testcases

init_per_testcase(TestCase, Config) ->
    ct:timetrap(timer:seconds(30)),
    ok = snabbkaffe:start_trace(),
    Name = iolist_to_binary(io_lib:format("~s~p", [TestCase, erlang:unique_integer()])),
    ConnectorConfig = connector_config(Name, Config),
    ActionConfig = action_config(Name, Name),
    [
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, Name},
        {connector_config, ConnectorConfig},
        {bridge_type, ?BRIDGE_TYPE},
        {bridge_name, Name},
        {bridge_config, ActionConfig}
        | Config
    ].

end_per_testcase(_TestCase, _Config) ->
    ok = snabbkaffe:stop(),
    ok.

connector_config(Name, _Config) ->
    BaseConf = emqx_s3_test_helpers:base_raw_config(tcp),
    emqx_bridge_s3_test_helpers:parse_and_check_config(
        <<"connectors">>, ?CONNECTOR_TYPE, Name, #{
            <<"enable">> => true,
            <<"description">> => <<"S3 Connector">>,
            <<"host">> => emqx_utils_conv:bin(maps:get(<<"host">>, BaseConf)),
            <<"port">> => maps:get(<<"port">>, BaseConf),
            <<"access_key_id">> => maps:get(<<"access_key_id">>, BaseConf),
            <<"secret_access_key">> => maps:get(<<"secret_access_key">>, BaseConf),
            <<"transport_options">> => #{
                <<"headers">> => #{
                    <<"content-type">> => <<?CONTENT_TYPE>>
                },
                <<"connect_timeout">> => <<"500ms">>,
                <<"request_timeout">> => <<"1s">>,
                <<"pool_size">> => 4,
                <<"max_retries">> => 0
            },
            <<"resource_opts">> => #{
                <<"health_check_interval">> => <<"5s">>,
                <<"start_timeout">> => <<"5s">>
            }
        }
    ).

action_config(Name, ConnectorId) ->
    emqx_bridge_s3_test_helpers:parse_and_check_config(
        <<"actions">>, ?BRIDGE_TYPE, Name, #{
            <<"enable">> => true,
            <<"connector">> => ConnectorId,
            <<"parameters">> => #{
                <<"mode">> => <<"direct">>,
                <<"bucket">> => <<"${clientid}">>,
                <<"key">> => <<"${topic}">>,
                <<"content">> => <<"${payload}">>,
                <<"acl">> => <<"public_read">>
            },
            <<"resource_opts">> => #{
                <<"buffer_mode">> => <<"memory_only">>,
                <<"buffer_seg_bytes">> => <<"10MB">>,
                <<"health_check_interval">> => <<"3s">>,
                <<"inflight_window">> => 40,
                <<"max_buffer_bytes">> => <<"256MB">>,
                <<"metrics_flush_interval">> => <<"1s">>,
                <<"query_mode">> => <<"sync">>,
                <<"request_ttl">> => <<"60s">>,
                <<"batch_size">> => 42,
                <<"batch_time">> => <<"100ms">>,
                <<"resume_interval">> => <<"3s">>,
                <<"worker_pool_size">> => <<"4">>
            }
        }
    ).

t_start_stop(Config) ->
    emqx_bridge_v2_testlib:t_start_stop(Config, s3_bridge_stopped).

t_create_unavailable_credentials(Config) ->
    ConnectorName = ?config(connector_name, Config),
    ConnectorType = ?config(connector_type, Config),
    ConnectorConfig = maps:without(
        [<<"access_key_id">>, <<"secret_access_key">>],
        ?config(connector_config, Config)
    ),
    ?assertMatch(
        {ok,
            {{_HTTP, 201, _}, _, #{
                <<"status_reason">> :=
                    <<"Unable to obtain AWS credentials:", _/bytes>>
            }}},
        emqx_bridge_v2_testlib:create_connector_api(ConnectorName, ConnectorType, ConnectorConfig)
    ).

t_ignore_batch_opts(Config) ->
    {ok, {_Status, _, Bridge}} = emqx_bridge_v2_testlib:create_bridge_api(Config),
    ?assertMatch(
        #{<<"resource_opts">> := #{<<"batch_size">> := 1, <<"batch_time">> := 0}},
        Bridge
    ).

t_start_broken_update_restart(Config) ->
    Name = ?config(connector_name, Config),
    Type = ?config(connector_type, Config),
    ConnectorConf = ?config(connector_config, Config),
    ConnectorConfBroken = maps:merge(
        ConnectorConf,
        #{<<"secret_access_key">> => <<"imnotanadmin">>}
    ),
    ?assertMatch(
        {ok, {{_HTTP, 201, _}, _, _}},
        emqx_bridge_v2_testlib:create_connector_api(Name, Type, ConnectorConfBroken)
    ),
    ConnectorId = emqx_connector_resource:resource_id(Type, Name),
    ?retry(
        _Sleep = 1_000,
        _Attempts = 20,
        ?assertEqual({ok, disconnected}, emqx_resource_manager:health_check(ConnectorId))
    ),
    ?assertMatch(
        {ok,
            {{_HTTP, 200, _}, _, #{
                <<"status_reason">> := <<"AWS error: SignatureDoesNotMatch:", _/bytes>>
            }}},
        emqx_bridge_v2_testlib:get_connector_api(Type, Name)
    ),
    ?assertMatch(
        {ok, {{_HTTP, 200, _}, _, _}},
        emqx_bridge_v2_testlib:update_connector_api(Name, Type, ConnectorConf)
    ),
    ?assertMatch(
        {ok, {{_HTTP, 204, _}, _, _}},
        emqx_bridge_v2_testlib:start_connector_api(Name, Type)
    ),
    ?retry(
        1_000,
        20,
        ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ConnectorId))
    ).

t_create_via_http(Config) ->
    emqx_bridge_v2_testlib:t_create_via_http(Config).

t_on_get_status(Config) ->
    emqx_bridge_v2_testlib:t_on_get_status(Config, #{}).

t_sync_query(Config) ->
    Bucket = emqx_s3_test_helpers:unique_bucket(),
    Topic = "a/b/c",
    Payload = rand:bytes(1024),
    AwsConfig = emqx_s3_test_helpers:aws_config(tcp),
    ok = erlcloud_s3:create_bucket(Bucket, AwsConfig),
    ok = emqx_bridge_v2_testlib:t_sync_query(
        Config,
        fun() -> emqx_bridge_s3_test_helpers:mk_message_event(Bucket, Topic, Payload) end,
        fun(Res) -> ?assertMatch(ok, Res) end,
        s3_bridge_connector_upload_ok
    ),
    ?assertMatch(
        #{
            content := Payload,
            content_type := ?CONTENT_TYPE
        },
        maps:from_list(erlcloud_s3:get_object(Bucket, Topic, AwsConfig))
    ).

t_query_retry_recoverable(Config) ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    BridgeName = ?config(bridge_name, Config),
    Bucket = emqx_s3_test_helpers:unique_bucket(),
    Topic = "d/e/f",
    Payload = rand:bytes(1024),
    AwsConfig = emqx_s3_test_helpers:aws_config(tcp),
    ok = erlcloud_s3:create_bucket(Bucket, AwsConfig),
    %% Create a bridge with the sample configuration.
    ?assertMatch(
        {ok, _Bridge},
        emqx_bridge_v2_testlib:create_bridge(Config)
    ),
    %% Simulate recoverable failure.
    _ = emqx_common_test_helpers:enable_failure(timeout, ?PROXY_NAME, ProxyHost, ProxyPort),
    _ = timer:apply_after(
        _Timeout = 5000,
        emqx_common_test_helpers,
        heal_failure,
        [timeout, ?PROXY_NAME, ProxyHost, ProxyPort]
    ),
    Message = emqx_bridge_s3_test_helpers:mk_message_event(Bucket, Topic, Payload),
    %% Verify that the message is sent eventually.
    ok = emqx_bridge_v2:send_message(?BRIDGE_TYPE, BridgeName, Message, #{}),
    ?assertMatch(
        #{content := Payload},
        maps:from_list(erlcloud_s3:get_object(Bucket, Topic, AwsConfig))
    ).
