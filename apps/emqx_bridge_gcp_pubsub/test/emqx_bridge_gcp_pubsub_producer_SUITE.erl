%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_gcp_pubsub_producer_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("jose/include/jose_jwt.hrl").
-include_lib("jose/include/jose_jws.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-define(ACTION_TYPE, gcp_pubsub_producer).
-define(ACTION_TYPE_BIN, <<"gcp_pubsub_producer">>).
-define(CONNECTOR_TYPE, gcp_pubsub_producer).
-define(CONNECTOR_TYPE_BIN, <<"gcp_pubsub_producer">>).

-define(PROXY_HOST, "toxiproxy").
-define(PROXY_PORT, 8474).
-define(PROXY_NAME, "gcp_emulator").

-define(mocked_gcp, mocked_gcp).
-define(async, async).
-define(sync, sync).
-define(with_batch, with_batch).
-define(without_batch, without_batch).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all_with_matrix(?MODULE).

groups() ->
    emqx_common_test_helpers:groups_with_matrix(?MODULE).

init_per_suite(TCConfig) ->
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
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    set_emulator_host_env(),
    [
        {apps, Apps}
        | TCConfig
    ].

end_per_suite(Config) ->
    reset_proxy(),
    Apps = get_config(apps, Config),
    emqx_cth_suite:stop(Apps),
    persistent_term:erase({emqx_bridge_gcp_pubsub_client, pubsub, transport}),
    os:unsetenv("PUBSUB_EMULATOR_HOST"),
    ok.

init_per_group(?sync, TCConfig) ->
    [{query_mode, sync} | TCConfig];
init_per_group(?async, TCConfig) ->
    [{query_mode, async} | TCConfig];
init_per_group(?with_batch, TCConfig) ->
    [{batch_size, 100}, {batch_time, <<"200ms">>} | TCConfig];
init_per_group(?without_batch, TCConfig) ->
    [{batch_size, 1} | TCConfig];
init_per_group(?mocked_gcp, TCConfig) ->
    TCConfig;
init_per_group(_Group, TCConfig) ->
    TCConfig.

end_per_group(_Group, _TCConfig) ->
    ok.

init_per_testcase(TestCase, TCConfig) ->
    reset_proxy(),
    Path = group_path(TCConfig, no_groups),
    ct:pal(asciiart:visible($%, "~p - ~s", [Path, TestCase])),
    GUID = emqx_guid:to_hexstr(emqx_guid:gen()),
    PubSubTopic = <<(atom_to_binary(?MODULE))/binary, GUID/binary>>,
    ConnectorName = atom_to_binary(TestCase),
    ServiceAccountJSON = emqx_bridge_gcp_pubsub_utils:generate_service_account_json(),
    ConnectorConfig = connector_config(#{
        <<"service_account_json">> => emqx_utils_json:encode(ServiceAccountJSON)
    }),
    ActionName = ConnectorName,
    ActionConfig = action_config(#{
        <<"connector">> => ConnectorName,
        <<"parameters">> => #{<<"pubsub_topic">> => PubSubTopic},
        <<"resource_opts">> => #{
            <<"query_mode">> => get_config(query_mode, TCConfig, <<"sync">>),
            <<"batch_size">> => get_config(batch_size, TCConfig, 1),
            <<"batch_time">> => get_config(batch_time, TCConfig, <<"0ms">>)
        }
    }),
    Tid = install_telemetry_handler(TestCase),
    put(telemetry_table, Tid),
    HTTPServer = start_echo_http_server(TCConfig),
    snabbkaffe:start_trace(),
    persistent_term:put({?MODULE, test_pid}, self()),
    [
        {bridge_kind, action},
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, ConnectorName},
        {connector_config, ConnectorConfig},
        {action_type, ?ACTION_TYPE},
        {action_name, ActionName},
        {action_config, ActionConfig},
        {telemetry_table, Tid},
        {http_server, HTTPServer},
        {service_account_json, ServiceAccountJSON}
        | TCConfig
    ].

end_per_testcase(_TestCase, _TCConfig) ->
    snabbkaffe:stop(),
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(),
    stop_echo_http_server(),
    persistent_term:erase({?MODULE, test_pid}),
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
        <<"connect_timeout">> => <<"5s">>,
        <<"pool_size">> => 8,
        <<"pipelining">> => 100,
        <<"max_inactive">> => <<"10s">>,
        <<"max_retries">> => 2,
        <<"service_account_json">> => <<"please override">>,
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_connector_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check_connector(?CONNECTOR_TYPE_BIN, <<"x">>, InnerConfigMap).

action_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my action">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"parameters">> =>
            #{
                <<"attributes_template">> => [],
                <<"ordering_key_template">> => <<"">>,
                <<"payload_template">> => <<"">>,
                <<"pubsub_topic">> => <<"please override">>
            },
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_action_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(action, ?ACTION_TYPE_BIN, <<"x">>, InnerConfigMap).

get_config(K, TCConfig) -> emqx_bridge_v2_testlib:get_value(K, TCConfig).

get_config(K, TCConfig, Default) -> proplists:get_value(K, TCConfig, Default).

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

is_mocked_gcp(TCConfig) ->
    case emqx_common_test_helpers:get_matrix_prop(TCConfig, [?mocked_gcp], undefined) of
        ?mocked_gcp -> true;
        _ -> false
    end.

start_echo_http_server(TCConfig) ->
    maybe
        true ?= is_mocked_gcp(TCConfig),
        HTTPHost = "localhost",
        HTTPPath = '_',
        ServerSSLOpts =
            [
                {verify, verify_none},
                {versions, ['tlsv1.2', 'tlsv1.3']},
                {ciphers, ["ECDHE-RSA-AES256-GCM-SHA384", "TLS_CHACHA20_POLY1305_SHA256"]}
            ] ++ certs(),
        {ok, {HTTPPort, Pid}} = emqx_bridge_http_connector_test_server:start_link(
            random, HTTPPath, ServerSSLOpts
        ),
        unlink(Pid),
        ok = emqx_bridge_http_connector_test_server:set_handler(success_http_handler()),
        HostPort = HTTPHost ++ ":" ++ integer_to_list(HTTPPort),
        true = os:putenv("PUBSUB_EMULATOR_HOST", HostPort),
        persistent_term:put({emqx_bridge_gcp_pubsub_client, pubsub, transport}, tls),
        #{
            host_port => HostPort,
            host => HTTPHost,
            port => HTTPPort
        }
    end.

stop_echo_http_server() ->
    ok = emqx_bridge_http_connector_test_server:stop(),
    persistent_term:erase({emqx_bridge_gcp_pubsub_client, pubsub, transport}),
    set_emulator_host_env(),
    ok.

test_pid() ->
    persistent_term:get({?MODULE, test_pid}).

set_emulator_host_env() ->
    HostPort = "toxiproxy:8085",
    true = os:putenv("PUBSUB_EMULATOR_HOST", HostPort),
    ok.

success_http_handler() ->
    fun(Req0, State) ->
        TestPid = test_pid(),
        case {cowboy_req:method(Req0), cowboy_req:path(Req0)} of
            {<<"GET">>, <<"/v1/projects/myproject/topics/", _/binary>>} ->
                Rep = cowboy_req:reply(
                    200,
                    #{<<"content-type">> => <<"application/json">>},
                    <<"{}">>,
                    Req0
                ),
                {ok, Rep, State};
            _ ->
                {ok, Body, Req} = cowboy_req:read_body(Req0),
                TestPid ! {http, cowboy_req:headers(Req), Body},
                Rep = cowboy_req:reply(
                    200,
                    #{<<"content-type">> => <<"application/json">>},
                    emqx_utils_json:encode(#{messageIds => [<<"6058891368195201">>]}),
                    Req
                ),
                {ok, Rep, State}
        end
    end.

%% Only for errors
fixed_status_handler(StatusCode, FailureAttempts) ->
    Tid = ets:new(requests, [public]),
    GetAndBump = fun(Method, Path, Body) ->
        K = {Method, Path, Body},
        ets:update_counter(Tid, K, 1, {K, 0})
    end,
    fun(Req0, State) ->
        case {cowboy_req:method(Req0), cowboy_req:path(Req0)} of
            {<<"GET">>, <<"/v1/projects/myproject/topics/", _/binary>>} ->
                Rep = cowboy_req:reply(
                    200,
                    #{<<"content-type">> => <<"application/json">>},
                    <<"{}">>,
                    Req0
                ),
                {ok, Rep, State};
            {Method, Path} ->
                {ok, Body, Req} = cowboy_req:read_body(Req0),
                N = GetAndBump(Method, Path, Body),
                Rep =
                    case N > FailureAttempts of
                        false ->
                            ?tp(request_fail, #{body => Body, method => Method, path => Path}),
                            cowboy_req:reply(
                                StatusCode,
                                #{<<"content-type">> => <<"application/json">>},
                                emqx_utils_json:encode(#{<<"gcp">> => <<"is down">>}),
                                Req
                            );
                        true ->
                            ?tp(retry_succeeded, #{body => Body, method => Method, path => Path}),
                            cowboy_req:reply(
                                200,
                                #{<<"content-type">> => <<"application/json">>},
                                emqx_utils_json:encode(#{<<"gcp">> => <<"is back up">>}),
                                Req
                            )
                    end,
                {ok, Rep, State}
        end
    end.

certs() ->
    CertsPath = emqx_common_test_helpers:deps_path(emqx, "etc/certs"),
    [
        {keyfile, filename:join([CertsPath, "key.pem"])},
        {certfile, filename:join([CertsPath, "cert.pem"])},
        {cacertfile, filename:join([CertsPath, "cacert.pem"])}
    ].

kill_gun_process(EhttpcPid) ->
    State = ehttpc:get_state(EhttpcPid, minimal),
    GunPid = maps:get(client, State),
    true = is_pid(GunPid),
    _ = exit(GunPid, kill),
    ok.

kill_gun_processes(TCConfig) ->
    ConnectorResourceId = emqx_bridge_v2_testlib:connector_resource_id(TCConfig),
    Pool = ehttpc:workers(ConnectorResourceId),
    Workers = lists:map(fun({_, Pid}) -> Pid end, Pool),
    %% assert there is at least one pool member
    ?assertMatch([_ | _], Workers),
    lists:foreach(fun(Pid) -> kill_gun_process(Pid) end, Workers).

install_telemetry_handler(TestCase) ->
    Tid = ets:new(TestCase, [ordered_set, public]),
    HandlerId = TestCase,
    TestPid = self(),
    _ = telemetry:attach_many(
        HandlerId,
        emqx_resource_metrics:events(),
        fun(EventName, Measurements, Metadata, _Config) ->
            Data = #{
                name => EventName,
                measurements => Measurements,
                metadata => Metadata
            },
            ets:insert(Tid, {erlang:monotonic_time(), Data}),
            TestPid ! {telemetry, Data},
            ok
        end,
        unused_config
    ),
    on_exit(fun() ->
        telemetry:detach(HandlerId),
        ets:delete(Tid)
    end),
    Tid.

wait_telemetry_event(EventName, TCConfig) ->
    wait_telemetry_event(EventName, TCConfig, #{timeout => 5_000, n_events => 1}).

wait_telemetry_event(
    EventName,
    TCConfig,
    _Opts = #{
        timeout := Timeout,
        n_events := NEvents
    }
) ->
    TelemetryTable = get_config(telemetry_table, TCConfig),
    ResourceId = emqx_bridge_v2_testlib:resource_id(TCConfig),
    wait_n_events(TelemetryTable, ResourceId, NEvents, Timeout, EventName).

wait_n_events(_TelemetryTable, _ResourceId, NEvents, _Timeout, _EventName) when NEvents =< 0 ->
    ok;
wait_n_events(TelemetryTable, ResourceId, NEvents, Timeout, EventName) ->
    receive
        {telemetry, #{name := [_, _, EventName], measurements := #{counter_inc := Inc}} = Event} ->
            ct:pal("telemetry event: ~p", [Event]),
            wait_n_events(TelemetryTable, ResourceId, NEvents - Inc, Timeout, EventName)
    after Timeout ->
        RecordedEvents = ets:tab2list(TelemetryTable),
        CurrentMetrics = current_metrics(ResourceId),
        ct:pal("recorded events: ~p", [RecordedEvents]),
        ct:pal("current metrics: ~p", [CurrentMetrics]),
        error({timeout_waiting_for_telemetry, EventName})
    end.

receive_http_request(TCConfig) ->
    %% Assert
    #{} = get_config(http_server, TCConfig),
    ServiceAccountJSON = get_config(service_account_json, TCConfig),
    receive
        {http, Headers, Body} ->
            ct:pal("received publish:\n  ~p", [#{headers => Headers, body => Body}]),
            assert_valid_request_headers(Headers, ServiceAccountJSON),
            #{<<"messages">> := Msgs} = emqx_utils_json:decode(Body),
            lists:map(
                fun(Msg) ->
                    #{<<"data">> := Content64} = Msg,
                    Content = base64:decode(Content64),
                    Decoded = emqx_utils_json:decode(Content),
                    Msg#{<<"data">> := Decoded}
                end,
                Msgs
            )
    after 5_000 ->
        {messages, Mailbox} = process_info(self(), messages),
        error({timeout, #{mailbox => Mailbox}})
    end.

receive_http_requests(TCConfig, Opts) ->
    Default = #{n => 1},
    #{n := N} = maps:merge(Default, Opts),
    lists:flatmap(fun(_) -> receive_http_request(TCConfig) end, lists:seq(1, N)).

receive_all_events(EventName, Timeout, MaxEvents, FilterFn) ->
    receive_all_events(EventName, Timeout, MaxEvents, FilterFn, _Count = 0, _Acc = []).

receive_all_events(_EventName, _Timeout, MaxEvents, _FilterFn, Count, Acc) when
    Count >= MaxEvents
->
    lists:reverse(Acc);
receive_all_events(EventName, Timeout, MaxEvents, FilterFn, Count, Acc) ->
    receive
        {telemetry, #{name := [_, _, EventName]} = Event} ->
            case FilterFn(Event) of
                true ->
                    receive_all_events(
                        EventName,
                        Timeout,
                        MaxEvents,
                        FilterFn,
                        Count + 1,
                        [Event | Acc]
                    );
                false ->
                    receive_all_events(
                        EventName,
                        Timeout,
                        MaxEvents,
                        FilterFn,
                        Count,
                        Acc
                    )
            end
    after Timeout ->
        lists:reverse(Acc)
    end.

dump_telemetry_table(TCConfig) ->
    TelemetryTable = get_config(telemetry_table, TCConfig),
    ets:tab2list(TelemetryTable).

metrics_mapping() ->
    #{
        dropped => fun emqx_resource_metrics:dropped_get/1,
        dropped_expired => fun emqx_resource_metrics:dropped_expired_get/1,
        dropped_other => fun emqx_resource_metrics:dropped_other_get/1,
        dropped_queue_full => fun emqx_resource_metrics:dropped_queue_full_get/1,
        dropped_resource_not_found => fun emqx_resource_metrics:dropped_resource_not_found_get/1,
        dropped_resource_stopped => fun emqx_resource_metrics:dropped_resource_stopped_get/1,
        late_reply => fun emqx_resource_metrics:late_reply_get/1,
        failed => fun emqx_resource_metrics:failed_get/1,
        inflight => fun emqx_resource_metrics:inflight_get/1,
        matched => fun emqx_resource_metrics:matched_get/1,
        queuing => fun emqx_resource_metrics:queuing_get/1,
        retried => fun emqx_resource_metrics:retried_get/1,
        retried_failed => fun emqx_resource_metrics:retried_failed_get/1,
        retried_success => fun emqx_resource_metrics:retried_success_get/1,
        success => fun emqx_resource_metrics:success_get/1
    }.

current_action_metrics(TCConfig) ->
    ResourceId = emqx_bridge_v2_testlib:resource_id(TCConfig),
    current_metrics(ResourceId).

current_metrics(ResourceId) ->
    Mapping = metrics_mapping(),
    maps:from_list([
        {Metric, F(ResourceId)}
     || {Metric, F} <- maps:to_list(Mapping)
    ]).

assert_empty_metrics(TCConfig) ->
    Mapping = metrics_mapping(),
    ExpectedMetrics =
        lists:foldl(
            fun(Metric, Acc) ->
                Acc#{Metric => 0}
            end,
            #{},
            maps:keys(Mapping)
        ),
    assert_metrics(ExpectedMetrics, TCConfig).

assert_metrics(ExpectedMetrics, TCConfig) ->
    ResourceId = emqx_bridge_v2_testlib:resource_id(TCConfig),
    Mapping = metrics_mapping(),
    Metrics =
        lists:foldl(
            fun(Metric, Acc) ->
                #{Metric := Fun} = Mapping,
                Value = Fun(ResourceId),
                Acc#{Metric => Value}
            end,
            #{},
            maps:keys(ExpectedMetrics)
        ),
    CurrentMetrics = current_metrics(ResourceId),
    TelemetryTable = get(telemetry_table),
    RecordedEvents = ets:tab2list(TelemetryTable),
    ?retry(
        _Sleep0 = 300,
        _Attempts = 20,
        ?assertEqual(ExpectedMetrics, Metrics, #{
            current_metrics => CurrentMetrics,
            recorded_events => RecordedEvents
        })
    ),
    ok.

mk_res_id_filter(TCConfig) ->
    ResourceId = emqx_bridge_v2_testlib:resource_id(TCConfig),
    fun(Event) ->
        case Event of
            #{metadata := #{resource_id := ResId}} when ResId =:= ResourceId ->
                true;
            _ ->
                false
        end
    end.

wait_until_gauge_is(GaugeName, ExpectedValue, Timeout) ->
    wait_until_gauge_is(#{
        gauge_name => GaugeName,
        expected => ExpectedValue,
        timeout => Timeout
    }).

wait_until_gauge_is(#{} = Opts) ->
    GaugeName = maps:get(gauge_name, Opts),
    ExpectedValue = maps:get(expected, Opts),
    Timeout = maps:get(timeout, Opts),
    MaxEvents = maps:get(max_events, Opts, 10),
    FilterFn = maps:get(filter_fn, Opts, fun(_Event) -> true end),
    Events = receive_all_events(GaugeName, Timeout, MaxEvents, FilterFn),
    case length(Events) > 0 andalso lists:last(Events) of
        #{measurements := #{gauge_set := ExpectedValue}} ->
            ok;
        #{measurements := #{gauge_set := Value}} ->
            ct:pal("events: ~p", [Events]),
            ct:fail(
                "gauge ~p didn't reach expected value ~p; last value: ~p",
                [GaugeName, ExpectedValue, Value]
            );
        false ->
            ct:pal("no ~p gauge events received!", [GaugeName])
    end.

assert_http_request(TCConfig) ->
    ServiceAccountJSON = get_config(service_account_json, TCConfig),
    receive
        {http, Headers, Body} ->
            assert_valid_request_headers(Headers, ServiceAccountJSON),
            assert_valid_request_body(Body)
    after 5_000 ->
        {messages, Mailbox} = process_info(self(), messages),
        error({timeout, #{mailbox => Mailbox}})
    end.

assert_valid_request_headers(Headers, ServiceAccountJSON) ->
    case Headers of
        #{<<"authorization">> := <<"Bearer ", JWT/binary>>} ->
            verify_token(ServiceAccountJSON, JWT),
            ok;
        _ ->
            %% better to raise a value than to use `ct:fail'
            %% because it doesn't output very well...
            error({expected_bearer_authn_header, #{headers => Headers}})
    end.

assert_valid_request_body(Body) ->
    BodyMap = emqx_utils_json:decode(Body),
    ?assertMatch(#{<<"messages">> := [_ | _]}, BodyMap),
    ct:pal("request: ~p", [BodyMap]),
    #{<<"messages">> := Messages} = BodyMap,
    lists:map(
        fun(Msg) ->
            ?assertMatch(#{<<"data">> := <<_/binary>>}, Msg),
            #{<<"data">> := Content64} = Msg,
            Content = base64:decode(Content64),
            Decoded = emqx_utils_json:decode(Content),
            ct:pal("decoded payload: ~p", [Decoded]),
            ?assert(is_map(Decoded)),
            Decoded
        end,
        Messages
    ).

verify_token(ServiceAccountJSON, JWTBin) ->
    #{
        <<"private_key">> := PrivateKeyPEM,
        <<"private_key_id">> := KId,
        <<"client_email">> := ServiceAccountEmail
    } = ServiceAccountJSON,
    JWK = jose_jwk:from_pem(PrivateKeyPEM),
    {IsValid, JWT, JWS} = jose_jwt:verify(JWK, JWTBin),
    ?assertMatch(
        #jose_jwt{
            fields =
                #{
                    <<"aud">> := <<"https://pubsub.googleapis.com/">>,
                    <<"exp">> := _,
                    <<"iat">> := _,
                    <<"iss">> := ServiceAccountEmail,
                    <<"sub">> := ServiceAccountEmail
                }
        },
        JWT
    ),
    #jose_jwt{
        fields =
            #{
                <<"exp">> := Exp,
                <<"iat">> := Iat
            }
    } = JWT,
    ?assertEqual(Iat + 60 * 60, Exp),
    ?assert(Iat =< erlang:system_time(seconds)),
    ?assertMatch(
        #jose_jws{
            alg = {_Module, 'RS256'},
            fields =
                #{
                    <<"kid">> := KId,
                    <<"typ">> := <<"JWT">>
                }
        },
        JWS
    ),
    ?assert(IsValid, #{
        jwt => JWT,
        jws => JWS
    }),
    ok.

create_connector_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(TCConfig, Overrides)
    ).

create_action_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api(TCConfig, Overrides)
    ).

delete_connector_api(TCConfig) ->
    emqx_bridge_v2_testlib:delete_connector_api(TCConfig).

get_connector_api(TCConfig) ->
    #{connector_type := Type, connector_name := Name} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_connector_api(Type, Name)
    ).

get_action_api(Config) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_action_api(
            Config
        )
    ).

simple_create_rule_api(TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(TCConfig).

probe_action_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:probe_bridge_api(TCConfig, Overrides)
    ).

start_client() ->
    start_client(_Opts = #{}).

start_client(Opts) ->
    {ok, C} = emqtt:start_link(Opts),
    on_exit(fun() -> emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    C.

assert_persisted_service_account_json_is_binary(TCConfig) ->
    #{connector_name := ConnectorName} = emqx_bridge_v2_testlib:get_common_values(TCConfig),
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
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop() ->
    [{matrix, true}].
t_start_stop(matrix) ->
    [[?mocked_gcp]];
t_start_stop(Config) when is_list(Config) ->
    emqx_bridge_v2_testlib:t_start_stop(Config, gcp_client_stop).

t_on_get_status(Config) when is_list(Config) ->
    emqx_bridge_v2_testlib:t_on_get_status(Config).

t_publish_success() ->
    [{matrix, true}].
t_publish_success(matrix) ->
    [[?mocked_gcp, ?with_batch], [?mocked_gcp, ?without_batch]];
t_publish_success(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := Topic, id := RuleId} = simple_create_rule_api(TCConfig),
    assert_empty_metrics(TCConfig),
    Payload = <<"payload">>,
    Message = emqx_message:make(Topic, Payload),
    emqx:publish(Message),
    DecodedMessages = assert_http_request(TCConfig),
    ?assertMatch(
        [
            #{
                <<"topic">> := Topic,
                <<"payload">> := Payload,
                <<"metadata">> := #{<<"rule_id">> := RuleId}
            } = Msg
        ] when not (is_map_key(<<"attributes">>, Msg) orelse is_map_key(<<"orderingKey">>, Msg)),
        DecodedMessages
    ),
    %% to avoid test flakiness
    wait_telemetry_event(success, TCConfig),
    wait_until_gauge_is(queuing, 0, 500),
    wait_until_gauge_is(inflight, 0, 500),
    assert_metrics(
        #{
            dropped => 0,
            failed => 0,
            inflight => 0,
            matched => 1,
            queuing => 0,
            retried => 0,
            success => 1
        },
        TCConfig
    ),
    ok.

t_publish_success_infinity_timeout() ->
    [{matrix, true}].
t_publish_success_infinity_timeout(matrix) ->
    [[?mocked_gcp]];
t_publish_success_infinity_timeout(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"resource_opts">> => #{<<"request_ttl">> => <<"infinity">>}
    }),
    #{topic := Topic, id := RuleId} = simple_create_rule_api(TCConfig),
    Payload = <<"payload">>,
    Message = emqx_message:make(Topic, Payload),
    emqx:publish(Message),
    DecodedMessages = assert_http_request(TCConfig),
    ?assertMatch(
        [
            #{
                <<"topic">> := Topic,
                <<"payload">> := Payload,
                <<"metadata">> := #{<<"rule_id">> := RuleId}
            }
        ],
        DecodedMessages
    ),
    ok.

t_publish_templated() ->
    [{matrix, true}].
t_publish_templated(matrix) ->
    [[?mocked_gcp]];
t_publish_templated(TCConfig) ->
    PayloadTemplate = <<
        "{\"payload\": \"${payload}\","
        " \"pub_props\": ${pub_props}}"
    >>,
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{<<"payload_template">> => PayloadTemplate}
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    assert_empty_metrics(TCConfig),
    Payload = <<"payload">>,
    Message =
        emqx_message:set_header(
            properties,
            #{'User-Property' => #{'Correlation-Data' => <<"321">>}},
            emqx_message:make(Topic, Payload)
        ),
    emqx:publish(Message),
    DecodedMessages = assert_http_request(TCConfig),
    ?assertMatch(
        [
            #{
                <<"payload">> := Payload,
                <<"pub_props">> := #{
                    <<"User-Property">> :=
                        #{
                            <<"Correlation-Data">> :=
                                <<"321">>
                        }
                }
            }
        ],
        DecodedMessages
    ),
    %% to avoid test flakiness
    wait_telemetry_event(success, TCConfig),
    wait_until_gauge_is(queuing, 0, 500),
    wait_until_gauge_is(inflight, 0, 500),
    assert_metrics(
        #{
            dropped => 0,
            failed => 0,
            inflight => 0,
            matched => 1,
            queuing => 0,
            retried => 0,
            success => 1
        },
        TCConfig
    ),
    ok.

t_publish_success_batch() ->
    [{matrix, true}].
t_publish_success_batch(matrix) ->
    [[?mocked_gcp, ?with_batch]];
t_publish_success_batch(TCConfig) ->
    BatchSize = get_config(batch_size, TCConfig),
    BatchTime = get_config(batch_time, TCConfig),
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"resource_opts">> => #{
            <<"worker_pool_size">> => 1,
            <<"batch_time">> => BatchTime,
            <<"batch_size">> => BatchSize
        }
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    assert_empty_metrics(TCConfig),
    NumMessages = BatchSize * 2,
    Messages = [emqx_message:make(Topic, integer_to_binary(N)) || N <- lists:seq(1, NumMessages)],
    %% publish in parallel to avoid each client blocking and then
    %% making 1-sized batches.  also important to note that the pool
    %% size for the resource (replayq buffering) must be set to 1 to
    %% avoid further segmentation of batches.
    emqx_utils:pmap(fun emqx:publish/1, Messages),
    DecodedMessages0 = assert_http_request(TCConfig),
    ?assertEqual(BatchSize, length(DecodedMessages0)),
    DecodedMessages1 = assert_http_request(TCConfig),
    ?assertEqual(BatchSize, length(DecodedMessages1)),
    PublishedPayloads =
        sets:from_list(
            [P || #{<<"payload">> := P} <- DecodedMessages0 ++ DecodedMessages1],
            [{version, 2}]
        ),
    ExpectedPayloads =
        sets:from_list(
            lists:map(fun integer_to_binary/1, lists:seq(1, NumMessages)),
            [{version, 2}]
        ),
    ?assertEqual(ExpectedPayloads, PublishedPayloads),
    wait_telemetry_event(
        success,
        TCConfig,
        #{timeout => 15_000, n_events => NumMessages}
    ),
    wait_until_gauge_is(queuing, 0, _Timeout = 400),
    wait_until_gauge_is(inflight, 0, _Timeout = 400),
    assert_metrics(
        #{
            dropped => 0,
            failed => 0,
            inflight => 0,
            matched => NumMessages,
            queuing => 0,
            retried => 0,
            success => NumMessages
        },
        TCConfig
    ),
    ok.

t_not_a_json(TCConfig) ->
    ?assertMatch(
        {400, #{
            <<"message">> := #{
                <<"kind">> := <<"validation_error">>,
                <<"reason">> := <<"not a json">>,
                %% should be censored as it contains secrets
                <<"value">> := <<"******">>
            }
        }},
        create_connector_api(
            TCConfig,
            #{
                <<"service_account_json">> => <<"not a json">>
            }
        )
    ),
    ok.

t_not_of_service_account_type(TCConfig) ->
    JSON = emqx_bridge_gcp_pubsub_utils:generate_service_account_json(),
    ?assertMatch(
        {400, #{
            <<"message">> := #{
                <<"kind">> := <<"validation_error">>,
                <<"reason">> := #{<<"wrong_type">> := <<"not a service account">>},
                %% should be censored as it contains secrets
                <<"value">> := <<"******">>
            }
        }},
        create_connector_api(
            TCConfig,
            #{
                <<"service_account_json">> => JSON#{<<"type">> := <<"not a service account">>}
            }
        )
    ),
    ok.

t_json_missing_fields(TCConfig) ->
    ?assertMatch(
        {400, #{
            <<"message">> := #{
                <<"kind">> := <<"validation_error">>,
                <<"reason">> :=
                    #{
                        <<"missing_keys">> := [
                            <<"client_email">>,
                            <<"private_key">>,
                            <<"private_key_id">>,
                            <<"project_id">>,
                            <<"type">>
                        ]
                    },
                %% should be censored as it contains secrets
                <<"value">> := <<"******">>
            }
        }},
        create_connector_api(
            TCConfig,
            #{
                <<"service_account_json">> => #{}
            }
        )
    ),
    ok.

t_invalid_private_key(TCConfig) ->
    InvalidPrivateKeyPEM = <<"xxxxxx">>,
    JSON = emqx_bridge_gcp_pubsub_utils:generate_service_account_json(),
    ?check_trace(
        begin
            {{201, _}, {ok, _Event}} =
                ?wait_async_action(
                    create_connector_api(TCConfig, #{
                        <<"service_account_json">> =>
                            JSON#{<<"private_key">> => InvalidPrivateKeyPEM}
                    }),
                    #{?snk_kind := gcp_client_startup_error},
                    20_000
                )
        end,
        fun(Trace) ->
            ?assertMatch(
                [#{error := empty_key}],
                ?of_kind(gcp_client_startup_error, Trace)
            ),
            ok
        end
    ),
    ok.

t_truncated_private_key(TCConfig) ->
    InvalidPrivateKeyPEM = <<"-----BEGIN PRIVATE KEY-----\nMIIEvQI...">>,
    JSON = emqx_bridge_gcp_pubsub_utils:generate_service_account_json(),
    ?check_trace(
        begin
            {{201, _}, {ok, _Event}} =
                ?wait_async_action(
                    create_connector_api(TCConfig, #{
                        <<"service_account_json">> =>
                            JSON#{<<"private_key">> => InvalidPrivateKeyPEM}
                    }),
                    #{?snk_kind := gcp_client_startup_error},
                    20_000
                )
        end,
        fun(Trace) ->
            ?assertMatch(
                [#{error := invalid_private_key}],
                ?of_kind(gcp_client_startup_error, Trace)
            ),
            ok
        end
    ),
    ok.

t_jose_error_tuple(TCConfig) ->
    ?check_trace(
        begin
            {{201, _}, {ok, _Event}} =
                ?wait_async_action(
                    emqx_common_test_helpers:with_mock(
                        jose_jwk,
                        from_pem,
                        fun(_PrivateKeyPEM) -> {error, some_error} end,
                        fun() ->
                            create_connector_api(TCConfig, #{})
                        end
                    ),
                    #{?snk_kind := gcp_client_startup_error},
                    20_000
                )
        end,
        fun(Trace) ->
            ?assertMatch(
                [#{error := {invalid_private_key, some_error}}],
                ?of_kind(gcp_client_startup_error, Trace)
            ),
            ok
        end
    ),
    ok.

t_jose_other_error(TCConfig) ->
    ?check_trace(
        begin
            {{201, _}, {ok, _Event}} =
                ?wait_async_action(
                    emqx_common_test_helpers:with_mock(
                        jose_jwk,
                        from_pem,
                        fun(_PrivateKeyPEM) -> {unknown, error} end,
                        fun() ->
                            create_connector_api(TCConfig, #{})
                        end
                    ),
                    #{?snk_kind := gcp_client_startup_error},
                    20_000
                )
        end,
        fun(Trace) ->
            ?assertMatch(
                [#{error := {invalid_private_key, {unknown, error}}} | _],
                ?of_kind(gcp_client_startup_error, Trace)
            ),
            ok
        end
    ),
    ok.

t_publish_econnrefused() ->
    [{matrix, true}].
t_publish_econnrefused(matrix) ->
    [[?mocked_gcp]];
t_publish_econnrefused(TCConfig) ->
    %% set pipelining to 1 so that one of the 2 requests is `pending'
    %% in ehttpc.
    {201, _} = create_connector_api(
        TCConfig,
        #{
            <<"pipelining">> => 1,
            <<"resource_opts">> => #{<<"resume_interval">> => <<"15s">>}
        }
    ),
    {201, _} = create_action_api(TCConfig, #{}),
    assert_empty_metrics(TCConfig),
    ok = emqx_bridge_http_connector_test_server:stop(),
    do_econnrefused_or_timeout_test(TCConfig, econnrefused).

t_publish_timeout() ->
    [{matrix, true}].
t_publish_timeout(matrix) ->
    [[?mocked_gcp]];
t_publish_timeout(TCConfig) ->
    %% set pipelining to 1 so that one of the 2 requests is `pending'
    %% in ehttpc. also, we set the batch size to 1 to also ensure the
    %% requests are done separately.
    {201, _} = create_connector_api(
        TCConfig,
        #{
            <<"pipelining">> => 1,
            <<"resource_opts">> => #{<<"resume_interval">> => <<"15s">>}
        }
    ),
    {201, _} = create_action_api(
        TCConfig,
        #{
            <<"resource_opts">> => #{
                <<"batch_size">> => 1,
                <<"resume_interval">> => <<"1s">>,
                <<"request_ttl">> => <<"500ms">>,
                <<"metrics_flush_interval">> => <<"700ms">>
            }
        }
    ),
    assert_empty_metrics(TCConfig),
    TestPid = self(),
    TimeoutHandler =
        fun(Req0, State) ->
            {ok, Body, Req} = cowboy_req:read_body(Req0),
            TestPid ! {http, cowboy_req:headers(Req), Body},
            %% NOTE: cannot just hang forever; ehttpc will never
            %% reply `sent' requests, so the callback will never
            %% be called...  We just delay responding so that a
            %% late response is delivered.
            timer:sleep(timer:seconds(3)),
            Rep = cowboy_req:reply(
                200,
                #{<<"content-type">> => <<"application/json">>},
                emqx_utils_json:encode(#{messageIds => [<<"6058891368195201">>]}),
                Req
            ),
            {ok, Rep, State}
        end,
    ok = emqx_bridge_http_connector_test_server:set_handler(TimeoutHandler),
    do_econnrefused_or_timeout_test(TCConfig, timeout).

do_econnrefused_or_timeout_test(TCConfig, Error) ->
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    Payload = <<"payload">>,
    C = start_client(),
    ?check_trace(
        begin
            case Error of
                econnrefused ->
                    %% at the time of writing, async requests
                    %% are never considered expired by ehttpc
                    %% (even if they arrive late, or never
                    %% arrive at all).
                    %% so, we set pipelining to 1 and shoot 2
                    %% requests, so that the second one may expire.
                    {_, {ok, _}} =
                        ?wait_async_action(
                            begin
                                emqtt:publish(C, Topic, Payload, [{qos, 0}]),
                                emqtt:publish(C, Topic, Payload, [{qos, 0}])
                            end,
                            #{
                                ?snk_kind := gcp_client_request_failed,
                                reason := econnrefused
                            },
                            15_000
                        );
                timeout ->
                    %% at the time of writing, async requests
                    %% are never considered expired by ehttpc
                    %% (even if they arrive late, or never
                    %% arrive at all).
                    %% with the timeout delay, this'll succeed.
                    emqtt:publish(C, Topic, Payload, [{qos, 0}]),
                    emqtt:publish(C, Topic, Payload, [{qos, 0}]),
                    {ok, _} = snabbkaffe:block_until(
                        ?match_n_events(2, #{
                            ?snk_kind := handle_async_reply_expired,
                            expired := [_]
                        }),
                        _Timeout1 = 15_000
                    )
            end
        end,
        fun(Trace) ->
            case Error of
                econnrefused ->
                    case ?of_kind(gcp_client_request_failed, Trace) of
                        [#{reason := Reason} | _] when
                            Reason == Error;
                            Reason == closed;
                            element(2, Reason) == closed
                        ->
                            ok;
                        Trace0 ->
                            error({unexpected_trace, Trace0})
                    end;
                timeout ->
                    ?assertMatch(
                        [_, _ | _],
                        ?of_kind(handle_async_reply_expired, Trace)
                    )
            end,
            ok
        end
    ),

    case Error of
        %% apparently, async with disabled queue doesn't mark the
        %% message as dropped; and since it never considers the
        %% response expired, this succeeds.
        econnrefused ->
            %% even waiting, hard to avoid flakiness... simpler to just sleep
            %% a bit until stabilization.
            ct:sleep(200),
            CurrentMetrics = current_action_metrics(TCConfig),
            RecordedEvents = dump_telemetry_table(TCConfig),
            ct:pal("telemetry events:\n  ~p", [RecordedEvents]),
            ?assertMatch(
                #{
                    dropped := Dropped,
                    failed := Failed,
                    inflight := Inflight,
                    matched := Matched,
                    queuing := Queueing,
                    retried := 0,
                    success := 0
                } when Matched >= 1 andalso Inflight + Queueing + Dropped + Failed =< 2,
                CurrentMetrics
            );
        timeout ->
            wait_telemetry_event(
                late_reply,
                TCConfig,
                #{timeout => 5_000, n_events => 2}
            ),
            wait_until_gauge_is(#{
                gauge_name => inflight,
                expected => 0,
                filter_fn => mk_res_id_filter(TCConfig),
                timeout => 1_000,
                max_events => 20
            }),
            wait_until_gauge_is(queuing, 0, _Timeout = 1_000),
            assert_metrics(
                #{
                    dropped => 0,
                    failed => 0,
                    inflight => 0,
                    matched => 2,
                    queuing => 0,
                    retried => 0,
                    success => 0,
                    late_reply => 2
                },
                TCConfig
            )
    end,
    ok.

%% for complete coverage; pubsub actually returns a body with message
%% ids
t_success_no_body() ->
    [{matrix, true}].
t_success_no_body(matrix) ->
    [[?mocked_gcp]];
t_success_no_body(TCConfig) ->
    TestPid = self(),
    SuccessNoBodyHandler =
        fun(Req0, State) ->
            {ok, Body, Req} = cowboy_req:read_body(Req0),
            TestPid ! {http, cowboy_req:headers(Req), Body},
            Rep = cowboy_req:reply(
                200,
                #{<<"content-type">> => <<"application/json">>},
                <<>>,
                Req
            ),
            {ok, Rep, State}
        end,
    ok = emqx_bridge_http_connector_test_server:set_handler(SuccessNoBodyHandler),
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    Payload = <<"payload">>,
    ?check_trace(
        {_, {ok, _}} =
            ?wait_async_action(
                emqtt:publish(C, Topic, Payload),
                #{?snk_kind := gcp_pubsub_response},
                5_000
            ),
        fun(Trace) ->
            ?assertMatch(
                [#{response := {ok, 200, _Headers}}],
                ?of_kind(gcp_pubsub_response, Trace)
            ),
            ok
        end
    ),
    ok.

t_failure_no_body() ->
    [{matrix, true}].
t_failure_no_body(matrix) ->
    [[?mocked_gcp]];
t_failure_no_body(TCConfig) ->
    TestPid = self(),
    FailureNoBodyHandler =
        fun(Req0, State) ->
            case {cowboy_req:method(Req0), cowboy_req:path(Req0)} of
                {<<"GET">>, <<"/v1/projects/myproject/topics/", _/binary>>} ->
                    Rep = cowboy_req:reply(
                        200,
                        #{<<"content-type">> => <<"application/json">>},
                        <<"{}">>,
                        Req0
                    ),
                    {ok, Rep, State};
                _ ->
                    {ok, Body, Req} = cowboy_req:read_body(Req0),
                    TestPid ! {http, cowboy_req:headers(Req), Body},
                    Rep = cowboy_req:reply(
                        400,
                        #{<<"content-type">> => <<"application/json">>},
                        <<>>,
                        Req
                    ),
                    {ok, Rep, State}
            end
        end,
    ok = emqx_bridge_http_connector_test_server:set_handler(FailureNoBodyHandler),
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    Payload = <<"payload">>,
    ?check_trace(
        {_, {ok, _}} =
            ?wait_async_action(
                emqtt:publish(C, Topic, Payload),
                #{?snk_kind := gcp_pubsub_response},
                5_000
            ),
        fun(Trace) ->
            ?assertMatch(
                [#{response := {ok, 400, _Headers}}],
                ?of_kind(gcp_pubsub_response, Trace)
            ),
            ok
        end
    ),
    ok.

t_unrecoverable_error() ->
    [{matrix, true}].
t_unrecoverable_error(matrix) ->
    [[?mocked_gcp]];
t_unrecoverable_error(TCConfig) ->
    TestPid = self(),
    FailureNoBodyHandler =
        fun(Req0, State) ->
            case {cowboy_req:method(Req0), cowboy_req:path(Req0)} of
                {<<"GET">>, <<"/v1/projects/myproject/topics/", _/binary>>} ->
                    Rep = cowboy_req:reply(
                        200,
                        #{<<"content-type">> => <<"application/json">>},
                        <<"{}">>,
                        Req0
                    ),
                    {ok, Rep, State};
                _ ->
                    {ok, Body, Req} = cowboy_req:read_body(Req0),
                    TestPid ! {http, cowboy_req:headers(Req), Body},
                    %% kill the gun process while it's waiting for the
                    %% response so we provoke an `{error, _}' response from
                    %% ehttpc.
                    ok = kill_gun_processes(TCConfig),
                    Rep = cowboy_req:reply(
                        200,
                        #{<<"content-type">> => <<"application/json">>},
                        <<>>,
                        Req
                    ),
                    {ok, Rep, State}
            end
        end,
    ok = emqx_bridge_http_connector_test_server:set_handler(FailureNoBodyHandler),
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    assert_empty_metrics(TCConfig),
    Payload = <<"payload">>,
    ?check_trace(
        {_, {ok, _}} =
            ?wait_async_action(
                emqtt:publish(C, Topic, Payload),
                #{?snk_kind := gcp_pubsub_request_failed},
                5_000
            ),
        fun(Trace) ->
            ?assertMatch(
                [#{reason := killed} | _],
                ?of_kind(gcp_pubsub_request_failed, Trace)
            ),
            ok
        end
    ),

    wait_until_gauge_is(queuing, 0, _Timeout = 400),
    %% TODO: once temporary clause in
    %% `emqx_resource_buffer_worker:is_unrecoverable_error'
    %% that marks all unknown errors as unrecoverable is
    %% removed, this inflight should be 1, because we retry if
    %% the worker is killed.
    wait_until_gauge_is(inflight, 0, _Timeout = 400),
    wait_telemetry_event(failed, TCConfig),
    assert_metrics(
        #{
            dropped => 0,
            %% FIXME: see comment above; failed should be 0
            %% and inflight should be 1.
            failed => 1,
            inflight => 0,
            matched => 1,
            queuing => 0,
            retried => 0,
            success => 0
        },
        TCConfig
    ),
    ok.

%% Usually not called, since the bridge has `async_if_possible' callback mode.
t_query_sync() ->
    [{matrix, true}].
t_query_sync(matrix) ->
    [[?mocked_gcp, ?without_batch], [?mocked_gcp, ?with_batch]];
t_query_sync(TCConfig) ->
    Payload = <<"payload">>,
    BatchSize = get_config(batch_size, TCConfig, 1),
    ?check_trace(
        emqx_common_test_helpers:with_mock(
            emqx_bridge_gcp_pubsub_impl_producer,
            callback_mode,
            fun() -> always_sync end,
            fun() ->
                {201, _} = create_connector_api(TCConfig, #{}),
                {201, _} = create_action_api(TCConfig, #{}),
                #{topic := Topic} = simple_create_rule_api(TCConfig),
                C = start_client(),
                emqx_utils:pmap(
                    fun(_) -> emqtt:publish(C, Topic, Payload, [{qos, 0}]) end,
                    lists:seq(1, BatchSize)
                ),
                DecodedMessages = assert_http_request(TCConfig),
                ?assertEqual(BatchSize, length(DecodedMessages)),
                ok
            end
        ),
        []
    ),
    ok.

t_stop(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    ?check_trace(
        {{204, _}, {ok, _}} =
            ?wait_async_action(
                delete_connector_api(TCConfig),
                #{?snk_kind := gcp_client_stop},
                5_000
            ),
        fun(Trace) ->
            ?assertMatch([_], ?of_kind(gcp_client_stop, Trace)),
            ?assertMatch([_ | _], ?of_kind(connector_jwt_deleted, Trace)),
            ok
        end
    ),
    ok.

t_get_status_no_worker(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    emqx_common_test_helpers:with_mock(
        ehttpc,
        workers,
        fun(_Poolname) -> [] end,
        fun() ->
            ?retry(
                500,
                20,
                ?assertMatch(
                    {200, #{<<"status">> := <<"disconnected">>}},
                    get_connector_api(TCConfig)
                )
            ),
            ok
        end
    ),
    ok.

t_get_status_down(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    emqx_common_test_helpers:with_mock(
        ehttpc,
        health_check,
        fun(_Worker, _Timeout) ->
            {error, connect_timeout}
        end,
        fun() ->
            ?retry(
                500,
                20,
                ?assertMatch(
                    {200, #{<<"status">> := <<"disconnected">>}},
                    get_connector_api(TCConfig)
                )
            ),
            ok
        end
    ),
    ok.

t_get_status_timeout_calling_workers(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{<<"connect_timeout">> => <<"1s">>}),
    emqx_common_test_helpers:with_mock(
        ehttpc,
        health_check,
        fun(_Worker, _Timeout) ->
            receive
            after infinity -> error(impossible)
            end
        end,
        fun() ->
            ?retry(
                _Sleep0 = 100,
                _Attempts0 = 20,
                ?assertMatch(
                    {200, #{<<"status">> := <<"disconnected">>}},
                    get_connector_api(TCConfig)
                )
            ),
            ok
        end
    ),
    ok.

t_on_start_ehttpc_pool_already_started(TCConfig) ->
    ?check_trace(
        begin
            ?force_ordering(
                #{?snk_kind := pool_started},
                #{?snk_kind := gcp_starting_ehttpc_pool}
            ),
            {ok, SubRef} =
                snabbkaffe:subscribe(
                    fun
                        (#{?snk_kind := gcp_on_start_before_starting_pool}) -> true;
                        (_) -> false
                    end,
                    5_000
                ),
            spawn_link(fun() ->
                {201, _} = create_connector_api(TCConfig, #{})
            end),
            {ok, [#{pool_name := PoolName, pool_opts := PoolOpts}]} = snabbkaffe:receive_events(
                SubRef
            ),
            ?assertMatch({ok, _}, ehttpc_sup:start_pool(PoolName, PoolOpts)),
            ?tp(pool_started, #{}),
            ?block_until(#{?snk_kind := gcp_ehttpc_pool_already_started}, 2_000),
            PoolName
        end,
        fun(PoolName, Trace) ->
            ?assertMatch(
                [#{pool_name := PoolName}],
                ?of_kind(gcp_ehttpc_pool_already_started, Trace)
            ),
            ok
        end
    ),
    ok.

t_on_start_ehttpc_pool_start_failure(TCConfig) ->
    ?check_trace(
        emqx_common_test_helpers:with_mock(
            ehttpc_sup,
            start_pool,
            fun(_PoolName, _PoolOpts) -> {error, some_error} end,
            fun() ->
                {201, _} = create_connector_api(TCConfig, #{})
            end
        ),
        fun(Trace) ->
            ?assertMatch(
                [#{reason := some_error} | _],
                ?of_kind(gcp_ehttpc_pool_start_failure, Trace)
            ),
            ok
        end
    ),
    ok.

t_attributes() ->
    [{matrix, true}].
t_attributes(matrix) ->
    [[?mocked_gcp]];
t_attributes(TCConfig) ->
    Name = get_config(action_name, TCConfig),
    ?check_trace(
        begin
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, _} = create_action_api(TCConfig, #{
                <<"parameters">> => #{
                    <<"attributes_template">> =>
                        [
                            #{
                                <<"key">> => <<"${.payload.key}">>,
                                <<"value">> => <<"fixed_value">>
                            },
                            #{
                                <<"key">> => <<"${.payload.key}2">>,
                                <<"value">> => <<"${.payload.value}">>
                            },
                            #{
                                <<"key">> => <<"fixed_key">>,
                                <<"value">> => <<"fixed_value">>
                            },
                            #{
                                <<"key">> => <<"fixed_key2">>,
                                <<"value">> => <<"${.payload.value}">>
                            }
                        ],
                    <<"ordering_key_template">> => <<"${.payload.ok}">>
                }
            }),
            #{topic := Topic} = simple_create_rule_api(TCConfig),
            C = start_client(),
            %% without ordering key
            Payload0 =
                emqx_utils_json:encode(
                    #{
                        <<"value">> => <<"payload_value">>,
                        <<"key">> => <<"payload_key">>
                    }
                ),
            emqtt:publish(C, Topic, Payload0),
            DecodedMessages0 = receive_http_request(TCConfig),
            ?assertMatch(
                [
                    #{
                        <<"attributes">> :=
                            #{
                                <<"fixed_key">> := <<"fixed_value">>,
                                <<"fixed_key2">> := <<"payload_value">>,
                                <<"payload_key">> := <<"fixed_value">>,
                                <<"payload_key2">> := <<"payload_value">>
                            },
                        <<"data">> := #{
                            <<"topic">> := _,
                            <<"payload">> := _
                        }
                    } = Msg
                ] when not is_map_key(<<"orderingKey">>, Msg),
                DecodedMessages0
            ),
            %% with ordering key
            Payload1 =
                emqx_utils_json:encode(
                    #{
                        <<"value">> => <<"payload_value">>,
                        <<"key">> => <<"payload_key">>,
                        <<"ok">> => <<"ordering_key">>
                    }
                ),
            emqtt:publish(C, Topic, Payload1),
            DecodedMessages1 = receive_http_request(TCConfig),
            ?assertMatch(
                [
                    #{
                        <<"attributes">> :=
                            #{
                                <<"fixed_key">> := <<"fixed_value">>,
                                <<"fixed_key2">> := <<"payload_value">>,
                                <<"payload_key">> := <<"fixed_value">>,
                                <<"payload_key2">> := <<"payload_value">>
                            },
                        <<"orderingKey">> := <<"ordering_key">>,
                        <<"data">> := #{
                            <<"topic">> := _,
                            <<"payload">> := _
                        }
                    }
                ],
                DecodedMessages1
            ),
            %% will result in empty key
            Payload2 =
                emqx_utils_json:encode(
                    #{
                        <<"value">> => <<"payload_value">>,
                        <<"ok">> => <<"ordering_key">>
                    }
                ),
            emqtt:publish(C, Topic, Payload2),
            [DecodedMessage2] = receive_http_request(TCConfig),
            ?assertEqual(
                #{
                    <<"fixed_key">> => <<"fixed_value">>,
                    <<"fixed_key2">> => <<"payload_value">>,
                    <<"2">> => <<"payload_value">>
                },
                maps:get(<<"attributes">>, DecodedMessage2)
            ),
            %% ensure loading cluster override file doesn't mangle the attribute
            %% placeholders...
            #{<<"actions">> := #{?ACTION_TYPE_BIN := #{Name := RawConf}}} =
                emqx_config:read_override_conf(#{override_to => cluster}),
            ?assertEqual(
                [
                    #{
                        <<"key">> => <<"${.payload.key}">>,
                        <<"value">> => <<"fixed_value">>
                    },
                    #{
                        <<"key">> => <<"${.payload.key}2">>,
                        <<"value">> => <<"${.payload.value}">>
                    },
                    #{
                        <<"key">> => <<"fixed_key">>,
                        <<"value">> => <<"fixed_value">>
                    },
                    #{
                        <<"key">> => <<"fixed_key2">>,
                        <<"value">> => <<"${.payload.value}">>
                    }
                ],
                emqx_utils_maps:deep_get([<<"parameters">>, <<"attributes_template">>], RawConf)
            ),
            ok
        end,
        []
    ),
    ok.

t_bad_attributes() ->
    [{matrix, true}].
t_bad_attributes(matrix) ->
    [[?mocked_gcp, ?with_batch]];
t_bad_attributes(TCConfig) ->
    ?check_trace(
        begin
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, _} = create_action_api(TCConfig, #{
                <<"parameters">> => #{
                    <<"attributes_template">> =>
                        [
                            #{
                                <<"key">> => <<"${.payload.key}">>,
                                <<"value">> => <<"${.payload.value}">>
                            }
                        ],
                    <<"ordering_key_template">> => <<"${.payload.ok}">>
                }
            }),
            #{topic := Topic} = simple_create_rule_api(TCConfig),
            C = start_client(),
            %% Ok: attribute value is a map or list
            lists:foreach(
                fun(OkValue) ->
                    Payload0 =
                        emqx_utils_json:encode(
                            #{
                                <<"ok">> => <<"ord_key">>,
                                <<"value">> => OkValue,
                                <<"key">> => <<"attr_key">>
                            }
                        ),
                    emqtt:publish(C, Topic, Payload0, [{qos, 0}])
                end,
                [
                    #{<<"some">> => <<"map">>},
                    [1, <<"str">>, #{<<"deep">> => true}]
                ]
            ),
            DecodedMessages0 = receive_http_requests(TCConfig, #{n => 1}),
            ?assertMatch(
                [
                    #{
                        <<"attributes">> :=
                            #{<<"attr_key">> := <<"{\"some\":\"map\"}">>},
                        <<"orderingKey">> := <<"ord_key">>
                    },
                    #{
                        <<"attributes">> :=
                            #{<<"attr_key">> := <<"[1,\"str\",{\"deep\":true}]">>},
                        <<"orderingKey">> := <<"ord_key">>
                    }
                ],
                DecodedMessages0
            ),
            %% Bad: key is not a plain value
            lists:foreach(
                fun(BadKey) ->
                    Payload1 =
                        emqx_utils_json:encode(
                            #{
                                <<"value">> => <<"v">>,
                                <<"key">> => BadKey,
                                <<"ok">> => BadKey
                            }
                        ),
                    emqtt:publish(C, Topic, Payload1, [{qos, 0}])
                end,
                [
                    #{<<"some">> => <<"map">>},
                    [1, <<"list">>, true],
                    true,
                    false
                ]
            ),
            DecodedMessages1 = receive_http_request(TCConfig),
            lists:foreach(
                fun(DMsg) ->
                    ?assertNot(is_map_key(<<"orderingKey">>, DMsg), #{decoded_message => DMsg}),
                    ?assertNot(is_map_key(<<"attributes">>, DMsg), #{decoded_message => DMsg}),
                    ok
                end,
                DecodedMessages1
            ),
            ok
        end,
        fun(Trace) ->
            ?assertMatch(
                [
                    #{placeholder := [<<"payload">>, <<"ok">>], value := #{}},
                    #{placeholder := [<<"payload">>, <<"key">>], value := #{}},
                    #{placeholder := [<<"payload">>, <<"ok">>], value := [_ | _]},
                    #{placeholder := [<<"payload">>, <<"key">>], value := [_ | _]},
                    #{placeholder := [<<"payload">>, <<"ok">>], value := true},
                    #{placeholder := [<<"payload">>, <<"key">>], value := true},
                    #{placeholder := [<<"payload">>, <<"ok">>], value := false},
                    #{placeholder := [<<"payload">>, <<"key">>], value := false}
                ],
                ?of_kind("gcp_pubsub_producer_bad_value_for_key", Trace)
            ),
            ok
        end
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

%% Check that creating an action (V2) with a non-existent topic returns an error.
t_bad_topic(TCConfig) ->
    ?check_trace(
        begin
            {201, _} = create_connector_api(TCConfig, #{}),
            ?assertMatch(
                {201, #{<<"status">> := <<"disconnected">>}},
                create_action_api(TCConfig, #{
                    <<"parameters">> => #{<<"pubsub_topic">> => <<"i-dont-exist">>}
                })
            ),
            ProbeRes = probe_action_api(
                TCConfig,
                #{<<"parameters">> => #{<<"pubsub_topic">> => <<"i-dont-exist">>}}
            ),
            ?assertMatch({400, _}, ProbeRes),
            {400, #{<<"message">> := Msg}} = ProbeRes,
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

%% Verifies the backoff and retry behavior when we receive a 502 or 503 error back.
t_backoff_retry() ->
    [{matrix, true}].
t_backoff_retry(matrix) ->
    [[?mocked_gcp]];
t_backoff_retry(TCConfig) ->
    ?check_trace(
        #{timetrap => 10_000},
        begin
            {201, #{<<"status">> := <<"connected">>}} = create_connector_api(TCConfig, #{
                <<"resource_opts">> => #{
                    <<"health_check_interval">> => <<"1s">>,
                    <<"resume_interval">> => <<"1s">>
                }
            }),
            {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{
                <<"resource_opts">> => #{
                    <<"health_check_interval">> => <<"1s">>
                }
            }),
            #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
            ?retry(
                200,
                10,
                ?assertMatch(
                    {200, #{<<"status">> := <<"connected">>}},
                    get_action_api(TCConfig)
                )
            ),
            C = start_client(),

            ok = emqx_bridge_http_connector_test_server:set_handler(fixed_status_handler(502, 2)),
            {{ok, _}, {ok, _}} =
                ?wait_async_action(
                    emqtt:publish(C, RuleTopic, <<"{}">>, [{qos, 1}]),
                    #{?snk_kind := retry_succeeded}
                ),

            ok = emqx_bridge_http_connector_test_server:set_handler(fixed_status_handler(503, 2)),
            {{ok, _}, {ok, _}} =
                ?wait_async_action(
                    emqtt:publish(C, RuleTopic, <<"{}">>, [{qos, 1}]),
                    #{?snk_kind := retry_succeeded}
                ),

            ok
        end,
        []
    ),
    ok.

%% Checks that we massage the error reason in case `jose_jwk:from_pem/1' raises a
%% `function_clause' error.  Currently, this can be caused by deleting one line from the
%% private key PEM in the service account JSON.  Instead of logging `{error,
%% function_clause}', we transform it to something more soothing to the user's eyes.
t_jose_jwk_function_clause(TCConfig) ->
    JSON0 =
        #{<<"private_key">> := PKey0} =
        emqx_bridge_gcp_pubsub_utils:generate_service_account_json(),
    Lines0 = binary:split(PKey0, <<"\n">>),
    NumLines = length(Lines0),
    {Lines1, [_ | Lines2]} = lists:split(NumLines div 2, Lines0),
    Lines = iolist_to_binary(lists:join(<<"\n">>, Lines1 ++ Lines2)),
    JSON = emqx_utils_json:encode(JSON0#{<<"private_key">> := Lines}),
    ?check_trace(
        {201, _} = create_connector_api(TCConfig, #{
            <<"service_account_json">> => JSON
        }),
        fun(Trace) ->
            ?assertMatch(
                [#{error := invalid_private_key} | _],
                ?of_kind(gcp_client_startup_error, Trace)
            ),
            ok
        end
    ),
    ok.

%% Checks that we return a pretty reason when health check fails.
t_connector_health_check_timeout(TCConfig) ->
    ?check_trace(
        begin
            with_failure(down, fun() ->
                ?assertMatch(
                    {201, #{<<"status_reason">> := <<"Connection refused">>}},
                    create_connector_api(TCConfig, #{})
                )
            end),
            ok
        end,
        []
    ),
    ok.
