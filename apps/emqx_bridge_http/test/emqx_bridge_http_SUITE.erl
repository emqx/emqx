%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_http_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx/include/emqx_config.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-define(CONNECTOR_TYPE, http).
-define(CONNECTOR_TYPE_BIN, <<"http">>).
-define(ACTION_TYPE, http).
-define(ACTION_TYPE_BIN, <<"http">>).

-define(tcp, tcp).
-define(tls, tls).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all_with_matrix(?MODULE).

groups() ->
    emqx_common_test_helpers:groups_with_matrix(?MODULE).

init_per_suite(TCConfig) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_bridge_http,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    [
        {apps, Apps}
        | TCConfig
    ].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(TestCase, TCConfig) ->
    Path = group_path(TCConfig, no_groups),
    ct:pal(asciiart:visible($%, "~p - ~s", [Path, TestCase])),
    HTTPPath = get_tc_prop(TestCase, http_path, <<"/path">>),
    {ServerSSLOpts, Scheme} =
        case is_tls(TCConfig) of
            ?tcp ->
                {false, <<"http">>};
            ?tls ->
                ServerSSLOpts0 =
                    [
                        {verify, get_tc_prop(TestCase, server_verify, verify_none)},
                        {fail_if_no_peer_cert, get_tc_prop(TestCase, mutual_tls, false)},
                        {versions, ['tlsv1.2', 'tlsv1.3']},
                        {ciphers, ["ECDHE-RSA-AES256-GCM-SHA384", "TLS_CHACHA20_POLY1305_SHA256"]}
                    ] ++ certs(),
                {ServerSSLOpts0, <<"https">>}
        end,
    Host = get_tc_prop(TestCase, http_host, "localhost"),
    ServerOpts = #{
        mode => get_tc_prop(TestCase, http_server_mode, normal),
        dns_control => get_tc_prop(TestCase, dns_control, false),
        host => Host,
        notify => self()
    },
    {ok, {HTTPPort, _Pid}} = emqx_bridge_http_connector_test_server:start_link(
        _Port = random, HTTPPath, ServerSSLOpts, ServerOpts
    ),
    on_exit(fun emqx_bridge_http_connector_test_server:stop/0),
    ok = emqx_bridge_http_connector_test_server:set_handler(success_http_handler(#{})),
    ConnectorName = atom_to_binary(TestCase),
    ConnectorConfig = connector_config(#{
        <<"url">> => emqx_bridge_v2_testlib:fmt(<<"${s}://${host}:${p}">>, #{
            s => Scheme,
            host => Host,
            p => HTTPPort
        })
    }),
    ActionName = ConnectorName,
    ActionConfig = action_config(#{
        <<"connector">> => ConnectorName
    }),
    snabbkaffe:start_trace(),
    [
        {bridge_kind, action},
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, ConnectorName},
        {connector_config, ConnectorConfig},
        {action_type, ?ACTION_TYPE},
        {action_name, ActionName},
        {action_config, ActionConfig},
        {http_server, #{port => HTTPPort, path => Path, host => Host}}
        | TCConfig
    ].

end_per_testcase(_TestCase, _TCConfig) ->
    snabbkaffe:stop(),
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

connector_config(Overrides) ->
    emqx_bridge_schema_testlib:http_connector_config(Overrides).

action_config(Overrides) ->
    emqx_bridge_schema_testlib:http_action_config(Overrides).

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

is_tls(TCConfig) ->
    emqx_common_test_helpers:get_matrix_prop(TCConfig, [?tls], ?tcp).

certs() ->
    CertsPath = emqx_common_test_helpers:ensure_test_certs(),
    [
        {keyfile, filename:join([CertsPath, "key.pem"])},
        {certfile, filename:join([CertsPath, "cert.pem"])},
        {cacertfile, filename:join([CertsPath, "cacert.pem"])}
    ].

receive_request_notifications(MessageIDs, _Acc) when map_size(MessageIDs) =:= 0 ->
    ok;
receive_request_notifications(MessageIDs, Acc) ->
    receive
        {http, _Headers, Body} ->
            RemainingMessageIDs = remove_message_id(MessageIDs, Body),
            receive_request_notifications(RemainingMessageIDs, [Body | Acc])
    after (30 * 1000) ->
        ct:pal("Waited a long time but did not get any message"),
        ct:pal("Messages received so far:\n  ~p", [Acc]),
        ct:pal("Mailbox:\n  ~p", [?drainMailbox()]),
        ct:fail("All requests did not reach server at least once")
    end.

remove_message_id(MessageIDs, IDBin) ->
    ID = erlang:binary_to_integer(IDBin),
    %% It is acceptable to get the same message more than once
    maps:without([ID], MessageIDs).

success_http_handler(Opts) ->
    ResponseDelay = maps:get(response_delay, Opts, 0),
    TestPid = self(),
    fun(Req0, State) ->
        {ok, Body, Req} = cowboy_req:read_body(Req0),
        Headers = cowboy_req:headers(Req),
        ct:pal("http request received: ~p", [
            #{body => Body, headers => Headers, response_delay => ResponseDelay}
        ]),
        ResponseDelay > 0 andalso timer:sleep(ResponseDelay),
        TestPid ! {http, Headers, Body},
        Rep = cowboy_req:reply(
            200,
            #{<<"content-type">> => <<"text/plain">>},
            <<"hello">>,
            Req
        ),
        {ok, Rep, State}
    end.

start_oauth2_token_server(TestPid) ->
    {ok, {Port, _Pid}} = emqx_utils_http_test_server:start_link(
        random, "/oauth/token", false
    ),
    on_exit(fun emqx_utils_http_test_server:stop/0),
    Token = <<"oauth2-access-token">>,
    ModeKey = {?MODULE, oauth2_token_mode, TestPid},
    persistent_term:put(ModeKey, {available, Token}),
    on_exit(fun() -> persistent_term:erase(ModeKey) end),
    ok = emqx_utils_http_test_server:set_handler(
        fun(Req0, State) ->
            {ok, RequestBody, Req1} = cowboy_req:read_body(Req0),
            TestPid ! {oauth2_token_request, RequestBody},
            Req =
                case persistent_term:get(ModeKey) of
                    {available, CurrentToken} ->
                        ResponseBody = emqx_utils_json:encode(#{
                            <<"access_token">> => CurrentToken,
                            <<"expires_in">> => 3600,
                            <<"token_type">> => <<"Bearer">>
                        }),
                        cowboy_req:reply(
                            200,
                            #{<<"content-type">> => <<"application/json">>},
                            ResponseBody,
                            Req1
                        );
                    unavailable ->
                        cowboy_req:reply(503, #{}, <<"unavailable">>, Req1)
                end,
            {ok, Req, State}
        end
    ),
    TokenEndpoint = emqx_bridge_v2_testlib:fmt(
        <<"http://127.0.0.1:${port}/oauth/token">>, #{port => Port}
    ),
    {TokenEndpoint, Token}.

set_oauth2_token_mode(TestPid, Mode) ->
    persistent_term:put({?MODULE, oauth2_token_mode, TestPid}, Mode).

set_oauth2_token(TestPid, Token) ->
    set_oauth2_token_mode(TestPid, {available, Token}).

not_found_http_handler() ->
    TestPid = self(),
    fun(Req0, State) ->
        {ok, Body, Req} = cowboy_req:read_body(Req0),
        TestPid ! {http, cowboy_req:headers(Req), Body},
        Rep = cowboy_req:reply(
            404,
            #{<<"content-type">> => <<"text/plain">>},
            <<"not found">>,
            Req
        ),
        {ok, Rep, State}
    end.

too_many_requests_http_handler() ->
    fail_then_success_http_handler(429).

service_unavailable_http_handler() ->
    fail_then_success_http_handler(503).

fail_then_success_http_handler(FailStatusCode) ->
    on_exit(fun() -> persistent_term:erase({?MODULE, times_called}) end),
    GetAndBump =
        fun() ->
            NCalled = persistent_term:get({?MODULE, times_called}, 0),
            persistent_term:put({?MODULE, times_called}, NCalled + 1),
            NCalled + 1
        end,
    TestPid = self(),
    fun(Req0, State) ->
        N = GetAndBump(),
        {ok, Body, Req} = cowboy_req:read_body(Req0),
        TestPid ! {http, cowboy_req:headers(Req), Body},
        Rep =
            case N >= 3 of
                true ->
                    cowboy_req:reply(
                        200,
                        #{<<"content-type">> => <<"text/plain">>},
                        <<"ok">>,
                        Req
                    );
                false ->
                    cowboy_req:reply(
                        FailStatusCode,
                        #{<<"content-type">> => <<"text/plain">>},
                        %% Body and no body to trigger different code paths
                        case N of
                            1 -> <<"slow down, buddy">>;
                            _ -> <<>>
                        end,
                        Req
                    )
            end,
        {ok, Rep, State}
    end.

create_connector_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:create_connector_api2(Config, Overrides).

get_connector_api(TCConfig) ->
    #{connector_type := Type, connector_name := Name} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_connector_api(Type, Name)
    ).

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

create_action_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:create_action_api2(Config, Overrides).

get_action_api(TCConfig) ->
    emqx_bridge_v2_testlib:get_action_api2(TCConfig).

get_action_metrics_api(TCConfig) ->
    emqx_bridge_v2_testlib:get_action_metrics_api(TCConfig).

trace_log_stream_api(TraceName, Opts) ->
    emqx_bridge_v2_testlib:trace_log_stream_api(TraceName, Opts).

probe_action_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:probe_bridge_api_simple(TCConfig, Overrides).

start_connector_api(TCConfig) ->
    #{connector_type := Type, connector_name := Name} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:start_connector_api(Name, Type)
    ).

disable_action_api(TCConfig) ->
    #{kind := Kind, type := Type, name := Name} = emqx_bridge_v2_testlib:get_common_values(
        TCConfig
    ),
    emqx_bridge_v2_testlib:disable_kind_api(Kind, Type, Name).

start_client() ->
    {ok, C} = emqtt:start_link(),
    on_exit(fun() -> emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    C.

unique_payload() ->
    integer_to_binary(erlang:unique_integer()).

simple_create_rule_api(TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(TCConfig).

delete_global_managed_certs_bundle(BundleName, Opts) ->
    QueryParams =
        case Opts of
            #{force_delete := ForceDelete} ->
                #{<<"force_delete">> => ForceDelete};
            _ ->
                #{}
        end,
    emqx_bridge_v2_testlib:simple_request(#{
        method => delete,
        url => emqx_mgmt_api_test_util:api_path(["certs", "global", "name", BundleName]),
        query_params => QueryParams
    }).

delete_global_managed_certs_file(BundleName, FileKind, Opts) ->
    QueryParams =
        case Opts of
            #{force_delete := ForceDelete} ->
                #{
                    <<"force_delete">> => ForceDelete,
                    <<"kind">> => FileKind
                };
            _ ->
                #{<<"kind">> => FileKind}
        end,
    emqx_bridge_v2_testlib:simple_request(#{
        method => delete,
        url => emqx_mgmt_api_test_util:api_path(["certs", "global", "name", BundleName]),
        query_params => QueryParams
    }).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop(Config) when is_list(Config) ->
    emqx_bridge_v2_testlib:t_start_stop(Config, emqx_connector_http_stopped).

t_on_get_status(Config) when is_list(Config) ->
    emqx_bridge_v2_testlib:t_on_get_status(Config).

t_active_health_check() ->
    [
        {matrix, true},
        {http_path, <<"/[...]">>},
        {server_verify, verify_peer},
        {mutual_tls, true}
    ].
-doc "Check active HTTP probes and MQTT delivery with GET/HEAD over HTTP/HTTPS.".
t_active_health_check(matrix) ->
    [[Transport, Method] || Transport <- [?tcp, ?tls], Method <- [get, head]];
t_active_health_check(TCConfig) when is_list(TCConfig) ->
    active_health_check_requests(TCConfig).

active_health_check_requests(TCConfig) ->
    TestPid = self(),
    Method = emqx_common_test_helpers:get_matrix_prop(TCConfig, [get, head], get),
    ok = emqx_bridge_http_connector_test_server:set_handler(fun(Req0, State) ->
        {ok, Body, Req1} = cowboy_req:read_body(Req0),
        TestPid !
            {active_http, cowboy_req:method(Req1), cowboy_req:path(Req1), cowboy_req:qs(Req1),
                cowboy_req:headers(Req1), cowboy_req:peer(Req1), Body},
        Req = cowboy_req:reply(204, #{}, <<>>, Req1),
        {ok, Req, State}
    end),
    URL = maps:get(<<"url">>, get_config(connector_config, TCConfig)),
    SSL =
        case is_tls(TCConfig) of
            tls -> (emqx_bridge_schema_testlib:tls_opts())#{<<"verify">> => <<"verify_peer">>};
            tcp -> #{}
        end,
    {201, #{<<"status">> := <<"connected">>}} = create_connector_api(TCConfig, #{
        <<"pool_size">> => 2,
        <<"url">> => <<URL/binary, "/ingest">>,
        <<"ssl">> => SSL,
        <<"headers">> => #{
            <<"authorization">> => <<"Basic static-credentials">>,
            <<"x-static">> => <<"static-value">>,
            <<"x-dynamic">> => <<"prefix-${clientid}">>,
            <<"x-${clientid}">> => <<"dynamic-name">>
        },
        <<"health_check">> => #{
            <<"enable">> => true,
            <<"method">> => atom_to_binary(Method),
            <<"path">> => <<"/health?full=1">>
        },
        <<"resource_opts">> => #{<<"health_check_interval">> => <<"1m">>}
    }),
    WireMethod = string:uppercase(atom_to_binary(Method)),
    {active_http, WireMethod, <<"/health">>, <<"full=1">>, Headers, Peer, <<>>} =
        ?assertReceive({active_http, WireMethod, <<"/health">>, _, _, _, _}, 5_000),
    ?assertMatch(
        #{
            <<"authorization">> := <<"Basic static-credentials">>,
            <<"x-static">> := <<"static-value">>
        },
        Headers
    ),
    ?assertNot(maps:is_key(<<"x-dynamic">>, Headers)),
    ?assertNot(maps:is_key(<<"x-">>, Headers)),
    ?assertNot(lists:member(<<"dynamic-name">>, maps:values(Headers))),
    ResId = emqx_bridge_v2_testlib:connector_resource_id(TCConfig),
    ?assertEqual({ok, connected}, emqx_resource:health_check(ResId)),
    {active_http, WireMethod, <<"/health">>, _, _, NextPeer, _} =
        ?assertReceive({active_http, WireMethod, <<"/health">>, _, _, _, _}, 2_000),
    ?assertEqual(Peer, NextPeer),
    ?assertNotReceive({active_http, WireMethod, <<"/health">>, _, _, _, _}, 100),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{<<"body">> => <<"${payload}">>}
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    Client = start_client(),
    Payload = unique_payload(),
    emqtt:publish(Client, Topic, Payload, [{qos, 1}]),
    {active_http, <<"POST">>, <<"/ingest/path">>, <<>>, BusinessHeaders, BusinessPeer, Payload} =
        ?assertReceive({active_http, <<"POST">>, _, _, _, _, Payload}, 5_000),
    ?assertNotEqual(Peer, BusinessPeer),
    ?assert(maps:is_key(<<"x-dynamic">>, BusinessHeaders)),
    ?assert(lists:member(<<"dynamic-name">>, maps:values(BusinessHeaders))),
    assert_http_metrics(TCConfig, #{<<"success">> => 1, <<"failed">> => 0, <<"matched">> => 1}).

t_active_health_check_failure() ->
    [{matrix, true}, {http_path, <<"/[...]">>}].
-doc "Check HTTP failures, resource alarms, buffered MQTT delivery and automatic recovery.".
t_active_health_check_failure(matrix) ->
    [[Fault] || Fault <- [hang, redirect, unauthorized, not_found, server_error]];
t_active_health_check_failure(TCConfig) when is_list(TCConfig) ->
    FaultKind = emqx_common_test_helpers:get_matrix_prop(
        TCConfig, [hang, redirect, unauthorized, not_found, server_error], hang
    ),
    Fault = maps:get(FaultKind, #{
        hang => hang, redirect => 301, unauthorized => 401, not_found => 404, server_error => 500
    }),
    TestPid = self(),
    Handler = fun(Req0, State) ->
        {ok, Body, Req1} = cowboy_req:read_body(Req0),
        TestPid ! {active_health_request, cowboy_req:path(Req1), Body},
        Req = cowboy_req:reply(200, #{}, <<>>, Req1),
        {ok, Req, State}
    end,
    ok = emqx_bridge_http_connector_test_server:set_handler(Handler),
    {201, #{<<"status">> := <<"connected">>}} = create_connector_api(TCConfig, #{
        <<"health_check">> => #{<<"enable">> => true, <<"path">> => <<"/health">>},
        <<"resource_opts">> => #{
            <<"health_check_interval">> => <<"1s">>,
            <<"health_check_timeout">> => <<"500ms">>
        }
    }),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{<<"body">> => <<"${payload}">>}
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    Client = start_client(),
    ok = emqx_bridge_http_connector_test_server:set_handler(fun(Req0, State) ->
        Req =
            case {cowboy_req:path(Req0), Fault} of
                {<<"/health">>, hang} ->
                    receive
                        continue -> ok
                    after 5_000 -> ok
                    end,
                    cowboy_req:reply(200, #{}, <<>>, Req0);
                {<<"/health">>, Status} ->
                    cowboy_req:reply(Status, #{<<"location">> => <<"/path">>}, <<>>, Req0);
                _ ->
                    cowboy_req:reply(200, #{}, <<>>, Req0)
            end,
        {ok, Req, State}
    end),
    ResId = emqx_bridge_v2_testlib:connector_resource_id(TCConfig),
    ?assertEqual({ok, disconnected}, emqx_resource:health_check(ResId)),
    {200, #{<<"status">> := <<"disconnected">>, <<"status_reason">> := Reason}} =
        get_connector_api(TCConfig),
    ExpectedReason =
        case Fault of
            hang -> <<"timeout">>;
            _ -> integer_to_binary(Fault)
        end,
    ?assertNotEqual(nomatch, binary:match(Reason, ExpectedReason)),
    ?retry(
        100,
        30,
        ?assertMatch(
            {200, #{<<"status">> := <<"disconnected">>}}, get_action_api(TCConfig)
        )
    ),
    ?assert(
        lists:any(fun(#{name := Name}) -> Name =:= ResId end, emqx_alarm:get_alarms(activated))
    ),
    Payload = unique_payload(),
    emqtt:publish_async(Client, Topic, Payload, [{qos, 1}], fun(_) -> ok end),
    ?assertNotReceive({active_health_request, <<"/path">>, Payload}, 200),
    ok = emqx_bridge_http_connector_test_server:set_handler(Handler),
    ?retry(100, 50, ?assertEqual({ok, connected}, emqx_resource:health_check(ResId))),
    ?assertReceive({active_health_request, <<"/path">>, Payload}, 5_000),
    ?retry(
        100,
        30,
        ?assertMatch(
            {200, #{<<"status">> := <<"connected">>}}, get_action_api(TCConfig)
        )
    ),
    ?assertNot(
        lists:any(fun(#{name := Name}) -> Name =:= ResId end, emqx_alarm:get_alarms(activated))
    ),
    assert_http_metrics(TCConfig, #{<<"success">> => 1, <<"failed">> => 0, <<"matched">> => 1}).

-doc "Check that a disabled probe sends no HTTP request and retains the TCP health check.".
t_active_health_check_disabled(TCConfig) ->
    TestPid = self(),
    ok = emqx_bridge_http_connector_test_server:set_handler(fun(Req0, State) ->
        TestPid ! unexpected_health_request,
        Req = cowboy_req:reply(500, #{}, <<>>, Req0),
        {ok, Req, State}
    end),
    {201, #{<<"status">> := <<"connected">>}} = create_connector_api(TCConfig, #{
        <<"health_check">> => #{<<"enable">> => false, <<"path">> => <<"/path">>}
    }),
    ResId = emqx_bridge_v2_testlib:connector_resource_id(TCConfig),
    ?assertEqual({ok, connected}, emqx_resource:health_check(ResId)),
    ?assertNotReceive(unexpected_health_request, 200),
    ?assertNot(maps:is_key(http_health_check_pool, emqx_resource:get_allocated_resources(ResId))).

t_active_health_check_oauth2_revoked(TCConfig) ->
    {First, Second, Client, Topic} = setup_oauth2_revocation(
        TCConfig,
        #{
            <<"health_check">> => #{<<"enable">> => true, <<"path">> => <<"/path">>},
            <<"resource_opts">> => #{<<"health_check_timeout">> => <<"2s">>}
        },
        #{}
    ),
    ResId = emqx_bridge_v2_testlib:connector_resource_id(TCConfig),
    ?assertEqual({ok, connected}, emqx_resource:health_check(ResId)),
    ?assertReceive({oauth2_http, First, <<>>, 401}, 2_000),
    ?assertReceive({oauth2_http, Second, <<>>, 200}, 2_000),
    ?assertReceive({oauth2_token_request, _}, 2_000),
    ?assertEqual({ok, Second}, emqx_connector_oauth2:get_token(ResId)),
    Payload = unique_payload(),
    emqtt:publish_async(Client, Topic, Payload, [{qos, 1}], fun(_) -> ok end),
    ?assertReceive({oauth2_http, Second, Payload, 200}, 3_000),
    assert_http_metrics(TCConfig, #{<<"success">> => 1}),
    ?assertNotReceive({oauth2_token_request, _}, 200).

t_active_health_check_oauth2_timeout(TCConfig) ->
    {_First, Second, Client, Topic} = setup_oauth2_revocation(
        TCConfig,
        #{
            <<"health_check">> => #{<<"enable">> => true, <<"path">> => <<"/path">>},
            <<"resource_opts">> => #{
                <<"health_check_interval">> => <<"1m">>,
                <<"health_check_timeout">> => <<"500ms">>
            }
        },
        #{}
    ),
    TestPid = self(),
    ok = emqx_utils_http_test_server:set_handler(fun(Req0, State) ->
        TestPid ! token_refresh_started,
        timer:sleep(2_000),
        Body = emqx_utils_json:encode(#{<<"access_token">> => Second, <<"expires_in">> => 3600}),
        Req = cowboy_req:reply(200, #{<<"content-type">> => <<"application/json">>}, Body, Req0),
        {ok, Req, State}
    end),
    ResId = emqx_bridge_v2_testlib:connector_resource_id(TCConfig),
    {ok, _, #{state := State}} = emqx_resource:get_instance(ResId),
    {Elapsed, Result} = timer:tc(fun() -> emqx_bridge_http_connector:on_get_status(ResId, State) end),
    ?assertMatch({disconnected, {http_health_check_failed, timeout}}, Result),
    ?assert(Elapsed < 1_000_000),
    ?assertReceive(token_refresh_started, 100),
    %% The caller timeout must not cancel a token fetch shared with business requests.
    ?assertEqual({ok, Second}, emqx_connector_oauth2:get_token(ResId)),
    ?assertEqual({ok, connected}, emqx_resource:health_check(ResId)),
    Payload = unique_payload(),
    emqtt:publish_async(Client, Topic, Payload, [{qos, 1}], fun(_) -> ok end),
    ?assertReceive({oauth2_http, Second, Payload, 200}, 3_000),
    assert_http_metrics(TCConfig, #{<<"success">> => 1}).

t_active_health_check_oauth2_retry_once(TCConfig) ->
    {First, Second, Client, Topic} = setup_oauth2_revocation(
        TCConfig,
        #{
            <<"health_check">> => #{<<"enable">> => true, <<"path">> => <<"/path">>},
            <<"resource_opts">> => #{<<"health_check_interval">> => <<"1m">>}
        },
        #{}
    ),
    set_oauth2_resource_handler(<<"reject-all-tokens">>),
    ResId = emqx_bridge_v2_testlib:connector_resource_id(TCConfig),
    {ok, _, #{state := State}} = emqx_resource:get_instance(ResId),
    ?assertMatch(
        {disconnected, {http_health_check_failed, #{status_code := 401}}},
        emqx_bridge_http_connector:on_get_status(ResId, State)
    ),
    ?assertReceive({oauth2_http, First, <<>>, 401}, 1_000),
    ?assertReceive({oauth2_http, Second, <<>>, 401}, 1_000),
    ?assertReceive({oauth2_token_request, _}, 1_000),
    ?assertNotReceive({oauth2_token_request, _}, 200),
    ?assertNotReceive({oauth2_http, _, _, 401}, 100),
    set_oauth2_resource_handler(Second),
    ?assertEqual({ok, connected}, emqx_resource:health_check(ResId)),
    Payload = unique_payload(),
    emqtt:publish_async(Client, Topic, Payload, [{qos, 1}], fun(_) -> ok end),
    ?assertReceive({oauth2_http, Second, Payload, 200}, 3_000),
    assert_http_metrics(TCConfig, #{<<"success">> => 1}).

t_active_health_check_busy_pool(TCConfig) ->
    TestPid = self(),
    ok = emqx_bridge_http_connector_test_server:set_handler(fun(Req0, State) ->
        {ok, Body, Req1} = cowboy_req:read_body(Req0),
        case cowboy_req:method(Req1) of
            <<"POST">> ->
                TestPid ! {slow_post, Body},
                timer:sleep(2_000);
            _ ->
                ok
        end,
        Req = cowboy_req:reply(200, #{}, <<>>, Req1),
        {ok, Req, State}
    end),
    {201, _} = create_connector_api(TCConfig, #{
        <<"health_check">> => #{<<"enable">> => true, <<"path">> => <<"/path">>},
        <<"resource_opts">> => #{
            <<"health_check_interval">> => <<"1m">>,
            <<"health_check_timeout">> => <<"300ms">>
        }
    }),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{<<"body">> => <<"${payload}">>}
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    Client = start_client(),
    Payload = unique_payload(),
    emqtt:publish_async(Client, Topic, Payload, [{qos, 1}], fun(_) -> ok end),
    ?assertReceive({slow_post, Payload}, 2_000),
    ResId = emqx_bridge_v2_testlib:connector_resource_id(TCConfig),
    ?assertEqual({ok, connected}, emqx_resource:health_check(ResId)),
    assert_http_metrics(TCConfig, #{<<"success">> => 1}).

t_active_health_check_infinite_timeout(TCConfig) ->
    Invalid = #{
        <<"health_check">> => #{<<"enable">> => true, <<"path">> => <<"/path">>},
        <<"resource_opts">> => #{<<"health_check_timeout">> => <<"infinity">>}
    },
    ?assertThrow({_, [_ | _]}, connector_config(Invalid)),
    ?assertMatch({400, _}, create_connector_api(TCConfig, Invalid)),
    {201, _} = create_connector_api(TCConfig, #{
        <<"health_check">> => #{<<"enable">> => true, <<"path">> => <<"/path">>}
    }),
    ?assertMatch({400, _}, update_connector_api(TCConfig, Invalid)),
    ResId = emqx_bridge_v2_testlib:connector_resource_id(TCConfig),
    ?assertEqual({ok, connected}, emqx_resource:health_check(ResId)),
    Disabled = emqx_utils_maps:deep_put([<<"health_check">>, <<"enable">>], Invalid, false),
    ?assertMatch(#{}, connector_config(Disabled)),
    ?assertMatch({200, _}, update_connector_api(TCConfig, Disabled)),
    ?assertEqual({ok, connected}, emqx_resource:health_check(ResId)).

t_active_health_check_cleanup() ->
    [{matrix, true}].
t_active_health_check_cleanup(matrix) ->
    [[Mode] || Mode <- [timeout, body_timeout, owner_killed]];
t_active_health_check_cleanup(TCConfig) when is_list(TCConfig) ->
    Mode = emqx_common_test_helpers:get_matrix_prop(
        TCConfig, [timeout, body_timeout, owner_killed], timeout
    ),
    {201, _} = create_connector_api(TCConfig, #{
        <<"health_check">> => #{<<"enable">> => true, <<"path">> => <<"/path">>},
        <<"resource_opts">> => #{
            <<"health_check_interval">> => <<"1m">>,
            <<"health_check_timeout">> => <<"500ms">>
        }
    }),
    ResId = emqx_bridge_v2_testlib:connector_resource_id(TCConfig),
    {ok, _, #{state := State}} = emqx_resource:get_instance(ResId),
    {_, #{client := BusinessClient}} = ehttpc:get_state(ResId),
    TestPid = self(),
    ok = emqx_bridge_http_connector_test_server:set_handler(fun(Req0, HandlerState) ->
        Req =
            case Mode of
                body_timeout -> cowboy_req:stream_reply(200, #{}, Req0);
                _ -> Req0
            end,
        TestPid ! {hanging_probe, self()},
        receive
            finish -> ok
        after 10_000 -> ok
        end,
        {ok, Req, HandlerState}
    end),
    #{health_check := #{pool := ProbePool}} = State,
    {ProbeWorker, #{client := ProbeClient}} = ehttpc:get_state(ProbePool),
    Caller = spawn(fun() ->
        Result = emqx_bridge_http_connector:on_get_status(ResId, State),
        TestPid ! {probe_result, Result}
    end),
    on_exit(fun() -> exit(Caller, kill) end),
    {hanging_probe, Handler} = ?assertReceive({hanging_probe, _}, 2_000),
    case Mode of
        owner_killed ->
            exit(Caller, kill);
        _ ->
            ?assertReceive(
                {probe_result, {disconnected, {http_health_check_failed, timeout}}}, 1_000
            )
    end,
    Handler ! finish,
    ?retry(50, 40, ?assertMatch(#{requests := #{sent := 0}}, ehttpc:get_state(ProbeWorker))),
    ?assert(is_process_alive(ProbeClient)),
    ?assert(is_process_alive(BusinessClient)),
    ?assertMatch({_, #{client := BusinessClient}}, ehttpc:get_state(ResId)),
    ok = emqx_bridge_http_connector_test_server:set_handler(success_http_handler(#{})),
    ?assertEqual({ok, connected}, emqx_resource:health_check(ResId)).

t_active_health_check_pool_recovery() ->
    [{matrix, true}].
t_active_health_check_pool_recovery(matrix) ->
    [[Fault] || Fault <- [worker, manager]];
t_active_health_check_pool_recovery(TCConfig) when is_list(TCConfig) ->
    Fault = emqx_common_test_helpers:get_matrix_prop(TCConfig, [worker, manager], worker),
    {201, #{<<"status">> := <<"connected">>}} = create_connector_api(TCConfig, #{
        <<"health_check">> => #{<<"enable">> => true, <<"path">> => <<"/path">>},
        <<"resource_opts">> => #{<<"health_check_interval">> => <<"1m">>}
    }),
    ResId = emqx_bridge_v2_testlib:connector_resource_id(TCConfig),
    {ok, _, #{state := #{health_check := #{pool := Pool}}}} = emqx_resource:get_instance(ResId),
    {Worker, #{client := Client}} = ehttpc:get_state(Pool),
    Monitor = monitor(process, Client),
    case Fault of
        worker ->
            {links, Links} = process_info(Worker, links),
            [Supervisor] = Links -- [Client],
            ok = sys:suspend(Supervisor),
            on_exit(fun() -> catch sys:resume(Supervisor) end),
            exit(Worker, kill),
            ?assertReceive({'DOWN', Monitor, process, Client, _}, 2_000),
            ?retry(50, 40, ?assertEqual([], ehttpc:workers(Pool))),
            {ok, _, #{state := State}} = emqx_resource:get_instance(ResId),
            ?assertEqual(
                {connecting, still_connecting},
                emqx_bridge_http_connector:on_get_status(ResId, State)
            ),
            ok = sys:resume(Supervisor);
        manager ->
            exit(emqx_resource_manager:where(ResId), kill),
            ?assertReceive({'DOWN', Monitor, process, Client, _}, 5_000)
    end,
    ?retry(100, 50, ?assertEqual({ok, connected}, emqx_resource:health_check(ResId))),
    {NewWorker, #{client := NewClient}} = ehttpc:get_state(Pool),
    ?assertNotEqual(Worker, NewWorker),
    ?assertNotEqual(Client, NewClient),
    ?assertEqual(ok, emqx_resource:stop(ResId)),
    ?assertEqual({error, not_found}, ehttpc:check_pool_integrity(Pool)).

t_active_health_check_trailers(TCConfig) ->
    TestPid = self(),
    ok = emqx_bridge_http_connector_test_server:set_handler(fun(Req0, State) ->
        {ok, Body, Req1} = cowboy_req:read_body(Req0),
        Req =
            case cowboy_req:method(Req1) of
                <<"GET">> ->
                    ?assertEqual(<<"trailers">>, cowboy_req:header(<<"te">>, Req1)),
                    R = cowboy_req:stream_reply(200, #{<<"trailer">> => <<"x-health">>}, Req1),
                    ok = cowboy_req:stream_body(<<"healthy">>, nofin, R),
                    ok = cowboy_req:stream_trailers(#{<<"x-health">> => <<"ok">>}, R),
                    TestPid ! {health_trailers, cowboy_req:peer(Req1)},
                    R;
                <<"POST">> ->
                    TestPid ! {business_request, Body},
                    cowboy_req:reply(200, #{}, <<>>, Req1)
            end,
        {ok, Req, State}
    end),
    {201, #{<<"status">> := <<"connected">>}} = create_connector_api(TCConfig, #{
        <<"headers">> => #{<<"te">> => <<"trailers">>},
        <<"health_check">> => #{<<"enable">> => true, <<"path">> => <<"/path">>},
        <<"resource_opts">> => #{
            <<"health_check_interval">> => <<"1m">>,
            <<"health_check_timeout">> => <<"500ms">>
        }
    }),
    {health_trailers, Peer} = ?assertReceive({health_trailers, _}),
    ResId = emqx_bridge_v2_testlib:connector_resource_id(TCConfig),
    ?assertEqual({ok, connected}, emqx_resource:health_check(ResId)),
    {health_trailers, NextPeer} = ?assertReceive({health_trailers, _}),
    ?assertEqual(Peer, NextPeer),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{<<"body">> => <<"${payload}">>}
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    Client = start_client(),
    Payload = unique_payload(),
    emqtt:publish_async(Client, Topic, Payload, [{qos, 1}], fun(_) -> ok end),
    ?assertReceive({business_request, Payload}, 5_000),
    assert_http_metrics(TCConfig, #{<<"success">> => 1, <<"failed">> => 0, <<"matched">> => 1}).

t_active_health_check_upgrade(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{
        <<"health_check">> => #{<<"enable">> => true, <<"path">> => <<"/path">>},
        <<"resource_opts">> => #{
            <<"health_check_interval">> => <<"1s">>,
            <<"health_check_timeout">> => <<"500ms">>
        }
    }),
    ok = emqx_bridge_http_connector_test_server:set_handler(fun(Req, State) ->
        Reply = cowboy_req:reply(
            101,
            #{
                <<"connection">> => <<"upgrade">>, <<"upgrade">> => <<"test-protocol">>
            },
            <<>>,
            Req
        ),
        {ok, Reply, State}
    end),
    ResId = emqx_bridge_v2_testlib:connector_resource_id(TCConfig),
    ?assertEqual({ok, disconnected}, emqx_resource:health_check(ResId)),
    {ok, _, #{error := Error}} = emqx_resource:get_instance(ResId),
    ?assertMatch({http_health_check_failed, timeout}, Error),
    ok = emqx_bridge_http_connector_test_server:set_handler(success_http_handler(#{})),
    ?retry(100, 30, ?assertEqual({ok, connected}, emqx_resource:health_check(ResId))).

t_active_health_check_tls_shutdown() ->
    [{matrix, true}].
t_active_health_check_tls_shutdown(matrix) ->
    [[Action] || Action <- [stop, disable, update]];
t_active_health_check_tls_shutdown(TCConfig) when is_list(TCConfig) ->
    Action = emqx_common_test_helpers:get_matrix_prop(TCConfig, [stop, disable, update], stop),
    TestPid = self(),
    Mode = ets:new(tls_health_check, [set, public]),
    ets:insert(Mode, {mode, healthy}),
    {ok, Listen} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, Port} = inet:port(Listen),
    Acceptor = spawn(fun() -> tls_health_accept(Listen, Mode, TestPid) end),
    on_exit(fun() ->
        gen_tcp:close(Listen),
        exit(Acceptor, shutdown),
        ok
    end),
    URL = iolist_to_binary(["https://localhost:", integer_to_list(Port)]),
    {201, #{<<"status">> := <<"connected">>}} = create_connector_api(TCConfig, #{
        <<"url">> => URL,
        <<"ssl">> => #{<<"enable">> => true, <<"verify">> => <<"verify_none">>},
        <<"health_check">> => #{<<"enable">> => true, <<"path">> => <<"/health">>},
        <<"resource_opts">> => #{
            <<"health_check_interval">> => <<"1m">>,
            <<"health_check_timeout">> => <<"5s">>
        }
    }),
    {tls_health_request, ProbeSocket} = ?assertReceive({tls_health_request, _}, 2_000),
    ResId = emqx_bridge_v2_testlib:connector_resource_id(TCConfig),
    {ok, _, #{state := #{health_check := #{pool := ProbePool}}}} = emqx_resource:get_instance(
        ResId
    ),
    {Worker, #{client := OldClient}} = ehttpc:get_state(ProbePool),
    {links, OldLinks} = process_info(Worker, links),
    BaseLinks = OldLinks -- [OldClient],
    ets:insert(Mode, {mode, stall}),
    ok = ssl:close(ProbeSocket),
    ?retry(50, 40, ?assertMatch(#{client := undefined}, ehttpc:get_state(Worker))),
    Caller = spawn(fun() -> TestPid ! {old_probe_result, emqx_resource:health_check(ResId)} end),
    on_exit(fun() -> exit(Caller, kill) end),
    {stalled_tls, Server} = ?assertReceive({stalled_tls, _}, 2_000),
    {links, NewLinks} = process_info(Worker, links),
    [Client] = NewLinks -- BaseLinks,
    Monitor = monitor(process, Client),
    case Action of
        stop ->
            ok = emqx_resource:stop(ResId);
        disable ->
            Name = get_config(connector_name, TCConfig),
            ?assertMatch(
                {204, _},
                emqx_bridge_v2_testlib:simplify_result(
                    emqx_bridge_v2_testlib:disable_connector_api(?CONNECTOR_TYPE, Name)
                )
            );
        update ->
            ?assertMatch(
                {200, #{<<"status">> := <<"connected">>}},
                update_connector_api(TCConfig, #{
                    <<"health_check">> => #{<<"enable">> => true, <<"path">> => <<"/path">>}
                })
            )
    end,
    ?assertReceive({'DOWN', Monitor, process, Client, _}, 1_000),
    ?assertReceive({tls_stall_closed, Server}, 1_000),
    ?assertReceive({old_probe_result, {error, resource_shutting_down}}, 1_000).

tls_health_accept(Listen, Mode, TestPid) ->
    case gen_tcp:accept(Listen) of
        {ok, Socket} ->
            [{mode, CurrentMode}] = ets:lookup(Mode, mode),
            Owner = spawn_link(fun() ->
                receive
                    {socket, S} -> tls_health_serve(S, CurrentMode, TestPid)
                end
            end),
            ok = gen_tcp:controlling_process(Socket, Owner),
            Owner ! {socket, Socket},
            tls_health_accept(Listen, Mode, TestPid);
        {error, _} ->
            ok
    end.

tls_health_serve(Socket, stall, TestPid) ->
    {ok, <<22, _/binary>>} = gen_tcp:recv(Socket, 0, 2_000),
    TestPid ! {stalled_tls, self()},
    tls_health_stall(Socket, TestPid);
tls_health_serve(Socket, healthy, TestPid) ->
    Opts = certs() ++ [{verify, verify_none}, {active, false}, {mode, binary}],
    case ssl:handshake(Socket, Opts, 2_000) of
        {ok, SSL} -> tls_health_requests(SSL, TestPid, <<>>);
        {error, _} -> gen_tcp:close(Socket)
    end.

tls_health_stall(Socket, TestPid) ->
    case gen_tcp:recv(Socket, 0, 10_000) of
        {ok, _} -> tls_health_stall(Socket, TestPid);
        {error, closed} -> TestPid ! {tls_stall_closed, self()};
        {error, _} -> gen_tcp:close(Socket)
    end.

tls_health_requests(Socket, TestPid, Buffer) ->
    case binary:split(Buffer, <<"\r\n\r\n">>) of
        [_, Rest] ->
            TestPid ! {tls_health_request, Socket},
            _ = ssl:send(Socket, <<"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n">>),
            tls_health_requests(Socket, TestPid, Rest);
        [_] ->
            case ssl:recv(Socket, 0, 10_000) of
                {ok, Data} -> tls_health_requests(Socket, TestPid, <<Buffer/binary, Data/binary>>);
                {error, _} -> ssl:close(Socket)
            end
    end.

-doc "Validate the optional probe path and GET/HEAD methods through the connector schema.".
t_active_health_check_schema(_TCConfig) ->
    ?assertNot(
        lists:keymember(
            health_check, 1, emqx_bridge_iotdb_connector:fields("config_restapi")
        )
    ),
    ?assertNot(lists:keymember(health_check, 1, emqx_bridge_es_connector:fields(config))),
    lists:foreach(
        fun(Check) ->
            ?assertMatch(#{}, connector_config(#{<<"health_check">> => Check}))
        end,
        [
            #{<<"enable">> => false},
            #{<<"enable">> => true, <<"path">> => <<"/health?full=1">>},
            #{<<"enable">> => true, <<"method">> => <<"head">>, <<"path">> => <<"/health">>}
        ]
    ),
    InvalidPaths = [
        <<>>,
        <<"health">>,
        <<"//other/health">>,
        <<"http://other/health">>,
        <<"/health#fragment">>,
        <<"/${topic}">>,
        <<"/health?token=${payload}">>,
        <<"/health", 13, 10, "x-injected: value">>
    ],
    InvalidChecks =
        [#{<<"enable">> => true}, #{<<"method">> => <<"post">>}] ++
            [#{<<"enable">> => true, <<"path">> => Path} || Path <- InvalidPaths],
    lists:foreach(
        fun(Check) ->
            ?assertThrow({_, [_ | _]}, connector_config(#{<<"health_check">> => Check}))
        end,
        InvalidChecks
    ).

%% A pooled connection has TCP keepalive on, with the connector's 30 second
%% idle time before the first probe.
t_tcp_keepalive(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    ConnResId = emqx_bridge_v2_testlib:connector_resource_id(TCConfig),
    ?assertMatch({ok, 200, _, _}, ehttpc:request(ConnResId, get, {<<"/path">>, []}, 5_000, 0)),
    {_Worker, #{client := Client}} = ehttpc:get_state(ConnResId),
    #{socket := Socket} = gun:info(Client),
    ?assertEqual({ok, [{keepalive, true}]}, inet:getopts(Socket, [keepalive])),
    case os:type() of
        {unix, linux} ->
            %% TCP_KEEPIDLE
            ?assertEqual(
                {ok, [{raw, 6, 4, <<30:32/native>>}]}, inet:getopts(Socket, [{raw, 6, 4, 4}])
            );
        _ ->
            ok
    end.

t_rule_action(Config) when is_list(Config) ->
    PostPublishFn = fun(Context) ->
        #{payload := Payload} = Context,
        {http, Headers, BodyRaw} = ?assertReceive({http, _, _}),
        ?assertMatch(#{<<"headerk">> := <<"headerv">>}, Headers),
        ?assertMatch(
            #{<<"payload">> := Payload},
            emqx_utils_json:decode(BodyRaw),
            #{payload => Payload}
        )
    end,
    Opts = #{
        post_publish_fn => PostPublishFn
    },
    emqx_bridge_v2_testlib:t_rule_action(Config, Opts).

t_oauth2_client_credentials(TCConfig) ->
    {TokenEndpoint, Token} = start_oauth2_token_server(self()),
    OAuth2 = #{
        <<"enable">> => true,
        <<"grant_type">> => <<"client_credentials">>,
        <<"token_endpoint">> => TokenEndpoint,
        <<"client_id">> => <<"client-id">>,
        <<"client_secret">> => <<"client-secret">>,
        <<"scope">> => <<"read write">>
    },
    {201, #{<<"status">> := <<"connected">>}} =
        create_connector_api(TCConfig, #{<<"oauth2">> => OAuth2}),
    TokenRequestBody = ?assertReceive({oauth2_token_request, _}, 2_000),
    {oauth2_token_request, FormBody} = TokenRequestBody,
    Form = uri_string:dissect_query(FormBody),
    ?assert(lists:member({<<"grant_type">>, <<"client_credentials">>}, Form)),
    ?assert(lists:member({<<"client_id">>, <<"client-id">>}, Form)),
    ?assert(lists:member({<<"client_secret">>, <<"client-secret">>}, Form)),
    ?assert(lists:member({<<"scope">>, <<"read write">>}, Form)),

    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    Client = start_client(),
    lists:foreach(
        fun(Payload) -> emqtt:publish(Client, Topic, Payload, [{qos, 1}]) end,
        [<<"first">>, <<"second">>]
    ),
    lists:foreach(
        fun(_) ->
            {http, Headers, _Body} = ?assertReceive({http, _, _}, 2_000),
            ?assertEqual(<<"Bearer ", Token/binary>>, maps:get(<<"authorization">>, Headers))
        end,
        [first, second]
    ),
    ?assertNotReceive({oauth2_token_request, _}, 200),
    ok.

t_oauth2_ssl_certs_are_saved(TCConfig) ->
    {TokenEndpoint, _Token} = start_oauth2_token_server(self()),
    SSL = inline_ssl_certs(),
    OAuth2 = #{
        <<"enable">> => true,
        <<"grant_type">> => <<"client_credentials">>,
        <<"token_endpoint">> => TokenEndpoint,
        <<"client_id">> => <<"client-id">>,
        <<"client_secret">> => <<"client-secret">>,
        <<"ssl">> => SSL
    },
    {201, #{<<"status">> := <<"connected">>}} =
        create_connector_api(TCConfig, #{<<"oauth2">> => OAuth2}),
    {200, #{<<"oauth2">> := #{<<"ssl">> := SavedSSL}}} =
        get_connector_api(TCConfig),
    #{connector_type := Type, connector_name := Name} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    CertDir = filename:join([emqx:mutable_certs_dir(), connectors, Type, Name, oauth2]),
    ?assertMatch({ok, [_, _, _]}, file:list_dir(CertDir)),
    lists:foreach(
        fun(Key) ->
            SavedPath = maps:get(Key, SavedSSL),
            ?assertNotEqual(maps:get(Key, SSL), SavedPath),
            ?assert(filelib:is_regular(SavedPath))
        end,
        [<<"cacertfile">>, <<"certfile">>, <<"keyfile">>]
    ).

t_oauth2_token_failure_is_recoverable(TCConfig) ->
    {TokenEndpoint, _Token} = start_oauth2_token_server(self()),
    OAuth2 = #{
        <<"enable">> => true,
        <<"grant_type">> => <<"client_credentials">>,
        <<"token_endpoint">> => TokenEndpoint,
        <<"client_id">> => <<"client-id">>,
        <<"client_secret">> => <<"client-secret">>
    },
    {201, #{<<"status">> := <<"connected">>}} =
        create_connector_api(TCConfig, #{<<"oauth2">> => OAuth2}),
    _ = ?assertReceive({oauth2_token_request, _}, 2_000),
    ConnectorResId = emqx_bridge_v2_testlib:connector_resource_id(TCConfig),
    set_oauth2_token_mode(self(), unavailable),
    ok = emqx_connector_oauth2:clear_cache(ConnectorResId),
    ?assertMatch(
        {error, {recoverable_error, {oauth2_token_unavailable, _}}},
        emqx_resource:simple_sync_query(
            ConnectorResId,
            {post, {<<"/path">>, [], <<"body">>}, 1_000}
        )
    ).

t_oauth2_async_retry_uses_latest_token(TCConfig) ->
    TestPid = self(),
    {TokenEndpoint, FirstToken} = start_oauth2_token_server(TestPid),
    OAuth2 = #{
        <<"enable">> => true,
        <<"grant_type">> => <<"client_credentials">>,
        <<"token_endpoint">> => TokenEndpoint,
        <<"client_id">> => <<"client-id">>,
        <<"client_secret">> => <<"client-secret">>
    },
    {201, #{<<"status">> := <<"connected">>}} =
        create_connector_api(TCConfig, #{<<"oauth2">> => OAuth2}),
    _ = ?assertReceive({oauth2_token_request, _}, 2_000),
    {201, _} = create_action_api(TCConfig, #{
        <<"resource_opts">> => #{<<"query_mode">> => <<"async">>}
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    ConnectorResId = emqx_bridge_v2_testlib:connector_resource_id(TCConfig),
    SecondToken = <<"oauth2-access-token-2">>,
    Counter = counters:new(1, []),
    ok = emqx_bridge_http_connector_test_server:set_handler(fun(Req0, State) ->
        {ok, Body, Req1} = cowboy_req:read_body(Req0),
        Headers = cowboy_req:headers(Req1),
        ok = counters:add(Counter, 1, 1),
        N = counters:get(Counter, 1),
        TestPid ! {http, Headers, Body},
        Status =
            case N of
                1 ->
                    set_oauth2_token(TestPid, SecondToken),
                    ok = emqx_connector_oauth2:clear_cache(ConnectorResId),
                    503;
                _ ->
                    200
            end,
        Req = cowboy_req:reply(Status, #{}, <<>>, Req1),
        {ok, Req, State}
    end),
    Client = start_client(),
    emqtt:publish(Client, Topic, <<"retry">>, [{qos, 1}]),
    {http, FirstHeaders, _} = ?assertReceive({http, _, _}, 2_000),
    {http, SecondHeaders, _} = ?assertReceive({http, _, _}, 2_000),
    ?assertEqual(
        <<"Bearer ", FirstToken/binary>>,
        maps:get(<<"authorization">>, FirstHeaders)
    ),
    ?assertEqual(
        <<"Bearer ", SecondToken/binary>>,
        maps:get(<<"authorization">>, SecondHeaders)
    ).

t_oauth2_refresh_on_401() -> [{matrix, true}].
t_oauth2_refresh_on_401(matrix) ->
    [[sync], [async]];
t_oauth2_refresh_on_401(TCConfig) when is_list(TCConfig) ->
    {FirstToken, SecondToken, Client, Topic} = setup_oauth2_revocation(
        TCConfig, #{}, #{<<"resume_interval">> => <<"1m">>}
    ),
    %% Independent actions let both sync and async requests reach the HTTP barrier.
    SecondName = <<(get_config(action_name, TCConfig))/binary, "_second">>,
    SecondConfig = [{action_name, SecondName} | proplists:delete(action_name, TCConfig)],
    {201, _} = create_action_api(SecondConfig, #{
        <<"parameters">> => #{<<"body">> => <<"${payload}">>},
        <<"resource_opts">> => #{
            <<"query_mode">> => atom_to_binary(oauth2_query_mode(TCConfig)),
            <<"resume_interval">> => <<"1m">>
        }
    }),
    #{topic := SecondTopic} = simple_create_rule_api(SecondConfig),
    SecondClient = start_client(),
    Requests = [{Client, Topic, <<"first">>}, {SecondClient, SecondTopic, <<"second">>}],
    BufferWorkers = buffer_oauth2_requests(TCConfig, Requests),
    TestPid = self(),
    ok = emqx_bridge_http_connector_test_server:set_handler(fun(Req0, State) ->
        {ok, Body, Req1} = cowboy_req:read_body(Req0),
        <<"Bearer ", Token/binary>> = cowboy_req:header(<<"authorization">>, Req1),
        Status =
            case Token of
                FirstToken ->
                    #{pid := ConnectionPid, streamid := StreamId} = Req1,
                    TestPid ! {revoked_request, {ConnectionPid, StreamId}, self()},
                    receive
                        continue -> ok
                    end,
                    401;
                SecondToken ->
                    200
            end,
        TestPid ! {oauth2_http, Token, Body, Status},
        {ok, cowboy_req:reply(Status, #{}, <<"response">>, Req1), State}
    end),
    send_oauth2_requests(TCConfig, BufferWorkers, Requests),
    Request1 = ?assertReceive({revoked_request, _, _}, 2_000),
    Request2 = ?assertReceive({revoked_request, _, _}, 2_000),
    %% HTTP/1.1 pipelined responses must follow stream order, regardless of notification order.
    [{revoked_request, _, FirstRequest}, {revoked_request, _, LateRequest}] =
        lists:sort([Request1, Request2]),
    FirstRequest ! continue,
    _ = ?assertReceive({oauth2_token_request, _}, 2_000),
    ConnectorResId = emqx_bridge_v2_testlib:connector_resource_id(TCConfig),
    ?assertEqual({ok, SecondToken}, emqx_connector_oauth2:get_token(ConnectorResId)),
    %% Deliver the second old-token 401 after the new token has been cached.
    LateRequest ! continue,
    lists:foreach(
        fun(Cfg) ->
            wait_oauth2_metrics(Cfg, #{<<"success">> => 1, <<"failed">> => 0, <<"dropped">> => 0})
        end,
        [TCConfig, SecondConfig]
    ),
    lists:foreach(
        fun(Payload) ->
            _ = ?assertReceive({oauth2_http, SecondToken, Payload, 200}, 2_000),
            _ = ?assertReceive({published, {ok, _}}, 2_000)
        end,
        [<<"first">>, <<"second">>]
    ),
    ?assertNotReceive({oauth2_token_request, _}, 200),
    assert_oauth2_query_mode(TCConfig).

t_oauth2_second_401_fails() -> [{matrix, true}].
t_oauth2_second_401_fails(matrix) ->
    [[sync], [async]];
t_oauth2_second_401_fails(TCConfig) when is_list(TCConfig) ->
    {FirstToken, SecondToken, Client, Topic} = setup_oauth2_revocation(
        TCConfig, #{}, #{<<"resume_interval">> => <<"1m">>}
    ),
    Requests = [{Client, Topic, <<"permanent-401">>}],
    BufferWorkers = buffer_oauth2_requests(TCConfig, Requests),
    set_oauth2_resource_handler(<<"never-accepted">>),
    send_oauth2_requests(TCConfig, BufferWorkers, Requests),
    wait_oauth2_metrics(TCConfig, #{<<"success">> => 0, <<"failed">> => 1}),
    _ = ?assertReceive({oauth2_http, FirstToken, _, 401}, 2_000),
    _ = ?assertReceive({oauth2_http, SecondToken, _, 401}, 2_000),
    _ = ?assertReceive({oauth2_token_request, _}, 2_000),
    ?assertNotReceive({oauth2_http, _, _, _}, 200),
    ?assertNotReceive({oauth2_token_request, _}, 200),
    assert_oauth2_query_mode(TCConfig).

t_oauth2_refresh_failure_recovers() -> [{matrix, true}].
t_oauth2_refresh_failure_recovers(matrix) ->
    [[sync], [async]];
t_oauth2_refresh_failure_recovers(TCConfig) when is_list(TCConfig) ->
    ResourceOpts =
        case oauth2_query_mode(TCConfig) of
            sync -> #{<<"resume_interval">> => <<"1m">>};
            async -> #{}
        end,
    {FirstToken, SecondToken, Client, Topic} = setup_oauth2_revocation(
        TCConfig, #{}, ResourceOpts
    ),
    Requests = [{Client, Topic, <<"buffered">>}],
    BufferWorkers = buffer_oauth2_requests(TCConfig, Requests),
    set_oauth2_resource_handler(SecondToken),
    set_oauth2_token_mode(self(), unavailable),
    send_oauth2_requests(TCConfig, BufferWorkers, Requests),
    _ = ?assertReceive({oauth2_http, FirstToken, _, 401}, 2_000),
    _ = ?assertReceive({oauth2_token_request, _}, 2_000),
    ConnectorResId = emqx_bridge_v2_testlib:connector_resource_id(TCConfig),
    ?assertMatch({error, _}, emqx_connector_oauth2:get_token(ConnectorResId)),
    ?assertMatch(
        {disconnected, {oauth2_token_unavailable, _}},
        emqx_bridge_http_connector:on_get_status(ConnectorResId, #{
            pool_name => ConnectorResId, connect_timeout => 1_000, oauth2 => #{enable => true}
        })
    ),
    set_oauth2_token(self(), SecondToken),
    case oauth2_query_mode(TCConfig) of
        sync ->
            %% Retry the buffer until the short token-fetch failure cache expires.
            ?retry(100, 50, begin
                lists:foreach(fun emqx_resource_buffer_worker:resume/1, BufferWorkers),
                {200, #{<<"metrics">> := Metrics}} = get_action_metrics_api(TCConfig),
                ?assertEqual(1, maps:get(<<"success">>, Metrics))
            end);
        async ->
            ok
    end,
    wait_oauth2_metrics(TCConfig, #{<<"success">> => 1, <<"failed">> => 0, <<"dropped">> => 0}),
    _ = ?assertReceive({oauth2_http, SecondToken, _, 200}, 2_000),
    _ = ?assertReceive({published, {ok, _}}, 2_000),
    assert_oauth2_query_mode(TCConfig).

t_oauth2_sync_retry_after_worker_restart(TCConfig) ->
    {FirstToken, SecondToken, Client, Topic} = setup_oauth2_revocation(
        TCConfig,
        #{<<"pool_type">> => <<"hash">>, <<"pool_size">> => 2},
        #{<<"resume_interval">> => <<"1m">>}
    ),
    Payload = <<"restart-during-refresh">>,
    Requests = [{Client, Topic, Payload}],
    BufferWorkers = buffer_oauth2_requests(TCConfig, Requests),
    TestPid = self(),
    set_oauth2_resource_handler(SecondToken, fun(Token, Req) ->
        TestPid ! {oauth2_http_peer, Token, cowboy_req:peer(Req)}
    end),
    ok = emqx_utils_http_test_server:set_handler(fun(Req0, State) ->
        {ok, RequestBody, Req1} = cowboy_req:read_body(Req0),
        TestPid ! {oauth2_token_request, RequestBody},
        TestPid ! {fetch_started, self()},
        receive
            continue -> ok
        end,
        Body = emqx_utils_json:encode(#{access_token => SecondToken, expires_in => 3600}),
        {ok, cowboy_req:reply(200, #{<<"content-type">> => <<"application/json">>}, Body, Req1),
            State}
    end),
    ConnectorId = emqx_bridge_v2_testlib:connector_resource_id(TCConfig),
    ClientId = proplists:get_value(clientid, emqtt:info(Client)),
    HTTPWorker = ehttpc_pool:pick_worker(ConnectorId, ClientId),
    send_oauth2_requests(TCConfig, BufferWorkers, Requests),
    {fetch_started, FetchHandler} = ?assertReceive({fetch_started, _}, 2_000),
    _ = ?assertReceive({oauth2_token_request, _}, 2_000),
    _ = ?assertReceive({oauth2_http, FirstToken, Payload, 401}, 2_000),
    %% Restart the actual pool worker while the synchronous query waits for a token.
    exit(HTTPWorker, kill),
    ?retry(100, 50, begin
        Worker = ehttpc_pool:pick_worker(ConnectorId, ClientId),
        ?assert(is_pid(Worker) andalso Worker =/= HTTPWorker andalso is_process_alive(Worker))
    end),
    NewWorker = ehttpc_pool:pick_worker(ConnectorId, ClientId),
    FetchHandler ! continue,
    wait_oauth2_metrics(TCConfig, #{<<"success">> => 1, <<"failed">> => 0, <<"dropped">> => 0}),
    _ = ?assertReceive({oauth2_http, SecondToken, Payload, 200}, 2_000),
    %% The replacement request must retain the client-ID hash, rather than choose another worker.
    {oauth2_http_peer, SecondToken, {PeerIP, PeerPort}} =
        ?assertReceive({oauth2_http_peer, SecondToken, _}, 2_000),
    #{client := GunClient} = ehttpc:get_state(NewWorker),
    GunInfo = gun:info(GunClient),
    ?assertEqual({PeerIP, PeerPort}, {maps:get(sock_ip, GunInfo), maps:get(sock_port, GunInfo)}),
    assert_oauth2_query_mode(TCConfig),
    ?assertNotReceive({oauth2_token_request, _}, 200).

t_oauth2_retry_budget() ->
    [{matrix, true}, {http_host, "127.0.0.1"}, {http_server_mode, raw}].
t_oauth2_retry_budget(matrix) ->
    [
        [Mode, Scenario]
     || Mode <- [sync, async], Scenario <- [spent_before_401, available_after_401, no_retries]
    ];
t_oauth2_retry_budget(TCConfig) when is_list(TCConfig) ->
    Scenario = emqx_common_test_helpers:get_matrix_prop(
        TCConfig, [spent_before_401, available_after_401, no_retries], spent_before_401
    ),
    {Responses, Retries, Success} =
        case Scenario of
            spent_before_401 -> {[bad_response, unauthorized, bad_response, ok], 1, 0};
            available_after_401 -> {[unauthorized, bad_response, ok], 1, 1};
            no_retries -> {[unauthorized, bad_response, ok], 0, 0}
        end,
    %% The raw socket server sends malformed responses to exercise actual transport retries.
    Counter = atomics:new(2, []),
    TestPid = self(),
    ok = emqx_bridge_http_connector_test_server:set_raw_handler(fun(Body) ->
        Response =
            case atomics:get(Counter, 1) of
                0 ->
                    service_unavailable;
                1 ->
                    N = atomics:add_get(Counter, 2, 1),
                    TestPid ! {oauth2_budget_request, N, Body},
                    lists:nth(min(N, length(Responses)), Responses)
            end,
        oauth2_budget_response(Response)
    end),
    {_, _, Client, Topic} = setup_oauth2_revocation(
        TCConfig,
        #{},
        #{<<"resume_interval">> => <<"1m">>},
        #{<<"max_retries">> => Retries}
    ),
    Payload = <<"retry-budget">>,
    Requests = [{Client, Topic, Payload}],
    BufferWorkers = buffer_oauth2_requests(TCConfig, Requests),
    atomics:put(Counter, 1, 1),
    send_oauth2_requests(TCConfig, BufferWorkers, Requests),
    wait_oauth2_metrics(TCConfig, #{
        <<"success">> => Success, <<"failed">> => 1 - Success, <<"dropped">> => 0
    }),
    %% One initial request, the configured transport retries, and one free OAuth2 retry.
    ExpectedRequests = Retries + 2,
    ?assertEqual(ExpectedRequests, atomics:get(Counter, 2)),
    lists:foreach(
        fun(N) -> _ = ?assertReceive({oauth2_budget_request, N, Payload}, 2_000) end,
        lists:seq(1, ExpectedRequests)
    ),
    _ = ?assertReceive({oauth2_token_request, _}, 2_000),
    ?assertNotReceive({oauth2_token_request, _}, 200),
    ?assertNotReceive({oauth2_budget_request, _, _}, 200),
    assert_oauth2_query_mode(TCConfig).

oauth2_budget_response(bad_response) ->
    <<"HTTP/1.1 invalid\r\n\r\n">>;
oauth2_budget_response(unauthorized) ->
    <<"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n">>;
oauth2_budget_response(service_unavailable) ->
    <<"HTTP/1.1 503 Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n">>;
oauth2_budget_response(ok) ->
    <<"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n">>.

buffer_oauth2_requests(TCConfig, Requests) ->
    case oauth2_query_mode(TCConfig) of
        async ->
            [];
        sync ->
            %% Initial MQTT submissions use async callbacks. A buffered retry forces on_query.
            ok = emqx_bridge_http_connector_test_server:set_handler(fun(Req0, State) ->
                {ok, _, Req1} = cowboy_req:read_body(Req0),
                {ok, cowboy_req:reply(503, #{}, <<>>, Req1), State}
            end),
            TestPid = self(),
            lists:map(
                fun({Client, Topic, Payload}) ->
                    {_, {ok, #{buffer_worker := Worker}}} = ?wait_async_action(
                        emqtt:publish_async(Client, Topic, Payload, [{qos, 1}], fun(Result) ->
                            TestPid ! {published, Result}
                        end),
                        #{?snk_kind := buffer_worker_enter_blocked},
                        10_000
                    ),
                    Worker
                end,
                Requests
            )
    end.

send_oauth2_requests(TCConfig, BufferWorkers, Requests) ->
    case oauth2_query_mode(TCConfig) of
        sync ->
            lists:foreach(fun emqx_resource_buffer_worker:resume/1, BufferWorkers);
        async ->
            TestPid = self(),
            lists:foreach(
                fun({Client, Topic, Payload}) ->
                    spawn(fun() ->
                        TestPid ! {published, emqtt:publish(Client, Topic, Payload, [{qos, 2}])}
                    end)
                end,
                Requests
            )
    end.

assert_oauth2_query_mode(TCConfig) ->
    ConnectorId = emqx_bridge_v2_testlib:connector_resource_id(TCConfig),
    QueryMode = oauth2_query_mode(TCConfig),
    Trace = snabbkaffe:collect_trace(),
    ?assert(
        lists:any(
            fun(#{connector := Id, query_mode := Mode}) ->
                Id =:= ConnectorId andalso Mode =:= QueryMode
            end,
            ?of_kind(http_connector_request, Trace)
        )
    ).

setup_oauth2_revocation(TCConfig) ->
    setup_oauth2_revocation(TCConfig, #{}, #{}).

setup_oauth2_revocation(TCConfig, ConnectorOverrides, ResourceOptsOverrides) ->
    setup_oauth2_revocation(TCConfig, ConnectorOverrides, ResourceOptsOverrides, #{}).

setup_oauth2_revocation(TCConfig, ConnectorOverrides, ResourceOptsOverrides, Parameters) ->
    {TokenEndpoint, FirstToken} = start_oauth2_token_server(self()),
    OAuth2 = #{
        <<"enable">> => true,
        <<"token_endpoint">> => TokenEndpoint,
        <<"client_id">> => <<"client-id">>,
        <<"client_secret">> => <<"client-secret">>
    },
    {201, #{<<"status">> := <<"connected">>}} = create_connector_api(
        TCConfig, ConnectorOverrides#{<<"oauth2">> => OAuth2}
    ),
    _ = ?assertReceive({oauth2_token_request, _}, 2_000),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => Parameters#{<<"body">> => <<"${payload}">>},
        <<"resource_opts">> => maps:merge(
            #{
                <<"query_mode">> => atom_to_binary(oauth2_query_mode(TCConfig)),
                <<"resume_interval">> => <<"100ms">>,
                <<"metrics_flush_interval">> => <<"100ms">>
            },
            ResourceOptsOverrides
        )
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    SecondToken = <<"new-access-token">>,
    set_oauth2_token(self(), SecondToken),
    set_oauth2_resource_handler(SecondToken),
    {FirstToken, SecondToken, start_client(), Topic}.

oauth2_query_mode(TCConfig) ->
    emqx_common_test_helpers:get_matrix_prop(TCConfig, [sync, async], sync).

set_oauth2_resource_handler(AcceptedToken) ->
    set_oauth2_resource_handler(AcceptedToken, fun(_Token, _Req) -> ok end).

set_oauth2_resource_handler(AcceptedToken, ObserveRequest) ->
    TestPid = self(),
    emqx_bridge_http_connector_test_server:set_handler(fun(Req0, State) ->
        {ok, Body, Req1} = cowboy_req:read_body(Req0),
        <<"Bearer ", Token/binary>> = cowboy_req:header(<<"authorization">>, Req1),
        Status =
            case Token =:= AcceptedToken of
                true -> 200;
                false -> 401
            end,
        TestPid ! {oauth2_http, Token, Body, Status},
        ObserveRequest(Token, Req1),
        %% Exercise both response forms handled by ehttpc (with and without body).
        ResponseBody =
            case Body of
                <<"permanent-401">> -> <<>>;
                _ -> <<"response">>
            end,
        {ok, cowboy_req:reply(Status, #{}, ResponseBody, Req1), State}
    end).

wait_oauth2_metrics(TCConfig, Expected) ->
    ?retry(100, 100, begin
        {200, #{<<"metrics">> := Metrics}} = get_action_metrics_api(TCConfig),
        ?assertEqual(Expected, maps:with(maps:keys(Expected), Metrics))
    end).

t_oauth2_rejects_action_authorization_header(TCConfig) ->
    {TokenEndpoint, _Token} = start_oauth2_token_server(self()),
    OAuth2 = #{
        <<"enable">> => true,
        <<"grant_type">> => <<"client_credentials">>,
        <<"token_endpoint">> => TokenEndpoint,
        <<"client_id">> => <<"client-id">>,
        <<"client_secret">> => <<"client-secret">>
    },
    {201, #{<<"status">> := <<"connected">>}} =
        create_connector_api(TCConfig, #{<<"oauth2">> => OAuth2}),
    {400, #{
        <<"message">> := #{
            <<"kind">> := <<"validation_error">>,
            <<"reason">> := Reason
        }
    }} =
        create_action_api(TCConfig, #{
            <<"parameters">> => #{
                <<"headers">> => #{<<"Authorization">> => <<"Basic credentials">>}
            }
        }),
    ?assertMatch(
        {match, _},
        re:run(Reason, <<"authorization.*conflicts with OAuth2">>, [caseless])
    ),
    ?assertMatch({404, _}, get_action_api(TCConfig)).

t_oauth2_rejects_action_authorization_header_at_runtime(_TCConfig) ->
    State = #{oauth2 => #{enable => true}, installed_actions => #{}},
    ActionConfig = #{
        parameters => #{headers => #{<<"Authorization">> => <<"Basic credentials">>}}
    },
    ?assertMatch(
        {error, #{reason := oauth2_auth_header_conflict}},
        emqx_bridge_http_connector:on_add_channel(
            <<"connector">>, State, <<"action">>, ActionConfig
        )
    ).

inline_ssl_certs() ->
    maps:from_list(
        [
            begin
                {ok, Content} = file:read_file(Path),
                {atom_to_binary(Key), Content}
            end
         || {Key, Path} <- certs()
        ] ++
            [
                {<<"enable">>, true},
                {<<"verify">>, <<"verify_peer">>}
            ]
    ).

%% This test ensures that https://emqx.atlassian.net/browse/CI-62 is fixed.
%% When the connection time out all the queued requests where dropped in
t_send_async_connection_timeout(TCConfig) ->
    {201, #{<<"status">> := <<"connected">>}} = create_connector_api(TCConfig, #{}),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{
        <<"parameters">> => #{<<"body">> => <<"${.payload}">>},
        <<"resource_opts">> => #{<<"query_mode">> => <<"async">>}
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    NumberOfMessagesToSend = 10,
    lists:foreach(
        fun(N) ->
            emqtt:publish(C, Topic, integer_to_binary(N), [{qos, 0}])
        end,
        lists:seq(1, NumberOfMessagesToSend)
    ),
    %% Make sure server receives all messages
    ct:pal("Sent messages\n"),
    MessageIDs = maps:from_keys(lists:seq(1, NumberOfMessagesToSend), void),
    receive_request_notifications(MessageIDs, []),
    ok.

t_send_get_trace_messages(TCConfig) ->
    {201, #{<<"status">> := <<"connected">>}} = create_connector_api(TCConfig, #{}),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{
        <<"parameters">> => #{<<"body">> => <<"${.payload.id}">>}
    }),
    #{id := RuleId, topic := Topic} = simple_create_rule_api(TCConfig),
    {200, #{<<"name">> := TraceName}} = emqx_bridge_v2_testlib:start_rule_test_trace(RuleId, #{}),
    on_exit(fun() -> emqx_bridge_v2_testlib:stop_rule_test_trace(TraceName) end),
    ?assertMatch({200, #{<<"items">> := <<"">>}}, trace_log_stream_api(TraceName, #{})),
    C = start_client(),
    emqtt:publish(C, Topic, <<"hello">>, [{qos, 1}]),
    ?retry(
        200,
        10,
        ?assertMatch(
            #{
                counters := #{
                    'matched' := 1,
                    'actions.failed' := 0,
                    'actions.failed.unknown' := 0,
                    'actions.success' := 1,
                    'actions.total' := 1
                }
            },
            emqx_bridge_v2_testlib:get_rule_metrics(RuleId)
        )
    ),
    emqx_trace:check(),
    {200, #{<<"items">> := Bin}} = trace_log_stream_api(TraceName, #{
        query_params => #{
            <<"bytes">> => integer_to_binary(1 bsl 20)
        }
    }),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"rule_activated">>])),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"SQL_yielded_result">>])),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"bridge_action">>])),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"action_template_rendered">>])),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"QUERY_ASYNC">>])),
    ok.

t_async_free_retries(TCConfig) ->
    {201, #{<<"status">> := <<"connected">>}} = create_connector_api(TCConfig, #{}),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{}),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    Publish = fun() -> emqtt:publish(C, Topic, <<"hello">>, [{qos, 1}]) end,
    %% Fail 5 times then succeed.
    Context = #{error_attempts => 5},
    ExpectedAttempts = 6,
    Fn = fun(Get, Error) ->
        {_, {ok, _}} =
            ?wait_async_action(
                Publish(),
                #{?snk_kind := buffer_worker_flush_ack},
                20_000
            ),
        ?assertEqual(ExpectedAttempts, Get(), #{error => Error})
    end,
    do_t_async_retries(?FUNCTION_NAME, Context, {error, normal}, Fn),
    do_t_async_retries(?FUNCTION_NAME, Context, {error, {shutdown, normal}}, Fn),
    ok.

t_async_common_retries(TCConfig) ->
    {201, #{<<"status">> := <<"connected">>}} = create_connector_api(TCConfig, #{}),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{}),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    Publish = fun() -> emqtt:publish(C, Topic, <<"hello">>, [{qos, 1}]) end,
    %% Keeps failing until connector gives up.
    Context = #{error_attempts => infinity},
    ExpectedAttempts = 3,
    FnSucceed = fun(Get, Error) ->
        {_, {ok, _}} =
            ?wait_async_action(
                Publish(),
                #{?snk_kind := buffer_worker_flush_ack},
                20_000
            ),
        ?assertEqual(ExpectedAttempts, Get(), #{error => Error})
    end,
    FnFail = fun(Get, Error) ->
        {_, {ok, _}} =
            ?wait_async_action(
                Publish(),
                #{?snk_kind := buffer_worker_flush_ack},
                20_000
            ),
        ?assertEqual(ExpectedAttempts, Get(), #{error => Error})
    end,
    %% These two succeed because they're further retried by the buffer
    %% worker synchronously, and we're not mock that call.
    do_t_async_retries(
        ?FUNCTION_NAME, Context, {error, {closed, "The connection was lost."}}, FnSucceed
    ),
    do_t_async_retries(?FUNCTION_NAME, Context, {error, {shutdown, closed}}, FnSucceed),
    %% This fails because this error is treated as unrecoverable.
    do_t_async_retries(?FUNCTION_NAME, Context, {error, something_else}, FnFail),
    ok.

do_t_async_retries(TestCase, TestContext, Error, Fn) ->
    #{error_attempts := ErrorAttempts} = TestContext,
    PTKey = {?MODULE, TestCase, attempts},
    persistent_term:put(PTKey, 0),
    on_exit(fun() -> persistent_term:erase(PTKey) end),
    Get = fun() -> persistent_term:get(PTKey) end,
    GetAndBump = fun() ->
        Attempts = persistent_term:get(PTKey),
        persistent_term:put(PTKey, Attempts + 1),
        Attempts + 1
    end,
    emqx_common_test_helpers:with_mock(
        emqx_bridge_http_connector,
        reply_delegator,
        fun(Context, ReplyFunAndArgs, Result) ->
            Attempts = GetAndBump(),
            case Attempts > ErrorAttempts of
                true ->
                    ct:pal("succeeding ~p : ~p", [Error, Attempts]),
                    meck:passthrough([Context, ReplyFunAndArgs, Result]);
                false ->
                    ct:pal("failing ~p : ~p", [Error, Attempts]),
                    meck:passthrough([Context, ReplyFunAndArgs, Error])
            end
        end,
        fun() -> Fn(Get, Error) end
    ),
    persistent_term:erase(PTKey),
    ok.

t_bridge_probes_header_atoms(TCConfig) ->
    ?check_trace(
        begin
            {201, #{<<"status">> := <<"connected">>}} = create_connector_api(TCConfig, #{}),
            Overrides = #{
                <<"parameters">> => #{
                    <<"headers">> => #{<<"some-non-existent-atom">> => <<"x">>}
                }
            },
            ?assertMatch({204, _}, probe_action_api(TCConfig, Overrides)),
            ?assertMatch(
                {201, #{<<"status">> := <<"connected">>}},
                create_action_api(TCConfig, Overrides)
            ),
            #{topic := Topic} = simple_create_rule_api(TCConfig),
            C = start_client(),
            emqtt:publish(C, Topic, <<"hi">>, [{qos, 1}]),
            receive
                {http, Headers, _Body} ->
                    ?assertMatch(#{<<"some-non-existent-atom">> := <<"x">>}, Headers),
                    ok
            after 5_000 ->
                ct:pal("mailbox: ~p", [process_info(self(), messages)]),
                ct:fail("request not made")
            end,
            ok
        end,
        []
    ),
    ok.

t_rule_action_expired(TCConfig) ->
    ?check_trace(
        begin
            {201, _} = create_connector_api(TCConfig, #{
                <<"url">> => <<"http://non.existent.host:9999">>
            }),
            {201, _} = create_action_api(TCConfig, #{
                <<"resource_opts">> => #{
                    <<"query_mode">> => <<"async">>,
                    <<"request_ttl">> => <<"100ms">>,
                    <<"metrics_flush_interval">> => <<"300ms">>
                }
            }),
            #{id := RuleId, topic := Topic} = simple_create_rule_api(TCConfig),
            C = start_client(),
            emqtt:publish(C, Topic, <<"timeout">>, [{qos, 1}]),
            ?retry(
                _Interval = 500,
                _NAttempts = 20,
                ?assertMatch(
                    {200, #{
                        <<"metrics">> := #{
                            <<"matched">> := 1,
                            <<"failed">> := 0,
                            <<"dropped">> := 1
                        }
                    }},
                    get_action_metrics_api(TCConfig)
                )
            ),
            ?retry(
                _Interval = 500,
                _NAttempts = 20,
                ?assertMatch(
                    #{
                        counters := #{
                            matched := 1,
                            'actions.failed' := 1,
                            'actions.failed.unknown' := 1,
                            'actions.total' := 1
                        }
                    },
                    emqx_bridge_v2_testlib:get_rule_metrics(RuleId)
                )
            ),
            ok
        end,
        []
    ),
    ok.

t_too_many_requests(TCConfig) ->
    ok = emqx_bridge_http_connector_test_server:set_handler(too_many_requests_http_handler()),
    check_send_is_retried(TCConfig).

t_service_unavailable(Config) ->
    ok = emqx_bridge_http_connector_test_server:set_handler(service_unavailable_http_handler()),
    check_send_is_retried(Config).

t_gateway_error() ->
    [{matrix, true}].
t_gateway_error(matrix) ->
    [[Mode, Status] || Mode <- [sync, async], Status <- [bad_gateway, gateway_timeout]];
t_gateway_error(TCConfig) when is_list(TCConfig) ->
    Status =
        case
            emqx_common_test_helpers:get_matrix_prop(
                TCConfig, [bad_gateway, gateway_timeout], bad_gateway
            )
        of
            bad_gateway -> 502;
            gateway_timeout -> 504
        end,
    ok = emqx_bridge_http_connector_test_server:set_handler(fail_then_success_http_handler(Status)),
    check_send_is_retried(TCConfig, http_query_mode(TCConfig)).

t_nxdomain() ->
    [{matrix, true}, {http_host, "http-retry.test"}, {dns_control, true}].
t_nxdomain(matrix) ->
    [[Mode, Outcome] || Mode <- [sync, async], Outcome <- [recover, expire]];
t_nxdomain(TCConfig) when is_list(TCConfig) ->
    QueryMode = http_query_mode(TCConfig),
    Outcome = emqx_common_test_helpers:get_matrix_prop(TCConfig, [recover, expire], recover),
    {201, #{<<"status">> := <<"connected">>}} = create_connector_api(TCConfig, #{
        <<"resource_opts">> => #{<<"health_check_interval">> => <<"1m">>}
    }),
    TTL =
        case Outcome of
            recover -> <<"15s">>;
            expire -> <<"1s">>
        end,
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{<<"body">> => <<"${payload}">>},
        <<"resource_opts">> => #{
            <<"query_mode">> => atom_to_binary(QueryMode),
            <<"request_ttl">> => TTL,
            <<"resume_interval">> => <<"200ms">>,
            <<"metrics_flush_interval">> => <<"100ms">>,
            <<"health_check_interval">> => <<"1m">>
        }
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    Client = start_client(),
    ok = emqx_bridge_http_connector_test_server:set_dns_available(false),
    Payload = unique_payload(),
    {_, {ok, _}} = ?wait_async_action(
        emqtt:publish_async(Client, Topic, Payload, [{qos, 1}], fun(_) -> ok end),
        #{result := {error, {recoverable_error, nxdomain}}},
        5_000
    ),
    case Outcome of
        recover ->
            ok = emqx_bridge_http_connector_test_server:set_dns_available(true),
            ?assertReceive({http, _, Payload}, 5_000),
            assert_http_metrics(TCConfig, #{
                <<"success">> => 1, <<"failed">> => 0, <<"dropped">> => 0
            });
        expire ->
            assert_http_metrics(TCConfig, #{
                <<"success">> => 0, <<"failed">> => 0, <<"dropped.expired">> => 1
            }),
            ok = emqx_bridge_http_connector_test_server:set_dns_available(true),
            ?assertNotReceive({http, _, Payload}, 500)
    end.

t_max_retries() ->
    [{matrix, true}, {http_host, "127.0.0.1"}, {http_server_mode, close_after_request}].
t_max_retries(matrix) ->
    [
        [Mode, Retries]
     || Mode <- [sync, async], Retries <- [no_retries, default_retries, more_retries]
    ];
t_max_retries(TCConfig) when is_list(TCConfig) ->
    RetryConfig = emqx_common_test_helpers:get_matrix_prop(
        TCConfig, [no_retries, default_retries, more_retries], default_retries
    ),
    {Params, Retries} =
        case RetryConfig of
            no_retries -> {#{<<"max_retries">> => 0}, 0};
            default_retries -> {#{}, 2};
            more_retries -> {#{<<"max_retries">> => 5}, 5}
        end,
    {201, _} = create_connector_api(TCConfig, #{
        <<"resource_opts">> => #{<<"health_check_interval">> => <<"1m">>}
    }),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => Params#{<<"body">> => <<"${payload}">>},
        <<"resource_opts">> => #{
            <<"query_mode">> => atom_to_binary(http_query_mode(TCConfig)),
            %% Keep automatic buffer retries outside the request-counting window.
            <<"resume_interval">> => <<"1m">>,
            <<"request_ttl">> => <<"1m">>,
            <<"health_check_interval">> => <<"1m">>
        }
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    Client = start_client(),
    Payload = unique_payload(),
    QueryMode = http_query_mode(TCConfig),
    QueryResult = exercise_http_retry(QueryMode, Client, Topic, Payload),
    ?assertReceive({http_attempt, Payload}, 5_000),
    Attempts = count_http_attempts(Payload, 1),
    %% A retry can fail while the previous connection is still closing, before reaching the server.
    ?assert(Attempts =< Retries + 1, #{attempts => Attempts, query_result => QueryResult}),
    ConnectorId = emqx_bridge_v2_testlib:connector_resource_id(TCConfig),
    assert_http_retry_budget(QueryMode, ConnectorId, Retries, snabbkaffe:collect_trace()).

exercise_http_retry(sync, Client, Topic, Payload) ->
    %% Initial submissions use async callbacks even in sync query mode. Buffer retries force sync.
    {_, {ok, #{buffer_worker := Worker}}} = ?wait_async_action(
        emqtt:publish_async(Client, Topic, Payload, [{qos, 1}], fun(_) -> ok end),
        #{?snk_kind := buffer_worker_enter_blocked},
        10_000
    ),
    ?assertReceive({http_attempt, Payload}, 5_000),
    _ = count_http_attempts(Payload, 1),
    {_, {ok, Result}} = ?wait_async_action(
        emqx_resource_buffer_worker:resume(Worker),
        #{?snk_kind := buffer_worker_retry_inflight_failed, result := {error, _}},
        10_000
    ),
    Result;
exercise_http_retry(async, Client, Topic, Payload) ->
    {_, {ok, Result}} = ?wait_async_action(
        emqtt:publish_async(Client, Topic, Payload, [{qos, 1}], fun(_) -> ok end),
        #{?snk_kind := handle_async_reply, result := {error, _}},
        10_000
    ),
    Result.

assert_http_retry_budget(sync, ConnectorId, Retries, Trace) ->
    Attempts = [
        {N, Max}
     || #{query_mode := sync, connector := Id, attempt := N, max_attempts := Max} <-
            ?of_kind(http_connector_request, Trace),
        Id =:= ConnectorId
    ],
    ?assertEqual([{1, Retries + 1}], Attempts);
assert_http_retry_budget(async, ConnectorId, Retries, Trace) ->
    Events = ?of_kind(http_connector_request, Trace) ++ ?of_kind(http_will_retry_async, Trace),
    Attempts = [
        {N, Max}
     || #{connector := Id, attempt := N, max_attempts := Max} <- Events,
        Id =:= ConnectorId
    ],
    %% Free retries repeat an attempt without consuming the budget.
    Expected = [{N, Retries + 1} || N <- lists:seq(1, Retries + 1)],
    ?assertEqual(Expected, lists:usort(Attempts)).

count_http_attempts(Payload, Count) ->
    receive
        {http_attempt, Payload} -> count_http_attempts(Payload, Count + 1)
    after 500 ->
        Count
    end.

http_query_mode(TCConfig) ->
    emqx_common_test_helpers:get_matrix_prop(TCConfig, [sync, async], sync).

assert_http_metrics(TCConfig, Expected) ->
    ?retry(100, 100, begin
        {200, #{<<"metrics">> := Metrics}} = get_action_metrics_api(TCConfig),
        ?assertEqual(Expected, maps:with(maps:keys(Expected), Metrics))
    end).

t_http_response_retries(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    ResourceId = emqx_bridge_v2_testlib:connector_resource_id(TCConfig),
    {ok, _, #{state := State0}} = emqx_resource:get_instance(ResourceId),
    Path = <<"/path">>,
    logger:set_process_metadata(#{}),
    TestPid = self(),
    lists:foreach(
        fun({QueryMode, MaxRetries, Status, Body}) ->
            Requests = counters:new(1, []),
            ok = emqx_bridge_http_connector_test_server:set_handler(fun(Req0, ServerState) ->
                counters:add(Requests, 1, 1),
                {ok, _, Req} = cowboy_req:read_body(Req0),
                {ok, cowboy_req:reply(Status, #{}, Body, Req), ServerState}
            end),
            Context = #{
                query_mode => QueryMode, max_retries => MaxRetries, status => Status, body => Body
            },
            State = State0#{max_attempts => MaxRetries + 1},
            Request = {Path, [], <<>>},
            Result =
                case QueryMode of
                    sync ->
                        emqx_bridge_http_connector:on_query(
                            ResourceId,
                            {undefined, undefined, post, Request, 5000, MaxRetries},
                            State
                        );
                    async ->
                        Ref = make_ref(),
                        Reply = {fun(R) -> TestPid ! {Ref, R} end, []},
                        {ok, _} = emqx_bridge_http_connector:on_query_async(
                            ResourceId, {undefined, undefined, post, Request, 5000}, Reply, State
                        ),
                        receive
                            {Ref, R} -> R
                        after 5000 ->
                            ct:fail(http_response_timeout)
                        end
                end,
            ErrorType =
                case Status of
                    429 -> recoverable_error;
                    503 -> recoverable_error;
                    _ -> unrecoverable_error
                end,
            ?assertMatch({error, {ErrorType, #{status_code := Status}}}, Result, Context),
            ?assertEqual(1, counters:get(Requests, 1), Context)
        end,
        [
            {QueryMode, MaxRetries, Status, Body}
         || QueryMode <- [sync, async],
            MaxRetries <- [5, 2, 0],
            Status <- [400, 429, 503],
            Body <- [<<>>, <<"retry later">>]
        ]
    ),
    ok.

t_http_response_expired(TCConfig) ->
    lists:foreach(
        fun(Status) ->
            Requests = counters:new(1, []),
            ok = emqx_bridge_http_connector_test_server:set_handler(fun(Req0, ServerState) ->
                counters:add(Requests, 1, 1),
                {ok, _, Req} = cowboy_req:read_body(Req0),
                {ok, cowboy_req:reply(Status, #{}, <<>>, Req), ServerState}
            end),
            {201, _} = create_connector_api(TCConfig, #{
                <<"resource_opts">> => #{<<"health_check_interval">> => <<"1m">>}
            }),
            {201, _} = create_action_api(TCConfig, #{
                <<"resource_opts">> => #{
                    <<"query_mode">> => <<"async">>,
                    <<"metrics_flush_interval">> => <<"300ms">>,
                    <<"health_check_interval">> => <<"1m">>,
                    <<"resume_interval">> => <<"2s">>,
                    <<"request_ttl">> => <<"1s">>
                }
            }),
            #{topic := Topic} = simple_create_rule_api(TCConfig),
            C = start_client(),
            emqtt:publish(C, Topic, <<"expire">>, [{qos, 1}]),
            ?retry(
                _Interval = 500,
                _NAttempts = 20,
                ?assertMatch(
                    {200, #{
                        <<"metrics">> := #{
                            <<"matched">> := 1,
                            <<"dropped">> := 1,
                            <<"dropped.expired">> := 1,
                            <<"failed">> := 0,
                            <<"success">> := 0,
                            <<"late_reply">> := 0
                        }
                    }},
                    get_action_metrics_api(TCConfig)
                )
            ),
            ?assertEqual(1, counters:get(Requests, 1), #{status => Status}),
            ok = emqtt:stop(C),
            emqx_bridge_v2_testlib:delete_all_rules(),
            emqx_bridge_v2_testlib:delete_all_bridges_and_connectors()
        end,
        [429, 503]
    ),
    ok.

check_send_is_retried(TCConfig) ->
    check_send_is_retried(TCConfig, async).

check_send_is_retried(TCConfig, QueryMode) ->
    ?check_trace(
        begin
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, _} = create_action_api(TCConfig, #{
                <<"resource_opts">> => #{
                    <<"query_mode">> => atom_to_binary(QueryMode),
                    <<"metrics_flush_interval">> => <<"300ms">>,
                    <<"resume_interval">> => <<"200ms">>,
                    <<"request_ttl">> => <<"15s">>
                }
            }),
            #{topic := Topic} = simple_create_rule_api(TCConfig),
            C = start_client(),
            emqtt:publish(C, Topic, <<"timeout">>, [{qos, 1}]),
            %% should retry
            ?assertReceive({http, _, _}),
            ?assertReceive({http, _, _}),
            ?assertReceive({http, _, _}),
            ?retry(
                _Interval = 500,
                _NAttempts = 20,
                ?assertMatch(
                    {200, #{
                        <<"metrics">> := #{
                            <<"matched">> := 1,
                            <<"failed">> := 0,
                            <<"dropped">> := 0,
                            <<"success">> := 1
                        }
                    }},
                    get_action_metrics_api(TCConfig)
                )
            ),
            ok
        end,
        fun(Trace) ->
            %% The connector no longer retries a recoverable response itself.
            %% The buffer worker owns the retry.
            ?assertEqual([], ?of_kind(http_will_retry_async, Trace)),
            case QueryMode of
                async ->
                    ?assertMatch([_ | _], ?of_kind(buffer_worker_retry_inflight_succeeded, Trace));
                sync ->
                    ok
            end,
            ok
        end
    ),
    ok.

t_empty_path() ->
    [{http_path, <<"/">>}].
t_empty_path(TCConfig) ->
    ?check_trace(
        begin
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, _} = create_action_api(TCConfig, #{
                <<"parameters">> => #{<<"path">> => <<"">>},
                <<"resource_opts">> => #{
                    <<"query_mode">> => <<"async">>,
                    <<"metrics_flush_interval">> => <<"300ms">>,
                    <<"resume_interval">> => <<"200ms">>,
                    <<"request_ttl">> => <<"infinity">>
                }
            }),
            #{topic := Topic} = simple_create_rule_api(TCConfig),
            C = start_client(),
            emqtt:publish(C, Topic, <<"timeout">>, [{qos, 1}]),
            ?assertReceive({http, _, _}),
            ?retry(
                _Interval = 500,
                _NAttempts = 20,
                ?assertMatch(
                    {200, #{
                        <<"metrics">> := #{
                            <<"matched">> := 1,
                            <<"failed">> := 0,
                            <<"dropped">> := 0,
                            <<"success">> := 1
                        }
                    }},
                    get_action_metrics_api(TCConfig)
                )
            ),
            ok
        end,
        fun(Trace) ->
            ?assertEqual([], ?of_kind(http_will_retry_async, Trace)),
            ok
        end
    ),
    ok.

t_path_not_found(TCConfig) ->
    ok = emqx_bridge_http_connector_test_server:set_handler(not_found_http_handler()),
    ?check_trace(
        begin
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, _} = create_action_api(TCConfig, #{
                <<"resource_opts">> => #{
                    <<"query_mode">> => <<"async">>,
                    <<"metrics_flush_interval">> => <<"300ms">>,
                    <<"resume_interval">> => <<"200ms">>,
                    <<"request_ttl">> => <<"infinity">>
                }
            }),
            #{topic := Topic} = simple_create_rule_api(TCConfig),
            C = start_client(),
            emqtt:publish(C, Topic, <<"timeout">>, [{qos, 1}]),
            ?assertReceive({http, _, _}),
            ?retry(
                _Interval = 500,
                _NAttempts = 20,
                ?assertMatch(
                    {200, #{
                        <<"metrics">> := #{
                            <<"matched">> := 1,
                            <<"failed">> := 1,
                            <<"dropped">> := 0,
                            <<"success">> := 0
                        }
                    }},
                    get_action_metrics_api(TCConfig)
                )
            ),
            ok
        end,
        fun(Trace) ->
            ?assertEqual([], ?of_kind(http_will_retry_async, Trace)),
            ok
        end
    ),
    ok.

t_bad_bridge_config(TCConfig) ->
    Port = emqx_common_test_helpers:select_free_port(tcp),
    URL = emqx_bridge_v2_testlib:fmt(<<"http://127.0.0.1:${port}">>, #{port => Port}),
    ?assertMatch(
        {201, #{
            <<"status">> := <<"disconnected">>,
            <<"status_reason">> := <<"Connection refused">>
        }},
        create_connector_api(TCConfig, #{<<"url">> => URL})
    ),
    %% try `/start` bridge
    ?assertMatch(
        {400, #{<<"message">> := <<"Connection refused">>}},
        start_connector_api(TCConfig)
    ),
    ok.

t_compose_connector_url_and_action_path() ->
    [{http_path, <<"/foo/bar">>}].
t_compose_connector_url_and_action_path(TCConfig) ->
    #{port := HTTPPort} = get_config(http_server, TCConfig),
    {201, _} = create_connector_api(TCConfig, #{
        <<"url">> => emqx_bridge_v2_testlib:fmt(<<"http://localhost:${p}/foo">>, #{p => HTTPPort})
    }),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{<<"path">> => <<"/bar">>}
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    emqtt:publish(C, Topic, <<"hey">>, [{qos, 1}]),
    ?assertReceive({http, _, _}),
    ok.

%% Checks that we can successfully update a connector containing sensitive headers and
%% they won't be clobbered by the update.
t_update_with_sensitive_data(TCConfig) ->
    AuthHeader = <<"Bearer some_token">>,
    {201, #{<<"headers">> := #{<<"authorization">> := Obfuscated}}} =
        create_connector_api(TCConfig, #{
            <<"headers">> => #{
                <<"authorization">> => AuthHeader,
                <<"x-test-header">> => <<"from-connector">>
            }
        }),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{<<"headers">> => #{<<"x-test-header">> => <<"from-action">>}}
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    emqtt:publish(C, Topic, <<"hey">>, [{qos, 1}]),
    ?assertReceive(
        {http,
            #{
                <<"authorization">> := AuthHeader,
                <<"x-test-header">> := <<"from-action">>
            },
            _}
    ),

    %% Now update the connector and see if the header stays deobfuscated.  We send the old
    %% auth header as an obfuscated value to simulate the behavior of the frontend.
    {200, _} = update_connector_api(TCConfig, #{
        <<"headers">> => #{
            <<"authorization">> => Obfuscated,
            <<"x-test-header">> => <<"from-connector-new">>,
            <<"x-test-header-2">> => <<"from-connector-new">>,
            <<"other_header">> => <<"new">>
        }
    }),

    emqtt:publish(C, Topic, <<"hey2">>, [{qos, 1}]),
    %% Should not be obfuscated.
    ?assertReceive(
        {http,
            #{
                <<"authorization">> := AuthHeader,
                <<"x-test-header">> := <<"from-action">>,
                <<"x-test-header-2">> := <<"from-connector-new">>
            },
            _},
        2_000
    ),
    ok.

t_disable_action_counters(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    #{id := RuleId, topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    emqtt:publish(C, Topic, <<"hey">>, [{qos, 1}]),
    ?assertReceive({http, _, _}, 2_000),

    ?retry(
        _Interval = 500,
        _NAttempts = 20,
        ?assertMatch(
            #{
                counters := #{
                    'matched' := 1,
                    'actions.failed' := 0,
                    'actions.failed.unknown' := 0,
                    'actions.success' := 1,
                    'actions.total' := 1,
                    'actions.discarded' := 0
                }
            },
            emqx_bridge_v2_testlib:get_rule_metrics(RuleId)
        )
    ),

    %% disable the action
    {204, _} = disable_action_api(TCConfig),

    %% this will trigger a discard
    emqtt:publish(C, Topic, <<"hey2">>, [{qos, 1}]),
    ?retry(
        _Interval = 500,
        _NAttempts = 20,
        ?assertMatch(
            #{
                counters := #{
                    'matched' := 2,
                    'actions.failed' := 0,
                    'actions.failed.unknown' := 0,
                    'actions.success' := 1,
                    'actions.total' := 2,
                    'actions.discarded' := 1
                }
            },
            emqx_bridge_v2_testlib:get_rule_metrics(RuleId)
        )
    ),
    ok.

t_rule_test_trace(TCConfig) ->
    emqx_bridge_v2_testlib:t_rule_test_trace(TCConfig, #{}).

-doc """
Smoke test for using managed certs for TLS clients.
""".
t_managed_certs() ->
    [{matrix, true}].
t_managed_certs(matrix) ->
    [[?tls]];
t_managed_certs(TCConfig) when is_list(TCConfig) ->
    BundleName = <<"bundle1">>,
    {ok, _} = emqx_bridge_v2_testlib:generate_and_upload_managed_certs(
        ?global_ns,
        BundleName,
        #{}
    ),
    {201, _} = create_connector_api(TCConfig, #{
        <<"ssl">> => #{
            <<"enable">> => true,
            <<"verify">> => <<"verify_none">>,
            <<"managed_certs">> => #{<<"bundle_name">> => BundleName}
        }
    }),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := Topic, id := RuleId} = simple_create_rule_api(TCConfig),
    C = start_client(),
    emqtt:publish(C, Topic, <<"hey">>, [{qos, 1}]),
    ?assertReceive({http, _, _}, 2_000),
    %% When configurations depend on the managed certs, we deny deleting them.
    ?assertMatch(
        {400, #{
            <<"referencing_configs">> := #{<<"global">> := [[<<"connectors">>, <<"http">>, _]]}
        }},
        delete_global_managed_certs_bundle(BundleName, #{})
    ),
    ?assertMatch(
        {400, #{
            <<"referencing_configs">> := #{<<"global">> := [[<<"connectors">>, <<"http">>, _]]}
        }},
        delete_global_managed_certs_file(BundleName, <<"ca">>, #{})
    ),
    %% `force_delete' is ignored: deletion is refused while the connector still
    %% references the bundle.
    ?assertMatch(
        {400, #{
            <<"referencing_configs">> := #{<<"global">> := [[<<"connectors">>, <<"http">>, _]]}
        }},
        delete_global_managed_certs_file(
            BundleName,
            <<"ca">>,
            #{force_delete => true}
        )
    ),
    ?assertMatch(
        {400, #{
            <<"referencing_configs">> := #{<<"global">> := [[<<"connectors">>, <<"http">>, _]]}
        }},
        delete_global_managed_certs_bundle(BundleName, #{force_delete => true})
    ),
    %% Once the referencing connector is gone, deletion succeeds.
    #{type := ActionType, name := ActionName} = emqx_bridge_v2_testlib:get_common_values(
        TCConfig
    ),
    {204, _} = emqx_bridge_v2_testlib:delete_rule_api(RuleId),
    {204, _} = emqx_bridge_v2_testlib:delete_kind_api(action, ActionType, ActionName),
    {204, _} = emqx_bridge_v2_testlib:delete_connector_api(TCConfig),
    ?assertMatch({204, _}, delete_global_managed_certs_bundle(BundleName, #{})),
    ok.

-doc """
Smoke test for using managed certs for TLS clients, when the private key is password protected.
""".
t_managed_certs_password() ->
    [{matrix, true}].
t_managed_certs_password(matrix) ->
    [[?tls]];
t_managed_certs_password(TCConfig) when is_list(TCConfig) ->
    BundleName = <<"bundle1">>,
    {ok, _} = emqx_bridge_v2_testlib:generate_and_upload_managed_certs(
        ?global_ns,
        BundleName,
        #{password => <<"$ecr3tP@sç"/utf8>>}
    ),
    {201, _} = create_connector_api(TCConfig, #{
        <<"ssl">> => #{
            <<"enable">> => true,
            <<"verify">> => <<"verify_none">>,
            <<"managed_certs">> => #{<<"bundle_name">> => BundleName}
        }
    }),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    emqtt:publish(C, Topic, <<"hey">>, [{qos, 1}]),
    ?assertReceive({http, _, _}, 2_000),
    ok.
