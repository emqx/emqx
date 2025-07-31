%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mt_api_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include("emqx_mt.hrl").

-define(NEW_CLIENTID(I),
    iolist_to_binary("c-" ++ atom_to_list(?FUNCTION_NAME) ++ "-" ++ integer_to_list(I))
).

-define(NEW_USERNAME(), iolist_to_binary("u-" ++ atom_to_list(?FUNCTION_NAME))).

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).
-define(ON_ALL(NODES, BODY), erpc:multicall(NODES, fun() -> BODY end)).

-define(MAX_NUM_TNS_CONFIGS, 1_000).
-define(AUTH_HEADER_PD_KEY, {?MODULE, auth_header}).

-import(emqx_common_test_helpers, [on_exit/1]).

%%------------------------------------------------------------------------------
%% CT Boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(t_adjust_limiters = TestCase, Config) ->
    Apps = [
        emqx,
        {emqx_conf, "mqtt.client_attrs_init = [{expression = username, set_as_attr = tns}]"},
        emqx_mt,
        emqx_management
    ],
    Cluster =
        [N1, _N2] = emqx_cth_cluster:start(
            [
                {node_name(TestCase, 1), #{
                    apps => Apps ++
                        [emqx_mgmt_api_test_util:emqx_dashboard()]
                }},
                {node_name(TestCase, 2), #{apps => Apps, role => replicant}}
            ],
            #{work_dir => emqx_cth_suite:work_dir(TestCase, Config)}
        ),
    AuthHeader = ?ON(N1, emqx_mgmt_api_test_util:auth_header_()),
    put(?AUTH_HEADER_PD_KEY, AuthHeader),
    [{cluster, Cluster} | Config];
init_per_testcase(TestCase, Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            {emqx_conf, "mqtt.client_attrs_init = [{expression = username, set_as_attr = tns}]"},
            {emqx_mt, "multi_tenancy.default_max_sessions = 10"},
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(TestCase, Config)}
    ),
    snabbkaffe:start_trace(),
    [{apps, Apps} | Config].

end_per_testcase(t_adjust_limiters, Config) ->
    Cluster = ?config(cluster, Config),
    snabbkaffe:stop(),
    emqx_common_test_helpers:call_janitor(),
    ok = emqx_cth_cluster:stop(Cluster),
    ok;
end_per_testcase(_TestCase, Config) ->
    Apps = ?config(apps, Config),
    snabbkaffe:stop(),
    emqx_common_test_helpers:call_janitor(),
    ok = emqx_cth_suite:stop(Apps),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

-define(assertIntersectionEqual(EXPECTED_MAP, MAP),
    ?assertEqual(
        EXPECTED_MAP,
        deep_intersect(EXPECTED_MAP, MAP),
        #{expected => EXPECTED_MAP, got => MAP}
    )
).

node_name(TestCase, N) ->
    NameBin = <<(atom_to_binary(TestCase))/binary, "_", (integer_to_binary(N))/binary>>,
    binary_to_atom(NameBin).

get_mqtt_tcp_port(Node) ->
    {_, Port} = erpc:call(Node, emqx_config, get, [[listeners, tcp, default, bind]]),
    Port.

connect(ClientId, Username) ->
    connect(#{
        clientid => ClientId,
        username => Username,
        password => "123456"
    }).

connect(Overrides) ->
    Opts0 = #{
        proto_ver => v5
    },
    Opts = emqx_utils_maps:deep_merge(Opts0, Overrides),
    {ok, Pid} = emqtt:start_link(Opts),
    monitor(process, Pid),
    unlink(Pid),
    case emqtt:connect(Pid) of
        {ok, _} ->
            Pid;
        {error, _Reason} = E ->
            stop_client(Pid),
            erlang:error(E)
    end.

stop_client(Pid) ->
    catch emqtt:stop(Pid),
    receive
        {'DOWN', _, process, Pid, _, _} -> ok
    after 3000 ->
        exit(Pid, kill)
    end.

url(Path) ->
    emqx_mgmt_api_test_util:api_path(["mt", Path]).

ns_url(Ns, Path) ->
    emqx_mgmt_api_test_util:api_path(["mt", "ns", Ns, Path]).

count_clients(Ns) ->
    URL = ns_url(Ns, "client_count"),
    simple_request(#{method => get, url => URL}).

list_clients(Ns, QueryParams) ->
    URL = ns_url(Ns, "client_list"),
    simple_request(#{method => get, url => URL, query_params => QueryParams}).

list_nss(QueryParams) ->
    URL = url("ns_list"),
    simple_request(#{method => get, url => URL, query_params => QueryParams}).

list_nss_details(QueryParams) ->
    URL = url("ns_list_details"),
    simple_request(#{method => get, url => URL, query_params => QueryParams}).

maybe_json_decode(X) ->
    case emqx_utils_json:safe_decode(X) of
        {ok, Decoded} -> Decoded;
        {error, _} -> X
    end.

simplify_result(Res) ->
    case Res of
        {error, {{_, StatusCode, _}, Body}} ->
            {StatusCode, Body};
        {ok, {{_, StatusCode, _}, Body}} ->
            {StatusCode, Body}
    end.

simple_request(Params) ->
    AuthHeader =
        case get(?AUTH_HEADER_PD_KEY) of
            undefined -> emqx_mgmt_api_test_util:auth_header_();
            Header -> Header
        end,
    emqx_mgmt_api_test_util:simple_request(Params#{auth_header => AuthHeader}).

simple_request(Method, Path, Body, QueryParams) ->
    simple_request(#{method => Method, url => Path, body => Body, query_params => QueryParams}).

simple_request(Method, Path, Body) ->
    simple_request(#{method => Method, url => Path, body => Body}).

list_managed_nss(QueryParams) ->
    URL = emqx_mgmt_api_test_util:api_path(["mt", "managed_ns_list"]),
    simple_request(#{method => get, url => URL, query_params => QueryParams}).

list_managed_nss_details(QueryParams) ->
    URL = emqx_mgmt_api_test_util:api_path(["mt", "managed_ns_list_details"]),
    simple_request(#{method => get, url => URL, query_params => QueryParams}).

create_managed_ns(Ns) ->
    Path = emqx_mgmt_api_test_util:api_path(["mt", "ns", Ns]),
    Res = simple_request(post, Path, ""),
    ct:pal("create managed ns result:\n  ~p", [Res]),
    Res.

delete_ns(Ns) ->
    delete_managed_ns(Ns).

delete_managed_ns(Ns) ->
    Path = emqx_mgmt_api_test_util:api_path(["mt", "ns", Ns]),
    Res = simple_request(delete, Path, ""),
    ct:pal("delete managed ns result:\n  ~p", [Res]),
    Res.

bulk_delete_ns(Nss) ->
    Path = emqx_mgmt_api_test_util:api_path(["mt", "bulk_delete_ns"]),
    Body = #{<<"nss">> => Nss},
    Res = simple_request(delete, Path, Body),
    ct:pal("bulk delete nss result:\n  ~p", [Res]),
    Res.

get_managed_ns_config(Ns) ->
    Path = emqx_mgmt_api_test_util:api_path(["mt", "ns", Ns, "config"]),
    Res = simple_request(get, Path, ""),
    ct:pal("get managed ns config result:\n  ~p", [Res]),
    Res.

update_managed_ns_config(Ns, Body) ->
    Path = emqx_mgmt_api_test_util:api_path(["mt", "ns", Ns, "config"]),
    Res = simple_request(put, Path, Body),
    ct:pal("update managed ns config result:\n  ~p", [Res]),
    Res.

bulk_import_configs(Body) ->
    Path = emqx_mgmt_api_test_util:api_path(["mt", "bulk_import_configs"]),
    Res = simple_request(post, Path, Body),
    ct:pal("bulk import config result:\n  ~p", [Res]),
    Res.

kick_all_clients(Ns) ->
    Path = emqx_mgmt_api_test_util:api_path(["mt", "ns", Ns, "kick_all_clients"]),
    Res = simple_request(post, Path, ""),
    ct:pal("kick all ns clients result:\n  ~p", [Res]),
    Res.

disable_tenant_limiter(Ns) ->
    Body = #{<<"limiter">> => #{<<"tenant">> => <<"disabled">>}},
    update_managed_ns_config(Ns, Body).

disable_client_limiter(Ns) ->
    Body = #{<<"limiter">> => #{<<"client">> => <<"disabled">>}},
    update_managed_ns_config(Ns, Body).

export_backup() ->
    URL = emqx_mgmt_api_test_util:api_path(["data", "export"]),
    simple_request(#{method => post, url => URL, body => {raw, <<"">>}}).

import_backup(BackupName) ->
    URL = emqx_mgmt_api_test_util:api_path(["data", "import"]),
    Body = #{<<"filename">> => unicode:characters_to_binary(BackupName)},
    simple_request(#{method => post, url => URL, body => Body}).

tenant_limiter_params() ->
    tenant_limiter_params(_Overrides = #{}).

tenant_limiter_params(Overrides) ->
    Defaults = #{
        <<"bytes">> => #{
            <<"rate">> => <<"10MB/10s">>,
            <<"burst">> => <<"200MB/1m">>
        },
        <<"messages">> => #{
            <<"rate">> => <<"3000/1s">>,
            <<"burst">> => <<"40/1m">>
        }
    },
    Merged = emqx_utils_maps:deep_merge(Defaults, Overrides),
    #{<<"limiter">> => #{<<"tenant">> => Merged}}.

client_limiter_params() ->
    client_limiter_params(_Overrides = #{}).

client_limiter_params(Overrides) ->
    Defaults = #{
        <<"bytes">> => #{
            <<"rate">> => <<"10MB/10s">>,
            <<"burst">> => <<"200MB/1m">>
        },
        <<"messages">> => #{
            <<"rate">> => <<"3000/1s">>,
            <<"burst">> => <<"40/1m">>
        }
    },
    Merged = emqx_utils_maps:deep_merge(Defaults, Overrides),
    #{<<"limiter">> => #{<<"client">> => Merged}}.

session_params() ->
    session_params(_Overrides = #{}).

session_params(Overrides) ->
    Defaults = #{
        <<"max_sessions">> => <<"infinity">>
    },
    Merged = emqx_utils_maps:deep_merge(Defaults, Overrides),
    #{<<"session">> => Merged}.

bulk_import_configs_params(Entries) when is_list(Entries) ->
    lists:map(
        fun({Ns, Cfg}) -> #{<<"ns">> => Ns, <<"config">> => Cfg} end,
        Entries
    ).

set_limiter_for_zone(Key, Value) ->
    KeyBin = atom_to_binary(Key, utf8),
    MqttConf0 = emqx_config:fill_defaults(#{<<"mqtt">> => emqx:get_raw_config([<<"mqtt">>])}),
    MqttConf1 = emqx_utils_maps:deep_put([<<"mqtt">>, <<"limiter">>, KeyBin], MqttConf0, Value),
    {ok, _} = emqx:update_config([mqtt], maps:get(<<"mqtt">>, MqttConf1)).

set_limiter_for_listener(Key, Value) ->
    KeyBin = atom_to_binary(Key, utf8),
    emqx:update_config(
        [listeners, tcp, default],
        {update, #{
            KeyBin => Value
        }}
    ),
    ok.

spawn_publisher(Opts) ->
    #{
        clientid := ClientId,
        username := Username,
        qos := QoS,
        payload_size := PayloadSize
    } = Opts,
    Port = maps:get(port, Opts, 1883),
    TestPid = self(),
    LoopPid = spawn_link(fun() ->
        C = connect(#{
            clientid => ClientId,
            username => Username,
            password => "123456",
            port => Port
        }),
        TestPid ! {client, C},
        receive
            go -> run_publisher(C, PayloadSize, QoS)
        end
    end),
    receive
        {client, C} ->
            {LoopPid, C}
    after 1_000 ->
        ct:fail("client didn't start properly")
    end.

run_publisher(C, PayloadSize, QoS) ->
    _ = emqtt:publish(C, <<"test">>, binary:copy(<<"a">>, PayloadSize), QoS),
    receive
        die ->
            ok
    after 10 ->
        run_publisher(C, PayloadSize, QoS)
    end.

assert_limited(Opts) ->
    #{
        event_matcher := EventMatcher,
        timeout := Timeout
    } = Opts,
    {LoopPid, _C} = spawn_publisher(Opts),
    {_, {ok, _}} =
        snabbkaffe:wait_async_action(
            fun() -> LoopPid ! go end,
            EventMatcher,
            Timeout
        ),
    MRef = monitor(process, LoopPid),
    LoopPid ! die,
    receive
        {'DOWN', MRef, process, LoopPid, _} ->
            ok
    after 1_000 ->
        ct:fail("loop pid didn't die")
    end.

deep_intersect(#{} = RefMap, #{} = Map) ->
    lists:foldl(
        fun({K, RefV}, Acc) ->
            case maps:find(K, Map) of
                {ok, #{} = V} when is_map(RefV) ->
                    Acc#{K => deep_intersect(RefV, V)};
                {ok, V} ->
                    Acc#{K => V};
                error ->
                    Acc
            end
        end,
        #{},
        maps:to_list(RefMap)
    ).

wait_downs(Refs, _Timeout) when map_size(Refs) =:= 0 ->
    ok;
wait_downs(Refs0, Timeout) ->
    receive
        {'DOWN', Ref, process, _Pid, _Reason} when is_map_key(Ref, Refs0) ->
            Refs = maps:remove(Ref, Refs0),
            wait_downs(Refs, Timeout)
    after Timeout ->
        ct:fail("processes didn't die; remaining: ~b", [map_size(Refs0)])
    end.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_list_apis(_Config) ->
    N = 9,
    ClientIds = [?NEW_CLIENTID(I) || I <- lists:seq(1, N)],
    Ns = ?NEW_USERNAME(),
    Clients = [connect(ClientId, Ns) || ClientId <- ClientIds],
    ?retry(200, 50, ?assertEqual({ok, N}, emqx_mt:count_clients(Ns))),
    ?assertMatch({200, #{<<"count">> := N}}, count_clients(Ns)),
    {200, ClientIds0} = list_clients(Ns, #{<<"limit">> => integer_to_binary(N div 2)}),
    LastClientId = lists:last(ClientIds0),
    {200, ClientIds1} =
        list_clients(Ns, #{
            <<"last_clientid">> => LastClientId,
            <<"limit">> => integer_to_binary(N)
        }),
    ?assertEqual(ClientIds, ClientIds0 ++ ClientIds1),
    ok = lists:foreach(fun stop_client/1, Clients),
    ?retry(
        200,
        50,
        ?assertMatch(
            {200, #{<<"count">> := 0}},
            count_clients(Ns)
        )
    ),
    ?assertMatch(
        {200, []},
        list_clients(Ns, #{})
    ),
    ?assertMatch(
        {200, [Ns]},
        list_nss(#{})
    ),
    ?assertMatch(
        {200, [Ns]},
        list_nss(#{<<"limit">> => <<"2">>})
    ),
    ?assertMatch(
        {200, []},
        list_nss(#{<<"last_ns">> => Ns, <<"limit">> => <<"1">>})
    ),
    ok.

%% Smoke CRUD operations test for managed namespaces.
%% Configuration management is tested in separate, specific test cases.
t_managed_namespaces_crud(_Config) ->
    ?assertMatch({200, []}, list_managed_nss(#{})),

    Ns1 = <<"tns1">>,
    Ns2 = <<"tns2">>,
    ?assertMatch({204, _}, delete_managed_ns(Ns1)),
    ?assertMatch({204, _}, delete_managed_ns(Ns2)),
    ?assertMatch({404, _}, get_managed_ns_config(Ns1)),
    ?assertMatch({404, _}, get_managed_ns_config(Ns2)),
    ?assertMatch({200, []}, list_managed_nss(#{})),

    ?assertMatch({204, _}, create_managed_ns(Ns1)),
    ?assertMatch({204, _}, delete_managed_ns(Ns2)),
    ?assertMatch({200, _}, get_managed_ns_config(Ns1)),
    ?assertMatch({404, _}, get_managed_ns_config(Ns2)),
    ?assertMatch({200, [Ns1]}, list_managed_nss(#{})),

    ?assertMatch({204, _}, create_managed_ns(Ns2)),
    ?assertMatch({200, [Ns1, Ns2]}, list_managed_nss(#{})),
    ?assertMatch({200, _}, get_managed_ns_config(Ns1)),
    ?assertMatch({200, _}, get_managed_ns_config(Ns2)),

    ?assertMatch({200, [Ns1]}, list_managed_nss(#{<<"limit">> => <<"1">>})),
    ?assertMatch({200, [Ns2]}, list_managed_nss(#{<<"last_ns">> => Ns1})),
    ?assertMatch({200, []}, list_managed_nss(#{<<"last_ns">> => Ns2})),

    ?assertMatch({204, _}, delete_managed_ns(Ns1)),
    %% Idempotency
    ?assertMatch({204, _}, delete_managed_ns(Ns1)),
    ?assertMatch({200, [Ns2]}, list_managed_nss(#{})),
    ?assertMatch({404, _}, get_managed_ns_config(Ns1)),
    ?assertMatch({200, _}, get_managed_ns_config(Ns2)),
    ?assertMatch({204, _}, delete_managed_ns(Ns2)),
    ?assertMatch({200, []}, list_managed_nss(#{})),
    ?assertMatch({404, _}, get_managed_ns_config(Ns1)),
    ?assertMatch({404, _}, get_managed_ns_config(Ns2)),

    ok.

t_bulk_delete_ns(_Config) ->
    ?assertMatch({200, []}, list_managed_nss(#{})),

    Ns1 = <<"tns1">>,
    Ns2 = <<"tns2">>,
    Ns3 = <<"tns3">>,

    lists:foreach(
        fun(Ns) ->
            {204, _} = create_managed_ns(Ns)
        end,
        [Ns1, Ns2, Ns3]
    ),
    {200, [Ns1, Ns2, Ns3]} = list_managed_nss(#{}),

    ?assertMatch({204, _}, bulk_delete_ns([Ns3, Ns1])),
    ?assertMatch({200, [Ns2]}, list_managed_nss(#{})),

    %% Idempotency
    ?assertMatch({204, _}, bulk_delete_ns([Ns3, Ns1])),
    ?assertMatch({200, [Ns2]}, list_managed_nss(#{})),

    ?assertMatch({204, _}, bulk_delete_ns([Ns2])),
    ?assertMatch({200, []}, list_managed_nss(#{})),

    %% Simulate failure during side-effect execution.
    lists:foreach(
        fun(Ns) ->
            {204, _} = create_managed_ns(Ns)
        end,
        [Ns1, Ns2, Ns3]
    ),

    emqx_common_test_helpers:with_mock(
        emqx_mt_state,
        delete_managed_ns,
        fun(Ns) ->
            case Ns of
                Ns2 ->
                    %% actually impossible to have an aborted txn here?
                    {error, {aborted, mocked_error}};
                _ ->
                    meck:passthrough([Ns])
            end
        end,
        fun() ->
            ?assertMatch(
                {500, #{
                    <<"hint">> := _,
                    <<"errors">> := #{Ns2 := <<"{aborted,mocked_error}">>}
                }},
                bulk_delete_ns([Ns1, Ns2, Ns3])
            ),
            ?assertMatch({200, [Ns2]}, list_managed_nss(#{}))
        end
    ),

    ok.

%% Checks that we can delete implicit namespace (`?NS_TAB`) entries when calling the
%% endpoint for deleting managed namespaces (originally meant only for them).
t_delete_implicit_namespaces(_Config) ->
    %% Sanity checks
    ?assertMatch({200, []}, list_managed_nss(#{})),
    ?assertMatch({200, []}, list_nss(#{})),

    Ns1 = <<"ns1">>,
    ?assertMatch({204, _}, delete_managed_ns(Ns1)),

    %% Create implicit NS
    ClientId1 = ?NEW_CLIENTID(1),
    {Pid1, {ok, _}} =
        ?wait_async_action(
            connect(ClientId1, Ns1),
            #{?snk_kind := multi_tenant_client_added}
        ),
    ?assertMatch({200, []}, list_managed_nss(#{})),
    ?assertMatch({200, [_]}, list_nss(#{})),

    %% Delete implicit NS.  Should kick client as a side-effect.
    ?assertMatch({204, _}, delete_managed_ns(Ns1)),
    receive
        {'DOWN', _, process, Pid1, _} ->
            ok
    after 1_000 -> ct:fail("~b: client ~p didn't get kicked", [?LINE, Pid1])
    end,
    ?assertMatch({200, []}, list_managed_nss(#{})),
    ?assertMatch({200, []}, list_nss(#{})),

    %% Create managed NS
    ct:pal("waiting for kicker to shut down"),
    ?retry(250, 10, ?assertMatch({error, not_found}, emqx_mt_client_kicker:whereis_kicker(Ns1))),
    ?assertMatch({204, _}, create_managed_ns(Ns1)),

    ClientId2 = ?NEW_CLIENTID(2),
    {Pid2, {ok, _}} =
        ?wait_async_action(
            connect(ClientId2, Ns1),
            #{?snk_kind := multi_tenant_client_added}
        ),
    ?assertMatch({200, [_]}, list_managed_nss(#{})),
    ?assertMatch({200, [_]}, list_nss(#{})),

    %% Delete implicit NS.  Should kick client as a side-effect.
    ?assertMatch({204, _}, delete_managed_ns(Ns1)),
    receive
        {'DOWN', _, process, Pid2, _} ->
            ok
    after 1_000 -> ct:fail("~b: client ~p didn't get kicked", [?LINE, Pid1])
    end,
    ?assertMatch({200, []}, list_managed_nss(#{})),
    ?assertMatch({200, []}, list_nss(#{})),

    ok.

%% Checks that managedly declared namespaces use their own maximum session count instead
%% of global defaults.
t_session_limit_exceeded(_Config) ->
    emqx_mt_config:tmp_set_default_max_sessions(1),
    Ns = ?NEW_USERNAME(),
    ClientId1 = ?NEW_CLIENTID(1),
    ClientId2 = ?NEW_CLIENTID(2),
    ClientId3 = ?NEW_CLIENTID(3),

    Params = session_params(#{<<"max_sessions">> => 2}),
    {204, _} = create_managed_ns(Ns),
    ?assertMatch({200, _}, update_managed_ns_config(Ns, Params)),

    %% First client is always fine.
    {Pid1, {ok, _}} =
        ?wait_async_action(
            connect(ClientId1, Ns),
            #{?snk_kind := multi_tenant_client_added}
        ),
    ?assertEqual(1, emqx_mt_state:update_ccache(Ns)),
    %% Second would fail with quota exceeded reason, if it were to use global default
    {Pid2, {ok, _}} =
        ?wait_async_action(
            connect(ClientId2, Ns),
            #{?snk_kind := multi_tenant_client_added}
        ),
    ?assertEqual(2, emqx_mt_state:update_ccache(Ns)),
    %% Now we hit the maximum session count
    try
        _Pid3 = connect(ClientId3, Ns),
        ct:fail("should have not connected successfully!")
    catch
        error:{error, {quota_exceeded, _}} ->
            ok;
        exit:{shutdown, quota_exceeded} ->
            ok
    end,
    emqtt:stop(Pid1),
    emqtt:stop(Pid2),
    ok.

%% When `multi_tenancy.allow_only_managed_namespaces = true', we don't allow clients from
%% non-managed namespaces to connect.
t_allow_only_managed_namespaces(_Config) ->
    ok = emqx_mt_config:set_allow_only_managed_namespaces(true),
    UnknownNs = <<"implicit-ns">>,
    ClientId = ?NEW_CLIENTID(1),
    ?assertError({error, {not_authorized, _}}, connect(ClientId, UnknownNs)),
    %% Clients without extracted namespace are also forbidden from connecting.
    {ok, Pid} = emqtt:start_link(#{proto_ver => v5}),
    unlink(Pid),
    ?assertMatch({error, {not_authorized, _}}, emqtt:connect(Pid)),
    ok.

%% Verifies bulk import config endpoint
t_bulk_import_configs(_Config) ->
    %% Seed config with some preexisting configs and namespaces
    ExistingNs1 = <<"ens1">>,
    {204, _} = create_managed_ns(ExistingNs1),

    %% Will receive new config keys and leave existing one untouched
    ExistingNs2 = <<"ens2">>,
    {204, _} = create_managed_ns(ExistingNs2),
    ExistingConfig2 = tenant_limiter_params(),
    {200, _} = update_managed_ns_config(ExistingNs2, ExistingConfig2),

    %% Will receive both new config keys and update existing one
    ExistingNs3 = <<"ens3">>,
    {204, _} = create_managed_ns(ExistingNs3),
    ExistingConfig3 = tenant_limiter_params(),
    {200, _} = update_managed_ns_config(ExistingNs3, ExistingConfig3),

    %% Untouched
    ExistingNs4 = <<"ens4">>,
    {204, _} = create_managed_ns(ExistingNs4),

    NewConfig1 = tenant_limiter_params(),
    NewConfig2 = client_limiter_params(),
    NewConfig3 = emqx_utils_maps:deep_merge(
        #{<<"limiter">> => #{<<"tenant">> => <<"disabled">>}},
        client_limiter_params()
    ),
    NewNs5 = <<"new-ns5">>,
    NewConfig5 = client_limiter_params(),
    ImportParams1 = bulk_import_configs_params([
        {ExistingNs1, NewConfig1},
        {ExistingNs2, NewConfig2},
        {ExistingNs3, NewConfig3},
        {NewNs5, NewConfig5}
    ]),
    ?assertMatch({204, _}, bulk_import_configs(ImportParams1)),

    {200, Config1} = get_managed_ns_config(ExistingNs1),
    ?assertIntersectionEqual(NewConfig1, Config1),
    {200, Config2} = get_managed_ns_config(ExistingNs2),
    ?assertIntersectionEqual(NewConfig2, Config2),
    {200, Config3} = get_managed_ns_config(ExistingNs3),
    ?assertIntersectionEqual(NewConfig3, Config3),
    {200, Config4} = get_managed_ns_config(ExistingNs4),
    ?assertEqual(#{}, Config4),
    {200, Config5} = get_managed_ns_config(NewNs5),
    ?assertIntersectionEqual(NewConfig5, Config5),

    ok.

%% Checks that we validate for duplicate namespaces when bulk importing.
t_bulk_import_configs_duplicated_nss(_Config) ->
    Ns1 = <<"ns1">>,
    Ns2 = <<"ns2">>,
    ImportParams1 = bulk_import_configs_params([
        {Ns1, #{}},
        {Ns1, #{}},
        {<<"not-duplicated">>, #{}},
        {Ns2, #{}},
        {Ns2, #{}}
    ]),
    ?assertMatch(
        {400, #{
            <<"message">> := <<"Duplicated namespaces in input: ns1, ns2">>
        }},
        bulk_import_configs(ImportParams1)
    ),
    ok.

%% Checks that we validate the maximum allowed number of configs.
t_bulk_import_configs_max_configs(_Config) ->
    TooManyConfigs0 =
        lists:map(
            fun(N) -> {<<"ns", (integer_to_binary(N))/binary>>, #{}} end,
            lists:seq(1, ?MAX_NUM_TNS_CONFIGS + 1)
        ),
    TooManyConfigs = bulk_import_configs_params(TooManyConfigs0),
    ?assertMatch(
        {400, #{
            <<"message">> := <<"Maximum number of managed namespaces reached">>
        }},
        bulk_import_configs(TooManyConfigs)
    ),
    MaxConfigs = lists:sublist(TooManyConfigs, ?MAX_NUM_TNS_CONFIGS),
    ?assertMatch({204, _}, bulk_import_configs(MaxConfigs)),
    %% Should be fine to bulk import again if only updating existing configs
    ?assertMatch({204, _}, bulk_import_configs(MaxConfigs)),
    ?assertMatch(
        {400, #{
            <<"message">> := <<"Maximum number of managed namespaces reached">>
        }},
        bulk_import_configs(TooManyConfigs)
    ),
    ok.

%% Smoke CRUD operations test for tenant limiter.
t_tenant_limiter(_Config) ->
    Ns1 = <<"tns">>,
    Params1 = tenant_limiter_params(),

    %% Must create the managed namespace first
    ?assertMatch({404, _}, get_managed_ns_config(Ns1)),
    ?assertMatch({404, _}, update_managed_ns_config(Ns1, Params1)),
    ?assertMatch({404, _}, disable_tenant_limiter(Ns1)),

    ?assertMatch({204, _}, create_managed_ns(Ns1)),

    ?assertMatch(
        {200, #{
            <<"limiter">> := #{
                <<"tenant">> := #{
                    <<"bytes">> := #{<<"rate">> := <<"10MB/10s">>, <<"burst">> := <<"200MB/1m">>},
                    <<"messages">> := #{<<"rate">> := <<"3000/1s">>, <<"burst">> := <<"40/1m">>}
                }
            }
        }},
        update_managed_ns_config(Ns1, Params1)
    ),
    %% Idempotency
    ?assertMatch(
        {200, #{
            <<"limiter">> := #{
                <<"tenant">> := #{
                    <<"bytes">> := #{<<"rate">> := <<"10MB/10s">>, <<"burst">> := <<"200MB/1m">>},
                    <<"messages">> := #{<<"rate">> := <<"3000/1s">>, <<"burst">> := <<"40/1m">>}
                }
            }
        }},
        update_managed_ns_config(Ns1, Params1)
    ),
    ?assertMatch(
        {200, #{
            <<"limiter">> := #{
                <<"tenant">> := #{
                    <<"bytes">> := #{<<"rate">> := <<"10MB/10s">>, <<"burst">> := <<"200MB/1m">>},
                    <<"messages">> := #{<<"rate">> := <<"3000/1s">>, <<"burst">> := <<"40/1m">>}
                }
            }
        }},
        get_managed_ns_config(Ns1)
    ),
    Params2 = tenant_limiter_params(#{
        <<"bytes">> => #{
            <<"rate">> => <<"infinity">>,
            <<"burst">> => <<"0/1d">>
        },
        <<"messages">> => #{
            <<"burst">> => <<"60/60s">>
        }
    }),
    ?assertMatch(
        {200, #{
            <<"limiter">> := #{
                <<"tenant">> := #{
                    <<"bytes">> := #{<<"rate">> := <<"infinity">>, <<"burst">> := <<"0/1d">>},
                    <<"messages">> := #{<<"rate">> := <<"3000/1s">>, <<"burst">> := <<"60/1m">>}
                }
            }
        }},
        update_managed_ns_config(Ns1, Params2)
    ),
    ?assertMatch(
        {200, #{
            <<"limiter">> := #{
                <<"tenant">> := #{
                    <<"bytes">> := #{<<"rate">> := <<"infinity">>, <<"burst">> := <<"0/1d">>},
                    <<"messages">> := #{<<"rate">> := <<"3000/1s">>, <<"burst">> := <<"60/1m">>}
                }
            }
        }},
        get_managed_ns_config(Ns1)
    ),

    ?assertMatch(
        {200, #{<<"limiter">> := #{<<"tenant">> := <<"disabled">>}}},
        disable_tenant_limiter(Ns1)
    ),
    ?assertMatch(
        {200, #{<<"limiter">> := #{<<"tenant">> := <<"disabled">>}}},
        get_managed_ns_config(Ns1)
    ),

    ok.

%% Smoke CRUD operations test for client limiter.
t_client_limiter(_Config) ->
    Ns1 = <<"tns">>,
    Params1 = client_limiter_params(),

    %% Must create the managed namespace first
    ?assertMatch({404, _}, get_managed_ns_config(Ns1)),
    ?assertMatch({404, _}, update_managed_ns_config(Ns1, Params1)),
    ?assertMatch({404, _}, disable_client_limiter(Ns1)),

    ?assertMatch({204, _}, create_managed_ns(Ns1)),

    ?assertMatch(
        {200, #{
            <<"limiter">> := #{
                <<"client">> := #{
                    <<"bytes">> := #{<<"rate">> := <<"10MB/10s">>, <<"burst">> := <<"200MB/1m">>},
                    <<"messages">> := #{<<"rate">> := <<"3000/1s">>, <<"burst">> := <<"40/1m">>}
                }
            }
        }},
        update_managed_ns_config(Ns1, Params1)
    ),
    %% Idempotency
    ?assertMatch(
        {200, #{
            <<"limiter">> := #{
                <<"client">> := #{
                    <<"bytes">> := #{<<"rate">> := <<"10MB/10s">>, <<"burst">> := <<"200MB/1m">>},
                    <<"messages">> := #{<<"rate">> := <<"3000/1s">>, <<"burst">> := <<"40/1m">>}
                }
            }
        }},
        update_managed_ns_config(Ns1, Params1)
    ),
    ?assertMatch(
        {200, #{
            <<"limiter">> := #{
                <<"client">> := #{
                    <<"bytes">> := #{<<"rate">> := <<"10MB/10s">>, <<"burst">> := <<"200MB/1m">>},
                    <<"messages">> := #{<<"rate">> := <<"3000/1s">>, <<"burst">> := <<"40/1m">>}
                }
            }
        }},
        get_managed_ns_config(Ns1)
    ),
    Params2 = client_limiter_params(#{
        <<"bytes">> => #{
            <<"rate">> => <<"infinity">>,
            <<"burst">> => <<"0/1d">>
        },
        <<"messages">> => #{
            <<"burst">> => <<"60/60s">>
        }
    }),
    ?assertMatch(
        {200, #{
            <<"limiter">> := #{
                <<"client">> := #{
                    <<"bytes">> := #{<<"rate">> := <<"infinity">>, <<"burst">> := <<"0/1d">>},
                    <<"messages">> := #{<<"rate">> := <<"3000/1s">>, <<"burst">> := <<"60/1m">>}
                }
            }
        }},
        update_managed_ns_config(Ns1, Params2)
    ),
    ?assertMatch(
        {200, #{
            <<"limiter">> := #{
                <<"client">> := #{
                    <<"bytes">> := #{<<"rate">> := <<"infinity">>, <<"burst">> := <<"0/1d">>},
                    <<"messages">> := #{<<"rate">> := <<"3000/1s">>, <<"burst">> := <<"60/1m">>}
                }
            }
        }},
        get_managed_ns_config(Ns1)
    ),

    ?assertMatch(
        {200, #{<<"limiter">> := #{<<"client">> := <<"disabled">>}}},
        disable_client_limiter(Ns1)
    ),
    ?assertMatch(
        {200, #{<<"limiter">> := #{<<"client">> := <<"disabled">>}}},
        get_managed_ns_config(Ns1)
    ),

    ok.

%% Verifies that the channel limiters are adjusted when client and/or tenant limiters are
%% configured.
%% We create the configs via HTTP API on one node, and connect MQTT clients to the other,
%% so we verify that limiter groups are changed on all nodes.
t_adjust_limiters(Config) when is_list(Config) ->
    Nodes = [_N1, N2] = ?config(cluster, Config),
    Port2 = get_mqtt_tcp_port(N2),
    Ns = atom_to_binary(?FUNCTION_NAME),
    ?check_trace(
        begin
            ?assertMatch({204, _}, create_managed_ns(Ns)),

            %% 1) Client limiter completely replaces listener limiter.
            ?ON_ALL(Nodes, set_limiter_for_listener(messages_rate, <<"infinity">>)),
            ?ON_ALL(Nodes, set_limiter_for_listener(bytes_rate, <<"infinity">>)),
            ClientParams1 = client_limiter_params(#{
                <<"bytes">> => #{<<"rate">> => <<"1/500ms">>, <<"burst">> => <<"0/1s">>},
                <<"messages">> => #{<<"rate">> => <<"1/500ms">>, <<"burst">> => <<"0/1s">>}
            }),
            ?assertMatch({200, _}, update_managed_ns_config(Ns, ClientParams1)),
            Username = Ns,
            ClientId1 = ?NEW_CLIENTID(1),
            assert_limited(#{
                clientid => ClientId1,
                username => Username,
                qos => 1,
                payload_size => 100,
                port => Port2,
                event_matcher => ?match_event(#{
                    ?snk_kind := limiter_exclusive_try_consume, success := false
                }),
                timeout => 1_000
            }),
            {200, _} = disable_client_limiter(Ns),
            %% Tenant limiter composes with zone limiter.
            ?ON_ALL(Nodes, set_limiter_for_zone(messages_rate, <<"infinity">>)),
            ?ON_ALL(Nodes, set_limiter_for_zone(bytes_rate, <<"infinity">>)),
            TenantParams1 = tenant_limiter_params(#{
                <<"bytes">> => #{<<"rate">> => <<"1/500ms">>, <<"burst">> => <<"0/1s">>},
                <<"messages">> => #{<<"rate">> => <<"1/500ms">>, <<"burst">> => <<"0/1s">>}
            }),
            ?assertMatch({200, _}, update_managed_ns_config(Ns, TenantParams1)),
            ClientId2 = ?NEW_CLIENTID(2),
            assert_limited(#{
                clientid => ClientId2,
                username => Username,
                qos => 1,
                payload_size => 100,
                port => Port2,
                event_matcher => ?match_event(#{
                    ?snk_kind := limiter_shared_try_consume, success := false
                }),
                timeout => 1_000
            }),
            %% Other way around
            ?ON_ALL(Nodes, set_limiter_for_zone(messages_rate, <<"1/500ms">>)),
            ?ON_ALL(Nodes, set_limiter_for_zone(bytes_rate, <<"1/500ms">>)),
            TenantParams2 = tenant_limiter_params(#{
                <<"bytes">> => #{<<"rate">> => <<"infinity">>, <<"burst">> => <<"0/1s">>},
                <<"messages">> => #{<<"rate">> => <<"infinity">>, <<"burst">> => <<"0/1s">>}
            }),
            ?assertMatch({200, _}, update_managed_ns_config(Ns, TenantParams2)),
            ClientId3 = ?NEW_CLIENTID(3),
            assert_limited(#{
                clientid => ClientId3,
                username => Username,
                qos => 1,
                payload_size => 100,
                port => Port2,
                event_matcher => ?match_event(#{
                    ?snk_kind := limiter_shared_try_consume, success := false
                }),
                timeout => 1_000
            }),
            {200, _} = disable_tenant_limiter(Ns),

            ok
        end,
        fun(Trace) ->
            ?assertMatch([_, _, _], ?of_kind("channel_limiter_adjusted", Trace)),
            ok
        end
    ),
    ok.

%% Checks that we kick clients of a managed namespace when we delete it.
t_kick_clients_when_deleting(_Config) ->
    Ns1 = <<"ns1">>,
    Ns2 = <<"ns2">>,
    Ns3 = <<"ns3">>,
    {204, _} = create_managed_ns(Ns1),
    {204, _} = create_managed_ns(Ns2),
    {204, _} = create_managed_ns(Ns3),

    N = 9,
    ClientIds1 = [?NEW_CLIENTID(I) || I <- lists:seq(1, N)],
    Clients1 = [connect(ClientId, Ns1) || ClientId <- ClientIds1],
    ClientIds2 = [?NEW_CLIENTID(I) || I <- lists:seq(10, 10 + N)],
    Clients2 = [connect(ClientId, Ns2) || ClientId <- ClientIds2],
    %% These should be left alone
    ClientIds3 = [?NEW_CLIENTID(I) || I <- lists:seq(20, 10 + N)],
    Clients3 = [connect(ClientId, Ns3) || ClientId <- ClientIds3],

    MRefs0 = lists:map(fun(Pid) -> monitor(process, Pid) end, Clients1 ++ Clients2),
    MRefs = maps:from_keys(MRefs0, true),

    %% Delete 2 NSs (almost) at the same time
    ct:pal("deleting namespaces"),
    ?assertMatch({204, _}, delete_managed_ns(Ns1)),
    ?assertMatch({204, _}, delete_managed_ns(Ns2)),
    %% Cannot re-create namespace while clients are being kicked
    ?assertMatch(
        {409, #{
            <<"message">> := <<
                "Clients from this namespace are still being kicked;",
                _/binary
            >>
        }},
        create_managed_ns(Ns2)
    ),

    ct:pal("waiting for clients to be kicked"),
    wait_downs(MRefs, _Timeout = 5_000),
    ct:pal("clients kicked"),

    %% Clients from 3rd namespace should not be kicked
    ?assert(lists:all(fun is_process_alive/1, Clients3)),

    %% Create one of the NSs again
    ct:pal("waiting for kicker to shut down"),
    ?retry(250, 10, ?assertMatch({error, not_found}, emqx_mt_client_kicker:whereis_kicker(Ns1))),
    ct:pal("recreating namespace"),
    {204, _} = create_managed_ns(Ns1),
    Clients1B = [connect(ClientId, Ns1) || ClientId <- ClientIds1],
    ct:sleep(500),

    ct:pal("checking new clients are not kicked"),
    ?assert(lists:all(fun is_process_alive/1, Clients1B)),

    lists:foreach(fun emqtt:stop/1, Clients1B ++ Clients3),

    ok.

%% Smoke test for "kick all NS clients" API.
t_kick_all_ns_clients(_Config) ->
    Ns = <<"ns1">>,
    {204, _} = create_managed_ns(Ns),

    N = 9,
    ClientIds = [?NEW_CLIENTID(I) || I <- lists:seq(1, N)],
    Clients1 = [connect(ClientId, Ns) || ClientId <- ClientIds],

    MRefs1A = lists:map(fun(Pid) -> monitor(process, Pid) end, Clients1),
    MRefs1 = maps:from_keys(MRefs1A, true),

    ?assertMatch({202, _}, kick_all_clients(Ns)),

    ct:pal("waiting for clients to be kicked"),
    Timeout = 5_000,
    wait_downs(MRefs1, Timeout),
    ct:pal("clients kicked"),

    ?retry(250, 10, ?assertMatch({error, not_found}, emqx_mt_client_kicker:whereis_kicker(Ns))),

    %% Check consecutive calls to this async API.
    Clients2 = [connect(ClientId, Ns) || ClientId <- ClientIds],

    MRefs2A = lists:map(fun(Pid) -> monitor(process, Pid) end, Clients2),
    MRefs2 = maps:from_keys(MRefs2A, true),

    ?assertMatch({202, _}, kick_all_clients(Ns)),
    %% Already started
    ?assertMatch({409, _}, kick_all_clients(Ns)),

    ct:pal("waiting for clients to be kicked (again)"),
    wait_downs(MRefs2, Timeout),
    ct:pal("clients kicked"),

    ok.

%% Simple test to check we are able to export and import MT config.
t_backup_export_and_import(_Config) ->
    ClientParams = client_limiter_params(),
    TenantParams = tenant_limiter_params(),
    SessionParams = session_params(),
    MTConfig0 = emqx_utils_maps:deep_merge(ClientParams, TenantParams),
    MTConfig = emqx_utils_maps:deep_merge(MTConfig0, SessionParams),
    Ns = <<"ns1">>,
    LimiterGroupsBefore = emqx_limiter_registry:list_groups(),
    ?assertMatch({204, _}, create_managed_ns(Ns)),
    ?assertMatch({200, _}, update_managed_ns_config(Ns, MTConfig)),
    LimiterGroupsAfter = emqx_limiter_registry:list_groups(),
    ?assertNotEqual(LimiterGroupsBefore, LimiterGroupsAfter),

    ok = emqx_mt_config:set_allow_only_managed_namespaces(true),
    {ok, _} = emqx_conf:update([?CONF_ROOT_KEY, default_max_sessions], 13, #{override_to => cluster}),

    {200, #{<<"filename">> := BackupName}} = export_backup(),

    ?assertMatch({204, _}, delete_managed_ns(Ns)),
    ok = emqx_mt_config:set_allow_only_managed_namespaces(false),
    {ok, _} = emqx_conf:update([?CONF_ROOT_KEY, default_max_sessions], 10, #{override_to => cluster}),

    ?assertEqual(LimiterGroupsBefore, emqx_limiter_registry:list_groups()),

    ?assertMatch({204, _}, import_backup(BackupName)),

    ?assertMatch(
        {200, #{
            <<"limiter">> := #{
                <<"client">> := #{},
                <<"tenant">> := #{}
            },
            <<"session">> := #{}
        }},
        get_managed_ns_config(Ns)
    ),
    %% Must have created limiters as side-effect of importing config.
    ?assertEqual(LimiterGroupsAfter, emqx_limiter_registry:list_groups()),

    ?assertMatch({200, [Ns]}, list_managed_nss(#{})),
    ?assertMatch({200, [Ns]}, list_nss(#{})),

    ?assertMatch(
        #{
            default_max_sessions := 13,
            allow_only_managed_namespaces := true
        },
        emqx_config:get([multi_tenancy])
    ),

    %% Idempotency
    ?assertMatch({204, _}, import_backup(BackupName)),
    ?assertMatch(
        {200, #{
            <<"limiter">> := #{
                <<"client">> := #{},
                <<"tenant">> := #{}
            },
            <<"session">> := #{}
        }},
        get_managed_ns_config(Ns)
    ),
    ?assertEqual(LimiterGroupsAfter, emqx_limiter_registry:list_groups()),
    ?assertMatch({200, [Ns]}, list_managed_nss(#{})),
    ?assertMatch({200, [Ns]}, list_nss(#{})),

    %% Simulate errors when executing side-effects.
    on_exit(fun meck:unload/0),
    ok = meck:new(emqx_mt_config_proto_v1, [passthrough, no_history]),
    ok = meck:expect(emqx_mt_config_proto_v1, execute_side_effects, fun(_) ->
        {ok, [
            #{path => [sessions], op => {mocked, error}, error => {throw, <<"mocked_error">>}}
        ]}
    end),
    {400, #{<<"message">> := #{<<"global">> := Msg}}} = import_backup(BackupName),
    ?assertEqual(match, re:run(Msg, <<"mocked_error">>, [global, {capture, none}])),

    ok.

%% Smoke tests for checking that we assign creation times for namespaces.
t_creation_date(_Config) ->
    ct:pal("implicit namespace"),
    Ns1 = <<"ins1">>,
    ClientId1 = ?NEW_CLIENTID(1),
    {Pid1, {ok, _}} =
        ?wait_async_action(
            connect(ClientId1, Ns1),
            #{?snk_kind := multi_tenant_client_added}
        ),
    stop_client(Pid1),
    ?assertMatch(
        {200, [
            #{
                <<"name">> := Ns1,
                <<"created_at">> := I
            }
        ]} when is_integer(I),
        list_nss_details(#{})
    ),
    {204, _} = delete_ns(Ns1),

    ct:pal("managed namespace"),
    Ns2 = <<"mns1">>,
    {204, _} = create_managed_ns(Ns2),
    ClientId2 = ?NEW_CLIENTID(2),
    {Pid2, {ok, _}} =
        ?wait_async_action(
            connect(ClientId2, Ns2),
            #{?snk_kind := multi_tenant_client_added}
        ),
    stop_client(Pid2),
    ?assertMatch(
        {200, [
            #{
                <<"name">> := Ns2,
                <<"created_at">> := I
            }
        ]} when is_integer(I),
        list_nss_details(#{})
    ),
    ?assertMatch(
        {200, [
            #{
                <<"name">> := Ns2,
                <<"created_at">> := I
            }
        ]} when is_integer(I),
        list_managed_nss_details(#{})
    ),
    {204, _} = delete_managed_ns(Ns2),

    ok.
