%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_crl_cache_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("stdlib/include/assert.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% from ssl_manager.erl
-record(state, {
    session_cache_client,
    session_cache_client_cb,
    session_lifetime,
    certificate_db,
    session_validation_timer,
    session_cache_client_max,
    session_client_invalidator,
    options,
    client_session_order
}).

-define(DEFAULT_URL, "http://localhost:9878/intermediate.crl.pem").

%%--------------------------------------------------------------------
%% CT boilerplate
%%--------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TestCase, Config) when
    TestCase =:= t_cache;
    TestCase =:= t_filled_cache;
    TestCase =:= t_revoked
->
    ct:timetrap({seconds, 30}),
    ok = snabbkaffe:start_trace(),
    DataDir = ?config(data_dir, Config),
    {CRLPem, CRLDer} = read_crl(filename:join(DataDir, "intermediate-revoked.crl.pem")),
    ServerPid = start_crl_server(CRLPem),
    IsCached = lists:member(TestCase, [t_filled_cache, t_revoked]),
    Apps = start_emqx_with_crl_cache(#{is_cached => IsCached}, TestCase, Config),
    [
        {crl_pem, CRLPem},
        {crl_der, CRLDer},
        {http_server, ServerPid},
        {tc_apps, Apps}
        | Config
    ];
init_per_testcase(t_revoke_then_refresh = TestCase, Config) ->
    ct:timetrap({seconds, 120}),
    ok = snabbkaffe:start_trace(),
    DataDir = ?config(data_dir, Config),
    {CRLPemNotRevoked, CRLDerNotRevoked} =
        read_crl(filename:join(DataDir, "intermediate-not-revoked.crl.pem")),
    {CRLPemRevoked, CRLDerRevoked} =
        read_crl(filename:join(DataDir, "intermediate-revoked.crl.pem")),
    ServerPid = start_crl_server(CRLPemNotRevoked),
    Apps = start_emqx_with_crl_cache(
        #{is_cached => true, overrides => #{crl_cache => #{refresh_interval => <<"10s">>}}},
        TestCase,
        Config
    ),
    [
        {crl_pem_not_revoked, CRLPemNotRevoked},
        {crl_der_not_revoked, CRLDerNotRevoked},
        {crl_pem_revoked, CRLPemRevoked},
        {crl_der_revoked, CRLDerRevoked},
        {http_server, ServerPid},
        {tc_apps, Apps}
        | Config
    ];
init_per_testcase(t_cache_overflow = TestCase, Config) ->
    ct:timetrap({seconds, 120}),
    ok = snabbkaffe:start_trace(),
    DataDir = ?config(data_dir, Config),
    {CRLPemRevoked, _} = read_crl(filename:join(DataDir, "intermediate-revoked.crl.pem")),
    ServerPid = start_crl_server(CRLPemRevoked),
    Apps = start_emqx_with_crl_cache(
        #{is_cached => false, overrides => #{crl_cache => #{capacity => 2}}},
        TestCase,
        Config
    ),
    [
        {http_server, ServerPid},
        {tc_apps, Apps}
        | Config
    ];
init_per_testcase(TestCase, Config) when
    TestCase =:= t_not_cached_and_unreachable;
    TestCase =:= t_update_config
->
    ct:timetrap({seconds, 30}),
    ok = snabbkaffe:start_trace(),
    DataDir = ?config(data_dir, Config),
    {CRLPem, CRLDer} = read_crl(filename:join(DataDir, "intermediate-revoked.crl.pem")),
    Apps = start_emqx_with_crl_cache(#{is_cached => false}, TestCase, Config),
    [
        {crl_pem, CRLPem},
        {crl_der, CRLDer},
        {tc_apps, Apps}
        | Config
    ];
init_per_testcase(t_refresh_config = TestCase, Config) ->
    ct:timetrap({seconds, 30}),
    ok = snabbkaffe:start_trace(),
    DataDir = ?config(data_dir, Config),
    {CRLPem, CRLDer} = read_crl(filename:join(DataDir, "intermediate-revoked.crl.pem")),
    TestPid = self(),
    ok = meck:new(emqx_crl_cache, [non_strict, passthrough, no_history, no_link]),
    meck:expect(
        emqx_crl_cache,
        http_get,
        fun(URL, _HTTPTimeout) ->
            ct:pal("http get crl ~p", [URL]),
            TestPid ! {http_get, URL},
            {ok, {{"HTTP/1.0", 200, "OK"}, [], CRLPem}}
        end
    ),
    Apps = start_emqx_with_crl_cache(#{is_cached => false}, TestCase, Config),
    [
        {crl_pem, CRLPem},
        {crl_der, CRLDer},
        {tc_apps, Apps}
        | Config
    ];
init_per_testcase(TestCase, Config) when
    TestCase =:= t_update_listener;
    TestCase =:= t_update_listener_enable_disable;
    TestCase =:= t_validations
->
    ct:timetrap({seconds, 30}),
    ok = snabbkaffe:start_trace(),
    %% when running emqx standalone tests, we can't use those
    %% features.
    case does_module_exist(emqx_mgmt) of
        true ->
            DataDir = ?config(data_dir, Config),
            CRLFile = filename:join([DataDir, "intermediate-revoked.crl.pem"]),
            {ok, CRLPem} = file:read_file(CRLFile),
            ServerPid = start_crl_server(CRLPem),
            ListenerConf = #{
                enable => true,
                ssl_options => #{
                    keyfile => filename:join(DataDir, "server.key.pem"),
                    certfile => filename:join(DataDir, "server.cert.pem"),
                    cacertfile => filename:join(DataDir, "ca-chain.cert.pem"),
                    verify => verify_peer,
                    enable_crl_check => false
                }
            },
            Apps = emqx_cth_suite:start(
                [
                    {emqx_conf, #{config => #{listeners => #{ssl => #{default => ListenerConf}}}}},
                    emqx,
                    emqx_management,
                    emqx_mgmt_api_test_util:emqx_dashboard()
                ],
                #{work_dir => emqx_cth_suite:work_dir(TestCase, Config)}
            ),
            [
                {http_server, ServerPid},
                {tc_apps, Apps}
                | Config
            ];
        false ->
            [{skip_does_not_apply, true} | Config]
    end;
init_per_testcase(_TestCase, Config) ->
    ct:timetrap({seconds, 30}),
    ok = snabbkaffe:start_trace(),
    DataDir = ?config(data_dir, Config),
    {CRLPem, CRLDer} = read_crl(filename:join(DataDir, "intermediate-revoked.crl.pem")),
    TestPid = self(),
    ok = meck:new(emqx_crl_cache, [non_strict, passthrough, no_history, no_link]),
    meck:expect(
        emqx_crl_cache,
        http_get,
        fun(URL, _HTTPTimeout) ->
            ct:pal("http get crl ~p", [URL]),
            TestPid ! {http_get, URL},
            {ok, {{"HTTP/1.0", 200, 'OK'}, [], CRLPem}}
        end
    ),
    [
        {crl_pem, CRLPem},
        {crl_der, CRLDer}
        | Config
    ].

read_crl(Filename) ->
    {ok, PEM} = file:read_file(Filename),
    [{'CertificateList', DER, not_encrypted}] = public_key:pem_decode(PEM),
    {PEM, DER}.

end_per_testcase(TestCase, Config) when
    TestCase =:= t_update_listener;
    TestCase =:= t_update_listener_enable_disable;
    TestCase =:= t_validations
->
    Skip = proplists:get_bool(skip_does_not_apply, Config),
    case Skip of
        true ->
            ok;
        false ->
            end_per_testcase(common, Config)
    end;
end_per_testcase(_TestCase, Config) ->
    ok = snabbkaffe:stop(),
    clear_crl_cache(),
    _ = emqx_maybe:apply(
        fun emqx_crl_cache_http_server:stop/1,
        proplists:get_value(http_server, Config)
    ),
    _ = emqx_maybe:apply(
        fun emqx_cth_suite:stop/1,
        proplists:get_value(tc_apps, Config)
    ),
    catch meck:unload([emqx_crl_cache]),
    case whereis(emqx_crl_cache) of
        Pid when is_pid(Pid) ->
            MRef = monitor(process, Pid),
            unlink(Pid),
            exit(Pid, kill),
            receive
                {'DOWN', MRef, process, Pid, _} ->
                    ok
            end;
        _ ->
            ok
    end,
    ok.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

does_module_exist(Mod) ->
    case erlang:module_loaded(Mod) of
        true ->
            true;
        false ->
            case code:ensure_loaded(Mod) of
                ok ->
                    true;
                {module, Mod} ->
                    true;
                _ ->
                    false
            end
    end.

assert_http_get(URL) ->
    receive
        {http_get, URL} ->
            ok
    after 1000 ->
        ct:pal("mailbox: ~p", [process_info(self(), messages)]),
        error({should_have_requested, URL})
    end.

get_crl_cache_table() ->
    #state{certificate_db = [_, _, _, {Ref, _}]} = sys:get_state(ssl_manager),
    Ref.

start_crl_server(Port, CRLPem) ->
    {ok, LSock} = gen_tcp:listen(Port, [binary, {active, true}, reusedaddr]),
    spawn_link(fun() -> accept_loop(LSock, CRLPem) end),
    ok.

accept_loop(LSock, CRLPem) ->
    case gen_tcp:accept(LSock) of
        {ok, Sock} ->
            Worker = spawn_link(fun() -> crl_loop(Sock, CRLPem) end),
            gen_tcp:controlling_process(Sock, Worker),
            accept_loop(LSock, CRLPem);
        {error, Reason} ->
            error({accept_error, Reason})
    end.

crl_loop(Sock, CRLPem) ->
    receive
        {tcp, Sock, _Data} ->
            gen_tcp:send(Sock, CRLPem),
            crl_loop(Sock, CRLPem);
        _Msg ->
            ok
    end.

drain_msgs() ->
    receive
        _Msg ->
            drain_msgs()
    after 0 ->
        ok
    end.

clear_crl_cache() ->
    %% reset the CRL cache
    Ref = monitor(process, whereis(ssl_manager)),
    exit(whereis(ssl_manager), kill),
    receive
        {'DOWN', Ref, process, _, _} ->
            ok
    after 1_000 ->
        ct:fail("ssl_manager didn't die")
    end,
    ensure_ssl_manager_alive(),
    ok.

start_emqx_with_crl_cache(#{is_cached := IsCached} = Opts, TC, Config) ->
    DataDir = ?config(data_dir, Config),
    Overrides = maps:get(overrides, Opts, #{}),
    ListenerConf = #{
        enable => true,
        ssl_options => #{
            keyfile => filename:join(DataDir, "server.key.pem"),
            certfile => filename:join(DataDir, "server.cert.pem"),
            cacertfile => filename:join(DataDir, "ca-chain.cert.pem"),
            verify => verify_peer,
            enable_crl_check => true
        }
    },
    Conf = #{
        listeners => #{ssl => #{default => ListenerConf}},
        crl_cache => #{
            refresh_interval => <<"11m">>,
            http_timeout => <<"17s">>,
            capacity => 100
        }
    },
    Apps = emqx_cth_suite:start(
        [{emqx, #{config => emqx_utils_maps:deep_merge(Conf, Overrides)}}],
        #{work_dir => emqx_cth_suite:work_dir(TC, Config)}
    ),
    case IsCached of
        true ->
            %% wait the cache to be filled
            emqx_crl_cache:refresh(?DEFAULT_URL),
            ?assertReceive({http_get, <<?DEFAULT_URL>>});
        false ->
            %% ensure cache is empty
            ok
    end,
    Apps.

start_crl_server(CRLPem) ->
    application:ensure_all_started(cowboy),
    {ok, ServerPid} = emqx_crl_cache_http_server:start_link(self(), 9878, CRLPem, []),
    receive
        {ServerPid, ready} -> ok
    after 1000 -> error(timeout_starting_http_server)
    end,
    ServerPid.

request(Method, Url, QueryParams, Body) ->
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    Opts = #{return_all => true},
    case emqx_mgmt_api_test_util:request_api(Method, Url, QueryParams, AuthHeader, Body, Opts) of
        {ok, {Reason, Headers, BodyR}} ->
            {ok, {Reason, Headers, emqx_utils_json:decode(BodyR, [return_maps])}};
        Error ->
            Error
    end.

get_listener_via_api(ListenerId) ->
    Path = emqx_mgmt_api_test_util:api_path(["listeners", ListenerId]),
    request(get, Path, [], []).

update_listener_via_api(ListenerId, NewConfig) ->
    Path = emqx_mgmt_api_test_util:api_path(["listeners", ListenerId]),
    request(put, Path, [], NewConfig).

assert_successful_connection(Config) ->
    assert_successful_connection(Config, default).

assert_successful_connection(Config, ClientNum) ->
    DataDir = ?config(data_dir, Config),
    Num =
        case ClientNum of
            default -> "";
            _ -> integer_to_list(ClientNum)
        end,
    ClientCert = filename:join(DataDir, "client" ++ Num ++ ".cert.pem"),
    ClientKey = filename:join(DataDir, "client" ++ Num ++ ".key.pem"),
    %% 1) At first, the cache is empty, and the CRL is fetched and
    %% cached on the fly.
    {ok, C0} = emqtt:start_link([
        {ssl, true},
        {ssl_opts, [
            {certfile, ClientCert},
            {keyfile, ClientKey},
            {verify, verify_none}
        ]},
        {port, 8883}
    ]),
    ?tp_span(
        mqtt_client_connection,
        #{client_num => ClientNum},
        begin
            {ok, _} = emqtt:connect(C0),
            emqtt:stop(C0),
            ok
        end
    ).

trace_between(Trace0, Marker1, Marker2) ->
    {Trace1, [_ | _]} = ?split_trace_at(#{?snk_kind := Marker2}, Trace0),
    {[_ | _], [_ | Trace2]} = ?split_trace_at(#{?snk_kind := Marker1}, Trace1),
    Trace2.

of_kinds(Trace0, Kinds0) ->
    Kinds = sets:from_list(Kinds0, [{version, 2}]),
    lists:filter(
        fun(#{?snk_kind := K}) -> sets:is_element(K, Kinds) end,
        Trace0
    ).

ensure_ssl_manager_alive() ->
    ?retry(
        _Sleep0 = 200,
        _Attempts0 = 50,
        true = is_pid(whereis(ssl_manager))
    ).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_init_empty_urls(_Config) ->
    Ref = get_crl_cache_table(),
    ?assertEqual([], ets:tab2list(Ref)),
    emqx_config_handler:start_link(),
    ?assertMatch({ok, _}, emqx_crl_cache:start_link()),
    receive
        {http_get, _} ->
            error(should_not_make_http_request)
    after 1000 -> ok
    end,
    ?assertEqual([], ets:tab2list(Ref)),
    emqx_config_handler:stop(),
    ok.

t_update_config(_Config) ->
    Conf = #{
        <<"refresh_interval">> => <<"5m">>,
        <<"http_timeout">> => <<"10m">>,
        <<"capacity">> => 123
    },
    ?assertMatch({ok, _}, emqx:update_config([<<"crl_cache">>], Conf)),
    State = emqx_crl_cache:info(),
    ?assertEqual(
        #{
            refresh_interval => timer:minutes(5),
            http_timeout => timer:minutes(10),
            cache_capacity => 123
        },
        maps:with([refresh_interval, http_timeout, cache_capacity], State)
    ).

t_manual_refresh(Config) ->
    CRLDer = ?config(crl_der, Config),
    Ref = get_crl_cache_table(),
    ?assertEqual([], ets:tab2list(Ref)),
    emqx_config_handler:start_link(),
    {ok, _} = emqx_crl_cache:start_link(),
    URL = "http://localhost/crl.pem",
    URLBin = iolist_to_binary(URL),
    ok = snabbkaffe:start_trace(),
    ?wait_async_action(
        ?assertEqual(ok, emqx_crl_cache:refresh(URL)),
        #{?snk_kind := crl_cache_insert},
        5_000
    ),
    ok = snabbkaffe:stop(),
    ?assertEqual([{URLBin, [CRLDer]}], ets:tab2list(Ref)),
    emqx_config_handler:stop(),
    ok.

t_refresh_request_error(_Config) ->
    meck:expect(
        emqx_crl_cache,
        http_get,
        fun(_URL, _HTTPTimeout) ->
            {ok, {{"HTTP/1.0", 404, 'Not Found'}, [], <<"not found">>}}
        end
    ),
    emqx_config_handler:start_link(),
    {ok, _} = emqx_crl_cache:start_link(),
    URL = "http://localhost/crl.pem",
    ?check_trace(
        ?wait_async_action(
            ?assertEqual(ok, emqx_crl_cache:refresh(URL)),
            #{?snk_kind := crl_cache_insert},
            5_000
        ),
        fun(Trace) ->
            ?assertMatch(
                [#{error := {bad_response, #{code := 404}}}],
                ?of_kind(crl_refresh_failure, Trace)
            ),
            ok
        end
    ),
    ok = snabbkaffe:stop(),
    emqx_config_handler:stop(),
    ok.

t_refresh_invalid_response(_Config) ->
    meck:expect(
        emqx_crl_cache,
        http_get,
        fun(_URL, _HTTPTimeout) ->
            {ok, {{"HTTP/1.0", 200, 'OK'}, [], <<"not a crl">>}}
        end
    ),
    emqx_config_handler:start_link(),
    {ok, _} = emqx_crl_cache:start_link(),
    URL = "http://localhost/crl.pem",
    ?check_trace(
        ?wait_async_action(
            ?assertEqual(ok, emqx_crl_cache:refresh(URL)),
            #{?snk_kind := crl_cache_insert},
            5_000
        ),
        fun(Trace) ->
            ?assertMatch(
                [#{crls := []}],
                ?of_kind(crl_cache_insert, Trace)
            ),
            ok
        end
    ),
    ok = snabbkaffe:stop(),
    emqx_config_handler:stop(),
    ok.

t_refresh_http_error(_Config) ->
    meck:expect(
        emqx_crl_cache,
        http_get,
        fun(_URL, _HTTPTimeout) ->
            {error, timeout}
        end
    ),
    emqx_config_handler:start_link(),
    {ok, _} = emqx_crl_cache:start_link(),
    URL = "http://localhost/crl.pem",
    ?check_trace(
        ?wait_async_action(
            ?assertEqual(ok, emqx_crl_cache:refresh(URL)),
            #{?snk_kind := crl_cache_insert},
            5_000
        ),
        fun(Trace) ->
            ?assertMatch(
                [#{error := {http_error, timeout}}],
                ?of_kind(crl_refresh_failure, Trace)
            ),
            ok
        end
    ),
    ok = snabbkaffe:stop(),
    emqx_config_handler:stop(),
    ok.

t_unknown_messages(_Config) ->
    emqx_config_handler:start_link(),
    {ok, Server} = emqx_crl_cache:start_link(),
    gen_server:call(Server, foo),
    gen_server:cast(Server, foo),
    Server ! foo,
    emqx_config_handler:stop(),
    ok.

t_evict(_Config) ->
    emqx_config_handler:start_link(),
    {ok, _} = emqx_crl_cache:start_link(),
    URL = "http://localhost/crl.pem",
    URLBin = iolist_to_binary(URL),
    ?wait_async_action(
        ?assertEqual(ok, emqx_crl_cache:refresh(URL)),
        #{?snk_kind := crl_cache_insert},
        5_000
    ),
    Ref = get_crl_cache_table(),
    ?assertMatch([{URLBin, _}], ets:tab2list(Ref)),
    {ok, {ok, _}} = ?wait_async_action(
        emqx_crl_cache:evict(URL),
        #{?snk_kind := crl_cache_evict}
    ),
    ?assertEqual([], ets:tab2list(Ref)),
    emqx_config_handler:stop(),
    ok.

t_cache(Config) ->
    DataDir = ?config(data_dir, Config),
    ClientCert = filename:join(DataDir, "client.cert.pem"),
    ClientKey = filename:join(DataDir, "client.key.pem"),
    %% 1) At first, the cache is empty, and the CRL is fetched and
    %% cached on the fly.
    {ok, C0} = emqtt:start_link([
        {ssl, true},
        {ssl_opts, [
            {certfile, ClientCert},
            {keyfile, ClientKey},
            {verify, verify_none}
        ]},
        {port, 8883}
    ]),
    {ok, _} = emqtt:connect(C0),
    receive
        {http_get, _} -> ok
    after 500 ->
        emqtt:stop(C0),
        error(should_have_checked_server)
    end,
    emqtt:stop(C0),
    %% 2) When another client using the cached CRL URL connects later,
    %% it uses the cache.
    {ok, C1} = emqtt:start_link([
        {ssl, true},
        {ssl_opts, [
            {certfile, ClientCert},
            {keyfile, ClientKey},
            {verify, verify_none}
        ]},
        {port, 8883}
    ]),
    {ok, _} = emqtt:connect(C1),
    receive
        {http_get, _} ->
            emqtt:stop(C1),
            error(should_not_have_checked_server)
    after 500 -> ok
    end,
    emqtt:stop(C1),

    ok.

t_cache_overflow(Config) ->
    %% we have capacity = 2 here.
    ?check_trace(
        begin
            %% First and second connections goes into the cache
            ?tp(first_connections, #{}),
            assert_successful_connection(Config, 1),
            assert_successful_connection(Config, 2),
            %% These should be cached
            ?tp(first_reconnections, #{}),
            assert_successful_connection(Config, 1),
            assert_successful_connection(Config, 2),
            %% A third client connects and evicts the oldest URL (1)
            ?tp(first_eviction, #{}),
            assert_successful_connection(Config, 3),
            assert_successful_connection(Config, 3),
            %% URL (1) connects again and needs to be re-cached; this
            %% time, (2) gets evicted
            ?tp(second_eviction, #{}),
            assert_successful_connection(Config, 1),
            %% TODO: force race condition where the same URL is fetched
            %% at the same time and tries to be registered
            ?tp(test_end, #{}),
            ok
        end,
        fun(Trace) ->
            URL1 = "http://localhost:9878/intermediate1.crl.pem",
            URL2 = "http://localhost:9878/intermediate2.crl.pem",
            URL3 = "http://localhost:9878/intermediate3.crl.pem",
            Kinds = [
                mqtt_client_connection,
                new_crl_url_inserted,
                crl_cache_ensure_timer,
                crl_cache_overflow
            ],
            Trace1 = of_kinds(
                trace_between(Trace, first_connections, first_reconnections),
                Kinds
            ),
            ?assertMatch(
                [
                    #{
                        ?snk_kind := mqtt_client_connection,
                        ?snk_span := start,
                        client_num := 1
                    },
                    #{
                        ?snk_kind := new_crl_url_inserted,
                        url := URL1
                    },
                    #{
                        ?snk_kind := crl_cache_ensure_timer,
                        url := URL1
                    },
                    #{
                        ?snk_kind := mqtt_client_connection,
                        ?snk_span := {complete, ok},
                        client_num := 1
                    },
                    #{
                        ?snk_kind := mqtt_client_connection,
                        ?snk_span := start,
                        client_num := 2
                    },
                    #{
                        ?snk_kind := new_crl_url_inserted,
                        url := URL2
                    },
                    #{
                        ?snk_kind := crl_cache_ensure_timer,
                        url := URL2
                    },
                    #{
                        ?snk_kind := mqtt_client_connection,
                        ?snk_span := {complete, ok},
                        client_num := 2
                    }
                ],
                Trace1
            ),
            Trace2 = of_kinds(
                trace_between(Trace, first_reconnections, first_eviction),
                Kinds
            ),
            ?assertMatch(
                [
                    #{
                        ?snk_kind := mqtt_client_connection,
                        ?snk_span := start,
                        client_num := 1
                    },
                    #{
                        ?snk_kind := mqtt_client_connection,
                        ?snk_span := {complete, ok},
                        client_num := 1
                    },
                    #{
                        ?snk_kind := mqtt_client_connection,
                        ?snk_span := start,
                        client_num := 2
                    },
                    #{
                        ?snk_kind := mqtt_client_connection,
                        ?snk_span := {complete, ok},
                        client_num := 2
                    }
                ],
                Trace2
            ),
            Trace3 = of_kinds(
                trace_between(Trace, first_eviction, second_eviction),
                Kinds
            ),
            ?assertMatch(
                [
                    #{
                        ?snk_kind := mqtt_client_connection,
                        ?snk_span := start,
                        client_num := 3
                    },
                    #{
                        ?snk_kind := new_crl_url_inserted,
                        url := URL3
                    },
                    #{
                        ?snk_kind := crl_cache_overflow,
                        oldest_url := URL1
                    },
                    #{
                        ?snk_kind := crl_cache_ensure_timer,
                        url := URL3
                    },
                    #{
                        ?snk_kind := mqtt_client_connection,
                        ?snk_span := {complete, ok},
                        client_num := 3
                    },
                    #{
                        ?snk_kind := mqtt_client_connection,
                        ?snk_span := start,
                        client_num := 3
                    },
                    #{
                        ?snk_kind := mqtt_client_connection,
                        ?snk_span := {complete, ok},
                        client_num := 3
                    }
                ],
                Trace3
            ),
            Trace4 = of_kinds(
                trace_between(Trace, second_eviction, test_end),
                Kinds
            ),
            ?assertMatch(
                [
                    #{
                        ?snk_kind := mqtt_client_connection,
                        ?snk_span := start,
                        client_num := 1
                    },
                    #{
                        ?snk_kind := new_crl_url_inserted,
                        url := URL1
                    },
                    #{
                        ?snk_kind := crl_cache_overflow,
                        oldest_url := URL2
                    },
                    #{
                        ?snk_kind := crl_cache_ensure_timer,
                        url := URL1
                    },
                    #{
                        ?snk_kind := mqtt_client_connection,
                        ?snk_span := {complete, ok},
                        client_num := 1
                    }
                ],
                Trace4
            ),
            ok
        end
    ).

%% check that the URL in the certificate is *not* checked if the cache
%% contains that URL.
t_filled_cache(Config) ->
    DataDir = ?config(data_dir, Config),
    ClientCert = filename:join(DataDir, "client.cert.pem"),
    ClientKey = filename:join(DataDir, "client.key.pem"),
    {ok, C} = emqtt:start_link([
        {ssl, true},
        {ssl_opts, [
            {certfile, ClientCert},
            {keyfile, ClientKey},
            {verify, verify_none}
        ]},
        {port, 8883}
    ]),
    {ok, _} = emqtt:connect(C),
    receive
        http_get ->
            emqtt:stop(C),
            error(should_have_used_cache)
    after 500 -> ok
    end,
    emqtt:stop(C),
    ok.

%% If the CRL is not cached when the client tries to connect and the
%% CRL server is unreachable, the client will be denied connection.
t_not_cached_and_unreachable(Config) ->
    DataDir = ?config(data_dir, Config),
    ClientCert = filename:join(DataDir, "client.cert.pem"),
    ClientKey = filename:join(DataDir, "client.key.pem"),
    {ok, C} = emqtt:start_link([
        {ssl, true},
        {ssl_opts, [
            {certfile, ClientCert},
            {keyfile, ClientKey},
            {verify, verify_none}
        ]},
        {port, 8883}
    ]),
    Ref = get_crl_cache_table(),
    ?assertEqual([], ets:tab2list(Ref)),
    unlink(C),
    ?assertMatch({error, {ssl_error, _Sock, {tls_alert, {bad_certificate, _}}}}, emqtt:connect(C)),
    ok.

t_revoked(Config) ->
    DataDir = ?config(data_dir, Config),
    ClientCert = filename:join(DataDir, "client-revoked.cert.pem"),
    ClientKey = filename:join(DataDir, "client-revoked.key.pem"),
    {ok, C} = emqtt:start_link([
        {connect_timeout, 2},
        {ssl, true},
        {ssl_opts, [
            {certfile, ClientCert},
            {keyfile, ClientKey},
            {verify, verify_none}
        ]},
        {port, 8883}
    ]),
    unlink(C),
    case emqtt:connect(C) of
        {error, {ssl_error, _Sock, {tls_alert, {certificate_revoked, _}}}} ->
            ok;
        {error, closed} ->
            %% this happens due to an unidentified race-condition
            ok
    end.

t_revoke_then_refresh(Config) ->
    DataDir = ?config(data_dir, Config),
    CRLPemRevoked = ?config(crl_pem_revoked, Config),
    ClientCert = filename:join(DataDir, "client-revoked.cert.pem"),
    ClientKey = filename:join(DataDir, "client-revoked.key.pem"),
    {ok, C0} = emqtt:start_link([
        {ssl, true},
        {ssl_opts, [
            {certfile, ClientCert},
            {keyfile, ClientKey},
            {verify, verify_none}
        ]},
        {port, 8883}
    ]),
    %% At first, the CRL contains no revoked entries, so the client
    %% should be allowed connection.
    ?assertMatch({ok, _}, emqtt:connect(C0)),
    emqtt:stop(C0),

    %% Now we update the CRL on the server and wait for the cache to
    %% be refreshed.
    {true, {ok, _}} =
        ?wait_async_action(
            emqx_crl_cache_http_server:set_crl(CRLPemRevoked),
            #{?snk_kind := crl_refresh_timer_done},
            70_000
        ),

    %% The *same client* should now be denied connection.
    {ok, C1} = emqtt:start_link([
        {ssl, true},
        {ssl_opts, [
            {certfile, ClientCert},
            {keyfile, ClientKey},
            {verify, verify_none}
        ]},
        {port, 8883}
    ]),
    unlink(C1),
    ?assertMatch(
        {error, {ssl_error, _Sock, {tls_alert, {certificate_revoked, _}}}}, emqtt:connect(C1)
    ),
    ok.

%% check that we can start with a non-crl listener and restart it with
%% the new crl config.
t_update_listener(Config) ->
    case proplists:get_bool(skip_does_not_apply, Config) of
        true ->
            ok;
        false ->
            do_t_update_listener(Config)
    end.

do_t_update_listener(Config) ->
    DataDir = ?config(data_dir, Config),
    Keyfile = filename:join([DataDir, "server.key.pem"]),
    Certfile = filename:join([DataDir, "server.cert.pem"]),
    Cacertfile = filename:join([DataDir, "ca-chain.cert.pem"]),
    ClientCert = filename:join(DataDir, "client-revoked.cert.pem"),
    ClientKey = filename:join(DataDir, "client-revoked.key.pem"),

    %% no crl at first
    ListenerId = "ssl:default",
    {ok, {{_, 200, _}, _, ListenerData0}} = get_listener_via_api(ListenerId),
    ?assertMatch(
        #{
            <<"ssl_options">> :=
                #{
                    <<"enable_crl_check">> := false,
                    <<"verify">> := <<"verify_peer">>
                }
        },
        ListenerData0
    ),
    {ok, C0} = emqtt:start_link([
        {ssl, true},
        {ssl_opts, [
            {certfile, ClientCert},
            {keyfile, ClientKey},
            {verify, verify_none}
        ]},
        {port, 8883}
    ]),
    %% At first, the CRL contains no revoked entries, so the client
    %% should be allowed connection.
    ?assertMatch({ok, _}, emqtt:connect(C0)),
    emqtt:stop(C0),

    %% configure crl
    CRLConfig =
        #{
            <<"ssl_options">> =>
                #{
                    <<"keyfile">> => Keyfile,
                    <<"certfile">> => Certfile,
                    <<"cacertfile">> => Cacertfile,
                    <<"enable_crl_check">> => true
                }
        },
    ListenerData1 = emqx_utils_maps:deep_merge(ListenerData0, CRLConfig),
    {ok, {_, _, ListenerData2}} = update_listener_via_api(ListenerId, ListenerData1),
    ?assertMatch(
        #{
            <<"ssl_options">> :=
                #{
                    <<"enable_crl_check">> := true,
                    <<"verify">> := <<"verify_peer">>
                }
        },
        ListenerData2
    ),

    %% Now should use CRL information to block connection
    {ok, C1} = emqtt:start_link([
        {ssl, true},
        {ssl_opts, [
            {certfile, ClientCert},
            {keyfile, ClientKey},
            {verify, verify_none}
        ]},
        {port, 8883}
    ]),
    unlink(C1),
    ?assertMatch(
        {error, {ssl_error, _Sock, {tls_alert, {certificate_revoked, _}}}}, emqtt:connect(C1)
    ),
    assert_http_get(<<?DEFAULT_URL>>),

    ok.

t_validations(Config) ->
    case proplists:get_bool(skip_does_not_apply, Config) of
        true ->
            ok;
        false ->
            do_t_validations(Config)
    end.

do_t_validations(_Config) ->
    ListenerId = <<"ssl:default">>,
    {ok, {{_, 200, _}, _, ListenerData0}} = get_listener_via_api(ListenerId),

    ListenerData1 =
        emqx_utils_maps:deep_merge(
            ListenerData0,
            #{
                <<"ssl_options">> =>
                    #{
                        <<"enable_crl_check">> => true,
                        <<"verify">> => <<"verify_none">>
                    }
            }
        ),
    {error, {_, _, ResRaw1}} = update_listener_via_api(ListenerId, ListenerData1),
    #{<<"code">> := <<"BAD_REQUEST">>, <<"message">> := MsgRaw1} =
        emqx_utils_json:decode(ResRaw1, [return_maps]),
    ?assertMatch(
        #{
            <<"kind">> := <<"validation_error">>,
            <<"reason">> :=
                <<"verify must be verify_peer when CRL check is enabled">>
        },
        emqx_utils_json:decode(MsgRaw1, [return_maps])
    ),

    ok.

%% Checks that if CRL is ever enabled and then disabled, clients can connect, even if they
%% would otherwise not have their corresponding CRLs cached and fail with `{bad_crls,
%% no_relevant_crls}`.
t_update_listener_enable_disable(Config) ->
    case proplists:get_bool(skip_does_not_apply, Config) of
        true ->
            ct:pal("skipping as this test does not apply in this profile"),
            ok;
        false ->
            do_t_update_listener_enable_disable(Config)
    end.

do_t_update_listener_enable_disable(Config) ->
    DataDir = ?config(data_dir, Config),
    Keyfile = filename:join([DataDir, "server.key.pem"]),
    Certfile = filename:join([DataDir, "server.cert.pem"]),
    Cacertfile = filename:join([DataDir, "ca-chain.cert.pem"]),
    ClientCert = filename:join(DataDir, "client.cert.pem"),
    ClientKey = filename:join(DataDir, "client.key.pem"),

    ListenerId = "ssl:default",
    %% Enable CRL
    {ok, {{_, 200, _}, _, ListenerData0}} = get_listener_via_api(ListenerId),
    CRLConfig0 =
        #{
            <<"ssl_options">> =>
                #{
                    <<"keyfile">> => Keyfile,
                    <<"certfile">> => Certfile,
                    <<"cacertfile">> => Cacertfile,
                    <<"enable_crl_check">> => true,
                    <<"fail_if_no_peer_cert">> => true
                }
        },
    ListenerData1 = emqx_utils_maps:deep_merge(ListenerData0, CRLConfig0),
    {ok, {_, _, ListenerData2}} = update_listener_via_api(ListenerId, ListenerData1),
    ?assertMatch(
        #{
            <<"ssl_options">> :=
                #{
                    <<"enable_crl_check">> := true,
                    <<"verify">> := <<"verify_peer">>,
                    <<"fail_if_no_peer_cert">> := true
                }
        },
        ListenerData2
    ),

    %% Disable CRL
    CRLConfig1 =
        #{
            <<"ssl_options">> =>
                #{
                    <<"keyfile">> => Keyfile,
                    <<"certfile">> => Certfile,
                    <<"cacertfile">> => Cacertfile,
                    <<"enable_crl_check">> => false,
                    <<"fail_if_no_peer_cert">> => true
                }
        },
    ListenerData3 = emqx_utils_maps:deep_merge(ListenerData2, CRLConfig1),
    redbug:start(
        [
            "esockd_server:get_listener_prop -> return",
            "esockd_server:set_listener_prop -> return",
            "esockd:merge_opts -> return",
            "esockd_listener_sup:set_options -> return",
            "emqx_listeners:inject_crl_config -> return"
        ],
        [{msgs, 100}]
    ),
    {ok, {_, _, ListenerData4}} = update_listener_via_api(ListenerId, ListenerData3),
    ?assertMatch(
        #{
            <<"ssl_options">> :=
                #{
                    <<"enable_crl_check">> := false,
                    <<"verify">> := <<"verify_peer">>,
                    <<"fail_if_no_peer_cert">> := true
                }
        },
        ListenerData4
    ),

    %% Now the client that would be blocked tries to connect and should now be allowed.
    {ok, C} = emqtt:start_link([
        {ssl, true},
        {ssl_opts, [
            {certfile, ClientCert},
            {keyfile, ClientKey},
            {verify, verify_none}
        ]},
        {port, 8883}
    ]),
    ?assertMatch({ok, _}, emqtt:connect(C)),
    emqtt:stop(C),

    ?assertNotReceive({http_get, _}),

    ok.
