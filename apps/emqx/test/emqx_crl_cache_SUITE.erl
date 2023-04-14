%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_crl_cache_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
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
    application:load(emqx),
    emqx_config:save_schema_mod_and_names(emqx_schema),
    emqx_common_test_helpers:boot_modules(all),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TestCase, Config) when
    TestCase =:= t_cache;
    TestCase =:= t_filled_cache;
    TestCase =:= t_revoked
->
    ct:timetrap({seconds, 30}),
    DataDir = ?config(data_dir, Config),
    CRLFile = filename:join([DataDir, "intermediate-revoked.crl.pem"]),
    {ok, CRLPem} = file:read_file(CRLFile),
    [{'CertificateList', CRLDer, not_encrypted}] = public_key:pem_decode(CRLPem),
    ok = snabbkaffe:start_trace(),
    ServerPid = start_crl_server(CRLPem),
    IsCached = lists:member(TestCase, [t_filled_cache, t_revoked]),
    ok = setup_crl_options(Config, #{is_cached => IsCached}),
    [
        {crl_pem, CRLPem},
        {crl_der, CRLDer},
        {http_server, ServerPid}
        | Config
    ];
init_per_testcase(t_revoke_then_refresh, Config) ->
    ct:timetrap({seconds, 120}),
    DataDir = ?config(data_dir, Config),
    CRLFileNotRevoked = filename:join([DataDir, "intermediate-not-revoked.crl.pem"]),
    {ok, CRLPemNotRevoked} = file:read_file(CRLFileNotRevoked),
    [{'CertificateList', CRLDerNotRevoked, not_encrypted}] = public_key:pem_decode(
        CRLPemNotRevoked
    ),
    CRLFileRevoked = filename:join([DataDir, "intermediate-revoked.crl.pem"]),
    {ok, CRLPemRevoked} = file:read_file(CRLFileRevoked),
    [{'CertificateList', CRLDerRevoked, not_encrypted}] = public_key:pem_decode(CRLPemRevoked),
    ok = snabbkaffe:start_trace(),
    ServerPid = start_crl_server(CRLPemNotRevoked),
    ExtraVars = #{refresh_interval => <<"10s">>},
    ok = setup_crl_options(Config, #{is_cached => true, extra_vars => ExtraVars}),
    [
        {crl_pem_not_revoked, CRLPemNotRevoked},
        {crl_der_not_revoked, CRLDerNotRevoked},
        {crl_pem_revoked, CRLPemRevoked},
        {crl_der_revoked, CRLDerRevoked},
        {http_server, ServerPid}
        | Config
    ];
init_per_testcase(t_cache_overflow, Config) ->
    ct:timetrap({seconds, 120}),
    DataDir = ?config(data_dir, Config),
    CRLFileRevoked = filename:join([DataDir, "intermediate-revoked.crl.pem"]),
    {ok, CRLPemRevoked} = file:read_file(CRLFileRevoked),
    ok = snabbkaffe:start_trace(),
    ServerPid = start_crl_server(CRLPemRevoked),
    ExtraVars = #{cache_capacity => <<"2">>},
    ok = setup_crl_options(Config, #{is_cached => false, extra_vars => ExtraVars}),
    [
        {http_server, ServerPid}
        | Config
    ];
init_per_testcase(t_not_cached_and_unreachable, Config) ->
    ct:timetrap({seconds, 30}),
    DataDir = ?config(data_dir, Config),
    CRLFile = filename:join([DataDir, "intermediate-revoked.crl.pem"]),
    {ok, CRLPem} = file:read_file(CRLFile),
    [{'CertificateList', CRLDer, not_encrypted}] = public_key:pem_decode(CRLPem),
    ok = snabbkaffe:start_trace(),
    application:stop(cowboy),
    ok = setup_crl_options(Config, #{is_cached => false}),
    [
        {crl_pem, CRLPem},
        {crl_der, CRLDer}
        | Config
    ];
init_per_testcase(t_refresh_config, Config) ->
    ct:timetrap({seconds, 30}),
    DataDir = ?config(data_dir, Config),
    CRLFile = filename:join([DataDir, "intermediate-revoked.crl.pem"]),
    {ok, CRLPem} = file:read_file(CRLFile),
    [{'CertificateList', CRLDer, not_encrypted}] = public_key:pem_decode(CRLPem),
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
    ok = snabbkaffe:start_trace(),
    ok = setup_crl_options(Config, #{is_cached => false}),
    [
        {crl_pem, CRLPem},
        {crl_der, CRLDer}
        | Config
    ];
init_per_testcase(TestCase, Config) when
    TestCase =:= t_update_listener;
    TestCase =:= t_validations
->
    %% when running emqx standalone tests, we can't use those
    %% features.
    case does_module_exist(emqx_mgmt_api_test_util) of
        true ->
            ct:timetrap({seconds, 30}),
            DataDir = ?config(data_dir, Config),
            PrivDir = ?config(priv_dir, Config),
            CRLFile = filename:join([DataDir, "intermediate-revoked.crl.pem"]),
            {ok, CRLPem} = file:read_file(CRLFile),
            ok = snabbkaffe:start_trace(),
            ServerPid = start_crl_server(CRLPem),
            ConfFilePath = filename:join([DataDir, "emqx_just_verify.conf"]),
            emqx_mgmt_api_test_util:init_suite(
                [emqx_conf],
                fun emqx_mgmt_api_test_util:set_special_configs/1,
                #{
                    extra_mustache_vars => #{
                        test_data_dir => DataDir,
                        test_priv_dir => PrivDir
                    },
                    conf_file_path => ConfFilePath
                }
            ),
            [
                {http_server, ServerPid}
                | Config
            ];
        false ->
            [{skip_does_not_apply, true} | Config]
    end;
init_per_testcase(_TestCase, Config) ->
    ct:timetrap({seconds, 30}),
    DataDir = ?config(data_dir, Config),
    CRLFile = filename:join([DataDir, "intermediate-revoked.crl.pem"]),
    {ok, CRLPem} = file:read_file(CRLFile),
    [{'CertificateList', CRLDer, not_encrypted}] = public_key:pem_decode(CRLPem),
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
    ok = snabbkaffe:start_trace(),
    [
        {crl_pem, CRLPem},
        {crl_der, CRLDer}
        | Config
    ].

end_per_testcase(TestCase, Config) when
    TestCase =:= t_cache;
    TestCase =:= t_filled_cache;
    TestCase =:= t_revoked
->
    ServerPid = ?config(http_server, Config),
    emqx_crl_cache_http_server:stop(ServerPid),
    emqx_common_test_helpers:stop_apps([]),
    clear_listeners(),
    application:stop(cowboy),
    clear_crl_cache(),
    ok = snabbkaffe:stop(),
    ok;
end_per_testcase(TestCase, Config) when
    TestCase =:= t_revoke_then_refresh;
    TestCase =:= t_cache_overflow
->
    ServerPid = ?config(http_server, Config),
    emqx_crl_cache_http_server:stop(ServerPid),
    emqx_common_test_helpers:stop_apps([]),
    clear_listeners(),
    clear_crl_cache(),
    application:stop(cowboy),
    ok = snabbkaffe:stop(),
    ok;
end_per_testcase(t_not_cached_and_unreachable, _Config) ->
    emqx_common_test_helpers:stop_apps([]),
    clear_listeners(),
    clear_crl_cache(),
    ok = snabbkaffe:stop(),
    ok;
end_per_testcase(t_refresh_config, _Config) ->
    meck:unload([emqx_crl_cache]),
    clear_crl_cache(),
    emqx_common_test_helpers:stop_apps([]),
    clear_listeners(),
    clear_crl_cache(),
    application:stop(cowboy),
    ok = snabbkaffe:stop(),
    ok;
end_per_testcase(TestCase, Config) when
    TestCase =:= t_update_listener;
    TestCase =:= t_validations
->
    Skip = proplists:get_bool(skip_does_not_apply, Config),
    case Skip of
        true ->
            ok;
        false ->
            ServerPid = ?config(http_server, Config),
            emqx_crl_cache_http_server:stop(ServerPid),
            emqx_mgmt_api_test_util:end_suite([emqx_conf]),
            clear_listeners(),
            ok = snabbkaffe:stop(),
            clear_crl_cache(),
            ok
    end;
end_per_testcase(_TestCase, _Config) ->
    meck:unload([emqx_crl_cache]),
    clear_crl_cache(),
    ok = snabbkaffe:stop(),
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

clear_listeners() ->
    emqx_config:put([listeners], #{}),
    emqx_config:put_raw([listeners], #{}),
    ok.

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
    exit(whereis(ssl_manager), kill),
    ok.

force_cacertfile(Cacertfile) ->
    {SSLListeners0, OtherListeners} = lists:partition(
        fun(#{proto := Proto}) -> Proto =:= ssl end,
        emqx:get_env(listeners)
    ),
    SSLListeners =
        lists:map(
            fun(Listener = #{opts := Opts0}) ->
                SSLOpts0 = proplists:get_value(ssl_options, Opts0),
                %% it injects some garbage...
                SSLOpts1 = lists:keydelete(cacertfile, 1, lists:keydelete(cacertfile, 1, SSLOpts0)),
                SSLOpts2 = [{cacertfile, Cacertfile} | SSLOpts1],
                Opts1 = lists:keyreplace(ssl_options, 1, Opts0, {ssl_options, SSLOpts2}),
                Listener#{opts => Opts1}
            end,
            SSLListeners0
        ),
    application:set_env(emqx, listeners, SSLListeners ++ OtherListeners),
    ok.

setup_crl_options(Config, #{is_cached := IsCached} = Opts) ->
    DataDir = ?config(data_dir, Config),
    ConfFilePath = filename:join([DataDir, "emqx.conf"]),
    Defaults = #{
        refresh_interval => <<"11m">>,
        cache_capacity => <<"100">>,
        test_data_dir => DataDir
    },
    ExtraVars0 = maps:get(extra_vars, Opts, #{}),
    ExtraVars = maps:merge(Defaults, ExtraVars0),
    emqx_common_test_helpers:start_apps(
        [],
        fun(_) -> ok end,
        #{
            extra_mustache_vars => ExtraVars,
            conf_file_path => ConfFilePath
        }
    ),
    case IsCached of
        true ->
            %% wait the cache to be filled
            emqx_crl_cache:refresh(?DEFAULT_URL),
            receive
                {http_get, <<?DEFAULT_URL>>} -> ok
            after 1_000 ->
                ct:pal("mailbox: ~p", [process_info(self(), messages)]),
                error(crl_cache_not_filled)
            end;
        false ->
            %% ensure cache is empty
            clear_crl_cache(),
            ct:sleep(200),
            ok
    end,
    drain_msgs(),
    ok.

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
            {keyfile, ClientKey}
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

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_init_empty_urls(_Config) ->
    Ref = get_crl_cache_table(),
    ?assertEqual([], ets:tab2list(Ref)),
    ?assertMatch({ok, _}, emqx_crl_cache:start_link()),
    receive
        {http_get, _} ->
            error(should_not_make_http_request)
    after 1000 -> ok
    end,
    ?assertEqual([], ets:tab2list(Ref)),
    ok.

t_manual_refresh(Config) ->
    CRLDer = ?config(crl_der, Config),
    Ref = get_crl_cache_table(),
    ?assertEqual([], ets:tab2list(Ref)),
    {ok, _} = emqx_crl_cache:start_link(),
    URL = "http://localhost/crl.pem",
    ok = snabbkaffe:start_trace(),
    ?wait_async_action(
        ?assertEqual(ok, emqx_crl_cache:refresh(URL)),
        #{?snk_kind := crl_cache_insert},
        5_000
    ),
    ok = snabbkaffe:stop(),
    ?assertEqual(
        [{"crl.pem", [CRLDer]}],
        ets:tab2list(Ref)
    ),
    ok.

t_refresh_request_error(_Config) ->
    meck:expect(
        emqx_crl_cache,
        http_get,
        fun(_URL, _HTTPTimeout) ->
            {ok, {{"HTTP/1.0", 404, 'Not Found'}, [], <<"not found">>}}
        end
    ),
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
    ok.

t_refresh_invalid_response(_Config) ->
    meck:expect(
        emqx_crl_cache,
        http_get,
        fun(_URL, _HTTPTimeout) ->
            {ok, {{"HTTP/1.0", 200, 'OK'}, [], <<"not a crl">>}}
        end
    ),
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
    ok.

t_refresh_http_error(_Config) ->
    meck:expect(
        emqx_crl_cache,
        http_get,
        fun(_URL, _HTTPTimeout) ->
            {error, timeout}
        end
    ),
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
    ok.

t_unknown_messages(_Config) ->
    {ok, Server} = emqx_crl_cache:start_link(),
    gen_server:call(Server, foo),
    gen_server:cast(Server, foo),
    Server ! foo,
    ok.

t_evict(_Config) ->
    {ok, _} = emqx_crl_cache:start_link(),
    URL = "http://localhost/crl.pem",
    ?wait_async_action(
        ?assertEqual(ok, emqx_crl_cache:refresh(URL)),
        #{?snk_kind := crl_cache_insert},
        5_000
    ),
    Ref = get_crl_cache_table(),
    ?assertMatch([{"crl.pem", _}], ets:tab2list(Ref)),
    {ok, {ok, _}} = ?wait_async_action(
        emqx_crl_cache:evict(URL),
        #{?snk_kind := crl_cache_evict}
    ),
    ?assertEqual([], ets:tab2list(Ref)),
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
            {keyfile, ClientKey}
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
            {keyfile, ClientKey}
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
            {keyfile, ClientKey}
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
            {keyfile, ClientKey}
        ]},
        {port, 8883}
    ]),
    Ref = get_crl_cache_table(),
    ?assertEqual([], ets:tab2list(Ref)),
    process_flag(trap_exit, true),
    ?assertMatch({error, {{shutdown, {tls_alert, {bad_certificate, _}}}, _}}, emqtt:connect(C)),
    ok.

t_revoked(Config) ->
    DataDir = ?config(data_dir, Config),
    ClientCert = filename:join(DataDir, "client-revoked.cert.pem"),
    ClientKey = filename:join(DataDir, "client-revoked.key.pem"),
    {ok, C} = emqtt:start_link([
        {ssl, true},
        {ssl_opts, [
            {certfile, ClientCert},
            {keyfile, ClientKey}
        ]},
        {port, 8883}
    ]),
    process_flag(trap_exit, true),
    Res = emqtt:connect(C),
    %% apparently, sometimes there's some race condition in
    %% `emqtt_sock:ssl_upgrade' when it calls
    %% `ssl:conetrolling_process' and a bad match happens at that
    %% point.
    case Res of
        {error, {{shutdown, {tls_alert, {certificate_revoked, _}}}, _}} ->
            ok;
        {error, closed} ->
            %% race condition?
            ok;
        _ ->
            ct:fail("unexpected result: ~p", [Res])
    end,
    ok.

t_revoke_then_refresh(Config) ->
    DataDir = ?config(data_dir, Config),
    CRLPemRevoked = ?config(crl_pem_revoked, Config),
    ClientCert = filename:join(DataDir, "client-revoked.cert.pem"),
    ClientKey = filename:join(DataDir, "client-revoked.key.pem"),
    {ok, C0} = emqtt:start_link([
        {ssl, true},
        {ssl_opts, [
            {certfile, ClientCert},
            {keyfile, ClientKey}
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
            {keyfile, ClientKey}
        ]},
        {port, 8883}
    ]),
    process_flag(trap_exit, true),
    ?assertMatch(
        {error, {{shutdown, {tls_alert, {certificate_revoked, _}}}, _}}, emqtt:connect(C1)
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
            {keyfile, ClientKey}
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
    process_flag(trap_exit, true),
    {ok, C1} = emqtt:start_link([
        {ssl, true},
        {ssl_opts, [
            {certfile, ClientCert},
            {keyfile, ClientKey}
        ]},
        {port, 8883}
    ]),
    ?assertMatch(
        {error, {{shutdown, {tls_alert, {certificate_revoked, _}}}, _}}, emqtt:connect(C1)
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
            <<"mismatches">> :=
                #{
                    <<"listeners:ssl_not_required_bind">> :=
                        #{
                            <<"reason">> :=
                                <<"verify must be verify_peer when CRL check is enabled">>
                        }
                }
        },
        emqx_utils_json:decode(MsgRaw1, [return_maps])
    ),

    ok.
