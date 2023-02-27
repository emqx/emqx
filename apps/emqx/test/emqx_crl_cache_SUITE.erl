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
    TestCase =:= t_empty_cache;
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
    NewRefreshInterval = timer:seconds(10),
    ExtraHandler =
        fun
            (emqx) ->
                emqx_config:put([crl_cache, refresh_interval], NewRefreshInterval),
                ok;
            (_) ->
                ok
        end,
    ok = setup_crl_options(Config, #{is_cached => true, extra_handler => ExtraHandler}),
    %% for some reason, the config is overridden even with the extra
    %% handler....
    emqx_config:put([crl_cache, refresh_interval], NewRefreshInterval),
    {ok, {ok, _}} =
        ?wait_async_action(
            emqx_crl_cache:refresh_config(),
            #{?snk_kind := crl_cache_refresh_config},
            _Timeout = 10_000
        ),
    [
        {crl_pem_not_revoked, CRLPemNotRevoked},
        {crl_der_not_revoked, CRLDerNotRevoked},
        {crl_pem_revoked, CRLPemRevoked},
        {crl_der_revoked, CRLDerRevoked},
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
                    extra_mustache_vars => [
                        {test_data_dir, DataDir},
                        {test_priv_dir, PrivDir}
                    ],
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
    TestCase =:= t_empty_cache;
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
end_per_testcase(t_revoke_then_refresh, Config) ->
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

setup_crl_options(Config, #{is_cached := IsCached}) ->
    DataDir = ?config(data_dir, Config),
    URLs =
        case IsCached of
            false -> [];
            true -> ["http://localhost:9878/intermediate.crl.pem"]
        end,
    ConfFilePath = filename:join([DataDir, "emqx.conf"]),
    emqx_common_test_helpers:start_apps(
        [],
        fun(_) -> ok end,
        #{
            extra_mustache_vars => [
                {test_data_dir, DataDir},
                {crl_cache_urls, io_lib:format("~p", [URLs])}
            ],
            conf_file_path => ConfFilePath
        }
    ),
    case IsCached of
        true ->
            %% wait the cache to be filled
            receive
                http_get -> ok
            after 1_000 -> error(crl_cache_not_filled)
            end,
            %% there's a second get because setting up the listener
            %% always makes the cache refresh immediately, specially
            %% when changing configurations.
            receive
                http_get -> ok
            after 200 -> error(crl_cache_not_filled)
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
            {ok, {Reason, Headers, emqx_json:decode(BodyR, [return_maps])}};
        Error ->
            Error
    end.

get_listener_via_api(ListenerId) ->
    Path = emqx_mgmt_api_test_util:api_path(["listeners", ListenerId]),
    request(get, Path, [], []).

update_listener_via_api(ListenerId, NewConfig) ->
    Path = emqx_mgmt_api_test_util:api_path(["listeners", ListenerId]),
    request(put, Path, [], NewConfig).

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

t_init_refresh(Config) ->
    CRLDer = ?config(crl_der, Config),
    Ref = get_crl_cache_table(),
    ?assertEqual([], ets:tab2list(Ref)),
    URL1 = "http://localhost/crl1.pem",
    URL2 = "http://localhost/crl2.pem",
    Opts = #{
        urls => [URL1, URL2],
        refresh_interval => timer:minutes(15),
        http_timeout => timer:seconds(15)
    },
    ok = snabbkaffe:start_trace(),
    {ok, SubRef} = snabbkaffe:subscribe(
        fun(#{?snk_kind := Kind}) ->
            Kind =:= crl_cache_insert
        end,
        _NEvents = 2,
        _Timeout = 2_000
    ),
    ?assertMatch({ok, _}, emqx_crl_cache:start_link(Opts)),
    lists:foreach(fun assert_http_get/1, [URL1, URL2]),
    {ok, _} = snabbkaffe:receive_events(SubRef),
    snabbkaffe:stop(),
    ?assertEqual(
        [{"crl1.pem", [CRLDer]}, {"crl2.pem", [CRLDer]}],
        lists:sort(ets:tab2list(Ref))
    ),
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
    ok = snabbkaffe:start_trace(),
    ?wait_async_action(
        ?assertEqual(ok, emqx_crl_cache:refresh(URL)),
        #{?snk_kind := crl_cache_insert},
        5_000
    ),
    ok = snabbkaffe:stop(),
    Ref = get_crl_cache_table(),
    ?assertMatch([{"crl.pem", _}], ets:tab2list(Ref)),
    snabbkaffe:start_trace(),
    {ok, {ok, _}} = ?wait_async_action(
        emqx_crl_cache:evict(URL),
        #{?snk_kind := crl_cache_evict}
    ),
    snabbkaffe:stop(),
    ?assertEqual([], ets:tab2list(Ref)),
    ok.

%% check that the URL in the certificate is checked on the fly if the
%% cache is empty.
t_empty_cache(Config) ->
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
        http_get -> ok
    after 2_000 -> error(should_have_checked_server)
    end,
    emqtt:disconnect(C),
    ok.

%% check that the URL in the certificate is *not* checked if the cache
%% contains that URL.
t_filled_cache(Config) ->
    ?check_trace(
        begin
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
                http_get -> error(should_have_used_cache)
            after 500 -> ok
            end,
            emqtt:disconnect(C),
            ok
        end,
        []
    ).

t_refresh_config(_Config) ->
    URLs = [
        <<"http://localhost:9878/some.crl.pem">>,
        <<"http://localhost:9878/another.crl.pem">>
    ],
    SortedURLs = lists:sort(URLs),
    emqx_config:put_listener_conf(
        ssl,
        default,
        [ssl_options, crl],
        #{
            enable_crl_check => true,
            cache_urls => URLs
        }
    ),

    %% has to be more than 1 minute
    NewRefreshInterval = timer:seconds(64),
    NewHTTPTimeout = timer:seconds(7),
    emqx_config:put([crl_cache, refresh_interval], NewRefreshInterval),
    emqx_config:put([crl_cache, http_timeout], NewHTTPTimeout),
    ?check_trace(
        ?wait_async_action(
            emqx_crl_cache:refresh_config(),
            #{?snk_kind := crl_cache_refresh_config},
            _Timeout = 10_000
        ),
        fun(Res, Trace) ->
            ?assertMatch({ok, {ok, _}}, Res),
            ?assertMatch(
                [
                    #{
                        urls := SortedURLs,
                        refresh_interval := NewRefreshInterval,
                        http_timeout := NewHTTPTimeout
                    }
                ],
                ?of_kind(crl_cache_refresh_config, Trace),
                #{
                    expected => #{
                        urls => SortedURLs,
                        refresh_interval => NewRefreshInterval,
                        http_timeout => NewHTTPTimeout
                    }
                }
            ),
            ?assertEqual(SortedURLs, ?projection(url, ?of_kind(crl_cache_ensure_timer, Trace))),
            ok
        end
    ),
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
    ?assertMatch({error, {{shutdown, {tls_alert, {certificate_revoked, _}}}, _}}, emqtt:connect(C)),
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
                    <<"crl">> :=
                        #{
                            <<"enable_crl_check">> := false,
                            %% fixme: how to make this return an array?
                            <<"cache_urls">> := <<"[]">>
                        },
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
                    <<"crl">> =>
                        #{
                            <<"enable_crl_check">> => true,
                            <<"cache_urls">> => [
                                URL = <<"http://localhost:9878/intermediate.crl.pem">>
                            ]
                        }
                }
        },
    ListenerData1 = emqx_map_lib:deep_merge(ListenerData0, CRLConfig),
    {{ok, {_, _, ListenerData2}}, {ok, _}} =
        ?wait_async_action(
            update_listener_via_api(ListenerId, ListenerData1),
            #{?snk_kind := crl_cache_insert, url := URL},
            5_000
        ),
    ?assertMatch(
        #{
            <<"ssl_options">> :=
                #{
                    <<"crl">> :=
                        #{
                            <<"enable_crl_check">> := true,
                            <<"cache_urls">> := _
                        },
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
        emqx_map_lib:deep_merge(
            ListenerData0,
            #{
                <<"ssl_options">> =>
                    #{
                        <<"crl">> => #{<<"enable_crl_check">> => true},
                        <<"verify">> => <<"verify_none">>
                    }
            }
        ),
    {error, {_, _, ResRaw1}} = update_listener_via_api(ListenerId, ListenerData1),
    #{<<"code">> := <<"BAD_REQUEST">>, <<"message">> := MsgRaw1} =
        emqx_json:decode(ResRaw1, [return_maps]),
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
        emqx_json:decode(MsgRaw1, [return_maps])
    ),

    ok.
