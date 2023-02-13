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
    emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:boot_modules(all),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TestCase, Config)
  when TestCase =:= t_empty_cache;
       TestCase =:= t_filled_cache;
       TestCase =:= t_revoked ->
    DataDir = ?config(data_dir, Config),
    CRLFile = filename:join([DataDir, "intermediate-revoked.crl.pem"]),
    {ok, CRLPem} = file:read_file(CRLFile),
    [{'CertificateList', CRLDer, not_encrypted}] = public_key:pem_decode(CRLPem),
    ServerPid = start_crl_server(CRLPem),
    IsCached = lists:member(TestCase, [t_filled_cache, t_revoked]),
    ok = setup_crl_options(Config, #{is_cached => IsCached}),
    [ {crl_pem, CRLPem}
    , {crl_der, CRLDer}
    , {http_server, ServerPid}
    | Config];
init_per_testcase(t_revoke_then_refresh, Config) ->
    DataDir = ?config(data_dir, Config),
    CRLFileNotRevoked = filename:join([DataDir, "intermediate-not-revoked.crl.pem"]),
    {ok, CRLPemNotRevoked} = file:read_file(CRLFileNotRevoked),
    [{'CertificateList', CRLDerNotRevoked, not_encrypted}] = public_key:pem_decode(CRLPemNotRevoked),
    CRLFileRevoked = filename:join([DataDir, "intermediate-revoked.crl.pem"]),
    {ok, CRLPemRevoked} = file:read_file(CRLFileRevoked),
    [{'CertificateList', CRLDerRevoked, not_encrypted}] = public_key:pem_decode(CRLPemRevoked),
    ServerPid = start_crl_server(CRLPemNotRevoked),
    OldListeners = emqx:get_env(listeners),
    OldRefreshInterval = emqx:get_env(crl_cache_refresh_interval),
    NewRefreshInterval = timer:seconds(10),
    ExtraHandler =
        fun(emqx) ->
                application:set_env(emqx, crl_cache_refresh_interval, NewRefreshInterval),
                ok;
           (_) ->
                ok
        end,
    ok = setup_crl_options(Config, #{is_cached => true, extra_handler => ExtraHandler}),
    ok = snabbkaffe:start_trace(),
    {ok, {ok, _}} =
       ?wait_async_action(
          emqx_crl_cache:refresh_config(),
          #{?snk_kind := crl_cache_refresh_config},
          _Timeout = 10_000),
    [ {crl_pem_not_revoked, CRLPemNotRevoked}
    , {crl_der_not_revoked, CRLDerNotRevoked}
    , {crl_pem_revoked, CRLPemRevoked}
    , {crl_der_revoked, CRLDerRevoked}
    , {http_server, ServerPid}
    , {old_configs, [ {listeners, OldListeners}
                    , {crl_cache_refresh_interval, OldRefreshInterval}
                    ]}
    | Config];
init_per_testcase(t_not_cached_and_unreachable, Config) ->
    DataDir = ?config(data_dir, Config),
    CRLFile = filename:join([DataDir, "intermediate-revoked.crl.pem"]),
    {ok, CRLPem} = file:read_file(CRLFile),
    [{'CertificateList', CRLDer, not_encrypted}] = public_key:pem_decode(CRLPem),
    application:stop(cowboy),
    ok = setup_crl_options(Config, #{is_cached => false}),
    [ {crl_pem, CRLPem}
    , {crl_der, CRLDer}
    | Config];
init_per_testcase(t_refresh_config, Config) ->
    DataDir = ?config(data_dir, Config),
    CRLFile = filename:join([DataDir, "intermediate-revoked.crl.pem"]),
    {ok, CRLPem} = file:read_file(CRLFile),
    [{'CertificateList', CRLDer, not_encrypted}] = public_key:pem_decode(CRLPem),
    TestPid = self(),
    ok = meck:new(emqx_crl_cache, [non_strict, passthrough, no_history, no_link]),
    meck:expect(emqx_crl_cache, http_get,
                fun(URL, _HTTPTimeout) ->
                  TestPid ! {http_get, URL},
                  {ok, {{"HTTP/1.0", 200, 'OK'}, [], CRLPem}}
                end),
    OldListeners = emqx:get_env(listeners),
    OldRefreshInterval = emqx:get_env(crl_cache_refresh_interval),
    OldHTTPTimeout = emqx:get_env(crl_cache_http_timeout),
    ok = setup_crl_options(Config, #{is_cached => false}),
    [ {crl_pem, CRLPem}
    , {crl_der, CRLDer}
    , {old_configs, [ {listeners, OldListeners}
                    , {crl_cache_refresh_interval, OldRefreshInterval}
                    , {crl_cache_http_timeout, OldHTTPTimeout}
                    ]}
    | Config];
init_per_testcase(_TestCase, Config) ->
    DataDir = ?config(data_dir, Config),
    CRLFile = filename:join([DataDir, "intermediate-revoked.crl.pem"]),
    {ok, CRLPem} = file:read_file(CRLFile),
    [{'CertificateList', CRLDer, not_encrypted}] = public_key:pem_decode(CRLPem),
    TestPid = self(),
    ok = meck:new(emqx_crl_cache, [non_strict, passthrough, no_history, no_link]),
    meck:expect(emqx_crl_cache, http_get,
                fun(URL, _HTTPTimeout) ->
                  TestPid ! {http_get, URL},
                  {ok, {{"HTTP/1.0", 200, 'OK'}, [], CRLPem}}
                end),
    [ {crl_pem, CRLPem}
    , {crl_der, CRLDer}
    | Config].

end_per_testcase(TestCase, Config)
  when TestCase =:= t_empty_cache;
       TestCase =:= t_filled_cache;
       TestCase =:= t_revoked ->
    ServerPid = ?config(http_server, Config),
    emqx_crl_cache_http_server:stop(ServerPid),
    emqx_ct_helpers:stop_apps([]),
    emqx_ct_helpers:change_emqx_opts(
      ssl_twoway, [ {crl_options, [ {crl_check_enabled, false}
                                  , {crl_cache_urls, []}
                                  ]}
                  ]),
    application:stop(cowboy),
    clear_crl_cache(),
    ok = snabbkaffe:stop(),
    ok;
end_per_testcase(t_revoke_then_refresh, Config) ->
    ServerPid = ?config(http_server, Config),
    emqx_crl_cache_http_server:stop(ServerPid),
    emqx_ct_helpers:stop_apps([]),
    OldConfigs = ?config(old_configs, Config),
    clear_crl_cache(),
    emqx_ct_helpers:stop_apps([]),
    emqx_ct_helpers:change_emqx_opts(
      ssl_twoway, [ {crl_options, [ {crl_check_enabled, false}
                                  , {crl_cache_urls, []}
                                  ]}
                  ]),
    clear_crl_cache(),
    lists:foreach(
      fun({Key, MValue}) ->
        case MValue of
            undefined -> ok;
            Value -> application:set_env(emqx, Key, Value)
        end
      end,
      OldConfigs),
    application:stop(cowboy),
    ok = snabbkaffe:stop(),
    ok;
end_per_testcase(t_not_cached_and_unreachable, _Config) ->
    emqx_ct_helpers:stop_apps([]),
    emqx_ct_helpers:change_emqx_opts(
      ssl_twoway, [ {crl_options, [ {crl_check_enabled, false}
                                  , {crl_cache_urls, []}
                                  ]}
                  ]),
    clear_crl_cache(),
    ok = snabbkaffe:stop(),
    ok;
end_per_testcase(t_refresh_config, Config) ->
    OldConfigs = ?config(old_configs, Config),
    meck:unload([emqx_crl_cache]),
    clear_crl_cache(),
    emqx_ct_helpers:stop_apps([]),
    emqx_ct_helpers:change_emqx_opts(
      ssl_twoway, [ {crl_options, [ {crl_check_enabled, false}
                                  , {crl_cache_urls, []}
                                  ]}
                  ]),
    clear_crl_cache(),
    lists:foreach(
      fun({Key, MValue}) ->
        case MValue of
            undefined -> ok;
            Value -> application:set_env(emqx, Key, Value)
        end
      end,
      OldConfigs),
    application:stop(cowboy),
    ok = snabbkaffe:stop(),
    ok;
end_per_testcase(_TestCase, _Config) ->
    meck:unload([emqx_crl_cache]),
    clear_crl_cache(),
    ok = snabbkaffe:stop(),
    ok.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

assert_http_get(URL) ->
    receive
        {http_get, URL} ->
            ok
    after
        1000 ->
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
    after
        0 ->
            ok
    end.

clear_crl_cache() ->
    %% reset the CRL cache
    exit(whereis(ssl_manager), kill),
    ok.

force_cacertfile(Cacertfile) ->
    {SSLListeners0, OtherListeners} = lists:partition(
                         fun(#{proto := Proto}) -> Proto =:= ssl end,
                         emqx:get_env(listeners)),
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
        SSLListeners0),
    application:set_env(emqx, listeners, SSLListeners ++ OtherListeners),
    ok.

setup_crl_options(Config, Opts = #{is_cached := IsCached}) ->
    DataDir = ?config(data_dir, Config),
    Cacertfile = filename:join(DataDir, "ca-chain.cert.pem"),
    Certfile = filename:join(DataDir, "server.cert.pem"),
    Keyfile = filename:join(DataDir, "server.key.pem"),
    URLs = case IsCached of
               false -> [];
               true -> ["http://localhost:9878/intermediate.crl.pem"]
           end,
    ExtraHandler = maps:get(extra_handler, Opts, fun(_) -> ok end),
    Handler =
        fun(emqx) ->
                emqx_ct_helpers:change_emqx_opts(
                  ssl_twoway, [ {ssl_options, [ {certfile, Certfile}
                                              , {keyfile, Keyfile}
                                              , {verify, verify_peer}
                                                %% {crl_check, true} does not work; probably bug in OTP
                                              , {crl_check, peer}
                                              , {crl_cache,
                                                 {ssl_crl_cache, {internal, [{http, timer:seconds(15)}]}}}
                                              ]}
                              , {crl_options, [ {crl_check_enabled, true}
                                              , {crl_cache_urls, URLs}
                                              ]}
                              ]),
                %% emqx_ct_helpers:change_emqx_opts has cacertfile hardcoded....
                ok = force_cacertfile(Cacertfile),
                ExtraHandler(emqx),
                ok;
           (App) ->
                ExtraHandler(App),
                ok
        end,
    emqx_ct_helpers:start_apps([], Handler),
    case IsCached of
        true ->
            %% wait the cache to be filled
            receive
                http_get -> ok
            after
                1_000 -> error(crl_cache_not_filled)
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
    after
        1000 -> error(timeout_starting_http_server)
    end,
    ServerPid.

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
    after
        1000 -> ok
    end,
    ?assertEqual([], ets:tab2list(Ref)),
    ok.

t_init_refresh(Config) ->
    CRLDer = ?config(crl_der, Config),
    Ref = get_crl_cache_table(),
    ?assertEqual([], ets:tab2list(Ref)),
    URL1 = "http://localhost/crl1.pem",
    URL2 = "http://localhost/crl2.pem",
    Opts = #{ urls => [URL1, URL2]
            , refresh_interval => timer:minutes(15)
            , http_timeout => timer:seconds(15)
            },
    ok = snabbkaffe:start_trace(),
    {ok, SubRef} = snabbkaffe:subscribe(
                     fun(#{?snk_kind := Kind}) ->
                             Kind =:= crl_cache_insert
                     end,
                     _NEvents = 2,
                     _Timeout = 2_000),
    ?assertMatch({ok, _}, emqx_crl_cache:start_link(Opts)),
    lists:foreach(fun assert_http_get/1, [URL1, URL2]),
    {ok, _} = snabbkaffe:receive_events(SubRef),
    snabbkaffe:stop(),
    ?assertEqual(
       [{"crl1.pem", [CRLDer]}, {"crl2.pem", [CRLDer]}],
       lists:sort(ets:tab2list(Ref))),
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
       5_000),
    ok = snabbkaffe:stop(),
    ?assertEqual(
       [{"crl.pem", [CRLDer]}],
       ets:tab2list(Ref)),
    ok.

t_refresh_request_error(_Config) ->
    meck:expect(emqx_crl_cache, http_get,
                fun(_URL, _HTTPTimeout) ->
                  {ok, {{"HTTP/1.0", 404, 'Not Found'}, [], <<"not found">>}}
                end),
    {ok, _} = emqx_crl_cache:start_link(),
    URL = "http://localhost/crl.pem",
    ?check_trace(
       ?wait_async_action(
          ?assertEqual(ok, emqx_crl_cache:refresh(URL)),
          #{?snk_kind := crl_cache_insert},
          5_000),
       fun(Trace) ->
         ?assertMatch(
            [#{error := {bad_response, #{code := 404}}}],
            ?of_kind(crl_refresh_failure, Trace)),
         ok
       end),
    ok = snabbkaffe:stop(),
    ok.

t_refresh_invalid_response(_Config) ->
    meck:expect(emqx_crl_cache, http_get,
                fun(_URL, _HTTPTimeout) ->
                  {ok, {{"HTTP/1.0", 200, 'OK'}, [], <<"not a crl">>}}
                end),
    {ok, _} = emqx_crl_cache:start_link(),
    URL = "http://localhost/crl.pem",
    ?check_trace(
       ?wait_async_action(
          ?assertEqual(ok, emqx_crl_cache:refresh(URL)),
          #{?snk_kind := crl_cache_insert},
          5_000),
       fun(Trace) ->
         ?assertMatch(
            [#{crls := []}],
            ?of_kind(crl_cache_insert, Trace)),
         ok
       end),
    ok = snabbkaffe:stop(),
    ok.

t_refresh_http_error(_Config) ->
    meck:expect(emqx_crl_cache, http_get,
                fun(_URL, _HTTPTimeout) ->
                  {error, timeout}
                end),
    {ok, _} = emqx_crl_cache:start_link(),
    URL = "http://localhost/crl.pem",
    ?check_trace(
       ?wait_async_action(
          ?assertEqual(ok, emqx_crl_cache:refresh(URL)),
          #{?snk_kind := crl_cache_insert},
          5_000),
       fun(Trace) ->
         ?assertMatch(
            [#{error := {http_error, timeout}}],
            ?of_kind(crl_refresh_failure, Trace)),
         ok
       end),
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
       5_000),
    ok = snabbkaffe:stop(),
    Ref = get_crl_cache_table(),
    ?assertMatch([{"crl.pem", _}], ets:tab2list(Ref)),
    snabbkaffe:start_trace(),
    {ok, {ok, _}} = ?wait_async_action(emqx_crl_cache:evict(URL),
                                       #{?snk_kind := crl_cache_evict}),
    snabbkaffe:stop(),
    ?assertEqual([], ets:tab2list(Ref)),
    ok.

%% check that the URL in the certificate is checked on the fly if the
%% cache is empty.
t_empty_cache(Config) ->
    DataDir = ?config(data_dir, Config),
    ClientCert = filename:join(DataDir, "client.cert.pem"),
    ClientKey = filename:join(DataDir, "client.key.pem"),
    {ok, C} = emqtt:start_link([ {ssl, true}
                               , {ssl_opts, [ {certfile, ClientCert}
                                            , {keyfile, ClientKey}
                                            ]}
                               , {port, 8883}
                               ]),
    {ok, _} = emqtt:connect(C),
    receive
        http_get -> ok
    after
        2_000 -> error(should_have_checked_server)
    end,
    emqtt:disconnect(C),
    ok.

%% check that the URL in the certificate is *not* checked if the cache
%% contains that URL.
t_filled_cache(Config) ->
    DataDir = ?config(data_dir, Config),
    ClientCert = filename:join(DataDir, "client.cert.pem"),
    ClientKey = filename:join(DataDir, "client.key.pem"),
    {ok, C} = emqtt:start_link([ {ssl, true}
                               , {ssl_opts, [ {certfile, ClientCert}
                                            , {keyfile, ClientKey}
                                            ]}
                               , {port, 8883}
                               ]),
    {ok, _} = emqtt:connect(C),
    receive
        http_get -> error(should_have_used_cache)
    after
        2_000 -> ok
    end,
    emqtt:disconnect(C),
    ok.

t_refresh_config(_Config) ->
    URLs = [ "http://localhost:9878/some.crl.pem"
           , "http://localhost:9878/another.crl.pem"
           ],
    SortedURLs = lists:sort(URLs),
    emqx_ct_helpers:change_emqx_opts(
      ssl_twoway, [ {crl_options, [ {crl_check_enabled, true}
                                  , {crl_cache_urls, URLs}
                                  ]}
                  ]),
    %% has to be more than 1 minute
    NewRefreshInterval = timer:seconds(64),
    NewHTTPTimeout = timer:seconds(7),
    application:set_env(emqx, crl_cache_refresh_interval, NewRefreshInterval),
    application:set_env(emqx, crl_cache_http_timeout, NewHTTPTimeout),
    ?check_trace(
       ?wait_async_action(
          emqx_crl_cache:refresh_config(),
          #{?snk_kind := crl_cache_refresh_config},
          _Timeout = 10_000),
       fun(Res, Trace) ->
         ?assertMatch({ok, {ok, _}}, Res),
         ?assertMatch(
            [#{ urls := SortedURLs
              , refresh_interval := NewRefreshInterval
              , http_timeout := NewHTTPTimeout
              }],
            ?of_kind(crl_cache_refresh_config, Trace),
            #{ expected => #{ urls => SortedURLs
                            , refresh_interval => NewRefreshInterval
                            , http_timeout => NewHTTPTimeout
                            }
             }),
         ?assertEqual(SortedURLs, ?projection(url, ?of_kind(crl_cache_ensure_timer, Trace))),
         ok
       end),
    ok.

%% If the CRL is not cached when the client tries to connect and the
%% CRL server is unreachable, the client will be denied connection.
t_not_cached_and_unreachable(Config) ->
    DataDir = ?config(data_dir, Config),
    ClientCert = filename:join(DataDir, "client.cert.pem"),
    ClientKey = filename:join(DataDir, "client.key.pem"),
    {ok, C} = emqtt:start_link([ {ssl, true}
                               , {ssl_opts, [ {certfile, ClientCert}
                                            , {keyfile, ClientKey}
                                            ]}
                               , {port, 8883}
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
    {ok, C} = emqtt:start_link([ {ssl, true}
                               , {ssl_opts, [ {certfile, ClientCert}
                                            , {keyfile, ClientKey}
                                            ]}
                               , {port, 8883}
                               ]),
    process_flag(trap_exit, true),
    ?assertMatch({error, {{shutdown, {tls_alert, {certificate_revoked, _}}}, _}}, emqtt:connect(C)),
    ok.

t_revoke_then_refresh(Config) ->
    DataDir = ?config(data_dir, Config),
    CRLPemRevoked = ?config(crl_pem_revoked, Config),
    ClientCert = filename:join(DataDir, "client-revoked.cert.pem"),
    ClientKey = filename:join(DataDir, "client-revoked.key.pem"),
    {ok, C0} = emqtt:start_link([ {ssl, true}
                                , {ssl_opts, [ {certfile, ClientCert}
                                             , {keyfile, ClientKey}
                                             ]}
                                , {port, 8883}
                                ]),
    %% At first, the CRL contains no revoked entries, so the client
    %% should be allowed connection.
    ?assertMatch({ok, _}, emqtt:connect(C0)),
    emqtt:stop(C0),

    %% Now we update the CRL on the server and wait for the cache to
    %% be refreshed.
    ok = snabbkaffe:start_trace(),
    {true, {ok, _}} =
        ?wait_async_action(
           emqx_crl_cache_http_server:set_crl(CRLPemRevoked),
           #{?snk_kind := crl_refresh_timer_done},
           70_000),

    %% The *same client* should now be denied connection.
    {ok, C1} = emqtt:start_link([ {ssl, true}
                                , {ssl_opts, [ {certfile, ClientCert}
                                             , {keyfile, ClientKey}
                                             ]}
                                , {port, 8883}
                                ]),
    process_flag(trap_exit, true),
    ?assertMatch({error, {{shutdown, {tls_alert, {certificate_revoked, _}}}, _}}, emqtt:connect(C1)),
    ok.
