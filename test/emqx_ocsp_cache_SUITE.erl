%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ocsp_cache_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-include_lib("ssl/src/ssl_handshake.hrl").

-define(CACHE_TAB, emqx_ocsp_cache).

all() ->
    [{group, openssl}] ++ tests().

tests() ->
    emqx_ct:all(?MODULE) -- openssl_tests().

openssl_tests() ->
    [t_openssl_client].

groups() ->
    OpensslTests = openssl_tests(),
    [ {openssl, [ {group, tls12}
                , {group, tls13}
                ]}
    , {tls12, [ {group, with_status_request}
              , {group, without_status_request}
              ]}
    , {tls13, [ {group, with_status_request}
              , {group, without_status_request}
              ]}
    , {with_status_request, [], OpensslTests}
    , {without_status_request, [], OpensslTests}
    ].

init_per_suite(Config) ->
    application:load(emqx),
    OriginalListeners = application:get_env(emqx, listeners, []),
    [ {original_listeners, OriginalListeners}
    | Config].

end_per_suite(Config) ->
    OriginalListeners = ?config(original_listeners, Config),
    application:set_env(emqx, listeners, OriginalListeners),
    ok.

init_per_group(tls12, Config) ->
    [{tls_vsn, "-tls1_2"} | Config];
init_per_group(tls13, Config) ->
    [{tls_vsn, "-tls1_3"} | Config];
init_per_group(with_status_request, Config) ->
    [{status_request, true} | Config];
init_per_group(without_status_request, Config) ->
    [{status_request, false} | Config];
init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(t_openssl_client, Config) ->
    ct:timetrap(30_000),
    OriginalListeners = application:get_env(emqx, listeners),
    DataDir = ?config(data_dir, Config),
    IssuerPem = filename:join([DataDir, "ocsp-issuer.pem"]),
    ServerCert = filename:join([DataDir, "server.pem"]),
    ServerKey = filename:join([DataDir, "server.key"]),
    CACert = filename:join([DataDir, "ca.pem"]),
    Handler =
        fun(emqx) ->
                Listeners0 = emqx:get_env(listeners, []),
                {[SSLListener0 = #{opts := Opts0}], Listeners1} =
                    lists:partition(
                      fun(#{proto := P, name := N}) ->
                        N =:= "external" andalso P =:= ssl
                      end,
                      Listeners0),
                SSLOpts0 = proplists:get_value(ssl_options, Opts0),
                SSLOpts1 = lists:foldl(
                             fun proplists:delete/2,
                             SSLOpts0,
                             [certfile, keyfile]),
                SSLOpts2 = lists:foldl(
                            fun({K, V}, Acc) ->
                              [{K, V} | Acc]
                            end,
                            SSLOpts1,
                            [ {certfile, ServerCert}
                            , {keyfile, ServerKey}
                            , {cacertfile, CACert}
                            ]),
                Opts1 = proplists:delete(ssl_options, Opts0),
                OCSPOpts = [ {ocsp_stapling_enabled, true}
                           , {ocsp_responder_url, "http://127.0.0.1:9877"}
                           , {ocsp_issuer_pem, IssuerPem}
                           , {ocsp_refresh_http_timeout, 15_000}
                           , {ocsp_refresh_interval, 1_000}
                           ],
                Opts2 = emqx_misc:merge_opts(Opts1, [ {ocsp_options, OCSPOpts}
                                                    , {ssl_options, SSLOpts2}]),
                Listeners = [ SSLListener0#{opts => Opts2}
                            | Listeners1],
                application:set_env(emqx, listeners, Listeners),
                ok;
           (_) ->
                ok
        end,
    OCSPResponderPort = spawn_openssl_ocsp_responder(Config),
    {os_pid, OCSPOSPid} = erlang:port_info(OCSPResponderPort, os_pid),
    %%%%%%%%  Warning!!!
    %% Apparently, openssl 3.0.7 introduced a bug in the responder
    %% that makes it hang forever if one probes the port with
    %% `gen_tcp:open' / `gen_tcp:close'...  Comment this out if
    %% openssl gets updated in CI or in your local machine.
    case openssl_version() of
        "3." ++ _ ->
            %% hope that the responder has started...
            ok;
        _ ->
            ensure_port_open(9877)
    end,
    ct:sleep(1_000),
    emqx_ct_helpers:start_apps([], Handler),
    ct:sleep(1_000),
    [ {original_listeners, OriginalListeners}
    , {ocsp_responder_port, OCSPResponderPort}
    , {ocsp_responder_os_pid, OCSPOSPid}
    | Config];
init_per_testcase(_TestCase, Config) ->
    ct:timetrap(10_000),
    TestPid = self(),
    ok = meck:new(emqx_ocsp_cache, [non_strict, passthrough, no_history, no_link]),
    meck:expect(emqx_ocsp_cache, http_get,
                fun(URL, _HTTPTimeout) ->
                  TestPid ! {http_get, URL},
                  {ok, {{"HTTP/1.0", 200, 'OK'}, [], <<"ocsp response">>}}
                end),
    {ok, CachePid} = emqx_ocsp_cache:start_link(),
    DataDir = ?config(data_dir, Config),
    OCSPOpts = [ {ocsp_stapling_enabled, true}
               , {ocsp_responder_url, "http://localhost:9877"}
               , {ocsp_issuer_pem,
                  filename:join(DataDir, "ocsp-issuer.pem")}
               , {ocsp_refresh_http_timeout, 15_000}
               , {ocsp_refresh_interval, 1_000}
               ],
    application:set_env(
      emqx, listeners,
      [#{ proto => ssl
        , name => "test_ocsp"
        , opts => [ {ssl_options, [{certfile,
                                    filename:join(DataDir, "server.pem")}]}
                  , {ocsp_options, OCSPOpts}
                  ]
        }]),
    snabbkaffe:start_trace(),
    [ {cache_pid, CachePid}
    | Config].

end_per_testcase(t_openssl_client, Config) ->
    OriginalListeners = ?config(original_listeners, Config),
    OCSPResponderOSPid = ?config(ocsp_responder_os_pid, Config),
    case OriginalListeners of
        {ok, Listeners} -> application:set_env(emqx, listeners, Listeners);
        _ -> ok
    end,
    catch kill_pid(OCSPResponderOSPid),
    emqx_ct_helpers:stop_apps([]),
    ok;
end_per_testcase(_TestCase, Config) ->
    CachePid = ?config(cache_pid, Config),
    catch gen_server:stop(CachePid),
    meck:unload([emqx_ocsp_cache]),
    ok.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

assert_no_http_get() ->
    receive
        {http_get, _URL} ->
            error(should_be_cached)
    after
        0 ->
            ok
    end.

assert_http_get(0) -> ok;
assert_http_get(N) when N > 0 ->
    receive
        {http_get, URL} ->
            ?assertMatch("http://localhost:9877/" ++ _Request64, URL),
            ok
    after
        0 ->
            error(no_http_get)
    end,
    assert_http_get(N - 1).

spawn_openssl_client(TLSVsn, RequestStatus, Config) ->
    DataDir = ?config(data_dir, Config),
    ClientCert = filename:join([DataDir, "client.pem"]),
    ClientKey = filename:join([DataDir, "client.key"]),
    Cacert = filename:join([DataDir, "ca.pem"]),
    Openssl = os:find_executable("openssl"),
    StatusOpt = case RequestStatus of
                    true -> ["-status"];
                    false -> []
                end,
    open_port( {spawn_executable, Openssl}
             , [ {args, [ "s_client"
                        , "-connect", "localhost:8883"
                          %% needed to trigger `sni_fun'
                        , "-servername", "localhost"
                        , TLSVsn
                        , "-CAfile", Cacert
                        , "-cert", ClientCert
                        , "-key", ClientKey
                        ] ++ StatusOpt}
               , binary
               , stderr_to_stdout
               ]
             ).

spawn_openssl_ocsp_responder(Config) ->
    DataDir = ?config(data_dir, Config),
    IssuerCert = filename:join([DataDir, "ocsp-issuer.pem"]),
    IssuerKey = filename:join([DataDir, "ocsp-issuer.key"]),
    Cacert = filename:join([DataDir, "ca.pem"]),
    Index = filename:join([DataDir, "index.txt"]),
    Openssl = os:find_executable("openssl"),
    open_port( {spawn_executable, Openssl}
             , [ {args, [ "ocsp"
                        , "-ignore_err"
                        , "-port", "9877"
                        , "-CA", Cacert
                        , "-rkey", IssuerKey
                        , "-rsigner", IssuerCert
                        , "-index", Index
                        ]}
               , binary
               , stderr_to_stdout
               ]
             ).

kill_pid(OSPid) ->
    os:cmd("kill -9 " ++ integer_to_list(OSPid)).

test_ocsp_connection(TLSVsn, WithRequestStatus = true, Config) ->
    ClientPort = spawn_openssl_client(TLSVsn, WithRequestStatus, Config),
    {os_pid, ClientOSPid} = erlang:port_info(ClientPort, os_pid),
    try
        timer:sleep(timer:seconds(1)),
        {messages, Messages} = process_info(self(), messages),
        OCSPOutput0 = [Output || {_Port, {data, Output}} <- Messages,
                                 re:run(Output, "OCSP response:") =/= nomatch],
        ?assertMatch([_], OCSPOutput0,
                     #{ all_messages => Messages
                      }),
        [OCSPOutput] = OCSPOutput0,
        ?assertMatch({match, _}, re:run(OCSPOutput, "OCSP Response Status: successful"),
                     #{all_messages => Messages}),
        ?assertMatch({match, _}, re:run(OCSPOutput, "Cert Status: good"),
                     #{all_messages => Messages}),
        ok
    after
        catch kill_pid(ClientOSPid)
    end;
test_ocsp_connection(TLSVsn, WithRequestStatus = false, Config) ->
    ClientPort = spawn_openssl_client(TLSVsn, WithRequestStatus, Config),
    {os_pid, ClientOSPid} = erlang:port_info(ClientPort, os_pid),
    try
        timer:sleep(timer:seconds(1)),
        {messages, Messages} = process_info(self(), messages),
        OCSPOutput = [Output || {_Port, {data, Output}} <- Messages,
                                re:run(Output, "OCSP response:") =/= nomatch],
        ?assertEqual([], OCSPOutput,
                     #{all_messages => Messages}),
        ok
    after
        catch kill_pid(ClientOSPid)
    end.

ensure_port_open(Port) ->
    do_ensure_port_open(Port, 10).

do_ensure_port_open(Port, 0) ->
    error({port_not_open, Port});
do_ensure_port_open(Port, N) when N > 0 ->
    Timeout = 1_000,
    case gen_tcp:connect("localhost", Port, [], Timeout) of
        {ok, Sock} ->
            gen_tcp:close(Sock),
            ok;
        {error, _} ->
            ct:sleep(500),
            do_ensure_port_open(Port, N - 1)
    end.

get_sni_fun(ListenerID) ->
    #{opts := Opts} = emqx_listeners:find_by_id(ListenerID),
    SSLOpts = proplists:get_value(ssl_options, Opts),
    proplists:get_value(sni_fun, SSLOpts).

openssl_version() ->
    Res0 = string:trim(os:cmd("openssl version"), trailing),
    [_, Res] = string:split(Res0, " "),
    {match, [Version]} = re:run(Res, "^([^ ]+)", [{capture, first, list}]),
    Version.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_request_ocsp_response(_Config) ->
    ?check_trace(
       begin
           ListenerID = <<"mqtt:ssl:test_ocsp">>,
           %% not yet cached.
           ?assertEqual([], ets:tab2list(?CACHE_TAB)),
           ?assertEqual({ok, <<"ocsp response">>},
                        emqx_ocsp_cache:fetch_response(ListenerID)),
           assert_http_get(1),
           ?assertMatch([{_, <<"ocsp response">>}], ets:tab2list(?CACHE_TAB)),
           %% already cached; should not perform request again.
           ?assertEqual({ok, <<"ocsp response">>},
                        emqx_ocsp_cache:fetch_response(ListenerID)),
           assert_no_http_get(),
           ok
       end,
       fun(Trace) ->
               ?assert(
                  ?strict_causality(
                     #{?snk_kind := ocsp_cache_miss, listener_id := _ListenerID},
                     #{?snk_kind := ocsp_http_fetch_and_cache, listener_id := _ListenerID},
                     Trace)),
               ?assertMatch(
                  [_],
                  ?of_kind(ocsp_cache_miss, Trace)),
               ?assertMatch(
                  [_],
                  ?of_kind(ocsp_http_fetch_and_cache, Trace)),
               ?assertMatch(
                  [_],
                  ?of_kind(ocsp_cache_hit, Trace)),
               ok
       end).

t_request_ocsp_response_restart_cache(Config) ->
    process_flag(trap_exit, true),
    CachePid = ?config(cache_pid, Config),
    ListenerID = <<"mqtt:ssl:test_ocsp">>,
    ?check_trace(
      begin
          [] = ets:tab2list(?CACHE_TAB),
          {ok, _} = emqx_ocsp_cache:fetch_response(ListenerID),
          ?wait_async_action(
             begin
                 Ref = monitor(process, CachePid),
                 exit(CachePid, kill),
                 receive
                     {'DOWN', Ref, process, CachePid, killed} ->
                         ok
                 after
                     1_000 ->
                         error(cache_not_killed)
                 end,
                 {ok, _} = emqx_ocsp_cache:start_link(),
                 ok
             end,
             #{?snk_kind := ocsp_cache_init}),
          {ok, _} = emqx_ocsp_cache:fetch_response(ListenerID),
          ok
      end,
      fun(Trace) ->
              ?assertMatch(
                 [_, _],
                 ?of_kind(ocsp_http_fetch_and_cache, Trace)),
              assert_http_get(2),
              ok
      end).

t_request_ocsp_response_bad_http_status(_Config) ->
    TestPid = self(),
    meck:expect(emqx_ocsp_cache, http_get,
                fun(URL, _HTTPTimeout) ->
                  TestPid ! {http_get, URL},
                  {ok, {{"HTTP/1.0", 404, 'Not Found'}, [], <<"not found">>}}
                end),
    ListenerID = <<"mqtt:ssl:test_ocsp">>,
    %% not yet cached.
    ?assertEqual([], ets:tab2list(?CACHE_TAB)),
    ?assertEqual(error,
                 emqx_ocsp_cache:fetch_response(ListenerID)),
    assert_http_get(1),
    ?assertEqual([], ets:tab2list(?CACHE_TAB)),
    ok.

t_request_ocsp_response_timeout(_Config) ->
    TestPid = self(),
    meck:expect(emqx_ocsp_cache, http_get,
                fun(URL, _HTTPTimeout) ->
                  TestPid ! {http_get, URL},
                  {error, timeout}
                end),
    ListenerID = <<"mqtt:ssl:test_ocsp">>,
    %% not yet cached.
    ?assertEqual([], ets:tab2list(?CACHE_TAB)),
    ?assertEqual(error,
                 emqx_ocsp_cache:fetch_response(ListenerID)),
    assert_http_get(1),
    ?assertEqual([], ets:tab2list(?CACHE_TAB)),
    ok.

t_register_listener(_Config) ->
    ListenerID = <<"mqtt:ssl:test_ocsp">>,
    %% should fetch and cache immediately
    {ok, {ok, _}} =
        ?wait_async_action(
           emqx_ocsp_cache:register_listener(ListenerID),
           #{?snk_kind := ocsp_http_fetch_and_cache, listener_id := ListenerID}),
    assert_http_get(1),
    ?assertMatch([{_, <<"ocsp response">>}], ets:tab2list(?CACHE_TAB)),
    ok.

t_register_twice(_Config) ->
    ListenerID = <<"mqtt:ssl:test_ocsp">>,
    {ok, {ok, _}} =
        ?wait_async_action(
           emqx_ocsp_cache:register_listener(ListenerID),
           #{?snk_kind := ocsp_http_fetch_and_cache, listener_id := ListenerID}),
    assert_http_get(1),
    ?assertMatch([{_, <<"ocsp response">>}], ets:tab2list(?CACHE_TAB)),
    %% should have no problem in registering the same listener again.
    %% this prompts an immediate refresh.
    {ok, {ok, _}} =
        ?wait_async_action(
           emqx_ocsp_cache:register_listener(ListenerID),
           #{?snk_kind := ocsp_http_fetch_and_cache, listener_id := ListenerID}),
    ok.

t_refresh_periodically(_Config) ->
    ListenerID = <<"mqtt:ssl:test_ocsp">>,
    %% should refresh periodically
    {ok, SubRef} =
        snabbkaffe:subscribe(
         fun(#{?snk_kind := ocsp_http_fetch_and_cache, listener_id := ListenerID0}) ->
                 ListenerID0 =:= ListenerID;
            (_) ->
                 false
         end,
         _NEvents = 2,
         _Timeout = 10_000),
    ok = emqx_ocsp_cache:register_listener(ListenerID),
    ?assertMatch({ok, [_, _]}, snabbkaffe:receive_events(SubRef)),
    assert_http_get(2),
    ok.

t_sni_fun_success(_Config) ->
    ListenerID = <<"mqtt:ssl:test_ocsp">>,
    ServerName = "localhost",
    ?assertEqual(
      [{certificate_status,
        #certificate_status{
           status_type = ?CERTIFICATE_STATUS_TYPE_OCSP,
           response = <<"ocsp response">>
          }}],
      emqx_ocsp_cache:sni_fun(ServerName, ListenerID)),
    ok.

t_sni_fun_http_error(_Config) ->
    meck:expect(emqx_ocsp_cache, http_get,
                fun(_URL, _HTTPTimeout) ->
                  {error, timeout}
                end),
    ListenerID = <<"mqtt:ssl:test_ocsp">>,
    ServerName = "localhost",
    ?assertEqual(
      [],
      emqx_ocsp_cache:sni_fun(ServerName, ListenerID)),
    ok.

t_openssl_client(Config) ->
    TLSVsn = ?config(tls_vsn, Config),
    WithStatusRequest = ?config(status_request, Config),
    %% ensure ocsp response is already cached.
    ListenerID = <<"mqtt:ssl:external">>,
    ?assertMatch(
       {ok, _},
       emqx_ocsp_cache:fetch_response(ListenerID),
       #{msgs => process_info(self(), messages)}),
    timer:sleep(500),
    test_ocsp_connection(TLSVsn, WithStatusRequest, Config).
