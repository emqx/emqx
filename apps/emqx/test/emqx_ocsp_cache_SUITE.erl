%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    emqx_common_test_helpers:all(?MODULE) -- openssl_tests().

openssl_tests() ->
    [t_openssl_client].

groups() ->
    OpensslTests = openssl_tests(),
    [
        {openssl, [
            {group, tls12},
            {group, tls13}
        ]},
        {tls12, [
            {group, with_status_request},
            {group, without_status_request}
        ]},
        {tls13, [
            {group, with_status_request},
            {group, without_status_request}
        ]},
        {with_status_request, [], OpensslTests},
        {without_status_request, [], OpensslTests}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(openssl, Config) ->
    DataDir = ?config(data_dir, Config),
    ListenerConf = #{
        bind => <<"0.0.0.0:8883">>,
        max_connections => 512000,
        ssl_options => #{
            keyfile => filename(DataDir, "server.key"),
            certfile => filename(DataDir, "server.pem"),
            cacertfile => filename(DataDir, "ca.pem"),
            ocsp => #{
                enable_ocsp_stapling => true,
                issuer_pem => filename(DataDir, "ocsp-issuer.pem"),
                responder_url => <<"http://127.0.0.1:9877">>
            }
        }
    },
    Conf = #{listeners => #{ssl => #{default => ListenerConf}}},
    Apps = emqx_cth_suite:start(
        [{emqx, #{config => Conf}}],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{group_apps, Apps} | Config];
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

end_per_group(openssl, Config) ->
    emqx_cth_suite:stop(?config(group_apps, Config));
end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(t_openssl_client, Config) ->
    ct:timetrap({seconds, 30}),
    {OCSPResponderPort, OCSPOSPid} = setup_openssl_ocsp(Config),
    [
        {ocsp_responder_port, OCSPResponderPort},
        {ocsp_responder_os_pid, OCSPOSPid}
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
            %% start the listener with the default (non-ocsp) config
            TestPid = self(),
            ok = meck:new(emqx_ocsp_cache, [non_strict, passthrough, no_history, no_link]),
            meck:expect(
                emqx_ocsp_cache,
                http_get,
                fun(URL, _HTTPTimeout) ->
                    ct:pal("ocsp http request ~p", [URL]),
                    TestPid ! {http_get, URL},
                    {ok, {{"HTTP/1.0", 200, 'OK'}, [], <<"ocsp response">>}}
                end
            ),
            Apps = emqx_cth_suite:start(
                [
                    emqx_conf,
                    emqx,
                    emqx_management,
                    {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
                ],
                #{work_dir => emqx_cth_suite:work_dir(TestCase, Config)}
            ),
            _ = emqx_common_test_http:create_default_app(),
            snabbkaffe:start_trace(),
            [{tc_apps, Apps} | Config];
        false ->
            [{skip_does_not_apply, true} | Config]
    end;
init_per_testcase(TC, Config) ->
    ct:timetrap({seconds, 30}),
    TestPid = self(),
    DataDir = ?config(data_dir, Config),
    ok = meck:new(emqx_ocsp_cache, [non_strict, passthrough, no_history, no_link]),
    meck:expect(
        emqx_ocsp_cache,
        http_get,
        fun(URL, _HTTPTimeout) ->
            ct:pal("ocsp http request ~p", [URL]),
            TestPid ! {http_get, URL},
            persistent_term:get(
                {?MODULE, http_response},
                {ok, {{"HTTP/1.0", 200, 'OK'}, [], <<"ocsp response">>}}
            )
        end
    ),
    ResponderURL = <<"http://localhost:9877/">>,
    ListenerConf = #{
        enable => false,
        bind => 0,
        ssl_options => #{
            certfile => filename(DataDir, "server.pem"),
            ocsp => #{
                enable_ocsp_stapling => true,
                responder_url => ResponderURL,
                issuer_pem => filename(DataDir, "ocsp-issuer.pem"),
                refresh_http_timeout => <<"15s">>,
                refresh_interval => <<"1s">>
            }
        }
    },
    Conf = #{listeners => #{ssl => #{test_ocsp => ListenerConf}}},
    Apps = emqx_cth_suite:start(
        [{emqx, #{config => Conf}}],
        #{work_dir => emqx_cth_suite:work_dir(TC, Config)}
    ),
    snabbkaffe:start_trace(),
    [
        {responder_url, ResponderURL},
        {tc_apps, Apps}
        | Config
    ].

filename(Dir, Name) ->
    unicode:characters_to_binary(filename:join(Dir, Name)).

end_per_testcase(t_openssl_client, Config) ->
    catch kill_pid(?config(ocsp_responder_os_pid, Config)),
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
            end_per_testcase(common, Config)
    end;
end_per_testcase(_TestCase, Config) ->
    snabbkaffe:stop(),
    emqx_cth_suite:stop(?config(tc_apps, Config)),
    persistent_term:erase({?MODULE, http_response}),
    meck:unload([emqx_ocsp_cache]),
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

assert_no_http_get() ->
    Timeout = 0,
    Error = should_be_cached,
    assert_no_http_get(Timeout, Error).

assert_no_http_get(Timeout, Error) ->
    receive
        {http_get, _URL} ->
            error(Error)
    after Timeout ->
        ok
    end.

assert_http_get(N) ->
    assert_http_get(N, 0).

assert_http_get(0, _Timeout) ->
    ok;
assert_http_get(N, Timeout) when N > 0 ->
    receive
        {http_get, URL} ->
            ?assertMatch(<<"http://localhost:9877/", _Request64/binary>>, URL),
            ok
    after Timeout ->
        error({no_http_get, #{mailbox => process_info(self(), messages)}})
    end,
    assert_http_get(N - 1, Timeout).

openssl_client_command(TLSVsn, RequestStatus, Config) ->
    DataDir = ?config(data_dir, Config),
    ClientCert = filename:join([DataDir, "client.pem"]),
    ClientKey = filename:join([DataDir, "client.key"]),
    Cacert = filename:join([DataDir, "ca.pem"]),
    Openssl = os:find_executable("openssl"),
    StatusOpt =
        case RequestStatus of
            true -> ["-status"];
            false -> []
        end,
    [
        Openssl,
        "s_client",
        "-connect",
        "localhost:8883",
        %% needed to trigger `sni_fun'
        "-servername",
        "localhost",
        TLSVsn,
        "-CAfile",
        Cacert,
        "-cert",
        ClientCert,
        "-key",
        ClientKey
    ] ++ StatusOpt.

run_openssl_client(TLSVsn, RequestStatus, Config) ->
    Command0 = openssl_client_command(TLSVsn, RequestStatus, Config),
    Command = lists:flatten(lists:join(" ", Command0)),
    os:cmd(Command).

%% fixme: for some reason, the port program doesn't return any output
%% when running in OTP 25 using `open_port`, but the `os:cmd` version
%% works fine.
%% the `open_port' version works fine in OTP 24 for some reason.
spawn_openssl_client(TLSVsn, RequestStatus, Config) ->
    [Openssl | Args] = openssl_client_command(TLSVsn, RequestStatus, Config),
    open_port(
        {spawn_executable, Openssl},
        [
            {args, Args},
            binary,
            stderr_to_stdout
        ]
    ).

spawn_openssl_ocsp_responder(Config) ->
    DataDir = ?config(data_dir, Config),
    IssuerCert = filename:join([DataDir, "ocsp-issuer.pem"]),
    IssuerKey = filename:join([DataDir, "ocsp-issuer.key"]),
    Cacert = filename:join([DataDir, "ca.pem"]),
    Index = filename:join([DataDir, "index.txt"]),
    Openssl = os:find_executable("openssl"),
    open_port(
        {spawn_executable, Openssl},
        [
            {args, [
                "ocsp",
                "-ignore_err",
                "-port",
                "9877",
                "-CA",
                Cacert,
                "-rkey",
                IssuerKey,
                "-rsigner",
                IssuerCert,
                "-index",
                Index
            ]},
            binary,
            stderr_to_stdout
        ]
    ).

kill_pid(OSPid) ->
    os:cmd("kill -9 " ++ integer_to_list(OSPid)).

test_ocsp_connection(TLSVsn, WithRequestStatus = true, Config) ->
    OCSPOutput = run_openssl_client(TLSVsn, WithRequestStatus, Config),
    ?assertMatch(
        {match, _},
        re:run(OCSPOutput, "OCSP Response Status: successful"),
        #{mailbox => process_info(self(), messages)}
    ),
    ?assertMatch(
        {match, _},
        re:run(OCSPOutput, "Cert Status: good"),
        #{mailbox => process_info(self(), messages)}
    ),
    ok;
test_ocsp_connection(TLSVsn, WithRequestStatus = false, Config) ->
    OCSPOutput = run_openssl_client(TLSVsn, WithRequestStatus, Config),
    ?assertMatch(
        nomatch,
        re:run(OCSPOutput, "Cert Status: good", [{capture, none}]),
        #{mailbox => process_info(self(), messages)}
    ),
    ok.

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

openssl_version() ->
    Res0 = string:trim(os:cmd("openssl version"), trailing),
    [_, Res] = string:split(Res0, " "),
    {match, [Version]} = re:run(Res, "^([^ ]+)", [{capture, first, list}]),
    Version.

setup_openssl_ocsp(Config) ->
    OCSPResponderPort = spawn_openssl_ocsp_responder(Config),
    {os_pid, OCSPOSPid} = erlang:port_info(OCSPResponderPort, os_pid),
    %%%%%%%%  Warning!!!
    %% Apparently, openssl 3.0.7 introduced a bug in the responder
    %% that makes it hang forever if one probes the port with
    %% `gen_tcp:open' / `gen_tcp:close'...  Comment this out if
    %% openssl gets updated in CI or in your local machine.
    OpenSSLVersion = openssl_version(),
    ct:pal("openssl version: ~p", [OpenSSLVersion]),
    case OpenSSLVersion of
        "3." ++ _ ->
            %% hope that the responder has started...
            ok;
        _ ->
            ensure_port_open(9877)
    end,
    ct:sleep(1_000),
    {OCSPResponderPort, OCSPOSPid}.

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

put_http_response(Response) ->
    persistent_term:put({?MODULE, http_response}, Response).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_request_ocsp_response(_Config) ->
    ?check_trace(
        begin
            ListenerID = <<"ssl:test_ocsp">>,
            %% not yet cached.
            ?assertEqual([], ets:tab2list(?CACHE_TAB)),
            ?assertEqual(
                {ok, <<"ocsp response">>},
                emqx_ocsp_cache:fetch_response(ListenerID)
            ),
            assert_http_get(1),
            ?assertMatch([{_, <<"ocsp response">>}], ets:tab2list(?CACHE_TAB)),
            %% already cached; should not perform request again.
            ?assertEqual(
                {ok, <<"ocsp response">>},
                emqx_ocsp_cache:fetch_response(ListenerID)
            ),
            assert_no_http_get(),
            ok
        end,
        fun(Trace) ->
            ?assert(
                ?strict_causality(
                    #{?snk_kind := ocsp_cache_miss, listener_id := _ListenerID},
                    #{?snk_kind := ocsp_http_fetch_and_cache, listener_id := _ListenerID},
                    Trace
                )
            ),
            ?assertMatch(
                [_],
                ?of_kind(ocsp_cache_miss, Trace)
            ),
            ?assertMatch(
                [_],
                ?of_kind(ocsp_http_fetch_and_cache, Trace)
            ),
            ?assertMatch(
                [_],
                ?of_kind(ocsp_cache_hit, Trace)
            ),
            ok
        end
    ).

t_request_ocsp_response_restart_cache(_Config) ->
    ListenerID = <<"ssl:test_ocsp">>,
    ?check_trace(
        begin
            [] = ets:tab2list(?CACHE_TAB),
            {ok, _} = emqx_ocsp_cache:fetch_response(ListenerID),
            ?wait_async_action(
                begin
                    CachePid = whereis(emqx_ocsp_cache),
                    Ref = monitor(process, CachePid),
                    exit(CachePid, kill),
                    receive
                        {'DOWN', Ref, process, CachePid, killed} ->
                            ok
                    after 1_000 ->
                        error(cache_not_killed)
                    end
                end,
                #{?snk_kind := ocsp_cache_init}
            ),
            {ok, _} = emqx_ocsp_cache:fetch_response(ListenerID),
            ok
        end,
        fun(Trace) ->
            %% Only one fetch because the cache table was preserved by
            %% its heir ("emqx_kernel_sup").
            ?assertMatch(
                [_],
                ?of_kind(ocsp_http_fetch_and_cache, Trace)
            ),
            assert_http_get(1),
            ok
        end
    ).

t_request_ocsp_response_bad_http_status(_Config) ->
    TestPid = self(),
    meck:expect(
        emqx_ocsp_cache,
        http_get,
        fun(URL, _HTTPTimeout) ->
            TestPid ! {http_get, URL},
            {ok, {{"HTTP/1.0", 404, 'Not Found'}, [], <<"not found">>}}
        end
    ),
    ListenerID = <<"ssl:test_ocsp">>,
    %% not yet cached.
    ?assertEqual([], ets:tab2list(?CACHE_TAB)),
    ?assertEqual(
        error,
        emqx_ocsp_cache:fetch_response(ListenerID)
    ),
    assert_http_get(1),
    ?assertEqual([], ets:tab2list(?CACHE_TAB)),
    ok.

t_request_ocsp_response_timeout(_Config) ->
    TestPid = self(),
    meck:expect(
        emqx_ocsp_cache,
        http_get,
        fun(URL, _HTTPTimeout) ->
            TestPid ! {http_get, URL},
            {error, timeout}
        end
    ),
    ListenerID = <<"ssl:test_ocsp">>,
    %% not yet cached.
    ?assertEqual([], ets:tab2list(?CACHE_TAB)),
    ?assertEqual(
        error,
        emqx_ocsp_cache:fetch_response(ListenerID)
    ),
    assert_http_get(1),
    ?assertEqual([], ets:tab2list(?CACHE_TAB)),
    ok.

t_register_listener(_Config) ->
    ListenerID = <<"ssl:test_ocsp">>,
    Conf = emqx_config:get_listener_conf(ssl, test_ocsp, []),
    %% should fetch and cache immediately
    {ok, {ok, _}} =
        ?wait_async_action(
            emqx_ocsp_cache:register_listener(ListenerID, Conf),
            #{?snk_kind := ocsp_http_fetch_and_cache, listener_id := ListenerID}
        ),
    assert_http_get(1),
    ?assertMatch([{_, <<"ocsp response">>}], ets:tab2list(?CACHE_TAB)),
    ok.

t_register_twice(_Config) ->
    ListenerID = <<"ssl:test_ocsp">>,
    Conf = emqx_config:get_listener_conf(ssl, test_ocsp, []),
    {ok, {ok, _}} =
        ?wait_async_action(
            emqx_ocsp_cache:register_listener(ListenerID, Conf),
            #{?snk_kind := ocsp_http_fetch_and_cache, listener_id := ListenerID}
        ),
    assert_http_get(1),
    ?assertMatch([{_, <<"ocsp response">>}], ets:tab2list(?CACHE_TAB)),
    %% should have no problem in registering the same listener again.
    %% this prompts an immediate refresh.
    {ok, {ok, _}} =
        ?wait_async_action(
            emqx_ocsp_cache:register_listener(ListenerID, Conf),
            #{?snk_kind := ocsp_http_fetch_and_cache, listener_id := ListenerID}
        ),
    ok.

t_refresh_periodically(_Config) ->
    ListenerID = <<"ssl:test_ocsp">>,
    Conf = emqx_config:get_listener_conf(ssl, test_ocsp, []),
    %% should refresh periodically
    {ok, SubRef} =
        snabbkaffe:subscribe(
            fun
                (#{?snk_kind := ocsp_http_fetch_and_cache, listener_id := ListenerID0}) ->
                    ListenerID0 =:= ListenerID;
                (_) ->
                    false
            end,
            _NEvents = 2,
            _Timeout = 10_000
        ),
    ok = emqx_ocsp_cache:register_listener(ListenerID, Conf),
    ?assertMatch({ok, [_, _]}, snabbkaffe:receive_events(SubRef)),
    assert_http_get(2),
    ok.

t_sni_fun_success(_Config) ->
    ListenerID = <<"ssl:test_ocsp">>,
    ServerName = "localhost",
    ?assertEqual(
        [
            {certificate_status, #certificate_status{
                status_type = ?CERTIFICATE_STATUS_TYPE_OCSP,
                response = <<"ocsp response">>
            }}
        ],
        emqx_ocsp_cache:sni_fun(ServerName, ListenerID)
    ),
    ok.

t_sni_fun_http_error(_Config) ->
    meck:expect(
        emqx_ocsp_cache,
        http_get,
        fun(_URL, _HTTPTimeout) ->
            {error, timeout}
        end
    ),
    ListenerID = <<"ssl:test_ocsp">>,
    ServerName = "localhost",
    ?assertEqual(
        [],
        emqx_ocsp_cache:sni_fun(ServerName, ListenerID)
    ),
    ok.

%% check that we can start with a non-ocsp stapling listener and
%% restart it with the new ocsp config.
t_update_listener(Config) ->
    case proplists:get_bool(skip_does_not_apply, Config) of
        true ->
            ok;
        false ->
            do_t_update_listener(Config)
    end.

do_t_update_listener(Config) ->
    DataDir = ?config(data_dir, Config),
    Keyfile = filename:join([DataDir, "server.key"]),
    Certfile = filename:join([DataDir, "server.pem"]),
    Cacertfile = filename:join([DataDir, "ca.pem"]),
    IssuerPemPath = filename:join([DataDir, "ocsp-issuer.pem"]),
    {ok, IssuerPem} = file:read_file(IssuerPemPath),

    %% no ocsp at first
    ListenerId = "ssl:default",
    {ok, {{_, 200, _}, _, ListenerData0}} = get_listener_via_api(ListenerId),
    ?assertMatch(
        #{
            <<"enable_ocsp_stapling">> := false,
            <<"refresh_http_timeout">> := _,
            <<"refresh_interval">> := _
        },
        emqx_utils_maps:deep_get([<<"ssl_options">>, <<"ocsp">>], ListenerData0, undefined)
    ),
    assert_no_http_get(),

    %% configure ocsp
    OCSPConfig =
        #{
            <<"ssl_options">> =>
                #{
                    <<"keyfile">> => Keyfile,
                    <<"certfile">> => Certfile,
                    <<"cacertfile">> => Cacertfile,
                    <<"ocsp">> =>
                        #{
                            <<"enable_ocsp_stapling">> => true,
                            %% we use the file contents to check that
                            %% the API converts that to an internally
                            %% managed file
                            <<"issuer_pem">> => IssuerPem,
                            <<"responder_url">> => <<"http://localhost:9877">>,
                            %% for quicker testing; min refresh in tests is 5 s.
                            <<"refresh_interval">> => <<"5s">>
                        }
                }
        },
    ListenerData1 = emqx_utils_maps:deep_merge(ListenerData0, OCSPConfig),
    {ok, {_, _, ListenerData2}} = update_listener_via_api(ListenerId, ListenerData1),
    ?assertMatch(
        #{
            <<"ssl_options">> :=
                #{
                    <<"ocsp">> :=
                        #{
                            <<"enable_ocsp_stapling">> := true,
                            <<"issuer_pem">> := _,
                            <<"responder_url">> := _
                        }
                }
        },
        ListenerData2
    ),
    %% issuer pem should have been uploaded and saved to a new
    %% location
    ?assertNotEqual(
        IssuerPemPath,
        emqx_utils_maps:deep_get(
            [<<"ssl_options">>, <<"ocsp">>, <<"issuer_pem">>],
            ListenerData2
        )
    ),
    ?assertNotEqual(
        IssuerPem,
        emqx_utils_maps:deep_get(
            [<<"ssl_options">>, <<"ocsp">>, <<"issuer_pem">>],
            ListenerData2
        )
    ),
    assert_http_get(1, 5_000),

    %% Disable OCSP Stapling; the periodic refreshes should stop
    RefreshInterval = emqx_config:get([listeners, ssl, default, ssl_options, ocsp, refresh_interval]),
    OCSPConfig1 =
        #{
            <<"ssl_options">> =>
                #{
                    <<"ocsp">> =>
                        #{
                            <<"enable_ocsp_stapling">> => false
                        }
                }
        },
    ListenerData3 = emqx_utils_maps:deep_merge(ListenerData2, OCSPConfig1),
    {ok, {_, _, ListenerData4}} = update_listener_via_api(ListenerId, ListenerData3),
    ?assertMatch(
        #{
            <<"ssl_options">> :=
                #{
                    <<"ocsp">> :=
                        #{
                            <<"enable_ocsp_stapling">> := false
                        }
                }
        },
        ListenerData4
    ),

    assert_no_http_get(2 * RefreshInterval, should_stop_refreshing),

    ok.

t_double_unregister(_Config) ->
    ListenerID = <<"ssl:test_ocsp">>,
    Conf = emqx_config:get_listener_conf(ssl, test_ocsp, []),
    ?check_trace(
        begin
            {ok, {ok, _}} =
                ?wait_async_action(
                    emqx_ocsp_cache:register_listener(ListenerID, Conf),
                    #{?snk_kind := ocsp_http_fetch_and_cache, listener_id := ListenerID},
                    5_000
                ),
            assert_http_get(1),

            {ok, {ok, _}} =
                ?wait_async_action(
                    emqx_ocsp_cache:unregister_listener(ListenerID),
                    #{?snk_kind := ocsp_cache_listener_unregistered, listener_id := ListenerID},
                    5_000
                ),

            %% Should be idempotent and not crash
            {ok, {ok, _}} =
                ?wait_async_action(
                    emqx_ocsp_cache:unregister_listener(ListenerID),
                    #{?snk_kind := ocsp_cache_listener_unregistered, listener_id := ListenerID},
                    5_000
                ),
            ok
        end,
        []
    ),

    ok.

t_ocsp_responder_error_responses(_Config) ->
    ListenerId = <<"ssl:test_ocsp">>,
    Conf = emqx_config:get_listener_conf(ssl, test_ocsp, []),
    ?check_trace(
        begin
            %% successful response without headers
            put_http_response({ok, {200, <<"ocsp_response">>}}),
            {ok, {ok, _}} =
                ?wait_async_action(
                    emqx_ocsp_cache:register_listener(ListenerId, Conf),
                    #{?snk_kind := ocsp_http_fetch_and_cache, headers := false},
                    1_000
                ),

            %% error response with headers
            put_http_response({ok, {{"HTTP/1.0", 500, "Internal Server Error"}, [], <<"error">>}}),
            {ok, {ok, _}} =
                ?wait_async_action(
                    emqx_ocsp_cache:register_listener(ListenerId, Conf),
                    #{?snk_kind := ocsp_http_fetch_bad_code, code := 500, headers := true},
                    1_000
                ),

            %% error response without headers
            put_http_response({ok, {500, <<"error">>}}),
            {ok, {ok, _}} =
                ?wait_async_action(
                    emqx_ocsp_cache:register_listener(ListenerId, Conf),
                    #{?snk_kind := ocsp_http_fetch_bad_code, code := 500, headers := false},
                    1_000
                ),

            %% econnrefused
            put_http_response(
                {error,
                    {failed_connect, [
                        {to_address, {"localhost", 9877}},
                        {inet, [inet], econnrefused}
                    ]}}
            ),
            {ok, {ok, _}} =
                ?wait_async_action(
                    emqx_ocsp_cache:register_listener(ListenerId, Conf),
                    #{?snk_kind := ocsp_http_fetch_error, error := {failed_connect, _}},
                    1_000
                ),

            %% timeout
            put_http_response({error, timeout}),
            {ok, {ok, _}} =
                ?wait_async_action(
                    emqx_ocsp_cache:register_listener(ListenerId, Conf),
                    #{?snk_kind := ocsp_http_fetch_error, error := timeout},
                    1_000
                ),

            ok
        end,
        []
    ),
    ok.

t_unknown_requests(_Config) ->
    emqx_ocsp_cache ! unknown,
    ?assertEqual(ok, gen_server:cast(emqx_ocsp_cache, unknown)),
    ?assertEqual({error, {unknown_call, unknown}}, gen_server:call(emqx_ocsp_cache, unknown)),
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
                    #{<<"ocsp">> => #{<<"enable_ocsp_stapling">> => true}}
            }
        ),
    {error, {_, _, ResRaw1}} = update_listener_via_api(ListenerId, ListenerData1),
    #{<<"code">> := <<"BAD_REQUEST">>, <<"message">> := MsgRaw1} =
        emqx_utils_json:decode(ResRaw1, [return_maps]),
    ?assertMatch(
        #{
            <<"kind">> := <<"validation_error">>,
            <<"reason">> :=
                <<"The responder URL is required for OCSP stapling">>
        },
        emqx_utils_json:decode(MsgRaw1, [return_maps])
    ),

    ListenerData2 =
        emqx_utils_maps:deep_merge(
            ListenerData0,
            #{
                <<"ssl_options">> =>
                    #{
                        <<"ocsp">> => #{
                            <<"enable_ocsp_stapling">> => true,
                            <<"responder_url">> => <<"http://localhost:9877">>
                        }
                    }
            }
        ),
    {error, {_, _, ResRaw2}} = update_listener_via_api(ListenerId, ListenerData2),
    #{<<"code">> := <<"BAD_REQUEST">>, <<"message">> := MsgRaw2} =
        emqx_utils_json:decode(ResRaw2, [return_maps]),
    ?assertMatch(
        #{
            <<"kind">> := <<"validation_error">>,
            <<"reason">> :=
                <<"The issuer PEM path is required for OCSP stapling">>
        },
        emqx_utils_json:decode(MsgRaw2, [return_maps])
    ),

    ListenerData3a =
        emqx_utils_maps:deep_merge(
            ListenerData0,
            #{
                <<"ssl_options">> =>
                    #{
                        <<"ocsp">> => #{
                            <<"enable_ocsp_stapling">> => true,
                            <<"responder_url">> => <<"http://localhost:9877">>,
                            <<"issuer_pem">> => <<"some_file">>
                        }
                    }
            }
        ),
    ListenerData3 = emqx_utils_maps:deep_remove(
        [<<"ssl_options">>, <<"certfile">>], ListenerData3a
    ),
    {error, {_, _, ResRaw3}} = update_listener_via_api(ListenerId, ListenerData3),
    #{<<"code">> := <<"BAD_REQUEST">>, <<"message">> := MsgRaw3} =
        emqx_utils_json:decode(ResRaw3, [return_maps]),
    %% we can't remove certfile now, because it has default value.
    ?assertMatch({match, _}, re:run(MsgRaw3, <<"enoent">>)),
    ?assertMatch({match, _}, re:run(MsgRaw3, <<"ocsp\\.issuer_pem">>)),
    ok.

t_unknown_error_fetching_ocsp_response(_Config) ->
    ListenerID = <<"ssl:test_ocsp">>,
    TestPid = self(),
    ok = meck:expect(
        emqx_ocsp_cache,
        http_get,
        fun(_RequestURI, _HTTPTimeout) ->
            TestPid ! error_raised,
            meck:exception(error, something_went_wrong)
        end
    ),
    ?assertEqual(error, emqx_ocsp_cache:fetch_response(ListenerID)),
    receive
        error_raised -> ok
    after 200 -> ct:fail("should have tried to fetch ocsp response")
    end,
    ok.

t_path_encoding(Config) ->
    ResponderURL = ?config(responder_url, Config),
    ListenerID = <<"ssl:test_ocsp">>,
    TestPid = self(),
    ok = meck:expect(
        emqx_ocsp_cache,
        http_get,
        fun(RequestURI, _HTTPTimeout) ->
            TestPid ! {request_uri, RequestURI},
            {ok, {{"HTTP/1.0", 200, 'OK'}, [], <<"ocsp response">>}}
        end
    ),
    ?check_trace(
        begin
            ?assertMatch({ok, _}, emqx_ocsp_cache:fetch_response(ListenerID)),
            receive
                {request_uri, <<RequestURI/binary>>} ->
                    <<ResponderURL:(size(ResponderURL))/binary, Path/binary>> = RequestURI,
                    ?assertEqual(nomatch, binary:match(Path, <<"/">>), #{path => Path}),
                    ok
            after 100 ->
                ct:pal(
                    "responder url: ~p\nmailbox: ~p",
                    [ResponderURL, process_info(self(), messages)]
                ),
                ct:fail("request not made")
            end,
            ok
        end,
        []
    ),
    ok.

t_openssl_client(Config) ->
    TLSVsn = ?config(tls_vsn, Config),
    WithStatusRequest = ?config(status_request, Config),
    %% ensure ocsp response is already cached.
    ListenerID = <<"ssl:default">>,
    ?assertMatch(
        {ok, _},
        emqx_ocsp_cache:fetch_response(ListenerID),
        #{msgs => process_info(self(), messages)}
    ),
    timer:sleep(500),
    test_ocsp_connection(TLSVsn, WithStatusRequest, Config).
