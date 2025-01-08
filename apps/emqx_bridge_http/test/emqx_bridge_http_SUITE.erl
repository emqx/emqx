%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_bridge_http_SUITE).

%% This suite should contains testcases that are specific for the webhook
%% bridge. There are also some test cases that implicitly tests the webhook
%% bridge in emqx_bridge_api_SUITE

-compile(nowarn_export_all).
-compile(export_all).

-import(emqx_mgmt_api_test_util, [request/3, uri/1]).
-import(emqx_common_test_helpers, [on_exit/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").

-define(BRIDGE_TYPE, emqx_bridge_http_test_lib:bridge_type()).
-define(BRIDGE_NAME, emqx_bridge_http_test_lib:bridge_name()).

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config0) ->
    Config =
        case os:getenv("DEBUG_CASE") of
            [_ | _] = DebugCase ->
                CaseName = list_to_atom(DebugCase),
                [{debug_case, CaseName} | Config0];
            _ ->
                Config0
        end,
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_connector,
            emqx_bridge_http,
            emqx_bridge,
            emqx_rule_engine
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    emqx_mgmt_api_test_util:init_suite(),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_mgmt_api_test_util:end_suite(),
    ok = emqx_cth_suite:stop(Apps),
    ok.

suite() ->
    [{timetrap, {seconds, 60}}].

init_per_testcase(t_bad_bridge_config, Config) ->
    Config;
init_per_testcase(Case, Config) when
    Case =:= t_send_async_connection_timeout orelse Case =:= t_send_get_trace_messages
->
    emqx_bridge_http_test_lib:init_http_success_server(Config);
init_per_testcase(t_path_not_found, Config) ->
    HTTPPath = <<"/nonexisting/path">>,
    ServerSSLOpts = false,
    {ok, {HTTPPort, _Pid}} = emqx_bridge_http_connector_test_server:start_link(
        _Port = random, HTTPPath, ServerSSLOpts
    ),
    ok = emqx_bridge_http_connector_test_server:set_handler(not_found_http_handler()),
    [{http_server, #{port => HTTPPort, path => HTTPPath}} | Config];
init_per_testcase(t_empty_path, Config) ->
    HTTPPath = <<"/">>,
    ServerSSLOpts = false,
    {ok, {HTTPPort, _Pid}} = emqx_bridge_http_connector_test_server:start_link(
        _Port = random, HTTPPath, ServerSSLOpts
    ),
    ok = emqx_bridge_http_connector_test_server:set_handler(
        emqx_bridge_http_test_lib:success_http_handler()
    ),
    [{http_server, #{port => HTTPPort, path => HTTPPath}} | Config];
init_per_testcase(t_too_many_requests, Config) ->
    HTTPPath = <<"/path">>,
    ServerSSLOpts = false,
    {ok, {HTTPPort, _Pid}} = emqx_bridge_http_connector_test_server:start_link(
        _Port = random, HTTPPath, ServerSSLOpts
    ),
    ok = emqx_bridge_http_connector_test_server:set_handler(too_many_requests_http_handler()),
    [{http_server, #{port => HTTPPort, path => HTTPPath}} | Config];
init_per_testcase(t_service_unavailable, Config) ->
    HTTPPath = <<"/path">>,
    ServerSSLOpts = false,
    {ok, {HTTPPort, _Pid}} = emqx_bridge_http_connector_test_server:start_link(
        _Port = random, HTTPPath, ServerSSLOpts
    ),
    ok = emqx_bridge_http_connector_test_server:set_handler(service_unavailable_http_handler()),
    [{http_server, #{port => HTTPPort, path => HTTPPath}} | Config];
init_per_testcase(t_rule_action_expired, Config) ->
    [
        {bridge_name, ?BRIDGE_NAME}
        | Config
    ];
init_per_testcase(t_bridge_probes_header_atoms, Config) ->
    HTTPPath = <<"/path">>,
    ServerSSLOpts = false,
    {ok, {HTTPPort, _Pid}} = emqx_bridge_http_connector_test_server:start_link(
        _Port = random, HTTPPath, ServerSSLOpts
    ),
    ok = emqx_bridge_http_connector_test_server:set_handler(
        emqx_bridge_http_test_lib:success_http_handler()
    ),
    [{http_server, #{port => HTTPPort, path => HTTPPath}} | Config];
init_per_testcase(_TestCase, Config) ->
    Server = start_http_server(#{response_delay_ms => 0}),
    [{http_server, Server} | Config].

end_per_testcase(TestCase, _Config) when
    TestCase =:= t_path_not_found;
    TestCase =:= t_empty_path;
    TestCase =:= t_too_many_requests;
    TestCase =:= t_service_unavailable;
    TestCase =:= t_rule_action_expired;
    TestCase =:= t_bridge_probes_header_atoms;
    TestCase =:= t_send_async_connection_timeout;
    TestCase =:= t_send_get_trace_messages
->
    ok = emqx_bridge_http_connector_test_server:stop(),
    persistent_term:erase({?MODULE, times_called}),
    emqx_bridge_v2_testlib:delete_all_bridges(),
    emqx_bridge_v2_testlib:delete_all_connectors(),
    emqx_common_test_helpers:call_janitor(),
    ok;
end_per_testcase(_TestCase, Config) ->
    case ?config(http_server, Config) of
        undefined -> ok;
        Server -> stop_http_server(Server)
    end,
    emqx_bridge_v2_testlib:delete_all_bridges(),
    emqx_bridge_v2_testlib:delete_all_connectors(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% HTTP server for testing
%% (Originally copied from emqx_bridge_api_SUITE)
%%------------------------------------------------------------------------------
start_http_server(HTTPServerConfig) ->
    process_flag(trap_exit, true),
    Parent = self(),
    ct:pal("Starting server for ~p", [Parent]),
    {ok, {Port, Sock}} = listen_on_random_port(),
    Acceptor = spawn(fun() ->
        accept_loop(Sock, Parent, HTTPServerConfig)
    end),
    ct:pal("Started server on port ~p", [Port]),
    timer:sleep(100),
    #{port => Port, sock => Sock, acceptor => Acceptor}.

stop_http_server(#{sock := Sock, acceptor := Acceptor}) ->
    ct:pal("Stop server\n"),
    exit(Acceptor, kill),
    gen_tcp:close(Sock).

listen_on_random_port() ->
    SockOpts = [binary, {active, false}, {packet, raw}, {reuseaddr, true}, {backlog, 1000}],
    case gen_tcp:listen(0, SockOpts) of
        {ok, Sock} ->
            {ok, Port} = inet:port(Sock),
            {ok, {Port, Sock}};
        {error, Reason} when Reason =/= eaddrinuse ->
            {error, Reason}
    end.

accept_loop(Sock, Parent, HTTPServerConfig) ->
    process_flag(trap_exit, true),
    case gen_tcp:accept(Sock) of
        {ok, Conn} ->
            spawn(fun() -> handle_fun_200_ok(Conn, Parent, HTTPServerConfig, <<>>) end),
            %%gen_tcp:controlling_process(Conn, Handler),
            accept_loop(Sock, Parent, HTTPServerConfig);
        {error, closed} ->
            %% socket owner died
            ok
    end.

make_response(CodeStr, Str) ->
    B = iolist_to_binary(Str),
    iolist_to_binary(
        io_lib:fwrite(
            "HTTP/1.0 ~s\r\nContent-Type: text/html\r\nContent-Length: ~p\r\n\r\n~s",
            [CodeStr, size(B), B]
        )
    ).

handle_fun_200_ok(Conn, Parent, HTTPServerConfig, Acc) ->
    ResponseDelayMS = maps:get(response_delay_ms, HTTPServerConfig, 0),
    ct:pal("Waiting for request~n"),
    case gen_tcp:recv(Conn, 0) of
        {ok, ReqStr} ->
            ct:pal("The http handler got request: ~p", [ReqStr]),
            case parse_http_request(<<Acc/binary, ReqStr/binary>>) of
                {ok, incomplete, NewAcc} ->
                    handle_fun_200_ok(Conn, Parent, HTTPServerConfig, NewAcc);
                {ok, Req, NewAcc} ->
                    timer:sleep(ResponseDelayMS),
                    Parent ! {http_server, received, Req},
                    gen_tcp:send(Conn, make_response("200 OK", "Request OK")),
                    handle_fun_200_ok(Conn, Parent, HTTPServerConfig, NewAcc)
            end;
        {error, closed} ->
            ct:pal("http connection closed");
        {error, Reason} ->
            ct:pal("the http handler recv error: ~p", [Reason]),
            timer:sleep(100),
            gen_tcp:close(Conn)
    end.

parse_http_request(ReqStr) ->
    try
        parse_http_request_assertive(ReqStr)
    catch
        _:_ ->
            {ok, incomplete, ReqStr}
    end.

parse_http_request_assertive(ReqStr0) ->
    %% find body length
    [_, LengthStr0] = string:split(ReqStr0, "content-length:"),
    [LengthStr, _] = string:split(LengthStr0, "\r\n"),
    Length = binary_to_integer(string:trim(LengthStr, both)),
    %% split between multiple requests
    [Method, ReqStr1] = string:split(ReqStr0, " ", leading),
    [Path, ReqStr2] = string:split(ReqStr1, " ", leading),
    [_ProtoVsn, ReqStr3] = string:split(ReqStr2, "\r\n", leading),
    [_HeaderStr, Rest] = string:split(ReqStr3, "\r\n\r\n", leading),
    <<Body:Length/binary, Remain/binary>> = Rest,
    {ok, #{method => Method, path => Path, body => Body}, Remain}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Helper functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_metrics(Name) ->
    %% Note: `emqx_bridge:get_metrics/2' is currently *only* called in prod by
    %% `emqx_bridge_api:lookup_from_local_node' with an action (not v1 bridge) type.
    Type = <<"http">>,
    emqx_bridge:get_metrics(Type, Name).

make_bridge(Config) ->
    emqx_bridge_http_test_lib:make_bridge(Config).

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

wait_http_request() ->
    receive
        {http, _Headers, _Req} ->
            ok
    after 1_000 ->
        ct:pal("mailbox: ~p", [process_info(self(), messages)]),
        ct:fail("http request not made")
    end.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

%% This test ensures that https://emqx.atlassian.net/browse/CI-62 is fixed.
%% When the connection time out all the queued requests where dropped in
t_send_async_connection_timeout(Config) ->
    ResponseDelayMS = ?config(response_delay_ms, Config),
    #{port := Port, path := Path} = ?config(http_server, Config),
    BridgeID = make_bridge(#{
        port => Port,
        path => Path,
        pool_size => 1,
        query_mode => "async",
        connect_timeout => integer_to_list(ResponseDelayMS * 2) ++ "ms",
        request_timeout => "10s",
        resume_interval => "200ms",
        health_check_interval => "200ms",
        resource_request_ttl => "infinity"
    }),
    ResourceId = emqx_bridge_resource:resource_id(BridgeID),
    ?retry(
        _Interval0 = 200,
        _NAttempts0 = 20,
        ?assertMatch({ok, connected}, emqx_resource_manager:health_check(ResourceId))
    ),
    NumberOfMessagesToSend = 10,
    [
        do_send_message(#{<<"id">> => Id})
     || Id <- lists:seq(1, NumberOfMessagesToSend)
    ],
    %% Make sure server receives all messages
    ct:pal("Sent messages\n"),
    MessageIDs = maps:from_keys(lists:seq(1, NumberOfMessagesToSend), void),
    receive_request_notifications(MessageIDs, ResponseDelayMS, []),
    ok.

t_send_get_trace_messages(Config) ->
    ResponseDelayMS = ?config(response_delay_ms, Config),
    #{port := Port, path := Path} = ?config(http_server, Config),
    BridgeID = make_bridge(#{
        port => Port,
        path => Path,
        pool_size => 1,
        query_mode => "async",
        connect_timeout => integer_to_list(ResponseDelayMS * 2) ++ "ms",
        request_timeout => "10s",
        resume_interval => "200ms",
        health_check_interval => "200ms",
        resource_request_ttl => "infinity"
    }),
    RuleTopic = iolist_to_binary([<<"my_rule_topic/">>, atom_to_binary(?FUNCTION_NAME)]),
    SQL = <<"SELECT payload.id as id FROM \"", RuleTopic/binary, "\"">>,
    {ok, #{<<"id">> := RuleId}} =
        emqx_bridge_testlib:create_rule_and_action_http(
            ?BRIDGE_TYPE,
            RuleTopic,
            Config,
            #{sql => SQL}
        ),
    %% ===================================
    %% Create trace for RuleId
    %% ===================================
    Now = erlang:system_time(second) - 10,
    Start = Now,
    End = Now + 60,
    TraceName = atom_to_binary(?FUNCTION_NAME),
    Trace = #{
        name => TraceName,
        type => ruleid,
        ruleid => RuleId,
        start_at => Start,
        end_at => End
    },
    emqx_trace_SUITE:reload(),
    ok = emqx_trace:clear(),
    {ok, _} = emqx_trace:create(Trace),
    %% ===================================

    ResourceId = emqx_bridge_resource:resource_id(BridgeID),
    ?retry(
        _Interval0 = 200,
        _NAttempts0 = 20,
        ?assertMatch({ok, connected}, emqx_resource_manager:health_check(ResourceId))
    ),
    ?retry(
        _Interval0 = 200,
        _NAttempts0 = 20,
        ?assertEqual(<<>>, read_rule_trace_file(TraceName, Now))
    ),
    Msg = emqx_message:make(RuleTopic, <<"{\"id\": 1}">>),
    emqx:publish(Msg),
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
                    'actions.total' := 1
                }
            },
            emqx_metrics_worker:get_metrics(rule_metrics, RuleId)
        )
    ),

    ok = emqx_trace_handler_SUITE:filesync(TraceName, ruleid),
    {ok, Bin} = file:read_file(emqx_trace:log_file(TraceName, Now)),

    ?retry(
        _Interval0 = 200,
        _NAttempts0 = 20,
        begin
            Bin = read_rule_trace_file(TraceName, Now),
            ?assertNotEqual(nomatch, binary:match(Bin, [<<"rule_activated">>])),
            ?assertNotEqual(nomatch, binary:match(Bin, [<<"SQL_yielded_result">>])),
            ?assertNotEqual(nomatch, binary:match(Bin, [<<"bridge_action">>])),
            ?assertNotEqual(nomatch, binary:match(Bin, [<<"action_template_rendered">>])),
            ?assertNotEqual(nomatch, binary:match(Bin, [<<"QUERY_ASYNC">>]))
        end
    ),
    emqx_trace:delete(TraceName),
    ok.

read_rule_trace_file(TraceName, From) ->
    emqx_trace:check(),
    ok = emqx_trace_handler_SUITE:filesync(TraceName, ruleid),
    {ok, Bin} = file:read_file(emqx_trace:log_file(TraceName, From)),
    Bin.

t_async_free_retries(Config) ->
    #{port := Port} = ?config(http_server, Config),
    _BridgeID = make_bridge(#{
        port => Port,
        pool_size => 1,
        query_mode => "sync",
        connect_timeout => "1s",
        request_timeout => "10s",
        resource_request_ttl => "10000s"
    }),
    %% Fail 5 times then succeed.
    Context = #{error_attempts => 5},
    ExpectedAttempts = 6,
    Fn = fun(Get, Error) ->
        ?assertMatch(
            {ok, 200, _, _},
            do_send_message(#{<<"hello">> => <<"world">>}),
            #{error => Error}
        ),
        ?assertEqual(ExpectedAttempts, Get(), #{error => Error})
    end,
    do_t_async_retries(?FUNCTION_NAME, Context, {error, normal}, Fn),
    do_t_async_retries(?FUNCTION_NAME, Context, {error, {shutdown, normal}}, Fn),
    ok.

t_async_common_retries(Config) ->
    #{port := Port} = ?config(http_server, Config),
    _BridgeID = make_bridge(#{
        port => Port,
        pool_size => 1,
        query_mode => "sync",
        resume_interval => "100ms",
        connect_timeout => "1s",
        request_timeout => "10s",
        resource_request_ttl => "10000s"
    }),
    %% Keeps failing until connector gives up.
    Context = #{error_attempts => infinity},
    ExpectedAttempts = 3,
    FnSucceed = fun(Get, Error) ->
        ?assertMatch(
            {ok, 200, _, _},
            do_send_message(#{<<"hello">> => <<"world">>}),
            #{error => Error, attempts => Get()}
        ),
        ?assertEqual(ExpectedAttempts, Get(), #{error => Error})
    end,
    FnFail = fun(Get, Error) ->
        ?assertMatch(
            Error,
            do_send_message(#{<<"hello">> => <<"world">>}),
            #{error => Error, attempts => Get()}
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

t_bad_bridge_config(_Config) ->
    BridgeConfig = emqx_bridge_http_test_lib:bridge_async_config(#{port => 12345}),
    ?assertMatch(
        {ok,
            {{_, 201, _}, _Headers, #{
                <<"status">> := <<"disconnected">>,
                <<"status_reason">> := <<"Connection refused">>
            }}},
        emqx_bridge_testlib:create_bridge_api(
            ?BRIDGE_TYPE,
            ?BRIDGE_NAME,
            BridgeConfig
        )
    ),
    %% try `/start` bridge
    ?assertMatch(
        {error, {{_, 400, _}, _Headers, #{<<"message">> := <<"Connection refused">>}}},
        emqx_bridge_testlib:op_bridge_api("start", ?BRIDGE_TYPE, ?BRIDGE_NAME)
    ),
    ok.

t_start_stop(Config) ->
    #{port := Port} = ?config(http_server, Config),
    BridgeConfig = emqx_bridge_http_test_lib:bridge_async_config(#{
        type => ?BRIDGE_TYPE,
        name => ?BRIDGE_NAME,
        port => Port
    }),
    emqx_bridge_testlib:t_start_stop(
        ?BRIDGE_TYPE, ?BRIDGE_NAME, BridgeConfig, emqx_connector_http_stopped
    ).

t_path_not_found(Config) ->
    ?check_trace(
        begin
            #{port := Port, path := Path} = ?config(http_server, Config),
            MQTTTopic = <<"t/webhook">>,
            BridgeConfig = emqx_bridge_http_test_lib:bridge_async_config(#{
                type => ?BRIDGE_TYPE,
                name => ?BRIDGE_NAME,
                local_topic => MQTTTopic,
                port => Port,
                path => Path
            }),
            {ok, _} = emqx_bridge:create(?BRIDGE_TYPE, ?BRIDGE_NAME, BridgeConfig),
            Msg = emqx_message:make(MQTTTopic, <<"{}">>),
            emqx:publish(Msg),
            wait_http_request(),
            ?retry(
                _Interval = 500,
                _NAttempts = 20,
                ?assertMatch(
                    #{
                        counters := #{
                            matched := 1,
                            failed := 1,
                            success := 0
                        }
                    },
                    get_metrics(?BRIDGE_NAME)
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

t_empty_path(Config) ->
    ?check_trace(
        begin
            #{port := Port, path := _Path} = ?config(http_server, Config),
            MQTTTopic = <<"t/webhook">>,
            BridgeConfig = emqx_bridge_http_test_lib:bridge_async_config(#{
                type => ?BRIDGE_TYPE,
                name => ?BRIDGE_NAME,
                local_topic => MQTTTopic,
                port => Port,
                path => <<"">>
            }),
            {ok, _} = emqx_bridge:create(?BRIDGE_TYPE, ?BRIDGE_NAME, BridgeConfig),
            Msg = emqx_message:make(MQTTTopic, <<"{}">>),
            emqx:publish(Msg),
            wait_http_request(),
            ?retry(
                _Interval = 500,
                _NAttempts = 20,
                ?assertMatch(
                    #{
                        counters := #{
                            matched := 1,
                            failed := 0,
                            success := 1
                        }
                    },
                    get_metrics(?BRIDGE_NAME)
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

t_too_many_requests(Config) ->
    check_send_is_retried(Config).

t_service_unavailable(Config) ->
    check_send_is_retried(Config).

check_send_is_retried(Config) ->
    ?check_trace(
        begin
            #{port := Port, path := Path} = ?config(http_server, Config),
            MQTTTopic = <<"t/webhook">>,
            BridgeConfig = emqx_bridge_http_test_lib:bridge_async_config(#{
                type => ?BRIDGE_TYPE,
                name => ?BRIDGE_NAME,
                local_topic => MQTTTopic,
                port => Port,
                path => Path
            }),
            {ok, _} = emqx_bridge:create(?BRIDGE_TYPE, ?BRIDGE_NAME, BridgeConfig),
            Msg = emqx_message:make(MQTTTopic, <<"{}">>),
            emqx:publish(Msg),
            %% should retry
            wait_http_request(),
            wait_http_request(),
            ?retry(
                _Interval = 500,
                _NAttempts = 20,
                ?assertMatch(
                    #{
                        counters := #{
                            matched := 1,
                            failed := 0,
                            success := 1
                        }
                    },
                    get_metrics(?BRIDGE_NAME)
                )
            ),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([_ | _], ?of_kind(http_will_retry_async, Trace)),
            ok
        end
    ),
    ok.

t_rule_action_expired(Config) ->
    ?check_trace(
        begin
            RuleTopic = <<"t/webhook/rule">>,
            BridgeConfig = emqx_bridge_http_test_lib:bridge_async_config(#{
                type => ?BRIDGE_TYPE,
                name => ?BRIDGE_NAME,
                host => "non.existent.host",
                port => 9999,
                path => <<"/some/path">>,
                resume_interval => "100ms",
                connect_timeout => "1s",
                request_timeout => "100ms",
                resource_request_ttl => "100ms"
            }),
            {ok, _} = emqx_bridge:create(?BRIDGE_TYPE, ?BRIDGE_NAME, BridgeConfig),
            {ok, #{<<"id">> := RuleId}} =
                emqx_bridge_testlib:create_rule_and_action_http(?BRIDGE_TYPE, RuleTopic, Config),
            Msg = emqx_message:make(RuleTopic, <<"timeout">>),
            emqx:publish(Msg),
            ?retry(
                _Interval = 500,
                _NAttempts = 20,
                ?assertMatch(
                    #{
                        counters := #{
                            matched := 1,
                            failed := 0,
                            dropped := 1
                        }
                    },
                    get_metrics(?BRIDGE_NAME)
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
                    emqx_metrics_worker:get_metrics(rule_metrics, RuleId)
                )
            ),
            ok
        end,
        []
    ),
    ok.

t_bridge_probes_header_atoms(Config) ->
    #{port := Port, path := Path} = ?config(http_server, Config),
    ?check_trace(
        begin
            LocalTopic = <<"t/local/topic">>,
            BridgeConfig0 = emqx_bridge_http_test_lib:bridge_async_config(#{
                type => ?BRIDGE_TYPE,
                name => ?BRIDGE_NAME,
                port => Port,
                path => Path,
                resume_interval => "100ms",
                connect_timeout => "1s",
                request_timeout => "100ms",
                resource_request_ttl => "100ms",
                local_topic => LocalTopic
            }),
            BridgeConfig = BridgeConfig0#{
                <<"headers">> => #{
                    <<"some-non-existent-atom">> => <<"x">>
                }
            },
            ?assertMatch(
                {ok, {{_, 204, _}, _Headers, _Body}},
                probe_bridge_api(BridgeConfig)
            ),
            ?assertMatch(
                {ok, {{_, 201, _}, _Headers, _Body}},
                emqx_bridge_testlib:create_bridge_api(
                    ?BRIDGE_TYPE,
                    ?BRIDGE_NAME,
                    BridgeConfig
                )
            ),
            Msg = emqx_message:make(LocalTopic, <<"hi">>),
            emqx:publish(Msg),
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

%% helpers

do_send_message(Message) ->
    Type = emqx_bridge_v2:bridge_v1_type_to_bridge_v2_type(?BRIDGE_TYPE),
    emqx_bridge_v2:send_message(Type, ?BRIDGE_NAME, Message, #{}).

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

receive_request_notifications(MessageIDs, _ResponseDelay, _Acc) when map_size(MessageIDs) =:= 0 ->
    ok;
receive_request_notifications(MessageIDs, ResponseDelay, Acc) ->
    receive
        {http, _Headers, Body} ->
            RemainingMessageIDs = remove_message_id(MessageIDs, Body),
            receive_request_notifications(RemainingMessageIDs, ResponseDelay, [Body | Acc])
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

probe_bridge_api(BridgeConfig) ->
    emqx_bridge_testlib:probe_bridge_api(?BRIDGE_TYPE, ?BRIDGE_NAME, BridgeConfig).
