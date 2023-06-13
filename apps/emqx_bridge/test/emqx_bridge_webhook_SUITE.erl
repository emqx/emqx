%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_bridge_webhook_SUITE).

%% This suite should contains testcases that are specific for the webhook
%% bridge. There are also some test cases that implicitly tests the webhook
%% bridge in emqx_bridge_api_SUITE

-compile(nowarn_export_all).
-compile(export_all).

-import(emqx_mgmt_api_test_util, [request/3, uri/1]).
-import(emqx_common_test_helpers, [on_exit/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(BRIDGE_TYPE, <<"webhook">>).
-define(BRIDGE_NAME, atom_to_binary(?MODULE)).

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(_Config) ->
    emqx_common_test_helpers:render_and_load_app_config(emqx_conf),
    ok = emqx_mgmt_api_test_util:init_suite([emqx_conf, emqx_bridge]),
    ok = emqx_connector_test_helpers:start_apps([emqx_resource]),
    {ok, _} = application:ensure_all_started(emqx_connector),
    [].

end_per_suite(_Config) ->
    ok = emqx_mgmt_api_test_util:end_suite([emqx_conf, emqx_bridge]),
    ok = emqx_connector_test_helpers:stop_apps([emqx_resource]),
    _ = application:stop(emqx_connector),
    _ = application:stop(emqx_bridge),
    ok.

suite() ->
    [{timetrap, {seconds, 60}}].

init_per_testcase(t_bad_bridge_config, Config) ->
    Config;
init_per_testcase(t_send_async_connection_timeout, Config) ->
    ResponseDelayMS = 500,
    Server = start_http_server(#{response_delay_ms => ResponseDelayMS}),
    [{http_server, Server}, {response_delay_ms, ResponseDelayMS} | Config];
init_per_testcase(_TestCase, Config) ->
    Server = start_http_server(#{response_delay_ms => 0}),
    [{http_server, Server} | Config].

end_per_testcase(_TestCase, Config) ->
    case ?config(http_server, Config) of
        undefined -> ok;
        Server -> stop_http_server(Server)
    end,
    emqx_bridge_testlib:delete_all_bridges(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% HTTP server for testing
%% (Orginally copied from emqx_bridge_api_SUITE)
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

bridge_async_config(#{port := Port} = Config) ->
    Type = maps:get(type, Config, ?BRIDGE_TYPE),
    Name = maps:get(name, Config, ?BRIDGE_NAME),
    PoolSize = maps:get(pool_size, Config, 1),
    QueryMode = maps:get(query_mode, Config, "async"),
    ConnectTimeout = maps:get(connect_timeout, Config, 1),
    RequestTimeout = maps:get(request_timeout, Config, 10000),
    ResumeInterval = maps:get(resume_interval, Config, "1s"),
    ResourceRequestTTL = maps:get(resource_request_ttl, Config, "infinity"),
    ConfigString = io_lib:format(
        "bridges.~s.~s {\n"
        "  url = \"http://localhost:~p\"\n"
        "  connect_timeout = \"~ps\"\n"
        "  enable = true\n"
        "  enable_pipelining = 100\n"
        "  max_retries = 2\n"
        "  method = \"post\"\n"
        "  pool_size = ~p\n"
        "  pool_type = \"random\"\n"
        "  request_timeout = \"~ps\"\n"
        "  body = \"${id}\""
        "  resource_opts {\n"
        "    inflight_window = 100\n"
        "    health_check_interval = \"15s\"\n"
        "    max_buffer_bytes = \"1GB\"\n"
        "    query_mode = \"~s\"\n"
        "    request_ttl = \"~p\"\n"
        "    resume_interval = \"~s\"\n"
        "    start_after_created = \"true\"\n"
        "    start_timeout = \"5s\"\n"
        "    worker_pool_size = \"1\"\n"
        "  }\n"
        "  ssl {\n"
        "    enable = false\n"
        "  }\n"
        "}\n",
        [
            Type,
            Name,
            Port,
            ConnectTimeout,
            PoolSize,
            RequestTimeout,
            QueryMode,
            ResourceRequestTTL,
            ResumeInterval
        ]
    ),
    ct:pal(ConfigString),
    parse_and_check(ConfigString, Type, Name).

parse_and_check(ConfigString, BridgeType, Name) ->
    {ok, RawConf} = hocon:binary(ConfigString, #{format => map}),
    hocon_tconf:check_plain(emqx_bridge_schema, RawConf, #{required => false, atom_key => false}),
    #{<<"bridges">> := #{BridgeType := #{Name := RetConfig}}} = RawConf,
    RetConfig.

make_bridge(Config) ->
    Type = ?BRIDGE_TYPE,
    Name = ?BRIDGE_NAME,
    BridgeConfig = bridge_async_config(Config#{
        name => Name,
        type => Type
    }),
    {ok, _} = emqx_bridge:create(
        Type,
        Name,
        BridgeConfig
    ),
    emqx_bridge_resource:bridge_id(Type, Name).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

%% This test ensures that https://emqx.atlassian.net/browse/CI-62 is fixed.
%% When the connection time out all the queued requests where dropped in
t_send_async_connection_timeout(Config) ->
    ResponseDelayMS = ?config(response_delay_ms, Config),
    #{port := Port} = ?config(http_server, Config),
    BridgeID = make_bridge(#{
        port => Port,
        pool_size => 1,
        query_mode => "async",
        connect_timeout => 10_000,
        request_timeout => ResponseDelayMS * 2,
        resource_request_ttl => "infinity"
    }),
    NumberOfMessagesToSend = 10,
    [
        emqx_bridge:send_message(BridgeID, #{<<"id">> => Id})
     || Id <- lists:seq(1, NumberOfMessagesToSend)
    ],
    %% Make sure server recive all messages
    ct:pal("Sent messages\n"),
    MessageIDs = maps:from_keys(lists:seq(1, NumberOfMessagesToSend), void),
    receive_request_notifications(MessageIDs, ResponseDelayMS),
    ok.

t_async_free_retries(Config) ->
    #{port := Port} = ?config(http_server, Config),
    BridgeID = make_bridge(#{
        port => Port,
        pool_size => 1,
        query_mode => "sync",
        connect_timeout => 1_000,
        request_timeout => 10_000,
        resource_request_ttl => "10000s"
    }),
    %% Fail 5 times then succeed.
    Context = #{error_attempts => 5},
    ExpectedAttempts = 6,
    Fn = fun(Get, Error) ->
        ?assertMatch(
            {ok, 200, _, _},
            emqx_bridge:send_message(BridgeID, #{<<"hello">> => <<"world">>}),
            #{error => Error}
        ),
        ?assertEqual(ExpectedAttempts, Get(), #{error => Error})
    end,
    do_t_async_retries(Context, {error, normal}, Fn),
    do_t_async_retries(Context, {error, {shutdown, normal}}, Fn),
    ok.

t_async_common_retries(Config) ->
    #{port := Port} = ?config(http_server, Config),
    BridgeID = make_bridge(#{
        port => Port,
        pool_size => 1,
        query_mode => "sync",
        resume_interval => "100ms",
        connect_timeout => 1_000,
        request_timeout => 10_000,
        resource_request_ttl => "10000s"
    }),
    %% Keeps failing until connector gives up.
    Context = #{error_attempts => infinity},
    ExpectedAttempts = 3,
    FnSucceed = fun(Get, Error) ->
        ?assertMatch(
            {ok, 200, _, _},
            emqx_bridge:send_message(BridgeID, #{<<"hello">> => <<"world">>}),
            #{error => Error, attempts => Get()}
        ),
        ?assertEqual(ExpectedAttempts, Get(), #{error => Error})
    end,
    FnFail = fun(Get, Error) ->
        ?assertMatch(
            Error,
            emqx_bridge:send_message(BridgeID, #{<<"hello">> => <<"world">>}),
            #{error => Error, attempts => Get()}
        ),
        ?assertEqual(ExpectedAttempts, Get(), #{error => Error})
    end,
    %% These two succeed because they're further retried by the buffer
    %% worker synchronously, and we're not mock that call.
    do_t_async_retries(Context, {error, {closed, "The connection was lost."}}, FnSucceed),
    do_t_async_retries(Context, {error, {shutdown, closed}}, FnSucceed),
    %% This fails because this error is treated as unrecoverable.
    do_t_async_retries(Context, {error, something_else}, FnFail),
    ok.

t_bad_bridge_config(_Config) ->
    BridgeConfig = bridge_async_config(#{port => 12345}),
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

%% helpers
do_t_async_retries(TestContext, Error, Fn) ->
    #{error_attempts := ErrorAttempts} = TestContext,
    persistent_term:put({?MODULE, ?FUNCTION_NAME, attempts}, 0),
    on_exit(fun() -> persistent_term:erase({?MODULE, ?FUNCTION_NAME, attempts}) end),
    Get = fun() -> persistent_term:get({?MODULE, ?FUNCTION_NAME, attempts}) end,
    GetAndBump = fun() ->
        Attempts = persistent_term:get({?MODULE, ?FUNCTION_NAME, attempts}),
        persistent_term:put({?MODULE, ?FUNCTION_NAME, attempts}, Attempts + 1),
        Attempts + 1
    end,
    emqx_common_test_helpers:with_mock(
        emqx_connector_http,
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
    ok.

receive_request_notifications(MessageIDs, _ResponseDelay) when map_size(MessageIDs) =:= 0 ->
    ok;
receive_request_notifications(MessageIDs, ResponseDelay) ->
    receive
        {http_server, received, Req} ->
            RemainingMessageIDs = remove_message_id(MessageIDs, Req),
            receive_request_notifications(RemainingMessageIDs, ResponseDelay)
    after (30 * 1000) ->
        ct:pal("Waited to long time but did not get any message\n"),
        ct:fail("All requests did not reach server at least once")
    end.

remove_message_id(MessageIDs, #{body := IDBin}) ->
    ID = erlang:binary_to_integer(IDBin),
    %% It is acceptable to get the same message more than once
    maps:without([ID], MessageIDs).
