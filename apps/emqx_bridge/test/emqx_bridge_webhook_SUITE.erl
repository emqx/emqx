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

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(_Config) ->
    emqx_common_test_helpers:render_and_load_app_config(emqx_conf),
    ok = emqx_common_test_helpers:start_apps([emqx_conf, emqx_bridge]),
    ok = emqx_connector_test_helpers:start_apps([emqx_resource]),
    {ok, _} = application:ensure_all_started(emqx_connector),
    [].

end_per_suite(_Config) ->
    ok = emqx_config:put([bridges], #{}),
    ok = emqx_config:put_raw([bridges], #{}),
    ok = emqx_common_test_helpers:stop_apps([emqx_conf, emqx_bridge]),
    ok = emqx_connector_test_helpers:stop_apps([emqx_resource]),
    _ = application:stop(emqx_connector),
    _ = application:stop(emqx_bridge),
    ok.

suite() ->
    [{timetrap, {seconds, 60}}].

%%------------------------------------------------------------------------------
%% HTTP server for testing
%% (Orginally copied from emqx_bridge_api_SUITE)
%%------------------------------------------------------------------------------
start_http_server(HTTPServerConfig) ->
    ct:pal("Start server\n"),
    process_flag(trap_exit, true),
    Parent = self(),
    {ok, {Port, Sock}} = listen_on_random_port(),
    Acceptor = spawn(fun() ->
        accept_loop(Sock, Parent, HTTPServerConfig)
    end),
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
    Type = maps:get(type, Config, <<"webhook">>),
    Name = maps:get(name, Config, atom_to_binary(?MODULE)),
    PoolSize = maps:get(pool_size, Config, 1),
    QueryMode = maps:get(query_mode, Config, "async"),
    ConnectTimeout = maps:get(connect_timeout, Config, 1),
    RequestTimeout = maps:get(request_timeout, Config, 10000),
    ResourceRequestTimeout = maps:get(resouce_request_timeout, Config, "infinity"),
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
        "    auto_restart_interval = \"60s\"\n"
        "    health_check_interval = \"15s\"\n"
        "    max_queue_bytes = \"1GB\"\n"
        "    query_mode = \"~s\"\n"
        "    request_timeout = \"~s\"\n"
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
            ResourceRequestTimeout
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
    Type = <<"webhook">>,
    Name = atom_to_binary(?MODULE),
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
t_send_async_connection_timeout(_Config) ->
    ResponseDelayMS = 90,
    #{port := Port} = Server = start_http_server(#{response_delay_ms => 900}),
    % Port = 9000,
    BridgeID = make_bridge(#{
        port => Port,
        pool_size => 1,
        query_mode => "async",
        connect_timeout => ResponseDelayMS * 2,
        request_timeout => 10000,
        resouce_request_timeout => "infinity"
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
    stop_http_server(Server),
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
