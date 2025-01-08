%%--------------------------------------------------------------------
%% Copyright (c) 2019-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_trace_handler_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-include_lib("common_test/include/ct.hrl").
-define(CLIENT, [
    {host, "localhost"},
    {clientid, <<"client">>},
    {username, <<"testuser">>},
    {password, <<"pass">>}
]).

all() -> [t_trace_clientid, t_trace_topic, t_trace_ip_address, t_trace_clientid_utf8].

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [emqx],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    ok = emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(t_trace_clientid, Config) ->
    init(),
    Config;
init_per_testcase(_Case, Config) ->
    _ = [logger:remove_handler(Id) || #{id := Id} <- emqx_trace_handler:running()],
    init(),
    Config.

end_per_testcase(_Case, _Config) ->
    terminate(),
    ok.

t_trace_clientid(_Config) ->
    %% Start tracing
    %% add list clientid
    ok = emqx_trace_handler:install("CLI-client1", clientid, "client", debug, "tmp/client.log"),
    ok = emqx_trace_handler:install("CLI-client2", clientid, <<"client2">>, all, "tmp/client2.log"),
    ok = emqx_trace_handler:install("CLI-client3", clientid, <<"client3">>, all, "tmp/client3.log"),
    {error, {handler_not_added, {file_error, ".", eisdir}}} =
        emqx_trace_handler:install(clientid, <<"client5">>, debug, "."),
    emqx_trace:check(),
    ok = filesync(<<"CLI-client1">>, clientid),
    ok = filesync(<<"CLI-client2">>, clientid),
    ok = filesync(<<"CLI-client3">>, clientid),

    %% Verify the tracing file exits
    ?assert(filelib:is_regular("tmp/client.log")),
    ?assert(filelib:is_regular("tmp/client2.log")),
    ?assert(filelib:is_regular("tmp/client3.log")),

    %% Get current traces
    ?assertMatch(
        [
            #{
                type := clientid,
                filter := <<"client">>,
                name := <<"CLI-client1">>,
                level := debug,
                dst := "tmp/client.log"
            },
            #{
                type := clientid,
                filter := <<"client2">>,
                name := <<"CLI-client2">>,
                level := debug,
                dst := "tmp/client2.log"
            },
            #{
                type := clientid,
                filter := <<"client3">>,
                name := <<"CLI-client3">>,
                level := debug,
                dst := "tmp/client3.log"
            }
        ],
        emqx_trace_handler:running()
    ),

    %% Client with clientid = "client" publishes a "hi" message to "a/b/c".
    {ok, T} = emqtt:start_link(?CLIENT),
    emqtt:connect(T),
    emqtt:publish(T, <<"a/b/c">>, <<"hi">>),
    emqtt:ping(T),
    ok = filesync(<<"CLI-client1">>, clientid),
    ok = filesync(<<"CLI-client2">>, clientid),
    ok = filesync(<<"CLI-client3">>, clientid),

    %% Verify messages are logged to "tmp/client.log" but not "tmp/client2.log".
    {ok, Bin} = file:read_file("tmp/client.log"),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"CONNECT">>])),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"CONNACK">>])),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"PUBLISH">>])),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"PINGREQ">>])),
    ?assert(filelib:file_size("tmp/client2.log") == 0),

    %% Stop tracing
    ok = emqx_trace_handler:uninstall(clientid, <<"CLI-client1">>),
    ok = emqx_trace_handler:uninstall(clientid, <<"CLI-client2">>),
    ok = emqx_trace_handler:uninstall(clientid, <<"CLI-client3">>),

    emqtt:disconnect(T),
    ?assertEqual([], emqx_trace_handler:running()).

t_trace_clientid_utf8(_) ->
    Utf8Id = <<"client 漢字編碼"/utf8>>,
    ok = emqx_trace_handler:install("CLI-UTF8", clientid, Utf8Id, debug, "tmp/client-utf8.log"),
    emqx_trace:check(),
    {ok, T} = emqtt:start_link([{clientid, Utf8Id}]),
    emqtt:connect(T),
    [
        begin
            emqtt:publish(T, <<"a/b/c">>, <<"hi">>)
        end
     || _ <- lists:seq(1, 10)
    ],
    emqtt:ping(T),

    ok = filesync("CLI-UTF8", clientid),
    ok = emqx_trace_handler:uninstall(clientid, "CLI-UTF8"),
    emqtt:disconnect(T),
    ?assertEqual([], emqx_trace_handler:running()),
    ok.

t_trace_topic(_Config) ->
    {ok, T} = emqtt:start_link(?CLIENT),
    emqtt:connect(T),

    %% Start tracing
    ok = emqx_trace_handler:install("CLI-TOPIC-1", topic, <<"x/#">>, all, "tmp/topic_trace_x.log"),
    ok = emqx_trace_handler:install("CLI-TOPIC-2", topic, <<"y/#">>, all, "tmp/topic_trace_y.log"),
    emqx_trace:check(),
    ok = filesync("CLI-TOPIC-1", topic),
    ok = filesync("CLI-TOPIC-2", topic),

    %% Verify the tracing file exits
    ?assert(filelib:is_regular("tmp/topic_trace_x.log")),
    ?assert(filelib:is_regular("tmp/topic_trace_y.log")),

    %% Get current traces
    ?assertMatch(
        [
            #{
                type := topic,
                filter := <<"x/#">>,
                level := debug,
                dst := "tmp/topic_trace_x.log",
                name := <<"CLI-TOPIC-1">>
            },
            #{
                type := topic,
                filter := <<"y/#">>,
                name := <<"CLI-TOPIC-2">>,
                level := debug,
                dst := "tmp/topic_trace_y.log"
            }
        ],
        emqx_trace_handler:running()
    ),

    %% Client with clientid = "client" publishes a "hi" message to "x/y/z".
    emqtt:publish(T, <<"x/y/z">>, <<"hi1">>),
    emqtt:publish(T, <<"x/y/z">>, <<"hi2">>),
    emqtt:subscribe(T, <<"x/y/z">>),
    emqtt:unsubscribe(T, <<"x/y/z">>),
    ok = filesync("CLI-TOPIC-1", topic),
    ok = filesync("CLI-TOPIC-2", topic),

    {ok, Bin} = file:read_file("tmp/topic_trace_x.log"),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"hi1">>])),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"hi2">>])),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"PUBLISH">>])),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"SUBSCRIBE">>])),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"UNSUBSCRIBE">>])),
    ?assert(filelib:file_size("tmp/topic_trace_y.log") =:= 0),

    %% Stop tracing
    ok = emqx_trace_handler:uninstall(topic, <<"CLI-TOPIC-1">>),
    ok = emqx_trace_handler:uninstall(topic, <<"CLI-TOPIC-2">>),
    {error, _Reason} = emqx_trace_handler:uninstall(topic, <<"z/#">>),
    ?assertEqual([], emqx_trace_handler:running()),
    emqtt:disconnect(T).

t_trace_ip_address(_Config) ->
    {ok, T} = emqtt:start_link(?CLIENT),
    emqtt:connect(T),

    %% Start tracing
    ok = emqx_trace_handler:install("CLI-IP-1", ip_address, "127.0.0.1", all, "tmp/ip_trace_x.log"),
    ok = emqx_trace_handler:install(
        "CLI-IP-2",
        ip_address,
        "192.168.1.1",
        all,
        "tmp/ip_trace_y.log"
    ),
    emqx_trace:check(),
    ok = filesync(<<"CLI-IP-1">>, ip_address),
    ok = filesync(<<"CLI-IP-2">>, ip_address),

    %% Verify the tracing file exits
    ?assert(filelib:is_regular("tmp/ip_trace_x.log")),
    ?assert(filelib:is_regular("tmp/ip_trace_y.log")),

    %% Get current traces
    ?assertMatch(
        [
            #{
                type := ip_address,
                filter := "127.0.0.1",
                name := <<"CLI-IP-1">>,
                level := debug,
                dst := "tmp/ip_trace_x.log"
            },
            #{
                type := ip_address,
                filter := "192.168.1.1",
                name := <<"CLI-IP-2">>,
                level := debug,
                dst := "tmp/ip_trace_y.log"
            }
        ],
        emqx_trace_handler:running()
    ),

    %% Client with clientid = "client" publishes a "hi" message to "x/y/z".
    emqtt:publish(T, <<"x/y/z">>, <<"hi1">>),
    emqtt:publish(T, <<"x/y/z">>, <<"hi2">>),
    emqtt:subscribe(T, <<"x/y/z">>),
    emqtt:unsubscribe(T, <<"x/y/z">>),
    ok = filesync(<<"CLI-IP-1">>, ip_address),
    ok = filesync(<<"CLI-IP-2">>, ip_address),

    {ok, Bin} = file:read_file("tmp/ip_trace_x.log"),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"hi1">>])),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"hi2">>])),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"PUBLISH">>])),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"SUBSCRIBE">>])),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"UNSUBSCRIBE">>])),
    ?assert(filelib:file_size("tmp/ip_trace_y.log") =:= 0),

    %% Stop tracing
    ok = emqx_trace_handler:uninstall(ip_address, <<"CLI-IP-1">>),
    ok = emqx_trace_handler:uninstall(ip_address, <<"CLI-IP-2">>),
    {error, _Reason} = emqx_trace_handler:uninstall(ip_address, <<"127.0.0.2">>),
    emqtt:disconnect(T),
    ?assertEqual([], emqx_trace_handler:running()).

filesync(Name, Type) ->
    ct:sleep(50),
    filesync(Name, Type, 3).

%% sometime the handler process is not started yet.
filesync(_Name, _Type, 0) ->
    ok;
filesync(Name0, Type, Retry) ->
    Name =
        case is_binary(Name0) of
            true -> Name0;
            false -> list_to_binary(Name0)
        end,
    try
        Handler = binary_to_atom(<<"trace_", (atom_to_binary(Type))/binary, "_", Name/binary>>),
        ok = logger_disk_log_h:filesync(Handler)
    catch
        E:R ->
            ct:pal("Filesync error:~p ~p~n", [{Name, Type, Retry}, {E, R}]),
            ct:sleep(100),
            filesync(Name, Type, Retry - 1)
    end.

init() ->
    emqx_trace:start_link().

terminate() ->
    catch ok = gen_server:stop(emqx_trace, normal, 5000).
