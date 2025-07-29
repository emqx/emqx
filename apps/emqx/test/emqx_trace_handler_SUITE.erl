%%--------------------------------------------------------------------
%% Copyright (c) 2019-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

all() ->
    emqx_common_test_helpers:all(?MODULE).

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
    ok = install_handler("CLI-client1", clientid, "client", debug, "tmp/client.log"),
    ok = install_handler("CLI-client2", clientid, <<"client2">>, all, "tmp/client2.log"),
    ok = install_handler("CLI-client3", clientid, <<"client3">>, all, "tmp/client3.log"),
    {error, {handler_not_added, {file_error, ".", eisdir}}} =
        install_handler("CLI-client5", clientid, <<"client5">>, debug, "."),
    emqx_trace:check(),

    %% Verify the tracing file exits
    ok = wait_filesync(),
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

    %% Verify messages are logged to "tmp/client.log" but not "tmp/client2.log".
    ok = wait_filesync(),
    {ok, Bin} = file:read_file("tmp/client.log"),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"CONNECT">>])),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"CONNACK">>])),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"PUBLISH">>])),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"PINGREQ">>])),
    ?assert(filelib:file_size("tmp/client2.log") == 0),

    %% Stop tracing
    ok = uninstall_handler("CLI-client1"),
    ok = uninstall_handler("CLI-client2"),
    ok = uninstall_handler("CLI-client3"),

    emqtt:disconnect(T),
    ?assertEqual([], emqx_trace_handler:running()).

t_trace_clientid_utf8(_) ->
    Utf8Id = <<"client 漢字編碼"/utf8>>,
    ok = install_handler("CLI-UTF8", clientid, Utf8Id, debug, "tmp/client-utf8.log"),
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

    ok = uninstall_handler("CLI-UTF8"),
    emqtt:disconnect(T),
    ?assertEqual([], emqx_trace_handler:running()),
    ok.

t_trace_topic(_Config) ->
    {ok, T} = emqtt:start_link(?CLIENT),
    emqtt:connect(T),

    %% Start tracing
    ok = install_handler("CLI-TOPIC-1", topic, <<"x/#">>, all, "tmp/topic_trace_x.log"),
    ok = install_handler("CLI-TOPIC-2", topic, <<"y/#">>, all, "tmp/topic_trace_y.log"),
    emqx_trace:check(),

    %% Verify the tracing file exits
    ok = wait_filesync(),
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

    ok = wait_filesync(),
    {ok, Bin} = file:read_file("tmp/topic_trace_x.log"),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"hi1">>])),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"hi2">>])),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"PUBLISH">>])),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"SUBSCRIBE">>])),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"UNSUBSCRIBE">>])),
    ?assert(filelib:file_size("tmp/topic_trace_y.log") =:= 0),

    %% Stop tracing
    ok = uninstall_handler("CLI-TOPIC-1"),
    ok = uninstall_handler("CLI-TOPIC-2"),
    {error, _Reason} = uninstall_handler("z/#"),
    ?assertEqual([], emqx_trace_handler:running()),
    emqtt:disconnect(T).

t_trace_ip_address(_Config) ->
    {ok, T} = emqtt:start_link(?CLIENT),
    emqtt:connect(T),

    %% Start tracing
    ok = install_handler("CLI-IP-1", ip_address, "127.0.0.1", all, "tmp/ip_trace_x.log"),
    ok = install_handler("CLI-IP-2", ip_address, "192.168.1.1", all, "tmp/ip_trace_y.log"),
    emqx_trace:check(),

    %% Verify the tracing file exits
    ok = wait_filesync(),
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

    ok = wait_filesync(),
    {ok, Bin} = file:read_file("tmp/ip_trace_x.log"),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"hi1">>])),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"hi2">>])),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"PUBLISH">>])),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"SUBSCRIBE">>])),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"UNSUBSCRIBE">>])),
    ?assert(filelib:file_size("tmp/ip_trace_y.log") =:= 0),

    %% Stop tracing
    ok = uninstall_handler("CLI-IP-1"),
    ok = uninstall_handler("CLI-IP-2"),
    {error, _Reason} = uninstall_handler("127.0.0.2"),
    emqtt:disconnect(T),
    ?assertEqual([], emqx_trace_handler:running()).

t_trace_max_file_size(_Config) ->
    Name = <<"CLI-trace_max_file_size">>,
    FileName = "tmp/trace_max_file_size.log",
    %% Configure relatively low size limit:
    MaxSize = 5 * 1024,
    MaxSizeDefault = emqx_config:get([trace, max_file_size]),
    ok = emqx_config:put([trace, max_file_size], MaxSize),
    %% Start tracing:
    ok = emqx_trace_handler:install(?FUNCTION_NAME, Name, topic, <<"t/#">>, all, FileName, text),
    ok = emqx_trace:check(),
    ?assertMatch(
        [#{name := Name, type := topic, filter := <<"t/#">>}],
        emqx_trace_handler:running()
    ),
    %% Start publisher publishing 50 messages with non-trivial payload:
    {ok, C} = emqtt:start_link(?CLIENT),
    {ok, _} = emqtt:connect(C),
    ok = lists:foreach(
        fun(N) ->
            Topic = emqx_topic:join(["t", "topic", integer_to_list(N)]),
            {ok, _} = emqtt:publish(C, Topic, binary:copy(<<"HELLO!">>, N), qos1)
        end,
        lists:seq(1, 50)
    ),
    %% At that point max size should already have been reached:
    ok = wait_filesync(),
    FileSize = filelib:file_size(FileName),
    ?assertMatch(
        FS when FS =< MaxSize andalso FS > MaxSize div 2,
        FileSize,
        {max_file_size, MaxSize}
    ),
    %% Verify log does not grow anymore:
    {ok, _} = emqtt:publish(C, <<"t/lastone">>, binary:copy(<<"BYE!">>, 10), qos1),
    ok = wait_filesync(),
    ?assertEqual(FileSize, filelib:file_size(FileName)),
    %% Cleanup:
    ok = emqtt:disconnect(C),
    ok = emqx_trace_handler:uninstall(?FUNCTION_NAME),
    ok = emqx_config:put([trace, max_file_size], MaxSizeDefault).

wait_filesync() ->
    %% NOTE: Twice as long as `?LOG_HANDLER_FILESYNC_INTERVAL` in `emqx_trace_handler`.
    timer:sleep(2 * 100).

install_handler(Name, Type, Filter, Level, LogFile) ->
    HandlerId = list_to_atom(?MODULE_STRING ++ ":" ++ Name),
    emqx_trace_handler:install(HandlerId, Name, Type, Filter, Level, LogFile, text).

uninstall_handler(Name) ->
    HandlerId = list_to_atom(?MODULE_STRING ++ ":" ++ Name),
    emqx_trace_handler:uninstall(HandlerId).

init() ->
    emqx_trace:start_link().

terminate() ->
    catch ok = gen_server:stop(emqx_trace, normal, 5000).
