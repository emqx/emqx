%%--------------------------------------------------------------------
%% Copyright (c) 2019-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_trace_handler_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("kernel/include/file.hrl").

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

    Filename1 = "tmp/client.log",
    Filename2 = "tmp/client2.log",
    Filename3 = "tmp/client3.log",
    ok = install_handler("CLI-client1", {clientid, "client"}, debug, Filename1),
    ok = install_handler("CLI-client2", {clientid, <<"client2">>}, all, Filename2),
    ok = install_handler("CLI-client3", {clientid, <<"client3">>}, all, Filename3),
    ?assertMatch(
        {error, {handler_not_added, {open_failed, _, eisdir}}},
        install_handler("CLI-client4", {clientid, <<"client4">>}, debug, ".")
    ),

    %% Update trace handlers pterm.
    emqx_trace:check(),

    %% Verify the tracing file exits
    ok = wait_filesync(),
    ?assert(filelib:is_regular(Filename1)),
    ?assert(filelib:is_regular(Filename2)),
    ?assert(filelib:is_regular(Filename3)),

    %% Get current traces
    Filepath1 = filename:absname(Filename1),
    Filepath2 = filename:absname(Filename2),
    Filepath3 = filename:absname(Filename3),
    ?assertMatch(
        [
            #{
                name := <<"CLI-client1">>,
                filter := {clientid, <<"client">>},
                level := debug,
                dst := Filepath1
            },
            #{
                name := <<"CLI-client2">>,
                filter := {clientid, <<"client2">>},
                level := debug,
                dst := Filepath2
            },
            #{
                name := <<"CLI-client3">>,
                filter := {clientid, <<"client3">>},
                level := debug,
                dst := Filepath3
            }
        ],
        emqx_trace_handler:running()
    ),

    %% Client with clientid = "client" publishes a "hi" message to "a/b/c".
    {ok, T} = emqtt:start_link(?CLIENT),
    emqtt:connect(T),
    emqtt:publish(T, <<"a/b/c">>, <<"hi">>),
    emqtt:ping(T),

    %% Verify messages are logged to the topmost "tmp/client.log" but not "tmp/client2.log".
    ok = wait_filesync(),
    {ok, _, _, Frag0} = emqx_trace_handler:find_log_fragment(first, Filename1),
    {ok, Bin, Frag1} = emqx_trace_handler:read_log_fragment_at(Frag0, Filename1, 0, 1 bsl 24),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"CONNECT">>])),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"CONNACK">>])),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"PUBLISH">>])),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"PINGREQ">>])),
    ?assert(filelib:file_size(Filename2) == 0),

    %% No rotations should have happened.
    ?assertEqual(none, emqx_trace_handler:find_log_fragment({next, Frag1}, Filename1)),
    ?assertEqual({error, enoent}, file:read_file(Filename1 ++ ".0")),

    %% Stop tracing
    ok = uninstall_handler("CLI-client1"),
    ok = uninstall_handler("CLI-client2"),
    ok = uninstall_handler("CLI-client3"),

    emqtt:disconnect(T),
    ?assertEqual([], emqx_trace_handler:running()).

t_trace_clientid_utf8(_) ->
    Utf8Id = <<"client 漢字編碼"/utf8>>,
    ok = install_handler("CLI-UTF8", {clientid, Utf8Id}, debug, "tmp/client-utf8.log"),
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
    Filename1 = "tmp/topic_trace_x.log",
    Filename2 = "tmp/topic_trace_y.log",
    ok = install_handler("CLI-TOPIC-1", {topic, <<"x/#">>}, all, Filename1),
    ok = install_handler("CLI-TOPIC-2", {topic, <<"y/#">>}, all, Filename2),
    emqx_trace:check(),

    %% Verify the tracing file exits
    ok = wait_filesync(),
    ?assert(filelib:is_regular(Filename1)),
    ?assert(filelib:is_regular(Filename2)),

    %% Get current traces
    Filepath1 = filename:absname(Filename1),
    Filepath2 = filename:absname(Filename2),
    ?assertMatch(
        [
            #{
                name := <<"CLI-TOPIC-1">>,
                filter := {topic, <<"x/#">>},
                level := debug,
                dst := Filepath1
            },
            #{
                name := <<"CLI-TOPIC-2">>,
                filter := {topic, <<"y/#">>},
                level := debug,
                dst := Filepath2
            }
        ],
        emqx_trace_handler:running()
    ),

    %% Client with clientid = "client" publishes a "hi" message to "x/y/z".
    emqtt:publish(T, <<"x/y/z">>, <<"hi1">>),
    emqtt:publish(T, <<"x/y/z">>, <<"hi2">>),
    emqtt:subscribe(T, <<"x/y/z">>),
    emqtt:unsubscribe(T, <<"x/y/z">>),

    %% Inspect the topmost tracing file.
    ok = wait_filesync(),
    {ok, _, _, Frag0} = emqx_trace_handler:find_log_fragment(first, Filename1),
    {ok, Bin, Frag1} = emqx_trace_handler:read_log_fragment_at(Frag0, Filename1, 0, 1 bsl 24),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"hi1">>])),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"hi2">>])),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"PUBLISH">>])),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"SUBSCRIBE">>])),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"UNSUBSCRIBE">>])),
    ?assert(filelib:file_size(Filename2) =:= 0),

    %% No rotations should have happened.
    ?assertEqual(none, emqx_trace_handler:find_log_fragment({next, Frag1}, Filename1)),
    ?assertEqual({error, enoent}, file:read_file(Filename1 ++ ".0")),

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
    Filename1 = "tmp/ip_trace_x.log",
    Filename2 = "tmp/ip_trace_y.log",
    ok = install_handler("CLI-IP-1", {ip_address, "127.0.0.1"}, all, Filename1),
    ok = install_handler("CLI-IP-2", {ip_address, "192.168.1.1"}, all, Filename2),
    emqx_trace:check(),

    %% Verify the topmost tracing file exits
    ok = wait_filesync(),
    ?assert(filelib:is_regular(Filename1)),
    ?assert(filelib:is_regular(Filename2)),

    %% Get current traces
    Filepath1 = filename:absname(Filename1),
    Filepath2 = filename:absname(Filename2),
    ?assertMatch(
        [
            #{
                name := <<"CLI-IP-1">>,
                filter := {ip_address, "127.0.0.1"},
                level := debug,
                dst := Filepath1
            },
            #{
                name := <<"CLI-IP-2">>,
                filter := {ip_address, "192.168.1.1"},
                level := debug,
                dst := Filepath2
            }
        ],
        emqx_trace_handler:running()
    ),

    %% Client with clientid = "client" publishes a "hi" message to "x/y/z".
    emqtt:publish(T, <<"x/y/z">>, <<"hi1">>),
    emqtt:publish(T, <<"x/y/z">>, <<"hi2">>),
    emqtt:subscribe(T, <<"x/y/z">>),
    emqtt:unsubscribe(T, <<"x/y/z">>),

    %% Inspect the topmost tracing file.
    ok = wait_filesync(),
    {ok, _, _, Frag0} = emqx_trace_handler:find_log_fragment(first, Filename1),
    {ok, Bin, _Frag1} = emqx_trace_handler:read_log_fragment_at(Frag0, Filename1, 0, 1 bsl 24),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"hi1">>])),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"hi2">>])),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"PUBLISH">>])),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"SUBSCRIBE">>])),
    ?assertNotEqual(nomatch, binary:match(Bin, [<<"UNSUBSCRIBE">>])),
    ?assert(filelib:file_size(Filename2) =:= 0),

    %% Stop tracing
    ok = uninstall_handler("CLI-IP-1"),
    ok = uninstall_handler("CLI-IP-2"),
    {error, _Reason} = uninstall_handler("127.0.0.2"),
    emqtt:disconnect(T),
    ?assertEqual([], emqx_trace_handler:running()).

t_trace_max_file_size(_Config) ->
    Name = <<"CLI-trace_max_file_size">>,
    Filename = "tmp/trace_max_file_size.log",
    %% Configure relatively low size limit:
    MaxSize = 10 * 1024,
    PayloadLimit = 400,
    MaxSizeDefault = emqx_config:get([trace, max_file_size]),
    ok = emqx_config:put([trace, max_file_size], MaxSize),
    %% Start tracing:
    ok = emqx_trace_handler:install(
        ?FUNCTION_NAME,
        #{
            name => Name,
            filter => {topic, <<"t/#">>},
            formatter => text,
            payload_encode => text,
            payload_limit => PayloadLimit
        },
        all,
        Filename
    ),
    ok = emqx_trace:check(),
    ?assertMatch(
        [#{name := Name, filter := {topic, <<"t/#">>}}],
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
        lists:seq(1, 80)
    ),
    %% At that point max size should already have been reached:
    ok = wait_filesync(),
    Fragments1 = enum_fragments(Filename),
    TotalSize1 = lists:sum([Size || {_FN, #file_info{size = Size}} <- Fragments1]),
    ?assertMatch(
        %% NOTE
        %% Small overspill is currently expected due to how `logger_std_h` works.
        %% Estimate it as `PayloadLimit * 4`.
        FS when FS < MaxSize + PayloadLimit * 4 andalso FS > MaxSize div 2,
        TotalSize1,
        #{max_file_size => MaxSize, payload_limit => PayloadLimit}
    ),
    %% Verify log does really grow:
    {ok, _} = emqtt:publish(C, <<"t/lastone">>, binary:copy(<<"BYE!">>, 10), qos1),
    ok = wait_filesync(),
    Fragments2 = enum_fragments(Filename),
    TotalSize2 = lists:sum([Size || {_FN, #file_info{size = Size}} <- Fragments2]),
    ?assertMatch(
        FS when FS < MaxSize + PayloadLimit * 4 andalso FS > MaxSize div 2,
        TotalSize2,
        #{max_file_size => MaxSize, payload_limit => PayloadLimit}
    ),
    %% Cleanup:
    ok = emqtt:disconnect(C),
    ok = emqx_trace_handler:uninstall(?FUNCTION_NAME),
    ok = emqx_config:put([trace, max_file_size], MaxSizeDefault).

enum_fragments(Basename) ->
    enum_fragments(first, Basename).

enum_fragments(Which, Basename) ->
    case emqx_trace_handler:find_log_fragment(Which, Basename) of
        {ok, Filename, Info, Frag} ->
            [{Filename, Info} | enum_fragments({next, Frag}, Basename)];
        none ->
            []
    end.

wait_filesync() ->
    %% NOTE: Twice as long as `?LOG_HANDLER_FILESYNC_INTERVAL` in `emqx_trace_handler`.
    timer:sleep(2 * 100).

install_handler(Name, Filter, Level, LogFile) ->
    HandlerId = list_to_atom(?MODULE_STRING ++ ":" ++ Name),
    emqx_trace_handler:install(HandlerId, Name, Filter, Level, LogFile, text).

uninstall_handler(Name) ->
    HandlerId = list_to_atom(?MODULE_STRING ++ ":" ++ Name),
    emqx_trace_handler:uninstall(HandlerId).

init() ->
    emqx_trace:start_link().

terminate() ->
    catch ok = gen_server:stop(emqx_trace, normal, 5000).
