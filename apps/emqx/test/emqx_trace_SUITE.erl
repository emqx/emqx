%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_trace_SUITE).

%% API
-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

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

init_per_testcase(_, Config) ->
    {ok, Pid} = emqx_trace:start_link(),
    true = erlang:unlink(Pid),
    ok = emqx_trace:clear(),
    [{server_pid, Pid} | Config].

end_per_testcase(_, Config) ->
    snabbkaffe:stop(),
    catch gen_server:stop(?config(server_pid, Config), shutdown, infinity),
    ok.

t_base_create_delete(_Config) ->
    Now = erlang:system_time(second),
    Start = Now,
    End = Now + 30 * 60,
    Name = <<"name1">>,
    ClientId = <<"test-device">>,
    Trace = #{
        name => Name,
        filter => {clientid, ClientId},
        start_at => Start,
        end_at => End
    },
    AnotherTrace = Trace#{name => <<"anotherTrace">>},
    {ok, _} = emqx_trace:create(Trace),
    ?assertEqual({error, {already_existed, Name}}, emqx_trace:create(Trace)),
    ?assertEqual({error, {duplicate_condition, Name}}, emqx_trace:create(AnotherTrace)),
    ?assertEqual(
        [
            #{
                enable => true,
                name => <<"name1">>,
                filter => {clientid, <<"test-device">>},
                namespace => global,
                start_at => Now,
                end_at => Now + 30 * 60,
                payload_encode => text,
                payload_limit => 1024,
                formatter => text
            }
        ],
        emqx_trace:list()
    ),
    ?assertEqual(ok, emqx_trace:delete(Name)),
    ?assertEqual({error, not_found}, emqx_trace:delete(Name)),
    ?assertEqual([], emqx_trace:list()),
    ok.

t_create_size_max(_Config) ->
    %% Configure lower limit:
    MaxTraces = 10,
    MaxTracesDefault = emqx_config:get([trace, max_traces]),
    ok = emqx_config:put([trace, max_traces], MaxTraces),
    %% Fill the trace table up to the limit:
    Names = lists:map(
        fun(Seq) ->
            Name = list_to_binary("name" ++ integer_to_list(Seq)),
            {ok, _} = emqx_trace:create(#{
                name => Name,
                filter => {topic, list_to_binary("/x/y/" ++ integer_to_list(Seq))}
            }),
            Name
        end,
        lists:seq(1, MaxTraces)
    ),
    %% Creating one more is disallowed:
    NameExtra = iolist_to_binary(["extra", integer_to_list(erlang:system_time())]),
    TraceExtra = #{
        name => NameExtra,
        filter => {topic, <<"/x/y/extra">>}
    },
    {error, _} = emqx_trace:create(TraceExtra),
    %% Record current atom table size:
    NAtom0 = erlang:system_info(atom_count),
    %% Make space for it, now it should succeed:
    ok = emqx_trace:delete(lists:last(Names)),
    {ok, _} = emqx_trace:create(TraceExtra),
    ?assertEqual(MaxTraces, erlang:length(emqx_trace:list())),
    %% Verify atom table is not growing:
    ?assertEqual(NAtom0, erlang:system_info(atom_count)),
    %% Cleanup:
    emqx_config:put([trace, max_traces], MaxTracesDefault).

t_create_failed(_Config) ->
    Trace = #{name => <<"test">>},
    UnknownField = Trace#{unknown => 12},
    {error, Reason1} = emqx_trace:create(UnknownField),
    ?assertEqual(<<"type=[topic,clientid,ip_address,ruleid] required">>, iolist_to_binary(Reason1)),

    InvalidTopic = Trace#{filter => {topic, <<"#/#//">>}},
    {error, Reason2} = emqx_trace:create(InvalidTopic),
    ?assertEqual(<<"topic: #/#// invalid by 'topic_invalid_#'">>, iolist_to_binary(Reason2)),

    {error, Reason5} = emqx_trace:create(#{
        name => <<"/test">>,
        filter => {clientid, <<"t">>}
    }),
    ?assertEqual(<<"Name should be ^[A-Za-z]+[A-Za-z0-9-_]*$">>, iolist_to_binary(Reason5)),

    ?assertEqual(
        {error, "ip address: einval"},
        emqx_trace:create(Trace#{
            filter => {ip_address, <<"test-name">>}
        })
    ).

t_create_long_name(_Config) ->
    ?assertMatch(
        {ok, #{}},
        emqx_trace:create(#{
            name => binary:copy(<<"test">>, 200),
            filter => {clientid, <<?MODULE_STRING>>}
        })
    ).

t_create_default(_Config) ->
    {error, "name required"} = emqx_trace:create(#{}),
    {ok, _} = emqx_trace:create(#{
        name => <<"test-name">>,
        filter => {clientid, <<"good">>}
    }),
    [#{name := <<"test-name">>}] = emqx_trace:list(),
    ok = emqx_trace:clear(),
    T0 = erlang:system_time(second),
    Trace = #{
        name => <<"test-name">>,
        filter => {topic, <<"/x/y/z">>},
        start_at => T0,
        end_at => T0 - 1
    },
    {error, "end_at time has already passed"} = emqx_trace:create(Trace),
    Trace2 = #{
        name => <<"test-name">>,
        filter => {topic, <<"/x/y/z">>},
        start_at => T0 + 10,
        end_at => T0 + 3
    },
    {error, "failed by start_at >= end_at"} = emqx_trace:create(Trace2),
    {ok, _} = emqx_trace:create(#{
        name => <<"test-name">>,
        filter => {topic, <<"/x/y/z">>}
    }),
    T1 = erlang:system_time(second),
    ?assertMatch(
        [#{start_at := Start, end_at := End}] when
            (End - Start =:= 10 * 60) andalso (Start - T1 < 5),
        emqx_trace:list(),
        T1
    ).

t_create_with_extra_fields(_Config) ->
    ok = emqx_trace:clear(),
    Trace = #{
        name => <<"test-name">>,
        filter => {topic, <<"/x/y/z">>},
        clientid => <<"dev001">>,
        ip_address => <<"127.0.0.1">>
    },
    {ok, _} = emqx_trace:create(Trace),
    ?assertMatch(
        [#{name := <<"test-name">>, filter := {topic, <<"/x/y/z">>}}],
        emqx_trace:list()
    ),
    ok.

t_update_enable(_Config) ->
    Name = <<"test-name">>,
    Now = erlang:system_time(second),
    {ok, _} = emqx_trace:create(#{
        name => Name,
        filter => {topic, <<"/x/y/z">>},
        end_at => Now + 2
    }),
    [#{enable := true}] = emqx_trace:list(),
    ok = emqx_trace:update(Name, false),
    [#{enable := false}] = emqx_trace:list(),
    ok = emqx_trace:update(Name, false),
    [#{enable := false}] = emqx_trace:list(),
    ok = emqx_trace:update(Name, true),
    [#{enable := true}] = emqx_trace:list(),
    ok = emqx_trace:update(Name, false),
    [#{enable := false}] = emqx_trace:list(),
    ?assertEqual({error, not_found}, emqx_trace:update(<<"Name not found">>, true)),
    ct:sleep(2100),
    ?assertEqual({error, finished}, emqx_trace:update(Name, true)),
    ok.

t_load_state(_Config) ->
    Now = erlang:system_time(second),
    Running = #{
        name => <<"Running">>,
        filter => {topic, <<"/x/y/1">>},
        start_at => Now - 1,
        end_at => Now + 2
    },
    Waiting = #{
        name => <<"Waiting">>,
        filter => {topic, <<"/x/y/2">>},
        start_at => Now + 3,
        end_at => Now + 8
    },
    Finished = #{
        name => <<"Finished">>,
        filter => {topic, <<"/x/y/3">>},
        start_at => Now - 5,
        end_at => Now
    },
    {ok, _} = emqx_trace:create(Running),
    {ok, _} = emqx_trace:create(Waiting),
    {error, "end_at time has already passed"} = emqx_trace:create(Finished),
    ?assertMatch(
        [
            #{name := <<"Running">>, enable := true},
            #{name := <<"Waiting">>, enable := true}
        ],
        lists:sort(emqx_trace:list())
    ),
    ct:sleep(3500),
    ?assertMatch(
        [
            #{name := <<"Running">>, enable := false},
            #{name := <<"Waiting">>, enable := true}
        ],
        lists:sort(emqx_trace:list())
    ),
    ok.

t_client_event(_Config) ->
    ClientId = <<"client-test">>,
    Now = erlang:system_time(second),
    Name1 = <<"test_client_id_event">>,
    Name2 = <<"test_topic">>,
    {ok, _} = emqx_trace:create(#{
        name => Name1,
        filter => {clientid, ClientId},
        start_at => Now
    }),
    {ok, Client} = emqtt:start_link([{clean_start, true}, {clientid, ClientId}]),
    {ok, _} = emqtt:connect(Client),
    emqtt:ping(Client),
    {ok, _} = emqtt:publish(Client, <<"/test">>, #{}, <<"1">>, [{qos, 1}]),
    {ok, _} = emqtt:publish(Client, <<"/test">>, #{}, <<"2">>, [{qos, 1}]),
    {ok, _} = emqx_trace:create(#{
        name => Name2,
        filter => {topic, <<"/test">>},
        start_at => Now
    }),
    {ok, Bin, _} = emqx_trace:stream_log(Name1, start, undefined),
    {ok, _} = emqtt:publish(Client, <<"/test">>, #{}, <<"3">>, [{qos, 1}]),
    {ok, _} = emqtt:publish(Client, <<"/test">>, #{}, <<"4">>, [{qos, 1}]),
    ok = emqtt:disconnect(Client),
    {ok, Bin2, _} = emqx_trace:stream_log(Name1, start, undefined),
    {ok, Bin3, _} = emqx_trace:stream_log(Name2, start, undefined),
    ct:pal("Bin1 ~p Bin2 ~p Bin3 ~p", [byte_size(Bin), byte_size(Bin2), byte_size(Bin3)]),
    ?assert(erlang:byte_size(Bin) > 0),
    ?assert(erlang:byte_size(Bin) < erlang:byte_size(Bin2)),
    ?assert(erlang:byte_size(Bin3) > 0),
    ok.

t_client_huge_payload_truncated(_Config) ->
    ClientId = <<"client-truncated1">>,
    Now = erlang:system_time(second),
    Name1 = <<"test_client_id_truncated1">>,
    Name2 = <<"test_topic">>,
    {ok, _} = emqx_trace:create(#{
        name => Name1,
        filter => {clientid, ClientId},
        start_at => Now
    }),
    {ok, Client} = emqtt:start_link([{clean_start, true}, {clientid, ClientId}]),
    {ok, _} = emqtt:connect(Client),
    emqtt:ping(Client),
    PayloadLimit = 1024,
    NormalPayload = iolist_to_binary(lists:duplicate(1024, "x")),
    Size1 = 1025,
    TruncatedBytes1 = Size1 - PayloadLimit,
    HugePayload1 = iolist_to_binary(lists:duplicate(Size1, "y")),
    Size2 = 1024 * 10,
    HugePayload2 = iolist_to_binary(lists:duplicate(Size2, "z")),
    TruncatedBytes2 = Size2 - PayloadLimit,
    {ok, _} = emqtt:publish(Client, <<"/test">>, #{}, NormalPayload, [{qos, 1}]),
    {ok, _} = emqx_trace:create(#{
        name => Name2,
        filter => {topic, <<"/test">>},
        start_at => Now,
        payload_limit => PayloadLimit
    }),

    {ok, Bin, _} = emqx_trace:stream_log(Name1, start, undefined),
    {ok, _} = emqtt:publish(Client, <<"/test">>, #{}, NormalPayload, [{qos, 1}]),
    {ok, _} = emqtt:publish(Client, <<"/test">>, #{}, HugePayload1, [{qos, 1}]),
    {ok, _} = emqtt:publish(Client, <<"/test">>, #{}, HugePayload2, [{qos, 1}]),
    ok = emqtt:disconnect(Client),
    {ok, Bin2, _} = emqx_trace:stream_log(Name1, start, undefined),
    {ok, Bin3, _} = emqx_trace:stream_log(Name2, start, undefined),
    ?assert(erlang:byte_size(Bin) > 1024),
    ?assert(erlang:byte_size(Bin) < erlang:byte_size(Bin2)),
    ?assert(erlang:byte_size(Bin3) > 1024),

    %% Don't have format crash
    CrashBin = <<"CRASH">>,
    ?assertEqual(nomatch, binary:match(Bin, [CrashBin])),
    ?assertEqual(nomatch, binary:match(Bin2, [CrashBin])),
    ?assertEqual(nomatch, binary:match(Bin3, [CrashBin])),
    Re = <<"\\.\\.\\.\\([0-9]+\\sbytes\\)">>,
    ?assertMatch(nomatch, re:run(Bin, Re, [unicode])),
    ReN = fun(N) -> iolist_to_binary(["\\.\\.\\.\\(", integer_to_list(N), "\\sbytes\\)"]) end,
    ?assertMatch({match, _}, re:run(Bin2, ReN(TruncatedBytes1), [unicode])),
    ?assertMatch({match, _}, re:run(Bin3, ReN(TruncatedBytes2), [unicode])),
    ok.

%% If no relevant event occurred, the log file size must be exactly 0 after stopping the trace.
t_empty_trace_log_file(_Config) ->
    ?check_trace(
        begin
            Now = erlang:system_time(second),
            Name = <<"empty_trace_log">>,
            Trace = #{
                name => Name,
                filter => {clientid, <<"test_trace_no_clientid_1">>},
                start_at => Now,
                end_at => Now + 100
            },
            ?wait_async_action(
                ?assertMatch({ok, _}, emqx_trace:create(Trace)),
                #{?snk_kind := update_trace_done}
            ),
            ?assertMatch({ok, #{size := 0}}, emqx_trace:log_details(Name)),
            ?wait_async_action(
                ?assertEqual(ok, emqx_trace:update(Name, false)),
                #{?snk_kind := update_trace_done}
            ),
            ?assertMatch({ok, #{size := 0}}, emqx_trace:log_details(Name)),
            ?assertEqual(ok, emqx_trace:delete(Name))
        end,
        []
    ).

t_stream_continuity(_Config) ->
    %% Configure relatively low size limit:
    MaxSize = 10 * 1024,
    PayloadLimit = 400,
    StreamLimit = 512,
    MaxSizeDefault = emqx_config:get([trace, max_file_size]),
    ok = emqx_config:put([trace, max_file_size], MaxSize),
    %% Start a trace:
    ClientId = atom_to_binary(?FUNCTION_NAME),
    Name = <<"test_", ClientId/binary>>,
    {ok, _} = emqx_trace:create(#{
        name => Name,
        filter => {clientid, ClientId},
        payload_limit => PayloadLimit
    }),
    {ok, Client} = emqtt:start_link([{clean_start, true}, {clientid, ClientId}]),
    {ok, _} = emqtt:connect(Client),
    %% Start a log stream:
    {ok, Bin1, {_, Cursor1}} = emqx_trace:stream_log(Name, start, StreamLimit),
    %% Publish a few messages containing incrementing counter:
    [
        {ok, _} = emqtt:publish(Client, <<"counter">>, integer_to_binary(I), qos1)
     || I <- lists:seq(1, 10)
    ],
    %% Trigger filesync:
    {ok, #{size := TotalSize}} = emqx_trace:log_details(Name),
    %% Continue reading the stream:
    {Bins, Cursor2} = stream_until_eof(Name, Cursor1, StreamLimit),
    %% Verify that "tailing" the file works:
    {ok, <<>>, {eof, Cursor3}} = emqx_trace:stream_log(Name, {cont, Cursor2}, undefined),
    %% Verify continuity:
    Content = [Bin1 | Bins],
    ?assertEqual(TotalSize, iolist_size(Content), Content),
    ?assertEqual(
        [integer_to_binary(I) || I <- lists:seq(1, 10)],
        trace_extract_payloads(Content)
    ),
    %% Publish more than max file size allows:
    [
        {ok, _} = emqtt:publish(Client, <<"counter">>, integer_to_binary(I), qos1)
     || I <- lists:seq(11, 100)
    ],
    %% Trigger filesync:
    {ok, #{}} = emqx_trace:log_details(Name),
    %% Stream observes discontinuity:
    ?assertMatch(
        {error, {file_error, Reason}} when Reason == enoent; Reason == stale,
        emqx_trace:stream_log(Name, {cont, Cursor3}, undefined)
    ),
    %% Cleanup:
    ok = emqx_trace:delete(Name),
    ok = emqtt:disconnect(Client),
    ok = emqx_config:put([trace, max_file_size], MaxSizeDefault).

t_stream_tailf(_Config) ->
    test_stream_tailf(10 * 1024, 400, 1024, 100, 1).

t_stream_tailf_small_reads(_Config) ->
    test_stream_tailf(10 * 1024, 400, 32, 50, 3).

t_stream_tailf_whole_reads(_Config) ->
    test_stream_tailf(8 * 1024, 400, undefined, 100, 1).

test_stream_tailf(MaxSize, PayloadLimit, StreamReadSize, NMsg, Cooldown) ->
    TCPid = self(),
    %% Configure relatively low size limit:
    MaxSizeDefault = emqx_config:get([trace, max_file_size]),
    ok = emqx_config:put([trace, max_file_size], MaxSize),
    %% Start a trace:
    ClientId = atom_to_binary(?FUNCTION_NAME),
    Name = <<"test_", ClientId/binary>>,
    {ok, _} = emqx_trace:create(#{
        name => Name,
        filter => {clientid, ClientId},
        payload_limit => PayloadLimit
    }),
    {ok, Client} = emqtt:start_link([{clean_start, true}, {clientid, ClientId}]),
    {ok, _} = emqtt:connect(Client),
    %% Spawn a log stream receiver:
    Receiver = fun Receiver(Cursor0, Acc0) ->
        {Bins, Cursor} = stream_until_eof(Name, Cursor0, StreamReadSize),
        Acc = [Acc0 | Bins],
        receive
            {TCPid, stop} ->
                exit({received, Acc})
        after 1 ->
            Receiver(Cursor, Acc)
        end
    end,
    {ReceiverPid, MRef} = spawn_monitor(fun() ->
        Receiver(start, [])
    end),
    %% Publish a lot of messages:
    lists:foreach(
        fun(I) ->
            ok = timer:sleep(Cooldown),
            {ok, _} = emqtt:publish(Client, <<"c">>, integer_to_binary(I), qos1)
        end,
        lists:seq(1, NMsg)
    ),
    ok = timer:sleep(100),
    %% Trigger filesync:
    {ok, #{size := CurrentSize}} = emqx_trace:log_details(Name),
    %% Ask the receiver to stop and hand us the log stream it has accumulated:
    ReceiverPid ! {self(), stop},
    Signal = ?assertReceive({'DOWN', MRef, process, _Pid, {received, _}}),
    {received, Content} = element(5, Signal),
    ?assertEqual(
        [integer_to_binary(I) || I <- lists:seq(1, NMsg)],
        trace_extract_payloads(Content)
    ),
    %% We were able to stream much more than what is currently used by the log:
    ?assert(
        CurrentSize < iolist_size(Content),
        {CurrentSize, iolist_size(Content)}
    ),
    %% Cleanup:
    ok = emqx_trace:delete(Name),
    ok = emqtt:disconnect(Client),
    ok = emqx_config:put([trace, max_file_size], MaxSizeDefault).

trace_extract_payloads(TraceContent) ->
    Lines = string:split(TraceContent, "\n", all),
    [
        M
     || L <- Lines,
        {match, [M]} <- [re:run(L, "payload: ([0-9]+)", [{capture, all_but_first, binary}])]
    ].

stream_until_eof(Name, Cursor0, Limit) ->
    case Cursor0 of
        start ->
            Cont = start;
        _ ->
            Cont = {cont, Cursor0}
    end,
    case emqx_trace:stream_log(Name, Cont, Limit) of
        {ok, Bin, {eof, CursorLast}} ->
            {[Bin], CursorLast};
        {ok, Bin, {X, Cursor}} ->
            case X of
                retry -> timer:sleep(10);
                cont -> ok
            end,
            {Acc, CursorLast} = stream_until_eof(Name, Cursor, Limit),
            {[Bin | Acc], CursorLast}
    end.
