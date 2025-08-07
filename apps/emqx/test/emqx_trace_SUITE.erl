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

t_trace_file(_Config) ->
    FileName = "test.log",
    Content = <<"test \n test">>,
    TraceDir = emqx_trace:trace_dir(),
    File = filename:join(TraceDir, FileName),
    ok = file:write_file(File, Content),
    {ok, Node, Bin} = emqx_trace:trace_file(FileName),
    ?assertEqual(Node, atom_to_list(node())),
    ?assertEqual(Content, Bin),
    ok = file:delete(File),
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
