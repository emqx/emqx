%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_trace_SUITE).

%% API
-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_trace.hrl").
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
    Listeners = emqx_listeners:list(),
    ct:pal("emqx_listeners:list() = ~p~n", [Listeners]),
    ?assertMatch(
        [_ | _],
        [ID || {ID, #{running := true}} <- Listeners]
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    ok = emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(_, Config) ->
    reload(),
    ok = emqx_trace:clear(),
    ct:pal("load:~p~n", [erlang:whereis(emqx_trace)]),
    Config.

end_per_testcase(_) ->
    snabbkaffe:stop(),
    ok.

t_base_create_delete(_Config) ->
    Now = erlang:system_time(second),
    Start = Now,
    End = Now + 30 * 60,
    Name = <<"name1">>,
    ClientId = <<"test-device">>,
    Trace = #{
        name => Name,
        type => clientid,
        clientid => ClientId,
        start_at => Start,
        end_at => End
    },
    AnotherTrace = Trace#{name => <<"anotherTrace">>},
    {ok, _} = emqx_trace:create(Trace),
    ?assertEqual({error, {already_existed, Name}}, emqx_trace:create(Trace)),
    ?assertEqual({error, {duplicate_condition, Name}}, emqx_trace:create(AnotherTrace)),
    [TraceRec] = emqx_trace:list(),
    Expect = #emqx_trace{
        name = Name,
        type = clientid,
        filter = ClientId,
        start_at = Now,
        end_at = Now + 30 * 60
    },
    ?assertEqual(Expect, TraceRec),
    ExpectFormat = [
        #{
            filter => <<"test-device">>,
            enable => true,
            type => clientid,
            name => <<"name1">>,
            start_at => Now,
            end_at => Now + 30 * 60,
            payload_encode => text,
            formatter => text
        }
    ],
    ?assertEqual(ExpectFormat, emqx_trace:format([TraceRec])),
    ?assertEqual(ok, emqx_trace:delete(Name)),
    ?assertEqual({error, not_found}, emqx_trace:delete(Name)),
    ?assertEqual([], emqx_trace:list()),
    ok.

t_create_size_max(_Config) ->
    lists:map(
        fun(Seq) ->
            Name = list_to_binary("name" ++ integer_to_list(Seq)),
            Trace = [
                {name, Name},
                {type, topic},
                {topic, list_to_binary("/x/y/" ++ integer_to_list(Seq))}
            ],
            {ok, _} = emqx_trace:create(Trace)
        end,
        lists:seq(1, 30)
    ),
    Trace31 = [
        {<<"name">>, <<"name31">>},
        {<<"type">>, topic},
        {<<"topic">>, <<"/x/y/31">>}
    ],
    {error, _} = emqx_trace:create(Trace31),
    ok = emqx_trace:delete(<<"name30">>),
    {ok, _} = emqx_trace:create(Trace31),
    ?assertEqual(30, erlang:length(emqx_trace:list())),
    ok.

t_create_failed(_Config) ->
    Name = {<<"name">>, <<"test">>},
    UnknownField = [Name, {<<"unknown">>, 12}],
    {error, Reason1} = emqx_trace:create(UnknownField),
    ?assertEqual(<<"type=[topic,clientid,ip_address] required">>, iolist_to_binary(Reason1)),

    InvalidTopic = [Name, {<<"topic">>, "#/#//"}, {<<"type">>, topic}],
    {error, Reason2} = emqx_trace:create(InvalidTopic),
    ?assertEqual(<<"topic: #/#// invalid by function_clause">>, iolist_to_binary(Reason2)),

    {error, Reason4} = emqx_trace:create([Name, {<<"type">>, clientid}]),
    ?assertEqual(<<"required clientid field">>, iolist_to_binary(Reason4)),

    InvalidPackets4 = [
        {<<"name">>, <<"/test">>},
        {<<"clientid">>, <<"t">>},
        {<<"type">>, clientid}
    ],
    {error, Reason5} = emqx_trace:create(InvalidPackets4),
    ?assertEqual(<<"Name should be ^[A-Za-z]+[A-Za-z0-9-_]*$">>, iolist_to_binary(Reason5)),

    ?assertEqual(
        {error, "type=[topic,clientid,ip_address] required"},
        emqx_trace:create([{<<"name">>, <<"test-name">>}, {<<"clientid">>, <<"good">>}])
    ),

    ?assertEqual(
        {error, "ip address: einval"},
        emqx_trace:create([
            Name,
            {<<"type">>, ip_address},
            {<<"ip_address">>, <<"test-name">>}
        ])
    ),
    ok.

t_create_default(_Config) ->
    {error, "name required"} = emqx_trace:create([]),
    {ok, _} = emqx_trace:create([
        {<<"name">>, <<"test-name">>},
        {<<"type">>, clientid},
        {<<"clientid">>, <<"good">>}
    ]),
    [#emqx_trace{name = <<"test-name">>}] = emqx_trace:list(),
    ok = emqx_trace:clear(),
    Now = erlang:system_time(second),
    Trace = [
        {<<"name">>, <<"test-name">>},
        {<<"type">>, topic},
        {<<"topic">>, <<"/x/y/z">>},
        {<<"start_at">>, Now},
        {<<"end_at">>, Now - 1}
    ],
    {error, "end_at time has already passed"} = emqx_trace:create(Trace),
    Trace2 = [
        {<<"name">>, <<"test-name">>},
        {<<"type">>, topic},
        {<<"topic">>, <<"/x/y/z">>},
        {<<"start_at">>, Now + 10},
        {<<"end_at">>, Now + 3}
    ],
    {error, "failed by start_at >= end_at"} = emqx_trace:create(Trace2),
    {ok, _} = emqx_trace:create([
        {<<"name">>, <<"test-name">>},
        {<<"type">>, topic},
        {<<"topic">>, <<"/x/y/z">>}
    ]),
    [#emqx_trace{start_at = Start, end_at = End}] = emqx_trace:list(),
    ?assertEqual(10 * 60, End - Start),
    ?assertEqual(true, Start - erlang:system_time(second) < 5),
    ok.

t_create_with_extra_fields(_Config) ->
    ok = emqx_trace:clear(),
    Trace = [
        {<<"name">>, <<"test-name">>},
        {<<"type">>, topic},
        {<<"topic">>, <<"/x/y/z">>},
        {<<"clientid">>, <<"dev001">>},
        {<<"ip_address">>, <<"127.0.0.1">>}
    ],
    {ok, _} = emqx_trace:create(Trace),
    ?assertMatch(
        [#emqx_trace{name = <<"test-name">>, filter = <<"/x/y/z">>, type = topic}],
        emqx_trace:list()
    ),
    ok.

t_update_enable(_Config) ->
    Name = <<"test-name">>,
    Now = erlang:system_time(second),
    {ok, _} = emqx_trace:create([
        {<<"name">>, Name},
        {<<"type">>, topic},
        {<<"topic">>, <<"/x/y/z">>},
        {<<"end_at">>, Now + 2}
    ]),
    [#emqx_trace{enable = Enable}] = emqx_trace:list(),
    ?assertEqual(Enable, true),
    ok = emqx_trace:update(Name, false),
    [#emqx_trace{enable = false}] = emqx_trace:list(),
    ok = emqx_trace:update(Name, false),
    [#emqx_trace{enable = false}] = emqx_trace:list(),
    ok = emqx_trace:update(Name, true),
    [#emqx_trace{enable = true}] = emqx_trace:list(),
    ok = emqx_trace:update(Name, false),
    [#emqx_trace{enable = false}] = emqx_trace:list(),
    ?assertEqual({error, not_found}, emqx_trace:update(<<"Name not found">>, true)),
    ct:sleep(2100),
    ?assertEqual({error, finished}, emqx_trace:update(Name, true)),
    ok.

t_load_state(_Config) ->
    Now = erlang:system_time(second),
    Running = #{
        name => <<"Running">>,
        type => topic,
        topic => <<"/x/y/1">>,
        start_at => Now - 1,
        end_at => Now + 2
    },
    Waiting = [
        {<<"name">>, <<"Waiting">>},
        {<<"type">>, topic},
        {<<"topic">>, <<"/x/y/2">>},
        {<<"start_at">>, Now + 3},
        {<<"end_at">>, Now + 8}
    ],
    Finished = [
        {<<"name">>, <<"Finished">>},
        {<<"type">>, topic},
        {<<"topic">>, <<"/x/y/3">>},
        {<<"start_at">>, Now - 5},
        {<<"end_at">>, Now}
    ],
    {ok, _} = emqx_trace:create(Running),
    {ok, _} = emqx_trace:create(Waiting),
    {error, "end_at time has already passed"} = emqx_trace:create(Finished),
    Traces = emqx_trace:format(emqx_trace:list()),
    ?assertEqual(2, erlang:length(Traces)),
    Enables = lists:map(fun(#{name := Name, enable := Enable}) -> {Name, Enable} end, Traces),
    ExpectEnables = [{<<"Running">>, true}, {<<"Waiting">>, true}],
    ?assertEqual(ExpectEnables, lists:sort(Enables)),
    ct:sleep(3500),
    Traces2 = emqx_trace:format(emqx_trace:list()),
    ?assertEqual(2, erlang:length(Traces2)),
    Enables2 = lists:map(fun(#{name := Name, enable := Enable}) -> {Name, Enable} end, Traces2),
    ExpectEnables2 = [{<<"Running">>, false}, {<<"Waiting">>, true}],
    ?assertEqual(ExpectEnables2, lists:sort(Enables2)),
    ok.

t_client_event(_Config) ->
    ClientId = <<"client-test">>,
    Now = erlang:system_time(second),
    Name = <<"test_client_id_event">>,
    {ok, _} = emqx_trace:create([
        {<<"name">>, Name},
        {<<"type">>, clientid},
        {<<"clientid">>, ClientId},
        {<<"start_at">>, Now}
    ]),
    ok = emqx_trace_handler_SUITE:filesync(Name, clientid),
    {ok, Client} = emqtt:start_link([{clean_start, true}, {clientid, ClientId}]),
    {ok, _} = emqtt:connect(Client),
    emqtt:ping(Client),
    ok = emqtt:publish(Client, <<"/test">>, #{}, <<"1">>, [{qos, 0}]),
    ok = emqtt:publish(Client, <<"/test">>, #{}, <<"2">>, [{qos, 0}]),
    ok = emqx_trace_handler_SUITE:filesync(Name, clientid),
    {ok, _} = emqx_trace:create([
        {<<"name">>, <<"test_topic">>},
        {<<"type">>, topic},
        {<<"topic">>, <<"/test">>},
        {<<"start_at">>, Now}
    ]),
    ok = emqx_trace_handler_SUITE:filesync(<<"test_topic">>, topic),
    {ok, Bin} = file:read_file(emqx_trace:log_file(Name, Now)),
    ok = emqtt:publish(Client, <<"/test">>, #{}, <<"3">>, [{qos, 0}]),
    ok = emqtt:publish(Client, <<"/test">>, #{}, <<"4">>, [{qos, 0}]),
    ok = emqtt:disconnect(Client),
    ok = emqx_trace_handler_SUITE:filesync(Name, clientid),
    ok = emqx_trace_handler_SUITE:filesync(<<"test_topic">>, topic),
    {ok, Bin2} = file:read_file(emqx_trace:log_file(Name, Now)),
    {ok, Bin3} = file:read_file(emqx_trace:log_file(<<"test_topic">>, Now)),
    ct:pal("Bin ~p Bin2 ~p Bin3 ~p", [byte_size(Bin), byte_size(Bin2), byte_size(Bin3)]),
    ?assert(erlang:byte_size(Bin) > 0),
    ?assert(erlang:byte_size(Bin) < erlang:byte_size(Bin2)),
    ?assert(erlang:byte_size(Bin3) > 0),
    ok.

t_client_huge_payload_truncated(_Config) ->
    ClientId = <<"client-truncated1">>,
    Now = erlang:system_time(second),
    Name = <<"test_client_id_truncated1">>,
    {ok, _} = emqx_trace:create([
        {<<"name">>, Name},
        {<<"type">>, clientid},
        {<<"clientid">>, ClientId},
        {<<"start_at">>, Now}
    ]),
    ok = emqx_trace_handler_SUITE:filesync(Name, clientid),
    {ok, Client} = emqtt:start_link([{clean_start, true}, {clientid, ClientId}]),
    {ok, _} = emqtt:connect(Client),
    emqtt:ping(Client),
    NormalPayload = iolist_to_binary(lists:duplicate(1024, "x")),
    Size1 = 1025,
    TruncatedBytes1 = Size1 - ?TRUNCATED_PAYLOAD_SIZE,
    HugePayload1 = iolist_to_binary(lists:duplicate(Size1, "y")),
    Size2 = 1024 * 10,
    HugePayload2 = iolist_to_binary(lists:duplicate(Size2, "z")),
    TruncatedBytes2 = Size2 - ?TRUNCATED_PAYLOAD_SIZE,
    ok = emqtt:publish(Client, <<"/test">>, #{}, NormalPayload, [{qos, 0}]),
    ok = emqx_trace_handler_SUITE:filesync(Name, clientid),
    {ok, _} = emqx_trace:create([
        {<<"name">>, <<"test_topic">>},
        {<<"type">>, topic},
        {<<"topic">>, <<"/test">>},
        {<<"start_at">>, Now}
    ]),
    ok = emqx_trace_handler_SUITE:filesync(<<"test_topic">>, topic),
    {ok, Bin} = file:read_file(emqx_trace:log_file(Name, Now)),
    ok = emqtt:publish(Client, <<"/test">>, #{}, NormalPayload, [{qos, 0}]),
    ok = emqtt:publish(Client, <<"/test">>, #{}, HugePayload1, [{qos, 0}]),
    ok = emqtt:publish(Client, <<"/test">>, #{}, HugePayload2, [{qos, 0}]),
    ok = emqtt:disconnect(Client),
    ok = emqx_trace_handler_SUITE:filesync(Name, clientid),
    ok = emqx_trace_handler_SUITE:filesync(<<"test_topic">>, topic),
    {ok, Bin2} = file:read_file(emqx_trace:log_file(Name, Now)),
    {ok, Bin3} = file:read_file(emqx_trace:log_file(<<"test_topic">>, Now)),
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

t_get_log_filename(_Config) ->
    Now = erlang:system_time(second),
    Name = <<"name1">>,
    Trace = [
        {<<"name">>, Name},
        {<<"type">>, ip_address},
        {<<"ip_address">>, <<"127.0.0.1">>},
        {<<"start_at">>, Now},
        {<<"end_at">>, Now + 2}
    ],
    {ok, _} = emqx_trace:create(Trace),
    ?assertEqual({error, not_found}, emqx_trace:get_trace_filename(<<"test">>)),
    ?assertEqual(ok, element(1, emqx_trace:get_trace_filename(Name))),
    ct:sleep(3000),
    ?assertEqual(ok, element(1, emqx_trace:get_trace_filename(Name))),
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

t_find_closed_time(_Config) ->
    DefaultMs = 60 * 15000,
    Now = erlang:system_time(second),
    Traces2 = [],
    ?assertEqual(DefaultMs, emqx_trace:find_closest_time(Traces2, Now)),
    Traces3 = [
        #emqx_trace{
            name = <<"disable">>,
            start_at = Now + 1,
            end_at = Now + 2,
            enable = false
        }
    ],
    ?assertEqual(DefaultMs, emqx_trace:find_closest_time(Traces3, Now)),
    Traces4 = [#emqx_trace{name = <<"running">>, start_at = Now, end_at = Now + 10, enable = true}],
    ?assertEqual(10000, emqx_trace:find_closest_time(Traces4, Now)),
    Traces5 = [
        #emqx_trace{
            name = <<"waiting">>,
            start_at = Now + 2,
            end_at = Now + 10,
            enable = true
        }
    ],
    ?assertEqual(2000, emqx_trace:find_closest_time(Traces5, Now)),
    Traces = [
        #emqx_trace{name = <<"waiting">>, start_at = Now + 1, end_at = Now + 2, enable = true},
        #emqx_trace{name = <<"running0">>, start_at = Now, end_at = Now + 5, enable = true},
        #emqx_trace{name = <<"running1">>, start_at = Now - 1, end_at = Now + 1, enable = true},
        #emqx_trace{name = <<"finished">>, start_at = Now - 2, end_at = Now - 1, enable = true},
        #emqx_trace{name = <<"waiting">>, start_at = Now + 1, end_at = Now + 1, enable = true},
        #emqx_trace{name = <<"stopped">>, start_at = Now, end_at = Now + 10, enable = false}
    ],
    ?assertEqual(1000, emqx_trace:find_closest_time(Traces, Now)),
    ok.

t_migrate_trace(_Config) ->
    build_new_trace_data(),
    build_old_trace_data(),
    reload(),
    Traces = emqx_trace:format(emqx_trace:list()),
    ?assertEqual(2, erlang:length(Traces)),
    lists:foreach(
        fun(#{name := Name, enable := Enable}) ->
            ?assertEqual(true, Enable, Name)
        end,
        Traces
    ),
    LoggerIds = logger:get_handler_ids(),
    lists:foreach(
        fun(Id) ->
            ?assertEqual(true, lists:member(Id, LoggerIds), LoggerIds)
        end,
        [
            trace_topic_test_topic_migrate_new,
            trace_topic_test_topic_migrate_old
        ]
    ),
    ok.

%% If no relevant event occurred, the log file size must be exactly 0 after stopping the trace.
t_empty_trace_log_file(_Config) ->
    ?check_trace(
        begin
            Now = erlang:system_time(second),
            Name = <<"empty_trace_log">>,
            Trace = [
                {<<"name">>, Name},
                {<<"type">>, clientid},
                {<<"clientid">>, <<"test_trace_no_clientid_1">>},
                {<<"start_at">>, Now},
                {<<"end_at">>, Now + 100}
            ],
            ?wait_async_action(
                ?assertMatch({ok, _}, emqx_trace:create(Trace)),
                #{?snk_kind := update_trace_done}
            ),
            ok = emqx_trace_handler_SUITE:filesync(Name, clientid),
            {ok, Filename} = emqx_trace:get_trace_filename(Name),
            ?assertMatch({ok, #{size := 0}}, emqx_trace:trace_file_detail(Filename)),
            ?wait_async_action(
                ?assertEqual(ok, emqx_trace:update(Name, false)),
                #{?snk_kind := update_trace_done}
            ),
            ?assertMatch({ok, #{size := 0}}, emqx_trace:trace_file_detail(Filename)),
            ?assertEqual(ok, emqx_trace:delete(Name))
        end,
        []
    ).

build_new_trace_data() ->
    Now = erlang:system_time(second),
    {ok, _} = emqx_trace:create([
        {<<"name">>, <<"test_topic_migrate_new">>},
        {<<"type">>, topic},
        {<<"topic">>, <<"/test/migrate/new">>},
        {<<"start_at">>, Now - 10}
    ]).

build_old_trace_data() ->
    Now = erlang:system_time(second),
    OldAttrs = [name, type, filter, enable, start_at, end_at],
    {atomic, ok} = mnesia:transform_table(emqx_trace, ignore, OldAttrs, emqx_trace),
    OldTrace =
        {emqx_trace, <<"test_topic_migrate_old">>, topic, <<"topic">>, true, Now - 10, Now + 100},
    ok = mnesia:dirty_write(OldTrace),
    ok.

reload() ->
    catch ok = gen_server:stop(emqx_trace),
    case emqx_trace:start_link() of
        {ok, _Pid} = Res ->
            Res;
        NotOKRes ->
            ct:pal(
                "emqx_trace:start_link() gave result: ~p\n"
                "(perhaps it is already started)",
                [NotOKRes]
            )
    end.
