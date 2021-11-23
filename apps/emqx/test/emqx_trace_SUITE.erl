%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-record(emqx_trace, {name, type, filter, enable = true, start_at, end_at}).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    application:load(emqx_plugin_libs),
    emqx_common_test_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([]).

t_base_create_delete(_Config) ->
    ok = emqx_trace:clear(),
    Now = erlang:system_time(second),
    Start = to_rfc3339(Now),
    End = to_rfc3339(Now + 30 * 60),
    Name = <<"name1">>,
    ClientId = <<"test-device">>,
    Trace = #{
        name => Name,
        type => <<"clientid">>,
        clientid => ClientId,
        start_at => Start,
        end_at => End
    },
    AnotherTrace = Trace#{name => <<"anotherTrace">>},
    ok = emqx_trace:create(Trace),
    ?assertEqual({error, {already_existed, Name}}, emqx_trace:create(Trace)),
    ?assertEqual({error, {duplicate_condition, Name}}, emqx_trace:create(AnotherTrace)),
    [TraceRec] = emqx_trace:list(),
    Expect = #emqx_trace{
        name     = Name,
        type     = clientid,
        filter   = ClientId,
        start_at = Now,
        end_at   = Now + 30 * 60
    },
    ?assertEqual(Expect, TraceRec),
    ExpectFormat = [
        #{
            filter => <<"test-device">>,
            enable => true,
            type => clientid,
            name => <<"name1">>,
            start_at => Now,
            end_at => Now + 30 * 60
        }
    ],
    ?assertEqual(ExpectFormat, emqx_trace:format([TraceRec])),
    ?assertEqual(ok, emqx_trace:delete(Name)),
    ?assertEqual({error, not_found}, emqx_trace:delete(Name)),
    ?assertEqual([], emqx_trace:list()),
    ok.

t_create_size_max(_Config) ->
    emqx_trace:clear(),
    lists:map(fun(Seq) ->
        Name = list_to_binary("name" ++ integer_to_list(Seq)),
        Trace = [{name, Name}, {type, <<"topic">>},
            {topic, list_to_binary("/x/y/" ++ integer_to_list(Seq))}],
        ok = emqx_trace:create(Trace)
              end, lists:seq(1, 30)),
    Trace31 = [{<<"name">>, <<"name31">>},
        {<<"type">>, <<"topic">>}, {<<"topic">>, <<"/x/y/31">>}],
    {error, _} = emqx_trace:create(Trace31),
    ok = emqx_trace:delete(<<"name30">>),
    ok = emqx_trace:create(Trace31),
    ?assertEqual(30, erlang:length(emqx_trace:list())),
    ok.

t_create_failed(_Config) ->
    ok = emqx_trace:clear(),
    UnknownField = [{<<"unknown">>, 12}],
    {error, Reason1} = emqx_trace:create(UnknownField),
    ?assertEqual(<<"unknown field: {unknown,12}">>, iolist_to_binary(Reason1)),

    InvalidTopic = [{<<"topic">>, "#/#//"}],
    {error, Reason2} = emqx_trace:create(InvalidTopic),
    ?assertEqual(<<"topic: #/#// invalid by function_clause">>, iolist_to_binary(Reason2)),

    InvalidStart = [{<<"start_at">>, <<"2021-12-3:12">>}],
    {error, Reason3} = emqx_trace:create(InvalidStart),
    ?assertEqual(<<"The rfc3339 specification not satisfied: 2021-12-3:12">>,
        iolist_to_binary(Reason3)),

    InvalidEnd = [{<<"end_at">>, <<"2021-12-3:12">>}],
    {error, Reason4} = emqx_trace:create(InvalidEnd),
    ?assertEqual(<<"The rfc3339 specification not satisfied: 2021-12-3:12">>,
        iolist_to_binary(Reason4)),

    {error, Reason7} = emqx_trace:create([{<<"name">>, <<"test">>}, {<<"type">>, <<"clientid">>}]),
    ?assertEqual(<<"topic/clientid/ip_address filter required">>, iolist_to_binary(Reason7)),

    InvalidPackets4 = [{<<"name">>, <<"/test">>}, {<<"clientid">>, <<"t">>},
        {<<"type">>, <<"clientid">>}],
    {error, Reason9} = emqx_trace:create(InvalidPackets4),
    ?assertEqual(<<"name cannot contain /">>, iolist_to_binary(Reason9)),

    ?assertEqual({error, "type=[topic,clientid,ip_address] required"},
        emqx_trace:create([{<<"name">>, <<"test-name">>}, {<<"clientid">>, <<"good">>}])),

    ?assertEqual({error, "incorrect type: only support clientid/topic/ip_address"},
        emqx_trace:create([{<<"name">>, <<"test-name">>},
            {<<"clientid">>, <<"good">>}, {<<"type">>, <<"typeerror">> }])),

    ?assertEqual({error, "ip address: einval"},
        emqx_trace:create([{<<"ip_address">>, <<"test-name">>}])),
    ok.

t_create_default(_Config) ->
    ok = emqx_trace:clear(),
    {error, "name required"} = emqx_trace:create([]),
    ok = emqx_trace:create([{<<"name">>, <<"test-name">>},
        {<<"type">>, <<"clientid">>}, {<<"clientid">>, <<"good">>}]),
    [#emqx_trace{name = <<"test-name">>}] = emqx_trace:list(),
    ok = emqx_trace:clear(),
    Trace = [
        {<<"name">>, <<"test-name">>},
        {<<"type">>, <<"topic">>},
        {<<"topic">>, <<"/x/y/z">>},
        {<<"start_at">>, <<"2021-10-28T10:54:47+08:00">>},
        {<<"end_at">>, <<"2021-10-27T10:54:47+08:00">>}
    ],
    {error, "end_at time has already passed"} = emqx_trace:create(Trace),
    Now = erlang:system_time(second),
    Trace2 = [
        {<<"name">>, <<"test-name">>},
        {<<"type">>, <<"topic">>},
        {<<"topic">>, <<"/x/y/z">>},
        {<<"start_at">>, to_rfc3339(Now + 10)},
        {<<"end_at">>, to_rfc3339(Now + 3)}
    ],
    {error, "failed by start_at >= end_at"} = emqx_trace:create(Trace2),
    ok = emqx_trace:create([{<<"name">>, <<"test-name">>},
        {<<"type">>, <<"topic">>}, {<<"topic">>, <<"/x/y/z">>}]),
    [#emqx_trace{start_at = Start, end_at = End}] = emqx_trace:list(),
    ?assertEqual(10 * 60, End - Start),
    ?assertEqual(true, Start - erlang:system_time(second) < 5),
    ok.

t_update_enable(_Config) ->
    ok = emqx_trace:clear(),
    Name = <<"test-name">>,
    Now = erlang:system_time(second),
    End = list_to_binary(calendar:system_time_to_rfc3339(Now + 2)),
    ok = emqx_trace:create([{<<"name">>, Name}, {<<"type">>, <<"topic">>},
        {<<"topic">>, <<"/x/y/z">>}, {<<"end_at">>, End}]),
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
    emqx_trace:clear(),
    load(),
    Now = erlang:system_time(second),
    Running = [{<<"name">>, <<"Running">>}, {<<"type">>, <<"topic">>},
        {<<"topic">>, <<"/x/y/1">>}, {<<"start_at">>, to_rfc3339(Now - 1)},
        {<<"end_at">>, to_rfc3339(Now + 2)}],
    Waiting = [{<<"name">>, <<"Waiting">>}, {<<"type">>, <<"topic">>},
        {<<"topic">>, <<"/x/y/2">>}, {<<"start_at">>, to_rfc3339(Now + 3)},
        {<<"end_at">>, to_rfc3339(Now + 8)}],
    Finished = [{<<"name">>, <<"Finished">>}, {<<"type">>, <<"topic">>},
        {<<"topic">>, <<"/x/y/3">>}, {<<"start_at">>, to_rfc3339(Now - 5)},
        {<<"end_at">>, to_rfc3339(Now)}],
    ok = emqx_trace:create(Running),
    ok = emqx_trace:create(Waiting),
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
    unload(),
    ok.

t_client_event(_Config) ->
    application:set_env(emqx, allow_anonymous, true),
    emqx_trace:clear(),
    ClientId = <<"client-test">>,
    load(),
    Now = erlang:system_time(second),
    Start = to_rfc3339(Now),
    Name = <<"test_client_id_event">>,
    ok = emqx_trace:create([{<<"name">>, Name},
        {<<"type">>, <<"clientid">>}, {<<"clientid">>, ClientId}, {<<"start_at">>, Start}]),
    ct:sleep(200),
    {ok, Client} = emqtt:start_link([{clean_start, true}, {clientid, ClientId}]),
    {ok, _} = emqtt:connect(Client),
    emqtt:ping(Client),
    ok = emqtt:publish(Client, <<"/test">>, #{}, <<"1">>, [{qos, 0}]),
    ok = emqtt:publish(Client, <<"/test">>, #{}, <<"2">>, [{qos, 0}]),
    ct:sleep(200),
    ok = emqx_trace:create([{<<"name">>, <<"test_topic">>},
        {<<"type">>, <<"topic">>}, {<<"topic">>, <<"/test">>}, {<<"start_at">>, Start}]),
    ct:sleep(200),
    {ok, Bin} = file:read_file(emqx_trace:log_file(Name, Now)),
    ok = emqtt:publish(Client, <<"/test">>, #{}, <<"3">>, [{qos, 0}]),
    ok = emqtt:publish(Client, <<"/test">>, #{}, <<"4">>, [{qos, 0}]),
    ok = emqtt:disconnect(Client),
    ct:sleep(200),
    {ok, Bin2} = file:read_file(emqx_trace:log_file(Name, Now)),
    {ok, Bin3} = file:read_file(emqx_trace:log_file(<<"test_topic">>, Now)),
    ct:pal("Bin ~p Bin2 ~p Bin3 ~p", [byte_size(Bin), byte_size(Bin2), byte_size(Bin3)]),
    ?assert(erlang:byte_size(Bin) > 0),
    ?assert(erlang:byte_size(Bin) < erlang:byte_size(Bin2)),
    ?assert(erlang:byte_size(Bin3) > 0),
    unload(),
    ok.

t_get_log_filename(_Config) ->
    ok = emqx_trace:clear(),
    load(),
    Now = erlang:system_time(second),
    Start = calendar:system_time_to_rfc3339(Now),
    End = calendar:system_time_to_rfc3339(Now + 2),
    Name = <<"name1">>,
    Trace = [
        {<<"name">>, Name},
        {<<"type">>, <<"ip_address">>},
        {<<"ip_address">>, <<"127.0.0.1">>},
        {<<"start_at">>, list_to_binary(Start)},
        {<<"end_at">>, list_to_binary(End)}
    ],
    ok = emqx_trace:create(Trace),
    ?assertEqual({error, not_found}, emqx_trace:get_trace_filename(<<"test">>)),
    ?assertEqual(ok, element(1, emqx_trace:get_trace_filename(Name))),
    ct:sleep(3000),
    ?assertEqual(ok, element(1, emqx_trace:get_trace_filename(Name))),
    unload(),
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

t_download_log(_Config) ->
    emqx_trace:clear(),
    load(),
    ClientId = <<"client-test">>,
    Now = erlang:system_time(second),
    Start = to_rfc3339(Now),
    Name = <<"test_client_id">>,
    ok = emqx_trace:create([{<<"name">>, Name},
        {<<"type">>, <<"clientid">>}, {<<"clientid">>, ClientId}, {<<"start_at">>, Start}]),
    {ok, Client} = emqtt:start_link([{clean_start, true}, {clientid, ClientId}]),
    {ok, _} = emqtt:connect(Client),
    [begin _ = emqtt:ping(Client) end ||_ <- lists:seq(1, 5)],
    ct:sleep(100),
    {ok, ZipFile} = emqx_trace_api:download_zip_log(#{name => Name}, []),
    ?assert(filelib:file_size(ZipFile) > 0),
    ok = emqtt:disconnect(Client),
    unload(),
    ok.

to_rfc3339(Second) ->
    list_to_binary(calendar:system_time_to_rfc3339(Second)).

load() ->
    emqx_trace:start_link().

unload() ->
    gen_server:stop(emqx_trace).
