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
-module(emqx_mod_trace_SUITE).

%% API
-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").

-define(HOST, "http://127.0.0.1:18083/").
-define(API_VERSION, "v4").
-define(BASE_PATH, "api").

-record(emqx_mod_trace, {
                         name,
                         type,
                         topic,
                         clientid,
                         packets = [],
                         enable = true,
                         start_at,
                         end_at,
                         log_size = #{}
                        }).

-define(PACKETS, ['CONNECT', 'CONNACK', 'PUBLISH', 'PUBACK', 'PUBREC', 'PUBREL'
    , 'PUBCOMP', 'SUBSCRIBE', 'SUBACK', 'UNSUBSCRIBE', 'UNSUBACK'
    , 'PINGREQ', 'PINGRESP', 'DISCONNECT', 'AUTH']).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([emqx_modules, emqx_dashboard]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([emqx_modules, emqx_dashboard]).

t_base_create_delete(_Config) ->
    ok = emqx_mod_trace:clear(),
    Now = erlang:system_time(second),
    Start = to_rfc3339(Now),
    End = to_rfc3339(Now + 30 * 60),
    Name = <<"name1">>,
    ClientId = <<"test-device">>,
    Packets = [atom_to_binary(E) || E <- ?PACKETS],
    Trace = [
        {<<"name">>, Name},
        {<<"type">>, <<"clientid">>},
        {<<"clientid">>, ClientId},
        {<<"packets">>, Packets},
        {<<"start_at">>, Start},
        {<<"end_at">>, End}
    ],
    AnotherTrace = lists:keyreplace(<<"name">>, 1, Trace, {<<"name">>, <<"AnotherName">>}),
    ok = emqx_mod_trace:create(Trace),
    ?assertEqual({error, {already_existed, Name}}, emqx_mod_trace:create(Trace)),
    ?assertEqual({error, {duplicate_condition, Name}}, emqx_mod_trace:create(AnotherTrace)),
    [TraceRec] = emqx_mod_trace:list(),
    Expect = #emqx_mod_trace{
        name     = Name,
        type     = clientid,
        topic    = undefined,
        clientid = ClientId,
        packets  = ?PACKETS,
        start_at = Now,
        end_at   = Now + 30 * 60
    },
    ?assertEqual(Expect, TraceRec),
    ExpectFormat = [
        #{
            clientid => <<"test-device">>,
            enable => true,
            type => clientid,
            packets => ?PACKETS,
            name => <<"name1">>,
            start_at => Start,
            end_at => End,
            log_size => #{},
            topic => undefined
        }
    ],
    ?assertEqual(ExpectFormat, emqx_mod_trace:format([TraceRec])),
    ?assertEqual(ok, emqx_mod_trace:delete(Name)),
    ?assertEqual({error, not_found}, emqx_mod_trace:delete(Name)),
    ?assertEqual([], emqx_mod_trace:list()),
    ok.

t_create_size_max(_Config) ->
    emqx_mod_trace:clear(),
    lists:map(fun(Seq) ->
        Name = list_to_binary("name" ++ integer_to_list(Seq)),
        Trace = [{<<"name">>, Name}, {<<"type">>, <<"topic">>},
            {<<"packets">>, [<<"PUBLISH">>]},
            {<<"topic">>, list_to_binary("/x/y/" ++ integer_to_list(Seq))}],
        ok = emqx_mod_trace:create(Trace)
              end, lists:seq(1, 30)),
    Trace31 = [{<<"name">>, <<"name31">>}, {<<"type">>, <<"topic">>},
        {<<"packets">>, [<<"PUBLISH">>]}, {<<"topic">>, <<"/x/y/31">>}],
    {error, _} = emqx_mod_trace:create(Trace31),
    ok = emqx_mod_trace:delete(<<"name30">>),
    ok = emqx_mod_trace:create(Trace31),
    ?assertEqual(30, erlang:length(emqx_mod_trace:list())),
    ok.

t_create_failed(_Config) ->
    ok = emqx_mod_trace:clear(),
    UnknownField = [{<<"unknown">>, 12}],
    {error, Reason1} = emqx_mod_trace:create(UnknownField),
    ?assertEqual(<<"unknown field: {<<\"unknown\">>,12}">>, iolist_to_binary(Reason1)),

    InvalidTopic = [{<<"topic">>, "#/#//"}],
    {error, Reason2} = emqx_mod_trace:create(InvalidTopic),
    ?assertEqual(<<"#/#// invalid by function_clause">>, iolist_to_binary(Reason2)),

    InvalidStart = [{<<"start_at">>, <<"2021-12-3:12">>}],
    {error, Reason3} = emqx_mod_trace:create(InvalidStart),
    ?assertEqual(<<"The rfc3339 specification not satisfied: 2021-12-3:12">>,
        iolist_to_binary(Reason3)),

    InvalidEnd = [{<<"end_at">>, <<"2021-12-3:12">>}],
    {error, Reason4} = emqx_mod_trace:create(InvalidEnd),
    ?assertEqual(<<"The rfc3339 specification not satisfied: 2021-12-3:12">>,
        iolist_to_binary(Reason4)),

    InvalidPackets = [{<<"packets">>, [<<"publish">>]}],
    {error, Reason5} = emqx_mod_trace:create(InvalidPackets),
    ?assertEqual(<<"unsupport packets: [publish]">>, iolist_to_binary(Reason5)),

    InvalidPackets2 = [{<<"packets">>, <<"publish">>}],
    {error, Reason6} = emqx_mod_trace:create(InvalidPackets2),
    ?assertEqual(<<"unsupport packets: <<\"publish\">>">>, iolist_to_binary(Reason6)),

    {error, Reason7} = emqx_mod_trace:create([{<<"name">>, <<"test">>}, {<<"type">>, <<"clientid">>}]),
    ?assertEqual(<<"topic/clientid cannot be both empty">>, iolist_to_binary(Reason7)),

    InvalidPackets4 = [{<<"name">>, <<"/test">>}, {<<"clientid">>, <<"t">>}, {<<"type">>, <<"clientid">>}],
    {error, Reason9} = emqx_mod_trace:create(InvalidPackets4),
    ?assertEqual(<<"name cannot contain /">>, iolist_to_binary(Reason9)),

    ?assertEqual({error, "type required"}, emqx_mod_trace:create([{<<"name">>, <<"test-name">>},
        {<<"packets">>, []}, {<<"clientid">>, <<"good">>}])),

    ?assertEqual({error, "incorrect type: only support clientid/topic"},
        emqx_mod_trace:create([{<<"name">>, <<"test-name">>},
            {<<"packets">>, []}, {<<"clientid">>, <<"good">>}, {<<"type">>, <<"typeerror">> }])),
    ok.

t_create_default(_Config) ->
    ok = emqx_mod_trace:clear(),
    {error, "name required"} = emqx_mod_trace:create([]),
    ok = emqx_mod_trace:create([{<<"name">>, <<"test-name">>},
        {<<"type">>, <<"clientid">>}, {<<"packets">>, []}, {<<"clientid">>, <<"good">>}]),
    [#emqx_mod_trace{packets = Packets}] = emqx_mod_trace:list(),
    ?assertEqual([], Packets),
    ok = emqx_mod_trace:clear(),
    Trace = [
        {<<"name">>, <<"test-name">>},
        {<<"packets">>, [<<"PUBLISH">>]},
        {<<"type">>, <<"topic">>},
        {<<"topic">>, <<"/x/y/z">>},
        {<<"start_at">>, <<"2021-10-28T10:54:47+08:00">>},
        {<<"end_at">>, <<"2021-10-27T10:54:47+08:00">>}
    ],
    {error, "end_at time has already passed"} = emqx_mod_trace:create(Trace),
    Now = erlang:system_time(second),
    Trace2 = [
        {<<"name">>, <<"test-name">>},
        {<<"type">>, <<"topic">>},
        {<<"packets">>, [<<"PUBLISH">>]},
        {<<"topic">>, <<"/x/y/z">>},
        {<<"start_at">>, to_rfc3339(Now + 10)},
        {<<"end_at">>, to_rfc3339(Now + 3)}
    ],
    {error, "failed by start_at >= end_at"} = emqx_mod_trace:create(Trace2),
    ok = emqx_mod_trace:create([{<<"name">>, <<"test-name">>},
        {<<"type">>, <<"topic">>},
        {<<"packets">>, [<<"PUBLISH">>]}, {<<"topic">>, <<"/x/y/z">>}]),
    [#emqx_mod_trace{start_at = Start, end_at = End}] = emqx_mod_trace:list(),
    ?assertEqual(10 * 60, End - Start),
    ?assertEqual(true, Start - erlang:system_time(second) < 5),
    ok.

t_update_enable(_Config) ->
    ok = emqx_mod_trace:clear(),
    Name = <<"test-name">>,
    Now = erlang:system_time(second),
    End = list_to_binary(calendar:system_time_to_rfc3339(Now + 2)),
    ok = emqx_mod_trace:create([{<<"name">>, Name}, {<<"packets">>, [<<"PUBLISH">>]},
        {<<"type">>, <<"topic">>}, {<<"topic">>, <<"/x/y/z">>}, {<<"end_at">>, End}]),
    [#emqx_mod_trace{enable = Enable}] = emqx_mod_trace:list(),
    ?assertEqual(Enable, true),
    ok = emqx_mod_trace:update(Name, false),
    [#emqx_mod_trace{enable = false}] = emqx_mod_trace:list(),
    ok = emqx_mod_trace:update(Name, false),
    [#emqx_mod_trace{enable = false}] = emqx_mod_trace:list(),
    ok = emqx_mod_trace:update(Name, true),
    [#emqx_mod_trace{enable = true}] = emqx_mod_trace:list(),
    ok = emqx_mod_trace:update(Name, false),
    [#emqx_mod_trace{enable = false}] = emqx_mod_trace:list(),
    ?assertEqual({error, not_found}, emqx_mod_trace:update(<<"Name not found">>, true)),
    ct:sleep(2100),
    ?assertEqual({error, finished}, emqx_mod_trace:update(Name, true)),
    ok.

t_load_state(_Config) ->
    emqx_mod_trace:clear(),
    ok = emqx_mod_trace:load(test),
    Now = erlang:system_time(second),
    Running = [{<<"name">>, <<"Running">>}, {<<"type">>, <<"topic">>},
        {<<"topic">>, <<"/x/y/1">>}, {<<"start_at">>, to_rfc3339(Now - 1)}, {<<"end_at">>, to_rfc3339(Now + 2)}],
    Waiting = [{<<"name">>, <<"Waiting">>}, {<<"type">>, <<"topic">>},
        {<<"topic">>, <<"/x/y/2">>}, {<<"start_at">>, to_rfc3339(Now + 3)}, {<<"end_at">>, to_rfc3339(Now + 8)}],
    Finished = [{<<"name">>, <<"Finished">>}, {<<"type">>, <<"topic">>},
        {<<"topic">>, <<"/x/y/3">>}, {<<"start_at">>, to_rfc3339(Now - 5)}, {<<"end_at">>, to_rfc3339(Now)}],
    ok = emqx_mod_trace:create(Running),
    ok = emqx_mod_trace:create(Waiting),
    {error, "end_at time has already passed"} = emqx_mod_trace:create(Finished),
    Traces = emqx_mod_trace:format(emqx_mod_trace:list()),
    ?assertEqual(2, erlang:length(Traces)),
    Enables = lists:map(fun(#{name := Name, enable := Enable}) -> {Name, Enable} end, Traces),
    ExpectEnables = [{<<"Running">>, true}, {<<"Waiting">>, true}],
    ?assertEqual(ExpectEnables, lists:sort(Enables)),
    ct:sleep(3500),
    Traces2 = emqx_mod_trace:format(emqx_mod_trace:list()),
    ?assertEqual(2, erlang:length(Traces2)),
    Enables2 = lists:map(fun(#{name := Name, enable := Enable}) -> {Name, Enable} end, Traces2),
    ExpectEnables2 = [{<<"Running">>, false}, {<<"Waiting">>, true}],
    ?assertEqual(ExpectEnables2, lists:sort(Enables2)),
    ok = emqx_mod_trace:unload(test),
    ok.

t_client_event(_Config) ->
    application:set_env(emqx, allow_anonymous, true),
    emqx_mod_trace:clear(),
    ClientId = <<"client-test">>,
    ok = emqx_mod_trace:load(test),
    Now = erlang:system_time(second),
    Start = to_rfc3339(Now),
    Name = <<"test_client_id_event">>,
    ok = emqx_mod_trace:create([{<<"name">>, Name},
        {<<"type">>, <<"clientid">>}, {<<"clientid">>, ClientId}, {<<"start_at">>, Start}]),
    ct:sleep(200),
    {ok, Client} = emqtt:start_link([{clean_start, true}, {clientid, ClientId}]),
    {ok, _} = emqtt:connect(Client),
    emqtt:ping(Client),
    ok = emqtt:publish(Client, <<"/test">>, #{}, <<"1">>, [{qos, 0}]),
    ok = emqtt:publish(Client, <<"/test">>, #{}, <<"2">>, [{qos, 0}]),
    ct:sleep(200),
    ok = emqx_mod_trace:create([{<<"name">>, <<"test_topic">>},
        {<<"type">>, <<"topic">>}, {<<"topic">>, <<"/test">>}, {<<"start_at">>, Start}]),
    ct:sleep(200),
    {ok, Bin} = file:read_file(emqx_mod_trace:log_file(Name, Now)),
    ok = emqtt:publish(Client, <<"/test">>, #{}, <<"3">>, [{qos, 0}]),
    ok = emqtt:publish(Client, <<"/test">>, #{}, <<"4">>, [{qos, 0}]),
    ok = emqtt:disconnect(Client),
    ct:sleep(200),
    {ok, Bin2} = file:read_file(emqx_mod_trace:log_file(Name, Now)),
    {ok, Bin3} = file:read_file(emqx_mod_trace:log_file(<<"test_topic">>, Now)),
    ct:pal("Bin ~p Bin2 ~p Bin3 ~p", [byte_size(Bin), byte_size(Bin2), byte_size(Bin3)]),
    ?assert(erlang:byte_size(Bin) > 0),
    ?assert(erlang:byte_size(Bin) < erlang:byte_size(Bin2)),
    ?assert(erlang:byte_size(Bin3) > 0),
    ok = emqx_mod_trace:unload(test),
    ok.

t_get_log_filename(_Config) ->
    ok = emqx_mod_trace:clear(),
    ok = emqx_mod_trace:load(test),
    Now = erlang:system_time(second),
    Start = calendar:system_time_to_rfc3339(Now),
    End = calendar:system_time_to_rfc3339(Now + 2),
    Name = <<"name1">>,
    ClientId = <<"test-device">>,
    Packets = [atom_to_binary(E) || E <- ?PACKETS],
    Trace = [
        {<<"name">>, Name},
        {<<"type">>, <<"clientid">>},
        {<<"clientid">>, ClientId},
        {<<"packets">>, Packets},
        {<<"start_at">>, list_to_binary(Start)},
        {<<"end_at">>, list_to_binary(End)}
    ],
    ok = emqx_mod_trace:create(Trace),
    ?assertEqual({error, not_found}, emqx_mod_trace:get_trace_filename(<<"test">>)),
    ?assertEqual(ok, element(1, emqx_mod_trace:get_trace_filename(Name))),
    ct:sleep(3000),
    ?assertEqual(ok, element(1, emqx_mod_trace:get_trace_filename(Name))),
    ok = emqx_mod_trace:unload(test),
    ok.

t_trace_file(_Config) ->
    FileName = "test.log",
    Content = <<"test \n test">>,
    TraceDir = emqx_mod_trace:trace_dir(),
    File = filename:join(TraceDir, FileName),
    ok = file:write_file(File, Content),
    {ok, Node, Bin} = emqx_mod_trace:trace_file(FileName),
    ?assertEqual(Node, atom_to_list(node())),
    ?assertEqual(Content, Bin),
    ok = file:delete(File),
    ok.

t_http_test(_Config) ->
    emqx_mod_trace:clear(),
    emqx_mod_trace:load(test),
    Header = auth_header_(),
    %% list
    {ok, Empty} = request_api(get, api_path("trace"), Header),
    ?assertEqual(#{<<"code">> => 0, <<"data">> => []}, json(Empty)),
    %% create
    ErrorTrace = #{},
    {ok, Error} = request_api(post, api_path("trace"), Header, ErrorTrace),
    ?assertEqual(#{<<"message">> => <<"unknown field: {}">>, <<"code">> => <<"INCORRECT_PARAMS">>}, json(Error)),

    Name = <<"test-name">>,
    Trace = [
        {<<"name">>, Name},
        {<<"type">>, <<"topic">>},
        {<<"packets">>, [<<"PUBLISH">>]},
        {<<"topic">>, <<"/x/y/z">>}
    ],

    {ok, Create} = request_api(post, api_path("trace"), Header, Trace),
    ?assertEqual(#{<<"code">> => 0}, json(Create)),

    {ok, List} = request_api(get, api_path("trace"), Header),
    #{<<"code">> := 0, <<"data">> := [Data]} = json(List),
    ?assertEqual(Name, maps:get(<<"name">>, Data)),

    %% update
    {ok, Update} = request_api(put, api_path("trace/test-name/disable"), Header, #{}),
    ?assertEqual(#{<<"code">> => 0,
        <<"data">> => #{<<"enable">> => false,
            <<"name">> => <<"test-name">>}}, json(Update)),

    {ok, List1} = request_api(get, api_path("trace"), Header),
    #{<<"code">> := 0, <<"data">> := [Data1]} = json(List1),
    ?assertEqual(false, maps:get(<<"enable">>, Data1)),

    %% delete
    {ok, Delete} = request_api(delete, api_path("trace/test-name"), Header),
    ?assertEqual(#{<<"code">> => 0}, json(Delete)),

    {ok, DeleteNotFound} = request_api(delete, api_path("trace/test-name"), Header),
    ?assertEqual(#{<<"code">> => <<"NOT_FOUND">>,
        <<"message">> => <<"test-nameNOT FOUND">>}, json(DeleteNotFound)),

    {ok, List2} = request_api(get, api_path("trace"), Header),
    ?assertEqual(#{<<"code">> => 0, <<"data">> => []}, json(List2)),

    %% clear
    {ok, Create1} = request_api(post, api_path("trace"), Header, Trace),
    ?assertEqual(#{<<"code">> => 0}, json(Create1)),

    {ok, Clear} = request_api(delete, api_path("trace"), Header),
    ?assertEqual(#{<<"code">> => 0}, json(Clear)),

    emqx_mod_trace:unload(test),
    ok.

t_download_log(_Config) ->
    emqx_mod_trace:clear(),
    emqx_mod_trace:load(test),
    ClientId = <<"client-test">>,
    Now = erlang:system_time(second),
    Start = to_rfc3339(Now),
    Name = <<"test_client_id">>,
    ok = emqx_mod_trace:create([{<<"name">>, Name},
        {<<"type">>, <<"clientid">>}, {<<"clientid">>, ClientId}, {<<"start_at">>, Start}]),
    {ok, Client} = emqtt:start_link([{clean_start, true}, {clientid, ClientId}]),
    {ok, _} = emqtt:connect(Client),
    [begin _ = emqtt:ping(Client) end ||_ <- lists:seq(1, 5)],
    ct:sleep(100),
    {ok, #{}, {sendfile, 0, ZipFileSize, _ZipFile}} =
        emqx_mod_trace_api:download_zip_log(#{name => Name}, []),
    ?assert(ZipFileSize > 0),
    %% download zip file failed by server_closed occasionally?
    %Header = auth_header_(),
    %{ok, ZipBin} = request_api(get, api_path("trace/test_client_id/download"), Header),
    %{ok, ZipHandler} = zip:zip_open(ZipBin),
    %{ok, [ZipName]} = zip:zip_get(ZipHandler),
    %?assertNotEqual(nomatch, string:find(ZipName, "test@127.0.0.1")),
    %{ok, _} = file:read_file(emqx_mod_trace:log_file(<<"test_client_id">>, Now)),
    ok = emqtt:disconnect(Client),
    emqx_mod_trace:unload(test),
    ok.

t_stream_log(_Config) ->
    application:set_env(emqx, allow_anonymous, true),
    emqx_mod_trace:clear(),
    emqx_mod_trace:load(test),
    ClientId = <<"client-stream">>,
    Now = erlang:system_time(second),
    Name = <<"test_stream_log">>,
    Start = to_rfc3339(Now - 10),
    ok = emqx_mod_trace:create([{<<"name">>, Name},
        {<<"type">>, <<"clientid">>}, {<<"clientid">>, ClientId}, {<<"start_at">>, Start}]),
    ct:sleep(200),
    {ok, Client} = emqtt:start_link([{clean_start, true}, {clientid, ClientId}]),
    {ok, _} = emqtt:connect(Client),
    [begin _ = emqtt:ping(Client) end ||_ <- lists:seq(1, 5)],
    emqtt:publish(Client, <<"/good">>, #{}, <<"ghood1">>, [{qos, 0}]),
    emqtt:publish(Client, <<"/good">>, #{}, <<"ghood2">>, [{qos, 0}]),
    ok = emqtt:disconnect(Client),
    ct:sleep(200),
    File = emqx_mod_trace:log_file(Name, Now),
    ct:pal("FileName: ~p", [File]),
    {ok, FileBin} = file:read_file(File),
    ct:pal("FileBin: ~p ~s", [byte_size(FileBin), FileBin]),
    Header = auth_header_(),
    {ok, Binary} = request_api(get, api_path("trace/test_stream_log/log?_limit=10"), Header),
    #{<<"code">> := 0, <<"data">> := #{<<"meta">> := Meta, <<"items">> := Bin}} = json(Binary),
    ?assertEqual(10, byte_size(Bin)),
    ?assertEqual(#{<<"page">> => 10, <<"limit">> => 10}, Meta),
    {ok, Binary1} = request_api(get, api_path("trace/test_stream_log/log?_page=20&_limit=10"), Header),
    #{<<"code">> := 0, <<"data">> := #{<<"meta">> := Meta1, <<"items">> := Bin1}} = json(Binary1),
    ?assertEqual(#{<<"page">> => 30, <<"limit">> => 10}, Meta1),
    ?assertEqual(10, byte_size(Bin1)),
    emqx_mod_trace:unload(test),
    ok.

to_rfc3339(Second) ->
    list_to_binary(calendar:system_time_to_rfc3339(Second)).

auth_header_() ->
    auth_header_("admin", "public").

auth_header_(User, Pass) ->
    Encoded = base64:encode_to_string(lists:append([User, ":", Pass])),
    {"Authorization", "Basic " ++ Encoded}.

request_api(Method, Url, Auth) -> do_request_api(Method, {Url, [Auth]}).

request_api(Method, Url, Auth, Body) ->
    Request = {Url, [Auth], "application/json", emqx_json:encode(Body)},
    do_request_api(Method, Request).

do_request_api(Method, Request) ->
    ct:pal("Method: ~p, Request: ~p", [Method, Request]),
    case httpc:request(Method, Request, [], [{body_format, binary}]) of
        {error, socket_closed_remotely} ->
            {error, socket_closed_remotely};
        {error,{shutdown, server_closed}} ->
            {error, server_closed};
        {ok, {{"HTTP/1.1", Code, _}, _Headers, Return} }
            when Code =:= 200 orelse Code =:= 201 ->
            {ok, Return};
        {ok, {Reason, _, _}} ->
            {error, Reason}
    end.

api_path(Path) ->
    ?HOST ++ filename:join([?BASE_PATH, ?API_VERSION, Path]).

json(Data) ->
    {ok, Jsx} = emqx_json:safe_decode(Data, [return_maps]), Jsx.
