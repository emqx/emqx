%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_mgmt_api_trace_SUITE).

%% API
-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("stdlib/include/zip.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(HOST, "http://127.0.0.1:18083/").
-define(API_VERSION, "v5").
-define(BASE_PATH, "api").

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_mgmt_api_test_util:init_suite(),
    Config.

end_per_suite(_) ->
    emqx_mgmt_api_test_util:end_suite().

t_http_test(_Config) ->
    emqx_trace:clear(),
    load(),
    Header = auth_header_(),
    %% list
    {ok, Empty} = request_api(get, api_path("trace"), Header),
    ?assertEqual([], json(Empty)),
    %% create
    ErrorTrace = #{},
    {error, {"HTTP/1.1", 400, "Bad Request"}, Body} =
        request_api(post, api_path("trace"), Header, ErrorTrace),
    ?assertMatch(#{<<"code">> := <<"BAD_REQUEST">>}, json(Body)),

    Name = <<"test-name">>,
    Trace = [
        {<<"name">>, Name},
        {<<"type">>, <<"topic">>},
        {<<"topic">>, <<"/x/y/z">>}
    ],

    {ok, Create} = request_api(post, api_path("trace"), Header, Trace),
    ?assertMatch(#{<<"name">> := Name}, json(Create)),

    {ok, List} = request_api(get, api_path("trace"), Header),
    [Data] = json(List),
    ?assertEqual(Name, maps:get(<<"name">>, Data)),

    %% update
    {ok, Update} = request_api(put, api_path("trace/test-name/stop"), Header, #{}),
    ?assertEqual(
        #{
            <<"enable">> => false,
            <<"name">> => <<"test-name">>
        },
        json(Update)
    ),

    ?assertMatch(
        {error, {"HTTP/1.1", 404, _}, _},
        request_api(put, api_path("trace/test-name-not-found/stop"), Header, #{})
    ),
    {ok, List1} = request_api(get, api_path("trace"), Header),
    [Data1] = json(List1),
    Node = atom_to_binary(node()),
    ?assertMatch(
        #{
            <<"status">> := <<"stopped">>,
            <<"name">> := <<"test-name">>,
            <<"log_size">> := #{Node := _},
            <<"start_at">> := _,
            <<"end_at">> := _,
            <<"type">> := <<"topic">>,
            <<"topic">> := <<"/x/y/z">>
        },
        Data1
    ),

    %% delete
    {ok, Delete} = request_api(delete, api_path("trace/test-name"), Header),
    ?assertEqual(<<>>, Delete),

    {error, {"HTTP/1.1", 404, "Not Found"}, DeleteNotFound} =
        request_api(delete, api_path("trace/test-name"), Header),
    ?assertEqual(
        #{
            <<"code">> => <<"NOT_FOUND">>,
            <<"message">> => <<"test-name NOT FOUND">>
        },
        json(DeleteNotFound)
    ),

    {ok, List2} = request_api(get, api_path("trace"), Header),
    ?assertEqual([], json(List2)),

    %% clear
    {ok, Create1} = request_api(post, api_path("trace"), Header, Trace),
    ?assertMatch(#{<<"name">> := Name}, json(Create1)),

    {ok, Clear} = request_api(delete, api_path("trace"), Header),
    ?assertEqual(<<>>, Clear),

    unload(),
    ok.

t_create_failed(_Config) ->
    load(),
    Header = auth_header_(),
    Trace = [{<<"type">>, <<"topic">>}, {<<"topic">>, <<"/x/y/z">>}],

    BadName1 = {<<"name">>, <<"test/bad">>},
    ?assertMatch(
        {error, {"HTTP/1.1", 400, _}, _},
        request_api(post, api_path("trace"), Header, [BadName1 | Trace])
    ),
    BadName2 = {<<"name">>, list_to_binary(lists:duplicate(257, "t"))},
    ?assertMatch(
        {error, {"HTTP/1.1", 400, _}, _},
        request_api(post, api_path("trace"), Header, [BadName2 | Trace])
    ),

    %% already_exist
    GoodName = {<<"name">>, <<"test-name-0">>},
    {ok, Create} = request_api(post, api_path("trace"), Header, [GoodName | Trace]),
    ?assertMatch(#{<<"name">> := <<"test-name-0">>}, json(Create)),
    ?assertMatch(
        {error, {"HTTP/1.1", 400, _}, _},
        request_api(post, api_path("trace"), Header, [GoodName | Trace])
    ),

    %% MAX Limited
    lists:map(
        fun(Seq) ->
            Name0 = list_to_binary("name" ++ integer_to_list(Seq)),
            Trace0 = [
                {name, Name0},
                {type, topic},
                {topic, list_to_binary("/x/y/" ++ integer_to_list(Seq))}
            ],
            {ok, _} = emqx_trace:create(Trace0)
        end,
        lists:seq(1, 30 - ets:info(emqx_trace, size))
    ),
    GoodName1 = {<<"name">>, <<"test-name-1">>},
    ?assertMatch(
        {error, {"HTTP/1.1", 400, _}, _},
        request_api(post, api_path("trace"), Header, [GoodName1 | Trace])
    ),
    unload(),
    emqx_trace:clear(),
    ok.

t_download_log(_Config) ->
    ClientId = <<"client-test-download">>,
    Now = erlang:system_time(second),
    Name = <<"test_client_id">>,
    load(),
    create_trace(Name, ClientId, Now),
    {ok, Client} = emqtt:start_link([{clean_start, true}, {clientid, ClientId}]),
    {ok, _} = emqtt:connect(Client),
    [
        begin
            _ = emqtt:ping(Client)
        end
     || _ <- lists:seq(1, 5)
    ],
    ok = emqx_trace_handler_SUITE:filesync(Name, clientid),
    Header = auth_header_(),
    {ok, Binary} = request_api(get, api_path("trace/test_client_id/download"), Header),
    {ok, [
        _Comment,
        #zip_file{
            name = ZipName,
            info = #file_info{size = Size, type = regular, access = read_write}
        }
    ]} =
        ZipTab =
        zip:table(Binary),
    ?assert(Size > 0),
    ZipNamePrefix = lists:flatten(io_lib:format("~s-trace_~s", [node(), Name])),
    ?assertNotEqual(nomatch, re:run(ZipName, [ZipNamePrefix])),
    Path = api_path("trace/test_client_id/download?node=" ++ atom_to_list(node())),
    {ok, Binary2} = request_api(get, Path, Header),
    ?assertEqual(ZipTab, zip:table(Binary2)),
    ok = emqtt:disconnect(Client),
    ok.

create_trace(Name, ClientId, Start) ->
    ?check_trace(
        #{timetrap => 900},
        begin
            {ok, _} = emqx_trace:create([
                {<<"name">>, Name},
                {<<"type">>, clientid},
                {<<"clientid">>, ClientId},
                {<<"start_at">>, Start}
            ]),
            ?block_until(#{?snk_kind := update_trace_done})
        end,
        fun(Trace) ->
            ?assertMatch([#{}], ?of_kind(update_trace_done, Trace))
        end
    ).

t_stream_log(_Config) ->
    application:set_env(emqx, allow_anonymous, true),
    emqx_trace:clear(),
    load(),
    ClientId = <<"client-stream">>,
    Now = erlang:system_time(second),
    Name = <<"test_stream_log">>,
    create_trace(Name, ClientId, Now - 10),
    {ok, Client} = emqtt:start_link([{clean_start, true}, {clientid, ClientId}]),
    {ok, _} = emqtt:connect(Client),
    [
        begin
            _ = emqtt:ping(Client)
        end
     || _ <- lists:seq(1, 5)
    ],
    emqtt:publish(Client, <<"/good">>, #{}, <<"ghood1">>, [{qos, 0}]),
    emqtt:publish(Client, <<"/good">>, #{}, <<"ghood2">>, [{qos, 0}]),
    ok = emqtt:disconnect(Client),
    ct:sleep(200),
    File = emqx_trace:log_file(Name, Now),
    ct:pal("FileName: ~p", [File]),
    {ok, FileBin} = file:read_file(File),
    ct:pal("FileBin: ~p ~s", [byte_size(FileBin), FileBin]),
    Header = auth_header_(),
    {ok, Binary} = request_api(get, api_path("trace/test_stream_log/log?bytes=10"), Header),
    #{<<"meta">> := Meta, <<"items">> := Bin} = json(Binary),
    ?assertEqual(10, byte_size(Bin)),
    ?assertEqual(#{<<"position">> => 10, <<"bytes">> => 10}, Meta),
    Path = api_path("trace/test_stream_log/log?position=20&bytes=10"),
    {ok, Binary1} = request_api(get, Path, Header),
    #{<<"meta">> := Meta1, <<"items">> := Bin1} = json(Binary1),
    ?assertEqual(#{<<"position">> => 30, <<"bytes">> => 10}, Meta1),
    ?assertEqual(10, byte_size(Bin1)),
    unload(),
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
        {error, {shutdown, server_closed}} ->
            {error, server_closed};
        {ok, {{"HTTP/1.1", Code, _}, _Headers, Return}} when
            Code =:= 200 orelse Code =:= 201 orelse Code =:= 204
        ->
            {ok, Return};
        {ok, {Reason, _Header, Body}} ->
            {error, Reason, Body}
    end.

api_path(Path) ->
    ?HOST ++ filename:join([?BASE_PATH, ?API_VERSION, Path]).

json(Data) ->
    {ok, Jsx} = emqx_json:safe_decode(Data, [return_maps]),
    Jsx.

load() ->
    emqx_trace:start_link().

unload() ->
    gen_server:stop(emqx_trace).
