%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_mod_trace_api_SUITE).

%% API
-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").

-define(HOST, "http://127.0.0.1:18083/").
-define(API_VERSION, "v4").
-define(BASE_PATH, "api").

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    application:load(emqx_plugin_libs),
    emqx_ct_helpers:start_apps([emqx_modules, emqx_dashboard]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([emqx_modules, emqx_dashboard]).

t_http_test(_Config) ->
    emqx_trace:clear(),
    load(),
    Header = auth_header_(),
    %% list
    {ok, Empty} = request_api(get, api_path("trace"), Header),
    ?assertEqual(#{<<"code">> => 0, <<"data">> => []}, json(Empty)),
    %% create
    ErrorTrace = #{},
    {ok, Error} = request_api(post, api_path("trace"), Header, ErrorTrace),
    ?assertEqual(#{<<"message">> => <<"name required">>,
        <<"code">> => <<"INCORRECT_PARAMS">>}, json(Error)),

    Name = <<"test-name">>,
    Trace = [
        {<<"name">>, Name},
        {<<"type">>, <<"topic">>},
        {<<"topic">>, <<"/x/y/z">>}
    ],

    {ok, Create} = request_api(post, api_path("trace"), Header, Trace),
    ?assertEqual(#{<<"code">> => 0}, json(Create)),

    {ok, List} = request_api(get, api_path("trace"), Header),
    #{<<"code">> := 0, <<"data">> := [Data]} = json(List),
    ?assertEqual(Name, maps:get(<<"name">>, Data)),

    %% update
    {ok, Update} = request_api(put, api_path("trace/test-name/stop"), Header, #{}),
    ?assertEqual(#{<<"code">> => 0,
        <<"data">> => #{<<"enable">> => false,
            <<"name">> => <<"test-name">>}}, json(Update)),

    {ok, List1} = request_api(get, api_path("trace"), Header),
    #{<<"code">> := 0, <<"data">> := [Data1]} = json(List1),
    Node = atom_to_binary(node()),
    ?assertMatch(#{
        <<"status">> := <<"stopped">>,
        <<"name">> := <<"test-name">>,
        <<"log_size">> := #{Node := _},
        <<"start_at">> := _,
        <<"end_at">> := _,
        <<"type">> := <<"topic">>,
        <<"topic">> := <<"/x/y/z">>
    }, Data1),

    %% delete
    {ok, Delete} = request_api(delete, api_path("trace/test-name"), Header),
    ?assertEqual(#{<<"code">> => 0}, json(Delete)),

    {ok, DeleteNotFound} = request_api(delete, api_path("trace/test-name"), Header),
    ?assertEqual(#{<<"code">> => <<"NOT_FOUND">>,
        <<"message">> => <<"test-name NOT FOUND">>}, json(DeleteNotFound)),

    {ok, List2} = request_api(get, api_path("trace"), Header),
    ?assertEqual(#{<<"code">> => 0, <<"data">> => []}, json(List2)),

    %% clear
    {ok, Create1} = request_api(post, api_path("trace"), Header, Trace),
    ?assertEqual(#{<<"code">> => 0}, json(Create1)),

    {ok, Clear} = request_api(delete, api_path("trace"), Header),
    ?assertEqual(#{<<"code">> => 0}, json(Clear)),

    unload(),
    ok.

t_stream_log(_Config) ->
    application:set_env(emqx, allow_anonymous, true),
    emqx_trace:clear(),
    load(),
    ClientId = <<"client-stream">>,
    Now = erlang:system_time(second),
    Name = <<"test_stream_log">>,
    Start = to_rfc3339(Now - 10),
    ok = emqx_trace:create([{<<"name">>, Name},
        {<<"type">>, <<"clientid">>}, {<<"clientid">>, ClientId}, {<<"start_at">>, Start}]),
    ct:sleep(200),
    {ok, Client} = emqtt:start_link([{clean_start, true}, {clientid, ClientId}]),
    {ok, _} = emqtt:connect(Client),
    [begin _ = emqtt:ping(Client) end ||_ <- lists:seq(1, 5)],
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
    #{<<"code">> := 0, <<"data">> := #{<<"meta">> := Meta, <<"items">> := Bin}} = json(Binary),
    ?assertEqual(10, byte_size(Bin)),
    ?assertEqual(#{<<"position">> => 10, <<"bytes">> => 10}, Meta),
    Path = api_path("trace/test_stream_log/log?position=20&bytes=10"),
    {ok, Binary1} = request_api(get, Path, Header),
    #{<<"code">> := 0, <<"data">> := #{<<"meta">> := Meta1, <<"items">> := Bin1}} = json(Binary1),
    ?assertEqual(#{<<"position">> => 30, <<"bytes">> => 10}, Meta1),
    ?assertEqual(10, byte_size(Bin1)),

    {ok, Detail} = request_api(get, api_path("trace/test_stream_log/detail"), Header),
    #{<<"data">> := [#{<<"size">> := Size, <<"node">> := Node, <<"mtime">> := Mtime}],
        <<"code">> := 0} = json(Detail),
    ?assertEqual(atom_to_binary(node()), Node),
    ?assert(Size > 0),
    ?assert(Mtime >= Now),
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

load() ->
    emqx_trace:start_link().

unload() ->
    gen_server:stop(emqx_trace).
