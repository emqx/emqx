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

-module(emqx_ft_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-include_lib("emqx/include/asserts.hrl").

-import(emqx_dashboard_api_test_helpers, [host/0, uri/1]).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    ok = emqx_mgmt_api_test_util:init_suite(
        [emqx_conf, emqx_ft], emqx_ft_test_helpers:env_handler(Config)
    ),
    {ok, _} = emqx:update_config([rpc, port_discovery], manual),
    Config.
end_per_suite(_Config) ->
    ok = emqx_mgmt_api_test_util:end_suite([emqx_ft, emqx_conf]),
    ok.

init_per_testcase(Case, Config) ->
    [{tc, Case} | Config].
end_per_testcase(_Case, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

t_list_files(Config) ->
    ClientId = client_id(Config),
    FileId = <<"f1">>,

    ok = emqx_ft_test_helpers:upload_file(ClientId, FileId, "f1", <<"data">>),

    {ok, 200, #{<<"files">> := Files}} =
        request_json(get, uri(["file_transfer", "files"])),

    ?assertMatch(
        [#{<<"clientid">> := ClientId, <<"fileid">> := <<"f1">>}],
        [File || File = #{<<"clientid">> := CId} <- Files, CId == ClientId]
    ),

    {ok, 200, #{<<"files">> := FilesTransfer}} =
        request_json(get, uri(["file_transfer", "files", ClientId, FileId])),

    ?assertMatch(
        [#{<<"clientid">> := ClientId, <<"fileid">> := <<"f1">>}],
        FilesTransfer
    ),

    ?assertMatch(
        {ok, 404, #{<<"code">> := <<"FILES_NOT_FOUND">>}},
        request_json(get, uri(["file_transfer", "files", ClientId, <<"no-such-file">>]))
    ).

t_download_transfer(Config) ->
    ClientId = client_id(Config),

    ok = emqx_ft_test_helpers:upload_file(ClientId, <<"f1">>, "f1", <<"data">>),

    ?assertMatch(
        {ok, 400, #{<<"code">> := <<"BAD_REQUEST">>}},
        request_json(
            get,
            uri(["file_transfer", "file"]) ++ query(#{fileref => <<"f1">>})
        )
    ),

    ?assertMatch(
        {ok, 503, _},
        request(
            get,
            uri(["file_transfer", "file"]) ++
                query(#{
                    fileref => <<"f1">>,
                    node => <<"nonode@nohost">>
                })
        )
    ),

    ?assertMatch(
        {ok, 404, _},
        request(
            get,
            uri(["file_transfer", "file"]) ++
                query(#{
                    fileref => <<"unknown_file">>,
                    node => node()
                })
        )
    ),

    {ok, 200, #{<<"files">> := [File]}} =
        request_json(get, uri(["file_transfer", "files"])),

    {ok, 200, Response} = request(get, host() ++ maps:get(<<"uri">>, File)),

    ?assertEqual(
        <<"data">>,
        Response
    ).

t_list_files_paging(Config) ->
    ClientId = client_id(Config),
    NFiles = 20,
    Uploads = [{mk_file_id("file:", N), mk_file_name(N)} || N <- lists:seq(1, NFiles)],
    ok = lists:foreach(
        fun({FileId, Name}) ->
            ok = emqx_ft_test_helpers:upload_file(ClientId, FileId, Name, <<"data">>)
        end,
        Uploads
    ),

    ?assertMatch(
        {ok, 200, #{<<"files">> := [_, _, _], <<"cursor">> := _}},
        request_json(get, uri(["file_transfer", "files"]) ++ query(#{limit => 3}))
    ),

    {ok, 200, #{<<"files">> := Files}} =
        request_json(get, uri(["file_transfer", "files"]) ++ query(#{limit => 100})),

    ?assert(length(Files) >= NFiles),

    ?assertNotMatch(
        {ok, 200, #{<<"cursor">> := _}},
        request_json(get, uri(["file_transfer", "files"]) ++ query(#{limit => 100}))
    ),

    ?assertMatch(
        {ok, 400, #{<<"code">> := <<"BAD_REQUEST">>}},
        request_json(get, uri(["file_transfer", "files"]) ++ query(#{limit => 0}))
    ),

    ?assertMatch(
        {ok, 400, #{<<"code">> := <<"BAD_REQUEST">>}},
        request_json(
            get,
            uri(["file_transfer", "files"]) ++ query(#{following => <<"whatsthat!?">>})
        )
    ),

    PageThrough = fun PageThrough(Query, Acc) ->
        case request_json(get, uri(["file_transfer", "files"]) ++ query(Query)) of
            {ok, 200, #{<<"files">> := FilesPage, <<"cursor">> := Cursor}} ->
                PageThrough(Query#{following => Cursor}, Acc ++ FilesPage);
            {ok, 200, #{<<"files">> := FilesPage}} ->
                Acc ++ FilesPage
        end
    end,

    ?assertEqual(Files, PageThrough(#{limit => 1}, [])),
    ?assertEqual(Files, PageThrough(#{limit => 8}, [])),
    ?assertEqual(Files, PageThrough(#{limit => NFiles}, [])).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

client_id(Config) ->
    atom_to_binary(?config(tc, Config), utf8).

mk_file_id(Prefix, N) ->
    iolist_to_binary([Prefix, integer_to_list(N)]).

mk_file_name(N) ->
    "file." ++ integer_to_list(N).

request(Method, Url) ->
    emqx_mgmt_api_test_util:request(Method, Url, []).

request_json(Method, Url) ->
    case emqx_mgmt_api_test_util:request(Method, Url, []) of
        {ok, Code, Body} ->
            {ok, Code, json(Body)};
        Otherwise ->
            Otherwise
    end.

json(Body) when is_binary(Body) ->
    emqx_utils_json:decode(Body, [return_maps]).

query(Params) ->
    KVs = lists:map(fun({K, V}) -> uri_encode(K) ++ "=" ++ uri_encode(V) end, maps:to_list(Params)),
    "?" ++ string:join(KVs, "&").

uri_encode(T) ->
    emqx_http_lib:uri_encode(to_list(T)).

to_list(A) when is_atom(A) ->
    atom_to_list(A);
to_list(A) when is_integer(A) ->
    integer_to_list(A);
to_list(B) when is_binary(B) ->
    binary_to_list(B);
to_list(L) when is_list(L) ->
    L.
