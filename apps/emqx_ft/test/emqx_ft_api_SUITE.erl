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
        [emqx_conf, emqx_ft], set_special_configs(Config)
    ),
    {ok, _} = emqx:update_config([rpc, port_discovery], manual),
    Config.
end_per_suite(_Config) ->
    ok = emqx_mgmt_api_test_util:end_suite([emqx_ft, emqx_conf]),
    ok.

set_special_configs(Config) ->
    fun
        (emqx_ft) ->
            emqx_ft_test_helpers:load_config(#{
                storage => emqx_ft_test_helpers:local_storage(Config)
            });
        (_) ->
            ok
    end.

init_per_testcase(Case, Config) ->
    [{tc, Case} | Config].
end_per_testcase(_Case, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

t_list_ready_transfers(Config) ->
    ClientId = client_id(Config),

    ok = emqx_ft_test_helpers:upload_file(ClientId, <<"f1">>, "f1", <<"data">>),

    {ok, 200, #{<<"files">> := Files}} =
        request(get, uri(["file_transfer", "files"]), fun json/1),

    ?assertInclude(
        #{<<"clientid">> := ClientId, <<"fileid">> := <<"f1">>},
        Files
    ).

t_download_transfer(Config) ->
    ClientId = client_id(Config),

    ok = emqx_ft_test_helpers:upload_file(ClientId, <<"f1">>, "f1", <<"data">>),

    ?assertMatch(
        {ok, 400, #{<<"code">> := <<"BAD_REQUEST">>}},
        request(
            get,
            uri(["file_transfer", "file"]) ++ query(#{fileref => <<"f1">>}),
            fun json/1
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
        request(get, uri(["file_transfer", "files"]), fun json/1),

    {ok, 200, Response} = request(get, host() ++ maps:get(<<"uri">>, File)),

    ?assertEqual(
        <<"data">>,
        Response
    ).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

client_id(Config) ->
    atom_to_binary(?config(tc, Config), utf8).

request(Method, Url) ->
    emqx_mgmt_api_test_util:request(Method, Url, []).

request(Method, Url, Decoder) when is_function(Decoder) ->
    case emqx_mgmt_api_test_util:request(Method, Url, []) of
        {ok, Code, Body} ->
            {ok, Code, Decoder(Body)};
        Otherwise ->
            Otherwise
    end.

json(Body) when is_binary(Body) ->
    emqx_json:decode(Body, [return_maps]).

query(Params) ->
    KVs = lists:map(fun({K, V}) -> uri_encode(K) ++ "=" ++ uri_encode(V) end, maps:to_list(Params)),
    "?" ++ string:join(KVs, "&").

uri_encode(T) ->
    emqx_http_lib:uri_encode(to_list(T)).

to_list(A) when is_atom(A) ->
    atom_to_list(A);
to_list(B) when is_binary(B) ->
    binary_to_list(B);
to_list(L) when is_list(L) ->
    L.
