%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_mgmt_api_ds_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-import(emqx_mgmt_api_test_util, [api_path/1, request_api/2, request_api_with_body/3]).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    case emqx_ds_test_helpers:skip_if_norepl() of
        false ->
            Apps = emqx_cth_suite:start(
                [
                    {emqx, "durable_sessions.enable = true"},
                    emqx_management,
                    {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
                ],
                #{work_dir => emqx_cth_suite:work_dir(Config)}
            ),
            {ok, _} = emqx_common_test_http:create_default_app(),
            [{suite_apps, Apps} | Config];
        Yes ->
            Yes
    end.

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, Config) ->
    Config.

t_get_sites(_) ->
    Path = api_path(["ds", "sites"]),
    {ok, Response} = request_api(get, Path),
    ?assertEqual(
        [emqx_ds_replication_layer_meta:this_site()],
        emqx_utils_json:decode(Response, [return_maps])
    ).

t_get_storages(_) ->
    Path = api_path(["ds", "storages"]),
    {ok, Response} = request_api(get, Path),
    ?assertEqual(
        [<<"messages">>],
        emqx_utils_json:decode(Response, [return_maps])
    ).

t_get_site(_) ->
    %% Unknown sites must result in error 404:
    Path404 = api_path(["ds", "sites", "unknown_site"]),
    ?assertMatch(
        {error, {_, 404, _}},
        request_api(get, Path404)
    ),
    %% Valid path:
    Path = api_path(["ds", "sites", emqx_ds_replication_layer_meta:this_site()]),
    {ok, Response} = request_api(get, Path),
    ThisNode = atom_to_binary(node()),
    ?assertMatch(
        #{
            <<"node">> := ThisNode,
            <<"up">> := true,
            <<"shards">> :=
                [
                    #{
                        <<"storage">> := <<"messages">>,
                        <<"id">> := _,
                        <<"status">> := <<"up">>
                    }
                    | _
                ]
        },
        emqx_utils_json:decode(Response, [return_maps])
    ).

t_get_db(_) ->
    %% Unknown DBs must result in error 404:
    Path404 = api_path(["ds", "storages", "unknown_ds"]),
    ?assertMatch(
        {error, {_, 404, _}},
        request_api(get, Path404)
    ),
    %% Valid path:
    Path = api_path(["ds", "storages", "messages"]),
    {ok, Response} = request_api(get, Path),
    ThisSite = emqx_ds_replication_layer_meta:this_site(),
    ?assertMatch(
        #{
            <<"name">> := <<"messages">>,
            <<"shards">> :=
                [
                    #{
                        <<"id">> := _,
                        <<"replicas">> :=
                            [
                                #{
                                    <<"site">> := ThisSite,
                                    <<"status">> := <<"up">>
                                }
                                | _
                            ]
                    }
                    | _
                ]
        },
        emqx_utils_json:decode(Response)
    ).

t_get_replicas(_) ->
    %% Unknown DBs must result in error 404:
    Path404 = api_path(["ds", "storages", "unknown_ds", "replicas"]),
    ?assertMatch(
        {error, {_, 404, _}},
        request_api(get, Path404)
    ),
    %% Valid path:
    Path = api_path(["ds", "storages", "messages", "replicas"]),
    {ok, Response} = request_api(get, Path),
    ThisSite = emqx_ds_replication_layer_meta:this_site(),
    ?assertEqual(
        [ThisSite],
        emqx_utils_json:decode(Response)
    ).

t_put_replicas(_) ->
    Path = api_path(["ds", "storages", "messages", "replicas"]),
    %% Error cases:
    ?assertMatch(
        {ok, 400, #{<<"message">> := <<"Unknown sites: invalid_site">>}},
        parse_error(request_api_with_body(put, Path, [<<"invalid_site">>]))
    ),
    %% Success case:
    ?assertMatch(
        {ok, 202, <<"OK">>},
        request_api_with_body(put, Path, [emqx_ds_replication_layer_meta:this_site()])
    ).

t_join(_) ->
    Path400 = api_path(["ds", "storages", "messages", "replicas", "unknown_site"]),
    ?assertMatch(
        {error, {_, 400, _}},
        parse_error(request_api(put, Path400))
    ),
    ThisSite = emqx_ds_replication_layer_meta:this_site(),
    Path = api_path(["ds", "storages", "messages", "replicas", ThisSite]),
    ?assertMatch(
        {ok, "OK"},
        request_api(put, Path)
    ).

t_leave(_) ->
    ThisSite = emqx_ds_replication_layer_meta:this_site(),
    Path = api_path(["ds", "storages", "messages", "replicas", ThisSite]),
    ?assertMatch(
        {error, {_, 400, _}},
        request_api(delete, Path)
    ).

t_leave_notfound(_) ->
    Site = "not_part_of_replica_set",
    Path = api_path(["ds", "storages", "messages", "replicas", Site]),
    ?assertMatch(
        {error, {_, 404, _}},
        request_api(delete, Path)
    ).

parse_error({ok, Code, JSON}) ->
    {ok, Code, emqx_utils_json:decode(JSON)};
parse_error(Err) ->
    Err.
