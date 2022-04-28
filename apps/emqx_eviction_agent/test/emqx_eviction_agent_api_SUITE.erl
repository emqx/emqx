%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_eviction_agent_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-import(emqx_mgmt_api_test_helpers,
        [request_api/3,
         auth_header_/0,
         api_path/1]).

all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([emqx_eviction_agent, emqx_management]),
    Config.

end_per_suite(Config) ->
    emqx_ct_helpers:stop_apps([emqx_management, emqx_eviction_agent]),
    Config.

t_status(_Config) ->
    ?assertMatch(
       {ok, #{<<"status">> := <<"disabled">>}},
       api_get(["node_eviction", "status"])),

    ok = emqx_eviction_agent:enable(apitest, undefined),

    ?assertMatch(
       {ok, #{<<"status">> := <<"enabled">>,
              <<"stats">> := #{}}},
        api_get(["node_eviction", "status"])),

    ok = emqx_eviction_agent:disable(apitest),

    ?assertMatch(
       {ok, #{<<"status">> := <<"disabled">>}},
       api_get(["node_eviction", "status"])).

api_get(Path) ->
    case request_api(get, api_path(Path), auth_header_()) of
        {ok, ResponseBody} ->
            {ok, jiffy:decode(list_to_binary(ResponseBody), [return_maps])};
        {error, _} = Error -> Error
    end.
