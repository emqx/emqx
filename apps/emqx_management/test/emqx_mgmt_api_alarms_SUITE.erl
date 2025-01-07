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
-module(emqx_mgmt_api_alarms_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(ACT_ALARM, test_act_alarm).
-define(DE_ACT_ALARM, test_de_act_alarm).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

t_alarms_api(_) ->
    ok = emqx_alarm:activate(?ACT_ALARM),
    ok = emqx_alarm:activate(?DE_ACT_ALARM),
    ok = emqx_alarm:deactivate(?DE_ACT_ALARM),
    get_alarms(1, true),
    get_alarms(1, false).

t_alarm_cpu(_) ->
    ok.

t_delete_alarms_api(_) ->
    Path = emqx_mgmt_api_test_util:api_path(["alarms"]),
    {ok, _} = emqx_mgmt_api_test_util:request_api(delete, Path),
    get_alarms(1, true),
    get_alarms(0, false).

get_alarms(AssertCount, Activated) when is_atom(Activated) ->
    get_alarms(AssertCount, atom_to_list(Activated));
get_alarms(AssertCount, Activated) ->
    Path = emqx_mgmt_api_test_util:api_path(["alarms"]),
    Qs = "activated=" ++ Activated,
    Headers = emqx_mgmt_api_test_util:auth_header_(),
    {ok, Response} = emqx_mgmt_api_test_util:request_api(get, Path, Qs, Headers),
    Data = emqx_utils_json:decode(Response, [return_maps]),
    Meta = maps:get(<<"meta">>, Data),
    Page = maps:get(<<"page">>, Meta),
    Limit = maps:get(<<"limit">>, Meta),
    Count = maps:get(<<"count">>, Meta),
    ?assertEqual(Page, 1),
    ?assertEqual(Limit, emqx_mgmt:default_row_limit()),
    ?assert(Count >= AssertCount).
