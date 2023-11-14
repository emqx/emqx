%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_dashboard_listener_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_mgmt_api_test_util:init_suite([emqx_conf]),
    ok = change_i18n_lang(en),
    Config.

end_per_suite(_Config) ->
    ok = change_i18n_lang(en),
    emqx_mgmt_api_test_util:end_suite([emqx_conf]).

init_per_testcase(t_dispatch_generate_retry, Config) ->
    ok = meck:new(application, [passthrough, no_link, unstick]),
    meck:expect(
        application,
        set_env,
        fun(_) ->
            meck:exception(exit, {gen_server, call, [application_controller, {set_env, ops}]})
        end
    ),
    Config;
init_per_testcase(_Testcase, Config) ->
    Config.

end_per_testcase(t_dispatch_generate_retry, _Config) ->
    catch meck:unload(application),
    ok;
end_per_testcase(_Testcase, _Config) ->
    ok.

t_dispatch_generate_retry(_Config) ->
    Pid1 = erlang:whereis(emqx_dashboard_listener),
    ok = gen_server:stop(emqx_dashboard_listener),
    timer:sleep(500),
    Pid2 = erlang:whereis(emqx_dashboard_listener),
    ?assertNotEqual(Pid1, Pid2),
    ?assertNot(emqx_dashboard_listener:is_ready(500)),
    ?assertExit(
        {timeout, _},
        sys:get_state(emqx_dashboard_listener, 1000),
        "emqx_dashboard_listenre is busy and timeout"
    ),
    meck:unload(application),
    ?assert(emqx_dashboard_listener:is_ready(11000)),
    ok.

t_change_i18n_lang(_Config) ->
    ?check_trace(
        begin
            ok = change_i18n_lang(zh),
            {ok, _} = ?block_until(#{?snk_kind := regenerate_minirest_dispatch}, 10_000),
            ok
        end,
        fun(ok, Trace) ->
            ?assertMatch([#{i18n_lang := zh}], ?of_kind(regenerate_minirest_dispatch, Trace))
        end
    ),
    ok.

change_i18n_lang(Lang) ->
    {ok, _} = emqx_conf:update([dashboard], {change_i18n_lang, Lang}, #{}),
    ok.
