%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
