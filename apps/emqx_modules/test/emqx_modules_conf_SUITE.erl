%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_modules_conf_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Conf) ->
    emqx_common_test_helpers:load_config(emqx_modules_schema, <<"gateway {}">>, #{
        raw_with_default => true
    }),
    emqx_common_test_helpers:start_apps([emqx_conf, emqx_modules]),
    Conf.

end_per_suite(_Conf) ->
    emqx_common_test_helpers:stop_apps([emqx_modules, emqx_conf]).

init_per_testcase(_CaseName, Conf) ->
    Conf.

%%--------------------------------------------------------------------
%% Cases
%%--------------------------------------------------------------------

t_topic_metrics_list(_) ->
    ok.

t_topic_metrics_add_remove(_) ->
    ok.
