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

-module(emqx_global_gc_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    emqx_config:erase_all(),
    ok.

t_run_gc(_) ->
    Conf0 = #{
        node => #{
            cookie => <<"cookie">>,
            data_dir => <<"data">>,
            global_gc_interval => <<"1s">>
        }
    },
    emqx_common_test_helpers:load_config(emqx_conf_schema, Conf0),
    ?assertEqual({ok, 1000}, application:get_env(emqx_machine, global_gc_interval)),
    {ok, _} = emqx_global_gc:start_link(),
    ok = timer:sleep(1500),
    {ok, MilliSecs} = emqx_global_gc:run(),
    ct:pal("Global GC: ~w(ms)~n", [MilliSecs]),
    emqx_global_gc:stop(),
    Conf1 = emqx_utils_maps:deep_put([node, global_gc_interval], Conf0, disabled),
    emqx_common_test_helpers:load_config(emqx_conf_schema, Conf1),
    {ok, Pid} = emqx_global_gc:start_link(),
    ?assertMatch(#{timer := undefined}, sys:get_state(Pid)),
    ?assertEqual({ok, disabled}, application:get_env(emqx_machine, global_gc_interval)),
    ok.
