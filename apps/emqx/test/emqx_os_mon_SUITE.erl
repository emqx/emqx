%%--------------------------------------------------------------------
%% Copyright (c) 2019-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_os_mon_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_common_test_helpers:boot_modules(all),
    emqx_common_test_helpers:start_apps(
        [],
        fun
            (emqx) ->
                application:set_env(emqx, os_mon, [
                    {cpu_check_interval, 1},
                    {cpu_high_watermark, 5},
                    {cpu_low_watermark, 80},
                    {procmem_high_watermark, 5}
                ]);
            (_) ->
                ok
        end
    ),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([]).

t_api(_) ->
    ?assertEqual(60000, emqx_os_mon:get_mem_check_interval()),
    ?assertEqual(ok, emqx_os_mon:set_mem_check_interval(30000)),
    ?assertEqual(60000, emqx_os_mon:get_mem_check_interval()),
    ?assertEqual(ok, emqx_os_mon:set_mem_check_interval(122000)),
    ?assertEqual(120000, emqx_os_mon:get_mem_check_interval()),

    ?assertEqual(0.7, emqx_os_mon:get_sysmem_high_watermark()),
    ?assertEqual(ok, emqx_os_mon:set_sysmem_high_watermark(0.8)),
    ?assertEqual(0.8, emqx_os_mon:get_sysmem_high_watermark()),

    ?assertEqual(5, emqx_os_mon:get_procmem_high_watermark()),
    ?assertEqual(ok, emqx_os_mon:set_procmem_high_watermark(0.11)),
    ?assertEqual(11, emqx_os_mon:get_procmem_high_watermark()),

    ?assertEqual(
        {error, {unexpected_call, ignored}},
        gen_server:call(emqx_os_mon, ignored)
    ),
    ?assertEqual(ok, gen_server:cast(emqx_os_mon, ignored)),
    emqx_os_mon ! ignored,
    gen_server:stop(emqx_os_mon),
    ok.
