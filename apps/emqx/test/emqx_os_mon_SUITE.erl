%%--------------------------------------------------------------------
%% Copyright (c) 2019-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_config:put([sysmon, os], #{
        cpu_check_interval => 60,cpu_high_watermark => 0.8,
        cpu_low_watermark => 0.6,mem_check_interval => 60,
        procmem_high_watermark => 0.05,sysmem_high_watermark => 0.7}),
    application:ensure_all_started(os_mon),
    Config.

end_per_suite(_Config) ->
    application:stop(os_mon).

t_api(_) ->
    gen_event:swap_handler(alarm_handler, {emqx_alarm_handler, swap}, {alarm_handler, []}),
    {ok, _} = emqx_os_mon:start_link(),

    ?assertEqual(60, emqx_os_mon:get_mem_check_interval()),
    ?assertEqual(ok, emqx_os_mon:set_mem_check_interval(30)),
    ?assertEqual(60, emqx_os_mon:get_mem_check_interval()),
    ?assertEqual(ok, emqx_os_mon:set_mem_check_interval(122)),
    ?assertEqual(120, emqx_os_mon:get_mem_check_interval()),

    ?assertEqual(70, emqx_os_mon:get_sysmem_high_watermark()),
    ?assertEqual(ok, emqx_os_mon:set_sysmem_high_watermark(0.8)),
    ?assertEqual(80, emqx_os_mon:get_sysmem_high_watermark()),

    ?assertEqual(5, emqx_os_mon:get_procmem_high_watermark()),
    ?assertEqual(ok, emqx_os_mon:set_procmem_high_watermark(0.11)),
    ?assertEqual(11, emqx_os_mon:get_procmem_high_watermark()),

    ?assertEqual(ignored, gen_server:call(emqx_os_mon, ignored)),
    ?assertEqual(ok, gen_server:cast(emqx_os_mon, ignored)),
    emqx_os_mon ! ignored,
    gen_server:stop(emqx_os_mon),
    ok.

