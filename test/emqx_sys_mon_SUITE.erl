%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_sys_mon_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-include_lib("common_test/include/ct.hrl").

-include("emqx_mqtt.hrl").

-define(SYSMONPID, emqx_sys_mon).
-define(INPUTINFO, [{self(), long_gc, concat_str("long_gc warning: pid = ~p, info: ~p", self(), "hello"), "hello"},
                    {self(), long_schedule, concat_str("long_schedule warning: pid = ~p, info: ~p", self(), "hello"), "hello"},
                    {self(), busy_port, concat_str("busy_port warning: suspid = ~p, port = ~p", self(), list_to_port("#Port<0.4>")), list_to_port("#Port<0.4>")},
                    {self(), busy_dist_port, concat_str("busy_dist_port warning: suspid = ~p, port = ~p", self(), list_to_port("#Port<0.4>")),list_to_port("#Port<0.4>")},
                    {list_to_port("#Port<0.4>"), long_schedule, concat_str("long_schedule warning: port = ~p, info: ~p", list_to_port("#Port<0.4>"), "hello"), "hello"}
                    ]).

all() -> [t_sys_mon].

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

t_sys_mon(_Config) ->
    lists:foreach(fun({PidOrPort, SysMonName,ValidateInfo, InfoOrPort}) ->
                      validate_sys_mon_info(PidOrPort, SysMonName,ValidateInfo, InfoOrPort)
                  end, ?INPUTINFO).

validate_sys_mon_info(PidOrPort, SysMonName,ValidateInfo, InfoOrPort) ->
    {ok, C} = emqx_client:start_link([{host, "localhost"}]),
    {ok, _} = emqx_client:connect(C),
    emqx_client:subscribe(C, emqx_topic:systop(lists:concat(['sysmon/', SysMonName])), qos1),
    timer:sleep(100),
    ?SYSMONPID ! {monitor, PidOrPort, SysMonName, InfoOrPort},
    receive
        {publish,  #{payload := Info}} ->
            ?assertEqual(ValidateInfo, binary_to_list(Info)),
            ct:pal("OK - received msg: ~p~n", [Info])
    after
        1000 ->
            ct:fail("flase")
    end,
    emqx_client:stop(C).

concat_str(ValidateInfo, InfoOrPort, Info) ->
    WarnInfo = io_lib:format(ValidateInfo, [InfoOrPort, Info]),
    lists:flatten(WarnInfo).
