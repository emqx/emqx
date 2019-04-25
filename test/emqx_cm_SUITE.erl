
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

-module(emqx_cm_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> [{group, cm}].

groups() ->
    [{cm, [non_parallel_tests],
      [t_get_set_conn_attrs,
       t_get_set_conn_stats,
       t_lookup_conn_pid]}].

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

init_per_testcase(_TestCase, Config) ->
    register_connection(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    unregister_connection(),
    ok.

t_get_set_conn_attrs(_) ->
    ?assert(emqx_cm:set_conn_attrs(<<"conn1">>, [{port, 8080}, {ip, "192.168.0.1"}])),
    ?assert(emqx_cm:set_conn_attrs(<<"conn2">>, self(), [{port, 8080}, {ip, "192.168.0.2"}])),
    ?assertEqual([{port, 8080}, {ip, "192.168.0.1"}], emqx_cm:get_conn_attrs(<<"conn1">>)),
    ?assertEqual([{port, 8080}, {ip, "192.168.0.2"}], emqx_cm:get_conn_attrs(<<"conn2">>, self())).

t_get_set_conn_stats(_) ->
    ?assert(emqx_cm:set_conn_stats(<<"conn1">>, [{count, 1}, {max, 2}])),
    ?assert(emqx_cm:set_conn_stats(<<"conn2">>, self(), [{count, 1}, {max, 2}])),
    ?assertEqual([{count, 1}, {max, 2}], emqx_cm:get_conn_stats(<<"conn1">>)),
    ?assertEqual([{count, 1}, {max, 2}], emqx_cm:get_conn_stats(<<"conn2">>, self())).

t_lookup_conn_pid(_) ->
    ?assertEqual(ok, emqx_cm:register_connection(<<"conn1">>, self())),
    ?assertEqual(self(), emqx_cm:lookup_conn_pid(<<"conn1">>)).

register_connection() ->
    ?assertEqual(ok, emqx_cm:register_connection(<<"conn1">>)),
    ?assertEqual(ok, emqx_cm:register_connection(<<"conn2">>, self())).

unregister_connection() ->
    ?assertEqual(ok, emqx_cm:unregister_connection(<<"conn1">>)),
    ?assertEqual(ok, emqx_cm:unregister_connection(<<"conn2">>, self())).
