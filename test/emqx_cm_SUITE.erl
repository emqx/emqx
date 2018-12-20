%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include("emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").

all() -> [t_register_unregister_connection].

t_register_unregister_connection(_) ->
    {ok, _} = emqx_cm_sup:start_link(),
    Pid = self(),
    ok = emqx_cm:register_connection(<<"conn1">>),
    ok = emqx_cm:register_connection(<<"conn2">>, Pid),
    true = emqx_cm:set_conn_attrs(<<"conn1">>, [{port, 8080}, {ip, "192.168.0.1"}]),
    true = emqx_cm:set_conn_attrs(<<"conn2">>, Pid, [{port, 8080}, {ip, "192.168.0.1"}]),
    timer:sleep(2000),
    ?assertEqual(Pid, emqx_cm:lookup_conn_pid(<<"conn1">>)),
    ?assertEqual(Pid, emqx_cm:lookup_conn_pid(<<"conn2">>)),
    ok = emqx_cm:unregister_connection(<<"conn1">>),
    ?assertEqual(undefined, emqx_cm:lookup_conn_pid(<<"conn1">>)),
    ?assertEqual([{port, 8080}, {ip, "192.168.0.1"}], emqx_cm:get_conn_attrs({<<"conn2">>, Pid})),
    true = emqx_cm:set_conn_stats(<<"conn2">>, [{count, 1}, {max, 2}]),
    ?assertEqual([{count, 1}, {max, 2}], emqx_cm:get_conn_stats({<<"conn2">>, Pid})).

