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

-include("emqx.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("emqx_mqtt.hrl").

all() -> [t_register_connection, t_unregister_connection, t_get_conn_attrs, t_set_conn_attrs,
          t_get_conn_stats, t_set_conn_stats, t_lookup_conn_pid].

t_register_connection(_) ->
    {ok, _} = emqx_cm_sup:start_link(),
    Pid = self(),
    ?assertEqual(ok, emqx_cm:register_connection(<<"conn1">>)),
    ?assertEqual(ok, emqx_cm:register_connection(<<"conn2">>, Pid)).

t_unregister_connection(_) ->
    {ok, _} = emqx_cm_sup:start_link(),
    Pid = self(),
    emqx_cm:register_connection(<<"conn1">>),
    emqx_cm:register_connection(<<"conn2">>, Pid),
    ?assertEqual(ok, emqx_cm:unregister_connection(<<"conn1">>)),
    ?assertEqual(ok, emqx_cm:unregister_connection(<<"conn2">>, Pid)).

t_get_conn_attrs(_) ->
    {ok, _} = emqx_cm_sup:start_link(),
    ok = emqx_cm:register_connection(<<"conn1">>),
    Pid = self(),
    true = emqx_cm:set_conn_attrs(<<"conn1">>, [{port, 8080}, {ip, "192.168.0.1"}]),
    true = emqx_cm:set_conn_attrs(<<"conn2">>, Pid, [{port, 8080}, {ip, "192.168.0.1"}]),
    ?assertEqual([{port, 8080}, {ip, "192.168.0.1"}], emqx_cm:get_conn_attrs(<<"conn1">>)),
    ?assertEqual([{port, 8080}, {ip, "192.168.0.1"}], emqx_cm:get_conn_attrs(<<"conn1">>, Pid)).

t_set_conn_attrs(_) ->
    {ok, _} = emqx_cm_sup:start_link(),
    ok = emqx_cm:register_connection(<<"conn1">>),
    Pid = self(),
    ?assertEqual(true, emqx_cm:set_conn_attrs(<<"conn1">>, [{port, 8080}, {ip, "192.168.0.1"}])),
    ?assertEqual(true, emqx_cm:set_conn_attrs(<<"conn2">>, Pid, [{port, 8080}, {ip, "192.168.0.1"}])).

t_get_conn_stats(_) ->
    {ok, _} = emqx_cm_sup:start_link(),
    ok = emqx_cm:register_connection(<<"conn1">>),
    Pid = self(),
    true = emqx_cm:set_conn_stats(<<"conn1">>, [{count, 1}, {max, 2}]),
    true = emqx_cm:set_conn_stats(<<"conn2">>, Pid, [{count, 1}, {max, 2}]),
    ?assertEqual([{count, 1}, {max, 2}], emqx_cm:get_conn_stats(<<"conn1">>)),
    ?assertEqual([{count, 1}, {max, 2}], emqx_cm:get_conn_stats(<<"conn2">>, Pid)).

t_set_conn_stats(_) ->
    {ok, _} = emqx_cm_sup:start_link(),
    ok = emqx_cm:register_connection(<<"conn1">>),
    Pid = self(),
    ?assertEqual(true, emqx_cm:set_conn_stats(<<"conn2">>, [{count, 1}, {max, 2}])),
    ?assertEqual(true, emqx_cm:set_conn_stats(<<"conn2">>, Pid, [{count, 1}, {max, 2}])).

t_lookup_conn_pid(_) ->
    {ok, _} = emqx_cm_sup:start_link(),
    Pid = self(),
    ok = emqx_cm:register_connection(<<"conn1">>, Pid),
    ?assertEqual(Pid, emqx_cm:lookup_conn_pid(<<"conn1">>)).
