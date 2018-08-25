%%--------------------------------------------------------------------
%% Copyright (c) 2013-2018 EMQ Enterprise, Inc. (http://emqtt.io)
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

-module(emqx_cm_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_mqtt.hrl").

all() -> [t_register_unregister_client].

t_register_unregister_client(_) ->
    {ok, _} = emqx_cm_sup:start_link(),
    Pid = self(),
    emqx_cm:register_client(<<0, 0, 1>>),
    emqx_cm:register_client({<<0, 0, 2>>, Pid}, [{port, 8080}, {ip, "192.168.0.1"}]),
    timer:sleep(2000),
    [{<<0, 0, 1>>, Pid}] = emqx_cm:lookup_client(<<0, 0, 1>>),
    [{<<0, 0, 2>>, Pid}] = emqx_cm:lookup_client(<<0, 0, 2>>),
    Pid = emqx_cm:lookup_client_pid(<<0, 0, 1>>),
    emqx_cm:unregister_client(<<0, 0, 1>>),
    [] = emqx_cm:lookup_client(<<0, 0, 1>>),
    [{port, 8080}, {ip, "192.168.0.1"}] = emqx_cm:get_client_attrs({<<0, 0, 2>>, Pid}),
    emqx_cm:set_client_stats(<<0, 0, 2>>, [[{count, 1}, {max, 2}]]),
    [[{count, 1}, {max, 2}]] = emqx_cm:get_client_stats({<<0, 0, 2>>, Pid}).