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

-module(emqx_stats_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").

all() -> [t_set_get_state, t_update_interval].

t_set_get_state(_) ->
    {ok, _} = emqx_stats:start_link(),
    SetClientsCount = emqx_stats:statsfun('clients/count'),
    SetClientsCount(1),
    1 = emqx_stats:getstat('clients/count'),
    emqx_stats:setstat('clients/count', 2),
    2 = emqx_stats:getstat('clients/count'),
    emqx_stats:setstat('clients/count', 'clients/max', 3),
    timer:sleep(100),
    3 = emqx_stats:getstat('clients/count'),
    3 = emqx_stats:getstat('clients/max'),
    emqx_stats:setstat('clients/count', 'clients/max', 2),
    timer:sleep(100),
    2 = emqx_stats:getstat('clients/count'),
    3 = emqx_stats:getstat('clients/max'),
    SetClients = emqx_stats:statsfun('clients/count', 'clients/max'),
    SetClients(4),
    timer:sleep(100),
    4 = emqx_stats:getstat('clients/count'),
    4 = emqx_stats:getstat('clients/max'),
    Clients = emqx_stats:getstats(),
    4 = proplists:get_value('clients/count', Clients),
    4 = proplists:get_value('clients/max', Clients).

t_update_interval(_) ->
    {ok, _} = emqx_stats:start_link(),
    ok = emqx_stats:update_interval(cm_stats, fun update_stats/0),
    timer:sleep(2000),
    1 = emqx_stats:getstat('clients/count').

update_stats() ->
    ClientsCount = emqx_stats:getstat('clients/count'),
    ct:log("hello~n"),
    % emqx_stats:setstat('clients/count', 'clients/max', ClientsCount + 1).
    emqx_stats:setstat('clients/count',  1).