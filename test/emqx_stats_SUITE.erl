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

-module(emqx_stats_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").

all() -> [t_set_get_state, t_update_interval].

t_set_get_state(_) ->
    {ok, _} = emqx_stats:start_link(),
    SetConnsCount = emqx_stats:statsfun('connections/count'),
    SetConnsCount(1),
    1 = emqx_stats:getstat('connections/count'),
    emqx_stats:setstat('connections/count', 2),
    2 = emqx_stats:getstat('connections/count'),
    emqx_stats:setstat('connections/count', 'connections/max', 3),
    timer:sleep(100),
    3 = emqx_stats:getstat('connections/count'),
    3 = emqx_stats:getstat('connections/max'),
    emqx_stats:setstat('connections/count', 'connections/max', 2),
    timer:sleep(100),
    2 = emqx_stats:getstat('connections/count'),
    3 = emqx_stats:getstat('connections/max'),
    SetConns = emqx_stats:statsfun('connections/count', 'connections/max'),
    SetConns(4),
    timer:sleep(100),
    4 = emqx_stats:getstat('connections/count'),
    4 = emqx_stats:getstat('connections/max'),
    Conns = emqx_stats:getstats(),
    4 = proplists:get_value('connections/count', Conns),
    4 = proplists:get_value('connections/max', Conns).

t_update_interval(_) ->
    {ok, _} = emqx_stats:start_link(),
    ok = emqx_stats:update_interval(cm_stats, fun update_stats/0),
    timer:sleep(2500),
    1 = emqx_stats:getstat('connections/count').

update_stats() ->
    emqx_stats:setstat('connections/count',  1).
