%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_pmon_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

% t_new(_) ->
%     error('TODO').

% t_count(_) ->
%     error('TODO').

t_monitor(_) ->
    PMon = emqx_pmon:new(),
    PMon1 = emqx_pmon:monitor(self(), PMon),
    ?assertEqual(1, emqx_pmon:count(PMon1)),
    PMon2 = emqx_pmon:demonitor(self(), PMon1),
    PMon2 = emqx_pmon:demonitor(self(), PMon2),
    ?assertEqual(0, emqx_pmon:count(PMon2)).

% t_demonitor(_) ->
%     error('TODO').

t_find(_) ->
    PMon = emqx_pmon:new(),
    PMon1 = emqx_pmon:monitor(self(), val, PMon),
    PMon1 = emqx_pmon:monitor(self(), val, PMon1),
    ?assertEqual(1, emqx_pmon:count(PMon1)),
    ?assertEqual({ok, val}, emqx_pmon:find(self(), PMon1)),
    PMon2 = emqx_pmon:erase(self(), PMon1),
    PMon2 = emqx_pmon:erase(self(), PMon1),
    ?assertEqual(error, emqx_pmon:find(self(), PMon2)).

t_erase(_) ->
    PMon = emqx_pmon:new(),
    PMon1 = emqx_pmon:monitor(self(), val, PMon),
    PMon2 = emqx_pmon:erase(self(), PMon1),
    ?assertEqual(0, emqx_pmon:count(PMon2)),
    {Items, PMon3} = emqx_pmon:erase_all([self()], PMon1),
    {[], PMon3} = emqx_pmon:erase_all([self()], PMon3),
    ?assertEqual([{self(), val}], Items),
    ?assertEqual(0, emqx_pmon:count(PMon3)).

% t_erase_all(_) ->
%     error('TODO').
