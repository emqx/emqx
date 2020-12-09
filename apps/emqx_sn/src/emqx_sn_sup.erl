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

-module(emqx_sn_sup).

-behaviour(supervisor).

-export([ start_link/2
        , start_registry_proc/3
        , init/1
        ]).

start_registry_proc(Sup, TabName, PredefTopics) ->
    Registry = #{id       => TabName,
                 start    => {emqx_sn_registry, start_link, [TabName, PredefTopics]},
                 restart  => permanent,
                 shutdown => 5000,
                 type     => worker,
                 modules  => [emqx_sn_registry]},
    handle_ret(supervisor:start_child(Sup, Registry)).

start_link(Addr, GwId) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Addr, GwId]).

init([{_Ip, Port}, GwId]) ->
    Broadcast = #{id       => emqx_sn_broadcast,
                  start    => {emqx_sn_broadcast, start_link, [GwId, Port]},
                  restart  => permanent,
                  shutdown => brutal_kill,
                  type     => worker,
                  modules  => [emqx_sn_broadcast]},
    {ok, {{one_for_one, 10, 3600}, [Broadcast]}}.

handle_ret({ok, Pid, _Info}) -> {ok, Pid};
handle_ret(Ret) -> Ret.

