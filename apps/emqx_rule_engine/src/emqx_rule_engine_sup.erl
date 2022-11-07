%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_rule_engine_sup).

-behaviour(supervisor).

-include("rule_engine.hrl").

-export([start_link/0]).

-export([ start_locker/0
        , start_jwt_sup/0
        ]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Opts = [public, named_table, set, {read_concurrency, true}],
    ensure_table(?ACTION_INST_PARAMS_TAB, [{keypos, #action_instance_params.id}|Opts]),
    ensure_table(?RES_PARAMS_TAB, [{keypos, #resource_params.id}|Opts]),
    SupFlags = #{ strategy => one_for_one
                , intensity => 10
                , period => 10
                },
    Registry = #{id => emqx_rule_registry,
                 start => {emqx_rule_registry, start_link, []},
                 restart => permanent,
                 shutdown => 5000,
                 type => worker,
                 modules => [emqx_rule_registry]},
    Metrics = #{id => emqx_rule_metrics,
                start => {emqx_rule_metrics, start_link, []},
                restart => permanent,
                shutdown => 5000,
                type => worker,
                modules => [emqx_rule_metrics]},
    Monitor = #{id => emqx_rule_monitor,
                start => {emqx_rule_monitor, start_link, []},
                restart => permanent,
                shutdown => 5000,
                type => worker,
                modules => [emqx_rule_monitor]},
    JWTSup = jwt_sup_child_spec(),
    {ok, {SupFlags, [Registry, Metrics, Monitor, JWTSup]}}.

start_locker() ->
    Locker = #{id => emqx_rule_locker,
               start => {emqx_rule_locker, start_link, []},
               restart => permanent,
               shutdown => 5000,
               type => worker,
               modules => [emqx_rule_locker]},
    supervisor:start_child(?MODULE, Locker).

start_jwt_sup() ->
    JWTSup = jwt_sup_child_spec(),
    supervisor:start_child(?MODULE, JWTSup).

jwt_sup_child_spec() ->
    #{ id => emqx_rule_engine_jwt_sup
     , start => {emqx_rule_engine_jwt_sup, start_link, []}
     , type => supervisor
     , restart => permanent
     , shutdown => 5_000
     , modules => [emqx_rule_engine_jwt_sup]
     }.

ensure_table(Name, Opts) ->
    try
        case ets:whereis(name) of
            undefined ->
                _ = ets:new(Name, Opts),
                ok;
            _ ->
                ok
        end
    catch
        %% stil the table exists (somehow can happen in hot-upgrade,
        %% it seems).
        error:badarg ->
            ok
    end.
