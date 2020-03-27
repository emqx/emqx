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

-module(emqx_sup).

-behaviour(supervisor).

-include("types.hrl").

-export([ start_link/0
        , start_child/1
        , start_child/2
        , stop_child/1
        ]).

-export([init/1]).

-type(startchild_ret() :: {ok, supervisor:child()}
                        | {ok, supervisor:child(), term()}
                        | {error, term()}).

-define(SUP, ?MODULE).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec(start_link() -> startlink_ret()).
start_link() ->
    supervisor:start_link({local, ?SUP}, ?MODULE, []).

-spec(start_child(supervisor:child_spec()) -> startchild_ret()).
start_child(ChildSpec) when is_map(ChildSpec) ->
    supervisor:start_child(?SUP, ChildSpec).

-spec(start_child(module(), worker | supervisor) -> startchild_ret()).
start_child(Mod, Type) ->
    start_child(child_spec(Mod, Type)).

-spec(stop_child(supervisor:child_id()) -> ok | {error, term()}).
stop_child(ChildId) ->
    case supervisor:terminate_child(?SUP, ChildId) of
        ok -> supervisor:delete_child(?SUP, ChildId);
        Error -> Error
    end.

%%--------------------------------------------------------------------
%% Supervisor callbacks
%%--------------------------------------------------------------------

init([]) ->
    KernelSup = child_spec(emqx_kernel_sup, supervisor),
    RouterSup = child_spec(emqx_router_sup, supervisor),
    BrokerSup = child_spec(emqx_broker_sup, supervisor),
    CMSup = child_spec(emqx_cm_sup, supervisor),
    SysSup = child_spec(emqx_sys_sup, supervisor),
    ModSup = child_spec(emqx_mod_sup, supervisor),
    Childs = [KernelSup] ++
             [RouterSup || emqx_boot:is_enabled(router)] ++
             [BrokerSup || emqx_boot:is_enabled(broker)] ++
             [CMSup || emqx_boot:is_enabled(broker)] ++
             [SysSup] ++ [ModSup],
    SupFlags = #{strategy => one_for_all,
                 intensity => 0,
                 period => 1
                },
    {ok, {SupFlags, Childs}}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

child_spec(Mod, supervisor) ->
    #{id => Mod,
      start => {Mod, start_link, []},
      restart => permanent,
      shutdown => infinity,
      type => supervisor,
      modules => [Mod]
     };

child_spec(Mod, worker) ->
    #{id => Mod,
      start => {Mod, start_link, []},
      restart => permanent,
      shutdown => 15000,
      type => worker,
      modules => [Mod]
     }.

