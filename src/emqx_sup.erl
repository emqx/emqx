%%--------------------------------------------------------------------
%% Copyright (c) 2013-2017 EMQ Enterprise, Inc. (http://emqtt.io)
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

-author("Feng Lee <feng@emqtt.io>").

-export([start_link/0, start_child/1, start_child/2, stop_child/1]).

%% Supervisor callbacks
-export([init/1]).

-type(startchild_ret() :: {ok, supervisor:child()}
                        | {ok, supervisor:child(), term()}
                        | {error, term()}).

-define(SUPERVISOR, ?MODULE).

-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

start_link() ->
    supervisor:start_link({local, ?SUPERVISOR}, ?MODULE, []).

-spec(start_child(atom(), worker | supervisor) -> startchild_ret()).
start_child(Mod, Type) when Type == worker orelse Type == supervisor ->
    start_child(?CHILD(Mod, Type)).

-spec(start_child(supervisor:child_spec()) -> startchild_ret()).
start_child(ChildSpec) when is_tuple(ChildSpec) ->
    supervisor:start_child(?SUPERVISOR, ChildSpec).

-spec(start_child(Mod::atom(), Type :: worker | supervisor) -> {ok, pid()}).
start_child(Mod, Type) when is_atom(Mod) and is_atom(Type) ->
    supervisor:start_child(?MODULE, ?CHILD(Mod, Type)).

-spec(stop_child(supervisor:child_id()) -> ok | {error, any()}).
stop_child(ChildId) ->
    case supervisor:terminate_child(?SUPERVISOR, ChildId) of
        ok    -> supervisor:delete_child(?SUPERVISOR, ChildId);
        Error -> Error
    end.

%%--------------------------------------------------------------------
%% Supervisor callbacks
%%--------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 0, 1},
          [?CHILD(emqx_ctl, worker),
           ?CHILD(emqx_hooks, worker),
           ?CHILD(emqx_router, worker),
           ?CHILD(emqx_pubsub_sup, supervisor),
           ?CHILD(emqx_stats, worker),
           ?CHILD(emqx_metrics, worker),
           ?CHILD(emqx_pooler, supervisor),
           ?CHILD(emqx_trace_sup, supervisor),
           ?CHILD(emqx_cm_sup, supervisor),
           ?CHILD(emqx_sm_sup, supervisor),
           ?CHILD(emqx_session_sup, supervisor),
           ?CHILD(emqx_ws_client_sup, supervisor),
           ?CHILD(emqx_broker, worker),
           ?CHILD(emqx_alarm, worker),
           ?CHILD(emqx_mod_sup, supervisor),
           ?CHILD(emqx_bridge_sup_sup, supervisor),
           ?CHILD(emqx_access_control, worker),
           ?CHILD(emqx_sysmon_sup, supervisor)]
         }}.

