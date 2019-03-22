%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_sup).

-behaviour(supervisor).

-export([ start_link/0
        , start_child/1
        , start_child/2
        , stop_child/1
        ]).

-export([init/1]).

-type(startchild_ret() :: {ok, supervisor:child()}
                        | {ok, supervisor:child(), term()}
                        | {error, term()}).

-define(SUPERVISOR, ?MODULE).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link() ->
    supervisor:start_link({local, ?SUPERVISOR}, ?MODULE, []).

-spec(start_child(supervisor:child_spec()) -> startchild_ret()).
start_child(ChildSpec) when is_tuple(ChildSpec) ->
    supervisor:start_child(?SUPERVISOR, ChildSpec).

-spec(start_child(module(), worker | supervisor) -> startchild_ret()).
start_child(Mod, worker) ->
    start_child(worker_spec(Mod));
start_child(Mod, supervisor) ->
    start_child(supervisor_spec(Mod)).

-spec(stop_child(supervisor:child_id()) -> ok | {error, term()}).
stop_child(ChildId) ->
    case supervisor:terminate_child(?SUPERVISOR, ChildId) of
        ok    -> supervisor:delete_child(?SUPERVISOR, ChildId);
        Error -> Error
    end.

%%--------------------------------------------------------------------
%% Supervisor callbacks
%%--------------------------------------------------------------------

init([]) ->
    %% Kernel Sup
    KernelSup = supervisor_spec(emqx_kernel_sup),
    %% Router Sup
    RouterSup = supervisor_spec(emqx_router_sup),
    %% Broker Sup
    BrokerSup = supervisor_spec(emqx_broker_sup),
    BridgeSup = supervisor_spec(emqx_bridge_sup),
    %% Session Manager
    SMSup = supervisor_spec(emqx_sm_sup),
    %% Connection Manager
    CMSup = supervisor_spec(emqx_cm_sup),
    %% Sys Sup
    SysSup = supervisor_spec(emqx_sys_sup),
    {ok, {{one_for_all, 0, 1},
          [KernelSup,
           RouterSup,
           BrokerSup,
           BridgeSup,
           SMSup,
           CMSup,
           SysSup]}}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

worker_spec(M) ->
    {M, {M, start_link, []}, permanent, 30000, worker, [M]}.
supervisor_spec(M) ->
    {M, {M, start_link, []}, permanent, infinity, supervisor, [M]}.
