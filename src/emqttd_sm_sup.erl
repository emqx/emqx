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

%% @doc Session Manager Supervisor.

-module(emqttd_sm_sup).

-behaviour(supervisor).

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd.hrl").

-define(HELPER, emqttd_sm_helper).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %% Create session tables
    ets:new(mqtt_local_session, [public, ordered_set, named_table, {write_concurrency, true}]),

    %% Helper
    StatsFun = emqttd_stats:statsfun('sessions/count', 'sessions/max'),
    Helper = {?HELPER, {?HELPER, start_link, [StatsFun]},
              permanent, 5000, worker, [?HELPER]},

    %% SM Pool Sup
    MFA = {emqttd_sm, start_link, []},
    PoolSup = emqttd_pool_sup:spec([emqttd_sm, hash, erlang:system_info(schedulers), MFA]),

    {ok, {{one_for_all, 10, 3600}, [Helper, PoolSup]}}.

