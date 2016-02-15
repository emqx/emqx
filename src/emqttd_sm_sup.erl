%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
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

-include("emqttd.hrl").

-define(SM, emqttd_sm).

-define(HELPER, emqttd_sm_helper).

-define(TABS, [mqtt_transient_session, mqtt_persistent_session]).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %% Create session tables
    create_session_tabs(),

    %% Helper
    StatsFun = emqttd_stats:statsfun('sessions/count', 'sessions/max'),
    Helper = {?HELPER, {?HELPER, start_link, [StatsFun]},
                permanent, 5000, worker, [?HELPER]},

    %% SM Pool Sup
    MFA = {?SM, start_link, []},
    PoolSup = emqttd_pool_sup:spec([?SM, hash, erlang:system_info(schedulers), MFA]),

    {ok, {{one_for_all, 10, 3600}, [Helper, PoolSup]}}.
    
create_session_tabs() ->
    Opts = [ordered_set, named_table, public,
               {write_concurrency, true}],
    [ets:new(Tab, Opts) || Tab <- ?TABS].

