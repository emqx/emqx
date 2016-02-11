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

%% @doc Client Manager Supervisor.
-module(emqttd_cm_sup).

-behaviour(supervisor).

-include("emqttd.hrl").

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(CM, emqttd_cm).

-define(TAB, mqtt_client).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %% Create client table
    create_client_tab(),

    %% CM Pool Sup
    MFA = {?CM, start_link, [emqttd_stats:statsfun('clients/count', 'clients/max')]},
    PoolSup = emqttd_pool_sup:spec([?CM, hash, erlang:system_info(schedulers), MFA]),

    {ok, {{one_for_all, 10, 3600}, [PoolSup]}}.

create_client_tab() ->
    case ets:info(?TAB, name) of
        undefined ->
            ets:new(?TAB, [ordered_set, named_table, public,
                           {keypos, 2}, {write_concurrency, true}]);
        _ ->
            ok
    end.

