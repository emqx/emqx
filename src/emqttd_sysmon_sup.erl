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

-module(emqttd_sysmon_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Sysmon = {sysmon, {emqttd_sysmon, start_link, [opts()]},
                permanent, 5000, worker, [emqttd_sysmon]},
    {ok, {{one_for_one, 10, 100}, [Sysmon]}}.

opts() ->
    Opts = [{long_gc,        config(sysmon_long_gc)},
            {long_schedule,  config(sysmon_long_schedule)},
            {large_heap,     config(sysmon_large_heap)},
            {busy_port,      config(busy_port)},
            {busy_dist_port, config(sysmon_busy_dist_port)}],
    [{Key, Val} || {Key, {ok, Val}} <- Opts].

config(Key) -> gen_conf:value(emqttd, Key).

