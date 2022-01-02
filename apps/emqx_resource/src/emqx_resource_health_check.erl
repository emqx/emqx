%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_resource_health_check).

-export([start_link/2]).

-export([health_check/2]).

start_link(Name, Sleep) ->
    Pid = proc_lib:spawn_link(?MODULE, health_check, [Name, Sleep]),
    {ok, Pid}.

health_check(Name, SleepTime) ->
    timer:sleep(SleepTime),
    case emqx_resource:health_check(Name) of
        ok ->
            emqx_alarm:deactivate(Name);
        {error, _} ->
            emqx_alarm:activate(Name, #{name => Name},
                <<Name/binary, " health check failed">>)
    end,
    health_check(Name, SleepTime).
