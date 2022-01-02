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

-export([ start_link/2
        , create_checker/2
        , delete_checker/1
        ]).

-export([health_check/2]).

-define(SUP, emqx_resource_health_check_sup).
-define(ID(NAME), {resource_health_check, NAME}).

child_spec(Name, Sleep) ->
    #{id => ?ID(Name),
      start => {?MODULE, start_link, [Name, Sleep]},
      restart => transient,
      shutdown => 5000, type => worker, modules => [?MODULE]}.

start_link(Name, Sleep) ->
    Pid = proc_lib:spawn_link(?MODULE, health_check, [Name, Sleep]),
    {ok, Pid}.

create_checker(Name, Sleep) ->
    create_checker(Name, Sleep, false).

create_checker(Name, Sleep, Retry) ->
    case supervisor:start_child(?SUP, child_spec(Name, Sleep)) of
        {ok, _} -> ok;
        {error, already_present} -> ok;
        {error, {already_started, _}} when Retry == false ->
            ok = delete_checker(Name),
            create_checker(Name, Sleep, true);
        Error -> Error
    end.

delete_checker(Name) ->
    case supervisor:terminate_child(?SUP, ?ID(Name)) of
        ok -> supervisor:delete_child(?SUP, ?ID(Name));
        Error -> Error
	end.

health_check(Name, SleepTime) ->
    case emqx_resource:health_check(Name) of
        ok ->
            emqx_alarm:deactivate(Name);
        {error, _} ->
            emqx_alarm:activate(Name, #{name => Name},
                <<Name/binary, " health check failed">>)
    end,
    timer:sleep(SleepTime),
    health_check(Name, SleepTime).
