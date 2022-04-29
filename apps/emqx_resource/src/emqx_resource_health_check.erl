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
-module(emqx_resource_health_check).

-include_lib("emqx/include/logger.hrl").

-export([
    start_link/3,
    create_checker/3,
    delete_checker/1
]).

-export([
    start_health_check/3,
    health_check_timeout_checker/4
]).

-define(SUP, emqx_resource_health_check_sup).
-define(ID(NAME), {resource_health_check, NAME}).

child_spec(Name, Sleep, Timeout) ->
    #{
        id => ?ID(Name),
        start => {?MODULE, start_link, [Name, Sleep, Timeout]},
        restart => transient,
        shutdown => 5000,
        type => worker,
        modules => [?MODULE]
    }.

start_link(Name, Sleep, Timeout) ->
    Pid = proc_lib:spawn_link(?MODULE, start_health_check, [Name, Sleep, Timeout]),
    {ok, Pid}.

create_checker(Name, Sleep, Timeout) ->
    create_checker(Name, Sleep, false, Timeout).

create_checker(Name, Sleep, Retry, Timeout) ->
    case supervisor:start_child(?SUP, child_spec(Name, Sleep, Timeout)) of
        {ok, _} ->
            ok;
        {error, already_present} ->
            ok;
        {error, {already_started, _}} when Retry == false ->
            ok = delete_checker(Name),
            create_checker(Name, Sleep, true, Timeout);
        Error ->
            Error
    end.

delete_checker(Name) ->
    case supervisor:terminate_child(?SUP, ?ID(Name)) of
        ok -> supervisor:delete_child(?SUP, ?ID(Name));
        Error -> Error
    end.

start_health_check(Name, Sleep, Timeout) ->
    Pid = self(),
    _ = proc_lib:spawn_link(?MODULE, health_check_timeout_checker, [Pid, Name, Sleep, Timeout]),
    health_check(Name).

health_check(Name) ->
    receive
        {Pid, begin_health_check} ->
            case emqx_resource:health_check(Name) of
                ok ->
                    emqx_alarm:deactivate(Name);
                {error, _} ->
                    emqx_alarm:activate(
                        Name,
                        #{name => Name},
                        <<Name/binary, " health check failed">>
                    )
            end,
            Pid ! health_check_finish
    end,
    health_check(Name).

health_check_timeout_checker(Pid, Name, SleepTime, Timeout) ->
    SelfPid = self(),
    Pid ! {SelfPid, begin_health_check},
    receive
        health_check_finish -> timer:sleep(SleepTime)
    after Timeout ->
        emqx_alarm:activate(
            Name,
            #{name => Name},
            <<Name/binary, " health check timeout">>
        ),
        ?SLOG(
            error,
            #{
                msg => "health check failed: timeout",
                name => Name
            }
        ),
        _ = emqx_resource_instance:set_resource_status(Name, disconnected),
        receive
            health_check_finish -> timer:sleep(SleepTime)
        end
    end,
    health_check_timeout_checker(Pid, Name, SleepTime, Timeout).
