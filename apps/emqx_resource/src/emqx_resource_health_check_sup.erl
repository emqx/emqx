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
-module(emqx_resource_health_check_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1,
         create_checker/2,
         delete_checker/1]).

-define(HEALTH_CHECK_MOD, emqx_resource_health_check).
-define(ID(NAME), {resource_health_check, NAME}).

child_spec(Name, Sleep) ->
    #{id => ?ID(Name),
      start => {?HEALTH_CHECK_MOD, start_link, [Name, Sleep]},
      restart => transient,
      shutdown => 5000, type => worker, modules => [?HEALTH_CHECK_MOD]}.

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => one_for_one, intensity => 10, period => 10},
    {ok, {SupFlags, []}}.

create_checker(Name, Sleep) ->
    case supervisor:start_child(?MODULE, child_spec(Name, Sleep)) of
        {ok, _} -> ok;
        {error, already_present} -> ok;
        {error, {already_started, _}} -> ok;
        Error -> Error
    end.

delete_checker(Name) ->
    case supervisor:terminate_child(?MODULE, {health_check, Name}) of
        ok ->
            case supervisor:delete_child(?MODULE, {health_check, Name}) of
                {error, not_found} -> ok;
                Error -> Error
            end;
        {error, not_found} -> ok;
        Error -> Error
	end.
