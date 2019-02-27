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

-module(emqx_portal_sup).
-behavior(supervisor).

-include("logger.hrl").

-export([start_link/0, start_link/1, portals/0]).
-export([create_portal/2, drop_portal/1]).
-export([init/1]).

-define(SUP, ?MODULE).
-define(WORKER_SUP, emqx_portal_worker_sup).

start_link() -> start_link(?SUP).

start_link(Name) ->
    supervisor:start_link({local, Name}, ?MODULE, Name).

init(?SUP) ->
    BridgesConf = emqx_config:get_env(bridges, []),
    BridgeSpec = lists:map(fun portal_spec/1, BridgesConf),
    SupFlag = #{strategy => one_for_one,
                intensity => 100,
                period => 10},
    {ok, {SupFlag, BridgeSpec}}.

portal_spec({Name, Config}) ->
    #{id => Name,
      start => {emqx_portal, start_link, [Name, Config]},
      restart => permanent,
      shutdown => 5000,
      type => worker,
      modules => [emqx_portal]}.

-spec(portals() -> [{node(), map()}]).
portals() ->
    [{Name, emqx_portal:status(Pid)} || {Name, Pid, _, _} <- supervisor:which_children(?SUP)].

create_portal(Id, Config) ->
    supervisor:start_child(?SUP, portal_spec({Id, Config})).

drop_portal(Id) ->
    case supervisor:terminate_child(?SUP, Id) of
        ok ->
            supervisor:delete_child(?SUP, Id);
        Error ->
            ?LOG(error, "[Bridge] Delete bridge failed", [Error]),
            Error
    end.
