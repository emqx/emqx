%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_bridge_mqtt_sup).
-behaviour(supervisor).

-include("emqx_bridge_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").

-logger_header("[Bridge]").

%% APIs
-export([ start_link/0
        , start_link/1
        ]).

-export([ create_bridge/2
        , drop_bridge/1
        , bridges/0
        , is_bridge_exist/1
        ]).

%% supervisor callbacks
-export([init/1]).

-define(SUP, ?MODULE).
-define(WORKER_SUP, emqx_bridge_worker_sup).

start_link() -> start_link(?SUP).

start_link(Name) ->
    supervisor:start_link({local, Name}, ?MODULE, Name).

init(?SUP) ->
    BridgesConf = application:get_env(?APP, bridges, []),
    BridgeSpec = lists:map(fun bridge_spec/1, BridgesConf),
    SupFlag = #{strategy => one_for_one,
                intensity => 100,
                period => 10},
    {ok, {SupFlag, BridgeSpec}}.

bridge_spec({Name, Config}) ->
    #{id => Name,
      start => {emqx_bridge_worker, start_link, [Name, Config]},
      restart => permanent,
      shutdown => 5000,
      type => worker,
      modules => [emqx_bridge_worker]}.

-spec(bridges() -> [{node(), map()}]).
bridges() ->
    [{Name, emqx_bridge_worker:status(Pid)} || {Name, Pid, _, _} <- supervisor:which_children(?SUP)].

-spec(is_bridge_exist(atom() | pid()) -> boolean()).
is_bridge_exist(Id) ->
    case supervisor:get_childspec(?SUP, Id) of
        {ok, _ChildSpec} -> true;
        {error, _Error} -> false
    end.

create_bridge(Id, Config) ->
    supervisor:start_child(?SUP, bridge_spec({Id, Config})).

drop_bridge(Id) ->
    case supervisor:terminate_child(?SUP, Id) of
        ok ->
            supervisor:delete_child(?SUP, Id);
        {error, Error} ->
            ?LOG(error, "Delete bridge failed, error : ~p", [Error]),
            {error, Error}
    end.
