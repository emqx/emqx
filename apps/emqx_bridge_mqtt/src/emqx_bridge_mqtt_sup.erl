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

-module(emqx_bridge_mqtt_sup).
-behaviour(supervisor).

-include("emqx_bridge_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").

-logger_header("[Bridge]").

%% APIs
-export([ start_link/0
        ]).

-export([ create_bridge/1
        , drop_bridge/1
        , bridges/0
        ]).

%% supervisor callbacks
-export([init/1]).

-define(WORKER_SUP, emqx_bridge_worker_sup).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    BridgesConf = emqx_config:get([?APP, bridges], []),
    BridgeSpec = lists:map(fun bridge_spec/1, BridgesConf),
    SupFlag = #{strategy => one_for_one,
                intensity => 100,
                period => 10},
    {ok, {SupFlag, BridgeSpec}}.

bridge_spec(Config) ->
    Name = list_to_atom(maps:get(name, Config)),
    #{id => Name,
      start => {emqx_bridge_worker, start_link, [Config]},
      restart => permanent,
      shutdown => 5000,
      type => worker,
      modules => [emqx_bridge_worker]}.

-spec(bridges() -> [{node(), map()}]).
bridges() ->
    [{Name, emqx_bridge_worker:status(Name)} || {Name, _Pid, _, _} <- supervisor:which_children(?MODULE)].

create_bridge(Config) ->
    supervisor:start_child(?MODULE, bridge_spec(Config)).

drop_bridge(Name) ->
    case supervisor:terminate_child(?MODULE, Name) of
        ok ->
            supervisor:delete_child(?MODULE, Name);
        {error, Error} ->
            ?LOG(error, "Delete bridge failed, error : ~p", [Error]),
            {error, Error}
    end.
