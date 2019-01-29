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

-module(emqx_bridge_sup).

-behavior(supervisor).

-include("emqx.hrl").

-export([start_link/0, bridges/0]).

%% Supervisor callbacks
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @doc List all bridges
-spec(bridges() -> [{node(), map()}]).
bridges() ->
    [{Name, emqx_bridge:status(Pid)} || {Name, Pid, _, _} <- supervisor:which_children(?MODULE)].

init([]) ->
    BridgesOpts = emqx_config:get_env(bridges, []),
    Bridges = [spec(Opts)|| Opts <- BridgesOpts],
    {ok, {{one_for_one, 10, 100}, Bridges}}.

spec({Id, Options})->
    #{id       => Id,
      start    => {emqx_bridge, start_link, [Id, Options]},
      restart  => permanent,
      shutdown => 5000,
      type     => worker,
      modules  => [emqx_bridge]}.
