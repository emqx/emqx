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
-module(emqx_data_bridge_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => one_for_one,
                 intensity => 10,
                 period => 10},
    ChildSpecs = [
        #{id => emqx_data_bridge_monitor,
          start => {emqx_data_bridge_monitor, start_link, []},
          restart => permanent,
          type => worker,
          modules => [emqx_data_bridge_monitor]},
        emqx_config_handler:child_spec(emqx_data_bridge_config_handler, config_key_path())
    ],
    {ok, {SupFlags, ChildSpecs}}.

%% internal functions

config_key_path() ->
    [emqx_data_bridge, bridges].
