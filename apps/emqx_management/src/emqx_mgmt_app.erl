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

-module(emqx_mgmt_app).

-behaviour(application).

-emqx_plugin(?MODULE).

-export([ start/2
        , stop/1
        , start_phase/3
        ]).

start(_Type, _Args) ->
    {ok, Sup} = emqx_mgmt_sup:start_link(),
    _ = emqx_mgmt_auth:add_default_app(),
    emqx_mgmt_http:start_listeners(),
    emqx_mgmt_cli:load(),
    {ok, Sup}.

stop(_State) ->
    ok = cowboy:stop_listener(emqx_api),
    emqx_mgmt_http:stop_listeners().

-spec start_phase(atom(), application:start_type(), []) -> ok | {error, term()}.
start_phase(start_trails_http, _StartType, []) ->
  Port = 18080, %% todo make configurable
  Trails = trails:trails([emqx_api_nodes,
                          cowboy_swagger_handler]),
  trails:store(Trails),
  Dispatch      = trails:single_host_compile(Trails),
  RanchOptions  = [{port, Port}],
  CowboyOptions = #{ env      => #{dispatch => Dispatch}
                   , compress => true
                   , timeout  => 12000
                   },
  {ok, _} = cowboy:start_clear(emqx_api, RanchOptions, CowboyOptions),
  ok.
