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

-module(emqx_dashboard_app).

-behaviour(application).

-export([ start/2
        , stop/1
        ]).

-include("emqx_dashboard.hrl").

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_dashboard_sup:start_link(),
    ok = mria_rlog:wait_for_shards([?DASHBOARD_SHARD], infinity),
    _ = emqx_dashboard:start_listeners(),
    emqx_dashboard_cli:load(),
    {ok, _Result} = emqx_dashboard_admin:add_default_user(),
    {ok, Sup}.

stop(_State) ->
    emqx_dashboard_cli:unload(),
    emqx_dashboard:stop_listeners().
