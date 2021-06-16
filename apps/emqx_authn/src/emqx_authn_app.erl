%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authn_app).

-include("emqx_authn.hrl").

-behaviour(application).

-emqx_plugin(?MODULE).

-include("emqx_authentication.hrl").

%% Application callbacks
-export([ start/2
        , stop/1
        ]).

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_authn_sup:start_link(),
    ok = ekka_rlog:wait_for_shards([?AUTH_SHARD], infinity),
    ok = emqx_authn:register_service_types(),
    recover_from_conf(),
    {ok, Sup}.

stop(_State) ->
    ok.

recover_from_conf() ->
    RawConfig = application:get_all_env(?APP),
    io:format("RawConfig: ~p~n", [RawConfig]),
    {ok, MapConfig} = hocon:binary(jsx:encode(RawConfig), #{format => richmap}),
    Config0 = hocon_schema:check(emqx_authn_schema, MapConfig),
    Config = hocon_schema:richmap_to_map(Config0),
    ok = application:set_env(?APP, chains, Config),
    io:format("Config: ~p~n", [Config]).
