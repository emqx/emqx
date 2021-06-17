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
    % ok = emqx_authn:register_service_types(),
    % generate_config(),
    initialize(),
    {ok, Sup}.

stop(_State) ->
    ok.

% generate_config() ->
%     ConfFile = filename:join([emqx:get_env(plugins_etc_dir), ?APP]) ++ ".conf",
%     {ok, RawConfig} = hocon:load(ConfFile),
%     #{authn := Config} = hocon_schema:check_plain(emqx_authn_schema, RawConfig, #{atom_key => true}),
%     ok = application:set_env(?APP, authn, Config).

initialize() ->
    ConfFile = filename:join([emqx:get_env(plugins_etc_dir), ?APP]) ++ ".conf",
    {ok, RawConfig} = hocon:load(ConfFile),
    #{authn := #{chains := Chains}} = hocon_schema:check_plain(emqx_authn_schema, RawConfig, #{atom_key => true}),
    io:format("Chains: ~p~n", [Chains]),
    ok.
%     initialize_chains(Chains).

% initialize_chains([]) ->
%     ok;
% initialize_chains([#{id := ChainID, providers := Providers} | More]) ->
%     {ok, _} = emqx_authn:create_chain(#{id => ChainID}),
%     {ok, _} = emqx_authn:add_providers(ChainID, Providers),
%     initialize_chains(More).

