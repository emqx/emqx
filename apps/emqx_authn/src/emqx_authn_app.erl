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
-include_lib("emqx/include/logger.hrl").

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
    initialize(),
    {ok, Sup}.

stop(_State) ->
    ok.

initialize() ->
    ConfFile = filename:join([emqx:get_env(plugins_etc_dir), ?APP]) ++ ".conf",
    {ok, RawConfig} = hocon:load(ConfFile),
    #{authn := #{chains := Chains}} = hocon_schema:check_plain(emqx_authn_schema, RawConfig, #{atom_key => true, nullable => true}),
    io:format("Chains: ~p~n", [Chains]),
    initialize_chains(Chains).

initialize_chains([]) ->
    ok;
initialize_chains([#{id := ChainID, services := Services} | More]) ->
    case emqx_authn:create_chain(#{id => ChainID}) of
        {ok, _} ->
            initialize_services(ChainID, Services),
            initialize_chains(More);
        {error, Reason} ->
            ?LOG(error, "Failed to create chain '~s': ~p", [ChainID, Reason])
    end.

initialize_services(_ChainID, []) ->
    ok;
initialize_services(ChainID, [#{name := Name} = Service | More]) ->
    case emqx_authn:create_service(ChainID, Service) of
        {ok, _} ->
            initialize_services(ChainID, More);
        {error, Reason} ->
            ?LOG(error, "Failed to create service '~s' in chain '~s': ~p", [Name, ChainID, Reason])
    end.
