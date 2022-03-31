%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Application callbacks
-export([
    start/2,
    stop/1
]).

-include_lib("emqx/include/emqx_authentication.hrl").

-dialyzer({nowarn_function, [start/2]}).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

start(_StartType, _StartArgs) ->
    ok = mria_rlog:wait_for_shards([?AUTH_SHARD], infinity),
    {ok, Sup} = emqx_authn_sup:start_link(),
    ok = initialize(),
    {ok, Sup}.

stop(_State) ->
    ok = deinitialize(),
    ok.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

initialize() ->
    ok = ?AUTHN:register_providers(emqx_authn:providers()),

    lists:foreach(
        fun({ChainName, RawAuthConfigs}) ->
            AuthConfig = emqx_authn:check_configs(RawAuthConfigs),
            ?AUTHN:initialize_authentication(
                ChainName,
                AuthConfig
            )
        end,
        chain_configs()
    ).

deinitialize() ->
    ok = ?AUTHN:deregister_providers(provider_types()),
    ok = emqx_authn_utils:cleanup_resources().

chain_configs() ->
    [global_chain_config() | listener_chain_configs()].

global_chain_config() ->
    {?GLOBAL, emqx:get_raw_config([?EMQX_AUTHENTICATION_CONFIG_ROOT_NAME_BINARY], [])}.

listener_chain_configs() ->
    lists:map(
        fun({ListenerID, _}) ->
            {ListenerID, emqx:get_raw_config(auth_config_path(ListenerID), [])}
        end,
        emqx_listeners:list()
    ).

auth_config_path(ListenerID) ->
    [<<"listeners">>] ++
        binary:split(atom_to_binary(ListenerID), <<":">>) ++
        [?EMQX_AUTHENTICATION_CONFIG_ROOT_NAME_BINARY].

provider_types() ->
    lists:map(fun({Type, _Module}) -> Type end, emqx_authn:providers()).
