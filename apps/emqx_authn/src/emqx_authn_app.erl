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

% -include("emqx_authn.hrl").
% -include_lib("emqx/include/logger.hrl").

-behaviour(application).

%% Application callbacks
-export([ start/2
        , stop/1
        ]).

start(_StartType, _StartArgs) ->
    % ok = ekka_rlog:wait_for_shards([?AUTH_SHARD], infinity),
    {ok, Sup} = emqx_authn_sup:start_link(),
    add_providers(),
    {ok, Sup}.

stop(_State) ->
    ok.

add_providers() ->
    Providers = [ {'password-based:built-in-database', emqx_authn_mnesia}
                , {'password-based:mysql', emqx_authn_mysql}
                , {'password-based:posgresql', emqx_authn_pgsql}
                , {'password-based:mongodb', emqx_authn_mongodb}
                , {'password-based:redis', emqx_authn_redis}
                , {'password-based:http-server', emqx_authn_http}
                , {jwt, emqx_authn_jwt}
                , {'scram:built-in-database', emqx_enhanced_authn_scram_mnesia}
                ],
    [emqx_authentication:add_provider(AuthNType, Provider) || {AuthNType, Provider} <- Providers].

% initialize() ->
%     emqx_config_handler:add_handler([authentication, authenticators], emqx_authn),
%     _ = emqx:hook('client.authenticate', {emqx_authn, authenticate, []}),
%     AuthNConfig = emqx:get_config([authentication], #{authenticators => []}),
%     initialize(AuthNConfig).

% initialize(#{authenticators := AuthenticatorsConfig}) ->
%     {ok, _} = emqx_authn:create_group(?GROUP),
%     initialize_authenticators(AuthenticatorsConfig),
%     ok.

% deinitialize() ->
%     _ = emqx:unhook('client.authenticate', {emqx_authn, authenticate, []}),
%     ok = emqx_authn:delete_group(?GROUP),
%     ok.

% initialize_authenticators([]) ->
%     ok;
% initialize_authenticators([#{name := Name} = AuthenticatorConfig | More]) ->
%     case emqx_authn:create_authenticator(?GROUP, AuthenticatorConfig) of
%         {ok, _} ->
%             initialize_authenticators(More);
%         {error, Reason} ->
%             ?LOG(error, "Failed to create authenticator '~s': ~p", [Name, Reason])
%     end.
