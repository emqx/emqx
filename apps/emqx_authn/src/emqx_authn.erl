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

-module(emqx_authn).

-behaviour(emqx_config_handler).

-include("emqx_authn.hrl").
-include_lib("emqx/include/logger.hrl").

-export([ initialize/0 ]).

-export([ pre_config_update/2
        , post_config_update/3
        , update_config/2
        ]).

-define(AUTHN, emqx_authentication).

initialize() ->
    AuthNConfig = emqx:get_raw_config([authentication, authenticators], []),
    initialize(check_config2(AuthNConfig)).

initialize(AuthNConfig) ->
    {ok, _} = ?AUTHN:create_chain(?GLOBAL),
    lists:foreach(fun(#{name := Name} = AuthNConfig0) ->
                      case ?AUTHN:create_authenticator(?GLOBAL, AuthNConfig0) of
                          {ok, _} ->
                            ok;
                          {error, Reason} ->
                            ?LOG(error, "Failed to create authenticator '~s': ~p", [Name, Reason])
                      end
                  end, AuthNConfig),
    ok.

pre_config_update({create_authenticator, _ChainName, Config}, OldConfig) ->
    NewConfig = check_config(OldConfig ++ [Config]),
    {ok, NewConfig};
pre_config_update({delete_authenticator, ChainName, AuthenticatorID}, OldConfig) ->
    case ?AUTHN:lookup_authenticator(ChainName, AuthenticatorID) of
        {error, {not_found, _}} -> {error, not_found};
        {error, Reason} -> {error, Reason};
        {ok, #{name := Name}} ->
            NewConfig = lists:filter(fun(#{<<"name">> := N}) ->
                                         N =/= Name
                                     end, OldConfig),
            {ok, check_config(NewConfig)}
    end;
pre_config_update({update_or_create_authenticator, ChainName, AuthenticatorID, Config}, OldConfig) ->
    NewConfig = case ?AUTHN:lookup_authenticator(ChainName, AuthenticatorID) of
                    {error, _Reason} -> OldConfig ++ [Config];
                    {ok, #{name := Name}} ->
                        lists:map(fun(#{<<"name">> := N} = C) ->
                                      case N =:= Name of
                                          true -> Config;
                                          false -> C
                                      end
                                  end, OldConfig)
                end,
    {ok, check_config(NewConfig)};
pre_config_update({update_authenticator, ChainName, AuthenticatorID, Config}, OldConfig) ->
    case ?AUTHN:lookup_authenticator(ChainName, AuthenticatorID) of
        {error, {not_found, _}} -> {error, not_found};
        {error, Reason} -> {error, Reason};
        {ok, #{name := Name}} ->
            NewConfig = lists:map(fun(#{<<"name">> := N} = C) ->
                                      case N =:= Name of
                                          true -> maps:merge(C, Config);
                                          false -> C
                                      end
                                  end, OldConfig),
            {ok, check_config(NewConfig)}
    end.

post_config_update({create_authenticator, ChainName, #{<<"name">> := Name}}, NewConfig, _OldConfig) ->
    case lists:filter(
             fun(#{name := N}) ->
                 N =:= Name
             end, NewConfig) of
        [Config] ->
            _ = ?AUTHN:create_chain(ChainName),
            ?AUTHN:create_authenticator(ChainName, Config);
        [_Config | _] ->
            {error, name_has_be_used}
    end;
post_config_update({delete_authenticator, ChainName, AuthenticatorID}, _NewConfig, _OldConfig) ->
    ?AUTHN:delete_authenticator(ChainName, AuthenticatorID);
post_config_update({update_or_create_authenticator, ChainName, AuthenticatorID, #{<<"name">> := Name}}, NewConfig, _OldConfig) ->
    case lists:filter(
             fun(#{name := N}) ->
                 N =:= Name
             end, NewConfig) of
        [Config] ->
            _ = ?AUTHN:create_chain(ChainName),
            ?AUTHN:update_or_create_authenticator(ChainName, AuthenticatorID, Config);
        [_Config | _] ->
            {error, name_has_be_used}
    end;
post_config_update({update_authenticator, ChainName, AuthenticatorID, _Config}, NewConfig, _OldConfig) ->
    {ok, #{name := Name}} = ?AUTHN:lookup_authenticator(ChainName, AuthenticatorID),
    case lists:filter(
             fun(#{name := N}) ->
                 N =:= Name
             end, NewConfig) of
        [Config] ->
            ?AUTHN:update_authenticator(ChainName, AuthenticatorID, Config);
        [_Config | _] ->
            {error, name_has_be_used}
    end.

update_config(Path, ConfigRequest) ->
    emqx:update_config(Path, ConfigRequest, #{rawconf_with_defaults => true}).

check_config(Config) ->
    #{<<"authentication">> := #{<<"authenticators">> := AuthenticatorsConfig}} = hocon_schema:check_plain(emqx_authn_schema, #{<<"authentication">> => #{<<"authenticators">> => Config}}, #{nullable => true}),
    AuthenticatorsConfig.

check_config2(Config) ->
    #{authentication := #{authenticators := AuthenticatorsConfig}} = hocon_schema:check_plain(emqx_authn_schema,
        #{<<"authentication">> => #{<<"authenticators">> => Config}}, #{nullable => true, atom_key => true}),
    AuthenticatorsConfig.

%     Config = [#{<<"name">> => <<"authenticator 3">>,
%                         <<"type">> => <<"password-based:built-in-database">>,
%                         <<"user_id_type">> => <<"username">>}].

% emqx_config:fill_defaults(emqx_authn_schema, #{<<"authentication">> => #{<<"authenticators">> => Config}}).


% hocon_schema:check_plain(emqx_authn_schema, #{<<"authentication">> => #{<<"authenticators">> => Config}}, #{nullable => true}).


% emqx_config:check_config(emqx_authn_schema, #{<<"authentication">> => #{<<"authenticators">> => Config}}).