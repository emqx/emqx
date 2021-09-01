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
        , post_config_update/4
        , update_config/2
        ]).

-define(AUTHN, emqx_authentication).

initialize() ->
    AuthNConfig = check_config(to_list(emqx:get_raw_config([authentication], []))),
    {ok, _} = ?AUTHN:create_chain(?GLOBAL),
    lists:foreach(fun(#{name := Name} = AuthNConfig0) ->
                      case ?AUTHN:create_authenticator(?GLOBAL, AuthNConfig0) of
                          {ok, _} ->
                            ok;
                          {error, Reason} ->
                            ?LOG(error, "Failed to create authenticator '~s': ~p", [Name, Reason])
                      end
                  end, to_list(AuthNConfig)),
    ok.

pre_config_update(UpdateReq, OldConfig) ->
    case do_pre_config_update(UpdateReq, to_list(OldConfig)) of
        {error, Reason} -> {error, Reason};
        {ok, NewConfig} -> {ok, may_to_map(NewConfig)}
    end.

do_pre_config_update({create_authenticator, _ChainName, Config}, OldConfig) ->
    {ok, OldConfig ++ [Config]};
do_pre_config_update({delete_authenticator, ChainName, AuthenticatorID}, OldConfig) ->
    case ?AUTHN:lookup_authenticator(ChainName, AuthenticatorID) of
        {error, {not_found, _}} -> {error, not_found};
        {error, Reason} -> {error, Reason};
        {ok, #{name := Name}} ->
            NewConfig = lists:filter(fun(#{<<"name">> := N}) ->
                                         N =/= Name
                                     end, OldConfig),
            {ok, NewConfig}
    end;
do_pre_config_update({update_or_create_authenticator, ChainName, AuthenticatorID, Config}, OldConfig) ->
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
    {ok, NewConfig};
do_pre_config_update({update_authenticator, ChainName, AuthenticatorID, Config}, OldConfig) ->
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
            {ok, NewConfig}
    end;
do_pre_config_update({move_authenticator, ChainName, AuthenticatorID, Position}, OldConfig) ->
    case ?AUTHN:lookup_authenticator(ChainName, AuthenticatorID) of
        {error, Reason} -> {error, Reason};
        {ok, #{name := Name}} ->
            {ok, Part1, [Found | Part2]} = split_by_name(Name, OldConfig),
            case Position of
                <<"top">> ->
                    {ok, [Found | Part1] ++ Part2};
                <<"bottom">> ->
                    {ok, Part1 ++ Part2 ++ [Found]};
                Before ->
                    case binary:split(Before, <<":">>, [global]) of
                        [<<"before">>, ID] ->
                            case ?AUTHN:lookup_authenticator(ChainName, ID) of
                                {error, Reason} -> {error, Reason};
                                {ok, #{name := Name1}} ->
                                    {ok, NPart1, [NFound | NPart2]} = split_by_name(Name1, Part1 ++ Part2),
                                    {ok, NPart1 ++ [Found, NFound | NPart2]}
                            end;
                        _ ->
                            {error, {invalid_parameter, position}}
                    end
            end
    end.

post_config_update(UpdateReq, NewConfig, OldConfig, AppEnvs) ->
    do_post_config_update(UpdateReq, check_config(to_list(NewConfig)), OldConfig, AppEnvs).

do_post_config_update({create_authenticator, ChainName, #{<<"name">> := Name}}, NewConfig, _OldConfig, _AppEnvs) ->
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
do_post_config_update({delete_authenticator, ChainName, AuthenticatorID}, _NewConfig, _OldConfig, _AppEnvs) ->
    ?AUTHN:delete_authenticator(ChainName, AuthenticatorID);
do_post_config_update({update_or_create_authenticator, ChainName, AuthenticatorID, #{<<"name">> := Name}}, NewConfig, _OldConfig, _AppEnvs) ->
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
do_post_config_update({update_authenticator, ChainName, AuthenticatorID, _Config}, NewConfig, _OldConfig, _AppEnvs) ->
    {ok, #{name := Name}} = ?AUTHN:lookup_authenticator(ChainName, AuthenticatorID),
    case lists:filter(
             fun(#{name := N}) ->
                 N =:= Name
             end, NewConfig) of
        [Config] ->
            ?AUTHN:update_authenticator(ChainName, AuthenticatorID, Config);
        [_Config | _] ->
            {error, name_has_be_used}
    end;
do_post_config_update({move_authenticator, ChainName, AuthenticatorID, Position}, _NewConfig, _OldConfig, _AppEnvs) ->
    NPosition = case Position of
                    <<"top">> -> top;
                    <<"bottom">> -> bottom;
                    Before ->
                        case binary:split(Before, <<":">>, [global]) of
                            [<<"before">>, ID0] ->
                                {before, ID0};
                            _ ->
                                {error, {invalid_parameter, position}}
                        end
                end,
    ?AUTHN:move_authenticator(ChainName, AuthenticatorID, NPosition).

update_config(Path, ConfigRequest) ->
    emqx:update_config(Path, ConfigRequest, #{rawconf_with_defaults => true}).

check_config(Config) ->
    #{authentication := CheckedConfig} = hocon_schema:check_plain(emqx_authentication,
        #{<<"authentication">> => Config}, #{nullable => true, atom_key => true}),
    CheckedConfig.

may_to_map([L]) ->
    L;
may_to_map(L) ->
    L.

to_list(M) when is_map(M) ->
    [M];
to_list(L) when is_list(L) ->
    L.

split_by_name(Name, Authenticators) ->
    {Part1, Part2, true} = lists:foldl(
             fun(#{<<"name">> := N} = C, {P1, P2, F0}) ->
                 F = case N =:= Name of
                         true -> true;
                         false -> F0
                     end,
                 case F of
                     false -> {[C | P1], P2, F};
                     true -> {P1, [C | P2], F}
                 end
             end, {[], [], false}, Authenticators),
    {ok, lists:reverse(Part1), lists:reverse(Part2)}.