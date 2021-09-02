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
    lists:foreach(fun(AuthNConfig0) ->
                      case ?AUTHN:create_authenticator(?GLOBAL, AuthNConfig0) of
                          {ok, _} ->
                            ok;
                          {error, Reason} ->
                            ?LOG(error, "Failed to create authenticator '~s': ~p", [?AUTHN:generate_id(AuthNConfig0), Reason])
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
do_pre_config_update({delete_authenticator, _ChainName, AuthenticatorID}, OldConfig) ->
    NewConfig = lists:filter(fun(OldConfig0) ->
                                AuthenticatorID =/= ?AUTHN:generate_id(OldConfig0)
                             end, OldConfig),
    {ok, NewConfig};
do_pre_config_update({update_authenticator, _ChainName, AuthenticatorID, Config}, OldConfig) ->
    NewConfig = lists:map(fun(OldConfig0) ->
                              case AuthenticatorID =:= ?AUTHN:generate_id(OldConfig0) of
                                  true -> maps:merge(OldConfig0, Config);
                                  false -> OldConfig0
                              end
                          end, OldConfig),
    {ok, NewConfig};
do_pre_config_update({move_authenticator, _ChainName, AuthenticatorID, Position}, OldConfig) ->
    case split_by_id(AuthenticatorID, OldConfig) of
        {error, Reason} -> {error, Reason};
        {ok, Part1, [Found | Part2]} ->
            case Position of
                <<"top">> ->
                    {ok, [Found | Part1] ++ Part2};
                <<"bottom">> ->
                    {ok, Part1 ++ Part2 ++ [Found]};
                <<"before:", Before/binary>> ->
                    case split_by_id(Before, Part1 ++ Part2) of
                        {error, Reason} ->
                            {error, Reason};
                        {ok, NPart1, [NFound | NPart2]} ->
                            {ok, NPart1 ++ [Found, NFound | NPart2]}
                    end;
                _ ->
                    {error, {invalid_parameter, position}}
            end
    end.

post_config_update(UpdateReq, NewConfig, OldConfig, AppEnvs) ->
    do_post_config_update(UpdateReq, check_config(to_list(NewConfig)), OldConfig, AppEnvs).

do_post_config_update({create_authenticator, ChainName, Config}, _NewConfig, _OldConfig, _AppEnvs) ->
    NConfig = check_config(Config),
    _ = ?AUTHN:create_chain(ChainName),
    ?AUTHN:create_authenticator(ChainName, NConfig);

do_post_config_update({delete_authenticator, ChainName, AuthenticatorID}, _NewConfig, _OldConfig, _AppEnvs) ->
    ?AUTHN:delete_authenticator(ChainName, AuthenticatorID);

do_post_config_update({update_authenticator, ChainName, AuthenticatorID, _Config}, NewConfig, _OldConfig, _AppEnvs) ->
    [Config] = lists:filter(fun(NewConfig0) ->
                                AuthenticatorID =:= ?AUTHN:generate_id(NewConfig0)
                            end, NewConfig),
    NConfig = check_config(Config),
    ?AUTHN:update_authenticator(ChainName, AuthenticatorID, NConfig);

do_post_config_update({move_authenticator, ChainName, AuthenticatorID, Position}, _NewConfig, _OldConfig, _AppEnvs) ->
    NPosition = case Position of
                    <<"top">> -> top;
                    <<"bottom">> -> bottom;
                    <<"before:", Before/binary>> ->
                        {before, Before}
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

split_by_id(ID, AuthenticatorsConfig) ->
    case lists:foldl(
             fun(C, {P1, P2, F0}) ->
                 F = case ID =:= ?AUTHN:generate_id(C) of
                         true -> true;
                         false -> F0
                     end,
                 case F of
                     false -> {[C | P1], P2, F};
                     true -> {P1, [C | P2], F}
                 end
             end, {[], [], false}, AuthenticatorsConfig) of
        {_, _, false} ->
            {error, {not_found, {authenticator, ID}}};
        {Part1, Part2, true} ->
            {ok, lists:reverse(Part1), lists:reverse(Part2)}
    end.