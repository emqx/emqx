%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc Authenticator configuration management module.
-module(emqx_authentication_config).

-behaviour(emqx_config_handler).

-export([
    pre_config_update/3,
    post_config_update/5
]).

-export([
    authenticator_id/1,
    authn_type/1
]).

-ifdef(TEST).
-export([convert_certs/2, convert_certs/3, clear_certs/2]).
-endif.

-export_type([config/0]).

-include("logger.hrl").
-include("emqx_authentication.hrl").

-type parsed_config() :: #{
    mechanism := atom(),
    backend => atom(),
    atom() => term()
}.
-type raw_config() :: #{binary() => term()}.
-type config() :: parsed_config() | raw_config().

-type authenticator_id() :: emqx_authentication:authenticator_id().
-type position() :: emqx_authentication:position().
-type chain_name() :: emqx_authentication:chain_name().
-type update_request() ::
    {create_authenticator, chain_name(), map()}
    | {delete_authenticator, chain_name(), authenticator_id()}
    | {update_authenticator, chain_name(), authenticator_id(), map()}
    | {move_authenticator, chain_name(), authenticator_id(), position()}.

%%------------------------------------------------------------------------------
%% Callbacks of config handler
%%------------------------------------------------------------------------------

-spec pre_config_update(list(atom()), update_request(), emqx_config:raw_config()) ->
    {ok, map() | list()} | {error, term()}.
pre_config_update(Paths, UpdateReq, OldConfig) ->
    try do_pre_config_update(Paths, UpdateReq, to_list(OldConfig)) of
        {error, Reason} -> {error, Reason};
        {ok, NewConfig} -> {ok, NewConfig}
    catch
        throw:Reason ->
            {error, Reason}
    end.

do_pre_config_update(_, {create_authenticator, ChainName, Config}, OldConfig) ->
    NewId = authenticator_id(Config),
    case filter_authenticator(NewId, OldConfig) of
        [] ->
            CertsDir = certs_dir(ChainName, Config),
            NConfig = convert_certs(CertsDir, Config),
            {ok, OldConfig ++ [NConfig]};
        [_] ->
            {error, {already_exists, {authenticator, NewId}}}
    end;
do_pre_config_update(_, {delete_authenticator, _ChainName, AuthenticatorID}, OldConfig) ->
    NewConfig = lists:filter(
        fun(OldConfig0) ->
            AuthenticatorID =/= authenticator_id(OldConfig0)
        end,
        OldConfig
    ),
    {ok, NewConfig};
do_pre_config_update(_, {update_authenticator, ChainName, AuthenticatorID, Config}, OldConfig) ->
    CertsDir = certs_dir(ChainName, AuthenticatorID),
    NewConfig = lists:map(
        fun(OldConfig0) ->
            case AuthenticatorID =:= authenticator_id(OldConfig0) of
                true -> convert_certs(CertsDir, Config, OldConfig0);
                false -> OldConfig0
            end
        end,
        OldConfig
    ),
    {ok, NewConfig};
do_pre_config_update(_, {move_authenticator, _ChainName, AuthenticatorID, Position}, OldConfig) ->
    case split_by_id(AuthenticatorID, OldConfig) of
        {error, Reason} ->
            {error, Reason};
        {ok, BeforeFound, [Found | AfterFound]} ->
            case Position of
                ?CMD_MOVE_FRONT ->
                    {ok, [Found | BeforeFound] ++ AfterFound};
                ?CMD_MOVE_REAR ->
                    {ok, BeforeFound ++ AfterFound ++ [Found]};
                ?CMD_MOVE_BEFORE(BeforeRelatedID) ->
                    case split_by_id(BeforeRelatedID, BeforeFound ++ AfterFound) of
                        {error, Reason} ->
                            {error, Reason};
                        {ok, BeforeNFound, [FoundRelated | AfterNFound]} ->
                            {ok, BeforeNFound ++ [Found, FoundRelated | AfterNFound]}
                    end;
                ?CMD_MOVE_AFTER(AfterRelatedID) ->
                    case split_by_id(AfterRelatedID, BeforeFound ++ AfterFound) of
                        {error, Reason} ->
                            {error, Reason};
                        {ok, BeforeNFound, [FoundRelated | AfterNFound]} ->
                            {ok, BeforeNFound ++ [FoundRelated, Found | AfterNFound]}
                    end
            end
    end;
do_pre_config_update(_, OldConfig, OldConfig) ->
    {ok, OldConfig};
do_pre_config_update(Paths, NewConfig, _OldConfig) ->
    ChainName = chain_name(Paths),
    {ok, [
        begin
            CertsDir = certs_dir(ChainName, New),
            convert_certs(CertsDir, New)
        end
     || New <- to_list(NewConfig)
    ]}.

-spec post_config_update(
    list(atom()),
    update_request(),
    map() | list(),
    emqx_config:raw_config(),
    emqx_config:app_envs()
) ->
    ok | {ok, map()} | {error, term()}.
post_config_update(Paths, UpdateReq, NewConfig, OldConfig, AppEnvs) ->
    do_post_config_update(Paths, UpdateReq, to_list(NewConfig), OldConfig, AppEnvs).

do_post_config_update(
    _, {create_authenticator, ChainName, Config}, NewConfig, _OldConfig, _AppEnvs
) ->
    NConfig = get_authenticator_config(authenticator_id(Config), NewConfig),
    emqx_authentication:create_authenticator(ChainName, NConfig);
do_post_config_update(
    _,
    {delete_authenticator, ChainName, AuthenticatorID},
    _NewConfig,
    OldConfig,
    _AppEnvs
) ->
    case emqx_authentication:delete_authenticator(ChainName, AuthenticatorID) of
        ok ->
            Config = get_authenticator_config(AuthenticatorID, to_list(OldConfig)),
            CertsDir = certs_dir(ChainName, AuthenticatorID),
            ok = clear_certs(CertsDir, Config);
        {error, Reason} ->
            {error, Reason}
    end;
do_post_config_update(
    _,
    {update_authenticator, ChainName, AuthenticatorID, Config},
    NewConfig,
    _OldConfig,
    _AppEnvs
) ->
    case get_authenticator_config(authenticator_id(Config), NewConfig) of
        {error, not_found} ->
            {error, {not_found, {authenticator, AuthenticatorID}}};
        NConfig ->
            emqx_authentication:update_authenticator(ChainName, AuthenticatorID, NConfig)
    end;
do_post_config_update(
    _,
    {move_authenticator, ChainName, AuthenticatorID, Position},
    _NewConfig,
    _OldConfig,
    _AppEnvs
) ->
    emqx_authentication:move_authenticator(ChainName, AuthenticatorID, Position);
do_post_config_update(_, _UpdateReq, OldConfig, OldConfig, _AppEnvs) ->
    ok;
do_post_config_update(Paths, _UpdateReq, NewConfig0, OldConfig0, _AppEnvs) ->
    ChainName = chain_name(Paths),
    OldConfig = to_list(OldConfig0),
    NewConfig = to_list(NewConfig0),
    OldIds = lists:map(fun authenticator_id/1, OldConfig),
    NewIds = lists:map(fun authenticator_id/1, NewConfig),
    %% delete authenticators that are not in the new config
    lists:foreach(
        fun(Conf) ->
            Id = authenticator_id(Conf),
            case lists:member(Id, NewIds) of
                true ->
                    ok;
                false ->
                    _ = emqx_authentication:delete_authenticator(ChainName, Id),
                    CertsDir = certs_dir(ChainName, Conf),
                    ok = clear_certs(CertsDir, Conf)
            end
        end,
        OldConfig
    ),
    %% create new authenticators and update existing ones
    lists:foreach(
        fun(Conf) ->
            Id = authenticator_id(Conf),
            case lists:member(Id, OldIds) of
                true ->
                    emqx_authentication:update_authenticator(ChainName, Id, Conf);
                false ->
                    emqx_authentication:create_authenticator(ChainName, Conf)
            end
        end,
        NewConfig
    ).

to_list(undefined) -> [];
to_list(M) when M =:= #{} -> [];
to_list(M) when is_map(M) -> [M];
to_list(L) when is_list(L) -> L.

convert_certs(CertsDir, Config) ->
    case emqx_tls_lib:ensure_ssl_files(CertsDir, maps:get(<<"ssl">>, Config, undefined)) of
        {ok, SSL} ->
            new_ssl_config(Config, SSL);
        {error, Reason} ->
            ?SLOG(error, Reason#{msg => "bad_ssl_config"}),
            throw({bad_ssl_config, Reason})
    end.

convert_certs(CertsDir, NewConfig, OldConfig) ->
    OldSSL = maps:get(<<"ssl">>, OldConfig, undefined),
    NewSSL = maps:get(<<"ssl">>, NewConfig, undefined),
    case emqx_tls_lib:ensure_ssl_files(CertsDir, NewSSL) of
        {ok, NewSSL1} ->
            ok = emqx_tls_lib:delete_ssl_files(CertsDir, NewSSL1, OldSSL),
            new_ssl_config(NewConfig, NewSSL1);
        {error, Reason} ->
            ?SLOG(error, Reason#{msg => "bad_ssl_config"}),
            throw({bad_ssl_config, Reason})
    end.

new_ssl_config(Config, undefined) -> Config;
new_ssl_config(Config, SSL) -> Config#{<<"ssl">> => SSL}.

clear_certs(CertsDir, Config) ->
    OldSSL = maps:get(<<"ssl">>, Config, undefined),
    ok = emqx_tls_lib:delete_ssl_files(CertsDir, undefined, OldSSL).

get_authenticator_config(AuthenticatorID, AuthenticatorsConfig) ->
    case filter_authenticator(AuthenticatorID, AuthenticatorsConfig) of
        [C] -> C;
        [] -> {error, not_found};
        _ -> error({duplicated_authenticator_id, AuthenticatorsConfig})
    end.

filter_authenticator(ID, Authenticators) ->
    lists:filter(fun(A) -> ID =:= authenticator_id(A) end, Authenticators).

split_by_id(ID, AuthenticatorsConfig) ->
    case
        lists:foldl(
            fun(C, {P1, P2, F0}) ->
                F =
                    case ID =:= authenticator_id(C) of
                        true -> true;
                        false -> F0
                    end,
                case F of
                    false -> {[C | P1], P2, F};
                    true -> {P1, [C | P2], F}
                end
            end,
            {[], [], false},
            AuthenticatorsConfig
        )
    of
        {_, _, false} ->
            {error, {not_found, {authenticator, ID}}};
        {Part1, Part2, true} ->
            {ok, lists:reverse(Part1), lists:reverse(Part2)}
    end.

to_bin(B) when is_binary(B) -> B;
to_bin(L) when is_list(L) -> list_to_binary(L);
to_bin(A) when is_atom(A) -> atom_to_binary(A).

%% @doc Make an authenticator ID from authenticator's config.
%% The authenticator config must contain a 'mechanism' key
%% and maybe a 'backend' key.
%% This function works with both parsed (atom keys) and raw (binary keys)
%% configurations.
-spec authenticator_id(config()) -> authenticator_id().
authenticator_id(#{mechanism := Mechanism0, backend := Backend0}) ->
    Mechanism = to_bin(Mechanism0),
    Backend = to_bin(Backend0),
    <<Mechanism/binary, ":", Backend/binary>>;
authenticator_id(#{mechanism := Mechanism}) ->
    to_bin(Mechanism);
authenticator_id(#{<<"mechanism">> := Mechanism, <<"backend">> := Backend}) ->
    <<Mechanism/binary, ":", Backend/binary>>;
authenticator_id(#{<<"mechanism">> := Mechanism}) ->
    to_bin(Mechanism);
authenticator_id(_C) ->
    throw({missing_parameter, #{name => mechanism}}).

%% @doc Make the authentication type.
authn_type(#{mechanism := M, backend := B}) -> {atom(M), atom(B)};
authn_type(#{mechanism := M}) -> atom(M);
authn_type(#{<<"mechanism">> := M, <<"backend">> := B}) -> {atom(M), atom(B)};
authn_type(#{<<"mechanism">> := M}) -> atom(M).

atom(A) when is_atom(A) -> A;
atom(Bin) -> binary_to_existing_atom(Bin, utf8).

%% The relative dir for ssl files.
certs_dir(ChainName, ConfigOrID) ->
    DirName = dir(ChainName, ConfigOrID),
    SubDir = iolist_to_binary(filename:join(["authn", DirName])),
    emqx_misc:safe_filename(SubDir).

dir(ChainName, ID) when is_binary(ID) ->
    emqx_misc:safe_filename(iolist_to_binary([to_bin(ChainName), "-", ID]));
dir(ChainName, Config) when is_map(Config) ->
    dir(ChainName, authenticator_id(Config)).

chain_name([authentication]) ->
    ?GLOBAL;
chain_name([listeners, Type, Name, authentication]) ->
    binary_to_existing_atom(<<(atom_to_binary(Type))/binary, ":", (atom_to_binary(Name))/binary>>).
