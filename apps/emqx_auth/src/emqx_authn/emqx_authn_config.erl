%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_authn_config).

-behaviour(emqx_config_handler).

-export([
    pre_config_update/3,
    post_config_update/5,
    propagated_pre_config_update/3,
    propagated_post_config_update/5
]).

-export([
    authenticator_id/1,
    authn_type/1
]).

%% Used in emqx_gateway
-export([
    certs_dir/2,
    convert_certs/2
]).

-export_type([config/0]).

-include_lib("emqx/include/logger.hrl").
-include("emqx_authn_chains.hrl").

-type parsed_config() :: #{
    mechanism := atom(),
    backend => atom(),
    atom() => term()
}.
-type raw_config() :: #{binary() => term()}.
-type config() :: parsed_config() | raw_config().

-type authenticator_id() :: emqx_authn_chains:authenticator_id().
-type position() :: emqx_authn_chains:position().
-type chain_name() :: emqx_authn_chains:chain_name().
-type update_request() ::
    {create_authenticator, chain_name(), map()}
    | {delete_authenticator, chain_name(), authenticator_id()}
    | {update_authenticator, chain_name(), authenticator_id(), map()}
    | {move_authenticator, chain_name(), authenticator_id(), position()}
    | {merge_authenticators, map()}
    | map().

%%------------------------------------------------------------------------------
%% Callbacks of config handler
%%------------------------------------------------------------------------------

-spec pre_config_update(list(atom()), update_request(), emqx_config:raw_config()) ->
    {ok, map() | list()} | {error, term()}.
pre_config_update(ConfPath, UpdateReq, OldConfig) ->
    try do_pre_config_update(ConfPath, UpdateReq, to_list(OldConfig)) of
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
                true -> convert_certs(CertsDir, Config);
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
do_pre_config_update(ConfPath, {merge_authenticators, NewConfig}, OldConfig) ->
    MergeConfig = merge_authenticators(OldConfig, NewConfig),
    do_pre_config_update(ConfPath, MergeConfig, OldConfig);
do_pre_config_update(_ConfPath, {reorder_authenticators, NewOrder}, OldConfig) ->
    reorder_authenticators(NewOrder, OldConfig);
do_pre_config_update(_, OldConfig, OldConfig) ->
    {ok, OldConfig};
do_pre_config_update(ConfPath, NewConfig, _OldConfig) ->
    convert_certs_for_conf_path(ConfPath, NewConfig).

%% @doc Handle listener config changes made at higher level.

-spec propagated_pre_config_update(list(binary()), update_request(), emqx_config:raw_config()) ->
    {ok, map() | list()} | {error, term()}.
propagated_pre_config_update(_, OldConfig, OldConfig) ->
    {ok, OldConfig};
propagated_pre_config_update(ConfPath, NewConfig, _OldConfig) ->
    convert_certs_for_conf_path(ConfPath, NewConfig).

-spec post_config_update(
    list(atom()),
    update_request(),
    map() | list() | undefined,
    emqx_config:raw_config(),
    emqx_config:app_envs()
) ->
    ok | {ok, map()} | {error, term()}.
post_config_update(ConfPath, UpdateReq, NewConfig, OldConfig, AppEnvs) ->
    do_post_config_update(ConfPath, UpdateReq, to_list(NewConfig), OldConfig, AppEnvs).

do_post_config_update(
    _, {create_authenticator, ChainName, Config}, NewConfig, _OldConfig, _AppEnvs
) ->
    NConfig = get_authenticator_config(authenticator_id(Config), NewConfig),
    emqx_authn_chains:create_authenticator(ChainName, NConfig);
do_post_config_update(
    _,
    {delete_authenticator, ChainName, AuthenticatorID},
    _NewConfig,
    _OldConfig,
    _AppEnvs
) ->
    emqx_authn_chains:delete_authenticator(ChainName, AuthenticatorID);
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
            emqx_authn_chains:update_authenticator(ChainName, AuthenticatorID, NConfig)
    end;
do_post_config_update(
    _,
    {move_authenticator, ChainName, AuthenticatorID, Position},
    _NewConfig,
    _OldConfig,
    _AppEnvs
) ->
    emqx_authn_chains:move_authenticator(ChainName, AuthenticatorID, Position);
do_post_config_update(
    ConfPath,
    {reorder_authenticators, NewOrder},
    _NewConfig,
    _OldConfig,
    _AppEnvs
) ->
    ChainName = chain_name(ConfPath),
    ok = emqx_authn_chains:reorder_authenticator(ChainName, NewOrder);
do_post_config_update(_, _UpdateReq, OldConfig, OldConfig, _AppEnvs) ->
    ok;
do_post_config_update(ConfPath, _UpdateReq, NewConfig0, OldConfig0, _AppEnvs) ->
    ChainName = chain_name(ConfPath),
    OldConfig = to_list(OldConfig0),
    NewConfig = to_list(NewConfig0),
    OldIds = lists:map(fun authenticator_id/1, OldConfig),
    NewIds = lists:map(fun authenticator_id/1, NewConfig),
    ok = delete_authenticators(NewIds, ChainName, OldConfig),
    ok = create_or_update_authenticators(OldIds, ChainName, NewConfig),
    ok = emqx_authn_chains:reorder_authenticator(ChainName, NewIds),
    ok.

%% @doc Handle listener config changes made at higher level.

-spec propagated_post_config_update(
    list(atom()),
    update_request(),
    map() | list() | undefined,
    emqx_config:raw_config(),
    emqx_config:app_envs()
) ->
    ok.
propagated_post_config_update(ConfPath, UpdateReq, NewConfig, OldConfig, AppEnvs) ->
    ok = post_config_update(ConfPath, UpdateReq, NewConfig, OldConfig, AppEnvs),
    ok.

%% create new authenticators and update existing ones
create_or_update_authenticators(OldIds, ChainName, NewConfig) ->
    lists:foreach(
        fun(Conf) ->
            Id = authenticator_id(Conf),
            case lists:member(Id, OldIds) of
                true ->
                    {ok, _} = emqx_authn_chains:update_authenticator(ChainName, Id, Conf);
                false ->
                    {ok, _} = emqx_authn_chains:create_authenticator(ChainName, Conf)
            end
        end,
        NewConfig
    ).

%% delete authenticators that are not in the new config
delete_authenticators(NewIds, ChainName, OldConfig) ->
    lists:foreach(
        fun(Conf) ->
            Id = authenticator_id(Conf),
            case lists:member(Id, NewIds) of
                true ->
                    ok;
                false ->
                    ok = emqx_authn_chains:delete_authenticator(ChainName, Id)
            end
        end,
        OldConfig
    ).

to_list(undefined) -> [];
to_list(M) when M =:= #{} -> [];
to_list(M) when is_map(M) -> [M];
to_list(L) when is_list(L) -> L.

convert_certs_for_conf_path(ConfPath, NewConfig) ->
    ChainName = chain_name_for_filepath(ConfPath),
    CovertedConfs = lists:map(
        fun(Conf) ->
            CertsDir = certs_dir(ChainName, Conf),
            convert_certs(CertsDir, Conf)
        end,
        to_list(NewConfig)
    ),
    {ok, CovertedConfs}.

convert_certs(CertsDir, NewConfig) ->
    NewSSL = maps:get(<<"ssl">>, NewConfig, undefined),
    case emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir(CertsDir, NewSSL) of
        {ok, NewSSL1} ->
            new_ssl_config(NewConfig, NewSSL1);
        {error, Reason} ->
            ?SLOG(error, Reason#{msg => "bad_ssl_config"}),
            throw({bad_ssl_config, Reason})
    end.

new_ssl_config(Config, undefined) -> Config;
new_ssl_config(Config, SSL) -> Config#{<<"ssl">> => SSL}.

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
    emqx_utils:safe_filename(SubDir).

dir(ChainName, ID) when is_binary(ID) ->
    emqx_utils:safe_filename(iolist_to_binary([to_bin(ChainName), "-", ID]));
dir(ChainName, Config) when is_map(Config) ->
    dir(ChainName, authenticator_id(Config)).

chain_name([authentication]) ->
    ?GLOBAL;
chain_name([listeners, Type, Name, authentication]) ->
    %% Type, Name atoms exist, so let 'Type:Name' exist too.
    binary_to_atom(<<(atom_to_binary(Type))/binary, ":", (atom_to_binary(Name))/binary>>).

chain_name_for_filepath(Path) ->
    do_chain_name_for_filepath([to_bin(Key) || Key <- Path]).

do_chain_name_for_filepath([<<"authentication">>]) ->
    to_bin(?GLOBAL);
do_chain_name_for_filepath([<<"listeners">>, Type, Name, <<"authentication">>]) ->
    <<(to_bin(Type))/binary, ":", (to_bin(Name))/binary>>.

merge_authenticators(OriginConf0, NewConf0) ->
    {OriginConf1, NewConf1} =
        lists:foldl(
            fun(Origin, {OriginAcc, NewAcc}) ->
                AuthenticatorID = authenticator_id(Origin),
                case split_by_id(AuthenticatorID, NewAcc) of
                    {error, _} ->
                        {[Origin | OriginAcc], NewAcc};
                    {ok, BeforeFound, [Found | AfterFound]} ->
                        Merged = emqx_utils_maps:deep_merge(Origin, Found),
                        {[Merged | OriginAcc], BeforeFound ++ AfterFound}
                end
            end,
            {[], NewConf0},
            OriginConf0
        ),
    lists:reverse(OriginConf1) ++ NewConf1.

reorder_authenticators(NewOrder, OldConfig) ->
    OldConfigWithIds = [{authenticator_id(Auth), Auth} || Auth <- OldConfig],
    reorder_authenticators(NewOrder, OldConfigWithIds, [], []).

reorder_authenticators([], [] = _RemConfigWithIds, ReorderedConfig, [] = _NotFoundIds) ->
    {ok, lists:reverse(ReorderedConfig)};
reorder_authenticators([], RemConfigWithIds, _ReorderedConfig, NotFoundIds) ->
    {error, #{not_found => NotFoundIds, not_reordered => [Id || {Id, _} <- RemConfigWithIds]}};
reorder_authenticators([Id | RemOrder], RemConfigWithIds, ReorderedConfig, NotFoundIds) ->
    case lists:keytake(Id, 1, RemConfigWithIds) of
        {value, {_Id, Auth}, RemConfigWithIds1} ->
            reorder_authenticators(
                RemOrder, RemConfigWithIds1, [Auth | ReorderedConfig], NotFoundIds
            );
        false ->
            reorder_authenticators(RemOrder, RemConfigWithIds, ReorderedConfig, [Id | NotFoundIds])
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile(nowarn_export_all).
-compile(export_all).

merge_authenticators_test() ->
    ?assertEqual([], merge_authenticators([], [])),

    Http = #{
        <<"mechanism">> => <<"password_based">>, <<"backend">> => <<"http">>, <<"enable">> => true
    },
    Jwt = #{<<"mechanism">> => <<"jwt">>, <<"enable">> => true},
    BuildIn = #{
        <<"mechanism">> => <<"password_based">>,
        <<"backend">> => <<"built_in_database">>,
        <<"enable">> => true
    },
    Mongodb = #{
        <<"mechanism">> => <<"password_based">>,
        <<"backend">> => <<"mongodb">>,
        <<"enable">> => true
    },
    Redis = #{
        <<"mechanism">> => <<"password_based">>, <<"backend">> => <<"redis">>, <<"enable">> => true
    },
    BuildInDisable = BuildIn#{<<"enable">> => false},
    MongodbDisable = Mongodb#{<<"enable">> => false},
    RedisDisable = Redis#{<<"enable">> => false},

    %% add
    ?assertEqual([Http], merge_authenticators([], [Http])),
    ?assertEqual([Http, Jwt, BuildIn], merge_authenticators([Http], [Jwt, BuildIn])),

    %% merge
    ?assertEqual(
        [BuildInDisable, MongodbDisable],
        merge_authenticators([BuildIn, Mongodb], [BuildInDisable, MongodbDisable])
    ),
    ?assertEqual(
        [BuildInDisable, Jwt],
        merge_authenticators([BuildIn, Jwt], [BuildInDisable])
    ),
    ?assertEqual(
        [BuildInDisable, Jwt, Mongodb],
        merge_authenticators([BuildIn, Jwt], [Mongodb, BuildInDisable])
    ),

    %% position changed
    ?assertEqual(
        [BuildInDisable, Jwt, Mongodb, RedisDisable, Http],
        merge_authenticators([BuildIn, Jwt, Mongodb, Redis], [RedisDisable, BuildInDisable, Http])
    ),
    ok.

-endif.
