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

%% @doc Authenticator configuration management module.
-module(emqx_authentication_config).

-behaviour(emqx_config_handler).

-export([ pre_config_update/2
        , post_config_update/4
        ]).

-export([ authenticator_id/1
        , authn_type/1
        ]).

-export_type([config/0]).

-include("logger.hrl").

-type parsed_config() :: #{mechanism := atom(),
                           backend => atom(),
                           atom() => term()}.
-type raw_config() :: #{binary() => term()}.
-type config() :: parsed_config() | raw_config().

-type authenticator_id() :: emqx_authentication:authenticator_id().
-type position() :: emqx_authentication:position().
-type chain_name() :: emqx_authentication:chain_name().
-type update_request() :: {create_authenticator, chain_name(), map()}
                        | {delete_authenticator, chain_name(), authenticator_id()}
                        | {update_authenticator, chain_name(), authenticator_id(), map()}
                        | {move_authenticator, chain_name(), authenticator_id(), position()}.

%%------------------------------------------------------------------------------
%% Callbacks of config handler
%%------------------------------------------------------------------------------

-spec pre_config_update(update_request(), emqx_config:raw_config())
    -> {ok, map() | list()} | {error, term()}.
pre_config_update(UpdateReq, OldConfig) ->
    case do_pre_config_update(UpdateReq, to_list(OldConfig)) of
        {error, Reason} -> {error, Reason};
        {ok, NewConfig} -> {ok, return_map(NewConfig)}
    end.

do_pre_config_update({create_authenticator, ChainName, Config}, OldConfig) ->
    try
        CertsDir = certs_dir([to_bin(ChainName), authenticator_id(Config)]),
        NConfig = convert_certs(CertsDir, Config),
        {ok, OldConfig ++ [NConfig]}
    catch
        error:{save_cert_to_file, _} = Reason ->
            {error, Reason};
        error:{missing_parameter, _} = Reason ->
            {error, Reason}
    end;
do_pre_config_update({delete_authenticator, _ChainName, AuthenticatorID}, OldConfig) ->
    NewConfig = lists:filter(fun(OldConfig0) ->
                                AuthenticatorID =/= authenticator_id(OldConfig0)
                             end, OldConfig),
    {ok, NewConfig};
do_pre_config_update({update_authenticator, ChainName, AuthenticatorID, Config}, OldConfig) ->
    try
        CertsDir = certs_dir([to_bin(ChainName), AuthenticatorID]),
        NewConfig = lists:map(
                        fun(OldConfig0) ->
                            case AuthenticatorID =:= authenticator_id(OldConfig0) of
                                true -> convert_certs(CertsDir, Config, OldConfig0);
                                false -> OldConfig0
                            end
                        end, OldConfig),
        {ok, NewConfig}
    catch
        error:{save_cert_to_file, _} = Reason ->
            {error, Reason};
        error:{missing_parameter, _} = Reason ->
            {error, Reason}
    end;
do_pre_config_update({move_authenticator, _ChainName, AuthenticatorID, Position}, OldConfig) ->
    case split_by_id(AuthenticatorID, OldConfig) of
        {error, Reason} -> {error, Reason};
        {ok, Part1, [Found | Part2]} ->
            case Position of
                top ->
                    {ok, [Found | Part1] ++ Part2};
                bottom ->
                    {ok, Part1 ++ Part2 ++ [Found]};
                {before, Before} ->
                    case split_by_id(Before, Part1 ++ Part2) of
                        {error, Reason} ->
                            {error, Reason};
                        {ok, NPart1, [NFound | NPart2]} ->
                            {ok, NPart1 ++ [Found, NFound | NPart2]}
                    end
            end
    end.

-spec post_config_update(update_request(), map() | list(), emqx_config:raw_config(), emqx_config:app_envs())
    -> ok | {ok, map()} | {error, term()}.
post_config_update(UpdateReq, NewConfig, OldConfig, AppEnvs) ->
    do_post_config_update(UpdateReq, check_configs(to_list(NewConfig)), OldConfig, AppEnvs).

do_post_config_update({create_authenticator, ChainName, Config}, _NewConfig, _OldConfig, _AppEnvs) ->
    NConfig = check_config(Config),
    _ = emqx_authentication:create_chain(ChainName),
    emqx_authentication:create_authenticator(ChainName, NConfig);
do_post_config_update({delete_authenticator, ChainName, AuthenticatorID}, _NewConfig, OldConfig, _AppEnvs) ->
    case emqx_authentication:delete_authenticator(ChainName, AuthenticatorID) of
        ok ->
            [Config] = [Config0 || Config0 <- to_list(OldConfig), AuthenticatorID == authenticator_id(Config0)],
            CertsDir = certs_dir([to_bin(ChainName), AuthenticatorID]),
            clear_certs(CertsDir, Config),
            ok;
        {error, Reason} ->
            {error, Reason}
    end;
do_post_config_update({update_authenticator, ChainName, AuthenticatorID, Config}, _NewConfig, _OldConfig, _AppEnvs) ->
    NConfig = check_config(Config),
    emqx_authentication:update_authenticator(ChainName, AuthenticatorID, NConfig);
do_post_config_update({move_authenticator, ChainName, AuthenticatorID, Position}, _NewConfig, _OldConfig, _AppEnvs) ->
    emqx_authentication:move_authenticator(ChainName, AuthenticatorID, Position).

check_config(Config) ->
    [Checked] = check_configs([Config]),
    Checked.

check_configs(Configs) ->
    Providers = emqx_authentication:get_providers(),
    lists:map(fun(C) -> do_check_conifg(C, Providers) end, Configs).

do_check_conifg(Config, Providers) ->
    Type = authn_type(Config),
    case maps:get(Type, Providers, false) of
        false ->
            ?SLOG(warning, #{msg => "unknown_authn_type",
                             type => Type,
                             providers => Providers}),
            throw(unknown_authn_type);
        Module ->
            %% TODO: check if Module:check_config/1 is exported
            %% so we do not force all providers to implement hocon schema
            try hocon_schema:check_plain(Module, #{<<"config">> => Config},
                                         #{atom_key => true}) of
                #{config := Result} ->
                    Result
            catch
                C : E : S ->
                    ?SLOG(warning, #{msg => "failed_to_check_config", config => Config}),
                    erlang:raise(C, E, S)
            end
    end.

return_map([L]) -> L;
return_map(L) -> L.

to_list(undefined) -> [];
to_list(M) when M =:= #{} -> [];
to_list(M) when is_map(M) -> [M];
to_list(L) when is_list(L) -> L.

certs_dir(Dirs) when is_list(Dirs) ->
    to_bin(filename:join([emqx:get_config([node, data_dir]), "certs", "authn"] ++ Dirs)).

convert_certs(CertsDir, Config) ->
    case maps:get(<<"ssl">>, Config, undefined) of
        undefined ->
            Config;
        SSLOpts ->
            NSSLOPts = lists:foldl(fun(K, Acc) ->
                               case maps:get(K, Acc, undefined) of
                                   undefined -> Acc;
                                   PemBin ->
                                       CertFile = generate_filename(CertsDir, K),
                                       ok = save_cert_to_file(CertFile, PemBin),
                                       Acc#{K => CertFile}
                               end
                           end, SSLOpts, [<<"certfile">>, <<"keyfile">>, <<"cacertfile">>]),
            Config#{<<"ssl">> => NSSLOPts}
    end.

convert_certs(CertsDir, NewConfig, OldConfig) ->
    case maps:get(<<"ssl">>, NewConfig, undefined) of
        undefined ->
            NewConfig;
        NewSSLOpts ->
            OldSSLOpts = maps:get(<<"ssl">>, OldConfig, #{}),
            Diff = diff_certs(NewSSLOpts, OldSSLOpts),
            NSSLOpts = lists:foldl(fun({identical, K}, Acc) ->
                                    Acc#{K => maps:get(K, OldSSLOpts)};
                                    ({_, K}, Acc) ->
                                    CertFile = generate_filename(CertsDir, K),
                                    ok = save_cert_to_file(CertFile, maps:get(K, NewSSLOpts)),
                                    Acc#{K => CertFile}
                                end, NewSSLOpts, Diff),
            NewConfig#{<<"ssl">> => NSSLOpts}
    end.

clear_certs(CertsDir, Config) ->
    case maps:get(<<"ssl">>, Config, undefined) of
        undefined ->
            ok;
        SSLOpts ->
            lists:foreach(
                fun({_, Filename}) ->
                    _ = file:delete(filename:join([CertsDir, Filename]))
                end,
                maps:to_list(maps:with([<<"certfile">>, <<"keyfile">>, <<"cacertfile">>], SSLOpts)))
    end.

save_cert_to_file(Filename, PemBin) ->
    case public_key:pem_decode(PemBin) =/= [] of
        true ->
            case filelib:ensure_dir(Filename) of
                ok ->
                    case file:write_file(Filename, PemBin) of
                        ok -> ok;
                        {error, Reason} -> error({save_cert_to_file, {write_file, Reason}})
                    end;
                {error, Reason} ->
                    error({save_cert_to_file, {ensure_dir, Reason}})
            end;
        false ->
            error({save_cert_to_file, invalid_certificate})
    end.

generate_filename(CertsDir, Key) ->
    Prefix = case Key of
                 <<"keyfile">> -> "key-";
                 <<"certfile">> -> "cert-";
                 <<"cacertfile">> -> "cacert-"
             end,
    to_bin(filename:join([CertsDir, Prefix ++ emqx_misc:gen_id() ++ ".pem"])).

diff_certs(NewSSLOpts, OldSSLOpts) ->
    Keys = [<<"cacertfile">>, <<"certfile">>, <<"keyfile">>],
    CertPems = maps:with(Keys, NewSSLOpts),
    CertFiles = maps:with(Keys, OldSSLOpts),
    Diff = lists:foldl(fun({K, CertFile}, Acc) ->
                    case maps:find(K, CertPems) of
                        error -> Acc;
                        {ok, PemBin1} ->
                            {ok, PemBin2} = file:read_file(CertFile),
                            case diff_cert(PemBin1, PemBin2) of
                                true ->
                                    [{changed, K} | Acc];
                                false ->
                                    [{identical, K} | Acc]
                            end
                    end
                end,
                [], maps:to_list(CertFiles)),
    Added = [{added, K} || K <- maps:keys(maps:without(maps:keys(CertFiles), CertPems))],
    Diff ++ Added.

diff_cert(Pem1, Pem2) ->
    cal_md5_for_cert(Pem1) =/= cal_md5_for_cert(Pem2).

cal_md5_for_cert(Pem) ->
    crypto:hash(md5, term_to_binary(public_key:pem_decode(Pem))).

split_by_id(ID, AuthenticatorsConfig) ->
    case lists:foldl(
             fun(C, {P1, P2, F0}) ->
                 F = case ID =:= authenticator_id(C) of
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
    Mechanism;
authenticator_id(C) ->
    error({missing_parameter, mechanism, C}).

%% @doc Make the authentication type.
authn_type(#{mechanism := M, backend :=  B}) -> {atom(M), atom(B)};
authn_type(#{mechanism := M}) -> atom(M);
authn_type(#{<<"mechanism">> := M, <<"backend">> := B}) -> {atom(M), atom(B)};
authn_type(#{<<"mechanism">> := M}) -> atom(M).

atom(Bin) ->
    binary_to_existing_atom(Bin, utf8).
