%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_auth_ext_tls_lib).
-elvis([{elvis_style, atom_naming_convention, #{regex => "^([a-z][a-z0-9A-Z]*_?)*(_SUITE)?$"}}]).

-export([
    opt_partial_chain/1,
    opt_verify_fun/1
]).

-include_lib("emqx/include/logger.hrl").

-define(CONST_MOD_V1, emqx_auth_ext_tls_const_v1).
%% @doc enable TLS partial_chain validation
-spec opt_partial_chain(SslOpts :: map()) -> NewSslOpts :: map().
opt_partial_chain(#{partial_chain := false} = SslOpts) ->
    %% For config update scenario, we must set it to override
    %% the 'existing' partial_chain in the listener
    SslOpts#{partial_chain := fun ?CONST_MOD_V1:default_root_fun/1};
opt_partial_chain(#{partial_chain := true} = SslOpts) ->
    SslOpts#{partial_chain := rootfun_trusted_ca_from_cacertfile(1, SslOpts)};
opt_partial_chain(#{partial_chain := cacert_from_cacertfile} = SslOpts) ->
    SslOpts#{partial_chain := rootfun_trusted_ca_from_cacertfile(1, SslOpts)};
opt_partial_chain(#{partial_chain := two_cacerts_from_cacertfile} = SslOpts) ->
    SslOpts#{partial_chain := rootfun_trusted_ca_from_cacertfile(2, SslOpts)};
opt_partial_chain(SslOpts) ->
    SslOpts.

%% @doc make verify_fun if set.
-spec opt_verify_fun(SslOpts :: map()) -> NewSslOpts :: map().
opt_verify_fun(#{verify_peer_ext_key_usage := V} = SslOpts) when V =/= undefined ->
    SslOpts#{verify_fun => ?CONST_MOD_V1:make_tls_verify_fun(verify_cert_extKeyUsage, V)};
opt_verify_fun(SslOpts) ->
    SslOpts.

%% @doc Helper, make TLS root_fun
rootfun_trusted_ca_from_cacertfile(NumOfCerts, #{cacertfile := Cacertfile}) ->
    case file:read_file(emqx_schema:naive_env_interpolation(Cacertfile)) of
        {ok, PemBin} ->
            try
                do_rootfun_trusted_ca_from_cacertfile(NumOfCerts, PemBin)
            catch
                _Error:_Info:ST ->
                    %% The cacertfile will be checked by OTP SSL as well and OTP choice to be silent on this.
                    %% We are touching security sutffs, don't leak extra info..
                    ?SLOG(error, #{
                        msg => "trusted_cacert_not_found_in_cacertfile", stacktrace => ST
                    }),
                    throw({error, ?FUNCTION_NAME})
            end;
        {error, Reason} ->
            throw({error, {read_cacertfile_error, Cacertfile, Reason}})
    end;
rootfun_trusted_ca_from_cacertfile(_NumOfCerts, _SslOpts) ->
    throw({error, cacertfile_unset}).

do_rootfun_trusted_ca_from_cacertfile(NumOfCerts, PemBin) ->
    %% The last one or two should be the top parent in the chain if it is a chain
    Certs = public_key:pem_decode(PemBin),
    Pos = length(Certs) - NumOfCerts + 1,
    Trusted = [
        CADer
     || {'Certificate', CADer, _} <-
            lists:sublist(public_key:pem_decode(PemBin), Pos, NumOfCerts)
    ],
    ?CONST_MOD_V1:make_tls_root_fun(cacert_from_cacertfile, Trusted).
