%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_auth_ext_tls_const_v1).
-elvis([{elvis_style, atom_naming_convention, #{regex => "^([a-z][a-z0-9A-Z]*_?)*(_SUITE)?$"}}]).

-export([
    make_tls_root_fun/2,
    make_tls_verify_fun/2
]).

-export([default_root_fun/1]).

-include_lib("public_key/include/public_key.hrl").

-define(unknown_ca, unknown_ca).

%% @doc Build a root fun for verify TLS partial_chain.
%% The `InputChain' is composed by OTP SSL with local cert store
%% AND the cert (chain if any) from the client.
%% @end
make_tls_root_fun(cacert_from_cacertfile, [Trusted]) ->
    %% Allow only one trusted ca cert, and just return the defined trusted CA cert,
    fun(_InputChain) ->
        %% Note, returing `trusted_ca` doesn't really mean it accepts the connection
        %% OTP SSL app will do the path validation, signature validation subsequently.
        {trusted_ca, Trusted}
    end;
make_tls_root_fun(cacert_from_cacertfile, [TrustedOne, TrustedTwo]) ->
    %% Allow two trusted CA certs in case of CA cert renewal
    %% This is a little expensive call as it compares the binaries.
    fun(InputChain) ->
        case lists:member(TrustedOne, InputChain) of
            true ->
                {trusted_ca, TrustedOne};
            false ->
                {trusted_ca, TrustedTwo}
        end
    end.

make_tls_verify_fun(verify_cert_extKeyUsage, KeyUsages) ->
    RequiredKeyUsages = ext_key_opts(KeyUsages),
    {fun verify_fun_peer_extKeyUsage/3, RequiredKeyUsages}.

verify_fun_peer_extKeyUsage(_, {bad_cert, invalid_ext_key_usage}, UserState) ->
    %% !! Override OTP verify peer default
    %% OTP SSL is unhappy with the ext_key_usage but we will check on our own.
    {unknown, UserState};
verify_fun_peer_extKeyUsage(_, {bad_cert, _} = Reason, _UserState) ->
    %% OTP verify_peer default
    {fail, Reason};
verify_fun_peer_extKeyUsage(_, {extension, _}, UserState) ->
    %% OTP verify_peer default
    {unknown, UserState};
verify_fun_peer_extKeyUsage(_, valid, UserState) ->
    %% OTP verify_peer default
    {valid, UserState};
verify_fun_peer_extKeyUsage(
    #'OTPCertificate'{tbsCertificate = #'OTPTBSCertificate'{extensions = ExtL}},
    %% valid peer cert
    valid_peer,
    RequiredKeyUsages
) ->
    %% override OTP verify_peer default
    %% must have id-ce-extKeyUsage
    case lists:keyfind(?'id-ce-extKeyUsage', 2, ExtL) of
        #'Extension'{extnID = ?'id-ce-extKeyUsage', extnValue = VL} ->
            case do_verify_ext_key_usage(VL, RequiredKeyUsages) of
                true ->
                    %% pass the check,
                    %% fallback to OTP verify_peer default
                    {valid, RequiredKeyUsages};
                false ->
                    {fail, extKeyUsage_unmatched}
            end;
        _ ->
            {fail, extKeyUsage_not_set}
    end.

%% @doc check required extkeyUsages are presented in the cert
do_verify_ext_key_usage(_, []) ->
    %% Verify finished
    true;
do_verify_ext_key_usage(CertExtL, [Usage | T] = _Required) ->
    case lists:member(Usage, CertExtL) of
        true ->
            do_verify_ext_key_usage(CertExtL, T);
        false ->
            false
    end.

%% @doc Helper tls cert extension
-spec ext_key_opts(string()) -> [OidString :: string() | public_key:oid()].
ext_key_opts(Str) ->
    Usages = string:tokens(Str, ","),
    lists:map(
        fun
            ("clientAuth") ->
                ?'id-kp-clientAuth';
            ("serverAuth") ->
                ?'id-kp-serverAuth';
            ("codeSigning") ->
                ?'id-kp-codeSigning';
            ("emailProtection") ->
                ?'id-kp-emailProtection';
            ("timeStamping") ->
                ?'id-kp-timeStamping';
            ("ocspSigning") ->
                ?'id-kp-OCSPSigning';
            ("OID:" ++ OidStr) ->
                OidList = string:tokens(OidStr, "."),
                list_to_tuple(lists:map(fun list_to_integer/1, OidList))
        end,
        Usages
    ).

%% @doc default root fun for partial_chain 'false'
-spec default_root_fun(_) -> ?unknown_ca.
default_root_fun(_) ->
    ?unknown_ca.
