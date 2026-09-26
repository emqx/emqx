%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_tls_lib_const_v1).
-elvis([{elvis_style, atom_naming_convention, #{regex => "^([a-z][a-z0-9A-Z]*_?)*$"}}]).

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
    {fun verify_fun_peer_extKeyUsage/3, RequiredKeyUsages};
make_tls_verify_fun(hostname_check_san_or_common_name, _) ->
    {fun verify_fun_hostname_cn_fallback/3, #{}}.

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

%% Client side. Since OTP 28.5.0.1 (RFC 9525), `public_key:pkix_verify_hostname/3'
%% no longer falls back to the subject CN when the certificate has no
%% subjectAltName, and a `match_fun' cannot restore it because the fun is never
%% called without SANs. OTP reports that case to the verify fun as
%% `{bad_cert, hostname_check_failed}'; accept it only when the peer certificate
%% has no SAN and one of its CNs matches the requested name the way OTP 28.4
%% did. Every other event keeps the OTP verify_peer default.
verify_fun_hostname_cn_fallback(Cert, {bad_cert, hostname_check_failed} = Reason, UserState) ->
    case is_cn_fallback_match(Cert, reference_hostname()) of
        true ->
            {valid, UserState};
        false ->
            {fail, Reason}
    end;
verify_fun_hostname_cn_fallback(_, {bad_cert, _} = Reason, _UserState) ->
    %% OTP verify_peer default
    {fail, Reason};
verify_fun_hostname_cn_fallback(_, {extension, _}, UserState) ->
    %% OTP verify_peer default
    {unknown, UserState};
verify_fun_hostname_cn_fallback(_, valid, UserState) ->
    %% OTP verify_peer default
    {valid, UserState};
verify_fun_hostname_cn_fallback(_, valid_peer, UserState) ->
    %% OTP verify_peer default
    {valid, UserState}.

%% The verify fun runs in the TLS client connection process, which OTP labels
%% `{tls | dtls, client, SNI}'. SNI is the `server_name_indication' option, or
%% the host passed to connect when the option is not set: the same name OTP
%% checked the certificate against.
reference_hostname() ->
    case proc_lib:get_label(self()) of
        {Protocol, client, SNI} when
            (Protocol =:= tls orelse Protocol =:= dtls) andalso is_binary(SNI)
        ->
            unicode:characters_to_list(SNI);
        _ ->
            undefined
    end.

is_cn_fallback_match(#'OTPCertificate'{tbsCertificate = TBS}, Hostname) when
    is_list(Hostname), Hostname =/= ""
->
    #'OTPTBSCertificate'{subject = Subject, extensions = Exts} = TBS,
    %% An IP address never matched a CN, not even before OTP 28.5.
    has_no_subject_alt_name(Exts) andalso
        inet:parse_address(Hostname) =:= {error, einval} andalso
        lists:any(
            fun(CN) -> is_cn_match(to_lower_ascii(Hostname), to_lower_ascii(CN)) end,
            subject_common_names(Subject)
        );
is_cn_fallback_match(_Cert, _Hostname) ->
    false.

has_no_subject_alt_name(Exts) when is_list(Exts) ->
    case lists:keyfind(?'id-ce-subjectAltName', #'Extension'.extnID, Exts) of
        #'Extension'{extnValue = [_ | _]} -> false;
        _ -> true
    end;
has_no_subject_alt_name(_) ->
    true.

subject_common_names({rdnSequence, RDNs}) ->
    [
        CN
     || ATVs <- RDNs,
        #'AttributeTypeAndValue'{type = ?'id-at-commonName', value = {_, V}} <- ATVs,
        CN <- [cn_to_string(V)],
        CN =/= ""
    ];
subject_common_names(_) ->
    [].

cn_to_string(V) when is_binary(V) ->
    case unicode:characters_to_list(V) of
        L when is_list(L) -> L;
        _ -> ""
    end;
cn_to_string(V) when is_list(V) ->
    V;
cn_to_string(_) ->
    "".

%% The CN rules of `public_key:verify_hostname_match_default0/2' in OTP 28.4:
%% an exact match of a name without wildcard, or a wildcard in the left-most
%% label only.
is_cn_match(Name, Name) ->
    not lists:member($*, Name);
is_cn_match(Name, Pattern) ->
    [N1 | Ns] = string:split(Name, "."),
    [P1 | Ps] = string:split(Pattern, "."),
    match_wild(N1, P1) andalso Ns =:= Ps.

match_wild(A, [$* | B]) -> match_wild_suffixes(A, B);
match_wild([C | A], [C | B]) -> match_wild(A, B);
match_wild([], []) -> true;
match_wild(_, _) -> false.

%% Match the parts after the only wildcard by comparing them from the end.
match_wild_suffixes(A, B) -> match_wild_sfx(lists:reverse(A), lists:reverse(B)).

%% A name with a wildcard, or a pattern with a second one, never matches.
match_wild_sfx([$* | _], _) -> false;
match_wild_sfx(_, [$* | _]) -> false;
match_wild_sfx([A | Ar], [A | Br]) -> match_wild_sfx(Ar, Br);
match_wild_sfx(Ar, []) -> not lists:member($*, Ar);
match_wild_sfx(_, _) -> false.

to_lower_ascii(S) ->
    [to_lower_ascii_char(C) || C <- S].

to_lower_ascii_char(C) when C >= $A, C =< $Z -> C - $A + $a;
to_lower_ascii_char(C) -> C.

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
