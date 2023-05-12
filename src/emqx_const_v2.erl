%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
%%
%% @doc Never update this module, create a v3 instead.
%%--------------------------------------------------------------------

-module(emqx_const_v2).

-export([ make_tls_root_fun/2
        , make_tls_verify_fun/2
        ]).

-include_lib("public_key/include/public_key.hrl").
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
  AllowedKeyUsages = ext_key_opts(KeyUsages),
  {fun verify_fun_peer_extKeyUsage/3, AllowedKeyUsages}.

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
verify_fun_peer_extKeyUsage(#'OTPCertificate'{tbsCertificate = #'OTPTBSCertificate'{extensions = ExtL}},
                            valid_peer,  %% valid peer cert
                            AllowedKeyUsages) ->
  %% override OTP verify_peer default
  %% must have id-ce-extKeyUsage
  case lists:keyfind(?'id-ce-extKeyUsage', 2, ExtL) of
    #'Extension'{extnID = ?'id-ce-extKeyUsage', extnValue = VL} ->
      case do_verify_ext_key_usage(VL, AllowedKeyUsages) of
        true ->
          %% pass the check,
          %% fallback to OTP verify_peer default
          {valid, AllowedKeyUsages};
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
-spec ext_key_opts(string()) -> [OidString::string() | public_key:oid()];
                  (undefined) -> undefined.
ext_key_opts(Str) ->
    Usages = string:tokens(Str, ","),
    lists:map(fun("clientAuth") ->
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
                 ([$O,$I,$D,$: | OidStr]) ->
                      OidList = string:tokens(OidStr, "."),
                      list_to_tuple(lists:map(fun list_to_integer/1, OidList))
              end, Usages).
