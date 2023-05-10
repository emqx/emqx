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
        ]).

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
