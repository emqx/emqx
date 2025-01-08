%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_passwd).

-export([
    hash/2,
    hash_data/2,
    check_pass/3,
    compare_secure/2
]).

-export_type([
    password/0,
    password_hash/0,
    hash_type_simple/0,
    hash_type/0,
    salt_position/0,
    salt/0
]).

-include("logger.hrl").

-type password() :: binary().
-type password_hash() :: binary().

-type hash_type_simple() :: plain | md5 | sha | sha256 | sha512.
-type hash_type() :: hash_type_simple() | bcrypt | pbkdf2.

-type salt_position() :: disable | prefix | suffix.
-type salt() :: binary().

-type pbkdf2_mac_fun() :: sha | sha224 | sha256 | sha384 | sha512.
-type pbkdf2_iterations() :: pos_integer().
-type pbkdf2_dk_length() :: pos_integer() | undefined.

-type hash_params() ::
    {bcrypt, salt()}
    | {pbkdf2, pbkdf2_mac_fun(), salt(), pbkdf2_iterations(), pbkdf2_dk_length()}
    | {hash_type_simple(), salt(), salt_position()}.

-export_type([pbkdf2_mac_fun/0]).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec check_pass(hash_params(), password_hash(), password() | undefined) -> boolean().
check_pass(_Algo, _Hash, undefined) ->
    false;
check_pass(Algo, Hash, Password) ->
    do_check_pass(Algo, Hash, Password).

do_check_pass({pbkdf2, MacFun, Salt, Iterations, DKLength}, PasswordHash, Password) ->
    HashPasswd = pbkdf2(MacFun, Password, Salt, Iterations, DKLength),
    compare_secure(hex(HashPasswd), PasswordHash);
do_check_pass({bcrypt, Salt}, PasswordHash, Password) ->
    case bcrypt:hashpw(Password, Salt) of
        {ok, HashPasswd} ->
            compare_secure(list_to_binary(HashPasswd), PasswordHash);
        {error, _Reason} ->
            false
    end;
do_check_pass({_SimpleHash, _Salt, _SaltPosition} = HashParams, PasswordHash, Password) ->
    Hash = hash(HashParams, Password),
    compare_secure(Hash, PasswordHash).

-spec hash(hash_params(), password()) -> password_hash().
hash({pbkdf2, MacFun, Salt, Iterations, DKLength}, Password) when Iterations > 0 ->
    HashPasswd = pbkdf2(MacFun, Password, Salt, Iterations, DKLength),
    hex(HashPasswd);
hash({bcrypt, Salt}, Password) ->
    case bcrypt:hashpw(Password, Salt) of
        {ok, HashPasswd} ->
            list_to_binary(HashPasswd);
        {error, Reason} ->
            error(Reason)
    end;
hash({SimpleHash, _Salt, disable}, Password) when is_binary(Password) ->
    hash_data(SimpleHash, Password);
hash({SimpleHash, Salt, prefix}, Password) when is_binary(Password), is_binary(Salt) ->
    hash_data(SimpleHash, <<Salt/binary, Password/binary>>);
hash({SimpleHash, Salt, suffix}, Password) when is_binary(Password), is_binary(Salt) ->
    hash_data(SimpleHash, <<Password/binary, Salt/binary>>);
hash({_SimpleHash, Salt, _SaltPos}, _Password) when not is_binary(Salt) ->
    error({salt_not_string, Salt});
hash({_SimpleHash, _Salt, _SaltPos}, Password) when not is_binary(Password) ->
    error({password_not_string, Password}).

-spec hash_data(hash_type(), binary()) -> binary().
hash_data(plain, Data) when is_binary(Data) ->
    Data;
hash_data(md5, Data) when is_binary(Data) ->
    hex(crypto:hash(md5, Data));
hash_data(sha, Data) when is_binary(Data) ->
    hex(crypto:hash(sha, Data));
hash_data(sha256, Data) when is_binary(Data) ->
    hex(crypto:hash(sha256, Data));
hash_data(sha512, Data) when is_binary(Data) ->
    hex(crypto:hash(sha512, Data)).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

compare_secure(X, Y) when is_binary(X), is_binary(Y) ->
    compare_secure(binary_to_list(X), binary_to_list(Y));
compare_secure(X, Y) when is_list(X), is_list(Y) ->
    case length(X) == length(Y) of
        true ->
            compare_secure(X, Y, 0);
        false ->
            false
    end.

compare_secure([X | RestX], [Y | RestY], Result) ->
    compare_secure(RestX, RestY, (X bxor Y) bor Result);
compare_secure([], [], Result) ->
    Result == 0.

-define(MAX_DERIVED_KEY_LENGTH, (1 bsl 32 - 1)).
pbkdf2(MacFun, Password, Salt, Iterations, undefined) ->
    crypto:pbkdf2_hmac(MacFun, Password, Salt, Iterations, dk_length(MacFun));
pbkdf2(_MacFun, _Password, _Salt, _Iterations, DKLength) when DKLength > ?MAX_DERIVED_KEY_LENGTH ->
    %% backword compatible with the old implementation in pbkdf2
    error(dklength_too_long);
pbkdf2(MacFun, Password, Salt, Iterations, DKLength) ->
    crypto:pbkdf2_hmac(MacFun, Password, Salt, Iterations, DKLength).

hex(X) when is_binary(X) ->
    %% TODO: change to binary:encode_hex(X, lowercase) when OTP version is always > 25
    string:lowercase(binary:encode_hex(X)).

%% @doc default derived key length for PBKDF2, backword compatible with the old implementation in pbkdf2
-spec dk_length(pbkdf2_mac_fun()) -> non_neg_integer().
dk_length(sha) -> 20;
dk_length(sha224) -> 28;
dk_length(sha256) -> 32;
dk_length(sha384) -> 48;
dk_length(sha512) -> 64.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

len_match(Alg) ->
    DKLength = byte_size(crypto:mac(hmac, Alg, <<"test key">>, <<"test data">>)),
    dk_length(Alg) =:= DKLength.

dk_length_test_() ->
    [
        ?_assert(len_match(Alg))
     || Alg <- [sha, sha224, sha256, sha384, sha512]
    ].

-endif.
