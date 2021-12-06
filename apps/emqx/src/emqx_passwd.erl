%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([ hash/2
        , hash_data/2
        , check_pass/3
        ]).

-export_type([ password/0
             , password_hash/0
             , hash_type_simple/0
             , hash_type/0
             , salt_position/0
             , salt/0]).

-include("logger.hrl").

-type(password() :: binary()).
-type(password_hash() :: binary()).

-type(hash_type_simple() :: plain | md5 | sha | sha256 | sha512).
-type(hash_type() :: hash_type_simple() | bcrypt).

-type(salt_position() :: prefix | suffix).
-type(salt() :: binary()).

-type(hash_params() :: {bcrypt, salt()} | {hash_type_simple(), salt(), salt_position()}).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec(check_pass(hash_params(), password_hash(), password()) -> boolean()).
check_pass({bcrypt, Salt}, PasswordHash, Password) ->
    case bcrypt:hashpw(Password, Salt) of
        {ok, HashPasswd} ->
            compare_secure(list_to_binary(HashPasswd), PasswordHash);
        {error, _Reason}->
            false
    end;
check_pass({_SimpleHash, _Salt, _SaltPosition} = HashParams, PasswordHash, Password) ->
    Hash = hash(HashParams, Password),
    compare_secure(Hash, PasswordHash).

-spec(hash(hash_params(), password()) -> password_hash()).
hash({bcrypt, Salt}, Password) ->
    case bcrypt:hashpw(Password, Salt) of
        {ok, HashPasswd} ->
            list_to_binary(HashPasswd);
        {error, Reason}->
            error(Reason)
    end;
hash({SimpleHash, Salt, prefix}, Password) when is_binary(Password), is_binary(Salt) ->
    hash_data(SimpleHash, <<Salt/binary, Password/binary>>);
hash({SimpleHash, Salt, suffix}, Password) when is_binary(Password), is_binary(Salt) ->
    hash_data(SimpleHash, <<Password/binary, Salt/binary>>).


-spec(hash_data(hash_type(), binary()) -> binary()).
hash_data(plain, Data) when is_binary(Data) ->
    Data;
hash_data(md5, Data) when is_binary(Data) ->
    hexstring(crypto:hash(md5, Data));
hash_data(sha, Data) when is_binary(Data) ->
    hexstring(crypto:hash(sha, Data));
hash_data(sha256, Data) when is_binary(Data) ->
    hexstring(crypto:hash(sha256, Data));
hash_data(sha512, Data) when is_binary(Data) ->
    hexstring(crypto:hash(sha512, Data)).

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


hexstring(<<X:128/big-unsigned-integer>>) ->
    iolist_to_binary(io_lib:format("~32.16.0b", [X]));
hexstring(<<X:160/big-unsigned-integer>>) ->
    iolist_to_binary(io_lib:format("~40.16.0b", [X]));
hexstring(<<X:256/big-unsigned-integer>>) ->
    iolist_to_binary(io_lib:format("~64.16.0b", [X]));
hexstring(<<X:512/big-unsigned-integer>>) ->
    iolist_to_binary(io_lib:format("~128.16.0b", [X])).
