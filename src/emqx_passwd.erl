%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
        , check_pass/2
        ]).

-type(hash_type() :: plain | md5 | sha | sha256 | pbkdf2 | bcrypt).

-export_type([hash_type/0]).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec(check_pass(binary() | tuple(), binary() | tuple())
      -> ok | {error, term()}).
check_pass({PassHash, Password}, bcrypt) ->
    try
        Salt = binary:part(PassHash, {0, 29}),
        check_pass(PassHash, emqx_passwd:hash(bcrypt, {Salt, Password}))
    catch
        error:badarg -> {error, incorrect_hash}
    end;
check_pass({PassHash, Password}, HashType) ->
    check_pass(PassHash, emqx_passwd:hash(HashType, Password));
check_pass({PassHash, Salt, Password}, {pbkdf2, Macfun, Iterations, Dklen}) ->
    check_pass(PassHash, emqx_passwd:hash(pbkdf2, {Salt, Password, Macfun, Iterations, Dklen}));
check_pass({PassHash, Salt, Password}, {salt, bcrypt}) ->
    check_pass(PassHash, emqx_passwd:hash(bcrypt, {Salt, Password}));
check_pass({PassHash, Salt, Password}, {bcrypt, salt}) ->
    check_pass(PassHash, emqx_passwd:hash(bcrypt, {Salt, Password}));
check_pass({PassHash, Salt, Password}, {salt, HashType}) ->
    check_pass(PassHash, emqx_passwd:hash(HashType, <<Salt/binary, Password/binary>>));
check_pass({PassHash, Salt, Password}, {HashType, salt}) ->
    check_pass(PassHash, emqx_passwd:hash(HashType, <<Password/binary, Salt/binary>>));
check_pass(PassHash, PassHash) -> ok;
check_pass(_Hash1, _Hash2)     -> {error, password_error}.

-spec(hash(hash_type(), binary() | tuple()) -> binary()).
hash(plain, Password)  ->
    Password;
hash(md5, Password)  ->
    hexstring(crypto:hash(md5, Password));
hash(sha, Password)  ->
    hexstring(crypto:hash(sha, Password));
hash(sha256, Password)  ->
    hexstring(crypto:hash(sha256, Password));
hash(sha512, Password)  ->
    hexstring(crypto:hash(sha512, Password));
hash(pbkdf2, {Salt, Password, Macfun, Iterations, Dklen}) ->
    case pbkdf2:pbkdf2(Macfun, Password, Salt, Iterations, Dklen) of
        {ok, Hexstring} ->
            pbkdf2:to_hex(Hexstring);
        {error, Error}  ->
            error_logger:error_msg("pbkdf2 hash error:~p", [Error]),
            <<>>
    end;
hash(bcrypt, {Salt, Password}) ->
    {ok, _} = application:ensure_all_started(bcrypt),
    case bcrypt:hashpw(Password, Salt) of
        {ok, HashPasswd} ->
            list_to_binary(HashPasswd);
        {error, Error}->
            error_logger:error_msg("bcrypt hash error:~p", [Error]),
            <<>>
    end.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

hexstring(<<X:128/big-unsigned-integer>>) ->
    iolist_to_binary(io_lib:format("~32.16.0b", [X]));
hexstring(<<X:160/big-unsigned-integer>>) ->
    iolist_to_binary(io_lib:format("~40.16.0b", [X]));
hexstring(<<X:256/big-unsigned-integer>>) ->
    iolist_to_binary(io_lib:format("~64.16.0b", [X]));
hexstring(<<X:512/big-unsigned-integer>>) ->
    iolist_to_binary(io_lib:format("~128.16.0b", [X])).

