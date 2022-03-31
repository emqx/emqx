%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authn_password_hashing).

-include_lib("typerefl/include/types.hrl").

-type simple_algorithm_name() :: plain | md5 | sha | sha256 | sha512.
-type salt_position() :: prefix | suffix.

-type simple_algorithm() :: #{
    name := simple_algorithm_name(),
    salt_position := salt_position()
}.

-type bcrypt_algorithm() :: #{name := bcrypt}.
-type bcrypt_algorithm_rw() :: #{name := bcrypt, salt_rounds := integer()}.

-type pbkdf2_algorithm() :: #{
    name := pbkdf2,
    mac_fun := emqx_passwd:pbkdf2_mac_fun(),
    iterations := pos_integer()
}.

-type algorithm() :: simple_algorithm() | pbkdf2_algorithm() | bcrypt_algorithm().
-type algorithm_rw() :: simple_algorithm() | pbkdf2_algorithm() | bcrypt_algorithm_rw().

%%------------------------------------------------------------------------------
%% Hocon Schema
%%------------------------------------------------------------------------------

-behaviour(hocon_schema).

-export([
    roots/0,
    fields/1,
    namespace/0
]).

-export([
    type_ro/1,
    type_rw/1
]).

-export([
    init/1,
    gen_salt/1,
    hash/2,
    check_password/4
]).

namespace() -> "authn-hash".
roots() -> [pbkdf2, bcrypt, bcrypt_rw, other_algorithms].

fields(bcrypt_rw) ->
    fields(bcrypt) ++
        [{salt_rounds, fun salt_rounds/1}];
fields(bcrypt) ->
    [{name, {enum, [bcrypt]}}];
fields(pbkdf2) ->
    [
        {name, {enum, [pbkdf2]}},
        {mac_fun, {enum, [md4, md5, ripemd160, sha, sha224, sha256, sha384, sha512]}},
        {iterations, integer()},
        {dk_length, fun dk_length/1}
    ];
fields(other_algorithms) ->
    [
        {name, {enum, [plain, md5, sha, sha256, sha512]}},
        {salt_position, fun salt_position/1}
    ].

salt_position(type) -> {enum, [prefix, suffix]};
salt_position(desc) -> "Specifies whether the password salt is stored as a prefix or the suffix.";
salt_position(default) -> prefix;
salt_position(_) -> undefined.

salt_rounds(type) -> integer();
salt_rounds(desc) -> "Cost factor for the bcrypt hash.";
salt_rounds(default) -> 10;
salt_rounds(_) -> undefined.

dk_length(type) -> integer();
dk_length(desc) -> "Length of the derived key.";
dk_length(required) -> false;
dk_length(_) -> undefined.

type_rw(type) ->
    hoconsc:union(rw_refs());
type_rw(default) ->
    #{<<"name">> => sha256, <<"salt_position">> => prefix};
type_rw(_) ->
    undefined.

type_ro(type) ->
    hoconsc:union(ro_refs());
type_ro(default) ->
    #{<<"name">> => sha256, <<"salt_position">> => prefix};
type_ro(_) ->
    undefined.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

-spec init(algorithm()) -> ok.
init(#{name := bcrypt}) ->
    {ok, _} = application:ensure_all_started(bcrypt),
    ok;
init(#{name := _Other}) ->
    ok.

-spec gen_salt(algorithm_rw()) -> emqx_passwd:salt().
gen_salt(#{name := plain}) ->
    <<>>;
gen_salt(#{
    name := bcrypt,
    salt_rounds := Rounds
}) ->
    {ok, Salt} = bcrypt:gen_salt(Rounds),
    list_to_binary(Salt);
gen_salt(#{name := Other}) when Other =/= plain, Other =/= bcrypt ->
    <<X:128/big-unsigned-integer>> = crypto:strong_rand_bytes(16),
    iolist_to_binary(io_lib:format("~32.16.0b", [X])).

-spec hash(algorithm_rw(), emqx_passwd:password()) -> {emqx_passwd:hash(), emqx_passwd:salt()}.
hash(#{name := bcrypt, salt_rounds := _} = Algorithm, Password) ->
    Salt0 = gen_salt(Algorithm),
    Hash = emqx_passwd:hash({bcrypt, Salt0}, Password),
    Salt = Hash,
    {Hash, Salt};
hash(
    #{
        name := pbkdf2,
        mac_fun := MacFun,
        iterations := Iterations
    } = Algorithm,
    Password
) ->
    Salt = gen_salt(Algorithm),
    DKLength = maps:get(dk_length, Algorithm, undefined),
    Hash = emqx_passwd:hash({pbkdf2, MacFun, Salt, Iterations, DKLength}, Password),
    {Hash, Salt};
hash(#{name := Other, salt_position := SaltPosition} = Algorithm, Password) ->
    Salt = gen_salt(Algorithm),
    Hash = emqx_passwd:hash({Other, Salt, SaltPosition}, Password),
    {Hash, Salt}.

-spec check_password(
    algorithm(),
    emqx_passwd:salt(),
    emqx_passwd:hash(),
    emqx_passwd:password()
) -> boolean().
check_password(#{name := bcrypt}, _Salt, PasswordHash, Password) ->
    emqx_passwd:check_pass({bcrypt, PasswordHash}, PasswordHash, Password);
check_password(
    #{
        name := pbkdf2,
        mac_fun := MacFun,
        iterations := Iterations
    } = Algorithm,
    Salt,
    PasswordHash,
    Password
) ->
    DKLength = maps:get(dk_length, Algorithm, undefined),
    emqx_passwd:check_pass({pbkdf2, MacFun, Salt, Iterations, DKLength}, PasswordHash, Password);
check_password(#{name := Other, salt_position := SaltPosition}, Salt, PasswordHash, Password) ->
    emqx_passwd:check_pass({Other, Salt, SaltPosition}, PasswordHash, Password).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

rw_refs() ->
    [
        hoconsc:ref(?MODULE, bcrypt_rw),
        hoconsc:ref(?MODULE, pbkdf2),
        hoconsc:ref(?MODULE, other_algorithms)
    ].

ro_refs() ->
    [
        hoconsc:ref(?MODULE, bcrypt),
        hoconsc:ref(?MODULE, pbkdf2),
        hoconsc:ref(?MODULE, other_algorithms)
    ].
