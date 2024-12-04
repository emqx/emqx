%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_passwd_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(bcrypt),
    Config.

end_per_suite(_Config) ->
    ok.

t_hash_data(_) ->
    Password = <<"password">>,
    Password = emqx_passwd:hash_data(plain, Password),

    <<"5f4dcc3b5aa765d61d8327deb882cf99">> =
        emqx_passwd:hash_data(md5, Password),

    <<"5baa61e4c9b93f3f0682250b6cf8331b7ee68fd8">> =
        emqx_passwd:hash_data(sha, Password),

    <<"5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8">> =
        emqx_passwd:hash_data(sha256, Password),

    Sha512 = iolist_to_binary(
        [
            <<"b109f3bbbc244eb82441917ed06d618b9008dd09b3befd1b5e07394c706a8bb9">>,
            <<"80b1d7785e5976ec049b46df5f1326af5a2ea6d103fd07c95385ffab0cacbc86">>
        ]
    ),

    Sha512 = emqx_passwd:hash_data(sha512, Password).

t_hash(_) ->
    Password = <<"password">>,
    Salt = <<"salt">>,
    WrongPassword = <<"wrongpass">>,

    Md5 = <<"67a1e09bb1f83f5007dc119c14d663aa">>,
    Md5 = emqx_passwd:hash({md5, Salt, prefix}, Password),
    true = emqx_passwd:check_pass({md5, Salt, prefix}, Md5, Password),
    false = emqx_passwd:check_pass({md5, Salt, prefix}, Md5, WrongPassword),
    ?assertEqual(
        emqx_passwd:hash_data(md5, Password),
        emqx_passwd:hash({md5, Salt, disable}, Password)
    ),

    Sha = <<"59b3e8d637cf97edbe2384cf59cb7453dfe30789">>,
    Sha = emqx_passwd:hash({sha, Salt, prefix}, Password),
    true = emqx_passwd:check_pass({sha, Salt, prefix}, Sha, Password),
    false = emqx_passwd:check_pass({sha, Salt, prefix}, Sha, WrongPassword),
    ?assertEqual(
        emqx_passwd:hash_data(sha, Password),
        emqx_passwd:hash({sha, Salt, disable}, Password)
    ),

    Sha256 = <<"7a37b85c8918eac19a9089c0fa5a2ab4dce3f90528dcdeec108b23ddf3607b99">>,
    Sha256 = emqx_passwd:hash({sha256, Salt, suffix}, Password),
    true = emqx_passwd:check_pass({sha256, Salt, suffix}, Sha256, Password),
    false = emqx_passwd:check_pass({sha256, Salt, suffix}, Sha256, WrongPassword),
    ?assertEqual(
        emqx_passwd:hash_data(sha256, Password),
        emqx_passwd:hash({sha256, Salt, disable}, Password)
    ),

    Sha512 = iolist_to_binary(
        [
            <<"fa6a2185b3e0a9a85ef41ffb67ef3c1fb6f74980f8ebf970e4e72e353ed9537d">>,
            <<"593083c201dfd6e43e1c8a7aac2bc8dbb119c7dfb7d4b8f131111395bd70e97f">>
        ]
    ),
    Sha512 = emqx_passwd:hash({sha512, Salt, suffix}, Password),
    true = emqx_passwd:check_pass({sha512, Salt, suffix}, Sha512, Password),
    false = emqx_passwd:check_pass({sha512, Salt, suffix}, Sha512, WrongPassword),
    ?assertEqual(
        emqx_passwd:hash_data(sha512, Password),
        emqx_passwd:hash({sha512, Salt, disable}, Password)
    ),

    BcryptSalt = <<"$2b$12$wtY3h20mUjjmeaClpqZVve">>,
    Bcrypt = <<"$2b$12$wtY3h20mUjjmeaClpqZVvehyw7F.V78F3rbK2xDkCzRTMi6pmfUB6">>,
    Bcrypt = emqx_passwd:hash({bcrypt, BcryptSalt}, Password),
    true = emqx_passwd:check_pass({bcrypt, Bcrypt}, Bcrypt, Password),
    false = emqx_passwd:check_pass({bcrypt, Bcrypt}, Bcrypt, WrongPassword),
    false = emqx_passwd:check_pass({bcrypt, <<>>}, <<>>, WrongPassword),

    %% Invalid salt, bcrypt fails
    ?assertException(error, _, emqx_passwd:hash({bcrypt, Salt}, Password)),

    BadDKlen = 1 bsl 32,
    Pbkdf2Salt = <<"ATHENA.MIT.EDUraeburn">>,
    Pbkdf2 = <<
        "01dbee7f4a9e243e988b62c73cda935d"
        "a05378b93244ec8f48a99e61ad799d86"
    >>,
    _Pbkdf2 = emqx_passwd:hash({pbkdf2, sha, Pbkdf2Salt, 2, 32}, Password),
    true = emqx_passwd:check_pass({pbkdf2, sha, Pbkdf2Salt, 2, 32}, Pbkdf2, Password),
    false = emqx_passwd:check_pass({pbkdf2, sha, Pbkdf2Salt, 2, 32}, Pbkdf2, WrongPassword),
    ?assertException(
        error, _, emqx_passwd:check_pass({pbkdf2, sha, Pbkdf2Salt, 2, BadDKlen}, Pbkdf2, Password)
    ),

    %% Invalid derived_length, pbkdf2 fails
    ?assertException(error, _, emqx_passwd:hash({pbkdf2, sha, Pbkdf2Salt, 2, BadDKlen}, Password)),

    %% invalid salt (not binary)
    ?assertException(
        error,
        {salt_not_string, false},
        emqx_passwd:hash({sha256, false, suffix}, Password)
    ),

    %% invalid password (not binary)
    ?assertException(
        error,
        {password_not_string, bad_password_type},
        emqx_passwd:hash({sha256, Salt, suffix}, bad_password_type)
    ).
