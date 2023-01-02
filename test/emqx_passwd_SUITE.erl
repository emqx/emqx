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

-module(emqx_passwd_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

all() -> [t_hash].

t_hash(_) ->
    Password = <<"password">>, Salt = <<"salt">>,
    _ = emqx_passwd:hash(plain, Password),
    _ = emqx_passwd:hash(md5, Password),
    _ = emqx_passwd:hash(sha, Password),
    _ = emqx_passwd:hash(sha256, Password),
    _ = emqx_passwd:hash(bcrypt, {Salt, Password}),
    _ = emqx_passwd:hash(pbkdf2, {Salt, Password, sha256, 1000, 20}).
