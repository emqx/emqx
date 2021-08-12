%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authn_implied_schema).

-include("emqx_authn.hrl").
-include_lib("typerefl/include/types.hrl").

-behaviour(hocon_schema).

-export([ structs/0
        , fields/1
        ]).

structs() -> [ "filename", "user_info", "new_user_info"].

fields("filename") ->
    [ {filename, fun filename/1} ];
fields("user_info") ->
    [ {user_id, fun user_id/1}
    , {password, fun password/1}
    ];
fields("new_user_info") ->
    [ {password, fun password/1}
    ].

filename(type) -> string();
filename(nullable) -> false;
filename(_) -> undefined.

user_id(type) -> binary();
user_id(nullable) -> false;
user_id(_) -> undefined.

password(type) -> binary();
password(nullable) -> false;
password(_) -> undefined.

