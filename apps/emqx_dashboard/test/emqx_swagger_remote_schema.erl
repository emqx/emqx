%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_swagger_remote_schema).

-include_lib("typerefl/include/types.hrl").

-export([roots/0, fields/1]).
-import(hoconsc, [mk/2]).
roots() -> ["root"].

fields("root") ->
    [
        {listeners,
            hoconsc:array(
                hoconsc:union([
                    hoconsc:ref(?MODULE, "ref1"),
                    hoconsc:ref(?MODULE, "ref2")
                ])
            )},
        {default_username, fun default_username/1},
        {default_password, fun default_password/1},
        {sample_interval, mk(emqx_schema:duration_s(), #{default => "10s"})},
        {token_expired_time, mk(emqx_schema:duration(), #{default => "30m"})}
    ];
fields("ref1") ->
    [
        {"protocol", hoconsc:enum([http, https])},
        {"port", mk(integer(), #{default => 18083})}
    ];
fields("ref2") ->
    [
        {page, mk(range(1, 100), #{desc => <<"good page">>})},
        {another_ref, hoconsc:ref(?MODULE, "ref3")}
    ];
fields("ref3") ->
    [
        {ip, mk(emqx_schema:ip_port(), #{desc => <<"IP:Port">>, example => "127.0.0.1:80"})},
        {version, mk(string(), #{desc => "a good version", example => "1.0.0"})}
    ].

default_username(type) -> string();
default_username(default) -> "admin";
default_username(required) -> true;
default_username(_) -> undefined.

default_password(type) -> string();
default_password(default) -> "public";
default_password(required) -> true;
default_password(_) -> undefined.
