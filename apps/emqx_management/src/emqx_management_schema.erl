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
-module(emqx_management_schema).

-include_lib("typerefl/include/types.hrl").

-behaviour(hocon_schema).

-export([ structs/0
        , fields/1]).

structs() -> ["emqx_management"].

fields("emqx_management") ->
    [ {applications, hoconsc:array(hoconsc:ref(?MODULE, "application"))}
    , {max_row_limit, fun max_row_limit/1}
    , {listeners, hoconsc:array(hoconsc:union([hoconsc:ref(?MODULE, "http"), hoconsc:ref(?MODULE, "https")]))}
    ];

fields("application") ->
    [ {"id", emqx_schema:t(string(), undefined, "admin")}
    , {"secret", emqx_schema:t(string(), undefined, "public")}
    ];


fields("http") ->
    [ {"protocol", hoconsc:enum([http, https])}
    , {"port", emqx_schema:t(integer(), undefined, 8081)}
    , {"num_acceptors", emqx_schema:t(integer(), undefined, 4)}
    , {"max_connections", emqx_schema:t(integer(), undefined, 512)}
    , {"backlog", emqx_schema:t(integer(), undefined, 1024)}
    , {"send_timeout", emqx_schema:t(emqx_schema:duration(), undefined, "15s")}
    , {"send_timeout_close", emqx_schema:t(boolean(), undefined, true)}
    , {"inet6", emqx_schema:t(boolean(), undefined, false)}
    , {"ipv6_v6only", emqx_schema:t(boolean(), undefined, false)}
    ];

fields("https") ->
    emqx_schema:ssl(#{enable => true}) ++ fields("http").

max_row_limit(type) -> integer();
max_row_limit(default) -> 1000;
max_row_limit(nullable) -> false;
max_row_limit(_) -> undefined.
