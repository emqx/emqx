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
-module(emqx_dashboard_schema).

-include_lib("typerefl/include/types.hrl").

-export([ roots/0
        , fields/1]).

roots() -> ["emqx_dashboard"].

fields("emqx_dashboard") ->
    [ {listeners, hoconsc:array(hoconsc:union([hoconsc:ref(?MODULE, "http"),
                                               hoconsc:ref(?MODULE, "https")]))}
    , {default_username, fun default_username/1}
    , {default_password, fun default_password/1}
    , {sample_interval, emqx_schema:t(emqx_schema:duration_s(), undefined, "10s")}
    , {token_expired_time, emqx_schema:t(emqx_schema:duration(), undefined, "30m")}
    ];

fields("http") ->
    [ {"protocol", hoconsc:enum([http, https])}
    , {"port", emqx_schema:t(integer(), undefined, 18083)}
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

default_username(type) -> string();
default_username(default) -> "admin";
default_username(nullable) -> false;
default_username(_) -> undefined.

default_password(type) -> string();
default_password(default) -> "public";
default_password(nullable) -> false;
default_password(_) -> undefined.
