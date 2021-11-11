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
        , fields/1
        ,namespace/0]).

namespace() -> <<"dashboard">>.
roots() -> ["emqx_dashboard"].

fields("emqx_dashboard") ->
    [ {listeners, hoconsc:array(hoconsc:union([hoconsc:ref(?MODULE, "http"),
                                               hoconsc:ref(?MODULE, "https")]))}
    , {default_username, fun default_username/1}
    , {default_password, fun default_password/1}
    , {sample_interval, sc(emqx_schema:duration_s(), #{default => "10s"})}
    , {token_expired_time, sc(emqx_schema:duration(), #{default => "30m"})}
    , {cors, fun cors/1}
    ];

fields("http") ->
    [ {"protocol", hoconsc:enum([http, https])}
    , {"port", hoconsc:mk(integer(), #{default => 18083})}
    , {"num_acceptors", sc(integer(), #{default => 4})}
    , {"max_connections", sc(integer(), #{default => 512})}
    , {"backlog", sc(integer(), #{default => 1024})}
    , {"send_timeout", sc(emqx_schema:duration(), #{default => "5s"})}
    , {"inet6", sc(boolean(), #{default => false})}
    , {"ipv6_v6only", sc(boolean(), #{default => false})}
    ];

fields("https") ->
    fields("http") ++
    proplists:delete("fail_if_no_peer_cert",
                     emqx_schema:server_ssl_opts_schema(#{}, true)).

default_username(type) -> string();
default_username(default) -> "admin";
default_username(nullable) -> false;
default_username(_) -> undefined.

default_password(type) -> string();
default_password(default) -> "public";
default_password(nullable) -> false;
default_password(sensitive) -> true;
default_password(desc) -> """
The initial default password for dashboard 'admin' user.
For safty, it should be changed as soon as possible.""";
default_password(_) -> undefined.

cors(type) -> boolean();
cors(default) -> false;
cors(nullable) -> true;
cors(_) -> undefined.

sc(Type, Meta) -> hoconsc:mk(Type, Meta).
