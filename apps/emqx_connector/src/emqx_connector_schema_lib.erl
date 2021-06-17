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
-module(emqx_connector_schema_lib).

-include("emqx_connector.hrl").
-include_lib("typerefl/include/types.hrl").

-export([ relational_db_fields/0
        , ssl_fields/0
        ]).

-export([ to_ip_port/1
        , ip_port_to_string/1
        , to_servers/1
        ]).

-export([ pool_size/1
        , database/1
        , username/1
        , password/1
        , servers/1
        , auto_reconnect/1
        ]).

-typerefl_from_string({ip_port/0, emqx_connector_schema_lib, to_ip_port}).
-typerefl_from_string({servers/0, emqx_connector_schema_lib, to_servers}).

-type database() :: binary().
-type pool_size() :: integer().
-type username() :: binary().
-type password() :: binary().
-type servers() :: list().

-reflect_type([ database/0
              , pool_size/0
              , username/0
              , password/0
              , servers/0
             ]).

relational_db_fields() ->
    [ {server, fun server/1}
    , {database, fun database/1}
    , {pool_size, fun pool_size/1}
    , {username, fun username/1}
    , {password, fun password/1}
    , {auto_reconnect, fun auto_reconnect/1}
    ].

ssl_fields() ->
    [ {ssl, fun ssl/1}
    , {cacertfile, fun cacertfile/1}
    , {keyfile, fun keyfile/1}
    , {certfile, fun certfile/1}
    , {verify, fun verify/1}
    ].

server(type) -> emqx_schema:ip_port();
server(validator) -> [?REQUIRED("the field 'server' is required")];
server(_) -> undefined.

database(type) -> binary();
database(validator) -> [?REQUIRED("the field 'database' is required")];
database(_) -> undefined.

pool_size(type) -> integer();
pool_size(default) -> 8;
pool_size(validator) -> [?MIN(1), ?MAX(64)];
pool_size(_) -> undefined.

username(type) -> binary();
username(default) -> "root";
username(_) -> undefined.

password(type) -> binary();
password(default) -> "";
password(_) -> undefined.

auto_reconnect(type) -> boolean();
auto_reconnect(default) -> true;
auto_reconnect(_) -> undefined.

ssl(type) -> boolean();
ssl(default) -> false;
ssl(_) -> undefined.

cacertfile(type) -> binary();
cacertfile(default) -> "";
cacertfile(_) -> undefined.

keyfile(type) -> binary();
keyfile(default) -> "";
keyfile(_) -> undefined.

certfile(type) -> binary();
certfile(default) -> "";
certfile(_) -> undefined.

verify(type) -> boolean();
verify(default) -> false;
verify(_) -> undefined.

servers(type) -> servers();
servers(validator) -> [?REQUIRED("the field 'servers' is required")];
servers(_) -> undefined.

to_ip_port(Str) ->
     case string:tokens(Str, ":") of
         [Ip, Port] ->
             case inet:parse_address(Ip) of
                 {ok, R} -> {ok, {R, list_to_integer(Port)}};
                 _ -> {error, Str}
             end;
         _ -> {error, Str}
     end.

ip_port_to_string({Ip, Port}) ->
    iolist_to_binary([inet:ntoa(Ip), ":", integer_to_list(Port)]).

to_servers(Str) ->
    {ok, lists:map(fun(Server) ->
             case string:tokens(Server, ":") of
                 [Ip] ->
                     [{host, Ip}];
                 [Ip, Port] ->
                     [{host, Ip}, {port, list_to_integer(Port)}]
             end
         end, string:tokens(Str, " , "))}.
