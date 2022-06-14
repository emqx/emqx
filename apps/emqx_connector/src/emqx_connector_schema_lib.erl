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
-module(emqx_connector_schema_lib).

-include("emqx_connector.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-export([
    relational_db_fields/0,
    ssl_fields/0,
    prepare_statement_fields/0
]).

-export([
    ip_port_to_string/1,
    parse_server/2
]).

-export([
    pool_size/1,
    database/1,
    username/1,
    password/1,
    auto_reconnect/1
]).

-type database() :: binary().
-type pool_size() :: pos_integer().
-type username() :: binary().
-type password() :: binary().

-reflect_type([
    database/0,
    pool_size/0,
    username/0,
    password/0
]).

-export([roots/0, fields/1]).

roots() -> [].

fields(_) -> [].

ssl_fields() ->
    [
        {ssl, #{
            type => hoconsc:ref(emqx_schema, "ssl_client_opts"),
            default => #{<<"enable">> => false},
            desc => ?DESC("ssl")
        }}
    ].

relational_db_fields() ->
    [
        {database, fun database/1},
        {pool_size, fun pool_size/1},
        {username, fun username/1},
        {password, fun password/1},
        {auto_reconnect, fun auto_reconnect/1}
    ].

prepare_statement_fields() ->
    [{prepare_statement, fun prepare_statement/1}].

prepare_statement(type) -> map();
prepare_statement(desc) -> ?DESC("prepare_statement");
prepare_statement(required) -> false;
prepare_statement(_) -> undefined.

database(type) -> binary();
database(desc) -> ?DESC("database_desc");
database(required) -> true;
database(validator) -> [?NOT_EMPTY("the value of the field 'database' cannot be empty")];
database(_) -> undefined.

pool_size(type) -> pos_integer();
pool_size(desc) -> ?DESC("pool_size");
pool_size(default) -> 8;
pool_size(validator) -> [?MIN(1)];
pool_size(_) -> undefined.

username(type) -> binary();
username(desc) -> ?DESC("username");
username(required) -> false;
username(_) -> undefined.

password(type) -> binary();
password(desc) -> ?DESC("password");
password(required) -> false;
password(_) -> undefined.

auto_reconnect(type) -> boolean();
auto_reconnect(desc) -> ?DESC("auto_reconnect");
auto_reconnect(default) -> true;
auto_reconnect(_) -> undefined.

ip_port_to_string({Ip, Port}) when is_list(Ip) ->
    iolist_to_binary([Ip, ":", integer_to_list(Port)]);
ip_port_to_string({Ip, Port}) when is_tuple(Ip) ->
    iolist_to_binary([inet:ntoa(Ip), ":", integer_to_list(Port)]).

parse_server(Str, #{host_type := inet_addr, default_port := DefaultPort}) ->
    case string:tokens(str(Str), ": ") of
        [Ip, Port] ->
            {parse_ip(Ip), parse_port(Port)};
        [Ip] ->
            {parse_ip(Ip), DefaultPort};
        _ ->
            throw("Bad server schema")
    end;
parse_server(Str, #{host_type := hostname, default_port := DefaultPort}) ->
    case string:tokens(str(Str), ": ") of
        [Hostname, Port] ->
            {Hostname, parse_port(Port)};
        [Hostname] ->
            {Hostname, DefaultPort};
        _ ->
            throw("Bad server schema")
    end;
parse_server(_, _) ->
    throw("Invalid Host").

parse_ip(Str) ->
    case inet:parse_address(Str) of
        {ok, R} ->
            R;
        _ ->
            %% check is a rfc1035's hostname
            case inet_parse:domain(Str) of
                true ->
                    Str;
                _ ->
                    throw("Bad IP or Host")
            end
    end.

parse_port(Port) ->
    try
        list_to_integer(Port)
    catch
        _:_ ->
            throw("Bad port number")
    end.

str(A) when is_atom(A) ->
    atom_to_list(A);
str(B) when is_binary(B) ->
    binary_to_list(B);
str(S) when is_list(S) ->
    S.
