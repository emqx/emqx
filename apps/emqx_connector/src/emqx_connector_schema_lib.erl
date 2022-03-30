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

-export([ relational_db_fields/0
        , ssl_fields/0
        ]).

-export([ ip_port_to_string/1
        , parse_server/2
        ]).

-export([ pool_size/1
        , database/1
        , username/1
        , password/1
        , auto_reconnect/1
        ]).

-type database() :: binary().
-type pool_size() :: integer().
-type username() :: binary().
-type password() :: binary().

-reflect_type([ database/0
              , pool_size/0
              , username/0
              , password/0
              ]).

-export([roots/0, fields/1]).

roots() -> [].

fields(_) -> [].

ssl_fields() ->
    [ {ssl, #{type => hoconsc:ref(emqx_schema, ssl_client_opts),
              default => #{<<"enable">> => false},
              desc => "SSL connection settings."
             }
      }
    ].

relational_db_fields() ->
    [ {database, fun database/1}
    , {pool_size, fun pool_size/1}
    , {username, fun username/1}
    , {password, fun password/1}
    , {auto_reconnect, fun auto_reconnect/1}
    ].

database(type) -> binary();
database(desc) -> "Database name.";
database(required) -> true;
database(validator) -> [?NOT_EMPTY("the value of the field 'database' cannot be empty")];
database(_) -> undefined.

pool_size(type) -> integer();
pool_size(desc) -> "Size of the connection pool.";
pool_size(default) -> 8;
pool_size(validator) -> [?MIN(1)];
pool_size(_) -> undefined.

username(type) -> binary();
username(desc) -> "EMQX's username in the external database.";
username(required) -> false;
username(_) -> undefined.

password(type) -> binary();
password(desc) -> "EMQX's password in the external database.";
password(required) -> false;
password(_) -> undefined.

auto_reconnect(type) -> boolean();
auto_reconnect(desc) -> "Enable automatic reconnect to the database.";
auto_reconnect(default) -> true;
auto_reconnect(_) -> undefined.

ip_port_to_string({Ip, Port}) when is_list(Ip) ->
    iolist_to_binary([Ip, ":", integer_to_list(Port)]);
ip_port_to_string({Ip, Port}) when is_tuple(Ip) ->
    iolist_to_binary([inet:ntoa(Ip), ":", integer_to_list(Port)]).

parse_server(Str, #{host_type := inet_addr, default_port := DefaultPort}) ->
    try string:tokens(str(Str), ": ") of
        [Ip, Port] ->
            case parse_ip(Ip) of
                {ok, R}    -> {R, list_to_integer(Port)}
            end;
        [Ip] ->
            case parse_ip(Ip) of
                {ok, R}    -> {R, DefaultPort}
            end;
        _ ->
            ?THROW_ERROR("Bad server schema.")
    catch
        error : Reason ->
            ?THROW_ERROR(Reason)
    end;
parse_server(Str, #{host_type := hostname, default_port := DefaultPort}) ->
    try string:tokens(str(Str), ": ") of
        [Ip, Port] ->
            {Ip, list_to_integer(Port)};
        [Ip] ->
            {Ip, DefaultPort};
        _ ->
            ?THROW_ERROR("Bad server schema.")
    catch
        error : Reason ->
            ?THROW_ERROR(Reason)
    end;
parse_server(_, _) ->
    ?THROW_ERROR("Invalid Host").

parse_ip(Str) ->
    case inet:parse_address(Str) of
        {ok, R} ->
            {ok, R};
        _ ->
            %% check is a rfc1035's hostname
            case inet_parse:domain(Str) of
                true ->
                    {ok, Str};
                _ ->
                    ?THROW_ERROR("Bad IP or Host")
            end
    end.

str(A) when is_atom(A) ->
    atom_to_list(A);
str(B) when is_binary(B) ->
    binary_to_list(B);
str(S) when is_list(S) ->
    S.
