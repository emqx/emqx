-module(emqx_connector_schema_lib).
-include_lib("typerefl/include/types.hrl").

-export([ relational_db_fields/0
        , ssl_fields/0
        ]).

-export([ to_ip_port/1
        , ip_port_to_string/1
        ]).

-typerefl_from_string({ip_port/0, emqx_connector_schema_lib, to_ip_port}).

-reflect_type([ip_port/0]).

-type ip_port() :: tuple().

-define(VALID, emqx_resource_validator).
-define(REQUIRED(MSG), ?VALID:required(MSG)).
-define(MAX(MAXV), ?VALID:max(number, MAXV)).
-define(MIN(MINV), ?VALID:min(number, MINV)).

relational_db_fields() ->
    [ {server, fun server/1}
    , {database, fun database/1}
    , {pool_size, fun pool_size/1}
    , {user, fun user/1}
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

server(mapping) -> "config.server";
server(type) -> ip_port();
server(validator) -> [?REQUIRED("the field 'server' is required")];
server(_) -> undefined.

database(mapping) -> "config.database";
database(type) -> string();
database(validator) -> [?REQUIRED("the field 'server' is required")];
database(_) -> undefined.

pool_size(mapping) -> "config.pool_size";
pool_size(type) -> integer();
pool_size(default) -> 8;
pool_size(validator) -> [?MIN(1), ?MAX(64)];
pool_size(_) -> undefined.

user(mapping) -> "config.user";
user(type) -> string();
user(default) -> "root";
user(_) -> undefined.

password(mapping) -> "config.password";
password(type) -> string();
password(default) -> "";
password(_) -> undefined.

auto_reconnect(mapping) -> "config.auto_reconnect";
auto_reconnect(type) -> boolean();
auto_reconnect(default) -> true;
auto_reconnect(_) -> undefined.
ssl(mapping) -> "config.ssl";
ssl(type) -> boolean();
ssl(default) -> false;
ssl(_) -> undefined.

cacertfile(mapping) -> "config.cacertfile";
cacertfile(type) -> string();
cacertfile(default) -> "";
cacertfile(_) -> undefined.

keyfile(mapping) -> "config.keyfile";
keyfile(type) -> string();
keyfile(default) -> "";
keyfile(_) -> undefined.

certfile(mapping) -> "config.certfile";
certfile(type) -> string();
certfile(default) -> "";
certfile(_) -> undefined.

verify(mapping) -> "config.verify";
verify(type) -> boolean();
verify(default) -> false;
verify(_) -> undefined.

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
    inet:ntoa(Ip) ++ ":" ++ integer_to_list(Port).
