-module(emqx_statsd_schema).

-include_lib("typerefl/include/types.hrl").

-behaviour(hocon_schema).

-export([ structs/0
        , fields/1]).

structs() -> ["emqx_statsd"].

fields("emqx_statsd") ->
    [ {server, fun server/1}
    , {prefix, fun prefix/1}
    , {tags, map()}
    , {batch_size, fun batch_size/1}
    , {sample_time_interval, fun duration_s/1}
    , {flush_time_interval,  fun duration_s/1}].

server(type) -> emqx_schema:ip_port();
server(default) -> "192.168.1.1:8125";
server(nullable) -> false;
server(_) -> undefined.

prefix(type) -> string();
prefix(default) -> "emqx";
prefix(nullable) -> true;
prefix(_) -> undefined.

batch_size(type) -> integer();
batch_size(nullable) -> false;
batch_size(default) -> 10;
batch_size(_) -> undefined.

duration_s(type) -> emqx_schema:duration_s();
duration_s(nullable) -> false;
duration_s(default) -> "10s";
duration_s(_) -> undefined.
