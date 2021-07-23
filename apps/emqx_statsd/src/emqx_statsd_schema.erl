-module(emqx_statsd_schema).

-include_lib("typerefl/include/types.hrl").

-behaviour(hocon_schema).

-export([to_ip_port/1]).

-export([ structs/0
        , fields/1]).

-typerefl_from_string({ip_port/0, emqx_statsd_schema, to_ip_port}).

structs() -> ["emqx_statsd"].

fields("emqx_statsd") ->
    [ {enable, emqx_schema:t(boolean(), undefined, false)}
    , {server, fun server/1}
    , {sample_time_interval, fun duration_ms/1}
    , {flush_time_interval,  fun duration_ms/1}
    ].

server(type) -> emqx_schema:ip_port();
server(default) -> "127.0.0.1:8125";
server(nullable) -> false;
server(_) -> undefined.

duration_ms(type) -> emqx_schema:duration_ms();
duration_ms(nullable) -> false;
duration_ms(default) -> "10s";
duration_ms(_) -> undefined.

to_ip_port(Str) ->
     case string:tokens(Str, ":") of
         [Ip, Port] ->
             case inet:parse_address(Ip) of
                 {ok, R} -> {ok, {R, list_to_integer(Port)}};
                 _ -> {error, Str}
             end;
         _ -> {error, Str}
     end.
