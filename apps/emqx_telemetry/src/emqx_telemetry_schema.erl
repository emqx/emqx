-module(emqx_telemetry_schema).

-include_lib("typerefl/include/types.hrl").

-behaviour(hocon_schema).

-export([ structs/0
        , fields/1]).

structs() -> ["emqx_telemetry"].

fields("emqx_telemetry") ->
    [{enabled, emqx_schema:t(boolean(), undefined, false)}].
