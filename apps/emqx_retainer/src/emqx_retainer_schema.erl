-module(emqx_retainer_schema).

-include_lib("typerefl/include/types.hrl").

-type storage_type() :: ram | disc | disc_only.

-reflect_type([storage_type/0]).

-export([structs/0, fields/1]).

structs() -> ["emqx_retainer"].

fields("emqx_retainer") ->
    [ {storage_type, t(storage_type(), ram)}
    , {max_retained_messages, t(integer(), 0, fun is_pos_integer/1)}
    , {max_payload_size, t(emqx_schema:bytesize(), "1MB")}
    , {expiry_interval, t(emqx_schema:duration_ms(), "0s")}
    ].

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
t(Type, Default) ->
    hoconsc:t(Type, #{default => Default}).

t(Type, Default, Validator) ->
    hoconsc:t(Type, #{default => Default,
                      validator => Validator}).

is_pos_integer(V) ->
    V >= 0.
