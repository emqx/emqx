-module(emqx_retainer_schema).

-include_lib("typerefl/include/types.hrl").

-export([roots/0, fields/1, namespace/0]).

-define(TYPE(Type), hoconsc:mk(Type)).

namespace() -> "retainer".

roots() -> ["retainer"].

fields("retainer") ->
    [ {enable, sc(boolean(), false)}
    , {msg_expiry_interval, sc(emqx_schema:duration_ms(), "0s")}
    , {msg_clear_interval, sc(emqx_schema:duration_ms(), "0s")}
    , {flow_control, ?TYPE(hoconsc:ref(?MODULE, flow_control))}
    , {max_payload_size, sc(emqx_schema:bytesize(), "1MB")}
    , {stop_publish_clear_msg, sc(boolean(), false)}
    , {backend, backend_config()}
    ];

fields(mnesia_config) ->
    [ {type, ?TYPE(hoconsc:union([built_in_database]))}
    , {storage_type, sc(hoconsc:union([ram, disc]), ram)}
    , {max_retained_messages, sc(integer(), 0, fun is_pos_integer/1)}
    ];

fields(flow_control) ->
    [ {batch_read_number, sc(integer(), 0, fun is_pos_integer/1)}
    , {batch_deliver_number, sc(range(0, 1000), 0)}
    , {limiter, sc(emqx_schema:map("limiter's type", atom()), #{})}
    ].

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
sc(Type, Default) ->
    hoconsc:mk(Type, #{default => Default}).

sc(Type, Default, Validator) ->
    hoconsc:mk(Type, #{default => Default,
                       validator => Validator}).

is_pos_integer(V) ->
    V >= 0.

backend_config() ->
    #{type => hoconsc:union([hoconsc:ref(?MODULE, mnesia_config)])}.
