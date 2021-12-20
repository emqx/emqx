-module(emqx_retainer_schema).

-include_lib("typerefl/include/types.hrl").

-export([roots/0, fields/1]).

-define(TYPE(Type), hoconsc:mk(Type)).

roots() -> ["emqx_retainer"].

fields("emqx_retainer") ->
    [ {enable, sc(boolean(), false)}
    , {msg_expiry_interval, sc(emqx_schema:duration_ms(), "0s")}
    , {msg_clear_interval, sc(emqx_schema:duration_ms(), "0s")}
    , {flow_control, ?TYPE(hoconsc:ref(?MODULE, flow_control))}
    , {max_payload_size, sc(emqx_schema:bytesize(), "1MB")}
    , {stop_publish_clear_msg, sc(boolean(), false)}
    , {config, config()}
    ];

fields(mnesia_config) ->
    [ {type, ?TYPE(hoconsc:union([built_in_database]))}
    , {storage_type, sc(hoconsc:union([ram, disc, disc_only]), ram)}
    , {max_retained_messages, sc(integer(), 0, fun is_pos_integer/1)}
    ];

fields(flow_control) ->
    [ {max_read_number, sc(integer(), 0, fun is_pos_integer/1)}
    , {msg_deliver_quota, sc(integer(), 0, fun is_pos_integer/1)}
    , {quota_release_interval, sc(emqx_schema:duration_ms(), "0ms")}
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

config() ->
    #{type => hoconsc:union([hoconsc:ref(?MODULE, mnesia_config)])}.
