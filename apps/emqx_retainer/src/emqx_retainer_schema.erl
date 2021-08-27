-module(emqx_retainer_schema).

-include_lib("typerefl/include/types.hrl").

-export([roots/0, fields/1]).

-define(TYPE(Type), hoconsc:t(Type)).

roots() -> ["emqx_retainer"].

fields("emqx_retainer") ->
    [ {enable, t(boolean(), false)}
    , {msg_expiry_interval, t(emqx_schema:duration_ms(), "0s")}
    , {msg_clear_interval, t(emqx_schema:duration_ms(), "0s")}
    , {flow_control, ?TYPE(hoconsc:ref(?MODULE, flow_control))}
    , {max_payload_size, t(emqx_schema:bytesize(), "1MB")}
    , {config, config()}
    ];

fields(mnesia_config) ->
    [ {type, ?TYPE(hoconsc:union([built_in_database]))}
    , {storage_type, t(hoconsc:union([ram, disc, disc_only]), ram)}
    , {max_retained_messages, t(integer(), 0, fun is_pos_integer/1)}
    ];

fields(flow_control) ->
    [ {max_read_number, t(integer(), 0, fun is_pos_integer/1)}
    , {msg_deliver_quota, t(integer(), 0, fun is_pos_integer/1)}
    , {quota_release_interval, t(emqx_schema:duration_ms(), "0ms")}
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

config() ->
    #{type => hoconsc:union([hoconsc:ref(?MODULE, mnesia_config)])}.
