-module(emqx_retainer_schema).

-include_lib("typerefl/include/types.hrl").

-export([roots/0, fields/1, namespace/0]).

-define(TYPE(Type), hoconsc:mk(Type)).

namespace() -> "retainer".

roots() -> ["retainer"].

fields("retainer") ->
    [ {enable, sc(boolean(), "Enable retainer feature.", false)}
    , {msg_expiry_interval, sc(emqx_schema:duration_ms(),
                               "Message retention time. 0 means message will never be expired.",
                               "0s")}
    , {msg_clear_interval, sc(emqx_schema:duration_ms(),
                              "Periodic interval for cleaning up expired messages. "
                              "Never clear if the value is 0.",
                              "0s")}
    , {flow_control, ?TYPE(hoconsc:ref(?MODULE, flow_control))}
    , {max_payload_size, sc(emqx_schema:bytesize(),
                            "Maximum retained message size.",
                            "1MB")}
    , {stop_publish_clear_msg, sc(boolean(),
                                  "When the retained flag of the `PUBLISH` message is set and Payload is empty, "
                                  "whether to continue to publish the message.<br/>"
                                  "See: http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718038",
                                  false)}
    , {backend, backend_config()}
    ];

fields(mnesia_config) ->
    [ {type, hoconsc:mk(hoconsc:union([built_in_database]), #{desc => "Backend type."})}
    , {storage_type, sc(hoconsc:union([ram, disc]),
                        "Specifies whether the messages are stored in RAM or persisted on disc.",
                        ram)}
    , {max_retained_messages, sc(integer(),
                                 "Maximum number of retained messages. 0 means no limit.",
                                 0,
                                 fun is_pos_integer/1)}
    ];

fields(flow_control) ->
    [ {batch_read_number, sc(integer(),
                             "Size of the batch when reading messages from storage. 0 means no limit.",
                             0,
                             fun is_pos_integer/1)}
    , {batch_deliver_number, sc(range(0, 1000),
                                "The number of retained messages can be delivered per batch.",
                                0)}
    , {batch_deliver_limiter, sc(emqx_limiter_schema:bucket_name(),
                                 "The rate limiter name for retained messages' delivery.<br/>"
                                 "Limiter helps to avoid delivering too many messages to the client at once, which may cause the client "
                                 "to block or crash, or drop messages due to exceeding the size of the message queue.<br/>"
                                 "The names of the available rate limiters are taken from the existing rate limiters under `limiter.batch`.<br/>"
                                 "If this field is empty, limiter is not used.",
                                 undefined)}
    ].

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
sc(Type, Desc, Default) ->
    hoconsc:mk(Type, #{default => Default, desc => Desc}).

sc(Type, Desc, Default, Validator) ->
    hoconsc:mk(Type, #{default => Default,
                       desc => Desc,
                       validator => Validator}).

is_pos_integer(V) ->
    V >= 0.

backend_config() ->
    #{type => hoconsc:union([hoconsc:ref(?MODULE, mnesia_config)])}.
