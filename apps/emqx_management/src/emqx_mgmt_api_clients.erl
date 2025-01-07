%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mgmt_api_clients).

-feature(maybe_expr, enable).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_cm.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_utils/include/emqx_utils_api.hrl").
-include_lib("emqx/include/emqx_durable_session_metadata.hrl").

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-include("emqx_mgmt.hrl").

%% API
-export([
    api_spec/0,
    paths/0,
    schema/1,
    fields/1,
    namespace/0
]).

-export([
    clients/2,
    list_clients_v2/2,
    kickout_clients/2,
    client/2,
    subscriptions/2,
    authz_cache/2,
    subscribe/2,
    subscribe_batch/2,
    unsubscribe/2,
    unsubscribe_batch/2,
    set_keepalive/2,
    sessions_count/2,
    inflight_msgs/2,
    mqueue_msgs/2
]).

-export([
    qs2ms/2,
    run_fuzzy_filter/2,
    format_channel_info/1,
    format_channel_info/2,
    format_channel_info/3
]).

%% for batch operation
-export([do_subscribe/3]).

-ifdef(TEST).
-export([parse_cursor/2, serialize_cursor/1]).
-endif.

-define(TAGS, [<<"Clients">>]).

-define(CLIENT_QSCHEMA, [
    {<<"node">>, atom},
    %% list
    {<<"username">>, binary},
    %% list
    {<<"clientid">>, binary},
    {<<"ip_address">>, ip},
    {<<"conn_state">>, atom},
    {<<"clean_start">>, atom},
    {<<"proto_ver">>, integer},
    {<<"like_clientid">>, binary},
    {<<"like_username">>, binary},
    {<<"gte_created_at">>, timestamp},
    {<<"lte_created_at">>, timestamp},
    {<<"gte_connected_at">>, timestamp},
    {<<"lte_connected_at">>, timestamp}
]).

-define(FORMAT_FUN, {?MODULE, format_channel_info}).

-define(CLIENTID_NOT_FOUND, #{
    code => 'CLIENTID_NOT_FOUND',
    message => <<"Client ID not found">>
}).

-define(CLIENT_SHUTDOWN, #{
    code => 'CLIENT_SHUTDOWN',
    message => <<"Client connection has been shutdown">>
}).

%% tags
-define(CURSOR_VSN1, 1).
-define(CURSOR_TYPE_ETS, 1).
-define(CURSOR_TYPE_DS, 2).
%% field keys
-define(CURSOR_ETS_NODE_IDX, 1).
-define(CURSOR_ETS_CONT, 2).

namespace() -> undefined.

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true, translate_body => true}).

paths() ->
    [
        "/clients",
        "/clients_v2",
        "/clients/kickout/bulk",
        "/clients/:clientid",
        "/clients/:clientid/authorization/cache",
        "/clients/:clientid/subscriptions",
        "/clients/:clientid/subscribe",
        "/clients/:clientid/subscribe/bulk",
        "/clients/:clientid/unsubscribe",
        "/clients/:clientid/unsubscribe/bulk",
        "/clients/:clientid/keepalive",
        "/clients/:clientid/mqueue_messages",
        "/clients/:clientid/inflight_messages",
        "/sessions_count"
    ].

schema("/clients_v2") ->
    #{
        'operationId' => list_clients_v2,
        get => #{
            description => ?DESC(list_clients),
            hidden => true,
            tags => ?TAGS,
            parameters => fields(list_clients_v2_inputs),
            responses => #{
                200 =>
                    %% TODO: unhide after API is ready
                    %% emqx_dashboard_swagger:schema_with_example(?R_REF(list_clients_v2_response), #{
                    %%     <<"data">> => [client_example()],
                    %%     <<"meta">> => #{
                    %%         <<"count">> => 1,
                    %%         <<"cursor">> => <<"g2wAAAADYQFhAm0AAAACYzJq">>,
                    %%         <<"hasnext">> => true
                    %%     }
                    %% }),
                    emqx_dashboard_swagger:schema_with_example(map(), #{}),
                400 =>
                    emqx_dashboard_swagger:error_codes(
                        ['INVALID_PARAMETER'], <<"Invalid parameters">>
                    )
            }
        }
    };
schema("/clients") ->
    #{
        'operationId' => clients,
        get => #{
            description => ?DESC(list_clients),
            tags => ?TAGS,
            parameters => fields(list_clients_v1_inputs),
            responses => #{
                200 =>
                    emqx_dashboard_swagger:schema_with_example(?R_REF(clients), #{
                        <<"data">> => [client_example()],
                        <<"meta">> => #{
                            <<"count">> => 1,
                            <<"limit">> => 50,
                            <<"page">> => 1,
                            <<"hasnext">> => false
                        }
                    }),
                400 =>
                    emqx_dashboard_swagger:error_codes(
                        ['INVALID_PARAMETER'], <<"Invalid parameters">>
                    )
            }
        }
    };
schema("/clients/kickout/bulk") ->
    #{
        'operationId' => kickout_clients,
        post => #{
            description => ?DESC(kickout_clients),
            tags => ?TAGS,
            'requestBody' => emqx_dashboard_swagger:schema_with_example(
                hoconsc:array(binary()),
                ["emqx_clientid_985bb09d", "emqx_clientid_211cc01c"]
            ),
            responses => #{
                204 => <<"Kick out clients successfully">>
            },
            log_meta => emqx_dashboard_audit:importance(low)
        }
    };
schema("/clients/:clientid") ->
    #{
        'operationId' => client,
        get => #{
            description => ?DESC(clients_info_from_id),
            tags => ?TAGS,
            parameters => [{clientid, hoconsc:mk(binary(), #{in => path})}],
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    ?R_REF(client),
                    client_example()
                ),
                404 => emqx_dashboard_swagger:error_codes(
                    ['CLIENTID_NOT_FOUND'], <<"Client ID not found">>
                )
            }
        },
        delete => #{
            description => ?DESC(kick_client_id),
            tags => ?TAGS,
            parameters => [
                {clientid, hoconsc:mk(binary(), #{in => path})}
            ],
            responses => #{
                204 => <<"Kick out client successfully">>,
                404 => emqx_dashboard_swagger:error_codes(
                    ['CLIENTID_NOT_FOUND'], <<"Client ID not found">>
                )
            }
        }
    };
schema("/clients/:clientid/authorization/cache") ->
    #{
        'operationId' => authz_cache,
        get => #{
            description => ?DESC(get_authz_cache),
            tags => ?TAGS,
            parameters => [{clientid, hoconsc:mk(binary(), #{in => path})}],
            responses => #{
                200 => hoconsc:mk(hoconsc:ref(?MODULE, authz_cache), #{}),
                404 => emqx_dashboard_swagger:error_codes(
                    ['CLIENTID_NOT_FOUND'], <<"Client ID not found">>
                )
            }
        },
        delete => #{
            description => ?DESC(clean_authz_cache),
            tags => ?TAGS,
            parameters => [{clientid, hoconsc:mk(binary(), #{in => path})}],
            responses => #{
                204 => <<"Clean client authz cache successfully">>,
                404 => emqx_dashboard_swagger:error_codes(
                    ['CLIENTID_NOT_FOUND'], <<"Client ID not found">>
                )
            }
        }
    };
schema("/clients/:clientid/subscriptions") ->
    #{
        'operationId' => subscriptions,
        get => #{
            description => ?DESC(get_client_subs),
            tags => ?TAGS,
            parameters => [{clientid, hoconsc:mk(binary(), #{in => path})}],
            responses => #{
                200 => hoconsc:mk(
                    hoconsc:array(hoconsc:ref(emqx_mgmt_api_subscriptions, subscription)), #{}
                ),
                404 => emqx_dashboard_swagger:error_codes(
                    ['CLIENTID_NOT_FOUND'], <<"Client ID not found">>
                )
            }
        }
    };
schema("/clients/:clientid/subscribe") ->
    #{
        'operationId' => subscribe,
        post => #{
            description => ?DESC(subscribe),
            tags => ?TAGS,
            parameters => [{clientid, hoconsc:mk(binary(), #{in => path})}],
            'requestBody' => hoconsc:mk(hoconsc:ref(?MODULE, subscribe)),
            responses => #{
                200 => hoconsc:ref(emqx_mgmt_api_subscriptions, subscription),
                404 => emqx_dashboard_swagger:error_codes(
                    ['CLIENTID_NOT_FOUND'], <<"Client ID not found">>
                )
            },
            log_meta => emqx_dashboard_audit:importance(low)
        }
    };
schema("/clients/:clientid/subscribe/bulk") ->
    #{
        'operationId' => subscribe_batch,
        post => #{
            description => ?DESC(subscribe_g),
            tags => ?TAGS,
            parameters => [{clientid, hoconsc:mk(binary(), #{in => path})}],
            'requestBody' => hoconsc:mk(hoconsc:array(hoconsc:ref(?MODULE, subscribe))),
            responses => #{
                200 => hoconsc:array(hoconsc:ref(emqx_mgmt_api_subscriptions, subscription)),
                404 => emqx_dashboard_swagger:error_codes(
                    ['CLIENTID_NOT_FOUND'], <<"Client ID not found">>
                )
            },
            log_meta => emqx_dashboard_audit:importance(low)
        }
    };
schema("/clients/:clientid/unsubscribe") ->
    #{
        'operationId' => unsubscribe,
        post => #{
            description => ?DESC(unsubscribe),
            tags => ?TAGS,
            parameters => [{clientid, hoconsc:mk(binary(), #{in => path})}],
            'requestBody' => hoconsc:mk(hoconsc:ref(?MODULE, unsubscribe)),
            responses => #{
                204 => <<"Unsubscribe OK">>,
                404 => emqx_dashboard_swagger:error_codes(
                    ['CLIENTID_NOT_FOUND'], <<"Client ID not found">>
                )
            },
            log_meta => emqx_dashboard_audit:importance(low)
        }
    };
schema("/clients/:clientid/unsubscribe/bulk") ->
    #{
        'operationId' => unsubscribe_batch,
        post => #{
            description => ?DESC(unsubscribe_g),
            tags => ?TAGS,
            parameters => [{clientid, hoconsc:mk(binary(), #{in => path})}],
            'requestBody' => hoconsc:mk(hoconsc:array(hoconsc:ref(?MODULE, unsubscribe))),
            responses => #{
                204 => <<"Unsubscribe OK">>,
                404 => emqx_dashboard_swagger:error_codes(
                    ['CLIENTID_NOT_FOUND'], <<"Client ID not found">>
                )
            },
            log_meta => emqx_dashboard_audit:importance(low)
        }
    };
schema("/clients/:clientid/keepalive") ->
    #{
        'operationId' => set_keepalive,
        put => #{
            description => ?DESC(set_keepalive_seconds),
            tags => ?TAGS,
            hidden => true,
            parameters => [{clientid, hoconsc:mk(binary(), #{in => path})}],
            'requestBody' => hoconsc:mk(hoconsc:ref(?MODULE, keepalive)),
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    ?R_REF(client),
                    client_example()
                ),
                404 => emqx_dashboard_swagger:error_codes(
                    ['CLIENTID_NOT_FOUND'], <<"Client ID not found">>
                )
            }
        }
    };
schema("/clients/:clientid/mqueue_messages") ->
    ContExample = <<"1710785444656449826_10">>,
    RespSchema = ?R_REF(mqueue_messages),
    client_msgs_schema(mqueue_msgs, ?DESC(get_client_mqueue_msgs), ContExample, RespSchema);
schema("/clients/:clientid/inflight_messages") ->
    ContExample = <<"1710785444656449826">>,
    RespSchema = ?R_REF(inflight_messages),
    client_msgs_schema(inflight_msgs, ?DESC(get_client_inflight_msgs), ContExample, RespSchema);
schema("/sessions_count") ->
    #{
        'operationId' => sessions_count,
        get => #{
            description => ?DESC(get_sessions_count),
            tags => ?TAGS,
            parameters => [
                {since,
                    hoconsc:mk(non_neg_integer(), #{
                        in => query,
                        required => false,
                        default => 0,
                        desc =>
                            <<"Include sessions expired after this time (UNIX Epoch in seconds precision)">>,
                        example => 1705391625
                    })}
            ],
            responses => #{
                200 => hoconsc:mk(binary(), #{
                    desc => <<"Number of sessions">>
                }),
                400 => emqx_dashboard_swagger:error_codes(
                    ['BAD_REQUEST'], <<"Node {name} cannot handle this request.">>
                )
            }
        }
    }.

fields(list_clients_v2_inputs) ->
    [
        hoconsc:ref(emqx_dashboard_swagger, cursor)
        | fields(common_list_clients_input)
    ];
fields(list_clients_v1_inputs) ->
    [
        hoconsc:ref(emqx_dashboard_swagger, page),
        {node,
            hoconsc:mk(binary(), #{
                in => query,
                required => false,
                desc => <<"Node name">>,
                example => <<"emqx@127.0.0.1">>
            })}
        | fields(common_list_clients_input)
    ];
fields(common_list_clients_input) ->
    [
        hoconsc:ref(emqx_dashboard_swagger, limit),
        {username,
            hoconsc:mk(hoconsc:array(binary()), #{
                in => query,
                required => false,
                desc => <<
                    "User name, multiple values can be specified by"
                    " repeating the parameter: username=u1&username=u2"
                >>
            })},
        {ip_address,
            hoconsc:mk(binary(), #{
                in => query,
                required => false,
                desc => <<"Client's IP address">>,
                example => <<"127.0.0.1">>
            })},
        {conn_state,
            hoconsc:mk(hoconsc:enum([connected, idle, disconnected]), #{
                in => query,
                required => false,
                desc =>
                    <<"The current connection status of the client, ",
                        "the possible values are connected,idle,disconnected">>
            })},
        {clean_start,
            hoconsc:mk(boolean(), #{
                in => query,
                required => false,
                description => <<"Whether the client uses a new session">>
            })},
        {proto_ver,
            hoconsc:mk(binary(), #{
                in => query,
                required => false,
                desc => <<"Client protocol version">>
            })},
        {like_clientid,
            hoconsc:mk(binary(), #{
                in => query,
                required => false,
                desc => <<"Fuzzy search `clientid` as substring">>
            })},
        {like_username,
            hoconsc:mk(binary(), #{
                in => query,
                required => false,
                desc => <<"Fuzzy search `username` as substring">>
            })},
        {gte_created_at,
            hoconsc:mk(emqx_utils_calendar:epoch_millisecond(), #{
                in => query,
                required => false,
                desc =>
                    <<"Search client session creation time by greater",
                        " than or equal method, rfc3339 or timestamp(millisecond)">>
            })},
        {lte_created_at,
            hoconsc:mk(emqx_utils_calendar:epoch_millisecond(), #{
                in => query,
                required => false,
                desc =>
                    <<"Search client session creation time by less",
                        " than or equal method, rfc3339 or timestamp(millisecond)">>
            })},
        {gte_connected_at,
            hoconsc:mk(emqx_utils_calendar:epoch_millisecond(), #{
                in => query,
                required => false,
                desc => <<
                    "Search client connection creation time by greater"
                    " than or equal method, rfc3339 or timestamp(epoch millisecond)"
                >>
            })},
        {lte_connected_at,
            hoconsc:mk(emqx_utils_calendar:epoch_millisecond(), #{
                in => query,
                required => false,
                desc => <<
                    "Search client connection creation time by less"
                    " than or equal method, rfc3339 or timestamp(millisecond)"
                >>
            })},
        {clientid,
            hoconsc:mk(hoconsc:array(binary()), #{
                in => query,
                required => false,
                desc => <<
                    "Client ID, multiple values can be specified by"
                    " repeating the parameter: clientid=c1&clientid=c2"
                >>
            })},
        ?R_REF(requested_client_fields)
    ];
fields(clients) ->
    [
        {data, hoconsc:mk(hoconsc:array(?REF(client)), #{})},
        {meta, hoconsc:mk(hoconsc:ref(emqx_dashboard_swagger, meta), #{})}
    ];
fields(list_clients_v2_response) ->
    [
        {data, hoconsc:mk(hoconsc:array(?REF(client)), #{})},
        {meta, hoconsc:mk(hoconsc:ref(emqx_dashboard_swagger, meta_with_cursor), #{})}
    ];
fields(client) ->
    [
        {awaiting_rel_cnt,
            hoconsc:mk(integer(), #{
                desc =>
                    <<"v4 api name [awaiting_rel] Number of awaiting PUBREC packet">>
            })},
        {awaiting_rel_max,
            hoconsc:mk(integer(), #{
                desc =>
                    <<
                        "v4 api name [max_awaiting_rel]. "
                        "Maximum allowed number of awaiting PUBREC packet"
                    >>
            })},
        {clean_start,
            hoconsc:mk(boolean(), #{
                desc =>
                    <<"Indicate whether the client is using a brand new session">>
            })},
        {clientid, hoconsc:mk(binary(), #{desc => <<"Client identifier">>})},
        {connected, hoconsc:mk(boolean(), #{desc => <<"Whether the client is connected">>})},
        {connected_at,
            hoconsc:mk(
                emqx_utils_calendar:epoch_millisecond(),
                #{desc => <<"Client connection time, rfc3339 or timestamp(millisecond)">>}
            )},
        {created_at,
            hoconsc:mk(
                emqx_utils_calendar:epoch_millisecond(),
                #{desc => <<"Session creation time, rfc3339 or timestamp(millisecond)">>}
            )},
        {disconnected_at,
            hoconsc:mk(emqx_utils_calendar:epoch_millisecond(), #{
                desc =>
                    <<
                        "Client offline time."
                        " It's Only valid and returned when connected is false, rfc3339 or timestamp(millisecond)"
                    >>
            })},
        {expiry_interval,
            hoconsc:mk(integer(), #{
                desc =>
                    <<"Session expiration interval, with the unit of second">>
            })},
        {heap_size,
            hoconsc:mk(integer(), #{
                desc =>
                    <<"Process heap size with the unit of byte">>
            })},
        {inflight_cnt, hoconsc:mk(integer(), #{desc => <<"Current length of inflight">>})},
        {inflight_max,
            hoconsc:mk(integer(), #{
                desc =>
                    <<"v4 api name [max_inflight]. Maximum length of inflight">>
            })},
        {ip_address, hoconsc:mk(binary(), #{desc => <<"Client's IP address">>})},
        {is_bridge,
            hoconsc:mk(boolean(), #{
                desc =>
                    <<"Indicates whether the client is connected via bridge">>
            })},
        {is_expired,
            hoconsc:mk(boolean(), #{
                desc =>
                    <<"Indicates whether the client session is expired">>
            })},
        {keepalive,
            hoconsc:mk(integer(), #{
                desc =>
                    <<"keepalive time, with the unit of second">>
            })},
        {mailbox_len, hoconsc:mk(integer(), #{desc => <<"Process mailbox size">>})},
        {mqueue_dropped,
            hoconsc:mk(integer(), #{
                desc =>
                    <<"Number of messages dropped by the message queue due to exceeding the length">>
            })},
        {mqueue_len, hoconsc:mk(integer(), #{desc => <<"Current length of message queue">>})},
        {mqueue_max,
            hoconsc:mk(integer(), #{
                desc =>
                    <<"v4 api name [max_mqueue]. Maximum length of message queue">>
            })},
        {node,
            hoconsc:mk(binary(), #{
                desc =>
                    <<"Name of the node to which the client is connected">>
            })},
        {port, hoconsc:mk(integer(), #{desc => <<"Client's port">>})},
        {proto_name, hoconsc:mk(binary(), #{desc => <<"Client protocol name">>})},
        {proto_ver, hoconsc:mk(integer(), #{desc => <<"Protocol version used by the client">>})},
        {recv_cnt, hoconsc:mk(integer(), #{desc => <<"Number of TCP packets received">>})},
        {recv_msg, hoconsc:mk(integer(), #{desc => <<"Number of PUBLISH packets received">>})},
        {'recv_msg.dropped',
            hoconsc:mk(integer(), #{
                desc =>
                    <<"Number of dropped PUBLISH packets">>
            })},
        {'recv_msg.dropped.await_pubrel_timeout',
            hoconsc:mk(integer(), #{
                desc =>
                    <<"Number of dropped PUBLISH packets due to expired">>
            })},
        {'recv_msg.qos0',
            hoconsc:mk(integer(), #{
                desc =>
                    <<"Number of PUBLISH QoS0 packets received">>
            })},
        {'recv_msg.qos1',
            hoconsc:mk(integer(), #{
                desc =>
                    <<"Number of PUBLISH QoS1 packets received">>
            })},
        {'recv_msg.qos2',
            hoconsc:mk(integer(), #{
                desc =>
                    <<"Number of PUBLISH QoS2 packets received">>
            })},
        {recv_oct, hoconsc:mk(integer(), #{desc => <<"Number of bytes received">>})},
        {recv_pkt, hoconsc:mk(integer(), #{desc => <<"Number of MQTT packets received">>})},
        {reductions, hoconsc:mk(integer(), #{desc => <<"Erlang reduction">>})},
        {send_cnt, hoconsc:mk(integer(), #{desc => <<"Number of TCP packets sent">>})},
        {send_msg, hoconsc:mk(integer(), #{desc => <<"Number of PUBLISH packets sent">>})},
        {'send_msg.dropped',
            hoconsc:mk(integer(), #{
                desc =>
                    <<"Number of dropped PUBLISH packets">>
            })},
        {'send_msg.dropped.expired',
            hoconsc:mk(integer(), #{
                desc =>
                    <<"Number of dropped PUBLISH packets due to expired">>
            })},
        {'send_msg.dropped.queue_full',
            hoconsc:mk(integer(), #{
                desc =>
                    <<"Number of dropped PUBLISH packets due to queue full">>
            })},
        {'send_msg.dropped.too_large',
            hoconsc:mk(integer(), #{
                desc =>
                    <<"Number of dropped PUBLISH packets due to packet length too large">>
            })},
        {'send_msg.qos0',
            hoconsc:mk(integer(), #{
                desc =>
                    <<"Number of PUBLISH QoS0 packets sent">>
            })},
        {'send_msg.qos1',
            hoconsc:mk(integer(), #{
                desc =>
                    <<"Number of PUBLISH QoS1 packets sent">>
            })},
        {'send_msg.qos2',
            hoconsc:mk(integer(), #{
                desc =>
                    <<"Number of PUBLISH QoS2 packets sent">>
            })},
        {send_oct, hoconsc:mk(integer(), #{desc => <<"Number of bytes sent">>})},
        {send_pkt, hoconsc:mk(integer(), #{desc => <<"Number of MQTT packets sent">>})},
        {subscriptions_cnt,
            hoconsc:mk(integer(), #{
                desc =>
                    <<"Number of subscriptions established by this client.">>
            })},
        {subscriptions_max,
            hoconsc:mk(integer(), #{
                desc =>
                    <<"v4 api name [max_subscriptions]",
                        " Maximum number of subscriptions allowed by this client">>
            })},
        {username, hoconsc:mk(binary(), #{desc => <<"User name of client when connecting">>})},
        {mountpoint, hoconsc:mk(binary(), #{desc => <<"Topic mountpoint">>})},
        {durable, hoconsc:mk(boolean(), #{desc => <<"Session is durable">>})},
        {n_streams,
            hoconsc:mk(non_neg_integer(), #{
                desc => <<"Number of streams used by the durable session">>
            })},

        {seqno_q1_comm,
            hoconsc:mk(non_neg_integer(), #{
                desc =>
                    <<
                        "Sequence number of the last PUBACK received from the client "
                        "(Durable sessions only)"
                    >>
            })},
        {seqno_q1_dup,
            hoconsc:mk(non_neg_integer(), #{
                desc =>
                    <<
                        "Sequence number of the last QoS1 message sent to the client, that hasn't been acked "
                        "(Durable sessions only)"
                    >>
            })},
        {seqno_q1_next,
            hoconsc:mk(non_neg_integer(), #{
                desc =>
                    <<
                        "Sequence number of next QoS1 message to be added to the batch "
                        "(Durable sessions only)"
                    >>
            })},

        {seqno_q2_comm,
            hoconsc:mk(non_neg_integer(), #{
                desc =>
                    <<
                        "Sequence number of the last PUBCOMP received from the client "
                        "(Durable sessions only)"
                    >>
            })},
        {seqno_q2_dup,
            hoconsc:mk(non_neg_integer(), #{
                desc =>
                    <<
                        "Sequence number of last unacked QoS2 PUBLISH message sent to the client "
                        "(Durable sessions only)"
                    >>
            })},
        {seqno_q2_rec,
            hoconsc:mk(non_neg_integer(), #{
                desc =>
                    <<"Sequence number of last PUBREC received from the client (Durable sessions only)">>
            })},
        {seqno_q2_next,
            hoconsc:mk(non_neg_integer(), #{
                desc =>
                    <<
                        "Sequence number of next QoS2 message to be added to the batch "
                        "(Durable sessions only)"
                    >>
            })}
    ];
fields(authz_cache) ->
    [
        {access, hoconsc:mk(binary(), #{desc => <<"Access type">>, example => <<"publish">>})},
        {result,
            hoconsc:mk(hoconsc:enum([allow, denny]), #{
                desc => <<"Allow or deny">>, example => <<"allow">>
            })},
        {topic, hoconsc:mk(binary(), #{desc => <<"Topic name">>, example => <<"testtopic/1">>})},
        {updated_time,
            hoconsc:mk(integer(), #{desc => <<"Update time">>, example => 1687850712989})}
    ];
fields(keepalive) ->
    [
        {interval,
            hoconsc:mk(range(0, 65535), #{desc => <<"Keepalive time, with the unit of second">>})}
    ];
fields(subscribe) ->
    [
        {topic,
            hoconsc:mk(binary(), #{
                required => true, desc => <<"Topic">>, example => <<"testtopic/#">>
            })},
        {qos, hoconsc:mk(emqx_schema:qos(), #{default => 0, desc => <<"QoS">>})},
        {nl, hoconsc:mk(integer(), #{default => 0, desc => <<"No Local">>})},
        {rap, hoconsc:mk(integer(), #{default => 0, desc => <<"Retain as Published">>})},
        {rh, hoconsc:mk(integer(), #{default => 0, desc => <<"Retain Handling">>})}
    ];
fields(unsubscribe) ->
    [
        {topic, hoconsc:mk(binary(), #{desc => <<"Topic">>, example => <<"testtopic/#">>})}
    ];
fields(mqueue_messages) ->
    [
        {data, hoconsc:mk(hoconsc:array(?REF(mqueue_message)), #{desc => ?DESC(mqueue_msgs_list)})},
        {meta, hoconsc:mk(hoconsc:ref(emqx_dashboard_swagger, continuation_meta), #{})}
    ];
fields(inflight_messages) ->
    [
        {data, hoconsc:mk(hoconsc:array(?REF(message)), #{desc => ?DESC(inflight_msgs_list)})},
        {meta, hoconsc:mk(hoconsc:ref(emqx_dashboard_swagger, continuation_meta), #{})}
    ];
fields(message) ->
    [
        {msgid, hoconsc:mk(binary(), #{desc => ?DESC(msg_id)})},
        {topic, hoconsc:mk(binary(), #{desc => ?DESC(msg_topic)})},
        {qos, hoconsc:mk(emqx_schema:qos(), #{desc => ?DESC(msg_qos)})},
        {publish_at, hoconsc:mk(integer(), #{desc => ?DESC(msg_publish_at)})},
        {from_clientid, hoconsc:mk(binary(), #{desc => ?DESC(msg_from_clientid)})},
        {from_username, hoconsc:mk(binary(), #{desc => ?DESC(msg_from_username)})},
        {payload, hoconsc:mk(binary(), #{desc => ?DESC(msg_payload)})},
        {inserted_at, hoconsc:mk(binary(), #{desc => ?DESC(msg_inserted_at)})}
    ];
fields(mqueue_message) ->
    fields(message) ++
        [
            {mqueue_priority,
                hoconsc:mk(
                    hoconsc:union([integer(), infinity]),
                    #{desc => ?DESC(msg_mqueue_priority)}
                )}
        ];
fields(requested_client_fields) ->
    %% NOTE: some Client fields actually returned in response are missing in schema:
    %%  enable_authn, is_persistent, listener, peerport
    ClientFields0 = [element(1, F) || F <- fields(client)],
    ClientFields = [client_attrs | ClientFields0],
    [
        {fields,
            hoconsc:mk(
                hoconsc:union([all, hoconsc:array(hoconsc:enum(ClientFields))]),
                #{
                    in => query,
                    required => false,
                    default => all,
                    desc => <<"Comma separated list of client fields to return in the response">>,
                    converter => fun
                        (all, _Opts) ->
                            all;
                        (<<"all">>, _Opts) ->
                            all;
                        (CsvFields, _Opts) when is_binary(CsvFields) ->
                            binary:split(CsvFields, <<",">>, [global, trim_all])
                    end
                }
            )}
    ].

%%%==============================================================================================
%% parameters trans
clients(get, #{query_string := QString}) ->
    list_clients(QString).

kickout_clients(post, #{body := ClientIDs}) ->
    case emqx_mgmt:kickout_clients(ClientIDs) of
        ok ->
            {204};
        {error, Reason} ->
            Message = list_to_binary(io_lib:format("~p", [Reason])),
            {500, #{code => <<"UNKNOWN_ERROR">>, message => Message}}
    end.

client(get, #{bindings := Bindings}) ->
    lookup(Bindings);
client(delete, #{bindings := Bindings}) ->
    kickout(Bindings).

authz_cache(get, #{bindings := Bindings}) ->
    get_authz_cache(Bindings);
authz_cache(delete, #{bindings := Bindings}) ->
    clean_authz_cache(Bindings).

subscribe(post, #{bindings := #{clientid := ClientID}, body := TopicInfo}) ->
    Opts = to_topic_info(TopicInfo),
    subscribe(Opts#{clientid => ClientID}).

subscribe_batch(post, #{bindings := #{clientid := ClientID}, body := TopicInfos}) ->
    Topics =
        [
            to_topic_info(TopicInfo)
         || TopicInfo <- TopicInfos
        ],
    subscribe_batch(#{clientid => ClientID, topics => Topics}).

unsubscribe(post, #{bindings := #{clientid := ClientID}, body := TopicInfo}) ->
    Topic = maps:get(<<"topic">>, TopicInfo),
    unsubscribe(#{clientid => ClientID, topic => Topic}).

unsubscribe_batch(post, #{bindings := #{clientid := ClientID}, body := TopicInfos}) ->
    Topics = [Topic || #{<<"topic">> := Topic} <- TopicInfos],
    unsubscribe_batch(#{clientid => ClientID, topics => Topics}).

subscriptions(get, #{bindings := #{clientid := ClientID}}) ->
    case emqx_mgmt:list_client_subscriptions(ClientID) of
        {error, not_found} ->
            {404, ?CLIENTID_NOT_FOUND};
        [] ->
            {200, []};
        {Node, Subs} ->
            Formatter =
                fun(_Sub = {Topic, SubOpts}) ->
                    emqx_mgmt_api_subscriptions:format(Node, {{Topic, ClientID}, SubOpts})
                end,
            {200, lists:map(Formatter, Subs)}
    end.

set_keepalive(put, #{bindings := #{clientid := ClientID}, body := Body}) ->
    case maps:find(<<"interval">>, Body) of
        error ->
            {400, 'BAD_REQUEST', "Interval Not Found"};
        {ok, Interval} ->
            case emqx_mgmt:set_keepalive(ClientID, Interval) of
                ok -> lookup(#{clientid => ClientID});
                {error, not_found} -> {404, ?CLIENTID_NOT_FOUND};
                {error, Reason} -> {400, #{code => 'PARAM_ERROR', message => Reason}}
            end
    end.

mqueue_msgs(get, #{bindings := #{clientid := ClientID}, query_string := QString}) ->
    list_client_msgs(mqueue_msgs, ClientID, QString).

inflight_msgs(get, #{
    bindings := #{clientid := ClientID},
    query_string := QString
}) ->
    list_client_msgs(inflight_msgs, ClientID, QString).

%%%==============================================================================================
%% api apply

list_clients(QString) ->
    Result =
        case maps:get(<<"node">>, QString, undefined) of
            undefined ->
                Options = #{fast_total_counting => true},
                list_clients_cluster_query(QString, Options);
            Node0 ->
                case emqx_utils:safe_to_existing_atom(Node0) of
                    {ok, Node1} ->
                        QStringWithoutNode = maps:remove(<<"node">>, QString),
                        Options = #{},
                        list_clients_node_query(Node1, QStringWithoutNode, Options);
                    {error, _} ->
                        {error, Node0, {badrpc, <<"invalid node">>}}
                end
        end,
    case Result of
        {error, page_limit_invalid} ->
            {400, #{code => <<"INVALID_PARAMETER">>, message => <<"page_limit_invalid">>}};
        {error, invalid_query_string_param, {Key, ExpectedType, AcutalValue}} ->
            Message = list_to_binary(
                io_lib:format(
                    "the ~s parameter expected type is ~s, but the value is ~s",
                    [Key, ExpectedType, emqx_utils_conv:str(AcutalValue)]
                )
            ),
            {400, #{code => <<"INVALID_PARAMETER">>, message => Message}};
        {error, Node, {badrpc, R}} ->
            Message = list_to_binary(io_lib:format("bad rpc call ~p, Reason ~p", [Node, R])),
            {500, #{code => <<"NODE_DOWN">>, message => Message}};
        Response ->
            {200, Response}
    end.

list_clients_v2(get, #{query_string := QString}) ->
    Nodes = emqx:running_nodes(),
    case QString of
        #{<<"cursor">> := CursorBin} ->
            case parse_cursor(CursorBin, Nodes) of
                {ok, Cursor} ->
                    do_list_clients_v2(Nodes, Cursor, QString);
                {error, bad_cursor} ->
                    ?BAD_REQUEST(<<"bad cursor">>)
            end;
        #{} ->
            Cursor = initial_ets_cursor(Nodes),
            do_list_clients_v2(Nodes, Cursor, QString)
    end.

do_list_clients_v2(Nodes, Cursor, QString0) ->
    Limit = maps:get(<<"limit">>, QString0, 100),
    Acc = #{
        rows => [],
        n => 0,
        limit => Limit
    },
    do_list_clients_v2(Nodes, Cursor, QString0, Acc).

do_list_clients_v2(_Nodes, Cursor = done, _QString, Acc) ->
    format_results(Acc, Cursor);
do_list_clients_v2(Nodes, Cursor = #{type := ?CURSOR_TYPE_ETS, node := Node}, QString0, Acc0) ->
    maybe
        {ok, {Rows, NewCursor}} ?= do_ets_select(Nodes, QString0, Cursor),
        Acc1 = maps:update_with(rows, fun(Rs) -> [{Node, Rows} | Rs] end, Acc0),
        Acc = #{limit := Limit, n := N} = maps:update_with(n, fun(N) -> N + length(Rows) end, Acc1),
        case N >= Limit of
            true ->
                format_results(Acc, NewCursor);
            false ->
                do_list_clients_v2(Nodes, NewCursor, QString0, Acc)
        end
    end;
do_list_clients_v2(Nodes, _Cursor = #{type := ?CURSOR_TYPE_DS, iterator := Iter0}, QString0, Acc0) ->
    #{limit := Limit} = Acc0,
    {Rows0, Iter} = emqx_persistent_session_ds_state:session_iterator_next(Iter0, Limit),
    NewCursor = next_ds_cursor(Iter),
    Rows1 = check_for_live_and_expired(Rows0),
    Rows = run_filters(Rows1, QString0),
    Acc1 = maps:update_with(rows, fun(Rs) -> [{undefined, Rows} | Rs] end, Acc0),
    Acc = #{n := N} = maps:update_with(n, fun(N) -> N + length(Rows) end, Acc1),
    case N >= Limit of
        true ->
            format_results(Acc, NewCursor);
        false ->
            do_list_clients_v2(Nodes, NewCursor, QString0, Acc)
    end.

format_results(Acc, Cursor) ->
    #{
        rows := NodeRows,
        n := N
    } = Acc,
    Meta =
        case Cursor of
            done ->
                #{count => N};
            _ ->
                #{
                    count => N,
                    cursor => serialize_cursor(Cursor)
                }
        end,
    Resp = #{
        meta => Meta,
        data => [
            format_channel_info(Node, Row)
         || {Node, Rows} <- NodeRows,
            Row <- Rows
        ]
    },
    ?OK(Resp).

do_ets_select(Nodes, QString0, #{node := Node, node_idx := NodeIdx, cont := Cont} = _Cursor) ->
    maybe
        {ok, {_, QString1}} ?= parse_qstring(QString0),
        Limit = maps:get(<<"limit">>, QString0, 10),
        {Rows, #{cont := NewCont, node_idx := NewNodeIdx}} = ets_select(
            QString1, Limit, Node, NodeIdx, Cont
        ),
        {ok, {Rows, next_ets_cursor(Nodes, NewNodeIdx, NewCont)}}
    end.

parse_qstring(QString) ->
    try
        {ok, emqx_mgmt_api:parse_qstring(QString, ?CLIENT_QSCHEMA)}
    catch
        throw:{bad_value_type, {Key, ExpectedType, AcutalValue}} ->
            Message = list_to_binary(
                io_lib:format(
                    "the ~s parameter expected type is ~s, but the value is ~s",
                    [Key, ExpectedType, emqx_utils_conv:str(AcutalValue)]
                )
            ),
            ?BAD_REQUEST('INVALID_PARAMETER', Message)
    end.

run_filters(Rows, QString0) ->
    {_NClauses, {QString1, FuzzyQString1}} = emqx_mgmt_api:parse_qstring(QString0, ?CLIENT_QSCHEMA),
    {QString, FuzzyQString} = adapt_custom_filters(QString1, FuzzyQString1),
    FuzzyFilterFn = fuzzy_filter_fun(FuzzyQString),
    lists:filter(
        fun(Row) ->
            does_offline_row_match_query(Row, QString) andalso
                does_row_match_fuzzy_filter(Row, FuzzyFilterFn)
        end,
        Rows
    ).

does_row_match_fuzzy_filter(_Row, undefined) ->
    true;
does_row_match_fuzzy_filter(Row, {Fn, Args}) ->
    erlang:apply(Fn, [Row | Args]).

%% These filters, while they are able to be adapted to efficient ETS match specs, must be
%% used as fuzzy filters when iterating over offlient persistent clients, which live
%% outside ETS.
adapt_custom_filters(Qs, Fuzzy) ->
    lists:foldl(
        fun
            ({Field, '=:=', X}, {QsAcc, FuzzyAcc}) when
                Field =:= username orelse Field =:= clientid
            ->
                Xs = wrap(X),
                {QsAcc, [{Field, in, Xs} | FuzzyAcc]};
            (Clause, {QsAcc, FuzzyAcc}) ->
                {[Clause | QsAcc], FuzzyAcc}
        end,
        {[], Fuzzy},
        Qs
    ).

wrap(Xs) when is_list(Xs) -> Xs;
wrap(X) -> [X].

initial_ets_cursor([Node | _Rest] = _Nodes) ->
    #{
        type => ?CURSOR_TYPE_ETS,
        node => Node,
        node_idx => 1,
        cont => undefined
    }.

initial_ds_cursor() ->
    case emqx_persistent_message:is_persistence_enabled() of
        true ->
            #{
                type => ?CURSOR_TYPE_DS,
                iterator => init_persistent_session_iterator()
            };
        false ->
            done
    end.

next_ets_cursor(Nodes, NodeIdx, Cont) ->
    case NodeIdx > length(Nodes) of
        true ->
            initial_ds_cursor();
        false ->
            Node = lists:nth(NodeIdx, Nodes),
            #{
                type => ?CURSOR_TYPE_ETS,
                node_idx => NodeIdx,
                node => Node,
                cont => Cont
            }
    end.

next_ds_cursor('$end_of_table') ->
    done;
next_ds_cursor(Iter) ->
    #{
        type => ?CURSOR_TYPE_DS,
        iterator => Iter
    }.

parse_cursor(CursorBin, Nodes) ->
    try emqx_base62:decode(CursorBin) of
        Bin ->
            parse_cursor1(Bin, Nodes)
    catch
        _:_ ->
            {error, bad_cursor}
    end.

parse_cursor1(CursorBin, Nodes) ->
    try binary_to_term(CursorBin, [safe]) of
        [
            ?CURSOR_VSN1,
            ?CURSOR_TYPE_ETS,
            #{?CURSOR_ETS_NODE_IDX := NodeIdx, ?CURSOR_ETS_CONT := Cont}
        ] ->
            case NodeIdx > length(Nodes) of
                true ->
                    {error, bad_cursor};
                false ->
                    Node = lists:nth(NodeIdx, Nodes),
                    Cursor = #{
                        type => ?CURSOR_TYPE_ETS,
                        node => Node,
                        node_idx => NodeIdx,
                        cont => Cont
                    },
                    {ok, Cursor}
            end;
        [?CURSOR_VSN1, ?CURSOR_TYPE_DS, DSIter] ->
            Cursor = #{type => ?CURSOR_TYPE_DS, iterator => DSIter},
            {ok, Cursor};
        _ ->
            {error, bad_cursor}
    catch
        error:badarg ->
            {error, bad_cursor}
    end.

serialize_cursor(#{type := ?CURSOR_TYPE_ETS, node_idx := NodeIdx, cont := Cont}) ->
    Cursor0 = [
        ?CURSOR_VSN1,
        ?CURSOR_TYPE_ETS,
        #{?CURSOR_ETS_NODE_IDX => NodeIdx, ?CURSOR_ETS_CONT => Cont}
    ],
    Bin = term_to_binary(Cursor0, [{compressed, 9}]),
    emqx_base62:encode(Bin);
serialize_cursor(#{type := ?CURSOR_TYPE_DS, iterator := Iter}) ->
    Cursor0 = [?CURSOR_VSN1, ?CURSOR_TYPE_DS, Iter],
    Bin = term_to_binary(Cursor0, [{compressed, 9}]),
    emqx_base62:encode(Bin).

%% An adapter function so we can reutilize all the logic in `emqx_mgmt_api' for
%% selecting/fuzzy filters, and also reutilize its BPAPI for selecting rows.
ets_select(NQString, Limit, Node, NodeIdx, Cont) ->
    QueryState0 = emqx_mgmt_api:init_query_state(
        ?CHAN_INFO_TAB,
        NQString,
        fun ?MODULE:qs2ms/2,
        _Meta = #{page => unused, limit => Limit},
        _Options = #{}
    ),
    QueryState = QueryState0#{continuation => Cont},
    case emqx_mgmt_api:do_query(Node, QueryState) of
        {Rows, #{complete := true}} ->
            {Rows, #{node_idx => NodeIdx + 1, cont => undefined}};
        {Rows, #{continuation := NCont}} ->
            {Rows, #{node_idx => NodeIdx, cont => NCont}}
    end.

lookup(#{clientid := ClientID}) ->
    case emqx_mgmt:lookup_client({clientid, ClientID}, ?FORMAT_FUN) of
        [] ->
            {404, ?CLIENTID_NOT_FOUND};
        ClientInfo ->
            {200, hd(ClientInfo)}
    end.

kickout(#{clientid := ClientID}) ->
    case emqx_mgmt:kickout_client(ClientID) of
        {error, not_found} ->
            {404, ?CLIENTID_NOT_FOUND};
        _ ->
            {204}
    end.

get_authz_cache(#{clientid := ClientID}) ->
    case emqx_mgmt:list_authz_cache(ClientID) of
        {error, not_found} ->
            {404, ?CLIENTID_NOT_FOUND};
        {error, Reason} ->
            Message = list_to_binary(io_lib:format("~p", [Reason])),
            {500, #{code => <<"UNKNOWN_ERROR">>, message => Message}};
        Caches ->
            Response = [format_authz_cache(Cache) || Cache <- Caches],
            {200, Response}
    end.

clean_authz_cache(#{clientid := ClientID}) ->
    case emqx_mgmt:clean_authz_cache(ClientID) of
        ok ->
            {204};
        {error, not_found} ->
            {404, ?CLIENTID_NOT_FOUND};
        {error, Reason} ->
            Message = list_to_binary(io_lib:format("~p", [Reason])),
            {500, #{code => <<"UNKNOWN_ERROR">>, message => Message}}
    end.

subscribe(#{clientid := ClientID, topic := Topic} = Sub) ->
    Opts = maps:with([qos, nl, rap, rh], Sub),
    case do_subscribe(ClientID, Topic, Opts) of
        {error, channel_not_found} ->
            {404, ?CLIENTID_NOT_FOUND};
        {error, invalid_subopts_nl} ->
            {400, #{
                code => <<"INVALID_PARAMETER">>,
                message =>
                    <<"Invalid Subscribe options: `no_local` not allowed for shared-sub. See [MQTT-3.8.3-4]">>
            }};
        {error, Reason} ->
            Message = list_to_binary(io_lib:format("~p", [Reason])),
            {500, #{code => <<"UNKNOWN_ERROR">>, message => Message}};
        {ok, SubInfo} ->
            {200, SubInfo}
    end.

subscribe_batch(#{clientid := ClientID, topics := Topics}) ->
    %% On the one hand, we first try to use `emqx_channel' instead of `emqx_channel_info'
    %% (used by the `emqx_mgmt:lookup_client/2'), as the `emqx_channel_info' table will
    %% only be populated after the hook `client.connected' has returned. So if one want to
    %% subscribe topics in this hook, it will fail.
    %% ... On the other hand, using only `emqx_channel' would render this API unusable if
    %% called from a node that doesn't have hold the targeted client connection, so we
    %% fall back to `emqx_mgmt:lookup_client/2', which consults the global registry.
    Result1 = ets:lookup(?CHAN_TAB, ClientID),
    Result =
        case Result1 of
            [] -> emqx_mgmt:lookup_client({clientid, ClientID}, _FormatFn = undefined);
            _ -> Result1
        end,
    case Result of
        [] ->
            {404, ?CLIENTID_NOT_FOUND};
        _ ->
            ArgList = [
                [ClientID, Topic, maps:with([qos, nl, rap, rh], Sub)]
             || #{topic := Topic} = Sub <- Topics
            ],
            {200, emqx_mgmt_util:batch_operation(?MODULE, do_subscribe, ArgList)}
    end.

unsubscribe(#{clientid := ClientID, topic := Topic}) ->
    {NTopic, _} = emqx_topic:parse(Topic),
    case do_unsubscribe(ClientID, Topic) of
        {error, channel_not_found} ->
            {404, ?CLIENTID_NOT_FOUND};
        {unsubscribe, [{UnSubedT, #{}}]} when
            (UnSubedT =:= NTopic) orelse (UnSubedT =:= Topic)
        ->
            {204}
    end.

unsubscribe_batch(#{clientid := ClientID, topics := Topics}) ->
    case lookup(#{clientid => ClientID}) of
        {200, _} ->
            _ = emqx_mgmt:unsubscribe_batch(ClientID, Topics),
            {204};
        {404, NotFound} ->
            {404, NotFound}
    end.

%%--------------------------------------------------------------------
%% internal function

client_msgs_schema(OpId, Desc, ContExample, RespSchema) ->
    #{
        'operationId' => OpId,
        get => #{
            description => Desc,
            tags => ?TAGS,
            parameters => client_msgs_params(),
            responses => #{
                200 =>
                    emqx_dashboard_swagger:schema_with_example(RespSchema, #{
                        <<"data">> => [message_example(OpId)],
                        <<"meta">> => #{
                            <<"count">> => 100,
                            <<"last">> => ContExample
                        }
                    }),
                400 =>
                    emqx_dashboard_swagger:error_codes(
                        ['INVALID_PARAMETER'], <<"Invalid parameters">>
                    ),
                404 => emqx_dashboard_swagger:error_codes(
                    ['CLIENTID_NOT_FOUND', 'CLIENT_SHUTDOWN'], <<"Client ID not found">>
                ),
                ?NOT_IMPLEMENTED => emqx_dashboard_swagger:error_codes(
                    ['NOT_IMPLEMENTED'], <<"API not implemented">>
                )
            }
        }
    }.

client_msgs_params() ->
    [
        {clientid, hoconsc:mk(binary(), #{in => path})},
        {payload,
            hoconsc:mk(hoconsc:enum([none, base64, plain]), #{
                in => query,
                default => base64,
                desc => <<
                    "Client's inflight/mqueue messages payload encoding."
                    " If set to `none`, no payload is returned in the response."
                >>
            })},
        {max_payload_bytes,
            hoconsc:mk(emqx_schema:bytesize(), #{
                in => query,
                default => <<"1MB">>,
                desc => <<
                    "Client's inflight/mqueue messages payload limit."
                    " The total payload size of all messages in the response will not exceed this value."
                    " Messages beyond the limit will be silently omitted in the response."
                    " The only exception to this rule is when the first message payload"
                    " is already larger than the limit."
                    " In this case, the first message will be returned in the response."
                >>,
                validator => fun max_bytes_validator/1
            })},
        hoconsc:ref(emqx_dashboard_swagger, position),
        hoconsc:ref(emqx_dashboard_swagger, limit)
    ].

do_subscribe(ClientID, Topic0, Options) ->
    try emqx_topic:parse(Topic0, Options) of
        {Topic, Opts} ->
            TopicTable = [{Topic, Opts}],
            case emqx_mgmt:subscribe(ClientID, TopicTable) of
                {error, Reason} ->
                    {error, Reason};
                {subscribe, Subscriptions, Node} ->
                    case proplists:is_defined(Topic, Subscriptions) of
                        true ->
                            {ok, Options#{node => Node, clientid => ClientID, topic => Topic0}};
                        false ->
                            {error, unknow_error}
                    end
            end
    catch
        error:{invalid_subopts_nl, _} ->
            {error, invalid_subopts_nl};
        _:Reason ->
            {error, Reason}
    end.

-spec do_unsubscribe(emqx_types:clientid(), emqx_types:topic()) ->
    {unsubscribe, _} | {error, channel_not_found}.
do_unsubscribe(ClientID, Topic) ->
    case emqx_mgmt:unsubscribe(ClientID, Topic) of
        {error, Reason} ->
            {error, Reason};
        Res ->
            Res
    end.

list_clients_cluster_query(QString, Options) ->
    case emqx_mgmt_api:parse_pager_params(QString) of
        false ->
            {error, page_limit_invalid};
        Meta = #{} ->
            try
                {_CodCnt, NQString} = emqx_mgmt_api:parse_qstring(QString, ?CLIENT_QSCHEMA),
                Nodes = emqx:running_nodes(),
                ResultAcc = emqx_mgmt_api:init_query_result(),
                QueryState = emqx_mgmt_api:init_query_state(
                    ?CHAN_INFO_TAB, NQString, fun ?MODULE:qs2ms/2, Meta, Options
                ),
                Res = do_list_clients_cluster_query(Nodes, QueryState, ResultAcc),
                Opts = #{fields => maps:get(<<"fields">>, QString, all)},
                emqx_mgmt_api:format_query_result(
                    fun ?MODULE:format_channel_info/3, Meta, Res, Opts
                )
            catch
                throw:{bad_value_type, {Key, ExpectedType, AcutalValue}} ->
                    {error, invalid_query_string_param, {Key, ExpectedType, AcutalValue}}
            end
    end.

%% adapted from `emqx_mgmt_api:do_cluster_query'
do_list_clients_cluster_query(
    [Node | Tail] = Nodes,
    QueryState0,
    ResultAcc
) ->
    case emqx_mgmt_api:do_query(Node, QueryState0) of
        {error, Error} ->
            {error, Node, Error};
        {Rows, QueryState1 = #{complete := Complete0}} ->
            case emqx_mgmt_api:accumulate_query_rows(Node, Rows, QueryState1, ResultAcc) of
                {enough, NResultAcc} ->
                    %% TODO: this may return `{error, _, _}'...
                    QueryState2 = emqx_mgmt_api:maybe_collect_total_from_tail_nodes(
                        Tail, QueryState1
                    ),
                    QueryState = add_persistent_session_count(QueryState2),
                    Complete = Complete0 andalso Tail =:= [] andalso no_persistent_sessions(),
                    emqx_mgmt_api:finalize_query(
                        NResultAcc, emqx_mgmt_api:mark_complete(QueryState, Complete)
                    );
                {more, NResultAcc} when not Complete0 ->
                    do_list_clients_cluster_query(Nodes, QueryState1, NResultAcc);
                {more, NResultAcc} when Tail =/= [] ->
                    do_list_clients_cluster_query(
                        Tail, emqx_mgmt_api:reset_query_state(QueryState1), NResultAcc
                    );
                {more, NResultAcc} ->
                    QueryState = add_persistent_session_count(QueryState1),
                    do_persistent_session_query(NResultAcc, QueryState)
            end
    end.

list_clients_node_query(Node, QString, Options) ->
    case emqx_mgmt_api:parse_pager_params(QString) of
        false ->
            {error, page_limit_invalid};
        Meta = #{} ->
            {_CodCnt, NQString} = emqx_mgmt_api:parse_qstring(QString, ?CLIENT_QSCHEMA),
            ResultAcc = emqx_mgmt_api:init_query_result(),
            QueryState = emqx_mgmt_api:init_query_state(
                ?CHAN_INFO_TAB, NQString, fun ?MODULE:qs2ms/2, Meta, Options
            ),
            Res = do_list_clients_node_query(Node, QueryState, ResultAcc),
            Opts = #{fields => maps:get(<<"fields">>, QString, all)},
            emqx_mgmt_api:format_query_result(fun ?MODULE:format_channel_info/3, Meta, Res, Opts)
    end.

add_persistent_session_count(QueryState0 = #{total := Totals0}) ->
    case emqx_persistent_message:is_persistence_enabled() of
        true ->
            %% TODO: currently, counting persistent sessions can be not only costly (needs
            %% to traverse the whole table), but also hard to deduplicate live connections
            %% from it...  So this count will possibly overshoot the true count of
            %% sessions.
            DisconnectedSessionCount =
                emqx_persistent_session_bookkeeper:get_disconnected_session_count(),
            Totals = Totals0#{undefined => DisconnectedSessionCount},
            QueryState0#{total := Totals};
        false ->
            QueryState0
    end;
add_persistent_session_count(QueryState) ->
    QueryState.

%% adapted from `emqx_mgmt_api:do_node_query'
do_list_clients_node_query(
    Node,
    QueryState,
    ResultAcc
) ->
    case emqx_mgmt_api:do_query(Node, QueryState) of
        {error, Error} ->
            {error, Node, Error};
        {Rows, NQueryState = #{complete := Complete}} ->
            case emqx_mgmt_api:accumulate_query_rows(Node, Rows, NQueryState, ResultAcc) of
                {enough, NResultAcc} ->
                    FComplete = Complete andalso no_persistent_sessions(),
                    emqx_mgmt_api:finalize_query(
                        NResultAcc, emqx_mgmt_api:mark_complete(NQueryState, FComplete)
                    );
                {more, NResultAcc} when Complete ->
                    do_persistent_session_query(NResultAcc, NQueryState);
                {more, NResultAcc} ->
                    do_list_clients_node_query(Node, NQueryState, NResultAcc)
            end
    end.

init_persistent_session_iterator() ->
    emqx_persistent_session_ds_state:make_session_iterator().

no_persistent_sessions() ->
    case emqx_persistent_message:is_persistence_enabled() of
        true ->
            Cursor = init_persistent_session_iterator(),
            case emqx_persistent_session_ds_state:session_iterator_next(Cursor, 1) of
                {[], _} ->
                    true;
                _ ->
                    false
            end;
        false ->
            true
    end.

is_expired(#{last_alive_at := LastAliveAt, expiry_interval := ExpiryInterval}) ->
    LastAliveAt + ExpiryInterval < erlang:system_time(millisecond).

do_persistent_session_query(ResultAcc, QueryState) ->
    case emqx_persistent_message:is_persistence_enabled() of
        true ->
            do_persistent_session_query1(
                ResultAcc,
                QueryState,
                init_persistent_session_iterator()
            );
        false ->
            emqx_mgmt_api:finalize_query(ResultAcc, QueryState)
    end.

do_persistent_session_query1(ResultAcc, QueryState, Iter0) ->
    %% Since persistent session data is accessible from all nodes, there's no need to go
    %% through all the nodes.
    #{limit := Limit} = QueryState,
    {Rows0, Iter} = emqx_persistent_session_ds_state:session_iterator_next(Iter0, Limit),
    Rows = check_for_live_and_expired(Rows0),
    case emqx_mgmt_api:accumulate_query_rows(undefined, Rows, QueryState, ResultAcc) of
        {enough, NResultAcc} ->
            emqx_mgmt_api:finalize_query(NResultAcc, emqx_mgmt_api:mark_complete(QueryState, true));
        {more, NResultAcc} when Iter =:= '$end_of_table' ->
            emqx_mgmt_api:finalize_query(NResultAcc, emqx_mgmt_api:mark_complete(QueryState, true));
        {more, NResultAcc} ->
            do_persistent_session_query1(NResultAcc, QueryState, Iter)
    end.

check_for_live_and_expired(Rows) ->
    lists:filtermap(
        fun({ClientId, _Session}) ->
            case is_live_session(ClientId) of
                true ->
                    false;
                false ->
                    DSSession = emqx_persistent_session_ds_state:print_session(ClientId),
                    {true, {ClientId, DSSession}}
            end
        end,
        Rows
    ).

%% Return 'true' if there is a live channel found in the global channel registry.
%% NOTE: We cannot afford to query all running nodes to find out if a session is live.
%% i.e. assuming the global session registry is always enabled.
%% Otherwise this function may return `false` for `true` causing the session to appear
%% twice in the query result.
is_live_session(ClientId) ->
    [] =/= emqx_cm_registry:lookup_channels(ClientId).

list_client_msgs(MsgType, ClientID, QString) ->
    case emqx_mgmt_api:parse_cont_pager_params(QString, pos_decoder(MsgType)) of
        false ->
            {400, #{code => <<"INVALID_PARAMETER">>, message => <<"position_limit_invalid">>}};
        PagerParams = #{} ->
            case emqx_mgmt:list_client_msgs(MsgType, ClientID, PagerParams) of
                {error, not_found} ->
                    {404, ?CLIENTID_NOT_FOUND};
                {error, shutdown} ->
                    {404, ?CLIENT_SHUTDOWN};
                {error, not_implemented} ->
                    {?NOT_IMPLEMENTED, #{
                        code => 'NOT_IMPLEMENTED',
                        message => <<"API not implemented for persistent sessions">>
                    }};
                {error, Reason} ->
                    ?INTERNAL_ERROR(Reason);
                {Msgs, Meta = #{}} when is_list(Msgs) ->
                    format_msgs_resp(MsgType, Msgs, Meta, QString)
            end
    end.

pos_decoder(mqueue_msgs) -> fun decode_mqueue_pos/1;
pos_decoder(inflight_msgs) -> fun decode_msg_pos/1.

encode_msgs_meta(_MsgType, #{start := StartPos, position := Pos}) ->
    #{start => encode_pos(StartPos), position => encode_pos(Pos)}.

encode_pos(none) ->
    none;
encode_pos({MsgPos, PrioPos}) ->
    MsgPosBin = integer_to_binary(MsgPos),
    PrioPosBin =
        case PrioPos of
            infinity -> <<"infinity">>;
            _ -> integer_to_binary(PrioPos)
        end,
    <<MsgPosBin/binary, "_", PrioPosBin/binary>>;
encode_pos(Pos) when is_integer(Pos) ->
    integer_to_binary(Pos).

-spec decode_mqueue_pos(binary()) -> {integer(), infinity | integer()}.
decode_mqueue_pos(Pos) ->
    [MsgPos, PrioPos] = binary:split(Pos, <<"_">>),
    {decode_msg_pos(MsgPos), decode_priority_pos(PrioPos)}.

decode_msg_pos(Pos) -> binary_to_integer(Pos).

decode_priority_pos(<<"infinity">>) -> infinity;
decode_priority_pos(Pos) -> binary_to_integer(Pos).

max_bytes_validator(MaxBytes) when is_integer(MaxBytes), MaxBytes > 0 ->
    ok;
max_bytes_validator(_MaxBytes) ->
    {error, "must be higher than 0"}.

%%--------------------------------------------------------------------
%% QueryString to Match Spec

-spec qs2ms(atom(), {list(), list()}) -> emqx_mgmt_api:match_spec_and_filter().
qs2ms(_Tab, {QString, FuzzyQString}) ->
    #{
        match_spec => qs2ms(QString),
        fuzzy_fun => fuzzy_filter_fun(FuzzyQString)
    }.

-spec qs2ms(list()) -> ets:match_spec().
qs2ms(Qs) ->
    {MatchHead, Conds} = qs2ms(Qs, 2, {#{}, []}),
    [{{{'$1', '_'}, MatchHead, '_'}, Conds, ['$_']}].

qs2ms([], _, {MtchHead, Conds}) ->
    {MtchHead, lists:reverse(Conds)};
qs2ms([{Key, '=:=', Value} | Rest], N, {MtchHead, Conds}) when is_list(Value) ->
    {Holder, NxtN} = holder_and_nxt(Key, N),
    NMtchHead = emqx_mgmt_util:merge_maps(MtchHead, ms(Key, Holder)),
    qs2ms(Rest, NxtN, {NMtchHead, [orelse_cond(Holder, Value) | Conds]});
qs2ms([{Key, '=:=', Value} | Rest], N, {MtchHead, Conds}) ->
    NMtchHead = emqx_mgmt_util:merge_maps(MtchHead, ms(Key, Value)),
    qs2ms(Rest, N, {NMtchHead, Conds});
qs2ms([Qs | Rest], N, {MtchHead, Conds}) ->
    Holder = holder(N),
    NMtchHead = emqx_mgmt_util:merge_maps(MtchHead, ms(element(1, Qs), Holder)),
    NConds = put_conds(Qs, Holder, Conds),
    qs2ms(Rest, N + 1, {NMtchHead, NConds}).

%% This is a special case: clientid is a part of the key (ClientId, Pid}, as the table is ordered_set,
%% using partially bound key optimizes traversal.
holder_and_nxt(clientid, N) ->
    {'$1', N};
holder_and_nxt(_, N) ->
    {holder(N), N + 1}.

holder(N) -> list_to_atom([$$ | integer_to_list(N)]).

orelse_cond(Holder, ValuesList) ->
    Conds = [{'=:=', Holder, V} || V <- ValuesList],
    erlang:list_to_tuple(['orelse' | Conds]).

put_conds({_, Op, V}, Holder, Conds) ->
    [{Op, Holder, V} | Conds];
put_conds({_, Op1, V1, Op2, V2}, Holder, Conds) ->
    [
        {Op2, Holder, V2},
        {Op1, Holder, V1}
        | Conds
    ].

ms(clientid, _X) ->
    #{};
ms(username, X) ->
    #{clientinfo => #{username => X}};
ms(conn_state, X) ->
    #{conn_state => X};
ms(ip_address, X) ->
    #{conninfo => #{peername => {X, '_'}}};
ms(clean_start, X) ->
    #{conninfo => #{clean_start => X}};
ms(proto_ver, X) ->
    #{conninfo => #{proto_ver => X}};
ms(connected_at, X) ->
    #{conninfo => #{connected_at => X}};
ms(created_at, X) ->
    #{session => #{created_at => X}}.

%%--------------------------------------------------------------------
%% Filter funcs
%% These functions are used to filter durable clients. Durable clients are not stored in
%% ETS tables, so we cannot use matchspecs to filter them. Instead, we filter ready rows,
%% like fuzzy filters do.
%%
%% These functions are used with clients_v2 API.

does_offline_chan_info_match({ip_address, '=:=', IpAddress}, #{
    conninfo := #{peername := {IpAddress, _}}
}) ->
    true;
%% This matchers match only offline clients, because online clients are listed directly from
%% channel manager's ETS tables. So we succeed here only if offline conn_state is requested.
does_offline_chan_info_match({conn_state, '=:=', disconnected}, _) ->
    true;
does_offline_chan_info_match({conn_state, '=:=', _}, _) ->
    false;
does_offline_chan_info_match({clean_start, '=:=', CleanStart}, #{
    conninfo := #{clean_start := CleanStart}
}) ->
    true;
does_offline_chan_info_match({proto_ver, '=:=', ProtoVer}, #{conninfo := #{proto_ver := ProtoVer}}) ->
    true;
does_offline_chan_info_match({connected_at, '>=', ConnectedAtFrom}, #{
    conninfo := #{connected_at := ConnectedAt}
}) when
    ConnectedAt >= ConnectedAtFrom
->
    true;
does_offline_chan_info_match({connected_at, '=<', ConnectedAtTo}, #{
    conninfo := #{connected_at := ConnectedAt}
}) when
    ConnectedAt =< ConnectedAtTo
->
    true;
does_offline_chan_info_match({created_at, '>=', CreatedAtFrom}, #{
    session := #{created_at := CreatedAt}
}) when
    CreatedAt >= CreatedAtFrom
->
    true;
does_offline_chan_info_match({created_at, '=<', CreatedAtTo}, #{
    session := #{created_at := CreatedAt}
}) when
    CreatedAt =< CreatedAtTo
->
    true;
does_offline_chan_info_match(_, _) ->
    false.

does_offline_row_match_query(
    {_Id, #{metadata := #{offline_info := #{chan_info := ChanInfo}}}}, CompiledQueryString
) ->
    lists:all(
        fun(FieldQuery) -> does_offline_chan_info_match(FieldQuery, ChanInfo) end,
        CompiledQueryString
    );
does_offline_row_match_query(_, _) ->
    false.

%%--------------------------------------------------------------------
%% Match funcs

fuzzy_filter_fun([]) ->
    undefined;
fuzzy_filter_fun(Fuzzy) ->
    {fun ?MODULE:run_fuzzy_filter/2, [Fuzzy]}.

run_fuzzy_filter(_, []) ->
    true;
run_fuzzy_filter(
    Row = {_, #{metadata := #{clientinfo := ClientInfo}}},
    [{Key, like, SubStr} | RestArgs]
) ->
    %% Row from DS
    run_fuzzy_filter1(ClientInfo, Key, SubStr) andalso
        run_fuzzy_filter(Row, RestArgs);
run_fuzzy_filter(
    Row = {_, #{metadata := #{clientinfo := ClientInfo}}},
    [{Key, in, Xs} | RestArgs]
) ->
    %% Row from DS
    IsMatch =
        case maps:find(Key, ClientInfo) of
            {ok, X} ->
                lists:member(X, Xs);
            error ->
                false
        end,
    IsMatch andalso
        run_fuzzy_filter(Row, RestArgs);
run_fuzzy_filter(Row = {_, #{clientinfo := ClientInfo}, _}, [{Key, like, SubStr} | RestArgs]) ->
    %% Row from ETS
    run_fuzzy_filter1(ClientInfo, Key, SubStr) andalso
        run_fuzzy_filter(Row, RestArgs).

run_fuzzy_filter1(ClientInfo, Key, SubStr) ->
    Val =
        case maps:get(Key, ClientInfo, <<>>) of
            undefined -> <<>>;
            V -> V
        end,
    binary:match(Val, SubStr) /= nomatch.

%%--------------------------------------------------------------------
%% format funcs

format_channel_info(ChannInfo = {_, _ClientInfo, _ClientStats}) ->
    %% channel info from ETS table (live and/or in-memory session)
    format_channel_info(node(), ChannInfo);
format_channel_info({ClientId, PSInfo}) ->
    %% offline persistent session
    format_persistent_session_info(ClientId, PSInfo).

format_channel_info(WhichNode, ChanInfo) ->
    DefaultOpts = #{fields => all},
    format_channel_info(WhichNode, ChanInfo, DefaultOpts).

format_channel_info(WhichNode, {_, ClientInfo0, ClientStats}, Opts) ->
    Node = maps:get(node, ClientInfo0, WhichNode),
    ConnInfo = maps:without(
        [clientid, username, sock, conn_shared_state],
        maps:get(conninfo, ClientInfo0)
    ),
    ClientInfo1 = ClientInfo0#{conninfo := ConnInfo},
    StatsMap = maps:without(
        [memory, next_pkt_id, total_heap_size],
        maps:from_list(ClientStats)
    ),
    ClientInfo2 = maps:remove(will_msg, ClientInfo1),
    ClientInfoMap0 = maps:fold(fun take_maps_from_inner/3, #{}, ClientInfo2),
    {IpAddress, Port} = peername_dispart(maps:get(peername, ClientInfoMap0)),
    Connected = maps:get(conn_state, ClientInfoMap0) =:= connected,
    ClientInfoMap1 = maps:merge(StatsMap, ClientInfoMap0),
    ClientInfoMap2 = maps:put(node, Node, ClientInfoMap1),
    ClientInfoMap3 = maps:put(ip_address, IpAddress, ClientInfoMap2),
    ClientInfoMap4 = maps:put(port, Port, ClientInfoMap3),
    ClientInfoMap5 = convert_expiry_interval_unit(ClientInfoMap4),
    ClientInfoMap6 = maps:put(connected, Connected, ClientInfoMap5),
    %% Since this is for the memory session format, and its lifetime is linked to the
    %% channel process, we may say it's not expired.  Durable sessions will override this
    %% field if needed in their format function.
    ClientInfoMap = maps:put(is_expired, false, ClientInfoMap6),

    #{fields := RequestedFields} = Opts,
    TimesKeys = [created_at, connected_at, disconnected_at],
    %% format timestamp to rfc3339
    result_format_undefined_to_null(
        lists:foldl(
            fun result_format_time_fun/2,
            with_client_info_fields(ClientInfoMap, RequestedFields),
            TimesKeys
        )
    );
format_channel_info(undefined, {ClientId, PSInfo0 = #{}}, _Opts) ->
    format_persistent_session_info(ClientId, PSInfo0);
format_channel_info(undefined, {ClientId, undefined = _PSInfo}, _Opts) ->
    %% Durable session missing its metadata: possibly a race condition, such as the client
    %% being kicked while the API is enumerating clients.  There's nothing much to do, we
    %% just return an almost empty map to avoid crashing this function.  The client may
    %% just retry listing in such cases.
    #{clientid => ClientId}.

format_persistent_session_info(
    _ClientId,
    #{
        metadata := #{?offline_info := #{chan_info := ChanInfo, stats := Stats} = OfflineInfo} =
            Metadata
    } =
        PSInfo
) ->
    Info0 = format_channel_info(_Node = undefined, {_Key = undefined, ChanInfo, Stats}, #{
        fields => all
    }),
    LastConnectedToNode = maps:get(last_connected_to, OfflineInfo, undefined),
    DisconnectedAt = maps:get(disconnected_at, OfflineInfo, undefined),
    %% `created_at' and `connected_at' have already been formatted by this point.
    Info = result_format_time_fun(
        disconnected_at,
        Info0#{
            connected => false,
            disconnected_at => DisconnectedAt,
            durable => true,
            is_persistent => true,
            is_expired => is_expired(Metadata),
            node => LastConnectedToNode,
            subscriptions_cnt => maps:size(maps:get(subscriptions, PSInfo, #{}))
        }
    ),
    result_format_undefined_to_null(Info);
format_persistent_session_info(ClientId, PSInfo0) ->
    Metadata = maps:get(metadata, PSInfo0, #{}),
    {ProtoName, ProtoVer} = maps:get(protocol, Metadata),
    PSInfo1 = maps:with([created_at, expiry_interval], Metadata),
    CreatedAt = maps:get(created_at, PSInfo1),
    case Metadata of
        #{peername := PeerName} ->
            {IpAddress, Port} = peername_dispart(PeerName);
        _ ->
            IpAddress = undefined,
            Port = undefined
    end,
    PSInfo2 = convert_expiry_interval_unit(PSInfo1),
    PSInfo3 = PSInfo2#{
        clientid => ClientId,
        connected => false,
        connected_at => CreatedAt,
        durable => true,
        ip_address => IpAddress,
        is_expired => is_expired(Metadata),
        is_persistent => true,
        port => Port,
        heap_size => 0,
        mqueue_len => 0,
        proto_name => ProtoName,
        proto_ver => ProtoVer,
        subscriptions_cnt => maps:size(maps:get(subscriptions, PSInfo0, #{}))
    },
    PSInfo = lists:foldl(
        fun result_format_time_fun/2,
        PSInfo3,
        [created_at, connected_at]
    ),
    result_format_undefined_to_null(PSInfo).

with_client_info_fields(ClientInfoMap, all) ->
    RemoveList =
        [
            auth_result,
            peername,
            sockname,
            peerhost,
            peerport,
            conn_state,
            send_pend,
            conn_props,
            peercert,
            sockstate,
            subscriptions,
            receive_maximum,
            protocol,
            is_superuser,
            sockport,
            anonymous,
            socktype,
            active_n,
            await_rel_timeout,
            conn_mod,
            sockname,
            retry_interval,
            upgrade_qos,
            zone,
            %% session_id, defined in emqx_session.erl
            id,
            acl
        ],
    maps:without(RemoveList, ClientInfoMap);
with_client_info_fields(ClientInfoMap, RequestedFields) when is_list(RequestedFields) ->
    maps:with(RequestedFields, ClientInfoMap).

format_msgs_resp(MsgType, Msgs, Meta, QString) ->
    #{
        <<"payload">> := PayloadFmt,
        <<"max_payload_bytes">> := MaxBytes
    } = QString,
    Meta1 = encode_msgs_meta(MsgType, Meta),
    Resp = #{meta => Meta1, data => format_msgs(MsgType, Msgs, PayloadFmt, MaxBytes)},
    %% Make sure minirest won't set another content-type for self-encoded JSON response body
    Headers = #{<<"content-type">> => <<"application/json">>},
    case emqx_utils_json:safe_encode(Resp) of
        {ok, RespBin} ->
            {200, Headers, RespBin};
        _Error when PayloadFmt =:= plain ->
            ?BAD_REQUEST(
                <<"INVALID_PARAMETER">>,
                <<"Some message payloads are not JSON serializable">>
            );
        %% Unexpected internal error
        Error ->
            ?INTERNAL_ERROR(Error)
    end.

format_msgs(MsgType, [FirstMsg | Msgs], PayloadFmt, MaxBytes) ->
    %% Always include at least one message payload, even if it exceeds the limit
    {FirstMsg1, PayloadSize0} = format_msg(MsgType, FirstMsg, PayloadFmt),
    {Msgs1, _} =
        catch lists:foldl(
            fun(Msg, {MsgsAcc, SizeAcc} = Acc) ->
                {Msg1, PayloadSize} = format_msg(MsgType, Msg, PayloadFmt),
                case SizeAcc + PayloadSize of
                    SizeAcc1 when SizeAcc1 =< MaxBytes ->
                        {[Msg1 | MsgsAcc], SizeAcc1};
                    _ ->
                        throw(Acc)
                end
            end,
            {[FirstMsg1], PayloadSize0},
            Msgs
        ),
    lists:reverse(Msgs1);
format_msgs(_MsgType, [], _PayloadFmt, _MaxBytes) ->
    [].

format_msg(
    MsgType,
    #message{
        id = ID,
        qos = Qos,
        topic = Topic,
        from = From,
        timestamp = Timestamp,
        headers = Headers,
        payload = Payload
    } = Msg,
    PayloadFmt
) ->
    MsgMap = #{
        msgid => emqx_guid:to_hexstr(ID),
        qos => Qos,
        topic => Topic,
        publish_at => Timestamp,
        from_clientid => emqx_utils_conv:bin(From),
        from_username => maps:get(username, Headers, <<>>)
    },
    MsgMap1 = format_by_msg_type(MsgType, Msg, MsgMap),
    format_payload(PayloadFmt, MsgMap1, Payload).

format_by_msg_type(mqueue_msgs, Msg, MsgMap) ->
    #message{extra = #{mqueue_priority := Prio, mqueue_insert_ts := Ts}} = Msg,
    MsgMap#{mqueue_priority => Prio, inserted_at => integer_to_binary(Ts)};
format_by_msg_type(inflight_msgs, Msg, MsgMap) ->
    #message{extra = #{inflight_insert_ts := Ts}} = Msg,
    MsgMap#{inserted_at => integer_to_binary(Ts)}.

format_payload(none, MsgMap, _Payload) ->
    {MsgMap, 0};
format_payload(base64, MsgMap, Payload) ->
    Payload1 = base64:encode(Payload),
    {MsgMap#{payload => Payload1}, erlang:byte_size(Payload1)};
format_payload(plain, MsgMap, Payload) ->
    {MsgMap#{payload => Payload}, erlang:iolist_size(Payload)}.

%% format func helpers
take_maps_from_inner(_Key, Value, Current) when is_map(Value) ->
    maps:merge(Current, Value);
take_maps_from_inner(Key, Value, Current) ->
    maps:put(Key, Value, Current).

result_format_time_fun(Key, NClientInfoMap) ->
    case NClientInfoMap of
        #{Key := TimeStamp} ->
            NClientInfoMap#{
                Key => emqx_utils_calendar:epoch_to_rfc3339(TimeStamp)
            };
        #{} ->
            NClientInfoMap
    end.

result_format_undefined_to_null(Map) ->
    maps:map(
        fun
            (_, undefined) -> null;
            (_, V) -> V
        end,
        Map
    ).

-spec peername_dispart(emqx_types:peername()) -> {binary(), inet:port_number()}.
peername_dispart({Addr, Port}) ->
    AddrBinary = list_to_binary(inet:ntoa(Addr)),
    %% PortBinary = integer_to_binary(Port),
    {AddrBinary, Port}.

convert_expiry_interval_unit(ClientInfoMap = #{expiry_interval := Interval}) ->
    ClientInfoMap#{expiry_interval := Interval div 1000}.

format_authz_cache({{PubSub, Topic}, {AuthzResult, Timestamp}}) ->
    #{
        access => PubSub,
        topic => Topic,
        result => AuthzResult,
        updated_time => Timestamp
    }.

to_topic_info(Data) ->
    M = maps:with([<<"topic">>, <<"qos">>, <<"nl">>, <<"rap">>, <<"rh">>], Data),
    emqx_utils_maps:safe_atom_key_map(M).

client_example() ->
    #{
        <<"recv_oct">> => 49,
        <<"expiry_interval">> => 0,
        <<"created_at">> => <<"2024-01-01T12:34:56.789+08:00">>,
        <<"awaiting_rel_max">> => 100,
        <<"send_msg">> => 0,
        <<"enable_authn">> => true,
        <<"send_msg.qos2">> => 0,
        <<"peerport">> => 52571,
        <<"connected_at">> => <<"2024-01-01T12:34:56.789+08:00">>,
        <<"send_msg.dropped.too_large">> => 0,
        <<"inflight_cnt">> => 0,
        <<"keepalive">> => 60,
        <<"node">> => <<"emqx@127.0.0.1">>,
        <<"send_cnt">> => 4,
        <<"recv_msg.dropped.await_pubrel_timeout">> => 0,
        <<"recv_msg.dropped">> => 0,
        <<"inflight_max">> => 32,
        <<"proto_name">> => <<"MQTT">>,
        <<"send_msg.dropped.expired">> => 0,
        <<"awaiting_rel_cnt">> => 0,
        <<"mqueue_max">> => 1000,
        <<"send_oct">> => 31,
        <<"send_msg.dropped.queue_full">> => 0,
        <<"mqueue_len">> => 0,
        <<"heap_size">> => 610,
        <<"is_persistent">> => false,
        <<"send_msg.qos0">> => 0,
        <<"clean_start">> => true,
        <<"mountpoint">> => <<"null">>,
        <<"proto_ver">> => 5,
        <<"ip_address">> => <<"127.0.0.1">>,
        <<"mqueue_dropped">> => 0,
        <<"port">> => 52571,
        <<"listener">> => <<"tcp:default">>,
        <<"recv_msg.qos2">> => 0,
        <<"recv_msg.qos1">> => 0,
        <<"is_bridge">> => false,
        <<"subscriptions_cnt">> => 1,
        <<"username">> => null,
        <<"send_msg.dropped">> => 0,
        <<"send_pkt">> => 4,
        <<"subscriptions_max">> => <<"infinity">>,
        <<"send_msg.qos1">> => 0,
        <<"connected">> => true,
        <<"reductions">> => 6836,
        <<"mailbox_len">> => 0,
        <<"clientid">> => "01",
        <<"recv_msg">> => 0,
        <<"recv_pkt">> => 4,
        <<"recv_cnt">> => 4,
        <<"recv_msg.qos0">> => 0,
        <<"durable">> => false
    }.

message_example(inflight_msgs) ->
    message_example();
message_example(mqueue_msgs) ->
    (message_example())#{<<"mqueue_priority">> => 0}.

message_example() ->
    #{
        <<"msgid">> => <<"000611F460D57FA9F44500000D360002">>,
        <<"topic">> => <<"t/test">>,
        <<"qos">> => 0,
        <<"publish_at">> => 1709055346487,
        <<"from_clientid">> => <<"mqttx_59ac0a87">>,
        <<"from_username">> => <<"test-user">>,
        <<"payload">> => <<"eyJmb28iOiAiYmFyIn0=">>
    }.

sessions_count(get, #{query_string := QString}) ->
    try
        Since = maps:get(<<"since">>, QString, 0),
        Count = emqx_cm_registry_keeper:count(Since),
        {200, integer_to_binary(Count)}
    catch
        exit:{noproc, _} ->
            Msg = io_lib:format("Node (~s) cannot handle this request.", [node()]),
            {400, 'BAD_REQUEST', iolist_to_binary(Msg)}
    end.
