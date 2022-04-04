%%--------------------------------------------------------------------
%% Copyright (c) 2017-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_schema).

-dialyzer(no_return).
-dialyzer(no_match).
-dialyzer(no_contracts).
-dialyzer(no_unused).
-dialyzer(no_fail_call).
-elvis([{elvis_style, invalid_dynamic_call, disable}]).

-include("emqx_authentication.hrl").
-include_lib("typerefl/include/types.hrl").

-type duration() :: integer().
-type duration_s() :: integer().
-type duration_ms() :: integer().
-type bytesize() :: integer().
-type wordsize() :: bytesize().
-type percent() :: float().
-type file() :: string().
-type comma_separated_list() :: list().
-type comma_separated_atoms() :: [atom()].
-type bar_separated_list() :: list().
-type ip_port() :: tuple().
-type cipher() :: map().

-typerefl_from_string({duration/0, emqx_schema, to_duration}).
-typerefl_from_string({duration_s/0, emqx_schema, to_duration_s}).
-typerefl_from_string({duration_ms/0, emqx_schema, to_duration_ms}).
-typerefl_from_string({bytesize/0, emqx_schema, to_bytesize}).
-typerefl_from_string({wordsize/0, emqx_schema, to_wordsize}).
-typerefl_from_string({percent/0, emqx_schema, to_percent}).
-typerefl_from_string({comma_separated_list/0, emqx_schema, to_comma_separated_list}).
-typerefl_from_string({bar_separated_list/0, emqx_schema, to_bar_separated_list}).
-typerefl_from_string({ip_port/0, emqx_schema, to_ip_port}).
-typerefl_from_string({cipher/0, emqx_schema, to_erl_cipher_suite}).
-typerefl_from_string({comma_separated_atoms/0, emqx_schema, to_comma_separated_atoms}).

-export([
    validate_heap_size/1,
    parse_user_lookup_fun/1,
    validate_alarm_actions/1,
    validations/0
]).

-export([qos/0]).

% workaround: prevent being recognized as unused functions
-export([
    to_duration/1,
    to_duration_s/1,
    to_duration_ms/1,
    mk_duration/2,
    to_bytesize/1,
    to_wordsize/1,
    to_percent/1,
    to_comma_separated_list/1,
    to_bar_separated_list/1,
    to_ip_port/1,
    to_erl_cipher_suite/1,
    to_comma_separated_atoms/1
]).

-behaviour(hocon_schema).

-reflect_type([
    duration/0,
    duration_s/0,
    duration_ms/0,
    bytesize/0,
    wordsize/0,
    percent/0,
    file/0,
    comma_separated_list/0,
    bar_separated_list/0,
    ip_port/0,
    cipher/0,
    comma_separated_atoms/0
]).

-export([namespace/0, roots/0, roots/1, fields/1, desc/1]).
-export([conf_get/2, conf_get/3, keys/2, filter/1]).
-export([server_ssl_opts_schema/2, client_ssl_opts_schema/1, ciphers_schema/1, default_ciphers/1]).
-export([sc/2, map/2]).

-elvis([{elvis_style, god_modules, disable}]).

-define(IDLE_TIMOUT_DESC,
    "Close transport-layer connections from the clients that have not sent MQTT CONNECT\n"
    "message within this interval."
).

namespace() -> undefined.

roots() ->
    %% TODO change config importance to a field metadata
    roots(high) ++ roots(medium) ++ roots(low).

roots(high) ->
    [
        {"listeners",
            sc(
                ref("listeners"),
                #{}
            )},
        {"zones",
            sc(
                map("name", ref("zone")),
                #{
                    desc =>
                        "A zone is a set of configs grouped by the zone <code>name</code>.<br>\n"
                        "For flexible configuration mapping, the <code>name</code>\n"
                        "can be set to a listener's <code>zone</code> config.<br>\n"
                        "NOTE: A built-in zone named <code>default</code> is auto created\n"
                        "and can not be deleted."
                }
            )},
        {"mqtt",
            sc(
                ref("mqtt"),
                #{}
            )},
        {?EMQX_AUTHENTICATION_CONFIG_ROOT_NAME,
            authentication(
                "Default authentication configs for all MQTT listeners.\n"
                "<br>\n"
                "For per-listener overrides see <code>authentication</code>\n"
                "in listener configs\n"
                "<br>\n"
                "<br>\n"
                "EMQX can be configured with:\n"
                "<br>\n"
                "<ul>\n"
                "<li><code>[]</code>: The default value, it allows *ALL* logins</li>\n"
                "<li>one: For example <code>{enable:true,backend:\"built_in_database\",mechanism=\"password_based\"}\n"
                "</code></li>\n"
                "<li>chain: An array of structs.</li>\n"
                "</ul>\n"
                "<br>\n"
                "When a chain is configured, the login credentials are checked against the backends\n"
                "per the configured order, until an 'allow' or 'deny' decision can be made.\n"
                "<br>\n"
                "If there is no decision after a full chain exhaustion, the login is rejected.\n"
            )},
        %% NOTE: authorization schema here is only to keep emqx app prue
        %% the full schema for EMQX node is injected in emqx_conf_schema.
        {"authorization",
            sc(
                ref("authorization"),
                #{}
            )}
    ];
roots(medium) ->
    [
        {"broker",
            sc(
                ref("broker"),
                #{}
            )},
        {"sys_topics",
            sc(
                ref("sys_topics"),
                #{}
            )},
        {"rate_limit",
            sc(
                ref("rate_limit"),
                #{}
            )},
        {"force_shutdown",
            sc(
                ref("force_shutdown"),
                #{}
            )},
        {"overload_protection",
            sc(
                ref("overload_protection"),
                #{}
            )}
    ];
roots(low) ->
    [
        {"force_gc",
            sc(
                ref("force_gc"),
                #{}
            )},
        {"conn_congestion",
            sc(
                ref("conn_congestion"),
                #{}
            )},
        {"stats",
            sc(
                ref("stats"),
                #{}
            )},
        {"sysmon",
            sc(
                ref("sysmon"),
                #{}
            )},
        {"alarm",
            sc(
                ref("alarm"),
                #{}
            )},
        {"flapping_detect",
            sc(
                ref("flapping_detect"),
                #{}
            )},
        {"persistent_session_store",
            sc(
                ref("persistent_session_store"),
                #{}
            )},
        {"trace",
            sc(
                ref("trace"),
                #{}
            )}
    ].

fields("persistent_session_store") ->
    [
        {"enabled",
            sc(
                boolean(),
                #{
                    default => false,
                    desc =>
                        "Use the database to store information about persistent sessions.\n"
                        "This makes it possible to migrate a client connection to another\n"
                        "cluster node if a node is stopped.\n"
                }
            )},
        {"storage_type",
            sc(
                hoconsc:union([ram, disc]),
                #{
                    default => disc,
                    desc =>
                        "Store information about persistent sessions on disc or in ram.\n"
                        "If ram is chosen, all information about persistent sessions remains\n"
                        "as long as at least one node in a cluster is alive to keep the information.\n"
                        "If disc is chosen, the information is persisted on disc and will survive\n"
                        "cluster restart, at the price of more disc usage and less throughput.\n"
                }
            )},
        {"max_retain_undelivered",
            sc(
                duration(),
                #{
                    default => "1h",
                    desc =>
                        "The time messages that was not delivered to a persistent session\n"
                        "is stored before being garbage collected if the node the previous\n"
                        "session was handled on restarts of is stopped.\n"
                }
            )},
        {"message_gc_interval",
            sc(
                duration(),
                #{
                    default => "1h",
                    desc =>
                        "The starting interval for garbage collection of undelivered messages to\n"
                        "a persistent session. This affects how often the \"max_retain_undelivered\"\n"
                        "is checked for removal.\n"
                }
            )},
        {"session_message_gc_interval",
            sc(
                duration(),
                #{
                    default => "1m",
                    desc =>
                        "The starting interval for garbage collection of transient data for\n"
                        "persistent session messages. This does not affect the lifetime length\n"
                        "of persistent session messages.\n"
                }
            )}
    ];
fields("stats") ->
    [
        {"enable",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => "Enable/disable statistic data collection."
                }
            )}
    ];
fields("authorization") ->
    [
        {"no_match",
            sc(
                hoconsc:enum([allow, deny]),
                #{
                    default => allow,
                    %% TODO: make sources a reference link
                    desc =>
                        "Default access control action if the user or client matches no ACL rules,\n"
                        "or if no such user or client is found by the configurable authorization\n"
                        "sources such as built_in_database, an HTTP API, or a query against PostgreSQL.\n"
                        "Find more details in 'authorization.sources' config."
                }
            )},
        {"deny_action",
            sc(
                hoconsc:enum([ignore, disconnect]),
                #{default => ignore}
            )},
        {"cache",
            sc(
                ref(?MODULE, "cache"),
                #{}
            )}
    ];
fields("cache") ->
    [
        {"enable",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => "Enable or disable the authorization cache."
                }
            )},
        {"max_size",
            sc(
                range(1, 1048576),
                #{
                    default => 32,
                    desc => "Maximum number of cached items."
                }
            )},
        {"ttl",
            sc(
                duration(),
                #{
                    default => "1m",
                    desc => "Time to live for the cached data."
                }
            )}
    ];
fields("mqtt") ->
    [
        {"idle_timeout",
            sc(
                hoconsc:union([infinity, duration()]),
                #{
                    default => "15s",
                    desc =>
                        "Close TCP connections from the clients that have not sent MQTT CONNECT\n"
                        "message within this interval."
                }
            )},
        {"max_packet_size",
            sc(
                bytesize(),
                #{
                    default => "1MB",
                    desc => "Maximum MQTT packet size allowed."
                }
            )},
        {"max_clientid_len",
            sc(
                range(23, 65535),
                #{
                    default => 65535,
                    desc => "Maximum allowed length of MQTT clientId."
                }
            )},
        {"max_topic_levels",
            sc(
                range(1, 65535),
                #{
                    default => 65535,
                    desc => "Maximum topic levels allowed."
                }
            )},
        {"max_qos_allowed",
            sc(
                qos(),
                #{
                    default => 2,
                    desc => "Maximum QoS allowed."
                }
            )},
        {"max_topic_alias",
            sc(
                range(0, 65535),
                #{
                    default => 65535,
                    desc => "Maximum Topic Alias, 0 means no topic alias supported."
                }
            )},
        {"retain_available",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => "Support MQTT retained messages."
                }
            )},
        {"wildcard_subscription",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => "Support MQTT Wildcard Subscriptions."
                }
            )},
        {"shared_subscription",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => "Support MQTT Shared Subscriptions."
                }
            )},
        {"ignore_loop_deliver",
            sc(
                boolean(),
                #{
                    default => false,
                    desc => "Ignore loop delivery of messages for MQTT v3.1.1."
                }
            )},
        {"strict_mode",
            sc(
                boolean(),
                #{
                    default => false,
                    desc =>
                        "Parse MQTT messages in strict mode. "
                        "When set to true, invalid utf8 strings in for example "
                        "client ID, topic name, etc. will cause the client to be "
                        "disconnected"
                }
            )},
        {"response_information",
            sc(
                string(),
                #{
                    default => "",
                    desc =>
                        "Specify the response information returned to the client\n"
                        "This feature is disabled if is set to \"\"."
                }
            )},
        {"server_keepalive",
            sc(
                hoconsc:union([integer(), disabled]),
                #{
                    default => disabled,
                    desc =>
                        "'Server Keep Alive' of MQTT 5.0.\n"
                        "If the server returns a 'Server Keep Alive' in the CONNACK packet,\n"
                        "the client MUST use that value instead of the value it sent as the 'Keep Alive'."
                }
            )},
        {"keepalive_backoff",
            sc(
                float(),
                #{
                    default => 0.75,
                    desc =>
                        "The backoff for MQTT keepalive timeout. The broker will close the connection\n"
                        "after idling for 'Keepalive * backoff * 2'."
                }
            )},
        {"max_subscriptions",
            sc(
                hoconsc:union([range(1, inf), infinity]),
                #{
                    default => infinity,
                    desc => "Maximum number of subscriptions allowed."
                }
            )},
        {"upgrade_qos",
            sc(
                boolean(),
                #{
                    default => false,
                    desc => "Force upgrade of QoS level according to subscription."
                }
            )},
        {"max_inflight",
            sc(
                range(1, 65535),
                #{
                    default => 32,
                    desc =>
                        "Maximum size of the Inflight Window storing QoS1/2 "
                        "messages delivered but un-acked."
                }
            )},
        {"retry_interval",
            sc(
                duration(),
                #{
                    default => "30s",
                    desc => "Retry interval for QoS1/2 message delivering."
                }
            )},
        {"max_awaiting_rel",
            sc(
                hoconsc:union([integer(), infinity]),
                #{
                    default => 100,
                    desc => "Maximum QoS2 packets (Client -> Broker) awaiting PUBREL."
                }
            )},
        {"await_rel_timeout",
            sc(
                duration(),
                #{
                    default => "300s",
                    desc =>
                        "The QoS2 messages (Client -> Broker) will be dropped "
                        "if awaiting PUBREL timeout."
                }
            )},
        {"session_expiry_interval",
            sc(
                duration(),
                #{
                    default => "2h",
                    desc => "Default session expiry interval for MQTT V3.1.1 connections."
                }
            )},
        {"max_mqueue_len",
            sc(
                hoconsc:union([non_neg_integer(), infinity]),
                #{
                    default => 1000,
                    desc =>
                        "Maximum queue length. Enqueued messages when persistent client disconnected,\n"
                        "or inflight window is full."
                }
            )},
        {"mqueue_priorities",
            sc(
                hoconsc:union([map(), disabled]),
                #{
                    default => disabled,
                    desc =>
                        "Topic priorities.<br>\n"
                        "There's no priority table by default, hence all messages are treated equal.<br>\n"
                        "Priority number [1-255]<br>\n"
                        "\n"
                        "**NOTE**: Comma and equal signs are not allowed for priority topic names.<br>\n"
                        "**NOTE**: Messages for topics not in the priority table are treated as\n"
                        "either highest or lowest priority depending on the configured value for\n"
                        "<code>mqtt.mqueue_default_priority</code>.\n"
                        "<br><br>\n"
                        "**Examples**:\n"
                        "To configure <code>\"topic/1\" > \"topic/2\"</code>:<br/>\n"
                        "<code>mqueue_priorities: {\"topic/1\": 10, \"topic/2\": 8}</code>"
                }
            )},
        {"mqueue_default_priority",
            sc(
                hoconsc:enum([highest, lowest]),
                #{
                    default => lowest,
                    desc =>
                        "Default to the highest priority for topics not matching priority table."
                }
            )},
        {"mqueue_store_qos0",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => "Support enqueue QoS0 messages."
                }
            )},
        {"use_username_as_clientid",
            sc(
                boolean(),
                #{
                    default => false,
                    desc => "Replace client ID with the username."
                }
            )},
        {"peer_cert_as_username",
            sc(
                hoconsc:enum([disabled, cn, dn, crt, pem, md5]),
                #{
                    default => disabled,
                    desc =>
                        "Use the CN, DN or CRT field from the client certificate as a username.\n"
                        "Only works for the TLS connection."
                }
            )},
        {"peer_cert_as_clientid",
            sc(
                hoconsc:enum([disabled, cn, dn, crt, pem, md5]),
                #{
                    default => disabled,
                    desc =>
                        "Use the CN, DN or CRT field from the client certificate as a clientid.\n"
                        "Only works for the TLS connection."
                }
            )}
    ];
fields("zone") ->
    Fields = emqx_zone_schema:roots(),
    [{F, ref(emqx_zone_schema, F)} || F <- Fields];
fields("rate_limit") ->
    [
        {"max_conn_rate",
            sc(
                hoconsc:union([infinity, integer()]),
                #{
                    default => 1000,
                    desc => "Maximum connections per second."
                }
            )},
        {"conn_messages_in",
            sc(
                hoconsc:union([infinity, comma_separated_list()]),
                #{
                    default => infinity,
                    desc => "Message limit for the external MQTT connections."
                }
            )},
        {"conn_bytes_in",
            sc(
                hoconsc:union([infinity, comma_separated_list()]),
                #{
                    default => infinity,
                    desc =>
                        "Limit the rate of receiving packets for a MQTT connection.\n"
                        "The rate is counted by bytes of packets per second."
                }
            )}
    ];
fields("flapping_detect") ->
    [
        {"enable",
            sc(
                boolean(),
                #{
                    default => false,
                    desc =>
                        "Enable flapping connection detection feature."
                }
            )},
        {"max_count",
            sc(
                integer(),
                #{
                    default => 15,
                    desc =>
                        "The maximum number of disconnects allowed for a MQTT Client in `window_time`"
                }
            )},
        {"window_time",
            sc(
                duration(),
                #{
                    default => "1m",
                    desc => "The time window for flapping detection."
                }
            )},
        {"ban_time",
            sc(
                duration(),
                #{
                    default => "5m",
                    desc => "How long the flapping clientid will be banned."
                }
            )}
    ];
fields("force_shutdown") ->
    [
        {"enable",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => "Enable `force_shutdown` feature."
                }
            )},
        {"max_message_queue_len",
            sc(
                range(0, inf),
                #{
                    default => 1000,
                    desc => "Maximum message queue length."
                }
            )},
        {"max_heap_size",
            sc(
                wordsize(),
                #{
                    default => "32MB",
                    desc => "Total heap size",
                    validator => fun ?MODULE:validate_heap_size/1
                }
            )}
    ];
fields("overload_protection") ->
    [
        {"enable",
            sc(
                boolean(),
                #{
                    desc => "React on system overload or not",
                    default => false
                }
            )},
        {"backoff_delay",
            sc(
                range(0, inf),
                #{
                    desc =>
                        "Some unimportant tasks could be delayed "
                        "for execution, here set the delays in ms",
                    default => 1
                }
            )},
        {"backoff_gc",
            sc(
                boolean(),
                #{
                    desc => "Skip forceful GC if necessary",
                    default => false
                }
            )},
        {"backoff_hibernation",
            sc(
                boolean(),
                #{
                    desc => "Skip process hibernation if necessary",
                    default => true
                }
            )},
        {"backoff_new_conn",
            sc(
                boolean(),
                #{
                    desc => "Close new incoming connections if necessary",
                    default => true
                }
            )}
    ];
fields("conn_congestion") ->
    [
        {"enable_alarm",
            sc(
                boolean(),
                #{
                    default => false,
                    desc => "Enable or disable connection congestion alarm."
                }
            )},
        {"min_alarm_sustain_duration",
            sc(
                duration(),
                #{
                    default => "1m",
                    desc =>
                        "Minimal time before clearing the alarm.\n\n"
                        "The alarm is cleared only when there's no pending data in\n"
                        "the queue, and at least `min_alarm_sustain_duration`\n"
                        "milliseconds passed since the last time we considered the connection \"congested\".\n\n"
                        "This is to avoid clearing and raising the alarm again too often."
                }
            )}
    ];
fields("force_gc") ->
    [
        {"enable",
            sc(
                boolean(),
                #{default => true, desc => "Enable forced garbage collection."}
            )},
        {"count",
            sc(
                range(0, inf),
                #{
                    default => 16000,
                    desc => "GC the process after this many received messages."
                }
            )},
        {"bytes",
            sc(
                bytesize(),
                #{
                    default => "16MB",
                    desc => "GC the process after specified number of bytes have passed through."
                }
            )}
    ];
fields("listeners") ->
    [
        {"tcp",
            sc(
                map(name, ref("mqtt_tcp_listener")),
                #{
                    desc => "TCP listeners",
                    required => {false, recursively}
                }
            )},
        {"ssl",
            sc(
                map(name, ref("mqtt_ssl_listener")),
                #{
                    desc => "SSL listeners",
                    required => {false, recursively}
                }
            )},
        {"ws",
            sc(
                map(name, ref("mqtt_ws_listener")),
                #{
                    desc => "HTTP websocket listeners",
                    required => {false, recursively}
                }
            )},
        {"wss",
            sc(
                map(name, ref("mqtt_wss_listener")),
                #{
                    desc => "HTTPS websocket listeners",
                    required => {false, recursively}
                }
            )},
        {"quic",
            sc(
                map(name, ref("mqtt_quic_listener")),
                #{
                    desc => "QUIC listeners",
                    required => {false, recursively}
                }
            )}
    ];
fields("mqtt_tcp_listener") ->
    [
        {"tcp",
            sc(
                ref("tcp_opts"),
                #{}
            )}
    ] ++ mqtt_listener();
fields("mqtt_ssl_listener") ->
    [
        {"tcp",
            sc(
                ref("tcp_opts"),
                #{}
            )},
        {"ssl",
            sc(
                ref("listener_ssl_opts"),
                #{}
            )}
    ] ++ mqtt_listener();
fields("mqtt_ws_listener") ->
    [
        {"tcp",
            sc(
                ref("tcp_opts"),
                #{}
            )},
        {"websocket",
            sc(
                ref("ws_opts"),
                #{}
            )}
    ] ++ mqtt_listener();
fields("mqtt_wss_listener") ->
    [
        {"tcp",
            sc(
                ref("tcp_opts"),
                #{}
            )},
        {"ssl",
            sc(
                ref("listener_wss_opts"),
                #{}
            )},
        {"websocket",
            sc(
                ref("ws_opts"),
                #{}
            )}
    ] ++ mqtt_listener();
fields("mqtt_quic_listener") ->
    [
        {"enabled",
            sc(
                boolean(),
                #{default => true, desc => "Enable QUIC listener."}
            )},
        %% TODO: ensure cacertfile is configurable
        {"certfile",
            sc(
                string(),
                #{desc => "Path to the certificate."}
            )},
        {"keyfile",
            sc(
                string(),
                #{desc => "Path to the secret key file."}
            )},
        {"ciphers", ciphers_schema(quic)},
        {"idle_timeout",
            sc(
                duration(),
                #{default => "15s", desc => ?IDLE_TIMOUT_DESC}
            )}
    ] ++ base_listener();
fields("ws_opts") ->
    [
        {"mqtt_path",
            sc(
                string(),
                #{
                    default => "/mqtt",
                    desc =>
                        "WebSocket's MQTT protocol path. So the address of\n"
                        " EMQX Broker's WebSocket is: <code>ws://{ip}:{port}/mqtt</code>"
                }
            )},
        {"mqtt_piggyback",
            sc(
                hoconsc:enum([single, multiple]),
                #{default => multiple}
            )},
        {"compress",
            sc(
                boolean(),
                #{
                    default => false,
                    desc =>
                        "If <code>true</code>, compress WebSocket messages using <code>zlib</code>.<br/>\n"
                        "The configuration items under <code>deflate_opts</code> "
                        "belong to the compression-related parameter configuration."
                }
            )},
        {"idle_timeout",
            sc(
                duration(),
                #{
                    default => "15s",
                    desc => ?IDLE_TIMOUT_DESC
                }
            )},
        {"max_frame_size",
            sc(
                hoconsc:union([infinity, integer()]),
                #{
                    default => infinity,
                    desc => "The maximum length of a single MQTT packet."
                }
            )},
        {"fail_if_no_subprotocol",
            sc(
                boolean(),
                #{
                    default => true,
                    desc =>
                        "If <code>true</code>, the server will return an error when\n"
                        " the client does not carry the <code>Sec-WebSocket-Protocol</code> field.\n"
                        " <br/>Note: WeChat applet needs to disable this verification."
                }
            )},
        {"supported_subprotocols",
            sc(
                comma_separated_list(),
                #{
                    default => "mqtt, mqtt-v3, mqtt-v3.1.1, mqtt-v5",
                    desc => "Comma-separated list of supported subprotocols."
                }
            )},
        {"check_origin_enable",
            sc(
                boolean(),
                #{
                    default => false,
                    desc =>
                        "If <code>true</code>, <code>origin</code> HTTP header will be\n"
                        " validated against the list of allowed origins configured in <code>check_origins</code>\n"
                        " parameter."
                }
            )},
        {"allow_origin_absence",
            sc(
                boolean(),
                #{
                    default => true,
                    desc =>
                        "If <code>false</code> and <code>check_origin_enable</code> is\n"
                        " <code>true</code>, the server will reject requests that don't have <code>origin</code>\n"
                        " HTTP header."
                }
            )},
        {"check_origins",
            sc(
                hoconsc:array(binary()),
                #{
                    default => [],
                    desc => "List of allowed origins.<br/>See <code>check_origin_enable</code>."
                }
            )},
        {"proxy_address_header",
            sc(
                string(),
                #{
                    default => "x-forwarded-for",
                    desc =>
                        "HTTP header used to pass information about the client IP address.\n"
                        " Relevant when the EMQX cluster is deployed behind a load-balancer."
                }
            )},
        {"proxy_port_header",
            sc(
                string(),
                #{
                    default => "x-forwarded-port",
                    desc =>
                        "HTTP header used to pass information about the client port.\n"
                        " Relevant when the EMQX cluster is deployed behind a load-balancer."
                }
            )},
        {"deflate_opts",
            sc(
                ref("deflate_opts"),
                #{}
            )}
    ];
fields("tcp_opts") ->
    [
        {"active_n",
            sc(
                integer(),
                #{
                    default => 100,
                    desc =>
                        "Specify the {active, N} option for this Socket.<br/>\n"
                        " See: https://erlang.org/doc/man/inet.html#setopts-2"
                }
            )},
        {"backlog",
            sc(
                integer(),
                #{
                    default => 1024,
                    desc =>
                        "TCP backlog defines the maximum length that the queue of\n"
                        " pending connections can grow to."
                }
            )},
        {"send_timeout",
            sc(
                duration(),
                #{
                    default => "15s",
                    desc => "The TCP send timeout for the connections."
                }
            )},
        {"send_timeout_close",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => "Close the connection if send timeout."
                }
            )},
        {"recbuf",
            sc(
                bytesize(),
                #{desc => "The TCP receive buffer (OS kernel) for the connections."}
            )},
        {"sndbuf",
            sc(
                bytesize(),
                #{desc => "The TCP send buffer (OS kernel) for the connections."}
            )},
        {"buffer",
            sc(
                bytesize(),
                #{desc => "The size of the user-space buffer used by the driver."}
            )},
        {"high_watermark",
            sc(
                bytesize(),
                #{
                    default => "1MB",
                    desc =>
                        "The socket is set to a busy state when the amount of data queued internally\n"
                        "  by the VM socket implementation reaches this limit."
                }
            )},
        {"nodelay",
            sc(
                boolean(),
                #{
                    default => false,
                    desc => "The TCP_NODELAY flag for the connections."
                }
            )},
        {"reuseaddr",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => "The SO_REUSEADDR flag for the connections."
                }
            )}
    ];
fields("listener_ssl_opts") ->
    server_ssl_opts_schema(
        #{
            depth => 10,
            reuse_sessions => true,
            versions => tls_all_available,
            ciphers => tls_all_available
        },
        false
    );
fields("listener_wss_opts") ->
    server_ssl_opts_schema(
        #{
            depth => 10,
            reuse_sessions => true,
            versions => tls_all_available,
            ciphers => tls_all_available
        },
        true
    );
fields(ssl_client_opts) ->
    client_ssl_opts_schema(#{});
fields("deflate_opts") ->
    [
        {"level",
            sc(
                hoconsc:enum([none, default, best_compression, best_speed]),
                #{desc => "Compression level."}
            )},
        {"mem_level",
            sc(
                range(1, 9),
                #{
                    default => 8,
                    desc =>
                        "Specifies the size of the compression state.<br/>\n"
                        " Lower values decrease memory usage per connection."
                }
            )},
        {"strategy",
            sc(
                hoconsc:enum([default, filtered, huffman_only, rle]),
                #{desc => "Specifies the compression strategy."}
            )},
        {"server_context_takeover",
            sc(
                hoconsc:enum([takeover, no_takeover]),
                #{
                    desc =>
                        "Takeover means the compression state is retained\n"
                        " between server messages."
                }
            )},
        {"client_context_takeover",
            sc(
                hoconsc:enum([takeover, no_takeover]),
                #{
                    desc =>
                        "Takeover means the compression state is retained\n"
                        " between client messages."
                }
            )},
        {"server_max_window_bits",
            sc(
                range(8, 15),
                #{
                    default => 15,
                    desc => "Specifies the size of the compression context for the server."
                }
            )},
        {"client_max_window_bits",
            sc(
                range(8, 15),
                #{
                    default => 15,
                    desc => "Specifies the size of the compression context for the client."
                }
            )}
    ];
fields("broker") ->
    [
        {"enable_session_registry",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => "Enable session registry"
                }
            )},
        {"session_locking_strategy",
            sc(
                hoconsc:enum([local, leader, quorum, all]),
                #{
                    default => quorum,
                    desc =>
                        "Session locking strategy in a cluster.<br/>\n"
                        " - `local`: only lock the session on the current node\n"
                        " - `one`: select only one remote node to lock the session\n"
                        " - `quorum`: select some nodes to lock the session\n"
                        " - `all`: lock the session on all the nodes in the cluster"
                }
            )},
        {"shared_subscription_strategy",
            sc(
                hoconsc:enum([random, round_robin, sticky, hash_topic, hash_clientid]),
                #{
                    default => round_robin,
                    desc =>
                        "Dispatch strategy for shared subscription.<br/>\n"
                        " - `random`: dispatch the message to a random selected subscriber\n"
                        " - `round_robin`: select the subscribers in a round-robin manner\n"
                        " - `sticky`: always use the last selected subscriber to dispatch,\n"
                        "   until the subscriber disconnects.\n"
                        " - `hash`: select the subscribers by the hash of `clientIds`"
                }
            )},
        {"shared_dispatch_ack_enabled",
            sc(
                boolean(),
                #{
                    default => false,
                    desc =>
                        "Enable/disable shared dispatch acknowledgement for QoS1 and QoS2 messages.<br/>\n"
                        " This should allow messages to be dispatched to a different subscriber in\n"
                        " the group in case the picked (based on `shared_subscription_strategy`) subscriber\n"
                        " is offline."
                }
            )},
        {"route_batch_clean",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => "Enable batch clean for deleted routes."
                }
            )},
        {"perf",
            sc(
                ref("broker_perf"),
                #{}
            )}
    ];
fields("broker_perf") ->
    [
        {"route_lock_type",
            sc(
                hoconsc:enum([key, tab, global]),
                #{default => key}
            )},
        {"trie_compaction",
            sc(
                boolean(),
                #{default => true}
            )}
    ];
fields("sys_topics") ->
    [
        {"sys_msg_interval",
            sc(
                hoconsc:union([disabled, duration()]),
                #{default => "1m"}
            )},
        {"sys_heartbeat_interval",
            sc(
                hoconsc:union([disabled, duration()]),
                #{default => "30s"}
            )},
        {"sys_event_messages",
            sc(
                ref("event_names"),
                #{}
            )}
    ];
fields("event_names") ->
    [
        {"client_connected",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => "Connection complete"
                }
            )},
        {"client_disconnected",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => "Disconnect"
                }
            )},
        {"client_subscribed",
            sc(
                boolean(),
                #{
                    default => false,
                    desc => "Subscribe"
                }
            )},
        {"client_unsubscribed",
            sc(
                boolean(),
                #{
                    default => false,
                    desc => "Unsubscribe"
                }
            )}
    ];
fields("sysmon") ->
    [
        {"vm",
            sc(
                ref("sysmon_vm"),
                #{}
            )},
        {"os",
            sc(
                ref("sysmon_os"),
                #{}
            )},
        {"top",
            sc(
                ref("sysmon_top"),
                #{}
            )}
    ];
fields("sysmon_vm") ->
    [
        {"process_check_interval",
            sc(
                duration(),
                #{
                    default => "30s",
                    desc => "The time interval for the periodic process limit check."
                }
            )},
        {"process_high_watermark",
            sc(
                percent(),
                #{
                    default => "80%",
                    desc =>
                        "The threshold, as percentage of processes, for how many\n"
                        " processes can simultaneously exist at the local node before the corresponding\n"
                        " alarm is raised."
                }
            )},
        {"process_low_watermark",
            sc(
                percent(),
                #{
                    default => "60%",
                    desc =>
                        "The threshold, as percentage of processes, for how many\n"
                        " processes can simultaneously exist at the local node before the corresponding\n"
                        " alarm is cleared."
                }
            )},
        {"long_gc",
            sc(
                hoconsc:union([disabled, duration()]),
                #{
                    desc =>
                        "Enable Long GC monitoring.<br/>\n"
                        "Notice: don't enable the monitor in production, because it adds overhead to\n"
                        " garbage collection."
                }
            )},
        {"long_schedule",
            sc(
                hoconsc:union([disabled, duration()]),
                #{
                    default => "240ms",
                    desc => "Enable Long Schedule monitoring."
                }
            )},
        {"large_heap",
            sc(
                hoconsc:union([disabled, bytesize()]),
                #{
                    default => "32MB",
                    desc => "Enable Large Heap monitoring."
                }
            )},
        {"busy_dist_port",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => "Enable Busy Distribution Port monitoring."
                }
            )},
        {"busy_port",
            sc(
                boolean(),
                #{
                    default => true,
                    desc => "Enable Busy Port monitoring."
                }
            )}
    ];
fields("sysmon_os") ->
    [
        {"cpu_check_interval",
            sc(
                duration(),
                #{
                    default => "60s",
                    desc => "The time interval for the periodic CPU check."
                }
            )},
        {"cpu_high_watermark",
            sc(
                percent(),
                #{
                    default => "80%",
                    desc =>
                        "The threshold, as percentage of system CPU load,\n"
                        " for how much system cpu can be used before the corresponding alarm is raised."
                }
            )},
        {"cpu_low_watermark",
            sc(
                percent(),
                #{
                    default => "60%",
                    desc =>
                        "The threshold, as percentage of system CPU load,\n"
                        " for how much system cpu can be used before the corresponding alarm is cleared."
                }
            )},
        {"mem_check_interval",
            sc(
                hoconsc:union([disabled, duration()]),
                #{
                    default => "60s",
                    desc => "The time interval for the periodic memory check."
                }
            )},
        {"sysmem_high_watermark",
            sc(
                percent(),
                #{
                    default => "70%",
                    desc =>
                        "The threshold, as percentage of system memory,\n"
                        " for how much system memory can be allocated before the corresponding alarm is raised."
                }
            )},
        {"procmem_high_watermark",
            sc(
                percent(),
                #{
                    default => "5%",
                    desc =>
                        "The threshold, as percentage of system memory,\n"
                        " for how much system memory can be allocated by one Erlang process before\n"
                        " the corresponding alarm is raised."
                }
            )}
    ];
fields("sysmon_top") ->
    [
        {"num_items",
            sc(
                non_neg_integer(),
                #{
                    mapping => "system_monitor.top_num_items",
                    default => 10,
                    desc => "The number of top processes per monitoring group"
                }
            )},
        {"sample_interval",
            sc(
                emqx_schema:duration(),
                #{
                    mapping => "system_monitor.top_sample_interval",
                    default => "2s",
                    desc => "Specifies how often process top should be collected"
                }
            )},
        {"max_procs",
            sc(
                non_neg_integer(),
                #{
                    mapping => "system_monitor.top_max_procs",
                    default => 1_000_000,
                    desc =>
                        "Stop collecting data when the number of processes\n"
                        "in the VM exceeds this value"
                }
            )},
        {"db_hostname",
            sc(
                string(),
                #{
                    mapping => "system_monitor.db_hostname",
                    desc => "Hostname of the PostgreSQL database that collects the data points",
                    default => ""
                }
            )},
        {"db_port",
            sc(
                integer(),
                #{
                    mapping => "system_monitor.db_port",
                    default => 5432,
                    desc => "Port of the PostgreSQL database that collects the data points"
                }
            )},
        {"db_username",
            sc(
                string(),
                #{
                    mapping => "system_monitor.db_username",
                    default => "system_monitor",
                    desc => "EMQX username in the PostgreSQL database"
                }
            )},
        {"db_password",
            sc(
                binary(),
                #{
                    mapping => "system_monitor.db_password",
                    default => "system_monitor_password",
                    desc => "EMQX user password in the PostgreSQL database"
                }
            )},
        {"db_name",
            sc(
                string(),
                #{
                    mapping => "system_monitor.db_name",
                    default => "postgres",
                    desc => "PostgreSQL database name"
                }
            )}
    ];
fields("alarm") ->
    [
        {"actions",
            sc(
                hoconsc:array(atom()),
                #{
                    default => [log, publish],
                    validator => fun ?MODULE:validate_alarm_actions/1,
                    example => [log, publish],
                    desc =>
                        "The actions triggered when the alarm is activated.<br/>\n"
                        "Currently, the following actions are supported: <code>log</code> and "
                        "<code>publish</code>.\n"
                        "<code>log</code> is to write the alarm to log (console or file).\n"
                        "<code>publish</code> is to publish the alarm as an MQTT message to "
                        "the system topics:\n"
                        "<code>$SYS/brokers/emqx@xx.xx.xx.x/alarms/activate</code> and\n"
                        "<code>$SYS/brokers/emqx@xx.xx.xx.x/alarms/deactivate</code>"
                }
            )},
        {"size_limit",
            sc(
                range(1, 3000),
                #{
                    default => 1000,
                    example => 1000,
                    desc =>
                        "The maximum total number of deactivated alarms to keep as history.<br>\n"
                        "When this limit is exceeded, the oldest deactivated alarms are "
                        "deleted to cap the total number.\n"
                }
            )},
        {"validity_period",
            sc(
                duration(),
                #{
                    default => "24h",
                    example => "24h",
                    desc =>
                        "Retention time of deactivated alarms. Alarms are not deleted immediately\n"
                        "when deactivated, but after the retention time.\n"
                }
            )}
    ];
fields("trace") ->
    [
        {"payload_encode",
            sc(hoconsc:enum([hex, text, hidden]), #{
                default => text,
                desc =>
                    "Determine the format of the payload format in the trace file.<br>\n"
                    "`text`: Text-based protocol or plain text protocol.\n"
                    " It is recommended when payload is JSON encoded.<br>\n"
                    "`hex`: Binary hexadecimal encode. It is recommended when payload is "
                    "a custom binary protocol.<br>\n"
                    "`hidden`: payload is obfuscated as `******`\n"
            })}
    ].

mqtt_listener() ->
    base_listener() ++
        [
            {"access_rules",
                sc(
                    hoconsc:array(string()),
                    #{
                        desc =>
                            "The access control rules for this listener.<br/>"
                            "See: https://github.com/emqtt/esockd#allowdeny"
                    }
                )},
            {"proxy_protocol",
                sc(
                    boolean(),
                    #{
                        default => false,
                        desc =>
                            "Enable the Proxy Protocol V1/2 if the EMQX cluster is deployed\n"
                            " behind HAProxy or Nginx.<br/>"
                            "See: https://www.haproxy.com/blog/haproxy/proxy-protocol/"
                    }
                )},
            {"proxy_protocol_timeout",
                sc(
                    duration(),
                    #{
                        desc =>
                            "Timeout for proxy protocol. EMQX will close the TCP connection "
                            "if proxy protocol packet is not received within the timeout."
                    }
                )},
            {?EMQX_AUTHENTICATION_CONFIG_ROOT_NAME,
                authentication("Per-listener authentication override")}
        ].

base_listener() ->
    [
        {"bind",
            sc(
                hoconsc:union([ip_port(), integer()]),
                #{
                    required => true,
                    desc => "IP address and port for the listening socket."
                }
            )},
        {"acceptors",
            sc(
                integer(),
                #{
                    default => 16,
                    desc => "The size of the listener's receiving pool."
                }
            )},
        {"max_connections",
            sc(
                hoconsc:union([infinity, integer()]),
                #{
                    default => infinity,
                    desc => "The maximum number of concurrent connections allowed by the listener."
                }
            )},
        {"mountpoint",
            sc(
                binary(),
                #{
                    default => <<>>,
                    desc =>
                        "When publishing or subscribing, prefix all topics with a mountpoint string.\n"
                        " The prefixed string will be removed from the topic name when the message\n"
                        " is delivered to the subscriber. The mountpoint is a way that users can use\n"
                        " to implement isolation of message routing between different listeners.\n"
                        " For example if a client A subscribes to `t` with `listeners.tcp.<name>.mountpoint`\n"
                        " set to `some_tenant`, then the client actually subscribes to the topic\n"
                        " `some_tenant/t`. Similarly, if another client B (connected to the same listener\n"
                        " as the client A) sends a message to topic `t`, the message is routed\n"
                        " to all the clients subscribed `some_tenant/t`, so client A will receive the\n"
                        " message, with topic name `t`.<br/>\n"
                        " Set to `\"\"` to disable the feature.<br/>\n"
                        "\n"
                        " Variables in mountpoint string:\n"
                        " - <code>${clientid}</code>: clientid\n"
                        " - <code>${username}</code>: username"
                }
            )},
        {"zone",
            sc(
                atom(),
                #{
                    default => 'default',
                    desc => "The configuration zone to which the listener belongs."
                }
            )},
        {"limiter",
            sc(
                map("ratelimit's type", emqx_limiter_schema:bucket_name()),
                #{
                    default => #{},
                    desc => "Type of the rate limit."
                }
            )}
    ].

desc("persistent_session_store") ->
    "Settings for message persistence.";
desc("stats") ->
    "Enable/disable statistic data collection.\n"
    "Statistic data such as message receive/send count/rate etc. "
    "It provides insights of system performance and helps to diagnose issues. "
    "You can find statistic data from the dashboard, or from the '/stats' API.";
desc("authorization") ->
    "Settings for client authorization.";
desc("mqtt") ->
    "Global MQTT configuration.<br>\n"
    "The configs here work as default values which can be overridden\n"
    "in <code>zone</code> configs";
desc("cache") ->
    "Settings for the authorization cache.";
desc("zone") ->
    "A `Zone` defines a set of configuration items (such as the maximum number of connections)"
    " that can be shared between multiple listeners.\n\n"
    "`Listener` can refer to a `Zone` through the configuration item"
    " <code>listener.<Protocol>.<Listener Name>.zone</code>.\n\n"
    "The configs defined in the zones will override the global configs with the same key.\n\n"
    "For example, given the following config:\n"
    "```\n"
    "a {\n"
    "    b: 1, c: 1\n"
    "}\n"
    "zone.my_zone {\n"
    "  a {\n"
    "    b:2\n"
    "  }\n"
    "}\n"
    "```\n\n"
    "The global config `a` is overridden by the configs `a` inside the zone `my_zone`.\n\n"
    "If there is a listener using the zone `my_zone`, the value of config `a` will be: "
    "`{b:2, c: 1}`.\n"
    "Note that although the default value of `a.c` is `0`, the global value is used,"
    " i.e. configs in the zone have no default values. To override `a.c` one must configure"
    " it explicitly in the zone.\n\n"
    "All the global configs that can be overridden in zones are:\n"
    " - `stats.*`\n"
    " - `mqtt.*`\n"
    " - `authorization.*`\n"
    " - `flapping_detect.*`\n"
    " - `force_shutdown.*`\n"
    " - `conn_congestion.*`\n"
    " - `force_gc.*`\n\n";
desc("rate_limit") ->
    "Rate limit settings.";
desc("flapping_detect") ->
    "This config controls the allowed maximum number of `CONNECT` packets received\n"
    "from the same clientid in a time frame defined by `window_time`.\n"
    "After the limit is reached, successive `CONNECT` requests are forbidden\n"
    "(banned) until the end of the time period defined by `ban_time`.";
desc("force_shutdown") ->
    "When the process message queue length, or the memory bytes\n"
    "reaches a certain value, the process is forced to close.\n\n"
    "Note: \"message queue\" here refers to the \"message mailbox\"\n"
    "of the Erlang process, not the `mqueue` of QoS 1 and QoS 2.";
desc("overload_protection") ->
    "Overload protection mechanism monitors the load of the system and temporarily\n"
    "disables some features (such as accepting new connections) when the load is high.";
desc("conn_congestion") ->
    "Settings for `conn_congestion` alarm.\n\n"
    "Sometimes the MQTT connection (usually an MQTT subscriber) may\n"
    "get \"congested\", because there are too many packets to be sent.\n"
    "The socket tries to buffer the packets until the buffer is\n"
    "full. If more packets arrive after that, the packets will be\n"
    "\"pending\" in the queue and we consider the connection\n"
    "congested.\n\n"
    "Note: `sndbuf` can be set to larger value if the\n"
    "alarm is triggered too often.\n"
    "The name of the alarm is of format `conn_congestion/<ClientID>/<Username>`,\n"
    "where the `<ClientID>` is the client ID of the congested MQTT connection,\n"
    "and `<Username>` is the username or `unknown_user`.";
desc("force_gc") ->
    "Force garbage collection in MQTT connection process after\n"
    " they process certain number of messages or bytes of data.";
desc("listeners") ->
    "MQTT listeners identified by their protocol type and assigned names";
desc("mqtt_tcp_listener") ->
    "Settings for the MQTT over TCP listener.";
desc("mqtt_ssl_listener") ->
    "Settings for the MQTT over SSL listener.";
desc("mqtt_ws_listener") ->
    "Settings for the MQTT over WebSocket listener.";
desc("mqtt_wss_listener") ->
    "Settings for the MQTT over WebSocket/SSL listener.";
desc("mqtt_quic_listener") ->
    "Settings for the MQTT over QUIC listener.";
desc("ws_opts") ->
    "WebSocket listener options.";
desc("tcp_opts") ->
    "TCP listener options.";
desc("listener_ssl_opts") ->
    "Socket options for SSL connections.";
desc("listener_wss_opts") ->
    "Socket options for WebSocket/SSL connections.";
desc(ssl_client_opts) ->
    "Socket options for SSL clients.";
desc("deflate_opts") ->
    "Compression options.";
desc("broker") ->
    "Message broker options.";
desc("broker_perf") ->
    "Broker performance tuning parameters.";
desc("sys_topics") ->
    "The EMQX Broker periodically publishes its own status, message statistics,\n"
    "client online and offline events to the system topic starting with `$SYS/`.\n\n"
    "The following options control the behavior of `$SYS` topics.";
desc("event_names") ->
    "Enable or disable client lifecycle event publishing.\n\n"
    "The following options affect MQTT clients as well as\n"
    "gateway clients. The types of the clients\n"
    "are distinguished by the topic prefix:\n\n"
    "- For the MQTT clients, the format is:\n"
    "`$SYS/broker/<node>/clients/<clientid>/<event>`\n"
    "- For the Gateway clients, it is\n"
    "`$SYS/broker/<node>/gateway/<gateway-name>/clients/<clientid>/<event>`\n";
desc("sysmon") ->
    "Features related to system monitoring and introspection.";
desc("sysmon_vm") ->
    "This part of the configuration is responsible for collecting\n"
    " BEAM VM events, such as long garbage collection, traffic congestion in the inter-broker\n"
    " communication, etc.";
desc("sysmon_os") ->
    "This part of the configuration is responsible for monitoring\n"
    " the host OS health, such as free memory, disk space, CPU load, etc.";
desc("sysmon_top") ->
    "This part of the configuration is responsible for monitoring\n"
    " the Erlang processes in the VM. This information can be sent to an external\n"
    " PostgreSQL database. This feature is inactive unless the PostgreSQL sink is configured.";
desc("alarm") ->
    "Settings for the alarms.";
desc("trace") ->
    "Real-time filtering logs for the ClientID or Topic or IP for debugging.";
desc(_) ->
    undefined.

%% utils
-spec conf_get(string() | [string()], hocon:config()) -> term().
conf_get(Key, Conf) ->
    V = hocon_maps:get(Key, Conf),
    case is_binary(V) of
        true ->
            binary_to_list(V);
        false ->
            V
    end.

conf_get(Key, Conf, Default) ->
    V = hocon_maps:get(Key, Conf, Default),
    case is_binary(V) of
        true ->
            binary_to_list(V);
        false ->
            V
    end.

filter(Opts) ->
    [{K, V} || {K, V} <- Opts, V =/= undefined].

%% @private This function defines the SSL opts which are commonly used by
%% SSL listener and client.
-spec common_ssl_opts_schema(map()) -> hocon_schema:field_schema().
common_ssl_opts_schema(Defaults) ->
    D = fun(Field) -> maps:get(to_atom(Field), Defaults, undefined) end,
    Df = fun(Field, Default) -> maps:get(to_atom(Field), Defaults, Default) end,
    [
        {"enable",
            sc(
                boolean(),
                #{
                    default => Df("enable", false),
                    desc => "Enable TLS."
                }
            )},
        {"cacertfile",
            sc(
                string(),
                #{
                    default => D("cacertfile"),
                    required => false,
                    desc =>
                        "Trusted PEM format CA certificates bundle file.<br>\n"
                        "The certificates in this file are used to verify the TLS peer's certificates.\n"
                        "Append new certificates to the file if new CAs are to be trusted.\n"
                        "There is no need to restart EMQX to have the updated file loaded, because\n"
                        "the system regularly checks if file has been updated (and reload).<br>\n"
                        "NOTE: invalidating (deleting) a certificate from the file will not affect\n"
                        "already established connections.\n"
                }
            )},
        {"certfile",
            sc(
                string(),
                #{
                    default => D("certfile"),
                    required => false,
                    desc =>
                        "PEM format certificates chain file.<br>\n"
                        "The certificates in this file should be in reversed order of the certificate\n"
                        "issue chain. That is, the host's certificate should be placed in the beginning\n"
                        "of the file, followed by the immediate issuer certificate and so on.\n"
                        "Although the root CA certificate is optional, it should be placed at the end of\n"
                        "the file if it is to be added."
                }
            )},
        {"keyfile",
            sc(
                string(),
                #{
                    default => D("keyfile"),
                    required => false,
                    desc =>
                        "PEM format private key file."
                }
            )},
        {"verify",
            sc(
                hoconsc:enum([verify_peer, verify_none]),
                #{
                    default => Df("verify", verify_none),
                    desc =>
                        "Enable or disable peer verification."
                }
            )},
        {"reuse_sessions",
            sc(
                boolean(),
                #{
                    default => Df("reuse_sessions", true),
                    desc =>
                        "Enable TLS session reuse."
                }
            )},
        {"depth",
            sc(
                integer(),
                #{
                    default => Df("depth", 10),
                    desc =>
                        "Maximum number of non-self-issued intermediate certificates that can follow "
                        "the peer certificate in a valid certification path. "
                        "So, if depth is 0 the PEER must be signed by the trusted ROOT-CA directly; "
                        "if 1 the path can be PEER, CA, ROOT-CA; if 2 the path can be PEER, CA, CA, ROOT-CA, "
                        "and so on. The default value is 10."
                }
            )},
        {"password",
            sc(
                string(),
                #{
                    sensitive => true,
                    required => false,
                    desc =>
                        "String containing the user's password. Only used if the private\n"
                        "key file is password-protected."
                }
            )},
        {"versions",
            sc(
                hoconsc:array(typerefl:atom()),
                #{
                    default => default_tls_vsns(maps:get(versions, Defaults, tls_all_available)),
                    desc =>
                        "All TLS/DTLS versions to be supported.<br>\n"
                        "NOTE: PSK ciphers are suppressed by 'tlsv1.3' version config<br>\n"
                        "In case PSK cipher suites are intended, make sure to configured\n"
                        "<code>['tlsv1.2', 'tlsv1.1']</code> here.",
                    validator => fun validate_tls_versions/1
                }
            )},
        {"ciphers", ciphers_schema(D("ciphers"))},
        {user_lookup_fun,
            sc(
                typerefl:alias("string", any()),
                #{
                    default => <<"emqx_tls_psk:lookup">>,
                    converter => fun ?MODULE:parse_user_lookup_fun/1,
                    desc =>
                        "EMQX-internal callback that is used to lookup pre-shared key (PSK) identity."
                }
            )},
        {"secure_renegotiate",
            sc(
                boolean(),
                #{
                    default => Df("secure_renegotiate", true),
                    desc =>
                        "SSL parameter renegotiation is a feature that allows a client and a server\n"
                        "to renegotiate the parameters of the SSL connection on the fly.\n"
                        "RFC 5746 defines a more secure way of doing this. By enabling secure renegotiation,\n"
                        "you drop support for the insecure renegotiation, prone to MitM attacks."
                }
            )}
    ].

%% @doc Make schema for SSL listener options.
%% When it's for ranch listener, an extra field `handshake_timeout' is added.
-spec server_ssl_opts_schema(map(), boolean()) -> hocon_schema:field_schema().
server_ssl_opts_schema(Defaults, IsRanchListener) ->
    D = fun(Field) -> maps:get(to_atom(Field), Defaults, undefined) end,
    Df = fun(Field, Default) -> maps:get(to_atom(Field), Defaults, Default) end,
    common_ssl_opts_schema(Defaults) ++
        [
            {"dhfile",
                sc(
                    string(),
                    #{
                        default => D("dhfile"),
                        required => false,
                        desc =>
                            "Path to a file containing PEM-encoded Diffie Hellman parameters\n"
                            "to be used by the server if a cipher suite using Diffie Hellman\n"
                            "key exchange is negotiated. If not specified, default parameters\n"
                            "are used.<br>\n"
                            "NOTE: The <code>dhfile</code> option is not supported by TLS 1.3."
                    }
                )},
            {"fail_if_no_peer_cert",
                sc(
                    boolean(),
                    #{
                        default => Df("fail_if_no_peer_cert", false),
                        desc =>
                            "Used together with {verify, verify_peer} by an TLS/DTLS server.\n"
                            "If set to true, the server fails if the client does not have a\n"
                            "certificate to send, that is, sends an empty certificate.\n"
                            "If set to false, it fails only if the client sends an invalid\n"
                            "certificate (an empty certificate is considered valid)."
                    }
                )},
            {"honor_cipher_order",
                sc(
                    boolean(),
                    #{
                        default => Df("honor_cipher_order", true),
                        desc =>
                            "An important security setting, it forces the cipher to be set based\n"
                            " on the server-specified order instead of the client-specified order,\n"
                            " hence enforcing the (usually more properly configured) security\n"
                            " ordering of the server administrator."
                    }
                )},
            {"client_renegotiation",
                sc(
                    boolean(),
                    #{
                        default => Df("client_renegotiation", true),
                        desc =>
                            "In protocols that support client-initiated renegotiation,\n"
                            "the cost of resources of such an operation is higher for the "
                            "server than the client.\n"
                            "This can act as a vector for denial of service attacks.\n"
                            "The SSL application already takes measures to counter-act such attempts,\n"
                            "but client-initiated renegotiation can be strictly disabled by setting "
                            "this option to false.\n"
                            "The default value is true. Note that disabling renegotiation can result in\n"
                            "long-lived connections becoming unusable due to limits on\n"
                            "the number of messages the underlying cipher suite can encipher."
                    }
                )}
            | [
                {"handshake_timeout",
                    sc(
                        duration(),
                        #{
                            default => Df("handshake_timeout", "15s"),
                            desc => "Maximum time duration allowed for the handshake to complete"
                        }
                    )}
             || IsRanchListener
            ]
        ].

%% @doc Make schema for SSL client.
-spec client_ssl_opts_schema(map()) -> hocon_schema:field_schema().
client_ssl_opts_schema(Defaults) ->
    common_ssl_opts_schema(Defaults) ++
        [
            {"server_name_indication",
                sc(
                    hoconsc:union([disable, string()]),
                    #{
                        required => false,
                        desc =>
                            "Specify the host name to be used in TLS Server Name Indication extension.<br>\n"
                            "For instance, when connecting to \"server.example.net\", the genuine server\n"
                            "which accepts the connection and performs TLS handshake may differ from the\n"
                            "host the TLS client initially connects to, e.g. when connecting to an IP address\n"
                            "or when the host has multiple resolvable DNS records <br>\n"
                            "If not specified, it will default to the host name string which is used\n"
                            "to establish the connection, unless it is IP addressed used.<br>\n"
                            "The host name is then also used in the host name verification of the peer\n"
                            "certificate.<br> The special value 'disable' prevents the Server Name\n"
                            "Indication extension from being sent and disables the hostname\n"
                            "verification check."
                    }
                )}
        ].

default_tls_vsns(dtls_all_available) ->
    proplists:get_value(available_dtls, ssl:versions());
default_tls_vsns(tls_all_available) ->
    emqx_tls_lib:default_versions().

-spec ciphers_schema(quic | dtls_all_available | tls_all_available | undefined) ->
    hocon_schema:field_schema().
ciphers_schema(Default) ->
    sc(
        hoconsc:array(string()),
        #{
            default => default_ciphers(Default),
            converter => fun
                (Ciphers) when is_binary(Ciphers) ->
                    binary:split(Ciphers, <<",">>, [global]);
                (Ciphers) when is_list(Ciphers) ->
                    Ciphers
            end,
            validator =>
                case Default =:= quic of
                    %% quic has openssl statically linked
                    true -> undefined;
                    false -> fun validate_ciphers/1
                end,
            desc =>
                "This config holds TLS cipher suite names separated by comma,\n"
                "or as an array of strings. e.g.\n"
                "<code>\"TLS_AES_256_GCM_SHA384,TLS_AES_128_GCM_SHA256\"</code> or\n"
                "<code>[\"TLS_AES_256_GCM_SHA384\",\"TLS_AES_128_GCM_SHA256\"]</code>.\n"
                "<br>\n"
                "Ciphers (and their ordering) define the way in which the\n"
                "client and server encrypts information over the network connection.\n"
                "Selecting a good cipher suite is critical for the\n"
                "application's data security, confidentiality and performance.\n"
                "\n"
                "The names should be in OpenSSL string format (not RFC format).\n"
                "All default values and examples provided by EMQX config\n"
                "documentation are all in OpenSSL format.<br>\n"
                "\n"
                "NOTE: Certain cipher suites are only compatible with\n"
                "specific TLS <code>versions</code> ('tlsv1.1', 'tlsv1.2' or 'tlsv1.3')\n"
                "incompatible cipher suites will be silently dropped.\n"
                "For instance, if only 'tlsv1.3' is given in the <code>versions</code>,\n"
                "configuring cipher suites for other versions will have no effect.\n"
                "<br>\n"
                "\n"
                "NOTE: PSK ciphers are suppressed by 'tlsv1.3' version config<br>\n"
                "If PSK cipher suites are intended, 'tlsv1.3' should be disabled from <code>versions</code>.<br>\n"
                "PSK cipher suites: <code>\"RSA-PSK-AES256-GCM-SHA384,RSA-PSK-AES256-CBC-SHA384,\n"
                "RSA-PSK-AES128-GCM-SHA256,RSA-PSK-AES128-CBC-SHA256,\n"
                "RSA-PSK-AES256-CBC-SHA,RSA-PSK-AES128-CBC-SHA,\n"
                "RSA-PSK-DES-CBC3-SHA,RSA-PSK-RC4-SHA\"</code><br>\n" ++
                case Default of
                    quic -> "NOTE: QUIC listener supports only 'tlsv1.3' ciphers<br>";
                    _ -> ""
                end
        }
    ).

default_ciphers(Which) ->
    lists:map(
        fun erlang:iolist_to_binary/1,
        do_default_ciphers(Which)
    ).

do_default_ciphers(undefined) ->
    do_default_ciphers(tls_all_available);
do_default_ciphers(quic) ->
    [
        "TLS_AES_256_GCM_SHA384",
        "TLS_AES_128_GCM_SHA256",
        "TLS_CHACHA20_POLY1305_SHA256"
    ];
do_default_ciphers(dtls_all_available) ->
    %% as of now, dtls does not support tlsv1.3 ciphers
    emqx_tls_lib:selected_ciphers(['dtlsv1.2', 'dtlsv1']);
do_default_ciphers(tls_all_available) ->
    emqx_tls_lib:default_ciphers().

%% @private return a list of keys in a parent field
-spec keys(string(), hocon:config()) -> [string()].
keys(Parent, Conf) ->
    [binary_to_list(B) || B <- maps:keys(conf_get(Parent, Conf, #{}))].

-spec ceiling(number()) -> integer().
ceiling(X) ->
    T = erlang:trunc(X),
    case (X - T) of
        Neg when Neg < 0 -> T;
        Pos when Pos > 0 -> T + 1;
        _ -> T
    end.

%% types

sc(Type, Meta) -> hoconsc:mk(Type, Meta).

map(Name, Type) -> hoconsc:map(Name, Type).

ref(Field) -> hoconsc:ref(?MODULE, Field).

ref(Module, Field) -> hoconsc:ref(Module, Field).

mk_duration(Desc, OverrideMeta) ->
    DefaultMeta = #{
        desc => Desc ++
            " Time interval is a string that contains a number followed by time unit:<br/>\n"
            "- `ms` for milliseconds,\n"
            "- `s` for seconds,\n"
            "- `m` for minutes,\n"
            "- `h` for hours;\n<br/>"
            "or combination of whereof: `1h5m0s`"
    },
    hoconsc:mk(typerefl:alias("string", duration()), maps:merge(DefaultMeta, OverrideMeta)).

to_duration(Str) ->
    case hocon_postprocess:duration(Str) of
        I when is_integer(I) -> {ok, I};
        _ -> {error, Str}
    end.

to_duration_s(Str) ->
    case hocon_postprocess:duration(Str) of
        I when is_number(I) -> {ok, ceiling(I / 1000)};
        _ -> {error, Str}
    end.

-spec to_duration_ms(Input) -> {ok, integer()} | {error, Input} when
    Input :: string() | binary().
to_duration_ms(Str) ->
    case hocon_postprocess:duration(Str) of
        I when is_number(I) -> {ok, ceiling(I)};
        _ -> {error, Str}
    end.

to_bytesize(Str) ->
    case hocon_postprocess:bytesize(Str) of
        I when is_integer(I) -> {ok, I};
        _ -> {error, Str}
    end.

to_wordsize(Str) ->
    WordSize = erlang:system_info(wordsize),
    case to_bytesize(Str) of
        {ok, Bytes} -> {ok, Bytes div WordSize};
        Error -> Error
    end.

to_percent(Str) ->
    {ok, hocon_postprocess:percent(Str)}.

to_comma_separated_list(Str) ->
    {ok, string:tokens(Str, ", ")}.

to_comma_separated_atoms(Str) ->
    {ok, lists:map(fun to_atom/1, string:tokens(Str, ", "))}.

to_bar_separated_list(Str) ->
    {ok, string:tokens(Str, "| ")}.

to_ip_port(Str) ->
    case string:tokens(Str, ": ") of
        [Ip, Port] ->
            PortVal = list_to_integer(Port),
            case inet:parse_address(Ip) of
                {ok, R} ->
                    {ok, {R, PortVal}};
                _ ->
                    %% check is a rfc1035's hostname
                    case inet_parse:domain(Ip) of
                        true ->
                            {ok, {Ip, PortVal}};
                        _ ->
                            {error, Str}
                    end
            end;
        _ ->
            {error, Str}
    end.

to_erl_cipher_suite(Str) ->
    case ssl:str_to_suite(Str) of
        {error, Reason} -> error({invalid_cipher, Reason});
        Cipher -> Cipher
    end.

to_atom(Atom) when is_atom(Atom) ->
    Atom;
to_atom(Str) when is_list(Str) ->
    list_to_atom(Str);
to_atom(Bin) when is_binary(Bin) ->
    binary_to_atom(Bin, utf8).

validate_heap_size(Siz) ->
    MaxSiz =
        case erlang:system_info(wordsize) of
            % arch_64
            8 ->
                (1 bsl 59) - 1;
            % arch_32
            4 ->
                (1 bsl 27) - 1
        end,
    case Siz > MaxSiz of
        true -> error(io_lib:format("force_shutdown_policy: heap-size ~ts is too large", [Siz]));
        false -> ok
    end.

validate_alarm_actions(Actions) ->
    UnSupported = lists:filter(
        fun(Action) -> Action =/= log andalso Action =/= publish end, Actions
    ),
    case UnSupported of
        [] -> ok;
        Error -> {error, Error}
    end.

parse_user_lookup_fun(StrConf) ->
    [ModStr, FunStr] = string:tokens(str(StrConf), ": "),
    Mod = list_to_atom(ModStr),
    Fun = list_to_atom(FunStr),
    {fun Mod:Fun/3, undefined}.

validate_ciphers(Ciphers) ->
    All = emqx_tls_lib:all_ciphers(),
    case lists:filter(fun(Cipher) -> not lists:member(Cipher, All) end, Ciphers) of
        [] -> ok;
        Bad -> {error, {bad_ciphers, Bad}}
    end.

validate_tls_versions(Versions) ->
    AvailableVersions =
        proplists:get_value(available, ssl:versions()) ++
            proplists:get_value(available_dtls, ssl:versions()),
    case lists:filter(fun(V) -> not lists:member(V, AvailableVersions) end, Versions) of
        [] -> ok;
        Vs -> {error, {unsupported_ssl_versions, Vs}}
    end.

validations() ->
    [
        {check_process_watermark, fun check_process_watermark/1},
        {check_cpu_watermark, fun check_cpu_watermark/1}
    ].

%% validations from emqx_conf_schema, we must filter other *_schema by undefined.
check_process_watermark(Conf) ->
    check_watermark("sysmon.vm.process_low_watermark", "sysmon.vm.process_high_watermark", Conf).

check_cpu_watermark(Conf) ->
    check_watermark("sysmon.os.cpu_low_watermark", "sysmon.os.cpu_high_watermark", Conf).

check_watermark(LowKey, HighKey, Conf) ->
    case hocon_maps:get(LowKey, Conf) of
        undefined ->
            true;
        Low ->
            High = hocon_maps:get(HighKey, Conf),
            case Low < High of
                true -> true;
                false -> {bad_watermark, #{LowKey => Low, HighKey => High}}
            end
    end.

str(A) when is_atom(A) ->
    atom_to_list(A);
str(B) when is_binary(B) ->
    binary_to_list(B);
str(S) when is_list(S) ->
    S.

authentication(Desc) ->
    %% authentication schema is lazy to make it more 'plugable'
    %% the type checks are done in emqx_auth application when it boots.
    %% and in emqx_authentication_config module for runtime changes.
    Default = hoconsc:lazy(hoconsc:union([typerefl:map(), hoconsc:array(typerefl:map())])),
    %% as the type is lazy, the runtime module injection
    %% from EMQX_AUTHENTICATION_SCHEMA_MODULE_PT_KEY
    %% is for now only affecting document generation.
    %% maybe in the future, we can find a more straightforward way to support
    %% * document generation (at compile time)
    %% * type checks before boot (in bin/emqx config generation)
    %% * type checks at runtime (when changing configs via management API)
    #{
        type =>
            case persistent_term:get(?EMQX_AUTHENTICATION_SCHEMA_MODULE_PT_KEY, undefined) of
                undefined -> Default;
                Module -> hoconsc:lazy(Module:root_type())
            end,
        desc => iolist_to_binary([
            Desc,
            "\nAuthentication can be one single authenticator instance or a chain of "
            "authenticators as an array.\n"
            "When authenticating a login (username, client ID, etc.) "
            "the authenticators are checked in the configured order.<br>\n"
        ])
    }.

-spec qos() -> typerefl:type().
qos() ->
    typerefl:alias("qos", typerefl:union([0, 1, 2])).
