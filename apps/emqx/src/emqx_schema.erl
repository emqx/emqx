%%--------------------------------------------------------------------
%% Copyright (c) 2017-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([ validate_heap_size/1
        , parse_user_lookup_fun/1
        ]).

% workaround: prevent being recognized as unused functions
-export([to_duration/1, to_duration_s/1, to_duration_ms/1,
         mk_duration/2, to_bytesize/1, to_wordsize/1,
         to_percent/1, to_comma_separated_list/1,
         to_bar_separated_list/1, to_ip_port/1,
         to_erl_cipher_suite/1,
         to_comma_separated_atoms/1]).

-behaviour(hocon_schema).

-reflect_type([ duration/0, duration_s/0, duration_ms/0,
                bytesize/0, wordsize/0, percent/0, file/0,
                comma_separated_list/0, bar_separated_list/0, ip_port/0,
                cipher/0,
                comma_separated_atoms/0]).

-export([namespace/0, roots/0, roots/1, fields/1]).
-export([conf_get/2, conf_get/3, keys/2, filter/1]).
-export([server_ssl_opts_schema/2, client_ssl_opts_schema/1, ciphers_schema/1, default_ciphers/1]).

namespace() -> undefined.

roots() ->
    %% TODO change config importance to a field metadata
    roots(high) ++ roots(medium) ++ roots(low).

roots(high) ->
    [ {"listeners",
      sc(ref("listeners"),
         #{ desc => "MQTT listeners identified by their protocol type and assigned names"
          })
      }
    , {"zones",
       sc(map("name", ref("zone")),
          #{ desc =>
"""A zone is a set of configs grouped by the zone <code>name</code>.<br>
For flexible configuration mapping, the <code>name</code>
can be set to a listener's <code>zone</code> config.<br>
NOTE: A builtin zone named <code>default</code> is auto created
and can not be deleted."""
           })}
    , {"mqtt",
       sc(ref("mqtt"),
         #{ desc =>
"""Global MQTT configuration.<br>
The configs here work as default values which can be overriden
in <code>zone</code> configs"""
          })}
    , {"authentication",
      sc(hoconsc:lazy(hoconsc:array(map())),
         #{ desc =>
"""Default authentication configs for all MQTT listeners.<br>
For per-listener overrides see <code>authentication</code>
in listener configs"""
          })}
    , {"authorization",
       sc(ref("authorization"),
          #{})}
    ];
roots(medium) ->
    [ {"broker",
       sc(ref("broker"),
         #{})}
    , {"rate_limit",
       sc(ref("rate_limit"),
          #{})}
    , {"force_shutdown",
       sc(ref("force_shutdown"),
          #{})}
    ];
roots(low) ->
    [ {"force_gc",
       sc(ref("force_gc"),
          #{})}
   , {"conn_congestion",
       sc(ref("conn_congestion"),
          #{})}
   , {"quota",
       sc(ref("quota"),
          #{})}
   , {"plugins", %% TODO: move to emqx_machine_schema
       sc(ref("plugins"),
          #{})}
   , {"stats",
       sc(ref("stats"),
          #{})}
   , {"sysmon",
       sc(ref("sysmon"),
          #{})}
   , {"alarm",
       sc(ref("alarm"),
          #{})}
   , {"flapping_detect",
       sc(ref("flapping_detect"),
          #{})}
    ].

fields("stats") ->
    [ {"enable",
       sc(boolean(),
          #{ default => true
           })}
    ];

fields("authorization") ->
    [ {"no_match",
       sc(hoconsc:enum([allow, deny]),
          #{ default => allow
           })}
    , {"deny_action",
       sc(hoconsc:enum([ignore, disconnect]),
          #{ default => ignore
           })}
    , {"cache",
       sc(ref(?MODULE, "cache"),
          #{
           })}
    ];

fields("cache") ->
    [ {"enable",
       sc(boolean(),
          #{ default => true
           })
      }
    , {"max_size",
       sc(range(1, 1048576),
          #{ default => 32
           })
      }
    , {"ttl",
       sc(duration(),
          #{ default => "1m"
           })
      }
    ];

fields("mqtt") ->
    [ {"idle_timeout",
       sc(hoconsc:union([infinity, duration()]),
          #{ default => "15s"
           })}
    , {"max_packet_size",
       sc(bytesize(),
          #{ default => "1MB"
           })}
    , {"max_clientid_len",
       sc(range(23, 65535),
          #{ default => 65535
           })}
    , {"max_topic_levels",
       sc(range(1, 65535),
          #{ default => 65535
           })}
    , {"max_qos_allowed",
       sc(range(0, 2),
          #{ default => 2
           })}
    , {"max_topic_alias",
       sc(range(0, 65535),
          #{ default => 65535
          })}
    , {"retain_available",
       sc(boolean(),
          #{ default => true
           })}
    , {"wildcard_subscription",
       sc(boolean(),
          #{ default => true
           })}
    , {"shared_subscription",
       sc(boolean(),
          #{ default => true
           })}
    , {"ignore_loop_deliver",
       sc(boolean(),
          #{ default => false
           })}
    , {"strict_mode",
       sc(boolean(),
          #{default => false
           })
      }
    , {"response_information",
       sc(string(),
          #{default => ""
           })
      }
    , {"server_keepalive",
       sc(hoconsc:union([integer(), disabled]),
          #{ default => disabled
           })
      }
    , {"keepalive_backoff",
       sc(float(),
          #{default => 0.75
           })
      }
    , {"max_subscriptions",
       sc(hoconsc:union([range(1, inf), infinity]),
          #{ default => infinity
           })
      }
    , {"upgrade_qos",
       sc(boolean(),
          #{ default => false
           })
      }
    , {"max_inflight",
       sc(range(1, 65535),
          #{ default => 32
           })
      }
    , {"retry_interval",
       sc(duration(),
          #{default => "30s"
           })
      }
    , {"max_awaiting_rel",
       sc(hoconsc:union([integer(), infinity]),
          #{ default => 100
           })
      }
    , {"await_rel_timeout",
       sc(duration(),
          #{ default => "300s"
           })
      }
    , {"session_expiry_interval",
       sc(duration(),
          #{ default => "2h"
           })
      }
    , {"max_mqueue_len",
       sc(hoconsc:union([range(0, inf), infinity]),
          #{ default => 1000
           })
      }
    , {"mqueue_priorities",
       sc(hoconsc:union([map(), disabled]),
          #{ default => disabled
           })
      }
    , {"mqueue_default_priority",
       sc(hoconsc:enum([highest, lowest]),
          #{ default => lowest
           })
      }
    , {"mqueue_store_qos0",
       sc(boolean(),
          #{ default => true
           })
      }
    , {"use_username_as_clientid",
       sc(boolean(),
          #{ default => false
           })
      }
    , {"peer_cert_as_username",
       sc(hoconsc:enum([disabled, cn, dn, crt, pem, md5]),
          #{ default => disabled
           })}
    , {"peer_cert_as_clientid",
       sc(hoconsc:enum([disabled, cn, dn, crt, pem, md5]),
          #{ default => disabled
           })}
    ];

fields("zone") ->
    Fields = ["mqtt", "stats", "flapping_detect", "force_shutdown",
              "conn_congestion", "rate_limit", "quota", "force_gc"],
    [{F, ref(emqx_zone_schema, F)} || F <- Fields];

fields("rate_limit") ->
    [ {"max_conn_rate",
       sc(hoconsc:union([infinity, integer()]),
          #{ default => 1000
           })
      }
    , {"conn_messages_in",
       sc(hoconsc:union([infinity, comma_separated_list()]),
          #{ default => infinity
           })
       }
    , {"conn_bytes_in",
       sc(hoconsc:union([infinity, comma_separated_list()]),
          #{ default => infinity
           })
       }
    ];

fields("quota") ->
    [ {"conn_messages_routing",
       sc(hoconsc:union([infinity, comma_separated_list()]),
          #{ default => infinity
           })
       }
    , {"overall_messages_routing",
       sc(hoconsc:union([infinity, comma_separated_list()]),
          #{ default => infinity
           })
      }
    ];

fields("flapping_detect") ->
    [ {"enable",
       sc(boolean(),
          #{ default => false
           })}
    , {"max_count",
       sc(integer(),
          #{ default => 15
           })}
    , {"window_time",
       sc(duration(),
          #{ default => "1m"
           })}
    , {"ban_time",
       sc(duration(),
          #{ default => "5m"
           })}
    ];

fields("force_shutdown") ->
    [ {"enable",
       sc(boolean(),
          #{ default => true})}
    , {"max_message_queue_len",
       sc(range(0, inf),
          #{ default => 1000
           })}
    , {"max_heap_size",
       sc(wordsize(),
          #{ default => "32MB",
             validator => fun ?MODULE:validate_heap_size/1
           })}
    ];

fields("conn_congestion") ->
    [ {"enable_alarm",
       sc(boolean(),
          #{ default => false
           })}
    , {"min_alarm_sustain_duration",
       sc(duration(),
          #{ default => "1m"
           })}
    ];

fields("force_gc") ->
    [ {"enable",
       sc(boolean(),
          #{ default => true
           })}
    , {"count",
       sc(range(0, inf),
          #{ default => 16000
           })}
    , {"bytes",
       sc(bytesize(),
          #{ default => "16MB"
           })}
    ];

fields("listeners") ->
    [ {"tcp",
       sc(map(name, ref("mqtt_tcp_listener")),
          #{ desc => "TCP listeners"
           , nullable => {true, recursive}
           })
      }
    , {"ssl",
       sc(map(name, ref("mqtt_ssl_listener")),
          #{ desc => "SSL listeners"
           , nullable => {true, recursive}
           })
      }
    , {"ws",
       sc(map(name, ref("mqtt_ws_listener")),
          #{ desc => "HTTP websocket listeners"
           , nullable => {true, recursive}
           })
      }
    , {"wss",
       sc(map(name, ref("mqtt_wss_listener")),
          #{ desc => "HTTPS websocket listeners"
           , nullable => {true, recursive}
           })
      }
    , {"quic",
       sc(map(name, ref("mqtt_quic_listener")),
          #{ desc => "QUIC listeners"
           , nullable => {true, recursive}
           })
      }
    ];

fields("mqtt_tcp_listener") ->
    [ {"tcp",
       sc(ref("tcp_opts"),
          #{ desc => "TCP listener options"
           })
      }
    ] ++ mqtt_listener();

fields("mqtt_ssl_listener") ->
    [ {"tcp",
       sc(ref("tcp_opts"),
          #{})
      }
    , {"ssl",
       sc(ref("listener_ssl_opts"),
          #{})
      }
    ] ++ mqtt_listener();

fields("mqtt_ws_listener") ->
    [ {"tcp",
       sc(ref("tcp_opts"),
          #{})
      }
    , {"websocket",
       sc(ref("ws_opts"),
          #{})
      }
    ] ++ mqtt_listener();

fields("mqtt_wss_listener") ->
    [ {"tcp",
       sc(ref("tcp_opts"),
          #{})
      }
    , {"ssl",
       sc(ref("listener_wss_opts"),
          #{})
      }
    , {"websocket",
       sc(ref("ws_opts"),
          #{})
      }
    ] ++ mqtt_listener();

fields("mqtt_quic_listener") ->
    [ {"enabled",
       sc(boolean(),
          #{ default => true
           })
      }
      %% TODO: ensure cacertfile is configurable
    , {"certfile",
       sc(string(),
          #{})
      }
    , {"keyfile",
       sc(string(),
          #{})
      }
    , {"ciphers", ciphers_schema(quic)}
    , {"idle_timeout",
       sc(duration(),
          #{ default => "15s"
           })
      }
    ] ++ base_listener();

fields("ws_opts") ->
    [ {"mqtt_path",
       sc(string(),
          #{ default => "/mqtt"
           })
      }
    , {"mqtt_piggyback",
       sc(hoconsc:enum([single, multiple]),
          #{ default => multiple
           })
      }
    , {"compress",
       sc(boolean(),
          #{ default => false
           })
      }
    , {"idle_timeout",
       sc(duration(),
          #{ default => "15s"
           })
      }
    , {"max_frame_size",
       sc(hoconsc:union([infinity, integer()]),
          #{ default => infinity
           })
      }
    , {"fail_if_no_subprotocol",
       sc(boolean(),
          #{ default => true
           })
      }
    , {"supported_subprotocols",
       sc(comma_separated_list(),
          #{ default => "mqtt, mqtt-v3, mqtt-v3.1.1, mqtt-v5"
           })
      }
    , {"check_origin_enable",
       sc(boolean(),
          #{ default => false
           })
      }
    , {"allow_origin_absence",
       sc(boolean(),
          #{ default => true
           })
      }
    , {"check_origins",
       sc(hoconsc:array(binary()),
          #{ default => []
           })
      }
    , {"proxy_address_header",
       sc(string(),
          #{ default => "x-forwarded-for"
           })
      }
    , {"proxy_port_header",
       sc(string(),
          #{ default => "x-forwarded-port"
           })
      }
    , {"deflate_opts",
       sc(ref("deflate_opts"),
          #{})
      }
    ];

fields("tcp_opts") ->
    [ {"active_n",
       sc(integer(),
          #{ default => 100
           })
      }
    , {"backlog",
       sc(integer(),
          #{ default => 1024
           })
      }
    , {"send_timeout",
       sc(duration(),
          #{ default => "15s"
           })
      }
    , {"send_timeout_close",
       sc(boolean(),
          #{ default => true
           })
      }
    , {"recbuf",
       sc(bytesize(),
          #{})
      }
    , {"sndbuf",
       sc(bytesize(),
          #{})
      }
    , {"buffer",
       sc(bytesize(),
          #{})
      }
    , {"high_watermark",
       sc(bytesize(),
          #{ default => "1MB"})
      }
    , {"nodelay",
       sc(boolean(),
          #{ default => false})
      }
    , {"reuseaddr",
       sc(boolean(),
          #{ default => true
           })
      }
    ];

fields("listener_ssl_opts") ->
    server_ssl_opts_schema(
      #{ depth => 10
       , reuse_sessions => true
       , versions => tls_all_available
       , ciphers => tls_all_available
       }, false);

fields("listener_wss_opts") ->
    server_ssl_opts_schema(
      #{ depth => 10
       , reuse_sessions => true
       , versions => tls_all_available
       , ciphers => tls_all_available
       }, true);
fields(ssl_client_opts) ->
    client_ssl_opts_schema(#{});

fields("deflate_opts") ->
    [ {"level",
       sc(hoconsc:enum([none, default, best_compression, best_speed]),
          #{})
      }
    , {"mem_level",
       sc(range(1, 9),
          #{ default => 8
           })
      }
    , {"strategy",
       sc(hoconsc:enum([default, filtered, huffman_only, rle]),
          #{})
      }
    , {"server_context_takeover",
       sc(hoconsc:enum([takeover, no_takeover]),
          #{})
      }
    , {"client_context_takeover",
       sc(hoconsc:enum([takeover, no_takeover]),
          #{})
      }
    , {"server_max_window_bits",
       sc(range(8, 15),
          #{ default => 15
           })
      }
    , {"client_max_window_bits",
       sc(range(8, 15),
          #{ default => 15
           })
      }
    ];

fields("plugins") ->
    [ {"expand_plugins_dir",
       sc(string(),
          #{})
      }
    ];

fields("broker") ->
    [ {"sys_msg_interval",
       sc(hoconsc:union([disabled, duration()]),
          #{ default => "1m"
           })
      }
    , {"sys_heartbeat_interval",
       sc(hoconsc:union([disabled, duration()]),
          #{ default => "30s"
           })
      }
    , {"enable_session_registry",
       sc(boolean(),
          #{ default => true
           })
      }
    , {"session_locking_strategy",
       sc(hoconsc:enum([local, leader, quorum, all]),
          #{ default => quorum
           })
      }
    , {"shared_subscription_strategy",
       sc(hoconsc:enum([random, round_robin]),
          #{ default => round_robin
           })
      }
    , {"shared_dispatch_ack_enabled",
       sc(boolean(),
          #{ default => false
           })
      }
    , {"route_batch_clean",
       sc(boolean(),
          #{ default => true
           })}
    , {"perf",
       sc(ref("broker_perf"),
          #{ desc => "Broker performance tuning pamaters"
           })
      }
    ];

fields("broker_perf") ->
    [ {"route_lock_type",
       sc(hoconsc:enum([key, tab, global]),
          #{ default => key
           })}
    , {"trie_compaction",
       sc(boolean(),
          #{ default => true
           })}
    ];

fields("sysmon") ->
    [ {"vm",
       sc(ref("sysmon_vm"),
          #{})
      }
    , {"os",
       sc(ref("sysmon_os"),
          #{})
      }
    ];

fields("sysmon_vm") ->
    [ {"process_check_interval",
       sc(duration(),
          #{ default => "30s"
           })
      }
    , {"process_high_watermark",
       sc(percent(),
          #{ default => "80%"
           })
      }
    , {"process_low_watermark",
       sc(percent(),
          #{ default => "60%"
           })
      }
    , {"long_gc",
       sc(hoconsc:union([disabled, duration()]),
          #{})
      }
    , {"long_schedule",
       sc(hoconsc:union([disabled, duration()]),
          #{ default => "240ms"
           })
      }
    , {"large_heap",
       sc(hoconsc:union([disabled, bytesize()]),
          #{default => "32MB"})
      }
    , {"busy_dist_port",
       sc(boolean(),
          #{ default => true
           })
      }
    , {"busy_port",
       sc(boolean(),
          #{ default => true
           })}
    ];

fields("sysmon_os") ->
    [ {"cpu_check_interval",
       sc(duration(),
          #{ default => "60s"})
      }
    , {"cpu_high_watermark",
       sc(percent(),
          #{ default => "80%"
           })
      }
    , {"cpu_low_watermark",
       sc(percent(),
          #{ default => "60%"
           })
      }
    , {"mem_check_interval",
       sc(hoconsc:union([disabled, duration()]),
          #{ default => "60s"
           })}
    , {"sysmem_high_watermark",
       sc(percent(),
          #{ default => "70%"
           })
      }
    , {"procmem_high_watermark",
       sc(percent(),
          #{ default => "5%"
           })
      }
    ];

fields("alarm") ->
    [ {"actions",
       sc(hoconsc:array(atom()),
          #{ default => [log, publish]
           })
      }
    , {"size_limit",
       sc(integer(),
          #{ default => 1000
           })
      }
    , {"validity_period",
       sc(duration(),
          #{ default => "24h"
           })
      }
    ].

mqtt_listener() ->
    base_listener() ++
    [ {"access_rules",
       sc(hoconsc:array(string()),
          #{})
      }
    , {"proxy_protocol",
       sc(boolean(),
          #{ default => false
           })
      }
    , {"proxy_protocol_timeout",
       sc(duration(),
          #{})
      }
    , {"authentication",
       sc(hoconsc:lazy(hoconsc:array(map())),
          #{})
      }
    ].

base_listener() ->
    [ {"bind",
       sc(hoconsc:union([ip_port(), integer()]),
          #{ nullable => false
           })}
    , {"acceptors",
       sc(integer(),
          #{ default => 16
           })}
    , {"max_connections",
       sc(hoconsc:union([infinity, integer()]),
          #{ default => infinity
           })}
    , {"mountpoint",
       sc(binary(),
          #{ default => <<>>
           })}
    , {"zone",
       sc(atom(),
          #{ default => 'default'
           })}
    ].

%% utils
-spec(conf_get(string() | [string()], hocon:config()) -> term()).
conf_get(Key, Conf) ->
    V = hocon_schema:get_value(Key, Conf),
    case is_binary(V) of
        true ->
            binary_to_list(V);
        false ->
            V
    end.

conf_get(Key, Conf, Default) ->
    V = hocon_schema:get_value(Key, Conf, Default),
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
    D = fun (Field) -> maps:get(to_atom(Field), Defaults, undefined) end,
    Df = fun (Field, Default) -> maps:get(to_atom(Field), Defaults, Default) end,
    [ {"enable",
       sc(boolean(),
          #{ default => Df("enable", false)
           })
      }
    , {"cacertfile",
       sc(string(),
          #{ default => D("cacertfile")
           , nullable => true
           , desc =>
"""Trusted PEM format CA certificates bundle file.<br>
The certificates in this file are used to verify the TLS peer's certificates.
Append new certificates to the file if new CAs are to be trusted.
There is no need to restart EMQ X to have the updated file loaded, because
the system regularly checks if file has been updated (and reload).<br>
NOTE: invalidating (deleting) a certificate from the file will not affect
already established connections.
"""
           })
      }
    , {"certfile",
       sc(string(),
          #{ default => D("certfile")
           , nullable => true
           , desc =>
"""PEM format certificates chain file.<br>
The certificates in this file should be in reversed order of the certificate
issue chain. That is, the host's certificate should be placed in the beginning
of the file, followed by the immediate issuer certificate and so on.
Although the root CA certificate is optional, it should placed at the end of
the file if it is to be added.
"""
           })
      }
    , {"keyfile",
       sc(string(),
          #{ default => D("keyfile")
           , nullable => true
           , desc =>
"""PEM format private key file.<br>
"""
           })
      }
    , {"verify",
       sc(hoconsc:enum([verify_peer, verify_none]),
          #{ default => Df("verify", verify_none)
           })
      }
    , {"reuse_sessions",
       sc(boolean(),
          #{ default => Df("reuse_sessions", true)
           })
      }
    , {"depth",
       sc(integer(),
          #{default => Df("depth", 10)
           })
      }
    , {"password",
       sc(string(),
          #{ sensitive => true
           , nullable => true
           , desc =>
"""String containing the user's password. Only used if the private
keyfile is password-protected."""
           })
      }
    , {"versions",
       sc(hoconsc:array(typerefl:atom()),
          #{ default => default_tls_vsns(maps:get(versions, Defaults, tls_all_available))
           , desc =>
"""All TLS/DTLS versions to be supported.<br>
NOTE: PSK ciphers are suppresed by 'tlsv1.3' version config<br>
In case PSK cipher suites are intended, make sure to configured
<code>['tlsv1.2', 'tlsv1.1']</code> here.
"""
           , validator => fun validate_tls_versions/1
           })
      }
    , {"ciphers", ciphers_schema(D("ciphers"))}
    , {user_lookup_fun,
       sc(typerefl:alias("string", any()),
          #{ default => "emqx_psk:lookup"
           , converter => fun ?MODULE:parse_user_lookup_fun/1
           })
      }
    , {"secure_renegotiate",
       sc(boolean(),
          #{ default => Df("secure_renegotiate", true)
           , desc => """
SSL parameter renegotiation is a feature that allows a client and a server
to renegotiate the parameters of the SSL connection on the fly.
RFC 5746 defines a more secure way of doing this. By enabling secure renegotiation,
you drop support for the insecure renegotiation, prone to MitM attacks.
"""
           })
      }
    ].

%% @doc Make schema for SSL listener options.
%% When it's for ranch listener, an extra field `handshake_timeout' is added.
-spec server_ssl_opts_schema(map(), boolean()) -> hocon_schema:field_schema().
server_ssl_opts_schema(Defaults, IsRanchListener) ->
    D = fun (Field) -> maps:get(to_atom(Field), Defaults, undefined) end,
    Df = fun (Field, Default) -> maps:get(to_atom(Field), Defaults, Default) end,
    common_ssl_opts_schema(Defaults) ++
    [ {"dhfile",
       sc(string(),
          #{ default => D("dhfile")
           , nullable => true
           , desc =>
"""Path to a file containing PEM-encoded Diffie Hellman parameters
to be used by the server if a cipher suite using Diffie Hellman
key exchange is negotiated. If not specified, default parameters
are used.<br>
NOTE: The dhfile option is not supported by TLS 1.3."""
           })
      }
    , {"fail_if_no_peer_cert",
       sc(boolean(),
          #{ default => Df("fail_if_no_peer_cert", false)
           , desc =>
"""
Used together with {verify, verify_peer} by an TLS/DTLS server.
If set to true, the server fails if the client does not have a
certificate to send, that is, sends an empty certificate.
If set to false, it fails only if the client sends an invalid
certificate (an empty certificate is considered valid).
"""
           })
      }
    , {"honor_cipher_order",
       sc(boolean(),
          #{ default => Df("honor_cipher_order", true)
           })
      }
    , {"client_renegotiation",
       sc(boolean(),
          #{ default => Df("client_renegotiation", true)
           , desc => """
In protocols that support client-initiated renegotiation,
the cost of resources of such an operation is higher for the server than the client.
This can act as a vector for denial of service attacks.
The SSL application already takes measures to counter-act such attempts,
but client-initiated renegotiation can be strictly disabled by setting this option to false.
The default value is true. Note that disabling renegotiation can result in
long-lived connections becoming unusable due to limits on
the number of messages the underlying cipher suite can encipher.
"""
           })
      }
    | [ {"handshake_timeout",
         sc(duration(),
            #{ default => Df("handshake_timeout", "15s")
             , desc => "Maximum time duration allowed for the handshake to complete"
             })}
       || IsRanchListener]
    ].

%% @doc Make schema for SSL client.
-spec client_ssl_opts_schema(map()) -> hocon_schema:field_schema().
client_ssl_opts_schema(Defaults) ->
    common_ssl_opts_schema(Defaults) ++
    [ { "server_name_indication",
        sc(hoconsc:union([disable, string()]),
           #{ default => disable
            , desc =>
"""Specify the host name to be used in TLS Server Name Indication extension.<br>
For instance, when connecting to \"server.example.net\", the genuine server
which accedpts the connection and performs TLS handshake may differ from the
host the TLS client initially connects to, e.g. when connecting to an IP address
or when the host has multiple resolvable DNS records <br>
If not specified, it will default to the host name string which is used
to establish the connection, unless it is IP addressed used.<br>
The host name is then also used in the host name verification of the peer
certificate.<br> The special value 'disable' prevents the Server Name
Indication extension from being sent and disables the hostname
verification check."""
            })}
    ].


default_tls_vsns(dtls_all_available) ->
    proplists:get_value(available_dtls, ssl:versions());
default_tls_vsns(tls_all_available) ->
    proplists:get_value(available, ssl:versions()).

-spec ciphers_schema(quic | dtls_all_available | tls_all_available | undefined) -> hocon_schema:field_schema().
ciphers_schema(Default) ->
    sc(hoconsc:array(string()),
       #{ default => default_ciphers(Default)
        , converter => fun(Ciphers) when is_binary(Ciphers) ->
                               binary:split(Ciphers, <<",">>, [global]);
                          (Ciphers) when is_list(Ciphers) ->
                               Ciphers
                       end
        , validator => fun validate_ciphers/1
        , desc =>
"""TLS cipher suite names separated by comma, or as an array of strings
<code>\"TLS_AES_256_GCM_SHA384,TLS_AES_128_GCM_SHA256\"</code> or
<code>[\"TLS_AES_256_GCM_SHA384\",\"TLS_AES_128_GCM_SHA256\"]</code].
<br>
Ciphers (and their ordering) define the way in which the
client and server encrypts information over the wire.
Selecting a good cipher suite is critical for the
application's data security, confidentiality and performance.
The names should be in OpenSSL sting format (not RFC format).
Default values and examples proveded by EMQ X config
documentation are all in OpenSSL format.<br>

NOTE: Certain cipher suites are only compatible with
specific TLS <code>versions</code> ('tlsv1.1', 'tlsv1.2' or 'tlsv1.3')
incompatible cipher suites will be silently dropped.
For instance, if only 'tlsv1.3' is given in the <code>versions</code>,
configuring cipher suites for other versions will have no effect.
<br>

NOTE: PSK ciphers are suppresed by 'tlsv1.3' version config<br>
If PSK cipher suites are intended, 'tlsv1.3' should be disabled from <code>versions</code>.<br>
PSK cipher suites: <code>\"RSA-PSK-AES256-GCM-SHA384,RSA-PSK-AES256-CBC-SHA384,
RSA-PSK-AES128-GCM-SHA256,RSA-PSK-AES128-CBC-SHA256,
RSA-PSK-AES256-CBC-SHA,RSA-PSK-AES128-CBC-SHA,
RSA-PSK-DES-CBC3-SHA,RSA-PSK-RC4-SHA\"</code><br>
""" ++ case Default of
           quic -> "NOTE: QUIC listener supports only 'tlsv1.3' ciphers<br>";
           _ -> ""
       end}).

default_ciphers(undefined) ->
    default_ciphers(tls_all_available);
default_ciphers(quic) -> [
    "TLS_AES_256_GCM_SHA384",
    "TLS_AES_128_GCM_SHA256",
    "TLS_CHACHA20_POLY1305_SHA256"
    ];
default_ciphers(tls_all_available) ->
    default_ciphers('tlsv1.3') ++
    default_ciphers('tlsv1.2') ++
    default_ciphers(psk);
default_ciphers(dtls_all_available) ->
    %% as of now, dtls does not support tlsv1.3 ciphers
    default_ciphers('tlsv1.2') ++ default_ciphers('psk');
default_ciphers('tlsv1.3') ->
    case is_tlsv13_available() of
        true -> ssl:cipher_suites(exclusive, 'tlsv1.3', openssl);
        false -> []
    end ++ default_ciphers('tlsv1.2');
default_ciphers('tlsv1.2') -> [
    "ECDHE-ECDSA-AES256-GCM-SHA384",
    "ECDHE-RSA-AES256-GCM-SHA384", "ECDHE-ECDSA-AES256-SHA384", "ECDHE-RSA-AES256-SHA384",
    "ECDHE-ECDSA-DES-CBC3-SHA", "ECDH-ECDSA-AES256-GCM-SHA384", "ECDH-RSA-AES256-GCM-SHA384",
    "ECDH-ECDSA-AES256-SHA384", "ECDH-RSA-AES256-SHA384", "DHE-DSS-AES256-GCM-SHA384",
    "DHE-DSS-AES256-SHA256", "AES256-GCM-SHA384", "AES256-SHA256",
    "ECDHE-ECDSA-AES128-GCM-SHA256", "ECDHE-RSA-AES128-GCM-SHA256",
    "ECDHE-ECDSA-AES128-SHA256", "ECDHE-RSA-AES128-SHA256", "ECDH-ECDSA-AES128-GCM-SHA256",
    "ECDH-RSA-AES128-GCM-SHA256", "ECDH-ECDSA-AES128-SHA256", "ECDH-RSA-AES128-SHA256",
    "DHE-DSS-AES128-GCM-SHA256", "DHE-DSS-AES128-SHA256", "AES128-GCM-SHA256", "AES128-SHA256",
    "ECDHE-ECDSA-AES256-SHA", "ECDHE-RSA-AES256-SHA", "DHE-DSS-AES256-SHA",
    "ECDH-ECDSA-AES256-SHA", "ECDH-RSA-AES256-SHA", "AES256-SHA", "ECDHE-ECDSA-AES128-SHA",
    "ECDHE-RSA-AES128-SHA", "DHE-DSS-AES128-SHA", "ECDH-ECDSA-AES128-SHA",
    "ECDH-RSA-AES128-SHA", "AES128-SHA"
    ];
default_ciphers(psk) ->
    [ "RSA-PSK-AES256-GCM-SHA384","RSA-PSK-AES256-CBC-SHA384",
      "RSA-PSK-AES128-GCM-SHA256","RSA-PSK-AES128-CBC-SHA256",
      "RSA-PSK-AES256-CBC-SHA","RSA-PSK-AES128-CBC-SHA",
      "RSA-PSK-DES-CBC3-SHA","RSA-PSK-RC4-SHA"
    ].

%% @private return a list of keys in a parent field
-spec(keys(string(), hocon:config()) -> [string()]).
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
    DefaultMeta = #{desc => Desc ++ " Time span. A text string with number followed by time units:
                    `ms` for milli-seconds,
                    `s` for seconds,
                    `m` for minutes,
                    `h` for hours;
                    or combined representation like `1h5m0s`"},
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

-spec to_duration_ms(Input) -> {ok, integer()} | {error, Input}
              when Input :: string() | binary().
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
    case string:tokens(Str, ":") of
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
        _ -> {error, Str}
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
    MaxSiz = case erlang:system_info(wordsize) of
                 8 -> % arch_64
                     (1 bsl 59) - 1;
                 4 -> % arch_32
                     (1 bsl 27) - 1
             end,
    case Siz > MaxSiz of
        true -> error(io_lib:format("force_shutdown_policy: heap-size ~s is too large", [Siz]));
        false -> ok
    end.
parse_user_lookup_fun(StrConf) ->
    [ModStr, FunStr] = string:tokens(StrConf, ":"),
    Mod = list_to_atom(ModStr),
    Fun = list_to_atom(FunStr),
    {fun Mod:Fun/3, <<>>}.

validate_ciphers(Ciphers) ->
    All = case is_tlsv13_available() of
              true -> ssl:cipher_suites(all, 'tlsv1.3', openssl);
              false -> []
          end ++ ssl:cipher_suites(all, 'tlsv1.2', openssl),
    case lists:filter(fun(Cipher) -> not lists:member(Cipher, All) end, Ciphers) of
        [] -> ok;
        Bad -> {error, {bad_ciphers, Bad}}
    end.

validate_tls_versions(Versions) ->
    AvailableVersions = proplists:get_value(available, ssl:versions()) ++
                        proplists:get_value(available_dtls, ssl:versions()),
    case lists:filter(fun(V) -> not lists:member(V, AvailableVersions) end, Versions) of
        [] -> ok;
        Vs -> {error, {unsupported_ssl_versions, Vs}}
    end.

is_tlsv13_available() ->
    lists:member('tlsv1.3', proplists:get_value(available, ssl:versions())).
