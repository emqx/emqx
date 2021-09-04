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
         to_bytesize/1, to_wordsize/1,
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

-export([namespace/0, roots/0, fields/1]).
-export([conf_get/2, conf_get/3, keys/2, filter/1]).
-export([ssl/1]).

namespace() -> undefined.

roots() ->
    [{"zones",
      sc(map("name", ref("zone")),
         #{ desc => "A zone is a set of configs grouped by the zone `$name`. <br>"
                    "The `$name` can be set to a listner's `zone` config for "
                    "flexible configuration mapping. <br>"
                    "NOTE: A builtin zone named `default` is auto created "
                    "and can not be deleted."
          })},
     "mqtt",
     "flapping_detect",
     "force_shutdown",
     "force_gc",
     "conn_congestion",
     "rate_limit",
     "quota",
     {"listeners",
      sc(ref("listeners"),
         #{ desc => "MQTT listeners identified by their protocol type and assigned names. "
                    "The listeners enabled by default are named with 'default'"})
     },
     "broker",
     "plugins",
     "stats",
     "sysmon",
     "alarm",
     {"authentication",
      sc(hoconsc:lazy(hoconsc:array(map())),
         #{ desc => "Default authentication configs for all MQTT listeners.<br>"
                    "For per-listener overrides see <code>authentication</code> "
                    "in listener configs"
          })},
     "authorization",
     "flapping_detect"
    ].

fields("stats") ->
    [ {"enable",
       sc(boolean(),
          #{ default => true
           })}
    ];

fields("authorization") ->
    [ {"no_match",
       sc(union(allow, deny),
          #{ default => allow
           })}
    , {"deny_action",
       sc(union(ignore, disconnect),
          #{ default => ignore
           })}
    , {"cache",
       sc(ref(?MODULE, "cache"),
          #{
           })
       }
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
       sc(union(highest, lowest),
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
       sc(hoconsc:union([disabled, cn, dn, crt, pem, md5]),
          #{ default => disabled
           })}
    , {"peer_cert_as_clientid",
       sc(hoconsc:union([disabled, cn, dn, crt, pem, md5]),
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
       sc(ref("listener_ssl_opts"),
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
    , {"certfile",
       sc(string(),
          #{})
      }
    , {"keyfile",
       sc(string(),
          #{})
      }
    , {"ciphers",
       sc(comma_separated_list(),
          #{ default => "TLS_AES_256_GCM_SHA384,TLS_AES_128_GCM_SHA256,"
                        "TLS_CHACHA20_POLY1305_SHA256"
           })}
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
       sc(hoconsc:union([single, multiple]),
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
    ssl(#{handshake_timeout => "15s"
        , depth => 10
        , reuse_sessions => true
        , versions => default_tls_vsns()
        , ciphers => default_ciphers()
        });

fields("deflate_opts") ->
    [ {"level",
       sc(hoconsc:union([none, default, best_compression, best_speed]),
          #{})
      }
    , {"mem_level",
       sc(range(1, 9),
          #{ default => 8
           })
      }
    , {"strategy",
       sc(hoconsc:union([default, filtered, huffman_only, rle]),
          #{})
      }
    , {"server_context_takeover",
       sc(hoconsc:union([takeover, no_takeover]),
          #{})
      }
    , {"client_context_takeover",
       sc(hoconsc:union([takeover, no_takeover]),
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
       sc(hoconsc:union([local, leader, quorum, all]),
          #{ default => quorum
           })
      }
    , {"shared_subscription_strategy",
       sc(hoconsc:union([random, round_robin]),
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
       sc(hoconsc:union([key, tab, global]),
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

ssl(Defaults) ->
    D = fun (Field) -> maps:get(to_atom(Field), Defaults, undefined) end,
    [ {"enable",
       sc(boolean(),
          #{ default => D("enable")
           })
      }
    , {"cacertfile",
       sc(string(),
          #{ default => D("cacertfile")
           })
      }
    , {"certfile",
       sc(string(),
          #{ default => D("certfile")
           })
      }
    , {"keyfile",
       sc(string(),
          #{ default => D("keyfile")
           })
      }
    , {"verify",
       sc(hoconsc:union([verify_peer, verify_none]),
          #{ default => D("verify")
           })
      }
    , {"fail_if_no_peer_cert",
       sc(boolean(),
          #{ default => D("fail_if_no_peer_cert")
           })
      }
    , {"secure_renegotiate",
       sc(boolean(),
          #{ default => D("secure_renegotiate")
           })
      }
    , {"reuse_sessions",
       sc(boolean(),
          #{ default => D("reuse_sessions")
           })
      }
    , {"honor_cipher_order",
       sc(boolean(),
          #{ default => D("honor_cipher_order")
           })
      }
    , {"handshake_timeout",
       sc(duration(),
          #{ default => D("handshake_timeout")
           })
      }
    , {"depth",
       sc(integer(),
          #{default => D("depth")
           })
      }
    , {"password",
       sc(string(),
          #{ default => D("key_password")
           , sensitive => true
           })
      }
    , {"dhfile",
       sc(string(),
          #{ default => D("dhfile")
           })
      }
    , {"server_name_indication",
       sc(hoconsc:union([disable, string()]),
          #{ default => D("server_name_indication")
           })
      }
    , {"versions",
       sc(typerefl:alias("string", list(atom())),
          #{ default => maps:get(versions, Defaults, default_tls_vsns())
           , converter => fun (Vsns) -> [tls_vsn(iolist_to_binary(V)) || V <- Vsns] end
           })
      }
    , {"ciphers",
       sc(hoconsc:array(string()),
          #{ default => D("ciphers")
           })
      }
    , {"user_lookup_fun",
       sc(typerefl:alias("string", any()),
          #{ default => "emqx_psk:lookup"
           , converter => fun ?MODULE:parse_user_lookup_fun/1
           })
      }
    ].

%% on erl23.2.7.2-emqx-2, sufficient_crypto_support('tlsv1.3') -> false
default_tls_vsns() -> [<<"tlsv1.2">>, <<"tlsv1.1">>, <<"tlsv1">>].

tls_vsn(<<"tlsv1.3">>) -> 'tlsv1.3';
tls_vsn(<<"tlsv1.2">>) -> 'tlsv1.2';
tls_vsn(<<"tlsv1.1">>) -> 'tlsv1.1';
tls_vsn(<<"tlsv1">>) -> 'tlsv1'.

default_ciphers() -> [
    "TLS_AES_256_GCM_SHA384", "TLS_AES_128_GCM_SHA256", "TLS_CHACHA20_POLY1305_SHA256",
    "TLS_AES_128_CCM_SHA256", "TLS_AES_128_CCM_8_SHA256", "ECDHE-ECDSA-AES256-GCM-SHA384",
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
    ] ++ psk_ciphers().

psk_ciphers() -> [
        "PSK-AES128-CBC-SHA", "PSK-AES256-CBC-SHA", "PSK-3DES-EDE-CBC-SHA", "PSK-RC4-SHA"
    ].

%% @private return a list of keys in a parent field
-spec(keys(string(), hocon:config()) -> [string()]).
keys(Parent, Conf) ->
    [binary_to_list(B) || B <- maps:keys(conf_get(Parent, Conf, #{}))].

-spec ceiling(float()) -> integer().
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

to_duration(Str) ->
    case hocon_postprocess:duration(Str) of
        I when is_integer(I) -> {ok, I};
        _ -> {error, Str}
    end.

to_duration_s(Str) ->
    case hocon_postprocess:duration(Str) of
        I when is_integer(I) -> {ok, ceiling(I / 1000)};
        _ -> {error, Str}
    end.

to_duration_ms(Str) ->
    case hocon_postprocess:duration(Str) of
        I when is_integer(I) -> {ok, ceiling(I)};
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
            case inet:parse_address(Ip) of
                {ok, R} -> {ok, {R, list_to_integer(Port)}};
                _ -> {error, Str}
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
