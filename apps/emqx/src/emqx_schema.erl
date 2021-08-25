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

-export([structs/0, fields/1]).
-export([t/1, t/3, t/4, ref/1]).
-export([conf_get/2, conf_get/3, keys/2, filter/1]).
-export([ssl/1]).

structs() -> ["zones", "mqtt", "flapping_detect", "force_shutdown", "force_gc",
    "conn_congestion", "rate_limit", "quota", "listeners", "broker", "plugins",
    "stats", "sysmon", "alarm", "authorization"].

fields("stats") ->
    [ {"enable", t(boolean(), undefined, true)}
    ];

fields("authorization") ->
    [ {"no_match", t(union(allow, deny), undefined, allow)}
    , {"deny_action", t(union(ignore, disconnect), undefined, ignore)}
    , {"cache", ref("authorization_cache")}
    ];

fields("authorization_cache") ->
    [ {"enable", t(boolean(), undefined, true)}
    , {"max_size", t(range(1, 1048576), undefined, 32)}
    , {"ttl", t(duration(), undefined, "1m")}
    ];

fields("mqtt") ->
    [ {"idle_timeout", maybe_infinity(duration(), "15s")}
    , {"max_packet_size", t(bytesize(), undefined, "1MB")}
    , {"max_clientid_len", t(range(23, 65535), undefined, 65535)}
    , {"max_topic_levels", t(range(1, 65535), undefined, 65535)}
    , {"max_qos_allowed", t(range(0, 2), undefined, 2)}
    , {"max_topic_alias", t(range(0, 65535), undefined, 65535)}
    , {"retain_available", t(boolean(), undefined, true)}
    , {"wildcard_subscription", t(boolean(), undefined, true)}
    , {"shared_subscription", t(boolean(), undefined, true)}
    , {"ignore_loop_deliver", t(boolean(), undefined, false)}
    , {"strict_mode", t(boolean(), undefined, false)}
    , {"response_information", t(string(), undefined, "")}
    , {"server_keepalive", maybe_disabled(integer())}
    , {"keepalive_backoff", t(float(), undefined, 0.75)}
    , {"max_subscriptions", maybe_infinity(range(1, inf))}
    , {"upgrade_qos", t(boolean(), undefined, false)}
    , {"max_inflight", t(range(1, 65535), undefined, 32)}
    , {"retry_interval", t(duration(), undefined, "30s")}
    , {"max_awaiting_rel", maybe_infinity(integer(), 100)}
    , {"await_rel_timeout", t(duration(), undefined, "300s")}
    , {"session_expiry_interval", t(duration(), undefined, "2h")}
    , {"max_mqueue_len", maybe_infinity(range(0, inf), 1000)}
    , {"mqueue_priorities", maybe_disabled(map())}
    , {"mqueue_default_priority", t(union(highest, lowest), undefined, lowest)}
    , {"mqueue_store_qos0", t(boolean(), undefined, true)}
    , {"use_username_as_clientid", t(boolean(), undefined, false)}
    , {"peer_cert_as_username", maybe_disabled(union([cn, dn, crt, pem, md5]))}
    , {"peer_cert_as_clientid", maybe_disabled(union([cn, dn, crt, pem, md5]))}
    ];

fields("zones") ->
    [ {"$name", ref("zone_settings")}];

fields("zone_settings") ->
    Fields = ["mqtt", "stats", "authorization", "flapping_detect", "force_shutdown",
              "conn_congestion", "rate_limit", "quota", "force_gc"],
    [{F, ref("strip_default:" ++ F)} || F <- Fields];

fields("rate_limit") ->
    [ {"max_conn_rate", maybe_infinity(integer(), 1000)}
    , {"conn_messages_in", maybe_infinity(comma_separated_list())}
    , {"conn_bytes_in", maybe_infinity(comma_separated_list())}
    ];

fields("quota") ->
    [ {"conn_messages_routing", maybe_infinity(comma_separated_list())}
    , {"overall_messages_routing", maybe_infinity(comma_separated_list())}
    ];

fields("flapping_detect") ->
    [ {"enable", t(boolean(), undefined, false)}
    , {"max_count", t(integer(), undefined, 15)}
    , {"window_time", t(duration(), undefined, "1m")}
    , {"ban_time", t(duration(), undefined, "5m")}
    ];

fields("force_shutdown") ->
    [ {"enable", t(boolean(), undefined, true)}
    , {"max_message_queue_len", t(range(0, inf), undefined, 1000)}
    , {"max_heap_size", t(wordsize(), undefined, "32MB", undefined,
        fun(Siz) ->
            MaxSiz = case erlang:system_info(wordsize) of
                8 -> % arch_64
                    (1 bsl 59) - 1;
                4 -> % arch_32
                    (1 bsl 27) - 1
            end,
            case Siz > MaxSiz of
                true ->
                    error(io_lib:format("force_shutdown_policy: heap-size ~s is too large", [Siz]));
                false ->
                    ok
            end
        end)}
    ];

fields("conn_congestion") ->
    [ {"enable_alarm", t(boolean(), undefined, false)}
    , {"min_alarm_sustain_duration", t(duration(), undefined, "1m")}
    ];

fields("force_gc") ->
    [ {"enable", t(boolean(), undefined, true)}
    , {"count", t(range(0, inf), undefined, 16000)}
    , {"bytes", t(bytesize(), undefined, "16MB")}
    ];

fields("listeners") ->
    [ {"tcp", ref("t_tcp_listeners")}
    , {"ssl", ref("t_ssl_listeners")}
    , {"ws", ref("t_ws_listeners")}
    , {"wss", ref("t_wss_listeners")}
    , {"quic", ref("t_quic_listeners")}
    ];

fields("t_tcp_listeners") ->
    [ {"$name", ref("mqtt_tcp_listener")}
    ];
fields("t_ssl_listeners") ->
    [ {"$name", ref("mqtt_ssl_listener")}
    ];
fields("t_ws_listeners") ->
    [ {"$name", ref("mqtt_ws_listener")}
    ];
fields("t_wss_listeners") ->
    [ {"$name", ref("mqtt_wss_listener")}
    ];
fields("t_quic_listeners") ->
    [ {"$name", ref("mqtt_quic_listener")}
    ];

fields("mqtt_tcp_listener") ->
    [ {"tcp", ref("tcp_opts")}
    ] ++ mqtt_listener();

fields("mqtt_ssl_listener") ->
    [ {"tcp", ref("tcp_opts")}
    , {"ssl", ref("ssl_opts")}
    ] ++ mqtt_listener();

fields("mqtt_ws_listener") ->
    [ {"tcp", ref("tcp_opts")}
    , {"websocket", ref("ws_opts")}
    ] ++ mqtt_listener();

fields("mqtt_wss_listener") ->
    [ {"tcp", ref("tcp_opts")}
    , {"ssl", ref("ssl_opts")}
    , {"websocket", ref("ws_opts")}
    ] ++ mqtt_listener();

fields("mqtt_quic_listener") ->
    [ {"enabled", t(boolean(), undefined, true)}
    , {"certfile", t(string(), undefined, undefined)}
    , {"keyfile", t(string(), undefined, undefined)}
    , {"ciphers", t(comma_separated_list(), undefined, "TLS_AES_256_GCM_SHA384,"
                    "TLS_AES_128_GCM_SHA256,TLS_CHACHA20_POLY1305_SHA256")}
    , {"idle_timeout", t(duration(), undefined, "15s")}
    ] ++ base_listener();

fields("ws_opts") ->
    [ {"mqtt_path", t(string(), undefined, "/mqtt")}
    , {"mqtt_piggyback", t(union(single, multiple), undefined, multiple)}
    , {"compress", t(boolean(), undefined, false)}
    , {"idle_timeout", t(duration(), undefined, "15s")}
    , {"max_frame_size", maybe_infinity(integer())}
    , {"fail_if_no_subprotocol", t(boolean(), undefined, true)}
    , {"supported_subprotocols", t(comma_separated_list(), undefined,
        "mqtt, mqtt-v3, mqtt-v3.1.1, mqtt-v5")}
    , {"check_origin_enable", t(boolean(), undefined, false)}
    , {"allow_origin_absence", t(boolean(), undefined, true)}
    , {"check_origins", t(hoconsc:array(binary()), undefined, [])}
    , {"proxy_address_header", t(string(), undefined, "x-forwarded-for")}
    , {"proxy_port_header", t(string(), undefined, "x-forwarded-port")}
    , {"deflate_opts", ref("deflate_opts")}
    ];

fields("tcp_opts") ->
    [ {"active_n", t(integer(), undefined, 100)}
    , {"backlog", t(integer(), undefined, 1024)}
    , {"send_timeout", t(duration(), undefined, "15s")}
    , {"send_timeout_close", t(boolean(), undefined, true)}
    , {"recbuf", t(bytesize())}
    , {"sndbuf", t(bytesize())}
    , {"buffer", t(bytesize())}
    , {"high_watermark", t(bytesize(), undefined, "1MB")}
    , {"nodelay", t(boolean(), undefined, false)}
    , {"reuseaddr", t(boolean(), undefined, true)}
    ];

fields("ssl_opts") ->
    ssl(#{handshake_timeout => "15s"
        , depth => 10
        , reuse_sessions => true
        , versions => default_tls_vsns()
        , ciphers => default_ciphers()
        });

fields("deflate_opts") ->
    [ {"level", t(union([none, default, best_compression, best_speed]))}
    , {"mem_level", t(range(1, 9), undefined, 8)}
    , {"strategy", t(union([default, filtered, huffman_only, rle]))}
    , {"server_context_takeover", t(union(takeover, no_takeover))}
    , {"client_context_takeover", t(union(takeover, no_takeover))}
    , {"server_max_window_bits", t(range(8, 15), undefined, 15)}
    , {"client_max_window_bits", t(range(8, 15), undefined, 15)}
    ];

fields("plugins") ->
    [ {"expand_plugins_dir", t(string())}
    ];

fields("broker") ->
    [ {"sys_msg_interval", maybe_disabled(duration(), "1m")}
    , {"sys_heartbeat_interval", maybe_disabled(duration(), "30s")}
    , {"enable_session_registry", t(boolean(), undefined, true)}
    , {"session_locking_strategy", t(union([local, leader, quorum, all]), undefined, quorum)}
    , {"shared_subscription_strategy", t(union(random, round_robin), undefined, round_robin)}
    , {"shared_dispatch_ack_enabled", t(boolean(), undefined, false)}
    , {"route_batch_clean", t(boolean(), undefined, true)}
    , {"perf", ref("perf")}
    ];

fields("perf") ->
    [ {"route_lock_type", t(union([key, tab, global]), undefined, key)}
    , {"trie_compaction", t(boolean(), undefined, true)}
    ];

fields("sysmon") ->
    [ {"vm", ref("sysmon_vm")}
    , {"os", ref("sysmon_os")}
    ];

fields("sysmon_vm") ->
    [ {"process_check_interval", t(duration(), undefined, "30s")}
    , {"process_high_watermark", t(percent(), undefined, "80%")}
    , {"process_low_watermark", t(percent(), undefined, "60%")}
    , {"long_gc", maybe_disabled(duration())}
    , {"long_schedule", maybe_disabled(duration(), "240ms")}
    , {"large_heap", maybe_disabled(bytesize(), "32MB")}
    , {"busy_dist_port", t(boolean(), undefined, true)}
    , {"busy_port", t(boolean(), undefined, true)}
    ];

fields("sysmon_os") ->
    [ {"cpu_check_interval", t(duration(), undefined, "60s")}
    , {"cpu_high_watermark", t(percent(), undefined, "80%")}
    , {"cpu_low_watermark", t(percent(), undefined, "60%")}
    , {"mem_check_interval", maybe_disabled(duration(), "60s")}
    , {"sysmem_high_watermark", t(percent(), undefined, "70%")}
    , {"procmem_high_watermark", t(percent(), undefined, "5%")}
    ];

fields("alarm") ->
    [ {"actions", t(hoconsc:array(atom()), undefined, [log, publish])}
    , {"size_limit", t(integer(), undefined, 1000)}
    , {"validity_period", t(duration(), undefined, "24h")}
    ];

fields("strip_default:" ++ Name) ->
    strip_default(fields(Name)).

mqtt_listener() ->
    base_listener() ++
    [ {"access_rules", t(hoconsc:array(string()))}
    , {"proxy_protocol", t(boolean(), undefined, false)}
    , {"proxy_protocol_timeout", t(duration())}
    ].

base_listener() ->
    [ {"bind", t(union(ip_port(), integer()))}
    , {"acceptors", t(integer(), undefined, 16)}
    , {"max_connections", maybe_infinity(integer(), infinity)}
    , {"mountpoint", t(binary(), undefined, <<>>)}
    , {"zone", t(atom(), undefined, default)}
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

%% generate a ssl field.
%% ssl(#{"verify" => verify_peer}) will return:
%%  [ {"cacertfile", t(string(), undefined, undefined)}
%%  , {"certfile", t(string(), undefined, undefined)}
%%  , {"keyfile", t(string(), undefined, undefined)}
%%  , {"verify", t(union(verify_peer, verify_none), undefined, verify_peer)}
%%  , {"server_name_indication", undefined, undefined)}
%%  ...]
ssl(Defaults) ->
    D = fun (Field) -> maps:get(to_atom(Field), Defaults, undefined) end,
    [ {"enable", t(boolean(), undefined, D("enable"))}
    , {"cacertfile", t(string(), undefined, D("cacertfile"))}
    , {"certfile", t(string(), undefined, D("certfile"))}
    , {"keyfile", t(string(), undefined, D("keyfile"))}
    , {"verify", t(union(verify_peer, verify_none), undefined, D("verify"))}
    , {"fail_if_no_peer_cert", t(boolean(), undefined, D("fail_if_no_peer_cert"))}
    , {"secure_renegotiate", t(boolean(), undefined, D("secure_renegotiate"))}
    , {"reuse_sessions", t(boolean(), undefined, D("reuse_sessions"))}
    , {"honor_cipher_order", t(boolean(), undefined, D("honor_cipher_order"))}
    , {"handshake_timeout", t(duration(), undefined, D("handshake_timeout"))}
    , {"depth", t(integer(), undefined, D("depth"))}
    , {"password", hoconsc:t(string(), #{default => D("key_password"),
                                         sensitive => true
                                        })}
    , {"dhfile", t(string(), undefined, D("dhfile"))}
    , {"server_name_indication", t(union(disable, string()), undefined,
                                   D("server_name_indication"))}
    , {"versions", #{ type => list(atom())
                    , default => maps:get(versions, Defaults, default_tls_vsns())
                    , converter => fun (Vsns) -> [tls_vsn(V) || V <- Vsns] end
                    }}
    , {"ciphers", t(hoconsc:array(string()), undefined, D("ciphers"))}
    , {"user_lookup_fun", t(any(), undefined, {fun emqx_psk:lookup/3, <<>>})}
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

t(Type) -> hoconsc:t(Type).

t(Type, Mapping, Default) ->
    hoconsc:t(Type, #{mapping => Mapping, default => Default}).

t(Type, Mapping, Default, OverrideEnv) ->
    hoconsc:t(Type, #{ mapping => Mapping
                     , default => Default
                     , override_env => OverrideEnv
                     }).

t(Type, Mapping, Default, OverrideEnv, Validator) ->
    hoconsc:t(Type, #{ mapping => Mapping
                     , default => Default
                     , override_env => OverrideEnv
                     , validator => Validator
                     }).

ref(Field) -> hoconsc:t(hoconsc:ref(?MODULE, Field)).

maybe_disabled(T) ->
    maybe_sth(disabled, T, disabled).

maybe_disabled(T, Default) ->
    maybe_sth(disabled, T, Default).

maybe_infinity(T) ->
    maybe_sth(infinity, T, infinity).

maybe_infinity(T, Default) ->
    maybe_sth(infinity, T, Default).

maybe_sth(What, Type, Default) ->
    t(union([What, Type]), undefined, Default).

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

strip_default(Fields) ->
    [do_strip_default(F) || F <- Fields].

do_strip_default({Name, #{type := {ref, Ref}}}) ->
    {Name, nullable_no_def(ref("strip_default:" ++ Ref))};
do_strip_default({Name, #{type := {ref, _Mod, Ref}}}) ->
    {Name, nullable_no_def(ref("strip_default:" ++ Ref))};
do_strip_default({Name, Type}) ->
    {Name, nullable_no_def(Type)}.

nullable_no_def(Type) when is_map(Type) ->
    Type#{default => undefined, nullable => true}.

to_atom(Atom) when is_atom(Atom) ->
    Atom;
to_atom(Str) when is_list(Str) ->
    list_to_atom(Str);
to_atom(Bin) when is_binary(Bin) ->
    binary_to_atom(Bin, utf8).
