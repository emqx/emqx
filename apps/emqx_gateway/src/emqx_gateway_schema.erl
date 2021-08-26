%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_gateway_schema).

-behaviour(hocon_schema).

-dialyzer(no_return).
-dialyzer(no_match).
-dialyzer(no_contracts).
-dialyzer(no_unused).
-dialyzer(no_fail_call).

-include_lib("typerefl/include/types.hrl").

-type ip_port() :: tuple().
-type duration() :: integer().
-type bytesize() :: integer().
-type comma_separated_list() :: list().

-typerefl_from_string({ip_port/0, emqx_schema, to_ip_port}).
-typerefl_from_string({duration/0, emqx_schema, to_duration}).
-typerefl_from_string({bytesize/0, emqx_schema, to_bytesize}).
-typerefl_from_string({comma_separated_list/0, emqx_schema,
                       to_comma_separated_list}).

-reflect_type([ duration/0
              , bytesize/0
              , comma_separated_list/0
              , ip_port/0
              ]).

-export([structs/0 , fields/1]).

-export([t/1, t/3, t/4, ref/1]).

%%--------------------------------------------------------------------
%% Structs

structs() -> [gateway].

fields(gateway) ->
    [{stomp, t(ref(stomp_structs))},
     {mqttsn, t(ref(mqttsn_structs))},
     {coap, t(ref(coap_structs))},
     {lwm2m, t(ref(lwm2m_structs))},
     {exproto, t(ref(exproto_structs))}
    ];

fields(stomp_structs) ->
    [ {frame, t(ref(stomp_frame))}
    , {listeners, t(ref(tcp_listener_group))}
    ] ++ gateway_common_options();

fields(stomp_frame) ->
    [ {max_headers, t(integer(), undefined, 10)}
    , {max_headers_length, t(integer(), undefined, 1024)}
    , {max_body_length, t(integer(), undefined, 8192)}
    ];

fields(mqttsn_structs) ->
    [ {gateway_id, t(integer())}
    , {broadcast, t(boolean())}
    , {enable_qos3, t(boolean())}
    , {predefined, hoconsc:array(ref(mqttsn_predefined))}
    , {listeners, t(ref(udp_listener_group))}
    ] ++ gateway_common_options();

fields(mqttsn_predefined) ->
    [ {id, t(integer())}
    , {topic, t(binary())}
    ];

fields(coap_structs) ->
    [ {heartbeat, t(duration(), undefined, <<"30s">>)}
    , {notify_type, t(union([non, con, qos]), undefined, qos)}
    , {subscribe_qos, t(union([qos0, qos1, qos2, coap]), undefined, coap)}
    , {publish_qos, t(union([qos0, qos1, qos2, coap]), undefined, coap)}
    , {listeners, t(ref(udp_listener_group))}
    ] ++ gateway_common_options();

fields(lwm2m_structs) ->
    [ {xml_dir, t(binary())}
    , {lifetime_min, t(duration())}
    , {lifetime_max, t(duration())}
    , {qmode_time_windonw, t(integer())}
    , {auto_observe, t(boolean())}
    , {update_msg_publish_condition, t(union([always, contains_object_list]))}
    , {translators, t(ref(translators))}
    , {listeners, t(ref(udp_listener_group))}
    ] ++ gateway_common_options();

fields(exproto_structs) ->
    [ {server, t(ref(exproto_grpc_server))}
    , {handler, t(ref(exproto_grpc_handler))}
    , {listeners, t(ref(udp_tcp_listener_group))}
    ] ++ gateway_common_options();

fields(exproto_grpc_server) ->
    [ {bind, t(union(ip_port(), integer()))}
      %% TODO: ssl options
    ];

fields(exproto_grpc_handler) ->
    [ {address, t(binary())}
      %% TODO: ssl
    ];

fields(clientinfo_override) ->
    [ {username, t(binary())}
    , {password, t(binary())}
    , {clientid, t(binary())}
    ];

fields(translators) ->
    [{"$name", t(binary())}];

fields(udp_listener_group) ->
    [ {udp, t(ref(udp_listener))}
    , {dtls, t(ref(dtls_listener))}
    ];

fields(tcp_listener_group) ->
    [ {tcp, t(ref(tcp_listener))}
    , {ssl, t(ref(ssl_listener))}
    ];

fields(udp_tcp_listener_group) ->
    [ {udp, t(ref(udp_listener))}
    , {dtls, t(ref(dtls_listener))}
    , {tcp, t(ref(tcp_listener))}
    , {ssl, t(ref(ssl_listener))}
    ];

fields(tcp_listener) ->
    [ {"$name", t(ref(tcp_listener_settings))}];

fields(ssl_listener) ->
    [ {"$name", t(ref(ssl_listener_settings))}];

fields(udp_listener) ->
    [ {"$name", t(ref(udp_listener_settings))}];

fields(dtls_listener) ->
    [ {"$name", t(ref(dtls_listener_settings))}];

fields(listener_settings) ->
    [ {enable, t(boolean(), undefined, true)}
    , {bind, t(union(ip_port(), integer()))}
    , {acceptors, t(integer(), undefined, 8)}
    , {max_connections, t(integer(), undefined, 1024)}
    , {max_conn_rate, t(integer())}
    , {active_n, t(integer(), undefined, 100)}
    %, {rate_limit, t(comma_separated_list())}
    , {access, t(ref(access))}
    , {proxy_protocol, t(boolean())}
    , {proxy_protocol_timeout, t(duration())}
    , {backlog, t(integer(), undefined, 1024)}
    , {send_timeout, t(duration(), undefined, <<"15s">>)}
    , {send_timeout_close, t(boolean(), undefined, true)}
    , {recbuf, t(bytesize())}
    , {sndbuf, t(bytesize())}
    , {buffer, t(bytesize())}
    , {high_watermark, t(bytesize(), undefined, <<"1MB">>)}
    , {tune_buffer, t(boolean())}
    , {nodelay, t(boolean())}
    , {reuseaddr, t(boolean())}
    ];

fields(tcp_listener_settings) ->
    [
     %% some special confs for tcp listener
    ] ++ fields(listener_settings);

fields(ssl_listener_settings) ->
    [
      %% some special confs for ssl listener
    ] ++
    ssl(undefined, #{handshake_timeout => <<"15s">>
                   , depth => 10
                   , reuse_sessions => true}) ++ fields(listener_settings);

fields(udp_listener_settings) ->
    [
      %% some special confs for udp listener
    ] ++ fields(listener_settings);

fields(dtls_listener_settings) ->
    [
      %% some special confs for dtls listener
    ] ++
    ssl(undefined, #{handshake_timeout => <<"15s">>
                   , depth => 10
                   , reuse_sessions => true}) ++ fields(listener_settings);

fields(access) ->
    [ {"$id", #{type => binary(),
                nullable => true}}];

fields(ExtraField) ->
    Mod = list_to_atom(ExtraField++"_schema"),
    Mod:fields(ExtraField).

authentication() ->
    hoconsc:union(
      [ undefined
      , hoconsc:ref(emqx_authn_mnesia, config)
      , hoconsc:ref(emqx_authn_mysql, config)
      , hoconsc:ref(emqx_authn_pgsql, config)
      , hoconsc:ref(emqx_authn_mongodb, standalone)
      , hoconsc:ref(emqx_authn_mongodb, 'replica-set')
      , hoconsc:ref(emqx_authn_mongodb, 'sharded-cluster')
      , hoconsc:ref(emqx_authn_redis, standalone)
      , hoconsc:ref(emqx_authn_redis, cluster)
      , hoconsc:ref(emqx_authn_redis, sentinel)
      , hoconsc:ref(emqx_authn_http, get)
      , hoconsc:ref(emqx_authn_http, post)
      , hoconsc:ref(emqx_authn_jwt, 'hmac-based')
      , hoconsc:ref(emqx_authn_jwt, 'public-key')
      , hoconsc:ref(emqx_authn_jwt, 'jwks')
      , hoconsc:ref(emqx_enhanced_authn_scram_mnesia, config)
      ]).

gateway_common_options() ->
    [ {enable, t(boolean(), undefined, true)}
    , {enable_stats, t(boolean(), undefined, true)}
    , {idle_timeout, t(duration(), undefined, <<"30s">>)}
    , {mountpoint, t(binary())}
    , {clientinfo_override, t(ref(clientinfo_override))}
    , {authentication,  t(authentication(), undefined, undefined)}
    ].

%%--------------------------------------------------------------------
%% Helpers

%% types

t(Type) -> #{type => Type}.

t(Type, Mapping, Default) ->
    hoconsc:t(Type, #{mapping => Mapping, default => Default}).

t(Type, Mapping, Default, OverrideEnv) ->
    hoconsc:t(Type, #{ mapping => Mapping
                     , default => Default
                     , override_env => OverrideEnv
                     }).

ref(Field) ->
    hoconsc:ref(?MODULE, Field).

%% utils

%% generate a ssl field.
%% ssl("emqx", #{"verify" => verify_peer}) will return
%% [ {"cacertfile", t(string(), "emqx.cacertfile", undefined)}
%% , {"certfile", t(string(), "emqx.certfile", undefined)}
%% , {"keyfile", t(string(), "emqx.keyfile", undefined)}
%% , {"verify", t(union(verify_peer, verify_none), "emqx.verify", verify_peer)}
%% , {"server_name_indication", "emqx.server_name_indication", undefined)}
%% ...
ssl(Mapping, Defaults) ->
    M = fun (Field) ->
        case (Mapping) of
            undefined -> undefined;
            _ -> Mapping ++ "." ++ Field
        end end,
    D = fun (Field) -> maps:get(list_to_atom(Field), Defaults, undefined) end,
    [ {"enable", t(boolean(), M("enable"), D("enable"))}
    , {"cacertfile", t(binary(), M("cacertfile"), D("cacertfile"))}
    , {"certfile", t(binary(), M("certfile"), D("certfile"))}
    , {"keyfile", t(binary(), M("keyfile"), D("keyfile"))}
    , {"verify", t(union(verify_peer, verify_none), M("verify"), D("verify"))}
    , {"fail_if_no_peer_cert", t(boolean(), M("fail_if_no_peer_cert"), D("fail_if_no_peer_cert"))}
    , {"secure_renegotiate", t(boolean(), M("secure_renegotiate"), D("secure_renegotiate"))}
    , {"reuse_sessions", t(boolean(), M("reuse_sessions"), D("reuse_sessions"))}
    , {"honor_cipher_order", t(boolean(), M("honor_cipher_order"), D("honor_cipher_order"))}
    , {"handshake_timeout", t(duration(), M("handshake_timeout"), D("handshake_timeout"))}
    , {"depth", t(integer(), M("depth"), D("depth"))}
    , {"password", hoconsc:t(binary(), #{mapping => M("key_password"),
                                         default => D("key_password"),
                                         sensitive => true
                                        })}
    , {"dhfile", t(binary(), M("dhfile"), D("dhfile"))}
    , {"server_name_indication", t(union(disable, binary()), M("server_name_indication"),
                                   D("server_name_indication"))}
    , {"tls_versions", t(comma_separated_list(), M("tls_versions"), D("tls_versions"))}
    , {"ciphers", t(comma_separated_list(), M("ciphers"), D("ciphers"))}
    , {"psk_ciphers", t(comma_separated_list(), M("ciphers"), D("ciphers"))}].
