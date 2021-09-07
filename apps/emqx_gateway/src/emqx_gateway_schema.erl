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

-export([namespace/0, roots/0 , fields/1]).

namespace() -> gateway.

roots() -> [gateway].

fields(gateway) ->
    [{stomp, sc(ref(stomp_structs))},
     {mqttsn, sc(ref(mqttsn_structs))},
     {coap, sc(ref(coap_structs))},
     {lwm2m, sc(ref(lwm2m_structs))},
     {exproto, sc(ref(exproto_structs))}
    ];

fields(stomp_structs) ->
    [ {frame, sc(ref(stomp_frame))}
    , {listeners, sc(ref(tcp_listener_group))}
    ] ++ gateway_common_options();

fields(stomp_frame) ->
    [ {max_headers, sc(integer(), undefined, 10)}
    , {max_headers_length, sc(integer(), undefined, 1024)}
    , {max_body_length, sc(integer(), undefined, 8192)}
    ];

fields(mqttsn_structs) ->
    [ {gateway_id, sc(integer())}
    , {broadcast, sc(boolean())}
    , {enable_qos3, sc(boolean())}
    , {predefined, hoconsc:array(ref(mqttsn_predefined))}
    , {listeners, sc(ref(udp_listener_group))}
    ] ++ gateway_common_options();

fields(mqttsn_predefined) ->
    [ {id, sc(integer())}
    , {topic, sc(binary())}
    ];

fields(coap_structs) ->
    [ {heartbeat, sc(duration(), undefined, <<"30s">>)}
    , {connection_required, sc(boolean(), undefined, false)}
    , {notify_type, sc(union([non, con, qos]), undefined, qos)}
    , {subscribe_qos, sc(union([qos0, qos1, qos2, coap]), undefined, coap)}
    , {publish_qos, sc(union([qos0, qos1, qos2, coap]), undefined, coap)}
    , {listeners, sc(ref(udp_listener_group))}
    ] ++ gateway_common_options();

fields(lwm2m_structs) ->
    [ {xml_dir, sc(binary())}
    , {lifetime_min, sc(duration())}
    , {lifetime_max, sc(duration())}
    , {qmode_time_windonw, sc(integer())}
    , {auto_observe, sc(boolean())}
    , {mountpoint, sc(string())}
    , {update_msg_publish_condition, sc(union([always, contains_object_list]))}
    , {translators, sc(ref(translators))}
    , {listeners, sc(ref(udp_listener_group))}
    ] ++ gateway_common_options();

fields(exproto_structs) ->
    [ {server, sc(ref(exproto_grpc_server))}
    , {handler, sc(ref(exproto_grpc_handler))}
    , {listeners, sc(ref(udp_tcp_listener_group))}
    ] ++ gateway_common_options();

fields(exproto_grpc_server) ->
    [ {bind, sc(union(ip_port(), integer()))}
      %% TODO: ssl options
    ];

fields(exproto_grpc_handler) ->
    [ {address, sc(binary())}
      %% TODO: ssl
    ];

fields(clientinfo_override) ->
    [ {username, sc(binary())}
    , {password, sc(binary())}
    , {clientid, sc(binary())}
    ];

fields(translators) ->
    [ {command, sc(ref(translator))}
    , {response, sc(ref(translator))}
    , {notify, sc(ref(translator))}
    , {register, sc(ref(translator))}
    , {update, sc(ref(translator))}
    ];

fields(translator) ->
    [ {topic, sc(binary())}
    , {qos, sc(range(0, 2))}
    ];

fields(udp_listener_group) ->
    [ {udp, sc(ref(udp_listener))}
    , {dtls, sc(ref(dtls_listener))}
    ];

fields(tcp_listener_group) ->
    [ {tcp, sc(ref(tcp_listener))}
    , {ssl, sc(ref(ssl_listener))}
    ];

fields(udp_tcp_listener_group) ->
    [ {udp, sc(ref(udp_listener))}
    , {dtls, sc(ref(dtls_listener))}
    , {tcp, sc(ref(tcp_listener))}
    , {ssl, sc(ref(ssl_listener))}
    ];

fields(tcp_listener) ->
    [ {"$name", sc(ref(tcp_listener_settings))}];

fields(ssl_listener) ->
    [ {"$name", sc(ref(ssl_listener_settings))}];

fields(udp_listener) ->
    [ {"$name", sc(ref(udp_listener_settings))}];

fields(dtls_listener) ->
    [ {"$name", sc(ref(dtls_listener_settings))}];

fields(listener_settings) ->
    [ {enable, sc(boolean(), undefined, true)}
    , {bind, sc(union(ip_port(), integer()))}
    , {acceptors, sc(integer(), undefined, 8)}
    , {max_connections, sc(integer(), undefined, 1024)}
    , {max_conn_rate, sc(integer())}
    , {active_n, sc(integer(), undefined, 100)}
    %, {rate_limit, sc(comma_separated_list())}
    , {access, sc(ref(access))}
    , {proxy_protocol, sc(boolean())}
    , {proxy_protocol_timeout, sc(duration())}
    , {backlog, sc(integer(), undefined, 1024)}
    , {send_timeout, sc(duration(), undefined, <<"15s">>)}
    , {send_timeout_close, sc(boolean(), undefined, true)}
    , {recbuf, sc(bytesize())}
    , {sndbuf, sc(bytesize())}
    , {buffer, sc(bytesize())}
    , {high_watermark, sc(bytesize(), undefined, <<"1MB">>)}
    , {tune_buffer, sc(boolean())}
    , {nodelay, sc(boolean())}
    , {reuseaddr, sc(boolean())}
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

% authentication() ->
%     hoconsc:union(
%       [ undefined
%       , hoconsc:ref(emqx_authn_mnesia, config)
%       , hoconsc:ref(emqx_authn_mysql, config)
%       , hoconsc:ref(emqx_authn_pgsql, config)
%       , hoconsc:ref(emqx_authn_mongodb, standalone)
%       , hoconsc:ref(emqx_authn_mongodb, 'replica-set')
%       , hoconsc:ref(emqx_authn_mongodb, 'sharded-cluster')
%       , hoconsc:ref(emqx_authn_redis, standalone)
%       , hoconsc:ref(emqx_authn_redis, cluster)
%       , hoconsc:ref(emqx_authn_redis, sentinel)
%       , hoconsc:ref(emqx_authn_http, get)
%       , hoconsc:ref(emqx_authn_http, post)
%       , hoconsc:ref(emqx_authn_jwt, 'hmac-based')
%       , hoconsc:ref(emqx_authn_jwt, 'public-key')
%       , hoconsc:ref(emqx_authn_jwt, 'jwks')
%       , hoconsc:ref(emqx_enhanced_authn_scram_mnesia, config)
%       ]).

gateway_common_options() ->
    [ {enable, sc(boolean(), undefined, true)}
    , {enable_stats, sc(boolean(), undefined, true)}
    , {idle_timeout, sc(duration(), undefined, <<"30s">>)}
    , {mountpoint, sc(binary())}
    , {clientinfo_override, sc(ref(clientinfo_override))}
    , {authentication,  sc(hoconsc:lazy(map()))}
    ].

%%--------------------------------------------------------------------
%% Helpers

%% types

sc(Type) -> #{type => Type}.

sc(Type, Mapping, Default) ->
    hoconsc:mk(Type, #{mapping => Mapping, default => Default}).

ref(Field) ->
    hoconsc:ref(?MODULE, Field).

%% utils

%% generate a ssl field.
%% ssl("emqx", #{"verify" => verify_peer}) will return
%% [ {"cacertfile", sc(string(), "emqx.cacertfile", undefined)}
%% , {"certfile", sc(string(), "emqx.certfile", undefined)}
%% , {"keyfile", sc(string(), "emqx.keyfile", undefined)}
%% , {"verify", sc(union(verify_peer, verify_none), "emqx.verify", verify_peer)}
%% , {"server_name_indication", "emqx.server_name_indication", undefined)}
%% ...
ssl(Mapping, Defaults) ->
    M = fun (Field) ->
                case (Mapping) of
            undefined -> undefined;
            _ -> Mapping ++ "." ++ Field
        end end,
    D = fun (Field) -> maps:get(list_to_atom(Field), Defaults, undefined) end,
    [ {"enable", sc(boolean(), M("enable"), D("enable"))}
    , {"cacertfile", sc(binary(), M("cacertfile"), D("cacertfile"))}
    , {"certfile", sc(binary(), M("certfile"), D("certfile"))}
    , {"keyfile", sc(binary(), M("keyfile"), D("keyfile"))}
    , {"verify", sc(union(verify_peer, verify_none), M("verify"), D("verify"))}
    , {"fail_if_no_peer_cert", sc(boolean(), M("fail_if_no_peer_cert"), D("fail_if_no_peer_cert"))}
    , {"secure_renegotiate", sc(boolean(), M("secure_renegotiate"), D("secure_renegotiate"))}
    , {"reuse_sessions", sc(boolean(), M("reuse_sessions"), D("reuse_sessions"))}
    , {"honor_cipher_order", sc(boolean(), M("honor_cipher_order"), D("honor_cipher_order"))}
    , {"handshake_timeout", sc(duration(), M("handshake_timeout"), D("handshake_timeout"))}
    , {"depth", sc(integer(), M("depth"), D("depth"))}
    , {"password", hoconsc:mk(binary(), #{ mapping => M("key_password")
                                         , default => D("key_password")
                                         , sensitive => true
                                        })}
    , {"dhfile", sc(binary(), M("dhfile"), D("dhfile"))}
    , {"server_name_indication", sc(union(disable, binary()), M("server_name_indication"),
                                   D("server_name_indication"))}
    , {"tls_versions", sc(comma_separated_list(), M("tls_versions"), D("tls_versions"))}
    , {"ciphers", sc(comma_separated_list(), M("ciphers"), D("ciphers"))}
    , {"psk_ciphers", sc(comma_separated_list(), M("ciphers"), D("ciphers"))}].
