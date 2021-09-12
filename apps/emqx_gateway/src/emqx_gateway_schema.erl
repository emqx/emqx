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
    [{stomp, sc(ref(stomp))},
     {mqttsn, sc(ref(mqttsn))},
     {coap, sc(ref(coap))},
     {lwm2m, sc(ref(lwm2m))},
     {exproto, sc(ref(exproto))}
    ];

fields(stomp) ->
    [ {frame, sc(ref(stomp_frame))}
    , {listeners, sc(ref(tcp_listeners))}
    ] ++ gateway_common_options();

fields(stomp_frame) ->
    [ {max_headers, sc(integer(), 10)}
    , {max_headers_length, sc(integer(), 1024)}
    , {max_body_length, sc(integer(), 8192)}
    ];

fields(mqttsn) ->
    [ {gateway_id, sc(integer())}
    , {broadcast, sc(boolean())}
    , {enable_qos3, sc(boolean())}
    , {predefined, hoconsc:array(ref(mqttsn_predefined))}
    , {listeners, sc(ref(udp_listeners))}
    ] ++ gateway_common_options();

fields(mqttsn_predefined) ->
    [ {id, sc(integer())}
    , {topic, sc(binary())}
    ];

fields(coap) ->
    [ {heartbeat, sc(duration(), <<"30s">>)}
    , {connection_required, sc(boolean(), false)}
    , {notify_type, sc(hoconsc:union([non, con, qos]), qos)}
    , {subscribe_qos, sc(hoconsc:union([qos0, qos1, qos2, coap]), coap)}
    , {publish_qos, sc(hoconsc:union([qos0, qos1, qos2, coap]), coap)}
    , {listeners, sc(ref(udp_listeners))}
    ] ++ gateway_common_options();

fields(lwm2m) ->
    [ {xml_dir, sc(binary())}
    , {lifetime_min, sc(duration())}
    , {lifetime_max, sc(duration())}
    , {qmode_time_windonw, sc(integer())}
    , {auto_observe, sc(boolean())}
    , {update_msg_publish_condition, sc(hoconsc:union([always, contains_object_list]))}
    , {translators, sc(ref(translators))}
    , {listeners, sc(ref(udp_listeners))}
    ] ++ gateway_common_options();

fields(exproto) ->
    [ {server, sc(ref(exproto_grpc_server))}
    , {handler, sc(ref(exproto_grpc_handler))}
    , {listeners, sc(ref(udp_tcp_listeners))}
    ] ++ gateway_common_options();

fields(exproto_grpc_server) ->
    [ {bind, sc(hoconsc:union([ip_port(), integer()]))}
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

fields(udp_listeners) ->
    [ {udp, sc(map(name, ref(udp_listener)))}
    , {dtls, sc(map(name, ref(dtls_listener)))}
    ];

fields(tcp_listeners) ->
    [ {tcp, sc(map(name, ref(tcp_listener)))}
    , {ssl, sc(map(name, ref(ssl_listener)))}
    ];

fields(udp_tcp_listeners) ->
    [ {udp, sc(map(name, ref(udp_listener)))}
    , {dtls, sc(map(name, ref(dtls_listener)))}
    , {tcp, sc(map(name, ref(tcp_listener)))}
    , {ssl, sc(map(name, ref(ssl_listener)))}
    ];

fields(tcp_listener) ->
    [
     %% some special confs for tcp listener
    ] ++
    tcp_opts() ++
    proxy_protocol_opts() ++
    common_listener_opts();

fields(ssl_listener) ->
    fields(tcp_listener) ++
    ssl_opts();

fields(udp_listener) ->
    [
     %% some special confs for udp listener
    ] ++
    udp_opts() ++
    common_listener_opts();

fields(dtls_listener) ->
    fields(udp_listener) ++
    dtls_opts();

fields(udp_opts) ->
    [ {active_n, sc(integer(), 100)}
    , {recbuf, sc(bytesize())}
    , {sndbuf, sc(bytesize())}
    , {buffer, sc(bytesize())}
    , {reuseaddr, sc(boolean(), true)}
    ];

fields(dtls_listener_ssl_opts) ->
    Base = emqx_schema:fields("listener_ssl_opts"),
    DtlsVers = hoconsc:mk(
                 typerefl:alias("string", list(atom())),
                 #{ default => default_dtls_vsns(),
                    converter => fun (Vsns) ->
                      [dtls_vsn(iolist_to_binary(V)) || V <- Vsns]
                    end
                  }),
    Ciphers = sc(hoconsc:array(string()), default_ciphers()),
    lists:keydelete(
        "handshake_timeout", 1,
        lists:keyreplace(
            "ciphers", 1,
            lists:keyreplace("versions", 1, Base, {"versions", DtlsVers}),
            {"ciphers", Ciphers}
         )
     ).

default_ciphers() ->
    ["ECDHE-ECDSA-AES256-GCM-SHA384",
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

psk_ciphers() ->
    ["PSK-AES128-CBC-SHA", "PSK-AES256-CBC-SHA",
     "PSK-3DES-EDE-CBC-SHA", "PSK-RC4-SHA"
    ].

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
    [ {enable, sc(boolean(), true)}
    , {enable_stats, sc(boolean(), true)}
    , {idle_timeout, sc(duration(), <<"30s">>)}
    , {mountpoint, sc(binary(), <<>>)}
    , {clientinfo_override, sc(ref(clientinfo_override))}
    , {authentication,  sc(hoconsc:lazy(map()))}
    ].

common_listener_opts() ->
    [ {enable, sc(boolean(), true)}
    , {bind, sc(union(ip_port(), integer()))}
    , {acceptors, sc(integer(), 16)}
    , {max_connections, sc(integer(), 1024)}
    , {max_conn_rate, sc(integer())}
    %, {rate_limit, sc(comma_separated_list())}
    , {mountpoint, sc(binary(), undefined)}
    , {access_rules, sc(hoconsc:array(string()), [])}
    ].

tcp_opts() ->
    [{tcp, sc_meta(ref(emqx_schema, "tcp_opts"), #{})}].

udp_opts() ->
    [{udp, sc_meta(ref(udp_opts), #{})}].

ssl_opts() ->
    [{ssl, sc_meta(ref(emqx_schema, "listener_ssl_opts"), #{})}].

dtls_opts() ->
    [{dtls, sc_meta(ref(dtls_listener_ssl_opts), #{})}].

proxy_protocol_opts() ->
    [ {proxy_protocol, sc(boolean())}
    , {proxy_protocol_timeout, sc(duration())}
    ].

default_dtls_vsns() ->
    [<<"dtlsv1.2">>, <<"dtlsv1">>].

dtls_vsn(<<"dtlsv1.2">>) -> 'dtlsv1.2';
dtls_vsn(<<"dtlsv1">>) -> 'dtlsv1'.

sc(Type) ->
    sc_meta(Type, #{}).

sc(Type, Default) ->
    sc_meta(Type, #{default => Default}).

sc_meta(Type, Meta) ->
    hoconsc:mk(Type, Meta).

map(Name, Type) ->
    hoconsc:map(Name, Type).

ref(StructName) ->
    ref(?MODULE, StructName).

ref(Mod, Field) ->
    hoconsc:ref(Mod, Field).
