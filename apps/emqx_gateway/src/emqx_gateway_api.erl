%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
%%
-module(emqx_gateway_api).

-include("emqx_gateway_http.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-behaviour(minirest_api).

-import(hoconsc, [mk/2, ref/1, ref/2]).

-import(
    emqx_gateway_http,
    [
        return_http_error/2,
        with_gateway/2
    ]
).

%% minirest/dashboard_swagger behaviour callbacks
-export([
    api_spec/0,
    paths/0,
    schema/1,
    namespace/0
]).

-export([
    roots/0,
    fields/1,
    listener_schema/0
]).

%% http handlers
-export([
    gateways/2,
    gateway/2,
    gateway_enable/2
]).

-define(KNOWN_GATEWAY_STATUSES, [<<"running">>, <<"stopped">>, <<"unloaded">>]).
-define(TAGS, [<<"Gateways">>]).

%%--------------------------------------------------------------------
%% minirest behaviour callbacks
%%--------------------------------------------------------------------

namespace() -> undefined.

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/gateways",
        "/gateways/:name",
        "/gateways/:name/enable/:enable"
    ].

%%--------------------------------------------------------------------
%% http handlers

gateways(get, Request) ->
    Params = maps:get(query_string, Request, #{}),
    Status = maps:get(<<"status">>, Params, <<"all">>),
    case lists:member(Status, [<<"all">> | ?KNOWN_GATEWAY_STATUSES]) of
        true ->
            {200, emqx_gateway_http:gateways(binary_to_existing_atom(Status, utf8))};
        false ->
            return_http_error(
                400,
                [
                    "Unknown gateway status in query: ",
                    Status,
                    "\n",
                    "Values allowed: ",
                    lists:join(", ", ?KNOWN_GATEWAY_STATUSES)
                ]
            )
    end.

gateway(get, #{bindings := #{name := Name}}) ->
    try
        case emqx_gateway:lookup(Name) of
            undefined ->
                {200, #{name => Name, status => unloaded}};
            Gateway ->
                GwConf = emqx_gateway_conf:gateway(Name),
                GwInfo0 = emqx_gateway_utils:unix_ts_to_rfc3339(
                    [created_at, started_at, stopped_at],
                    Gateway
                ),
                GwInfo1 = maps:with(
                    [
                        name,
                        status,
                        created_at,
                        started_at,
                        stopped_at
                    ],
                    GwInfo0
                ),
                {200, maps:merge(GwConf, GwInfo1)}
        end
    catch
        throw:not_found ->
            return_http_error(404, <<"NOT FOUND">>)
    end;
gateway(put, #{
    body := GwConf0,
    bindings := #{name := Name}
}) ->
    GwConf = maps:without([<<"name">>], GwConf0),
    try
        LoadOrUpdateF =
            case emqx_gateway:lookup(Name) of
                undefined ->
                    fun emqx_gateway_conf:load_gateway/2;
                _ ->
                    fun emqx_gateway_conf:update_gateway/2
            end,
        case LoadOrUpdateF(Name, GwConf) of
            {ok, _} ->
                {204};
            {error, Reason} ->
                emqx_gateway_http:reason2resp(Reason)
        end
    catch
        error:{badconf, _} = Reason1 ->
            emqx_gateway_http:reason2resp(Reason1);
        throw:not_found ->
            return_http_error(404, <<"NOT FOUND">>)
    end.

gateway_enable(put, #{bindings := #{name := Name, enable := Enable}}) ->
    try
        case emqx_gateway:lookup(Name) of
            undefined ->
                return_http_error(404, <<"NOT FOUND">>);
            _Gateway ->
                {ok, _} = emqx_gateway_conf:update_gateway(Name, #{<<"enable">> => Enable}),
                {204}
        end
    catch
        throw:not_found ->
            return_http_error(404, <<"NOT FOUND">>)
    end.

%%--------------------------------------------------------------------
%% Swagger defines
%%--------------------------------------------------------------------

schema("/gateways") ->
    #{
        'operationId' => gateways,
        get =>
            #{
                tags => ?TAGS,
                desc => ?DESC(list_gateway),
                summary => <<"List all gateways">>,
                parameters => params_gateway_status_in_qs(),
                responses =>
                    #{
                        200 => emqx_dashboard_swagger:schema_with_example(
                            hoconsc:array(ref(gateway_overview)),
                            examples_gateway_overview()
                        ),
                        400 => emqx_dashboard_swagger:error_codes(
                            [?BAD_REQUEST], <<"Bad request">>
                        )
                    }
            }
    };
schema("/gateways/:name") ->
    #{
        'operationId' => gateway,
        get =>
            #{
                tags => ?TAGS,
                desc => ?DESC(get_gateway),
                summary => <<"Get gateway">>,
                parameters => params_gateway_name_in_path(),
                responses =>
                    #{
                        200 => schema_gateways_conf(),
                        404 => emqx_dashboard_swagger:error_codes(
                            [?NOT_FOUND, ?RESOURCE_NOT_FOUND], <<"Not Found">>
                        )
                    }
            },
        put =>
            #{
                tags => ?TAGS,
                desc => ?DESC(update_gateway),
                % [FIXME] add proper desc
                summary => <<"Load or update the gateway confs">>,
                parameters => params_gateway_name_in_path(),
                'requestBody' => schema_load_or_update_gateways_conf(),
                responses =>
                    ?STANDARD_RESP(#{204 => <<"Gateway configuration updated">>})
            }
    };
schema("/gateways/:name/enable/:enable") ->
    #{
        'operationId' => gateway_enable,
        put =>
            #{
                tags => ?TAGS,
                desc => ?DESC(update_gateway),
                summary => <<"Enable or disable gateway">>,
                parameters => params_gateway_name_in_path() ++ params_gateway_enable_in_path(),
                responses =>
                    #{
                        204 => <<"Gateway configuration updated">>,
                        404 => emqx_dashboard_swagger:error_codes(
                            [?NOT_FOUND, ?RESOURCE_NOT_FOUND], <<"Not Found">>
                        )
                    }
            }
    }.

%%--------------------------------------------------------------------
%% params defines

params_gateway_name_in_path() ->
    [
        {name,
            mk(
                hoconsc:enum(emqx_gateway_schema:gateway_names()),
                #{
                    in => path,
                    desc => ?DESC(gateway_name_in_qs),
                    example => <<"stomp">>
                }
            )}
    ].

params_gateway_status_in_qs() ->
    [
        {status,
            mk(
                binary(),
                #{
                    in => query,
                    enum => ?KNOWN_GATEWAY_STATUSES,
                    required => false,
                    desc => ?DESC(gateway_status_in_qs),
                    example => <<"running">>
                }
            )}
    ].

params_gateway_enable_in_path() ->
    [
        {enable,
            mk(
                boolean(),
                #{
                    in => path,
                    desc => ?DESC(gateway_enable_in_path),
                    example => true
                }
            )}
    ].
%%--------------------------------------------------------------------
%% schemas

roots() ->
    [
        gateway_overview,
        gateway_stats
    ].

fields(gateway_overview) ->
    [
        {name,
            mk(
                binary(),
                #{desc => ?DESC(gateway_name)}
            )},
        {status,
            mk(
                hoconsc:enum([running, stopped, unloaded]),
                #{desc => ?DESC(gateway_status)}
            )},
        {created_at,
            mk(
                binary(),
                #{desc => ?DESC(gateway_created_at)}
            )},
        {started_at,
            mk(
                binary(),
                #{
                    required => false,
                    desc => ?DESC(gateway_started_at)
                }
            )},
        {stopped_at,
            mk(
                binary(),
                #{
                    required => false,
                    desc => ?DESC(gateway_stopped_at)
                }
            )},
        {max_connections,
            mk(
                pos_integer(),
                #{desc => ?DESC(gateway_max_connections)}
            )},
        {current_connections,
            mk(
                non_neg_integer(),
                #{desc => ?DESC(gateway_current_connections)}
            )},
        {listeners,
            mk(
                hoconsc:array(ref(gateway_listener_overview)),
                #{
                    required => {false, recursively},
                    desc => ?DESC(gateway_listeners)
                }
            )},
        {node_status,
            mk(hoconsc:array(ref(gateway_node_status)), #{desc => ?DESC(gateway_node_status)})}
    ];
fields(gateway_listener_overview) ->
    [
        {id,
            mk(
                binary(),
                #{desc => ?DESC(gateway_listener_id)}
            )},
        {running,
            mk(
                boolean(),
                #{desc => ?DESC(gateway_listener_running)}
            )},
        {type,
            mk(
                hoconsc:enum([tcp, ssl, udp, dtls]),
                #{desc => ?DESC(gateway_listener_type)}
            )}
    ];
fields(gateway_node_status) ->
    [
        {node, mk(node(), #{desc => ?DESC(node)})},
        {status,
            mk(
                hoconsc:enum([running, stopped, unloaded]),
                #{desc => ?DESC(gateway_status)}
            )},
        {max_connections,
            mk(
                pos_integer(),
                #{desc => ?DESC(gateway_max_connections)}
            )},
        {current_connections,
            mk(
                non_neg_integer(),
                #{desc => ?DESC(gateway_current_connections)}
            )}
    ];
fields(Gw) when
    Gw == stomp;
    Gw == mqttsn;
    Gw == coap;
    Gw == lwm2m;
    Gw == exproto;
    Gw == gbt32960;
    Gw == ocpp;
    Gw == jt808
->
    [{name, mk(Gw, #{desc => ?DESC(gateway_name)})}] ++
        convert_listener_struct(emqx_gateway_schema:gateway_schema(Gw));
fields(Gw) when
    Gw == update_stomp;
    Gw == update_mqttsn;
    Gw == update_coap;
    Gw == update_lwm2m;
    Gw == update_exproto;
    Gw == update_gbt32960;
    Gw == update_ocpp;
    Gw == update_jt808
->
    "update_" ++ GwStr = atom_to_list(Gw),
    Gw1 = list_to_existing_atom(GwStr),
    remove_listener_and_authn(emqx_gateway_schema:gateway_schema(Gw1));
fields(Listener) when
    Listener == tcp_listener;
    Listener == ssl_listener;
    Listener == udp_listener;
    Listener == dtls_listener;
    Listener == ws_listener;
    Listener == wss_listener
->
    Type =
        case Listener of
            tcp_listener -> tcp;
            ssl_listener -> ssl;
            udp_listener -> udp;
            dtls_listener -> dtls;
            ws_listener -> ws;
            wss_listener -> wss
        end,
    [
        {id,
            mk(
                binary(),
                #{
                    desc => ?DESC(gateway_listener_id)
                }
            )},
        {type,
            mk(
                Type,
                #{desc => ?DESC(gateway_listener_type)}
            )},
        {name,
            mk(
                binary(),
                #{desc => ?DESC(gateway_listener_name)}
            )},
        {running,
            mk(
                boolean(),
                #{
                    desc => ?DESC(gateway_listener_running)
                }
            )}
    ] ++ emqx_gateway_schema:fields(Listener);
fields(gateway_stats) ->
    [{key, mk(binary(), #{})}].

schema_load_or_update_gateways_conf() ->
    Names = emqx_gateway_schema:gateway_names(),
    emqx_dashboard_swagger:schema_with_examples(
        hoconsc:union(
            [
                ref(?MODULE, Name)
             || Name <-
                    Names ++
                        [
                            erlang:list_to_existing_atom("update_" ++ erlang:atom_to_list(Name))
                         || Name <- Names
                        ]
            ]
        ),
        examples_update_gateway_confs()
    ).

schema_gateways_conf() ->
    emqx_dashboard_swagger:schema_with_examples(
        hoconsc:union(
            [
                ref(?MODULE, Name)
             || Name <- emqx_gateway_schema:gateway_names()
            ]
        ),
        examples_gateway_confs()
    ).

convert_listener_struct(Schema) ->
    {value, {listeners, #{type := Type}}, Schema1} = lists:keytake(listeners, 1, Schema),
    ListenerSchema = hoconsc:mk(
        listeners_schema(Type),
        #{required => {false, recursively}}
    ),
    lists:keystore(listeners, 1, Schema1, {listeners, ListenerSchema}).

remove_listener_and_authn(Schema) ->
    lists:keydelete(
        authentication,
        1,
        lists:keydelete(listeners, 1, Schema)
    ).

listeners_schema(?R_REF(_Mod, tcp_listeners)) ->
    hoconsc:array(hoconsc:union([ref(tcp_listener), ref(ssl_listener)]));
listeners_schema(?R_REF(_Mod, udp_listeners)) ->
    hoconsc:array(hoconsc:union([ref(udp_listener), ref(dtls_listener)]));
listeners_schema(?R_REF(_Mod, tcp_udp_listeners)) ->
    hoconsc:array(
        hoconsc:union([
            ref(tcp_listener),
            ref(ssl_listener),
            ref(udp_listener),
            ref(dtls_listener)
        ])
    );
listeners_schema(?R_REF(_Mod, ws_listeners)) ->
    hoconsc:array(hoconsc:union([ref(ws_listener), ref(wss_listener)])).

listener_schema() ->
    hoconsc:union([
        ref(?MODULE, tcp_listener),
        ref(?MODULE, ssl_listener),
        ref(?MODULE, udp_listener),
        ref(?MODULE, dtls_listener),
        ref(?MODULE, ws_listener),
        ref(?MODULE, wss_listener)
    ]).

%%--------------------------------------------------------------------
%% examples

examples_gateway_overview() ->
    [
        #{
            name => <<"coap">>,
            status => <<"unloaded">>
        },
        #{
            name => <<"exproto">>,
            status => <<"unloaded">>
        },
        #{
            name => <<"lwm2m">>,
            status => <<"running">>,
            current_connections => 0,
            max_connections => 1024000,
            listeners =>
                [
                    #{
                        id => <<"lwm2m:udp:default">>,
                        type => <<"udp">>,
                        name => <<"default">>,
                        running => true
                    }
                ],
            created_at => <<"2021-12-08T14:41:26.171+08:00">>,
            started_at => <<"2021-12-08T14:41:26.202+08:00">>,
            node_status => [
                #{
                    node => <<"node@127.0.0.1">>,
                    status => <<"running">>,
                    current_connections => 0,
                    max_connections => 1024000
                }
            ]
        },
        #{
            name => <<"mqttsn">>,
            status => <<"stopped">>,
            current_connections => 0,
            max_connections => 1024000,
            listeners =>
                [
                    #{
                        id => <<"mqttsn:udp:default">>,
                        name => <<"default">>,
                        running => false,
                        type => <<"udp">>
                    }
                ],
            created_at => <<"2021-12-08T14:41:45.071+08:00">>,
            stopped_at => <<"2021-12-08T14:56:35.576+08:00">>,
            node_status => [
                #{
                    node => <<"node@127.0.0.1">>,
                    status => <<"running">>,
                    current_connections => 0,
                    max_connections => 1024000
                }
            ]
        },
        #{
            name => <<"stomp">>,
            status => <<"running">>,
            current_connections => 0,
            max_connections => 1024000,
            listeners =>
                [
                    #{
                        id => <<"stomp:tcp:default">>,
                        name => <<"default">>,
                        running => true,
                        type => <<"tcp">>
                    }
                ],
            created_at => <<"2021-12-08T14:42:15.272+08:00">>,
            started_at => <<"2021-12-08T14:42:15.274+08:00">>,
            node_status => [
                #{
                    node => <<"node@127.0.0.1">>,
                    status => <<"running">>,
                    current_connections => 0,
                    max_connections => 1024000
                }
            ]
        }
    ].

examples_gateway_confs() ->
    #{
        stomp_gateway =>
            #{
                summary => <<"A simple STOMP gateway config">>,
                value =>
                    #{
                        enable => true,
                        name => <<"stomp">>,
                        enable_stats => true,
                        idle_timeout => <<"30s">>,
                        mountpoint => <<"stomp/">>,
                        frame =>
                            #{
                                max_headers => 10,
                                max_headers_length => 1024,
                                max_body_length => 65535
                            },
                        listeners =>
                            [
                                #{
                                    type => <<"tcp">>,
                                    name => <<"default">>,
                                    bind => <<"61613">>,
                                    max_connections => 1024000,
                                    max_conn_rate => 1000
                                }
                            ]
                    }
            },
        mqttsn_gateway =>
            #{
                summary => <<"A simple MQTT-SN gateway config">>,
                value =>
                    #{
                        enable => true,
                        name => <<"mqttsn">>,
                        enable_stats => true,
                        idle_timeout => <<"30s">>,
                        mountpoint => <<"mqttsn/">>,
                        gateway_id => 1,
                        broadcast => true,
                        enable_qos3 => true,
                        predefined =>
                            [
                                #{
                                    id => <<"1001">>,
                                    topic => <<"pred/1001">>
                                },
                                #{
                                    id => <<"1002">>,
                                    topic => <<"pred/1002">>
                                }
                            ],
                        listeners =>
                            [
                                #{
                                    type => <<"udp">>,
                                    name => <<"default">>,
                                    bind => <<"1884">>,
                                    max_connections => 1024000,
                                    max_conn_rate => 1000
                                }
                            ]
                    }
            },
        coap_gateway =>
            #{
                summary => <<"A simple CoAP gateway config">>,
                value =>
                    #{
                        enable => true,
                        name => <<"coap">>,
                        enable_stats => true,
                        idle_timeout => <<"30s">>,
                        mountpoint => <<"coap/">>,
                        heartbeat => <<"30s">>,
                        connection_required => false,
                        notify_type => <<"qos">>,
                        subscribe_qos => <<"coap">>,
                        publish_qos => <<"coap">>,
                        listeners =>
                            [
                                #{
                                    type => <<"udp">>,
                                    name => <<"default">>,
                                    bind => <<"5683">>,
                                    max_connections => 1024000,
                                    max_conn_rate => 1000
                                }
                            ]
                    }
            },
        lwm2m_gateway =>
            #{
                summary => <<"A simple LwM2M gateway config">>,
                value =>
                    #{
                        enable => true,
                        name => <<"lwm2m">>,
                        enable_stats => true,
                        idle_timeout => <<"30s">>,
                        mountpoint => <<"lwm2m/">>,
                        xml_dir => <<"etc/lwm2m_xml">>,
                        lifetime_min => <<"1s">>,
                        lifetime_max => <<"86400s">>,
                        qmode_time_window => <<"22s">>,
                        auto_observe => false,
                        update_msg_publish_condition => <<"always">>,
                        translators =>
                            #{
                                command => #{topic => <<"dn/#">>},
                                response => #{topic => <<"up/resp">>},
                                notify => #{topic => <<"up/notify">>},
                                register => #{topic => <<"up/resp">>},
                                update => #{topic => <<"up/resp">>}
                            },
                        listeners =>
                            [
                                #{
                                    type => <<"udp">>,
                                    name => <<"default">>,
                                    bind => <<"5783">>,
                                    max_connections => 1024000,
                                    max_conn_rate => 1000
                                }
                            ]
                    }
            },
        exproto_gateway =>
            #{
                summary => <<"A simple ExProto gateway config">>,
                value =>
                    #{
                        enable => true,
                        name => <<"exproto">>,
                        enable_stats => true,
                        idle_timeout => <<"30s">>,
                        mountpoint => <<"exproto/">>,
                        server =>
                            #{bind => <<"9100">>},
                        handler =>
                            #{address => <<"http://127.0.0.1:9001">>},
                        listeners =>
                            [
                                #{
                                    type => <<"tcp">>,
                                    name => <<"default">>,
                                    bind => <<"7993">>,
                                    max_connections => 1024000,
                                    max_conn_rate => 1000
                                }
                            ]
                    }
            },
        gbt32960_gateway =>
            #{
                summary => <<"A simple GBT32960 gateway config">>,
                value =>
                    #{
                        enable => true,
                        name => <<"gbt32960">>,
                        enable_stats => true,
                        mountpoint => <<"gbt32960/${clientid}">>,
                        retry_interval => <<"8s">>,
                        max_retry_times => 3,
                        message_queue_len => 10,
                        listeners =>
                            [
                                #{
                                    type => <<"tcp">>,
                                    name => <<"default">>,
                                    bind => <<"7325">>,
                                    max_connections => 1024000,
                                    max_conn_rate => 1000
                                }
                            ]
                    }
            },
        ocpp_gateway =>
            #{
                summary => <<"A simple OCPP gateway config">>,
                value =>
                    #{
                        enable => true,
                        name => <<"ocpp">>,
                        enable_stats => true,
                        mountpoint => <<"ocpp/">>,
                        default_heartbeat_interval => <<"60s">>,
                        upstream =>
                            #{
                                topic => <<"cp/${cid}">>,
                                reply_topic => <<"cp/${cid}/reply">>,
                                error_topic => <<"cp/${cid}/error">>
                            },
                        dnstream => #{topic => <<"cp/${cid}">>},
                        message_format_checking => disable,
                        listeners =>
                            [
                                #{
                                    type => <<"ws">>,
                                    name => <<"default">>,
                                    bind => <<"33033">>,
                                    max_connections => 1024000
                                }
                            ]
                    }
            }
    }.

examples_update_gateway_confs() ->
    #{
        stomp_gateway =>
            #{
                summary => <<"A simple STOMP gateway config">>,
                value =>
                    #{
                        enable => true,
                        enable_stats => true,
                        idle_timeout => <<"30s">>,
                        mountpoint => <<"stomp2/">>,
                        frame =>
                            #{
                                max_headers => 100,
                                max_headers_length => 10240,
                                max_body_length => 655350
                            }
                    }
            },
        mqttsn_gateway =>
            #{
                summary => <<"A simple MQTT-SN gateway config">>,
                value =>
                    #{
                        enable => true,
                        enable_stats => true,
                        idle_timeout => <<"30s">>,
                        mountpoint => <<"mqttsn2/">>,
                        gateway_id => 1,
                        broadcast => true,
                        enable_qos3 => false,
                        predefined =>
                            [
                                #{
                                    id => <<"1003">>,
                                    topic => <<"pred/1003">>
                                }
                            ]
                    }
            },
        coap_gateway =>
            #{
                summary => <<"A simple CoAP gateway config">>,
                value =>
                    #{
                        enable => true,
                        enable_stats => true,
                        idle_timeout => <<"30s">>,
                        mountpoint => <<"coap2/">>,
                        heartbeat => <<"30s">>,
                        connection_required => false,
                        notify_type => <<"qos">>,
                        subscribe_qos => <<"coap">>,
                        publish_qos => <<"coap">>
                    }
            },
        lwm2m_gateway =>
            #{
                summary => <<"A simple LwM2M gateway config">>,
                value =>
                    #{
                        enable => true,
                        enable_stats => true,
                        idle_timeout => <<"30s">>,
                        mountpoint => <<"lwm2m2/">>,
                        xml_dir => <<"/etc/emqx/lwm2m_xml">>,
                        lifetime_min => <<"1s">>,
                        lifetime_max => <<"86400s">>,
                        qmode_time_window => <<"22s">>,
                        auto_observe => false,
                        update_msg_publish_condition => <<"always">>,
                        translators =>
                            #{
                                command => #{topic => <<"dn/#">>},
                                response => #{topic => <<"up/resp">>},
                                notify => #{topic => <<"up/notify">>},
                                register => #{topic => <<"up/resp">>},
                                update => #{topic => <<"up/resp">>}
                            }
                    }
            },
        exproto_gateway =>
            #{
                summary => <<"A simple ExProto gateway config">>,
                value =>
                    #{
                        enable => true,
                        enable_stats => true,
                        idle_timeout => <<"30s">>,
                        mountpoint => <<"exproto2/">>,
                        server =>
                            #{bind => <<"9100">>},
                        handler =>
                            #{address => <<"http://127.0.0.1:9001">>}
                    }
            },
        gbt32960_gateway =>
            #{
                summary => <<"A simple GBT32960 gateway config">>,
                value =>
                    #{
                        enable => true,
                        enable_stats => true,
                        mountpoint => <<"gbt32960/${clientid}">>,
                        retry_interval => <<"8s">>,
                        max_retry_times => 3,
                        message_queue_len => 10
                    }
            },
        ocpp_gateway =>
            #{
                summary => <<"A simple OCPP gateway config">>,
                value =>
                    #{
                        enable => true,
                        enable_stats => true,
                        mountpoint => <<"ocpp/">>,
                        default_heartbeat_interval => <<"60s">>,
                        upstream =>
                            #{
                                topic => <<"cp/${cid}">>,
                                reply_topic => <<"cp/${cid}/reply">>,
                                error_topic => <<"cp/${cid}/error">>
                            },
                        dnstream => #{topic => <<"cp/${cid}">>},
                        message_format_checking => disable
                    }
            }
    }.
