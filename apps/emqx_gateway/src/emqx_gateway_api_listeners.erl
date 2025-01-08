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

-module(emqx_gateway_api_listeners).

-behaviour(minirest_api).

-include("emqx_gateway_http.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").

-import(hoconsc, [mk/2, ref/1, ref/2]).

-import(
    emqx_gateway_http,
    [
        return_http_error/2,
        with_gateway/2,
        with_listener_authn/3,
        checks/2
    ]
).

-import(emqx_gateway_api_authn, [schema_authn/0]).

%% minirest/dashboard_swagger behaviour callbacks
-export([
    api_spec/0,
    paths/0,
    schema/1
]).

-export([
    roots/0,
    fields/1,
    namespace/0
]).

%% http handlers
-export([
    listeners/2,
    listeners_insta/2,
    listeners_insta_authn/2,
    users/2,
    users_insta/2
]).

%% RPC
-export([do_listeners_cluster_status/1]).

-define(TAGS, [<<"Gateway Listeners">>]).

%%--------------------------------------------------------------------
%% minirest behaviour callbacks
%%--------------------------------------------------------------------

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/gateways/:name/listeners",
        "/gateways/:name/listeners/:id",
        "/gateways/:name/listeners/:id/authentication",
        "/gateways/:name/listeners/:id/authentication/users",
        "/gateways/:name/listeners/:id/authentication/users/:uid"
    ].

%%--------------------------------------------------------------------
%% http handlers

listeners(get, #{bindings := #{name := Name0}}) ->
    with_gateway(Name0, fun(GwName, _) ->
        Result = lists:map(fun bind2str/1, get_cluster_listeners_info(GwName)),
        {200, Result}
    end);
listeners(post, #{bindings := #{name := Name0}, body := LConf}) ->
    with_gateway(Name0, fun(GwName, Gateway) ->
        RunningConf = maps:get(config, Gateway),
        %% XXX: check params miss? check badly data tpye??
        _ = checks([<<"type">>, <<"name">>, <<"bind">>], LConf),

        Type = binary_to_existing_atom(maps:get(<<"type">>, LConf)),
        LName = binary_to_atom(maps:get(<<"name">>, LConf)),

        Path = [listeners, Type, LName],
        case emqx_utils_maps:deep_get(Path, RunningConf, undefined) of
            undefined ->
                ListenerId = emqx_gateway_utils:listener_id(
                    GwName, Type, LName
                ),
                {ok, RespConf} = emqx_gateway_http:add_listener(
                    ListenerId, LConf
                ),
                {201, RespConf};
            _ ->
                return_http_error(400, "Listener name has occupied")
        end
    end).

listeners_insta(delete, #{bindings := #{name := Name0, id := ListenerId}}) ->
    with_gateway(Name0, fun(_GwName, _) ->
        case emqx_gateway_conf:listener(ListenerId) of
            {ok, _Listener} ->
                ok = emqx_gateway_http:remove_listener(ListenerId),
                {204};
            {error, not_found} ->
                return_http_error(404, "Listener not found")
        end
    end);
listeners_insta(get, #{bindings := #{name := Name0, id := ListenerId}}) ->
    with_gateway(Name0, fun(_GwName, _) ->
        case emqx_gateway_conf:listener(ListenerId) of
            {ok, Listener} ->
                {200, bind2str(Listener)};
            {error, not_found} ->
                return_http_error(404, "Listener not found");
            {error, Reason} ->
                return_http_error(500, Reason)
        end
    end);
listeners_insta(put, #{
    body := LConf,
    bindings := #{name := Name0, id := ListenerId}
}) ->
    with_gateway(Name0, fun(_GwName, _) ->
        {ok, RespConf} = emqx_gateway_http:update_listener(ListenerId, LConf),
        {200, RespConf}
    end).

listeners_insta_authn(get, #{
    bindings := #{
        name := Name0,
        id := ListenerId
    }
}) ->
    with_gateway(Name0, fun(GwName, _) ->
        try emqx_gateway_http:authn(GwName, ListenerId) of
            Authn -> {200, Authn}
        catch
            error:{config_not_found, _} ->
                {204}
        end
    end);
listeners_insta_authn(post, #{
    body := Conf,
    bindings := #{
        name := Name0,
        id := ListenerId
    }
}) ->
    with_gateway(Name0, fun(GwName, _) ->
        {ok, Authn} = emqx_gateway_http:add_authn(GwName, ListenerId, Conf),
        {201, Authn}
    end);
listeners_insta_authn(put, #{
    body := Conf,
    bindings := #{
        name := Name0,
        id := ListenerId
    }
}) ->
    with_gateway(Name0, fun(GwName, _) ->
        {ok, Authn} = emqx_gateway_http:update_authn(
            GwName, ListenerId, Conf
        ),
        {200, Authn}
    end);
listeners_insta_authn(delete, #{
    bindings := #{
        name := Name0,
        id := ListenerId
    }
}) ->
    with_gateway(Name0, fun(GwName, _) ->
        ok = emqx_gateway_http:remove_authn(GwName, ListenerId),
        {204}
    end).

users(get, #{bindings := #{name := Name0, id := Id}, query_string := Qs}) ->
    with_listener_authn(
        Name0,
        Id,
        fun(_GwName, #{id := AuthId, chain_name := ChainName}) ->
            emqx_authn_api:list_users(ChainName, AuthId, page_params(Qs))
        end
    );
users(post, #{
    bindings := #{name := Name0, id := Id},
    body := Body
}) ->
    with_listener_authn(
        Name0,
        Id,
        fun(_GwName, #{id := AuthId, chain_name := ChainName}) ->
            emqx_authn_api:add_user(ChainName, AuthId, Body)
        end
    ).

users_insta(get, #{bindings := #{name := Name0, id := Id, uid := UserId}}) ->
    with_listener_authn(
        Name0,
        Id,
        fun(_GwName, #{id := AuthId, chain_name := ChainName}) ->
            emqx_authn_api:find_user(ChainName, AuthId, UserId)
        end
    );
users_insta(put, #{
    bindings := #{name := Name0, id := Id, uid := UserId},
    body := Body
}) ->
    with_listener_authn(
        Name0,
        Id,
        fun(_GwName, #{id := AuthId, chain_name := ChainName}) ->
            emqx_authn_api:update_user(ChainName, AuthId, UserId, Body)
        end
    );
users_insta(delete, #{bindings := #{name := Name0, id := Id, uid := UserId}}) ->
    with_listener_authn(
        Name0,
        Id,
        fun(_GwName, #{id := AuthId, chain_name := ChainName}) ->
            emqx_authn_api:delete_user(ChainName, AuthId, UserId)
        end
    ).

%%--------------------------------------------------------------------
%% Utils

page_params(Qs) ->
    maps:with([<<"page">>, <<"limit">>], Qs).

get_cluster_listeners_info(GwName) ->
    Listeners = emqx_gateway_conf:listeners(GwName),
    ListenOns = lists:map(
        fun(#{id := Id, type := Type0} = Conf) ->
            Type = binary_to_existing_atom(Type0),
            ListenOn = emqx_gateway_conf:get_bind(Conf),
            {Type, Id, ListenOn}
        end,
        Listeners
    ),

    ClusterStatus = listeners_cluster_status(ListenOns),

    lists:map(
        fun(#{id := Id} = Listener) ->
            NodeStatus = lists:foldl(
                fun(Info, Acc) ->
                    Status = maps:get(Id, Info),
                    [Status | Acc]
                end,
                [],
                ClusterStatus
            ),

            {MaxCons, CurrCons, Running} = aggregate_listener_status(NodeStatus),

            Listener#{
                status => #{
                    running => Running,
                    max_connections => MaxCons,
                    current_connections => CurrCons
                },
                node_status => NodeStatus
            }
        end,
        Listeners
    ).

listeners_cluster_status(Listeners) ->
    Nodes = mria:running_nodes(),
    case emqx_gateway_api_listeners_proto_v1:listeners_cluster_status(Nodes, Listeners) of
        {Results, []} ->
            Results;
        {_, _BadNodes} ->
            error(badrpc)
    end.

do_listeners_cluster_status(Listeners) ->
    Node = node(),
    lists:foldl(
        fun({Type, Id, ListenOn}, Acc) ->
            {Running, Curr} = current_listener_status(Type, Id, ListenOn),
            {ok, #{<<"max_connections">> := Max}} = emqx_gateway_conf:listener(
                erlang:atom_to_binary(Id)
            ),
            Acc#{
                Id => #{
                    node => Node,
                    status => #{
                        running => Running,
                        current_connections => Curr,
                        max_connections => ensure_integer_or_infinity(Max)
                    }
                }
            }
        end,
        #{},
        Listeners
    ).

current_listener_status(Type, Id, _ListenOn) when Type =:= ws; Type =:= wss ->
    Info = ranch:info(Id),
    Conns = proplists:get_value(all_connections, Info, 0),
    Running =
        case proplists:get_value(status, Info) of
            running -> true;
            _ -> false
        end,
    {Running, Conns};
current_listener_status(_Type, Id, ListenOn) ->
    try esockd:get_current_connections({Id, ListenOn}) of
        Int -> {true, Int}
    catch
        %% not started
        error:not_found ->
            {false, 0}
    end.

ensure_integer_or_infinity(infinity) ->
    infinity;
ensure_integer_or_infinity(<<"infinity">>) ->
    infinity;
ensure_integer_or_infinity(B) when is_binary(B) ->
    binary_to_integer(B);
ensure_integer_or_infinity(I) when is_integer(I) ->
    I.

aggregate_listener_status(NodeStatus) ->
    aggregate_listener_status(NodeStatus, 0, 0, undefined).

aggregate_listener_status(
    [
        #{status := #{running := Running, max_connections := Max, current_connections := Current}}
        | T
    ],
    MaxAcc,
    CurrAcc,
    RunningAcc
) ->
    NMaxAcc = emqx_gateway_utils:plus_max_connections(MaxAcc, Max),
    NRunning = aggregate_running(Running, RunningAcc),
    aggregate_listener_status(T, NMaxAcc, Current + CurrAcc, NRunning);
aggregate_listener_status([], MaxAcc, CurrAcc, RunningAcc) ->
    {MaxAcc, CurrAcc, RunningAcc}.

aggregate_running(R, R) -> R;
aggregate_running(R, undefined) -> R;
aggregate_running(_, _) -> inconsistent.

bind2str(Listener = #{bind := Bind}) ->
    Listener#{bind := iolist_to_binary(emqx_listeners:format_bind(Bind))};
bind2str(Listener = #{<<"bind">> := Bind}) ->
    Listener#{<<"bind">> := iolist_to_binary(emqx_listeners:format_bind(Bind))}.

%%--------------------------------------------------------------------
%% Swagger defines
%%--------------------------------------------------------------------

schema("/gateways/:name/listeners") ->
    #{
        'operationId' => listeners,
        get =>
            #{
                tags => ?TAGS,
                desc => ?DESC(list_listeners),
                summary => <<"List all listeners">>,
                parameters => params_gateway_name_in_path(),
                responses =>
                    ?STANDARD_RESP(
                        #{
                            200 => emqx_dashboard_swagger:schema_with_example(
                                hoconsc:array(listener_node_status_schema()),
                                examples_listener_list()
                            )
                        }
                    )
            },
        post =>
            #{
                tags => ?TAGS,
                desc => ?DESC(add_listener),
                summary => <<"Add listener">>,
                parameters => params_gateway_name_in_path(),
                %% XXX: How to distinguish the different listener supported by
                %% different types of gateways?
                'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                    emqx_gateway_api:listener_schema(),
                    examples_listener()
                ),
                responses =>
                    ?STANDARD_RESP(
                        #{
                            201 => emqx_dashboard_swagger:schema_with_examples(
                                emqx_gateway_api:listener_schema(),
                                examples_listener()
                            )
                        }
                    )
            }
    };
schema("/gateways/:name/listeners/:id") ->
    #{
        'operationId' => listeners_insta,
        get =>
            #{
                tags => ?TAGS,
                desc => ?DESC(get_listener),
                summary => <<"Get listener config">>,
                parameters => params_gateway_name_in_path() ++
                    params_listener_id_in_path(),
                responses =>
                    ?STANDARD_RESP(
                        #{
                            200 => emqx_dashboard_swagger:schema_with_examples(
                                emqx_gateway_api:listener_schema(),
                                examples_listener()
                            )
                        }
                    )
            },
        delete =>
            #{
                tags => ?TAGS,
                desc => ?DESC(delete_listener),
                summary => <<"Delete listener">>,
                parameters => params_gateway_name_in_path() ++
                    params_listener_id_in_path(),
                responses =>
                    ?STANDARD_RESP(#{204 => <<"Deleted">>})
            },
        put =>
            #{
                tags => ?TAGS,
                desc => ?DESC(update_listener),
                summary => <<"Update listener config">>,
                parameters => params_gateway_name_in_path() ++
                    params_listener_id_in_path(),
                'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                    emqx_gateway_api:listener_schema(),
                    examples_listener()
                ),
                responses =>
                    ?STANDARD_RESP(
                        #{
                            200 => emqx_dashboard_swagger:schema_with_examples(
                                emqx_gateway_api:listener_schema(),
                                examples_listener()
                            )
                        }
                    )
            }
    };
schema("/gateways/:name/listeners/:id/authentication") ->
    #{
        'operationId' => listeners_insta_authn,
        get =>
            #{
                tags => ?TAGS,
                desc => ?DESC(get_listener_authn),
                summary => <<"Get the listener's authenticator">>,
                parameters => params_gateway_name_in_path() ++
                    params_listener_id_in_path(),
                responses =>
                    ?STANDARD_RESP(
                        #{
                            200 => schema_authn(),
                            204 => <<"Authentication or listener does not existed">>
                        }
                    )
            },
        post =>
            #{
                tags => ?TAGS,
                desc => ?DESC(add_listener_authn),
                summary => <<"Create authenticator for listener">>,
                parameters => params_gateway_name_in_path() ++
                    params_listener_id_in_path(),
                'requestBody' => schema_authn(),
                responses =>
                    ?STANDARD_RESP(#{201 => schema_authn()})
            },
        put =>
            #{
                tags => ?TAGS,
                desc => ?DESC(update_listener_authn),
                summary => <<"Update config of authenticator for listener">>,
                parameters => params_gateway_name_in_path() ++
                    params_listener_id_in_path(),
                'requestBody' => schema_authn(),
                responses =>
                    ?STANDARD_RESP(#{200 => schema_authn()})
            },
        delete =>
            #{
                tags => ?TAGS,
                desc => ?DESC(delete_listener_authn),
                summary => <<"Delete the listener's authenticator">>,
                parameters => params_gateway_name_in_path() ++
                    params_listener_id_in_path(),
                responses =>
                    ?STANDARD_RESP(#{200 => <<"Deleted">>})
            }
    };
schema("/gateways/:name/listeners/:id/authentication/users") ->
    #{
        'operationId' => users,
        get =>
            #{
                tags => ?TAGS,
                desc => ?DESC(list_users),
                summary => <<"List authenticator's users">>,
                parameters => params_gateway_name_in_path() ++
                    params_listener_id_in_path() ++
                    params_paging_in_qs(),
                responses =>
                    ?STANDARD_RESP(
                        #{
                            200 => emqx_dashboard_swagger:schema_with_example(
                                ref(emqx_authn_api, response_user),
                                emqx_authn_api:response_user_examples()
                            )
                        }
                    )
            },
        post =>
            #{
                tags => ?TAGS,
                desc => ?DESC(add_user),
                summary => <<"Add user for an authenticator">>,
                parameters => params_gateway_name_in_path() ++
                    params_listener_id_in_path(),
                'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                    ref(emqx_authn_api, request_user_create),
                    emqx_authn_api:request_user_create_examples()
                ),
                responses =>
                    ?STANDARD_RESP(
                        #{
                            201 => emqx_dashboard_swagger:schema_with_example(
                                ref(emqx_authn_api, response_user),
                                emqx_authn_api:response_user_examples()
                            )
                        }
                    )
            }
    };
schema("/gateways/:name/listeners/:id/authentication/users/:uid") ->
    #{
        'operationId' => users_insta,
        get =>
            #{
                tags => ?TAGS,
                desc => ?DESC(get_user),
                summary => <<"Get user info">>,
                parameters => params_gateway_name_in_path() ++
                    params_listener_id_in_path() ++
                    params_userid_in_path(),
                responses =>
                    ?STANDARD_RESP(
                        #{
                            200 => emqx_dashboard_swagger:schema_with_example(
                                ref(emqx_authn_api, response_user),
                                emqx_authn_api:response_user_examples()
                            )
                        }
                    )
            },
        put =>
            #{
                tags => ?TAGS,
                desc => ?DESC(update_user),
                summary => <<"Update user info">>,
                parameters => params_gateway_name_in_path() ++
                    params_listener_id_in_path() ++
                    params_userid_in_path(),
                'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                    ref(emqx_authn_api, request_user_update),
                    emqx_authn_api:request_user_update_examples()
                ),
                responses =>
                    ?STANDARD_RESP(
                        #{
                            200 => emqx_dashboard_swagger:schema_with_example(
                                ref(emqx_authn_api, response_user),
                                emqx_authn_api:response_user_examples()
                            )
                        }
                    )
            },
        delete =>
            #{
                tags => ?TAGS,
                desc => ?DESC(delete_user),
                summary => <<"Delete user">>,
                parameters => params_gateway_name_in_path() ++
                    params_listener_id_in_path() ++
                    params_userid_in_path(),
                responses =>
                    ?STANDARD_RESP(#{204 => <<"Deleted">>})
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
                    desc => ?DESC(emqx_gateway_api, gateway_name_in_qs),
                    example => <<"stomp">>
                }
            )}
    ].

params_listener_id_in_path() ->
    [
        {id,
            mk(
                binary(),
                #{
                    in => path,
                    desc => ?DESC(listener_id),
                    example => <<"">>
                }
            )}
    ].

params_userid_in_path() ->
    [
        {uid,
            mk(
                binary(),
                #{
                    in => path,
                    desc => ?DESC(emqx_gateway_api_authn, user_id),
                    example => <<"">>
                }
            )}
    ].

params_paging_in_qs() ->
    emqx_dashboard_swagger:fields(page) ++
        emqx_dashboard_swagger:fields(limit).

%%--------------------------------------------------------------------
%% schemas

namespace() ->
    undefined.

roots() ->
    [listener].

fields(listener_status) ->
    [
        {status,
            mk(ref(emqx_mgmt_api_listeners, status), #{
                desc => ?DESC(listener_status)
            })},
        {node_status,
            mk(hoconsc:array(ref(emqx_mgmt_api_listeners, node_status)), #{
                desc => ?DESC(listener_node_status)
            })}
    ];
fields(tcp_listener) ->
    emqx_gateway_api:fields(tcp_listener) ++ fields(listener_status);
fields(ssl_listener) ->
    emqx_gateway_api:fields(ssl_listener) ++ fields(listener_status);
fields(udp_listener) ->
    emqx_gateway_api:fields(udp_listener) ++ fields(listener_status);
fields(dtls_listener) ->
    emqx_gateway_api:fields(dtls_listener) ++ fields(listener_status);
fields(_) ->
    [].

listener_node_status_schema() ->
    hoconsc:union([
        ref(tcp_listener),
        ref(ssl_listener),
        ref(udp_listener),
        ref(dtls_listener)
    ]).

%%--------------------------------------------------------------------
%% examples

examples_listener_list() ->
    Convert = fun(Cfg) ->
        Cfg#{
            status => #{
                running => true,
                max_connections => 1024000,
                current_connections => 10
            },
            node_status => [
                #{
                    node => <<"emqx@127.0.0.1">>,
                    status => #{
                        running => true,
                        current_connections => 10,
                        max_connections => 1024000
                    }
                }
            ]
        }
    end,
    [Convert(Config) || #{value := Config} <- maps:values(examples_listener())].

examples_listener() ->
    #{
        tcp_listener =>
            #{
                summary => <<"A simple TCP listener example">>,
                value =>
                    #{
                        name => <<"tcp-def">>,
                        type => <<"tcp">>,
                        bind => <<"22210">>,
                        acceptors => 16,
                        max_connections => 1024000,
                        max_conn_rate => 1000,
                        tcp_options =>
                            #{
                                active_n => 100,
                                backlog => 1024,
                                send_timeout => <<"15s">>,
                                send_timeout_close => true,
                                recbuf => <<"10KB">>,
                                sndbuf => <<"10KB">>,
                                buffer => <<"10KB">>,
                                high_watermark => <<"1MB">>,
                                nodelay => false,
                                reuseaddr => true,
                                keepalive => "none"
                            }
                    }
            },
        ssl_listener =>
            #{
                summary => <<"A simple SSL listener example">>,
                value =>
                    #{
                        name => <<"ssl-def">>,
                        type => <<"ssl">>,
                        bind => <<"22211">>,
                        acceptors => 16,
                        max_connections => 1024000,
                        max_conn_rate => 1000,
                        access_rules => [<<"allow all">>],
                        ssl_options =>
                            #{
                                versions => [
                                    <<"tlsv1.3">>,
                                    <<"tlsv1.2">>,
                                    <<"tlsv1.1">>,
                                    <<"tlsv1">>
                                ],
                                cacertfile => <<"${EMQX_ETC_DIR}/certs/cacert.pem">>,
                                certfile => <<"${EMQX_ETC_DIR}/certs/cert.pem">>,
                                keyfile => <<"${EMQX_ETC_DIR}/certs/key.pem">>,
                                verify => <<"verify_none">>,
                                fail_if_no_peer_cert => false
                            },
                        tcp_options =>
                            #{
                                active_n => 100,
                                backlog => 1024
                            }
                    }
            },
        udp_listener =>
            #{
                summary => <<"A simple UDP listener example">>,
                value =>
                    #{
                        name => <<"udp-def">>,
                        type => udp,
                        bind => <<"22212">>,
                        udp_options =>
                            #{
                                active_n => 100,
                                recbuf => <<"10KB">>,
                                sndbuf => <<"10KB">>,
                                buffer => <<"10KB">>,
                                reuseaddr => true
                            }
                    }
            },
        dtls_listener =>
            #{
                summary => <<"A simple DTLS listener example">>,
                value =>
                    #{
                        name => <<"dtls-def">>,
                        type => <<"dtls">>,
                        bind => <<"22213">>,
                        acceptors => 16,
                        max_connections => 1024000,
                        max_conn_rate => 1000,
                        access_rules => [<<"allow all">>],
                        dtls_options =>
                            #{
                                versions => [<<"dtlsv1.2">>, <<"dtlsv1">>],
                                cacertfile => <<"${EMQX_ETC_DIR}/certs/cacert.pem">>,
                                certfile => <<"${EMQX_ETC_DIR}/certs/cert.pem">>,
                                keyfile => <<"${EMQX_ETC_DIR}/certs/key.pem">>,
                                verify => <<"verify_none">>,
                                fail_if_no_peer_cert => false
                            },
                        udp_options =>
                            #{
                                active_n => 100,
                                backlog => 1024
                            }
                    }
            },
        dtls_listener_with_psk_ciphers =>
            #{
                summary => <<"A DTLS listener with PSK example">>,
                value =>
                    #{
                        name => <<"dtls-psk">>,
                        type => <<"dtls">>,
                        bind => <<"22214">>,
                        acceptors => 16,
                        max_connections => 1024000,
                        max_conn_rate => 1000,
                        dtls_options =>
                            #{
                                versions => [<<"dtlsv1.2">>, <<"dtlsv1">>],
                                cacertfile => <<"${EMQX_ETC_DIR}/certs/cacert.pem">>,
                                certfile => <<"${EMQX_ETC_DIR}/certs/cert.pem">>,
                                keyfile => <<"${EMQX_ETC_DIR}/certs/key.pem">>,
                                verify => <<"verify_none">>,
                                user_lookup_fun => <<"emqx_tls_psk:lookup">>,
                                ciphers =>
                                    <<
                                        "RSA-PSK-AES256-GCM-SHA384,RSA-PSK-AES256-CBC-SHA384,RSA-PSK-AES128-GCM-SHA256,"
                                        "RSA-PSK-AES128-CBC-SHA256,RSA-PSK-AES256-CBC-SHA,RSA-PSK-AES128-CBC-SHA"
                                    >>,
                                fail_if_no_peer_cert => false
                            }
                    }
            },
        lisetner_with_authn =>
            #{
                summary => <<"A TCP listener with authentication example">>,
                value =>
                    #{
                        name => <<"tcp-with-authn">>,
                        type => <<"tcp">>,
                        bind => <<"22215">>,
                        acceptors => 16,
                        max_connections => 1024000,
                        max_conn_rate => 1000,
                        authentication =>
                            #{
                                backend => <<"built_in_database">>,
                                mechanism => <<"password_based">>,
                                password_hash_algorithm =>
                                    #{name => <<"sha256">>},
                                user_id_type => <<"username">>
                            }
                    }
            },
        ws_listener =>
            #{
                summary => <<"A simple WebSocket listener example">>,
                value =>
                    #{
                        name => <<"ws-def">>,
                        type => <<"ws">>,
                        bind => <<"33043">>,
                        acceptors => 16,
                        max_connections => 1024000,
                        max_conn_rate => 1000,
                        websocket =>
                            #{
                                path => <<"/ocpp">>,
                                fail_if_no_subprotocol => true,
                                supported_subprotocols => <<"ocpp1.6">>,
                                check_origin_enable => false,
                                check_origins =>
                                    <<"http://localhost:18083, http://127.0.0.1:18083">>,
                                compress => false,
                                piggyback => <<"single">>
                            },
                        tcp_options =>
                            #{
                                active_n => 100,
                                backlog => 1024,
                                send_timeout => <<"15s">>,
                                send_timeout_close => true,
                                recbuf => <<"10KB">>,
                                sndbuf => <<"10KB">>,
                                buffer => <<"10KB">>,
                                high_watermark => <<"1MB">>,
                                nodelay => false,
                                reuseaddr => true,
                                keepalive => "none"
                            }
                    }
            },
        wss_listener =>
            #{
                summary => <<"A simple WebSocket/TLS listener example">>,
                value =>
                    #{
                        name => <<"ws-ssl-def">>,
                        type => <<"wss">>,
                        bind => <<"33053">>,
                        acceptors => 16,
                        max_connections => 1024000,
                        max_conn_rate => 1000,
                        websocket =>
                            #{
                                path => <<"/ocpp">>,
                                fail_if_no_subprotocol => true,
                                supported_subprotocols => <<"ocpp1.6">>,
                                check_origin_enable => false,
                                check_origins =>
                                    <<"http://localhost:18083, http://127.0.0.1:18083">>,
                                compress => false,
                                piggyback => <<"single">>
                            },
                        ssl_options =>
                            #{
                                versions => [
                                    <<"tlsv1.3">>,
                                    <<"tlsv1.2">>,
                                    <<"tlsv1.1">>,
                                    <<"tlsv1">>
                                ],
                                cacertfile => <<"${EMQX_ETC_DIR}/certs/cacert.pem">>,
                                certfile => <<"${EMQX_ETC_DIR}/certs/cert.pem">>,
                                keyfile => <<"${EMQX_ETC_DIR}/certs/key.pem">>,
                                verify => <<"verify_none">>,
                                fail_if_no_peer_cert => false
                            },
                        tcp_options =>
                            #{
                                active_n => 100,
                                backlog => 1024,
                                send_timeout => <<"15s">>,
                                send_timeout_close => true,
                                recbuf => <<"10KB">>,
                                sndbuf => <<"10KB">>,
                                buffer => <<"10KB">>,
                                high_watermark => <<"1MB">>,
                                nodelay => false,
                                reuseaddr => true,
                                keepalive => "none"
                            }
                    }
            }
    }.
