%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    fields/1
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

%%--------------------------------------------------------------------
%% minirest behaviour callbacks
%%--------------------------------------------------------------------

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/gateway/:name/listeners",
        "/gateway/:name/listeners/:id",
        "/gateway/:name/listeners/:id/authentication",
        "/gateway/:name/listeners/:id/authentication/users",
        "/gateway/:name/listeners/:id/authentication/users/:uid"
    ].

%%--------------------------------------------------------------------
%% http handlers

listeners(get, #{bindings := #{name := Name0}}) ->
    with_gateway(Name0, fun(GwName, _) ->
        Result = get_cluster_listeners_info(GwName),
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
        case emqx_map_lib:deep_get(Path, RunningConf, undefined) of
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

listeners_insta(delete, #{bindings := #{name := Name0, id := ListenerId0}}) ->
    ListenerId = emqx_mgmt_util:urldecode(ListenerId0),
    with_gateway(Name0, fun(_GwName, _) ->
        ok = emqx_gateway_http:remove_listener(ListenerId),
        {204}
    end);
listeners_insta(get, #{bindings := #{name := Name0, id := ListenerId0}}) ->
    ListenerId = emqx_mgmt_util:urldecode(ListenerId0),
    with_gateway(Name0, fun(_GwName, _) ->
        case emqx_gateway_conf:listener(ListenerId) of
            {ok, Listener} ->
                {200, Listener};
            {error, not_found} ->
                return_http_error(404, "Listener not found");
            {error, Reason} ->
                return_http_error(500, Reason)
        end
    end);
listeners_insta(put, #{
    body := LConf,
    bindings := #{name := Name0, id := ListenerId0}
}) ->
    ListenerId = emqx_mgmt_util:urldecode(ListenerId0),
    with_gateway(Name0, fun(_GwName, _) ->
        {ok, RespConf} = emqx_gateway_http:update_listener(ListenerId, LConf),
        {200, RespConf}
    end).

listeners_insta_authn(get, #{
    bindings := #{
        name := Name0,
        id := ListenerId0
    }
}) ->
    ListenerId = emqx_mgmt_util:urldecode(ListenerId0),
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
        id := ListenerId0
    }
}) ->
    ListenerId = emqx_mgmt_util:urldecode(ListenerId0),
    with_gateway(Name0, fun(GwName, _) ->
        {ok, Authn} = emqx_gateway_http:add_authn(GwName, ListenerId, Conf),
        {201, Authn}
    end);
listeners_insta_authn(put, #{
    body := Conf,
    bindings := #{
        name := Name0,
        id := ListenerId0
    }
}) ->
    ListenerId = emqx_mgmt_util:urldecode(ListenerId0),
    with_gateway(Name0, fun(GwName, _) ->
        {ok, Authn} = emqx_gateway_http:update_authn(
            GwName, ListenerId, Conf
        ),
        {200, Authn}
    end);
listeners_insta_authn(delete, #{
    bindings := #{
        name := Name0,
        id := ListenerId0
    }
}) ->
    ListenerId = emqx_mgmt_util:urldecode(ListenerId0),
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
        fun(#{id := Id} = Conf) ->
            ListenOn = emqx_gateway_conf:get_bind(Conf),
            {Id, ListenOn}
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

            {MaxCons, CurrCons} = emqx_gateway_http:sum_cluster_connections(NodeStatus),

            Listener#{
                max_connections => MaxCons,
                current_connections => CurrCons,
                node_status => NodeStatus
            }
        end,
        Listeners
    ).

listeners_cluster_status(Listeners) ->
    Nodes = mria_mnesia:running_nodes(),
    case emqx_gateway_api_listeners_proto_v1:listeners_cluster_status(Nodes, Listeners) of
        {Results, []} ->
            Results;
        {_, _BadNodes} ->
            error(badrpc)
    end.

do_listeners_cluster_status(Listeners) ->
    Node = node(),
    lists:foldl(
        fun({Id, ListenOn}, Acc) ->
            BinId = erlang:atom_to_binary(Id),
            {ok, #{<<"max_connections">> := Max}} = emqx_gateway_conf:listener(BinId),
            Curr =
                try esockd:get_current_connections({Id, ListenOn}) of
                    Int -> Int
                catch
                    %% not started
                    error:not_found ->
                        0
                end,
            Acc#{
                Id => #{
                    node => Node,
                    current_connections => Curr,
                    %% XXX: Since it is taken from raw-conf, it is possible a string
                    max_connections => int(Max)
                }
            }
        end,
        #{},
        Listeners
    ).

int(B) when is_binary(B) ->
    binary_to_integer(B);
int(I) when is_integer(I) ->
    I.

%%--------------------------------------------------------------------
%% Swagger defines
%%--------------------------------------------------------------------

schema("/gateway/:name/listeners") ->
    #{
        'operationId' => listeners,
        get =>
            #{
                desc => ?DESC(list_listeners),
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
                desc => ?DESC(add_listener),
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
schema("/gateway/:name/listeners/:id") ->
    #{
        'operationId' => listeners_insta,
        get =>
            #{
                desc => ?DESC(get_listener),
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
                desc => ?DESC(delete_listener),
                parameters => params_gateway_name_in_path() ++
                    params_listener_id_in_path(),
                responses =>
                    ?STANDARD_RESP(#{204 => <<"Deleted">>})
            },
        put =>
            #{
                desc => ?DESC(update_listener),
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
schema("/gateway/:name/listeners/:id/authentication") ->
    #{
        'operationId' => listeners_insta_authn,
        get =>
            #{
                desc => ?DESC(get_listener_authn),
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
                desc => ?DESC(add_listener_authn),
                parameters => params_gateway_name_in_path() ++
                    params_listener_id_in_path(),
                'requestBody' => schema_authn(),
                responses =>
                    ?STANDARD_RESP(#{201 => schema_authn()})
            },
        put =>
            #{
                desc => ?DESC(update_listener_authn),
                parameters => params_gateway_name_in_path() ++
                    params_listener_id_in_path(),
                'requestBody' => schema_authn(),
                responses =>
                    ?STANDARD_RESP(#{200 => schema_authn()})
            },
        delete =>
            #{
                desc => ?DESC(delete_listener_authn),
                parameters => params_gateway_name_in_path() ++
                    params_listener_id_in_path(),
                responses =>
                    ?STANDARD_RESP(#{200 => <<"Deleted">>})
            }
    };
schema("/gateway/:name/listeners/:id/authentication/users") ->
    #{
        'operationId' => users,
        get =>
            #{
                desc => ?DESC(list_users),
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
                desc => ?DESC(add_user),
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
schema("/gateway/:name/listeners/:id/authentication/users/:uid") ->
    #{
        'operationId' => users_insta,
        get =>
            #{
                desc => ?DESC(get_user),
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
                desc => ?DESC(update_user),
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
                desc => ?DESC(delete_user),
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
                binary(),
                #{
                    in => path,
                    desc => ?DESC(emqx_gateway_api, gateway_name),
                    example => <<"">>
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

roots() ->
    [listener].

fields(listener_node_status) ->
    [
        {current_connections, mk(non_neg_integer(), #{desc => ?DESC(current_connections)})},
        {node_status,
            mk(hoconsc:array(ref(emqx_mgmt_api_listeners, node_status)), #{
                desc => ?DESC(listener_node_status)
            })}
    ];
fields(tcp_listener) ->
    emqx_gateway_api:fields(tcp_listener) ++ fields(listener_node_status);
fields(ssl_listener) ->
    emqx_gateway_api:fields(ssl_listener) ++ fields(listener_node_status);
fields(udp_listener) ->
    emqx_gateway_api:fields(udp_listener) ++ fields(listener_node_status);
fields(dtls_listener) ->
    emqx_gateway_api:fields(dtls_listener) ++ fields(listener_node_status);
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
            current_connections => 0,
            node_status => [
                #{
                    node => <<"127.0.0.1">>,
                    current_connections => 0,
                    max_connections => 1024000
                }
            ]
        }
    end,
    [Convert(Config) || #{value := Config} <- maps:values(examples_listener())].

examples_listener() ->
    #{
        tcp_listener =>
            #{
                summary => <<"A simple tcp listener example">>,
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
                                reuseaddr => true
                            }
                    }
            },
        ssl_listener =>
            #{
                summary => <<"A simple ssl listener example">>,
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
                                cacertfile => emqx:cert_file(<<"cacert.pem">>),
                                certfile => emqx:cert_file(<<"cert.pem">>),
                                keyfile => emqx:cert_file(<<"key.pem">>),
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
                summary => <<"A simple udp listener example">>,
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
                summary => <<"A simple dtls listener example">>,
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
                                cacertfile => emqx:cert_file(<<"cacert.pem">>),
                                certfile => emqx:cert_file(<<"cert.pem">>),
                                keyfile => emqx:cert_file(<<"key.pem">>),
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
                summary => <<"A dtls listener with PSK example">>,
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
                                cacertfile => emqx:cert_file(<<"cacert.pem">>),
                                certfile => emqx:cert_file(<<"cert.pem">>),
                                keyfile => emqx:cert_file(<<"key.pem">>),
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
                summary => <<"A tcp listener with authentication example">>,
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
            }
    }.
