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
    users_insta/2,
    import_users/2
]).

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
        "/gateway/:name/listeners/:id/authentication/users/:uid",
        "/gateway/:name/listeners/:id/authentication/import_users"
    ].

%%--------------------------------------------------------------------
%% http handlers

listeners(get, #{bindings := #{name := Name0}}) ->
    with_gateway(Name0, fun(GwName, _) ->
        {200, emqx_gateway_conf:listeners(GwName)}
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
            emqx_authn_api:list_users(ChainName, AuthId, page_pramas(Qs))
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

import_users(post, #{
    bindings := #{name := Name0, id := Id},
    body := Body
}) ->
    with_listener_authn(
        Name0,
        Id,
        fun(_GwName, #{id := AuthId, chain_name := ChainName}) ->
            case maps:get(<<"filename">>, Body, undefined) of
                undefined ->
                    emqx_authn_api:serialize_error({missing_parameter, filename});
                Filename ->
                    case
                        emqx_authentication:import_users(
                            ChainName, AuthId, Filename
                        )
                    of
                        ok -> {204};
                        {error, Reason} -> emqx_authn_api:serialize_error(Reason)
                    end
            end
        end
    ).

%%--------------------------------------------------------------------
%% Utils

page_pramas(Qs) ->
    maps:with([<<"page">>, <<"limit">>], Qs).

%%--------------------------------------------------------------------
%% Swagger defines
%%--------------------------------------------------------------------

schema("/gateway/:name/listeners") ->
    #{
        'operationId' => listeners,
        get =>
            #{
                desc => <<"Get the gateway listeners">>,
                parameters => params_gateway_name_in_path(),
                responses =>
                    ?STANDARD_RESP(
                        #{
                            200 => emqx_dashboard_swagger:schema_with_example(
                                hoconsc:array(ref(listener)),
                                examples_listener_list()
                            )
                        }
                    )
            },
        post =>
            #{
                desc => <<"Create the gateway listener">>,
                parameters => params_gateway_name_in_path(),
                %% XXX: How to distinguish the different listener supported by
                %% different types of gateways?
                'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                    ref(listener),
                    examples_listener()
                ),
                responses =>
                    ?STANDARD_RESP(
                        #{
                            201 => emqx_dashboard_swagger:schema_with_examples(
                                ref(listener),
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
                desc => <<"Get the gateway listener configurations">>,
                parameters => params_gateway_name_in_path() ++
                    params_listener_id_in_path(),
                responses =>
                    ?STANDARD_RESP(
                        #{
                            200 => emqx_dashboard_swagger:schema_with_examples(
                                ref(listener),
                                examples_listener()
                            )
                        }
                    )
            },
        delete =>
            #{
                desc => <<"Delete the gateway listener">>,
                parameters => params_gateway_name_in_path() ++
                    params_listener_id_in_path(),
                responses =>
                    ?STANDARD_RESP(#{204 => <<"Deleted">>})
            },
        put =>
            #{
                desc => <<"Update the gateway listener">>,
                parameters => params_gateway_name_in_path() ++
                    params_listener_id_in_path(),
                'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                    ref(listener),
                    examples_listener()
                ),
                responses =>
                    ?STANDARD_RESP(
                        #{
                            200 => emqx_dashboard_swagger:schema_with_examples(
                                ref(listener),
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
                desc => <<"Get the listener's authentication info">>,
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
                desc => <<"Add authentication for the listener">>,
                parameters => params_gateway_name_in_path() ++
                    params_listener_id_in_path(),
                'requestBody' => schema_authn(),
                responses =>
                    ?STANDARD_RESP(#{201 => schema_authn()})
            },
        put =>
            #{
                desc => <<"Update authentication for the listener">>,
                parameters => params_gateway_name_in_path() ++
                    params_listener_id_in_path(),
                'requestBody' => schema_authn(),
                responses =>
                    ?STANDARD_RESP(#{200 => schema_authn()})
            },
        delete =>
            #{
                desc => <<"Remove authentication for the listener">>,
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
                desc => <<"Get the users for the authentication">>,
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
                desc => <<"Add user for the authentication">>,
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
                desc => <<
                    "Get user info from the gateway "
                    "authentication"
                >>,
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
                desc => <<
                    "Update the user info for the gateway "
                    "authentication"
                >>,
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
                desc => <<
                    "Delete the user for the gateway "
                    "authentication"
                >>,
                parameters => params_gateway_name_in_path() ++
                    params_listener_id_in_path() ++
                    params_userid_in_path(),
                responses =>
                    ?STANDARD_RESP(#{204 => <<"Deleted">>})
            }
    };
schema("/gateway/:name/listeners/:id/authentication/import_users") ->
    #{
        'operationId' => import_users,
        post =>
            #{
                desc => <<"Import users into the gateway authentication">>,
                parameters => params_gateway_name_in_path() ++
                    params_listener_id_in_path(),
                'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                    ref(emqx_authn_api, request_import_users),
                    emqx_authn_api:request_import_users_examples()
                ),
                responses =>
                    ?STANDARD_RESP(#{204 => <<"Imported">>})
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
                    desc => <<"Gateway Name">>,
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
                    desc => <<"Listener ID">>,
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
                    desc => <<"User ID">>,
                    example => <<"">>
                }
            )}
    ].

params_paging_in_qs() ->
    [
        {page,
            mk(
                integer(),
                #{
                    in => query,
                    required => false,
                    desc => <<"Page Index">>,
                    example => 1
                }
            )},
        {limit,
            mk(
                integer(),
                #{
                    in => query,
                    required => false,
                    desc => <<"Page Limit">>,
                    example => 100
                }
            )}
    ].

%%--------------------------------------------------------------------
%% schemas

roots() ->
    [listener].

fields(listener) ->
    common_listener_opts() ++
        [
            {tcp,
                mk(
                    ref(tcp_listener_opts),
                    #{
                        required => {false, recursively},
                        desc => <<"The tcp socket options for tcp or ssl listener">>
                    }
                )},
            {ssl,
                mk(
                    ref(ssl_listener_opts),
                    #{
                        required => {false, recursively},
                        desc => <<"The ssl socket options for ssl listener">>
                    }
                )},
            {udp,
                mk(
                    ref(udp_listener_opts),
                    #{
                        required => {false, recursively},
                        desc => <<"The udp socket options for udp or dtls listener">>
                    }
                )},
            {dtls,
                mk(
                    ref(dtls_listener_opts),
                    #{
                        required => {false, recursively},
                        desc => <<"The dtls socket options for dtls listener">>
                    }
                )}
        ];
fields(tcp_listener_opts) ->
    [
        {active_n, mk(integer(), #{})},
        {backlog, mk(integer(), #{})},
        {buffer, mk(binary(), #{})},
        {recbuf, mk(binary(), #{})},
        {sndbuf, mk(binary(), #{})},
        {high_watermark, mk(binary(), #{})},
        {nodelay, mk(boolean(), #{})},
        {reuseaddr, boolean()},
        {send_timeout, binary()},
        {send_timeout_close, boolean()}
    ];
fields(ssl_listener_opts) ->
    [
        {cacertfile, binary()},
        {certfile, binary()},
        {keyfile, binary()},
        {verify, binary()},
        {fail_if_no_peer_cert, boolean()},
        {depth, integer()},
        {password, binary()},
        {handshake_timeout, binary()},
        {versions, hoconsc:array(binary())},
        {ciphers, hoconsc:array(binary())},
        {user_lookup_fun, binary()},
        {reuse_sessions, boolean()},
        {secure_renegotiate, boolean()},
        {honor_cipher_order, boolean()},
        {dhfile, binary()}
    ];
fields(udp_listener_opts) ->
    [
        {active_n, integer()},
        {buffer, binary()},
        {recbuf, binary()},
        {sndbuf, binary()},
        {reuseaddr, boolean()}
    ];
fields(dtls_listener_opts) ->
    Ls = lists_key_without(
        [versions, ciphers, handshake_timeout],
        1,
        fields(ssl_listener_opts)
    ),
    [
        {versions, hoconsc:array(binary())},
        {ciphers, hoconsc:array(binary())}
        | Ls
    ].

lists_key_without([], _N, L) ->
    L;
lists_key_without([K | Ks], N, L) ->
    lists_key_without(Ks, N, lists:keydelete(K, N, L)).

common_listener_opts() ->
    [
        {enable,
            mk(
                boolean(),
                #{
                    required => false,
                    desc => <<"Whether to enable this listener">>
                }
            )},
        {id,
            mk(
                binary(),
                #{
                    required => false,
                    desc => <<"Listener Id">>
                }
            )},
        {name,
            mk(
                binary(),
                #{
                    required => false,
                    desc => <<"Listener name">>
                }
            )},
        {type,
            mk(
                hoconsc:enum([tcp, ssl, udp, dtls]),
                #{
                    required => false,
                    desc => <<"Listener type. Enum: tcp, udp, ssl, dtls">>
                }
            )},
        {running,
            mk(
                boolean(),
                #{
                    required => false,
                    desc => <<"Listener running status">>
                }
            )},
        {bind,
            mk(
                binary(),
                #{
                    required => false,
                    desc => <<"Listener bind address or port">>
                }
            )},
        {acceptors,
            mk(
                integer(),
                #{
                    required => false,
                    desc => <<"Listener acceptors number">>
                }
            )},
        {access_rules,
            mk(
                hoconsc:array(binary()),
                #{
                    required => false,
                    desc => <<"Listener Access rules for client">>
                }
            )},
        {max_conn_rate,
            mk(
                integer(),
                #{
                    required => false,
                    desc => <<"Max connection rate for the listener">>
                }
            )},
        {max_connections,
            mk(
                integer(),
                #{
                    required => false,
                    desc => <<"Max connections for the listener">>
                }
            )},
        {mountpoint,
            mk(
                binary(),
                #{
                    required => false,
                    desc =>
                        <<
                            "The Mounpoint for clients of the listener. "
                            "The gateway-level mountpoint configuration can be overloaded "
                            "when it is not null or empty string"
                        >>
                }
            )},
        %% FIXME:
        {authentication,
            mk(
                emqx_authn_schema:authenticator_type(),
                #{
                    required => {false, recursively},
                    desc => <<"The authenticatior for this listener">>
                }
            )}
    ] ++ emqx_gateway_schema:proxy_protocol_opts().

%%--------------------------------------------------------------------
%% examples

examples_listener_list() ->
    [Config || #{value := Config} <- maps:values(examples_listener())].

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
                        tcp =>
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
                        ssl =>
                            #{
                                versions => [
                                    <<"tlsv1.3">>,
                                    <<"tlsv1.2">>,
                                    <<"tlsv1.1">>,
                                    <<"tlsv1">>
                                ],
                                cacertfile => <<"etc/certs/cacert.pem">>,
                                certfile => <<"etc/certs/cert.pem">>,
                                keyfile => <<"etc/certs/key.pem">>,
                                verify => <<"verify_none">>,
                                fail_if_no_peer_cert => false
                            },
                        tcp =>
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
                        udp =>
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
                        dtls =>
                            #{
                                versions => [<<"dtlsv1.2">>, <<"dtlsv1">>],
                                cacertfile => <<"etc/certs/cacert.pem">>,
                                certfile => <<"etc/certs/cert.pem">>,
                                keyfile => <<"etc/certs/key.pem">>,
                                verify => <<"verify_none">>,
                                fail_if_no_peer_cert => false
                            },
                        udp =>
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
                        dtls =>
                            #{
                                versions => [<<"dtlsv1.2">>, <<"dtlsv1">>],
                                cacertfile => <<"etc/certs/cacert.pem">>,
                                certfile => <<"etc/certs/cert.pem">>,
                                keyfile => <<"etc/certs/key.pem">>,
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
