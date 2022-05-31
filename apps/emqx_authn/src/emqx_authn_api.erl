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

-module(emqx_authn_api).

-behaviour(minirest_api).

-include("emqx_authn.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_placeholder.hrl").
-include_lib("emqx/include/emqx_authentication.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-import(hoconsc, [mk/2, ref/1, ref/2]).
-import(emqx_dashboard_swagger, [error_codes/2]).

-define(BAD_REQUEST, 'BAD_REQUEST').
-define(NOT_FOUND, 'NOT_FOUND').
-define(ALREADY_EXISTS, 'ALREADY_EXISTS').

% Swagger

-define(API_TAGS_GLOBAL, [
    ?EMQX_AUTHENTICATION_CONFIG_ROOT_NAME_BINARY,
    <<"authentication config(global)">>
]).
-define(API_TAGS_SINGLE, [
    ?EMQX_AUTHENTICATION_CONFIG_ROOT_NAME_BINARY,
    <<"authentication config(single listener)">>
]).

-export([
    api_spec/0,
    paths/0,
    schema/1
]).

-export([
    roots/0,
    fields/1
]).

-export([
    authenticators/2,
    authenticator/2,
    authenticator_status/2,
    listener_authenticators/2,
    listener_authenticator/2,
    listener_authenticator_status/2,
    authenticator_move/2,
    listener_authenticator_move/2,
    authenticator_users/2,
    authenticator_user/2,
    listener_authenticator_users/2,
    listener_authenticator_user/2,
    lookup_from_local_node/2,
    lookup_from_all_nodes/2
]).

-export([
    authenticator_examples/0,
    request_move_examples/0,
    request_user_create_examples/0,
    request_user_update_examples/0,
    response_user_examples/0,
    response_users_example/0
]).

%% export these funcs for gateway
-export([
    list_users/3,
    add_user/3,
    delete_user/3,
    find_user/3,
    update_user/4,
    serialize_error/1,
    aggregate_metrics/1,

    with_chain/2,
    param_auth_id/0,
    param_listener_id/0
]).

-elvis([{elvis_style, god_modules, disable}]).

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/authentication",
        "/authentication/:id",
        "/authentication/:id/status",
        "/authentication/:id/move",
        "/authentication/:id/users",
        "/authentication/:id/users/:user_id",

        "/listeners/:listener_id/authentication",
        "/listeners/:listener_id/authentication/:id",
        "/listeners/:listener_id/authentication/:id/status",
        "/listeners/:listener_id/authentication/:id/move",
        "/listeners/:listener_id/authentication/:id/users",
        "/listeners/:listener_id/authentication/:id/users/:user_id"
    ].

roots() ->
    [
        request_user_create,
        request_user_update,
        request_move,
        response_user,
        response_users
    ].

fields(request_user_create) ->
    [
        {user_id, mk(binary(), #{required => true})}
        | fields(request_user_update)
    ];
fields(request_user_update) ->
    [
        {password, mk(binary(), #{required => true})},
        {is_superuser, mk(boolean(), #{default => false, required => false})}
    ];
fields(request_move) ->
    [{position, mk(binary(), #{required => true})}];
fields(response_user) ->
    [
        {user_id, mk(binary(), #{required => true})},
        {is_superuser, mk(boolean(), #{default => false, required => false})}
    ];
fields(response_users) ->
    paginated_list_type(ref(response_user)).

schema("/authentication") ->
    #{
        'operationId' => authenticators,
        get => #{
            tags => ?API_TAGS_GLOBAL,
            description => ?DESC(authentication_get),
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    hoconsc:array(emqx_authn_schema:authenticator_type()),
                    authenticator_array_example()
                )
            }
        },
        post => #{
            tags => ?API_TAGS_GLOBAL,
            description => ?DESC(authentication_post),
            'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                emqx_authn_schema:authenticator_type(),
                authenticator_examples()
            ),
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_examples(
                    emqx_authn_schema:authenticator_type(),
                    authenticator_examples()
                ),
                400 => error_codes([?BAD_REQUEST], <<"Bad Request">>),
                409 => error_codes([?ALREADY_EXISTS], <<"ALREADY_EXISTS">>)
            }
        }
    };
schema("/authentication/:id") ->
    #{
        'operationId' => authenticator,
        get => #{
            tags => ?API_TAGS_GLOBAL,
            description => ?DESC(authentication_id_get),
            parameters => [param_auth_id()],
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_examples(
                    emqx_authn_schema:authenticator_type(),
                    authenticator_examples()
                ),
                404 => error_codes([?NOT_FOUND], <<"Not Found">>)
            }
        },
        put => #{
            tags => ?API_TAGS_GLOBAL,
            description => ?DESC(authentication_id_put),
            parameters => [param_auth_id()],
            'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                emqx_authn_schema:authenticator_type(),
                authenticator_examples()
            ),
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_examples(
                    emqx_authn_schema:authenticator_type(),
                    authenticator_examples()
                ),
                400 => error_codes([?BAD_REQUEST], <<"Bad Request">>),
                404 => error_codes([?NOT_FOUND], <<"Not Found">>),
                409 => error_codes([?ALREADY_EXISTS], <<"ALREADY_EXISTS">>)
            }
        },
        delete => #{
            tags => ?API_TAGS_GLOBAL,
            description => ?DESC(authentication_id_delete),
            parameters => [param_auth_id()],
            responses => #{
                204 => <<"Authenticator deleted">>,
                404 => error_codes([?NOT_FOUND], <<"Not Found">>)
            }
        }
    };
schema("/authentication/:id/status") ->
    #{
        'operationId' => authenticator_status,
        get => #{
            tags => ?API_TAGS_GLOBAL,
            description => ?DESC(authentication_id_status_get),
            parameters => [param_auth_id()],
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_examples(
                    hoconsc:ref(emqx_authn_schema, "metrics_status_fields"),
                    status_metrics_example()
                ),
                400 => error_codes([?BAD_REQUEST], <<"Bad Request">>)
            }
        }
    };
schema("/listeners/:listener_id/authentication") ->
    #{
        'operationId' => listener_authenticators,
        get => #{
            tags => ?API_TAGS_SINGLE,
            description => ?DESC(listeners_listener_id_authentication_get),
            parameters => [param_listener_id()],
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    hoconsc:array(emqx_authn_schema:authenticator_type()),
                    authenticator_array_example()
                )
            }
        },
        post => #{
            tags => ?API_TAGS_SINGLE,
            description => ?DESC(listeners_listener_id_authentication_post),
            parameters => [param_listener_id()],
            'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                emqx_authn_schema:authenticator_type(),
                authenticator_examples()
            ),
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_examples(
                    emqx_authn_schema:authenticator_type(),
                    authenticator_examples()
                ),
                400 => error_codes([?BAD_REQUEST], <<"Bad Request">>),
                409 => error_codes([?ALREADY_EXISTS], <<"ALREADY_EXISTS">>)
            }
        }
    };
schema("/listeners/:listener_id/authentication/:id") ->
    #{
        'operationId' => listener_authenticator,
        get => #{
            tags => ?API_TAGS_SINGLE,
            description => ?DESC(listeners_listener_id_authentication_id_get),
            parameters => [param_listener_id(), param_auth_id()],
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_examples(
                    emqx_authn_schema:authenticator_type(),
                    authenticator_examples()
                ),
                404 => error_codes([?NOT_FOUND], <<"Not Found">>)
            }
        },
        put => #{
            tags => ?API_TAGS_SINGLE,
            description => ?DESC(listeners_listener_id_authentication_id_put),
            parameters => [param_listener_id(), param_auth_id()],
            'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                emqx_authn_schema:authenticator_type(),
                authenticator_examples()
            ),
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_examples(
                    emqx_authn_schema:authenticator_type(),
                    authenticator_examples()
                ),
                400 => error_codes([?BAD_REQUEST], <<"Bad Request">>),
                404 => error_codes([?NOT_FOUND], <<"Not Found">>),
                409 => error_codes([?ALREADY_EXISTS], <<"ALREADY_EXISTS">>)
            }
        },
        delete => #{
            tags => ?API_TAGS_SINGLE,
            description => ?DESC(listeners_listener_id_authentication_id_delete),
            parameters => [param_listener_id(), param_auth_id()],
            responses => #{
                204 => <<"Authenticator deleted">>,
                404 => error_codes([?NOT_FOUND], <<"Not Found">>)
            }
        }
    };
schema("/listeners/:listener_id/authentication/:id/status") ->
    #{
        'operationId' => listener_authenticator_status,
        get => #{
            tags => ?API_TAGS_SINGLE,
            description => ?DESC(listeners_listener_id_authentication_id_status_get),
            parameters => [param_listener_id(), param_auth_id()],
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_examples(
                    hoconsc:ref(emqx_authn_schema, "metrics_status_fields"),
                    status_metrics_example()
                ),
                400 => error_codes([?BAD_REQUEST], <<"Bad Request">>)
            }
        }
    };
schema("/authentication/:id/move") ->
    #{
        'operationId' => authenticator_move,
        post => #{
            tags => ?API_TAGS_GLOBAL,
            description => ?DESC(authentication_id_move_post),
            parameters => [param_auth_id()],
            'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                ref(request_move),
                request_move_examples()
            ),
            responses => #{
                204 => <<"Authenticator moved">>,
                400 => error_codes([?BAD_REQUEST], <<"Bad Request">>),
                404 => error_codes([?NOT_FOUND], <<"Not Found">>)
            }
        }
    };
schema("/listeners/:listener_id/authentication/:id/move") ->
    #{
        'operationId' => listener_authenticator_move,
        post => #{
            tags => ?API_TAGS_SINGLE,
            description => ?DESC(listeners_listener_id_authentication_id_move_post),
            parameters => [param_listener_id(), param_auth_id()],
            'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                ref(request_move),
                request_move_examples()
            ),
            responses => #{
                204 => <<"Authenticator moved">>,
                400 => error_codes([?BAD_REQUEST], <<"Bad Request">>),
                404 => error_codes([?NOT_FOUND], <<"Not Found">>)
            }
        }
    };
schema("/authentication/:id/users") ->
    #{
        'operationId' => authenticator_users,
        post => #{
            tags => ?API_TAGS_GLOBAL,
            description => ?DESC(authentication_id_users_post),
            parameters => [param_auth_id()],
            'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                ref(request_user_create),
                request_user_create_examples()
            ),
            responses => #{
                201 => emqx_dashboard_swagger:schema_with_examples(
                    ref(response_user),
                    response_user_examples()
                ),
                400 => error_codes([?BAD_REQUEST], <<"Bad Request">>),
                404 => error_codes([?NOT_FOUND], <<"Not Found">>)
            }
        },
        get => #{
            tags => ?API_TAGS_GLOBAL,
            description => ?DESC(authentication_id_users_get),
            parameters => [
                param_auth_id(),
                ref(emqx_dashboard_swagger, page),
                ref(emqx_dashboard_swagger, limit),
                {like_user_id,
                    mk(binary(), #{
                        in => query,
                        desc => ?DESC(like_user_id),
                        required => false
                    })},
                {is_superuser,
                    mk(boolean(), #{
                        in => query,
                        desc => ?DESC(is_superuser),
                        required => false
                    })}
            ],
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    ref(response_users),
                    response_users_example()
                ),
                404 => error_codes([?NOT_FOUND], <<"Not Found">>)
            }
        }
    };
schema("/listeners/:listener_id/authentication/:id/users") ->
    #{
        'operationId' => listener_authenticator_users,
        post => #{
            tags => ?API_TAGS_SINGLE,
            description => ?DESC(listeners_listener_id_authentication_id_users_post),
            parameters => [param_auth_id(), param_listener_id()],
            'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                ref(request_user_create),
                request_user_create_examples()
            ),
            responses => #{
                201 => emqx_dashboard_swagger:schema_with_examples(
                    ref(response_user),
                    response_user_examples()
                ),
                400 => error_codes([?BAD_REQUEST], <<"Bad Request">>),
                404 => error_codes([?NOT_FOUND], <<"Not Found">>)
            }
        },
        get => #{
            tags => ?API_TAGS_SINGLE,
            description => ?DESC(listeners_listener_id_authentication_id_users_get),
            parameters => [
                param_listener_id(),
                param_auth_id(),
                ref(emqx_dashboard_swagger, page),
                ref(emqx_dashboard_swagger, limit),
                {is_superuser,
                    mk(boolean(), #{
                        in => query,
                        desc => ?DESC(is_superuser),
                        required => false
                    })}
            ],
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    ref(response_users),
                    response_users_example()
                ),
                404 => error_codes([?NOT_FOUND], <<"Not Found">>)
            }
        }
    };
schema("/authentication/:id/users/:user_id") ->
    #{
        'operationId' => authenticator_user,
        get => #{
            tags => ?API_TAGS_GLOBAL,
            description => ?DESC(authentication_id_users_user_id_get),
            parameters => [param_auth_id(), param_user_id()],
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_examples(
                    ref(response_user),
                    response_user_examples()
                ),
                404 => error_codes([?NOT_FOUND], <<"Not Found">>)
            }
        },
        put => #{
            tags => ?API_TAGS_GLOBAL,
            description => ?DESC(authentication_id_users_user_id_put),
            parameters => [param_auth_id(), param_user_id()],
            'requestBody' => emqx_dashboard_swagger:schema_with_examples(
                ref(request_user_update),
                request_user_update_examples()
            ),
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    ref(response_user),
                    response_user_examples()
                ),
                400 => error_codes([?BAD_REQUEST], <<"Bad Request">>),
                404 => error_codes([?NOT_FOUND], <<"Not Found">>)
            }
        },
        delete => #{
            tags => ?API_TAGS_GLOBAL,
            description => ?DESC(authentication_id_users_user_id_delete),
            parameters => [param_auth_id(), param_user_id()],
            responses => #{
                204 => <<"User deleted">>,
                404 => error_codes([?NOT_FOUND], <<"Not Found">>)
            }
        }
    };
schema("/listeners/:listener_id/authentication/:id/users/:user_id") ->
    #{
        'operationId' => listener_authenticator_user,
        get => #{
            tags => ?API_TAGS_SINGLE,
            description => ?DESC(listeners_listener_id_authentication_id_users_user_id_get),
            parameters => [param_listener_id(), param_auth_id(), param_user_id()],
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    ref(response_user),
                    response_user_examples()
                ),
                404 => error_codes([?NOT_FOUND], <<"Not Found">>)
            }
        },
        put => #{
            tags => ?API_TAGS_SINGLE,
            description => ?DESC(listeners_listener_id_authentication_id_users_user_id_put),
            parameters => [param_listener_id(), param_auth_id(), param_user_id()],
            'requestBody' => emqx_dashboard_swagger:schema_with_example(
                ref(request_user_update),
                request_user_update_examples()
            ),
            responses => #{
                200 => emqx_dashboard_swagger:schema_with_example(
                    ref(response_user),
                    response_user_examples()
                ),
                400 => error_codes([?BAD_REQUEST], <<"Bad Request">>),
                404 => error_codes([?NOT_FOUND], <<"Not Found">>)
            }
        },
        delete => #{
            tags => ?API_TAGS_SINGLE,
            description => ?DESC(listeners_listener_id_authentication_id_users_user_id_delete),
            parameters => [param_listener_id(), param_auth_id(), param_user_id()],
            responses => #{
                204 => <<"User deleted">>,
                404 => error_codes([?NOT_FOUND], <<"Not Found">>)
            }
        }
    }.

param_auth_id() ->
    {
        id,
        mk(binary(), #{
            in => path,
            desc => ?DESC(param_auth_id),
            required => true
        })
    }.

param_listener_id() ->
    {
        listener_id,
        mk(binary(), #{
            in => path,
            desc => ?DESC(param_listener_id),
            required => true,
            example => emqx_listeners:id_example()
        })
    }.

param_user_id() ->
    {
        user_id,
        mk(binary(), #{
            in => path,
            desc => ?DESC(param_user_id)
        })
    }.

authenticators(post, #{body := Config}) ->
    create_authenticator([authentication], ?GLOBAL, Config);
authenticators(get, _Params) ->
    list_authenticators([authentication]).

authenticator(get, #{bindings := #{id := AuthenticatorID}}) ->
    list_authenticator(?GLOBAL, [authentication], AuthenticatorID);
authenticator(put, #{bindings := #{id := AuthenticatorID}, body := Config}) ->
    update_authenticator([authentication], ?GLOBAL, AuthenticatorID, Config);
authenticator(delete, #{bindings := #{id := AuthenticatorID}}) ->
    delete_authenticator([authentication], ?GLOBAL, AuthenticatorID).

authenticator_status(get, #{bindings := #{id := AuthenticatorID}}) ->
    lookup_from_all_nodes(?GLOBAL, AuthenticatorID).

listener_authenticators(post, #{bindings := #{listener_id := ListenerID}, body := Config}) ->
    with_listener(
        ListenerID,
        fun(Type, Name, ChainName) ->
            create_authenticator(
                [listeners, Type, Name, authentication],
                ChainName,
                Config
            )
        end
    );
listener_authenticators(get, #{bindings := #{listener_id := ListenerID}}) ->
    with_listener(
        ListenerID,
        fun(Type, Name, _) ->
            list_authenticators([listeners, Type, Name, authentication])
        end
    ).

listener_authenticator(get, #{bindings := #{listener_id := ListenerID, id := AuthenticatorID}}) ->
    with_listener(
        ListenerID,
        fun(Type, Name, ChainName) ->
            list_authenticator(
                ChainName,
                [listeners, Type, Name, authentication],
                AuthenticatorID
            )
        end
    );
listener_authenticator(
    put,
    #{
        bindings := #{listener_id := ListenerID, id := AuthenticatorID},
        body := Config
    }
) ->
    with_listener(
        ListenerID,
        fun(Type, Name, ChainName) ->
            update_authenticator(
                [listeners, Type, Name, authentication],
                ChainName,
                AuthenticatorID,
                Config
            )
        end
    );
listener_authenticator(
    delete,
    #{bindings := #{listener_id := ListenerID, id := AuthenticatorID}}
) ->
    with_listener(
        ListenerID,
        fun(Type, Name, ChainName) ->
            delete_authenticator(
                [listeners, Type, Name, authentication],
                ChainName,
                AuthenticatorID
            )
        end
    ).

listener_authenticator_status(
    get,
    #{bindings := #{listener_id := ListenerID, id := AuthenticatorID}}
) ->
    with_listener(
        ListenerID,
        fun(_, _, ChainName) ->
            lookup_from_all_nodes(ChainName, AuthenticatorID)
        end
    ).

authenticator_move(
    post,
    #{
        bindings := #{id := AuthenticatorID},
        body := #{<<"position">> := Position}
    }
) ->
    move_authenticator([authentication], ?GLOBAL, AuthenticatorID, Position);
authenticator_move(post, #{bindings := #{id := _}, body := _}) ->
    serialize_error({missing_parameter, position}).

listener_authenticator_move(
    post,
    #{
        bindings := #{listener_id := ListenerID, id := AuthenticatorID},
        body := #{<<"position">> := Position}
    }
) ->
    with_listener(
        ListenerID,
        fun(Type, Name, ChainName) ->
            move_authenticator(
                [listeners, Type, Name, authentication],
                ChainName,
                AuthenticatorID,
                Position
            )
        end
    );
listener_authenticator_move(post, #{bindings := #{listener_id := _, id := _}, body := _}) ->
    serialize_error({missing_parameter, position}).

authenticator_users(post, #{bindings := #{id := AuthenticatorID}, body := UserInfo}) ->
    add_user(?GLOBAL, AuthenticatorID, UserInfo);
authenticator_users(get, #{bindings := #{id := AuthenticatorID}, query_string := QueryString}) ->
    list_users(?GLOBAL, AuthenticatorID, QueryString).

authenticator_user(put, #{
    bindings := #{
        id := AuthenticatorID,
        user_id := UserID
    },
    body := UserInfo
}) ->
    update_user(?GLOBAL, AuthenticatorID, UserID, UserInfo);
authenticator_user(get, #{bindings := #{id := AuthenticatorID, user_id := UserID}}) ->
    find_user(?GLOBAL, AuthenticatorID, UserID);
authenticator_user(delete, #{bindings := #{id := AuthenticatorID, user_id := UserID}}) ->
    delete_user(?GLOBAL, AuthenticatorID, UserID).

listener_authenticator_users(post, #{
    bindings := #{
        listener_id := ListenerID,
        id := AuthenticatorID
    },
    body := UserInfo
}) ->
    with_chain(
        ListenerID,
        fun(ChainName) ->
            add_user(ChainName, AuthenticatorID, UserInfo)
        end
    );
listener_authenticator_users(get, #{
    bindings := #{
        listener_id := ListenerID,
        id := AuthenticatorID
    },
    query_string := PageParams
}) ->
    with_chain(
        ListenerID,
        fun(ChainName) ->
            list_users(ChainName, AuthenticatorID, PageParams)
        end
    ).

listener_authenticator_user(put, #{
    bindings := #{
        listener_id := ListenerID,
        id := AuthenticatorID,
        user_id := UserID
    },
    body := UserInfo
}) ->
    with_chain(
        ListenerID,
        fun(ChainName) ->
            update_user(ChainName, AuthenticatorID, UserID, UserInfo)
        end
    );
listener_authenticator_user(get, #{
    bindings := #{
        listener_id := ListenerID,
        id := AuthenticatorID,
        user_id := UserID
    }
}) ->
    with_chain(
        ListenerID,
        fun(ChainName) ->
            find_user(ChainName, AuthenticatorID, UserID)
        end
    );
listener_authenticator_user(delete, #{
    bindings := #{
        listener_id := ListenerID,
        id := AuthenticatorID,
        user_id := UserID
    }
}) ->
    with_chain(
        ListenerID,
        fun(ChainName) ->
            delete_user(ChainName, AuthenticatorID, UserID)
        end
    ).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

with_listener(ListenerID, Fun) ->
    case find_listener(ListenerID) of
        {ok, {BType, BName}} ->
            Type = binary_to_existing_atom(BType),
            Name = binary_to_existing_atom(BName),
            ChainName = binary_to_atom(ListenerID),
            Fun(Type, Name, ChainName);
        {error, Reason} ->
            serialize_error(Reason)
    end.

find_listener(ListenerID) ->
    case binary:split(ListenerID, <<":">>) of
        [BType, BName] ->
            case emqx_config:find([listeners, BType, BName]) of
                {ok, _} ->
                    {ok, {BType, BName}};
                {not_found, _, _} ->
                    {error, {not_found, {listener, ListenerID}}}
            end;
        _ ->
            {error, {not_found, {listener, ListenerID}}}
    end.

with_chain(ListenerID, Fun) ->
    {ok, ChainNames} = emqx_authentication:list_chain_names(),
    ListenerChainName =
        [Name || Name <- ChainNames, atom_to_binary(Name) =:= ListenerID],
    case ListenerChainName of
        [ChainName] ->
            Fun(ChainName);
        _ ->
            serialize_error({not_found, {chain, ListenerID}})
    end.

create_authenticator(ConfKeyPath, ChainName, Config) ->
    case update_config(ConfKeyPath, {create_authenticator, ChainName, Config}) of
        {ok, #{
            post_config_update := #{emqx_authentication := #{id := ID}},
            raw_config := AuthenticatorsConfig
        }} ->
            {ok, AuthenticatorConfig} = find_config(ID, AuthenticatorsConfig),
            {200, maps:put(id, ID, convert_certs(fill_defaults(AuthenticatorConfig)))};
        {error, {_PrePostConfigUpdate, emqx_authentication, Reason}} ->
            serialize_error(Reason);
        {error, Reason} ->
            serialize_error(Reason)
    end.

list_authenticators(ConfKeyPath) ->
    AuthenticatorsConfig = get_raw_config_with_defaults(ConfKeyPath),
    NAuthenticators = [
        maps:put(
            id,
            emqx_authentication:authenticator_id(AuthenticatorConfig),
            convert_certs(AuthenticatorConfig)
        )
     || AuthenticatorConfig <- AuthenticatorsConfig
    ],
    {200, NAuthenticators}.

list_authenticator(_, ConfKeyPath, AuthenticatorID) ->
    AuthenticatorsConfig = get_raw_config_with_defaults(ConfKeyPath),
    case find_config(AuthenticatorID, AuthenticatorsConfig) of
        {ok, AuthenticatorConfig} ->
            {200, maps:put(id, AuthenticatorID, convert_certs(AuthenticatorConfig))};
        {error, Reason} ->
            serialize_error(Reason)
    end.

resource_provider() ->
    [
        emqx_authn_mysql,
        emqx_authn_pgsql,
        emqx_authn_mongodb,
        emqx_authn_redis,
        emqx_authn_http
    ].

lookup_from_local_node(ChainName, AuthenticatorID) ->
    NodeId = node(self()),
    case emqx_authentication:lookup_authenticator(ChainName, AuthenticatorID) of
        {ok, #{provider := Provider, state := State}} ->
            MetricsId = emqx_authentication:metrics_id(ChainName, AuthenticatorID),
            Metrics = emqx_metrics_worker:get_metrics(authn_metrics, MetricsId),
            case lists:member(Provider, resource_provider()) of
                false ->
                    {ok, {NodeId, connected, Metrics, #{}}};
                true ->
                    #{resource_id := ResourceId} = State,
                    case emqx_resource:get_instance(ResourceId) of
                        {error, not_found} ->
                            {error, {NodeId, not_found_resource}};
                        {ok, _, #{status := Status, metrics := ResourceMetrics}} ->
                            {ok, {NodeId, Status, Metrics, ResourceMetrics}}
                    end
            end;
        {error, Reason} ->
            {error, {NodeId, list_to_binary(io_lib:format("~p", [Reason]))}}
    end.

lookup_from_all_nodes(ChainName, AuthenticatorID) ->
    Nodes = mria_mnesia:running_nodes(),
    case is_ok(emqx_authn_proto_v1:lookup_from_all_nodes(Nodes, ChainName, AuthenticatorID)) of
        {ok, ResList} ->
            {StatusMap, MetricsMap, ResourceMetricsMap, ErrorMap} = make_result_map(ResList),
            AggregateStatus = aggregate_status(maps:values(StatusMap)),
            AggregateMetrics = aggregate_metrics(maps:values(MetricsMap)),
            AggregateResourceMetrics = aggregate_metrics(maps:values(ResourceMetricsMap)),
            Fun = fun(_, V1) -> restructure_map(V1) end,
            MKMap = fun(Name) -> fun({Key, Val}) -> #{node => Key, Name => Val} end end,
            HelpFun = fun(M, Name) -> lists:map(MKMap(Name), maps:to_list(M)) end,
            {200, #{
                node_resource_metrics => HelpFun(maps:map(Fun, ResourceMetricsMap), metrics),
                resource_metrics =>
                    case maps:size(AggregateResourceMetrics) of
                        0 -> #{};
                        _ -> restructure_map(AggregateResourceMetrics)
                    end,
                node_metrics => HelpFun(maps:map(Fun, MetricsMap), metrics),
                metrics => restructure_map(AggregateMetrics),

                node_status => HelpFun(StatusMap, status),
                status => AggregateStatus,
                node_error => HelpFun(maps:map(Fun, ErrorMap), reason)
            }};
        {error, ErrL} ->
            {400, #{
                code => <<"INTERNAL_ERROR">>,
                message => list_to_binary(io_lib:format("~p", [ErrL]))
            }}
    end.

aggregate_status([]) ->
    empty_metrics_and_status;
aggregate_status(AllStatus) ->
    Head = fun([A | _]) -> A end,
    HeadVal = Head(AllStatus),
    AllRes = lists:all(fun(Val) -> Val == HeadVal end, AllStatus),
    case AllRes of
        true -> HeadVal;
        false -> inconsistent
    end.

aggregate_metrics([]) ->
    #{};
aggregate_metrics([HeadMetrics | AllMetrics]) ->
    ErrorLogger = fun(Reason) -> ?SLOG(info, #{msg => "bad_metrics_value", error => Reason}) end,
    Fun = fun(ElemMap, AccMap) ->
        emqx_map_lib:best_effort_recursive_sum(AccMap, ElemMap, ErrorLogger)
    end,
    lists:foldl(Fun, HeadMetrics, AllMetrics).

make_result_map(ResList) ->
    Fun =
        fun(Elem, {StatusMap, MetricsMap, ResourceMetricsMap, ErrorMap}) ->
            case Elem of
                {ok, {NodeId, Status, Metrics, ResourceMetrics}} ->
                    {
                        maps:put(NodeId, Status, StatusMap),
                        maps:put(NodeId, Metrics, MetricsMap),
                        maps:put(NodeId, ResourceMetrics, ResourceMetricsMap),
                        ErrorMap
                    };
                {error, {NodeId, Reason}} ->
                    {StatusMap, MetricsMap, ResourceMetricsMap, maps:put(NodeId, Reason, ErrorMap)}
            end
        end,
    lists:foldl(Fun, {maps:new(), maps:new(), maps:new(), maps:new()}, ResList).

restructure_map(#{
    counters := #{failed := Failed, total := Total, success := Succ, nomatch := Nomatch},
    rate := #{total := #{current := Rate, last5m := Rate5m, max := RateMax}}
}) ->
    #{
        total => Total,
        success => Succ,
        failed => Failed,
        nomatch => Nomatch,
        rate => Rate,
        rate_last5m => Rate5m,
        rate_max => RateMax
    };
restructure_map(#{
    counters := #{failed := Failed, matched := Match, success := Succ},
    rate := #{matched := #{current := Rate, last5m := Rate5m, max := RateMax}}
}) ->
    #{
        matched => Match,
        success => Succ,
        failed => Failed,
        rate => Rate,
        rate_last5m => Rate5m,
        rate_max => RateMax
    };
restructure_map(Error) ->
    Error.

is_ok(ResL) ->
    case
        lists:filter(
            fun
                ({ok, _}) -> false;
                (_) -> true
            end,
            ResL
        )
    of
        [] -> {ok, [Res || {ok, Res} <- ResL]};
        ErrL -> {error, ErrL}
    end.

update_authenticator(ConfKeyPath, ChainName, AuthenticatorID, Config) ->
    case
        update_config(
            ConfKeyPath,
            {update_authenticator, ChainName, AuthenticatorID, Config}
        )
    of
        {ok, #{
            post_config_update := #{emqx_authentication := #{id := ID}},
            raw_config := AuthenticatorsConfig
        }} ->
            {ok, AuthenticatorConfig} = find_config(ID, AuthenticatorsConfig),
            {200, maps:put(id, ID, convert_certs(fill_defaults(AuthenticatorConfig)))};
        {error, {_PrePostConfigUpdate, emqx_authentication, Reason}} ->
            serialize_error(Reason);
        {error, Reason} ->
            serialize_error(Reason)
    end.

delete_authenticator(ConfKeyPath, ChainName, AuthenticatorID) ->
    case update_config(ConfKeyPath, {delete_authenticator, ChainName, AuthenticatorID}) of
        {ok, _} ->
            {204};
        {error, {_PrePostConfigUpdate, emqx_authentication, Reason}} ->
            serialize_error(Reason);
        {error, Reason} ->
            serialize_error(Reason)
    end.

move_authenticator(ConfKeyPath, ChainName, AuthenticatorID, Position) ->
    case parse_position(Position) of
        {ok, NPosition} ->
            case
                update_config(
                    ConfKeyPath,
                    {move_authenticator, ChainName, AuthenticatorID, NPosition}
                )
            of
                {ok, _} ->
                    {204};
                {error, {_PrePostConfigUpdate, emqx_authentication, Reason}} ->
                    serialize_error(Reason);
                {error, Reason} ->
                    serialize_error(Reason)
            end;
        {error, Reason} ->
            serialize_error(Reason)
    end.

add_user(
    ChainName,
    AuthenticatorID,
    #{<<"user_id">> := UserID, <<"password">> := Password} = UserInfo
) ->
    IsSuperuser = maps:get(<<"is_superuser">>, UserInfo, false),
    case
        emqx_authentication:add_user(
            ChainName,
            AuthenticatorID,
            #{
                user_id => UserID,
                password => Password,
                is_superuser => IsSuperuser
            }
        )
    of
        {ok, User} ->
            {201, User};
        {error, Reason} ->
            serialize_error({user_error, Reason})
    end;
add_user(_, _, #{<<"user_id">> := _}) ->
    serialize_error({missing_parameter, password});
add_user(_, _, _) ->
    serialize_error({missing_parameter, user_id}).

update_user(ChainName, AuthenticatorID, UserID, UserInfo0) ->
    case maps:with([<<"password">>, <<"is_superuser">>], UserInfo0) =:= #{} of
        true ->
            serialize_error({missing_parameter, password});
        false ->
            UserInfo = emqx_map_lib:safe_atom_key_map(UserInfo0),
            case emqx_authentication:update_user(ChainName, AuthenticatorID, UserID, UserInfo) of
                {ok, User} ->
                    {200, User};
                {error, Reason} ->
                    serialize_error({user_error, Reason})
            end
    end.

find_user(ChainName, AuthenticatorID, UserID) ->
    case emqx_authentication:lookup_user(ChainName, AuthenticatorID, UserID) of
        {ok, User} ->
            {200, User};
        {error, Reason} ->
            serialize_error({user_error, Reason})
    end.

delete_user(ChainName, AuthenticatorID, UserID) ->
    case emqx_authentication:delete_user(ChainName, AuthenticatorID, UserID) of
        ok ->
            {204};
        {error, Reason} ->
            serialize_error({user_error, Reason})
    end.

list_users(ChainName, AuthenticatorID, QueryString) ->
    case emqx_authentication:list_users(ChainName, AuthenticatorID, QueryString) of
        {error, page_limit_invalid} ->
            {400, #{code => <<"INVALID_PARAMETER">>, message => <<"page_limit_invalid">>}};
        {error, Reason} ->
            {400, #{
                code => <<"INVALID_PARAMETER">>,
                message => list_to_binary(io_lib:format("Reason ~p", [Reason]))
            }};
        Result ->
            {200, Result}
    end.

update_config(Path, ConfigRequest) ->
    emqx_conf:update(Path, ConfigRequest, #{
        rawconf_with_defaults => true,
        override_to => cluster
    }).

get_raw_config_with_defaults(ConfKeyPath) ->
    NConfKeyPath = [atom_to_binary(Key, utf8) || Key <- ConfKeyPath],
    RawConfig = emqx:get_raw_config(NConfKeyPath, []),
    ensure_list(fill_defaults(RawConfig)).

find_config(AuthenticatorID, AuthenticatorsConfig) ->
    MatchingACs =
        [
            AC
         || AC <- ensure_list(AuthenticatorsConfig),
            AuthenticatorID =:= emqx_authentication:authenticator_id(AC)
        ],
    case MatchingACs of
        [] -> {error, {not_found, {authenticator, AuthenticatorID}}};
        [AuthenticatorConfig] -> {ok, AuthenticatorConfig}
    end.

fill_defaults(Configs) when is_list(Configs) ->
    lists:map(fun fill_defaults/1, Configs);
fill_defaults(Config) ->
    emqx_authn:check_config(merge_default_headers(Config), #{only_fill_defaults => true}).

merge_default_headers(Config) ->
    case maps:find(<<"headers">>, Config) of
        {ok, Headers} ->
            NewHeaders =
                case Config of
                    #{<<"method">> := <<"get">>} ->
                        (emqx_authn_http:headers_no_content_type(converter))(Headers);
                    #{<<"method">> := <<"post">>} ->
                        (emqx_authn_http:headers(converter))(Headers);
                    _ ->
                        Headers
                end,
            Config#{<<"headers">> => NewHeaders};
        error ->
            Config
    end.

convert_certs(#{ssl := SSL} = Config) when SSL =/= undefined ->
    Config#{ssl := emqx_tls_lib:drop_invalid_certs(SSL)};
convert_certs(Config) ->
    Config.

serialize_error({user_error, not_found}) ->
    {404, #{
        code => <<"NOT_FOUND">>,
        message => binfmt("User not found", [])
    }};
serialize_error({user_error, already_exist}) ->
    {409, #{
        code => <<"ALREADY_EXISTS">>,
        message => binfmt("User already exists", [])
    }};
serialize_error({user_error, Reason}) ->
    {400, #{
        code => <<"BAD_REQUEST">>,
        message => binfmt("User error: ~p", [Reason])
    }};
serialize_error({not_found, {authenticator, ID}}) ->
    {404, #{
        code => <<"NOT_FOUND">>,
        message => binfmt("Authenticator '~ts' does not exist", [ID])
    }};
serialize_error({not_found, {listener, ID}}) ->
    {404, #{
        code => <<"NOT_FOUND">>,
        message => binfmt("Listener '~ts' does not exist", [ID])
    }};
serialize_error({not_found, {chain, ?GLOBAL}}) ->
    {404, #{
        code => <<"NOT_FOUND">>,
        message => <<"Authenticator not found in the 'global' scope">>
    }};
serialize_error({not_found, {chain, Name}}) ->
    {400, #{
        code => <<"BAD_REQUEST">>,
        message => binfmt("No authentication has been created for listener ~p", [Name])
    }};
serialize_error({already_exists, {authenticator, ID}}) ->
    {409, #{
        code => <<"ALREADY_EXISTS">>,
        message => binfmt("Authenticator '~ts' already exist", [ID])
    }};
serialize_error(no_available_provider) ->
    {400, #{
        code => <<"BAD_REQUEST">>,
        message => <<"Unsupported authentication type">>
    }};
serialize_error(change_of_authentication_type_is_not_allowed) ->
    {400, #{
        code => <<"BAD_REQUEST">>,
        message => <<"Change of authentication type is not allowed">>
    }};
serialize_error(unsupported_operation) ->
    {400, #{
        code => <<"BAD_REQUEST">>,
        message => <<"Operation not supported in this authentication type">>
    }};
serialize_error({bad_ssl_config, Details}) ->
    {400, #{
        code => <<"BAD_REQUEST">>,
        message => binfmt("bad_ssl_config ~p", [Details])
    }};
serialize_error({missing_parameter, Detail}) ->
    {400, #{
        code => <<"BAD_REQUEST">>,
        message => binfmt("Missing required parameter: ~p", [Detail])
    }};
serialize_error({invalid_parameter, Name}) ->
    {400, #{
        code => <<"BAD_REQUEST">>,
        message => binfmt("Invalid value for '~p'", [Name])
    }};
serialize_error({unknown_authn_type, Type}) ->
    {400, #{
        code => <<"BAD_REQUEST">>,
        message => binfmt("Unknown type '~p'", [Type])
    }};
serialize_error({bad_authenticator_config, Reason}) ->
    {400, #{
        code => <<"BAD_REQUEST">>,
        message => binfmt("Bad authenticator config ~p", [Reason])
    }};
serialize_error(Reason) ->
    {400, #{
        code => <<"BAD_REQUEST">>,
        message => binfmt("~p", [Reason])
    }}.

parse_position(<<"front">>) ->
    {ok, ?CMD_MOVE_FRONT};
parse_position(<<"rear">>) ->
    {ok, ?CMD_MOVE_REAR};
parse_position(<<"before:">>) ->
    {error, {invalid_parameter, position}};
parse_position(<<"after:">>) ->
    {error, {invalid_parameter, position}};
parse_position(<<"before:", Before/binary>>) ->
    {ok, ?CMD_MOVE_BEFORE(Before)};
parse_position(<<"after:", After/binary>>) ->
    {ok, ?CMD_MOVE_AFTER(After)};
parse_position(_) ->
    {error, {invalid_parameter, position}}.

ensure_list(M) when is_map(M) -> [M];
ensure_list(L) when is_list(L) -> L.

binfmt(Fmt, Args) -> iolist_to_binary(io_lib:format(Fmt, Args)).

paginated_list_type(Type) ->
    [
        {data, hoconsc:array(Type)},
        {meta, ref(emqx_dashboard_swagger, meta)}
    ].

authenticator_array_example() ->
    [Config || #{value := Config} <- maps:values(authenticator_examples())].

authenticator_examples() ->
    #{
        'password_based:built_in_database' => #{
            summary => <<"Built-in password_based authentication">>,
            value => #{
                mechanism => <<"password_based">>,
                backend => <<"built_in_database">>,
                user_id_type => <<"username">>,
                password_hash_algorithm => #{
                    name => <<"sha256">>,
                    salt_position => <<"suffix">>
                }
            }
        },
        'password_based:http' => #{
            summary => <<"password_based authentication through external HTTP API">>,
            value => #{
                mechanism => <<"password_based">>,
                backend => <<"http">>,
                method => <<"post">>,
                url => <<"http://127.0.0.1:18083">>,
                headers => #{
                    <<"content-type">> => <<"application/json">>
                },
                body => #{
                    <<"username">> => ?PH_USERNAME,
                    <<"password">> => ?PH_PASSWORD
                },
                pool_size => 8,
                connect_timeout => 5000,
                request_timeout => 5000,
                enable_pipelining => 100,
                ssl => #{enable => false}
            }
        },
        'jwt' => #{
            summary => <<"JWT authentication">>,
            value => #{
                mechanism => <<"jwt">>,
                use_jwks => false,
                algorithm => <<"hmac-based">>,
                secret => <<"mysecret">>,
                secret_base64_encoded => false,
                verify_claims => #{
                    <<"username">> => ?PH_USERNAME
                }
            }
        },
        'password_based:mongodb' => #{
            summary => <<"password_based authentication with MongoDB backend">>,
            value => #{
                mechanism => <<"password_based">>,
                backend => <<"mongodb">>,
                server => <<"127.0.0.1:27017">>,
                database => example,
                collection => users,
                filter => #{
                    username => ?PH_USERNAME
                },
                password_hash_field => <<"password_hash">>,
                salt_field => <<"salt">>,
                is_superuser_field => <<"is_superuser">>,
                password_hash_algorithm => #{
                    name => <<"sha256">>,
                    salt_position => <<"suffix">>
                }
            }
        },
        'password_based:redis' => #{
            summary => <<"password_based authentication with Redis backend">>,
            value => #{
                mechanism => <<"password_based">>,
                backend => <<"redis">>,
                server => <<"127.0.0.1:6379">>,
                redis_type => <<"single">>,
                database => 0,
                cmd => <<"HMGET ${username} password_hash salt">>,
                password_hash_algorithm => #{
                    name => <<"sha256">>,
                    salt_position => <<"suffix">>
                }
            }
        }
    }.

status_metrics_example() ->
    #{
        status_metrics => #{
            summary => <<"Authn status metrics">>,
            value => #{
                resource_metrics => #{
                    matched => 0,
                    success => 0,
                    failed => 0,
                    rate => 0.0,
                    rate_last5m => 0.0,
                    rate_max => 0.0
                },
                node_resource_metrics => [
                    #{
                        node => node(),
                        metrics => #{
                            matched => 0,
                            success => 0,
                            failed => 0,
                            rate => 0.0,
                            rate_last5m => 0.0,
                            rate_max => 0.0
                        }
                    }
                ],
                metrics => #{
                    total => 0,
                    success => 0,
                    failed => 0,
                    nomatch => 0,
                    rate => 0.0,
                    rate_last5m => 0.0,
                    rate_max => 0.0
                },
                node_error => [],
                node_metrics => [
                    #{
                        node => node(),
                        metrics => #{
                            matched => 0,
                            total => 0,
                            failed => 0,
                            nomatch => 0,
                            rate => 0.0,
                            rate_last5m => 0.0,
                            rate_max => 0.0
                        }
                    }
                ],
                status => connected,
                node_status => [
                    #{
                        node => node(),
                        status => connected
                    }
                ]
            }
        }
    }.

request_user_create_examples() ->
    #{
        regular_user => #{
            summary => <<"Regular user">>,
            value => #{
                user_id => <<"user1">>,
                password => <<"secret">>
            }
        },
        super_user => #{
            summary => <<"Superuser">>,
            value => #{
                user_id => <<"user2">>,
                password => <<"secret">>,
                is_superuser => true
            }
        }
    }.

request_user_update_examples() ->
    #{
        regular_user => #{
            summary => <<"Update regular user">>,
            value => #{
                password => <<"newsecret">>
            }
        },
        super_user => #{
            summary => <<"Update user and promote to superuser">>,
            value => #{
                password => <<"newsecret">>,
                is_superuser => true
            }
        }
    }.

request_move_examples() ->
    #{
        move_to_front => #{
            summary => <<"Move authenticator to the beginning of the chain">>,
            value => #{
                position => <<"front">>
            }
        },
        move_to_rear => #{
            summary => <<"Move authenticator to the end of the chain">>,
            value => #{
                position => <<"rear">>
            }
        },
        'move_before_password_based:built_in_database' => #{
            summary => <<"Move authenticator to the position preceding some other authenticator">>,
            value => #{
                position => <<"before:password_based:built_in_database">>
            }
        }
    }.

response_user_examples() ->
    #{
        regular_user => #{
            summary => <<"Regular user">>,
            value => #{
                user_id => <<"user1">>
            }
        },
        super_user => #{
            summary => <<"Superuser">>,
            value => #{
                user_id => <<"user2">>,
                is_superuser => true
            }
        }
    }.

response_users_example() ->
    #{
        data => [
            #{
                user_id => <<"user1">>
            },
            #{
                user_id => <<"user2">>,
                is_superuser => true
            }
        ],
        meta => #{
            page => 0,
            limit => 20,
            count => 300
        }
    }.
