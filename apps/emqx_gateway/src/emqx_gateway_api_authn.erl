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

-module(emqx_gateway_api_authn).

-behaviour(minirest_api).

-include("emqx_gateway_http.hrl").
-include_lib("typerefl/include/types.hrl").

-import(hoconsc, [mk/2, ref/2]).
-import(emqx_dashboard_swagger, [error_codes/2]).

-import(
    emqx_gateway_http,
    [
        return_http_error/2,
        with_gateway/2,
        with_authn/2,
        checks/2
    ]
).

%% minirest/dashbaord_swagger behaviour callbacks
-export([
    api_spec/0,
    paths/0,
    schema/1
]).

%% http handlers
-export([
    authn/2,
    users/2,
    users_insta/2,
    import_users/2
]).

%% internal export for emqx_gateway_api_listeners module
-export([schema_authn/0]).

%%--------------------------------------------------------------------
%% minirest behaviour callbacks
%%--------------------------------------------------------------------

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/gateway/:name/authentication",
        "/gateway/:name/authentication/users",
        "/gateway/:name/authentication/users/:uid",
        "/gateway/:name/authentication/import_users"
    ].

%%--------------------------------------------------------------------
%% http handlers

authn(get, #{bindings := #{name := Name0}}) ->
    with_gateway(Name0, fun(GwName, _) ->
        try emqx_gateway_http:authn(GwName) of
            Authn -> {200, Authn}
        catch
            error:{config_not_found, _} ->
                {204}
        end
    end);
authn(put, #{
    bindings := #{name := Name0},
    body := Body
}) ->
    with_gateway(Name0, fun(GwName, _) ->
        {ok, Authn} = emqx_gateway_http:update_authn(GwName, Body),
        {200, Authn}
    end);
authn(post, #{
    bindings := #{name := Name0},
    body := Body
}) ->
    with_gateway(Name0, fun(GwName, _) ->
        {ok, Authn} = emqx_gateway_http:add_authn(GwName, Body),
        {201, Authn}
    end);
authn(delete, #{bindings := #{name := Name0}}) ->
    with_gateway(Name0, fun(GwName, _) ->
        ok = emqx_gateway_http:remove_authn(GwName),
        {204}
    end).

users(get, #{bindings := #{name := Name0}, query_string := Qs}) ->
    with_authn(Name0, fun(
        _GwName,
        #{
            id := AuthId,
            chain_name := ChainName
        }
    ) ->
        emqx_authn_api:list_users(ChainName, AuthId, parse_qstring(Qs))
    end);
users(post, #{
    bindings := #{name := Name0},
    body := Body
}) ->
    with_authn(Name0, fun(
        _GwName,
        #{
            id := AuthId,
            chain_name := ChainName
        }
    ) ->
        emqx_authn_api:add_user(ChainName, AuthId, Body)
    end).

users_insta(get, #{bindings := #{name := Name0, uid := UserId}}) ->
    with_authn(Name0, fun(
        _GwName,
        #{
            id := AuthId,
            chain_name := ChainName
        }
    ) ->
        emqx_authn_api:find_user(ChainName, AuthId, UserId)
    end);
users_insta(put, #{
    bindings := #{name := Name0, uid := UserId},
    body := Body
}) ->
    with_authn(Name0, fun(
        _GwName,
        #{
            id := AuthId,
            chain_name := ChainName
        }
    ) ->
        emqx_authn_api:update_user(ChainName, AuthId, UserId, Body)
    end);
users_insta(delete, #{bindings := #{name := Name0, uid := UserId}}) ->
    with_authn(Name0, fun(
        _GwName,
        #{
            id := AuthId,
            chain_name := ChainName
        }
    ) ->
        emqx_authn_api:delete_user(ChainName, AuthId, UserId)
    end).

import_users(post, #{
    bindings := #{name := Name0},
    body := Body
}) ->
    with_authn(Name0, fun(
        _GwName,
        #{
            id := AuthId,
            chain_name := ChainName
        }
    ) ->
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
    end).

%%--------------------------------------------------------------------
%% Utils

parse_qstring(Qs) ->
    maps:with(
        [
            <<"page">>,
            <<"limit">>,
            <<"like_username">>,
            <<"like_clientid">>
        ],
        Qs
    ).

%%--------------------------------------------------------------------
%% Swagger defines
%%--------------------------------------------------------------------

schema("/gateway/:name/authentication") ->
    #{
        'operationId' => authn,
        get =>
            #{
                desc => <<"Get the gateway authentication">>,
                parameters => params_gateway_name_in_path(),
                responses =>
                    ?STANDARD_RESP(
                        #{
                            200 => schema_authn(),
                            204 => <<"Authentication does not initiated">>
                        }
                    )
            },
        put =>
            #{
                desc => <<"Update authentication for the gateway">>,
                parameters => params_gateway_name_in_path(),
                'requestBody' => schema_authn(),
                responses =>
                    ?STANDARD_RESP(#{200 => schema_authn()})
            },
        post =>
            #{
                desc => <<"Add authentication for the gateway">>,
                parameters => params_gateway_name_in_path(),
                'requestBody' => schema_authn(),
                responses =>
                    ?STANDARD_RESP(#{201 => schema_authn()})
            },
        delete =>
            #{
                desc => <<"Remove the gateway authentication">>,
                parameters => params_gateway_name_in_path(),
                responses =>
                    ?STANDARD_RESP(#{204 => <<"Deleted">>})
            }
    };
schema("/gateway/:name/authentication/users") ->
    #{
        'operationId' => users,
        get =>
            #{
                desc => <<"Get the users for the authentication">>,
                parameters => params_gateway_name_in_path() ++
                    params_paging_in_qs() ++
                    params_fuzzy_in_qs(),
                responses =>
                    ?STANDARD_RESP(
                        #{
                            200 => emqx_dashboard_swagger:schema_with_example(
                                ref(emqx_authn_api, response_users),
                                emqx_authn_api:response_users_example()
                            )
                        }
                    )
            },
        post =>
            #{
                desc => <<"Add user for the authentication">>,
                parameters => params_gateway_name_in_path(),
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
schema("/gateway/:name/authentication/users/:uid") ->
    #{
        'operationId' => users_insta,
        get =>
            #{
                desc => <<
                    "Get user info from the gateway "
                    "authentication"
                >>,
                parameters => params_gateway_name_in_path() ++
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
                    params_userid_in_path(),
                responses =>
                    ?STANDARD_RESP(#{204 => <<"User Deleted">>})
            }
    };
schema("/gateway/:name/authentication/import_users") ->
    #{
        'operationId' => import_users,
        post =>
            #{
                desc => <<"Import users into the gateway authentication">>,
                parameters => params_gateway_name_in_path(),
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

params_fuzzy_in_qs() ->
    [
        {like_username,
            mk(
                binary(),
                #{
                    in => query,
                    required => false,
                    desc => <<"Fuzzy search by username">>,
                    example => <<"username">>
                }
            )},
        {like_clientid,
            mk(
                binary(),
                #{
                    in => query,
                    required => false,
                    desc => <<"Fuzzy search by clientid">>,
                    example => <<"clientid">>
                }
            )}
    ].

%%--------------------------------------------------------------------
%% schemas

schema_authn() ->
    emqx_dashboard_swagger:schema_with_examples(
        emqx_authn_schema:authenticator_type(),
        emqx_authn_api:authenticator_examples()
    ).
