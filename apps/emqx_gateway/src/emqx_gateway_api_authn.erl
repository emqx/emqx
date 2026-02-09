%%--------------------------------------------------------------------
%% Copyright (c) 2021-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_gateway_api_authn).

-behaviour(minirest_api).

-include_lib("emqx_utils/include/emqx_http_api.hrl").
-include("emqx_gateway_http.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-import(hoconsc, [mk/2, ref/2]).
-import(emqx_dashboard_swagger, [error_codes/2]).

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
    users_insta/2
]).

%% internal export for emqx_gateway_api_listeners module
-export([schema_authn/0]).

-define(TAGS, [<<"Gateway Authentication">>]).

-define(NAMESPACE, ?global_ns).

%%--------------------------------------------------------------------
%% minirest behaviour callbacks
%%--------------------------------------------------------------------

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/gateways/:name/authentication",
        "/gateways/:name/authentication/users",
        "/gateways/:name/authentication/users/:uid"
    ].

%%--------------------------------------------------------------------
%% http handlers

authn(get, #{bindings := #{name := Name0}}) ->
    with_gateway(Name0, fun(GwName, _) ->
        try emqx_gateway_http:authn(GwName) of
            Authn -> {200, Authn}
        catch
            error:{config_not_found, _} ->
                %% FIXME: should return 404?
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

users(get, #{query_string := Qs} = Req) ->
    #{gateway := #{id := AuthId, chain_name := ChainName}} = Req,
    emqx_authn_api:list_users(ChainName, AuthId, parse_qstring(Qs));
users(post, #{body := Body} = Req) ->
    #{gateway := #{id := AuthId, chain_name := ChainName}} = Req,
    emqx_authn_api:add_user(ChainName, AuthId, Body).

users_insta(get, #{bindings := #{uid := UserId}} = Req) ->
    #{
        gateway := #{id := AuthId, chain_name := ChainName},
        resolved_ns := Namespace
    } = Req,
    emqx_authn_api:find_user(ChainName, Namespace, AuthId, UserId);
users_insta(put, #{bindings := #{uid := UserId}, body := Body} = Req) ->
    #{
        gateway := #{id := AuthId, chain_name := ChainName},
        resolved_ns := Namespace
    } = Req,
    emqx_authn_api:update_user(ChainName, Namespace, AuthId, UserId, Body);
users_insta(delete, #{bindings := #{uid := UserId}} = Req) ->
    #{
        gateway := #{id := AuthId, chain_name := ChainName},
        resolved_ns := Namespace
    } = Req,
    emqx_authn_api:delete_user(ChainName, Namespace, AuthId, UserId).

%%--------------------------------------------------------------------
%% Utils

parse_qstring(Qs) ->
    maps:with(
        [
            <<"page">>,
            <<"limit">>,
            <<"like_user_id">>,
            <<"is_superuser">>
        ],
        Qs
    ).

%%--------------------------------------------------------------------
%% Swagger defines
%%--------------------------------------------------------------------

schema("/gateways/:name/authentication") ->
    #{
        'operationId' => authn,
        get =>
            #{
                tags => ?TAGS,
                desc => ?DESC(get_authn),
                parameters => params_gateway_name_in_path(),
                responses =>
                    ?STANDARD_RESP(
                        #{
                            200 => schema_authn(),
                            204 => <<"Authenticator not initialized">>
                        }
                    )
            },
        put =>
            #{
                tags => ?TAGS,
                desc => ?DESC(update_authn),
                parameters => params_gateway_name_in_path(),
                'requestBody' => schema_authn(),
                responses =>
                    ?STANDARD_RESP(#{200 => schema_authn()})
            },
        post =>
            #{
                tags => ?TAGS,
                desc => ?DESC(add_authn),
                parameters => params_gateway_name_in_path(),
                'requestBody' => schema_authn(),
                responses =>
                    ?STANDARD_RESP(#{201 => schema_authn()})
            },
        delete =>
            #{
                tags => ?TAGS,
                desc => ?DESC(delete_authn),
                parameters => params_gateway_name_in_path(),
                responses =>
                    ?STANDARD_RESP(#{204 => <<"Deleted">>})
            }
    };
schema("/gateways/:name/authentication/users") ->
    #{
        'operationId' => users,
        filter => fun emqx_gateway_http:filter/2,
        get =>
            #{
                tags => ?TAGS,
                desc => ?DESC(list_users),
                summary => <<"List users for gateway authenticator">>,
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
                tags => ?TAGS,
                desc => ?DESC(add_user),
                summary => <<"Add user for gateway authenticator">>,
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
schema("/gateways/:name/authentication/users/:uid") ->
    #{
        'operationId' => users_insta,
        filter => fun emqx_gateway_http:filter/2,
        get =>
            #{
                tags => ?TAGS,
                desc => ?DESC(get_user),
                summary => <<"Get user info for gateway authenticator">>,
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
                tags => ?TAGS,
                desc => ?DESC(update_user),
                summary => <<"Update user info for gateway authenticator">>,
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
                tags => ?TAGS,
                desc => ?DESC(delete_user),
                summary => <<"Delete user for gateway authenticator">>,
                parameters => params_gateway_name_in_path() ++
                    params_userid_in_path(),
                responses =>
                    ?STANDARD_RESP(#{204 => <<"User Deleted">>})
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

params_userid_in_path() ->
    [
        {uid,
            mk(
                binary(),
                #{
                    in => path,
                    desc => ?DESC(user_id),
                    example => <<"test_username">>
                }
            )}
    ].

params_paging_in_qs() ->
    emqx_dashboard_swagger:fields(page) ++
        emqx_dashboard_swagger:fields(limit).

params_fuzzy_in_qs() ->
    [
        {like_user_id,
            mk(
                binary(),
                #{
                    in => query,
                    required => false,
                    desc => ?DESC(like_user_id),
                    example => <<"test_">>
                }
            )},
        {is_superuser,
            mk(
                boolean(),
                #{
                    in => query,
                    required => false,
                    desc => ?DESC(is_superuser)
                }
            )}
    ].

%%--------------------------------------------------------------------
%% schemas

schema_authn() ->
    emqx_dashboard_swagger:schema_with_examples(
        emqx_authn_schema:authenticator_type_without([
            emqx_authn_scram_mnesia_schema,
            emqx_authn_scram_restapi_schema
        ]),
        emqx_authn_api:authenticator_examples()
    ).

with_gateway(GwName, Fun) ->
    emqx_gateway_http:with_gateway(GwName, Fun).
