%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_gcp_pubsub_schema_lib).

%% API
-export([
    authentication_field/0
]).
-export([
    legacy_service_account_json_root_converter/2,
    service_account_json_validator/1
]).

%% `hocon_schema` API
-export([
    namespace/0,
    fields/1,
    desc/1
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-include_lib("hocon/include/hoconsc.hrl").

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

authentication_field() ->
    {authentication,
        mk(
            emqx_schema:mkunion(type, #{
                <<"service_account_json">> => ref(auth_service_account_json),
                <<"wif">> => ref(auth_wif)
            }),
            #{
                required => true,
                desc => ?DESC(authentication)
            }
        )}.

-spec service_account_json_validator(binary()) ->
    ok
    | {error, {wrong_type, term()}}
    | {error, {missing_keys, [binary()]}}.
service_account_json_validator(Val) ->
    case emqx_utils_json:safe_decode(Val) of
        {ok, Map} ->
            ExpectedKeys = [
                <<"type">>,
                <<"project_id">>,
                <<"private_key_id">>,
                <<"private_key">>,
                <<"client_email">>
            ],
            MissingKeys = lists:sort([
                K
             || K <- ExpectedKeys,
                not maps:is_key(K, Map)
            ]),
            Type = maps:get(<<"type">>, Map, null),
            case {MissingKeys, Type} of
                {[], <<"service_account">>} ->
                    ok;
                {[], Type} ->
                    {error, #{wrong_type => Type}};
                {_, _} ->
                    {error, #{missing_keys => MissingKeys}}
            end;
        {error, _} ->
            {error, "not a json"}
    end.

legacy_service_account_json_root_converter(Conf0, _HoconOpts) ->
    case Conf0 of
        #{<<"service_account_json">> := SAJ0} ->
            Conf = maps:remove(<<"service_account_json">>, Conf0),
            %% Older schema was an object instead of a binary.
            SAJ =
                case is_map(SAJ0) of
                    true -> emqx_utils_json:encode(SAJ0);
                    false -> SAJ0
                end,
            Conf#{
                <<"authentication">> => #{
                    <<"type">> => <<"service_account_json">>,
                    <<"service_account_json">> => SAJ
                }
            };
        _ ->
            Conf0
    end.

%%------------------------------------------------------------------------------
%% `hocon_schema` API
%%------------------------------------------------------------------------------

namespace() ->
    "gcp_pubsub".

fields(auth_service_account_json) ->
    [
        {type,
            mk(service_account_json, #{
                required => true, desc => ?DESC("auth_service_account_json")
            })},
        {service_account_json,
            mk(
                binary(),
                #{
                    required => false,
                    validator => fun ?MODULE:service_account_json_validator/1,
                    sensitive => true,
                    desc => ?DESC("service_account_json")
                }
            )}
    ];
fields(auth_wif) ->
    [
        {type,
            mk(wif, #{
                required => true,
                desc => ?DESC("auth_wif")
            })},
        {initial_token,
            mk(
                emqx_schema:mkunion(type, #{
                    <<"oidc_client_credentials">> => ref(auth_wif_oidc_client_credentials)
                }),
                #{
                    required => true,
                    desc => ?DESC("auth_wif_initial_token")
                }
            )},
        {service_account_email,
            mk(binary(), #{
                required => true,
                desc => ?DESC("auth_wif_service_account_email")
            })},
        {gcp_project_id,
            mk(binary(), #{
                required => true,
                desc => ?DESC("auth_wif_gcp_project_id")
            })},
        {gcp_project_number,
            mk(binary(), #{
                required => true,
                desc => ?DESC("auth_wif_gcp_project_number")
            })},
        {gcp_wif_pool_id,
            mk(binary(), #{
                required => true,
                desc => ?DESC("auth_wif_gcp_wif_pool_id")
            })},
        {gcp_wif_pool_provider_id,
            mk(binary(), #{
                required => true,
                desc => ?DESC("auth_wif_gcp_wif_pool_provider_id")
            })}
    ];
fields(auth_wif_oidc_client_credentials) ->
    [
        {type,
            mk(oidc_client_credentials, #{
                required => true,
                desc => ?DESC("auth_wif_oidc_client_credentials")
            })},
        {endpoint_uri,
            mk(binary(), #{
                required => true,
                desc => ?DESC("auth_wif_oidc_client_credentials_endpoint_uri")
            })},
        {client_id,
            mk(binary(), #{
                required => true,
                desc => ?DESC("auth_wif_oidc_client_credentials_client_id")
            })},
        {client_secret,
            emqx_schema_secret:mk(#{
                required => true,
                sensitive => true,
                desc => ?DESC("auth_wif_oidc_client_credentials_client_secret")
            })},
        {scope,
            mk(binary(), #{
                required => true,
                desc => ?DESC("auth_wif_oidc_client_credentials_scope")
            })}
    ].

desc(authentication) ->
    ?DESC(authentication);
desc(auth_service_account_json) ->
    ?DESC(auth_service_account_json);
desc(auth_wif) ->
    ?DESC(auth_wif);
desc(auth_wif_oidc_client_credentials) ->
    ?DESC(auth_wif_oidc_client_credentials);
desc(_) ->
    undefined.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

mk(Type, Meta) -> hoconsc:mk(Type, Meta).
ref(StructName) -> hoconsc:ref(?MODULE, StructName).
