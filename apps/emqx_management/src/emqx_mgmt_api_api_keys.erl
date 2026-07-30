%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mgmt_api_api_keys).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_dashboard/include/emqx_dashboard_rbac.hrl").
-include_lib("emqx_utils/include/emqx_api_key_scopes.hrl").

-export([api_spec/0, fields/1, paths/0, schema/1, namespace/0]).
-export([api_key/2, api_key_by_name/2, api_key_scopes/2]).
-export([validate_name/1]).

-export([scopes/0]).

-define(TAGS, [<<"API Keys">>]).

namespace() -> "api_key".

scopes() ->
    %% API key management endpoints are bearer-auth-only; API keys
    %% themselves cannot reach these paths. The login user scope
    %% check consults this map.
    %%
    %% /api_key_scopes is marked ?SCOPE_PUBLIC: it returns only the
    %% static scope catalog (names + i18n descriptions), no tenant
    %% data, so any authenticated login user may read it. It is a
    %% top-level path (sibling to /action_types, /source_types)
    %% chosen to avoid wildcard routing collisions with /api_key/:name.
    #{
        <<"/api_key">> => ?SCOPE_API_KEY_MGMT,
        <<"/api_key/:name">> => ?SCOPE_API_KEY_MGMT,
        <<"/api_key_scopes">> => ?SCOPE_PUBLIC
    }.

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true, translate_body => true}).

paths() ->
    ["/api_key", "/api_key/:name", "/api_key_scopes"].

schema("/api_key") ->
    #{
        'operationId' => api_key,
        get => #{
            description => ?DESC(api_key_list),
            tags => ?TAGS,
            security => [#{'bearerAuth' => []}],
            responses => #{
                200 => delete([api_secret], fields(app_response))
            }
        },
        post => #{
            description => ?DESC(create_new_api_key),
            tags => ?TAGS,
            security => [#{'bearerAuth' => []}],
            'requestBody' => delete([created_at, api_key, api_secret], fields(app)),
            responses => #{
                200 => hoconsc:ref(app_response),
                400 => emqx_dashboard_swagger:error_codes(['BAD_REQUEST'])
            }
        }
    };
schema("/api_key/:name") ->
    #{
        'operationId' => api_key_by_name,
        get => #{
            description => ?DESC(get_api_key),
            tags => ?TAGS,
            parameters => [hoconsc:ref(name)],
            responses => #{
                200 => delete([api_secret], fields(app_response)),
                404 => emqx_dashboard_swagger:error_codes(['NOT_FOUND'])
            }
        },
        put => #{
            description => ?DESC(update_api_key),
            tags => ?TAGS,
            parameters => [hoconsc:ref(name)],
            'requestBody' => delete([created_at, api_key, api_secret, name], fields(app)),
            responses => #{
                200 => delete([api_secret], fields(app_response)),
                404 => emqx_dashboard_swagger:error_codes(['NOT_FOUND'])
            }
        },
        delete => #{
            description => ?DESC(delete_api_key),
            tags => ?TAGS,
            parameters => [hoconsc:ref(name)],
            responses => #{
                204 => <<"Delete successfully">>,
                404 => emqx_dashboard_swagger:error_codes(['NOT_FOUND'])
            }
        }
    };
schema("/api_key_scopes") ->
    #{
        'operationId' => api_key_scopes,
        get => #{
            description => ?DESC(api_key_scopes_list),
            tags => ?TAGS,
            security => [#{'bearerAuth' => []}],
            responses => #{
                200 => hoconsc:ref(?MODULE, scopes_response)
            }
        }
    }.

fields(app) ->
    [
        {name,
            hoconsc:mk(
                binary(),
                #{
                    desc => "Unique and format by [a-zA-Z0-9-_]",
                    validator => fun ?MODULE:validate_name/1,
                    example => <<"EMQX-API-KEY-1">>
                }
            )},
        {api_key,
            hoconsc:mk(
                binary(),
                #{
                    desc => "" "TODO:uses HMAC-SHA256 for signing." "",
                    example => <<"a4697a5c75a769f6">>
                }
            )},
        {api_secret,
            hoconsc:mk(
                binary(),
                #{
                    desc =>
                        ""
                        "An API secret is a simple encrypted string that identifies"
                        ""
                        ""
                        "an application without any principal."
                        ""
                        ""
                        "They are useful for accessing public data anonymously,"
                        ""
                        ""
                        "and are used to associate API requests."
                        "",
                    example => <<"MzAyMjk3ODMwMDk0NjIzOTUxNjcwNzQ0NzQ3MTE2NDYyMDI">>
                }
            )},
        {expired_at,
            hoconsc:mk(
                hoconsc:union([infinity, emqx_utils_calendar:epoch_second()]),
                #{
                    desc => "No longer valid datetime",
                    example => <<"2021-12-05T02:01:34.186Z">>,
                    required => false,
                    default => infinity
                }
            )},
        {created_at,
            hoconsc:mk(
                emqx_utils_calendar:epoch_second(),
                #{
                    desc => "ApiKey create datetime",
                    example => <<"2021-12-01T00:00:00.000Z">>
                }
            )},
        {desc,
            hoconsc:mk(
                binary(),
                #{example => <<"Note">>, required => false}
            )},
        {enable, hoconsc:mk(boolean(), #{desc => "Enable/Disable", required => false})},
        {expired, hoconsc:mk(boolean(), #{desc => "Expired", required => false})},
        %% Accepts the same shapes the response emits (array or the `unset'
        %% sentinel) so a read-modify-write can round-trip the value verbatim.
        {scopes,
            hoconsc:mk(
                hoconsc:union([unset, hoconsc:array(binary())]),
                #{
                    desc => ?DESC(api_key_scopes_request),
                    required => false,
                    example => [<<"clients">>, <<"rules">>]
                }
            )}
    ] ++ app_extend_fields();
%% Response shape: `scopes' MAY be the binary sentinel <<"unset">> in addition
%% to the array-of-binaries form, surfaced when the record holds no explicit
%% `scopes' field (legacy record or unset-equivalent write).
%%
%% Listed explicitly (rather than overriding via `lists:keystore') so that the
%% OpenAPI spec reads as a self-contained response schema and reviewers do not
%% have to mentally diff `fields(app)' against `fields(app_response)'.
fields(app_response) ->
    [
        {name,
            hoconsc:mk(
                binary(),
                #{
                    desc => "Unique and format by [a-zA-Z0-9-_]",
                    validator => fun ?MODULE:validate_name/1,
                    example => <<"EMQX-API-KEY-1">>
                }
            )},
        {api_key,
            hoconsc:mk(
                binary(),
                #{
                    desc => "" "TODO:uses HMAC-SHA256 for signing." "",
                    example => <<"a4697a5c75a769f6">>
                }
            )},
        {api_secret,
            hoconsc:mk(
                binary(),
                #{
                    desc =>
                        ""
                        "An API secret is a simple encrypted string that identifies"
                        ""
                        ""
                        "an application without any principal."
                        ""
                        ""
                        "They are useful for accessing public data anonymously,"
                        ""
                        ""
                        "and are used to associate API requests."
                        "",
                    example => <<"MzAyMjk3ODMwMDk0NjIzOTUxNjcwNzQ0NzQ3MTE2NDYyMDI">>
                }
            )},
        {expired_at,
            hoconsc:mk(
                hoconsc:union([infinity, emqx_utils_calendar:epoch_second()]),
                #{
                    desc => "No longer valid datetime",
                    example => <<"2021-12-05T02:01:34.186Z">>,
                    required => false,
                    default => infinity
                }
            )},
        {created_at,
            hoconsc:mk(
                emqx_utils_calendar:epoch_second(),
                #{
                    desc => "ApiKey create datetime",
                    example => <<"2021-12-01T00:00:00.000Z">>
                }
            )},
        {desc,
            hoconsc:mk(
                binary(),
                #{example => <<"Note">>, required => false}
            )},
        {enable, hoconsc:mk(boolean(), #{desc => "Enable/Disable", required => false})},
        {expired, hoconsc:mk(boolean(), #{desc => "Expired", required => false})},
        {scopes,
            hoconsc:mk(
                hoconsc:union([unset, hoconsc:array(binary())]),
                #{
                    desc => ?DESC(api_key_scopes_response),
                    required => false,
                    example => [<<"clients">>, <<"rules">>]
                }
            )}
    ] ++ app_extend_fields();
fields(name) ->
    [
        {name,
            hoconsc:mk(
                binary(),
                #{
                    desc => <<"^[A-Za-z]+[A-Za-z0-9-_]*$">>,
                    example => <<"EMQX-API-KEY-1">>,
                    in => path,
                    validator => fun ?MODULE:validate_name/1
                }
            )}
    ];
fields(scope_info) ->
    [
        {name,
            hoconsc:mk(
                binary(),
                #{
                    desc => ?DESC(scope_info_name),
                    example => <<"connections">>
                }
            )},
        {desc,
            hoconsc:mk(
                binary(),
                #{
                    desc => ?DESC(scope_info_desc),
                    example => <<
                        "Client connections, subscriptions, topics, banning, "
                        "retained messages, file transfer, and delayed messages"
                    >>
                }
            )}
    ];
fields(scopes_response) ->
    [
        {scopes,
            hoconsc:mk(
                hoconsc:array(hoconsc:ref(scope_info)),
                #{
                    desc => ?DESC(scopes_response_scopes)
                }
            )}
    ].

-define(NAME_RE, "^[A-Za-z]+[A-Za-z0-9-_]*$").

validate_name(Name) ->
    NameLen = byte_size(Name),
    case NameLen > 0 andalso NameLen =< 256 of
        true ->
            case re:run(Name, ?NAME_RE) of
                nomatch -> {error, "Name should be " ?NAME_RE};
                _ -> ok
            end;
        false ->
            {error, "Name Length must =< 256"}
    end.

delete(Keys, Fields) ->
    lists:foldl(fun(Key, Acc) -> lists:keydelete(Key, 1, Acc) end, Fields, Keys).

api_key(get, _) ->
    {200, [emqx_mgmt_auth:format(App) || App <- emqx_mgmt_auth:list()]};
api_key(post, #{body := App}) ->
    #{
        <<"name">> := Name,
        <<"desc">> := Desc0,
        <<"enable">> := Enable
    } = App,
    ExpiredAt = ensure_expired_at(App),
    Desc = unicode:characters_to_binary(Desc0, unicode),
    Role = maps:get(<<"role">>, App, ?ROLE_API_DEFAULT),
    RawScopes = maps:get(<<"scopes">>, App, undefined),
    case create_scopes(Role, RawScopes) of
        {ok, Scopes} ->
            do_create_api_key(Name, Enable, ExpiredAt, Desc, Role, Scopes);
        {error, Msg} ->
            {400, #{code => 'BAD_REQUEST', message => Msg}}
    end.

%% Resolve the `scopes' value to persist on POST: omitted -> materialize
%% the role default (privilege mutex not applied to that mix);
%% unset-equivalent -> store no `scopes' field (valid by construction);
%% explicit list -> validate and store verbatim.
create_scopes(Role, undefined) ->
    Scopes = emqx_mgmt_auth:role_default_scopes(Role),
    case validate_scopes(Role, undefined, Scopes) of
        ok -> {ok, Scopes};
        Error -> Error
    end;
create_scopes(Role, RawScopes) ->
    case emqx_mgmt_auth:write_scope_intent(Role, RawScopes) of
        unset ->
            {ok, undefined};
        {set, Scopes} ->
            case validate_scopes(Role, Scopes, Scopes) of
                ok -> {ok, Scopes};
                Error -> Error
            end
    end.

do_create_api_key(Name, Enable, ExpiredAt, Desc, Role, Scopes) ->
    case emqx_mgmt_auth:create(Name, Enable, ExpiredAt, Desc, Role, Scopes) of
        {ok, NewApp} ->
            {200, emqx_mgmt_auth:format(NewApp)};
        {error, Reason} ->
            {400, #{
                code => 'BAD_REQUEST',
                message => iolist_to_binary(io_lib:format("~p", [Reason]))
            }}
    end.

-define(NOT_FOUND_RESPONSE, #{code => 'NOT_FOUND', message => <<"Name NOT FOUND">>}).

api_key_by_name(get, #{bindings := #{name := Name}}) ->
    case emqx_mgmt_auth:read(Name) of
        {ok, App} -> {200, emqx_mgmt_auth:format(App)};
        {error, not_found} -> {404, ?NOT_FOUND_RESPONSE}
    end;
api_key_by_name(delete, #{bindings := #{name := Name}}) ->
    case emqx_mgmt_auth:delete(Name) of
        {ok, _} -> {204};
        {error, not_found} -> {404, ?NOT_FOUND_RESPONSE}
    end;
api_key_by_name(put, #{bindings := #{name := Name}, body := Body}) ->
    Enable = maps:get(<<"enable">>, Body, undefined),
    ExpiredAt = ensure_expired_at(Body),
    Desc = maps:get(<<"desc">>, Body, undefined),
    Role = maps:get(<<"role">>, Body, ?ROLE_API_DEFAULT),
    RawScopes = maps:get(<<"scopes">>, Body, undefined),
    Intent = emqx_mgmt_auth:write_scope_intent(Role, RawScopes),
    case validate_update_scopes(Role, Name, Intent) of
        ok ->
            Scopes = scopes_update_arg(Intent),
            case emqx_mgmt_auth:update(Name, Enable, ExpiredAt, Desc, Role, Scopes) of
                {ok, App} ->
                    {200, emqx_mgmt_auth:format(App)};
                {error, not_found} ->
                    {404, ?NOT_FOUND_RESPONSE};
                {error, Reason} ->
                    {400, #{
                        code => 'BAD_REQUEST',
                        message => iolist_to_binary(io_lib:format("~p", [Reason]))
                    }}
            end;
        {error, Msg} ->
            {400, #{code => 'BAD_REQUEST', message => Msg}}
    end.

%% Validate the effective scope list for a PUT. `keep' validates the
%% persisted scopes against the (possibly changed) role, so a role
%% change to `publisher' cannot keep non-`publish' scopes via a partial
%% update; `unset' clears to role default (valid by construction);
%% `{set, L}' validates `L' with the privilege mutex applied.
validate_update_scopes(_Role, _Name, unset) ->
    ok;
validate_update_scopes(Role, Name, keep) ->
    validate_scopes(Role, undefined, persisted_scopes(Name));
validate_update_scopes(Role, _Name, {set, Scopes}) ->
    validate_scopes(Role, Scopes, Scopes).

%% Intent -> `Scopes' argument of `emqx_mgmt_auth:update/6'.
scopes_update_arg(keep) -> undefined;
scopes_update_arg(unset) -> unset;
scopes_update_arg({set, Scopes}) -> Scopes.

%% A missing key surfaces as `undefined' so validation passes and the
%% downstream `emqx_mgmt_auth:update/6' returns the proper 404.
persisted_scopes(Name) ->
    case emqx_mgmt_auth:read(Name) of
        {ok, #{scopes := Persisted}} when is_list(Persisted) -> Persisted;
        _ -> undefined
    end.

ensure_expired_at(#{<<"expired_at">> := ExpiredAt}) when is_integer(ExpiredAt) -> ExpiredAt;
ensure_expired_at(_) -> infinity.

api_key_scopes(get, _) ->
    Scopes = [resolve_scope_desc(S) || S <- emqx_scope_catalog:scope_catalog()],
    {200, #{
        scopes => Scopes
    }}.

resolve_scope_desc(#{desc := Desc} = Scope) ->
    Scope#{desc => emqx_dashboard_swagger:get_i18n(<<"desc">>, Desc, <<>>, #{})}.

%% Four-layer schema validation, returning the FIRST error encountered.
%% Layers 1-3 run against `EffectiveScopes' (the materialised list —
%% request body when supplied, otherwise role default / persisted
%% scopes). Layer 4 (the privilege-scope mutex) runs against
%% `RawScopes' — the value the client actually sent — so that an
%% omitted scope list (which materialises to the administrator role
%% default, itself a mix of privilege and non-privilege scopes) is
%% treated as the unrestricted case rather than an explicit mixed list.
validate_scopes(Role, RawScopes, EffectiveScopes) ->
    case validate_publisher_scopes(Role, EffectiveScopes) of
        ok ->
            case validate_no_login_only_scopes(EffectiveScopes) of
                ok ->
                    case validate_scopes_in_catalog(EffectiveScopes) of
                        ok -> emqx_scope_catalog:check_privilege_scope_mutex(RawScopes);
                        Error -> Error
                    end;
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

%% Layer 1: publisher role can only hold the `publish' scope (or an
%% empty / absent scope list, which falls back to RBAC `?ROLE_API_PUBLISHER'
%% hardcoded path matching). Defense-in-depth — the runtime path check
%% in emqx_dashboard_rbac already restricts publishers to /publish and
%% /publish/bulk; this validator prevents misconfiguration where an
%% operator assigns a meaningless scope list to a publisher key.
validate_publisher_scopes(?ROLE_API_PUBLISHER, undefined) ->
    ok;
validate_publisher_scopes(?ROLE_API_PUBLISHER, []) ->
    ok;
validate_publisher_scopes(?ROLE_API_PUBLISHER, [?SCOPE_PUBLISH]) ->
    ok;
validate_publisher_scopes(?ROLE_API_PUBLISHER, _Other) ->
    {error, <<"Publisher API keys can only hold the 'publish' scope">>};
validate_publisher_scopes(_OtherRole, _Scopes) ->
    ok.

%% Layer 2: API keys (regardless of role) must not hold login-only
%% scopes. These four scopes are reserved for dashboard login users.
validate_no_login_only_scopes(undefined) ->
    ok;
validate_no_login_only_scopes(Scopes) when is_list(Scopes) ->
    case [S || S <- Scopes, lists:member(S, ?LOGIN_ONLY_SCOPES)] of
        [] ->
            ok;
        Conflicts ->
            Names = lists:join(<<", ">>, Conflicts),
            Msg = iolist_to_binary([
                <<"API keys cannot hold login-only scopes: ">>, Names
            ]),
            {error, Msg}
    end;
validate_no_login_only_scopes(_) ->
    ok.

%% Layer 3: scope names must exist in the catalog (unknown name -> 400).
validate_scopes_in_catalog(undefined) ->
    ok;
validate_scopes_in_catalog(Scopes) when is_list(Scopes) ->
    emqx_mgmt_api_key_scopes:validate_scopes(Scopes);
validate_scopes_in_catalog(_) ->
    {error, <<"scopes must be a list of strings">>}.

-if(?EMQX_RELEASE_EDITION == ee).

app_extend_fields() ->
    [
        {role,
            hoconsc:mk(binary(), #{
                desc => ?DESC(role),
                default => ?ROLE_API_DEFAULT,
                example => ?ROLE_API_DEFAULT,
                validator => fun emqx_dashboard_rbac:valid_api_role/1
            })}
    ].

-else.

app_extend_fields() ->
    [].

-endif.
