%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mgmt_api_api_keys).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_dashboard/include/emqx_dashboard_rbac.hrl").
-include_lib("emqx_utils/include/emqx_api_key_scopes.hrl").
-include_lib("emqx/include/emqx_config.hrl").

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
                400 => emqx_dashboard_swagger:error_codes(['BAD_REQUEST']),
                403 => emqx_dashboard_swagger:error_codes(['FORBIDDEN'])
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
                400 => emqx_dashboard_swagger:error_codes(['BAD_REQUEST']),
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
                    desc => ?DESC("name_format"),
                    validator => fun ?MODULE:validate_name/1,
                    example => <<"EMQX-API-KEY-1">>
                }
            )},
        {api_key,
            hoconsc:mk(
                binary(),
                #{
                    desc => ?DESC("api_key_desc"),
                    example => <<"a4697a5c75a769f6">>
                }
            )},
        {api_secret,
            hoconsc:mk(
                binary(),
                #{
                    desc => ?DESC("api_secret_desc"),
                    example => <<"MzAyMjk3ODMwMDk0NjIzOTUxNjcwNzQ0NzQ3MTE2NDYyMDI">>
                }
            )},
        {expired_at,
            hoconsc:mk(
                hoconsc:union([infinity, emqx_utils_calendar:epoch_second()]),
                #{
                    desc => ?DESC("expired_at_desc"),
                    example => <<"2021-12-05T02:01:34.186Z">>,
                    required => false,
                    default => infinity
                }
            )},
        {created_at,
            hoconsc:mk(
                emqx_utils_calendar:epoch_second(),
                #{
                    desc => ?DESC("created_at_desc"),
                    example => <<"2021-12-01T00:00:00.000Z">>
                }
            )},
        {desc,
            hoconsc:mk(
                binary(),
                #{example => <<"Note">>, required => false}
            )},
        {enable, hoconsc:mk(boolean(), #{desc => ?DESC("enable_desc"), required => false})},
        {expired, hoconsc:mk(boolean(), #{desc => ?DESC("expired_desc"), required => false})},
        {scopes,
            hoconsc:mk(
                hoconsc:array(binary()),
                #{
                    desc => ?DESC(api_key_scopes_request),
                    required => false,
                    example => [<<"clients">>, <<"rules">>]
                }
            )}
    ] ++ app_extend_fields();
%% Response shape: `scopes' MAY be the binary sentinel <<"unset">> in addition
%% to the array-of-binaries form. This sentinel surfaces only for legacy
%% records that survived an upgrade from a release where the scopes feature
%% did not exist; the POST / bootstrap / SSO-provisioning paths all
%% materialize role-default scopes at creation time, so no fresh record will
%% ever appear with `scopes => <<"unset">>'.
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
                    desc => ?DESC("name_format"),
                    validator => fun ?MODULE:validate_name/1,
                    example => <<"EMQX-API-KEY-1">>
                }
            )},
        {api_key,
            hoconsc:mk(
                binary(),
                #{
                    desc => ?DESC("api_key_desc"),
                    example => <<"a4697a5c75a769f6">>
                }
            )},
        {api_secret,
            hoconsc:mk(
                binary(),
                #{
                    desc => ?DESC("api_secret_desc"),
                    example => <<"MzAyMjk3ODMwMDk0NjIzOTUxNjcwNzQ0NzQ3MTE2NDYyMDI">>
                }
            )},
        {expired_at,
            hoconsc:mk(
                hoconsc:union([infinity, emqx_utils_calendar:epoch_second()]),
                #{
                    desc => ?DESC("expired_at_desc"),
                    example => <<"2021-12-05T02:01:34.186Z">>,
                    required => false,
                    default => infinity
                }
            )},
        {created_at,
            hoconsc:mk(
                emqx_utils_calendar:epoch_second(),
                #{
                    desc => ?DESC("created_at_desc"),
                    example => <<"2021-12-01T00:00:00.000Z">>
                }
            )},
        {desc,
            hoconsc:mk(
                binary(),
                #{example => <<"Note">>, required => false}
            )},
        {enable, hoconsc:mk(boolean(), #{desc => ?DESC("enable_desc"), required => false})},
        {expired, hoconsc:mk(boolean(), #{desc => ?DESC("expired_desc"), required => false})},
        {scopes,
            hoconsc:mk(
                hoconsc:union([unset, hoconsc:array(binary())]),
                #{
                    desc => ?DESC(api_key_scopes_response),
                    required => false,
                    example => [<<"connections">>, <<"monitoring">>]
                }
            )}
    ] ++ app_extend_fields();
fields(name) ->
    [
        {name,
            hoconsc:mk(
                binary(),
                #{
                    desc => ?DESC("name_pattern"),
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

api_key(get, Req) ->
    CallerNs = emqx_dashboard:get_namespace(Req),
    Apps = [App || App <- emqx_mgmt_auth:list(), namespace_accessible(CallerNs, App)],
    {200, [emqx_mgmt_auth:format(App) || App <- Apps]};
api_key(post, #{body := App} = Req) ->
    #{
        <<"name">> := Name,
        <<"desc">> := Desc0,
        <<"enable">> := Enable
    } = App,
    ExpiredAt = ensure_expired_at(App),
    Desc = unicode:characters_to_binary(Desc0, unicode),
    Role0 = maps:get(<<"role">>, App, ?ROLE_API_DEFAULT),
    Namespace = maps:get(<<"namespace">>, App, undefined),
    CallerNs = emqx_dashboard:get_namespace(Req),
    maybe
        {ok, Role} ?= resolve_effective_role(Role0, Namespace),
        ok ?= authorize_target_namespace(CallerNs, role_namespace(Role)),
        create_api_key(App, Name, Enable, ExpiredAt, Desc, Role)
    else
        {error, forbidden, Msg} ->
            {403, #{code => 'FORBIDDEN', message => Msg}};
        {error, Msg} ->
            {400, #{code => 'BAD_REQUEST', message => Msg}}
    end.

create_api_key(App, Name, Enable, ExpiredAt, Desc, Role) ->
    %% Materialize role defaults when the client omitted `scopes' entirely.
    %% Explicit `[]' (deny-all) and explicit lists pass through unchanged.
    %% After this PR `<<"unset">>' in a GET response is reserved for legacy
    %% records that survived an upgrade; no creation path stores `undefined'.
    RawScopes = maps:get(<<"scopes">>, App, undefined),
    Scopes = emqx_mgmt_auth:effective_scopes_on_create(Role, RawScopes),
    case validate_scopes(Role, RawScopes, Scopes) of
        ok ->
            case emqx_mgmt_auth:create(Name, Enable, ExpiredAt, Desc, Role, Scopes) of
                {ok, NewApp} ->
                    {200, emqx_mgmt_auth:format(NewApp)};
                {error, Reason} when is_map(Reason) ->
                    {400, #{
                        code => 'BAD_REQUEST',
                        message => Reason
                    }};
                {error, Reason} ->
                    {400, #{
                        code => 'BAD_REQUEST',
                        message => iolist_to_binary(io_lib:format("~p", [Reason]))
                    }}
            end;
        {error, Msg} ->
            {400, #{code => 'BAD_REQUEST', message => Msg}}
    end.

-define(NOT_FOUND_RESPONSE, #{code => 'NOT_FOUND', message => ?DESC("name_not_found")}).

api_key_by_name(get, #{bindings := #{name := Name}} = Req) ->
    CallerNs = emqx_dashboard:get_namespace(Req),
    case read_in_namespace(CallerNs, Name) of
        {ok, App} -> {200, emqx_mgmt_auth:format(App)};
        {error, not_found} -> {404, ?NOT_FOUND_RESPONSE}
    end;
api_key_by_name(delete, #{bindings := #{name := Name}} = Req) ->
    CallerNs = emqx_dashboard:get_namespace(Req),
    case read_in_namespace(CallerNs, Name) of
        {ok, _App} ->
            case emqx_mgmt_auth:delete(Name) of
                {ok, _} -> {204};
                {error, not_found} -> {404, ?NOT_FOUND_RESPONSE}
            end;
        {error, not_found} ->
            {404, ?NOT_FOUND_RESPONSE}
    end;
api_key_by_name(put, #{bindings := #{name := Name}, body := Body} = Req) ->
    CallerNs = emqx_dashboard:get_namespace(Req),
    Role0 = maps:get(<<"role">>, Body, ?ROLE_API_DEFAULT),
    Namespace = maps:get(<<"namespace">>, Body, undefined),
    case read_in_namespace(CallerNs, Name) of
        {ok, _App} ->
            case resolve_effective_role(Role0, Namespace) of
                {ok, Role} ->
                    %% A move into another namespace is rejected with 400 by
                    %% `emqx_mgmt_auth:update/6' (changing_namespace_is_forbidden).
                    update_api_key(Name, Role, Body);
                {error, Msg} ->
                    {400, #{code => 'BAD_REQUEST', message => Msg}}
            end;
        {error, not_found} ->
            %% A key in another namespace is reported as not found so its
            %% existence is not leaked to a namespaced administrator.
            {404, ?NOT_FOUND_RESPONSE}
    end.

update_api_key(Name, Role, Body) ->
    Enable = maps:get(<<"enable">>, Body, undefined),
    ExpiredAt = ensure_expired_at(Body),
    Desc = maps:get(<<"desc">>, Body, undefined),
    Scopes = maps:get(<<"scopes">>, Body, undefined),
    %% Validation runs against the effective scope list (request body
    %% when supplied, otherwise the persisted scopes) so a role change
    %% to `publisher' on a key that already holds non-`publish' scopes
    %% via a partial-update PUT is rejected — the runtime RBAC layer
    %% would catch it at request time, but the stored config and the
    %% API response must not be allowed to drift from the
    %% publisher-only-`publish' invariant.
    EffectiveScopes = effective_request_scopes(Name, Scopes),
    case validate_scopes(Role, Scopes, EffectiveScopes) of
        ok ->
            case emqx_mgmt_auth:update(Name, Enable, ExpiredAt, Desc, Role, Scopes) of
                {ok, App} ->
                    {200, emqx_mgmt_auth:format(App)};
                {error, not_found} ->
                    {404, ?NOT_FOUND_RESPONSE};
                {error, Reason} when is_binary(Reason) ->
                    {400, #{code => 'BAD_REQUEST', message => Reason}};
                {error, Reason} ->
                    {400, #{
                        code => 'BAD_REQUEST',
                        message => iolist_to_binary(io_lib:format("~p", [Reason]))
                    }}
            end;
        {error, Msg} ->
            {400, #{code => 'BAD_REQUEST', message => Msg}}
    end.

%% Fall back to persisted scopes only when the body did not supply a
%% `scopes' field. An explicit list (including `[]') is taken verbatim.
%% A missing API key surfaces here as `undefined' so validation accepts
%% the partial update; the downstream `emqx_mgmt_auth:update/6' call
%% will then return 404 with the proper error.
effective_request_scopes(_Name, Scopes) when is_list(Scopes) ->
    Scopes;
effective_request_scopes(Name, undefined) ->
    case emqx_mgmt_auth:read(Name) of
        {ok, #{scopes := Persisted}} when is_list(Persisted) -> Persisted;
        _ -> undefined
    end.

ensure_expired_at(#{<<"expired_at">> := ExpiredAt}) when is_integer(ExpiredAt) -> ExpiredAt;
ensure_expired_at(_) -> infinity.

%% Resolve the effective role string from the request `role' field and the
%% optional top-level `namespace' field.
%%
%% A namespace can be specified in two ways: encoded into the role string
%% (`ns:<namespace>::<role>') or via the standalone `namespace' field. When
%% both are present they must agree.
%%
%%   * neither present                       -> role unchanged (global key)
%%   * only role-encoded `ns:X::r'           -> role unchanged
%%   * only `namespace = X' + bare role `r'  -> assemble `ns:X::r'
%%   * both, encoded X == field X            -> role unchanged (idempotent)
%%   * both, encoded X /= field Y            -> {error, mismatch}
resolve_effective_role(Role, undefined) ->
    {ok, Role};
resolve_effective_role(_Role, <<>>) ->
    {error, <<"namespace must not be empty">>};
resolve_effective_role(Role, Namespace) when is_binary(Namespace) ->
    case emqx_dashboard_rbac:parse_api_role(Role) of
        {ok, #{?role := BareRole, ?namespace := ?global_ns}} ->
            {ok, emqx_dashboard_rbac:serialize_role(#{?role => BareRole, ?namespace => Namespace})};
        {ok, #{?namespace := Namespace}} ->
            %% Role-encoded namespace equals the namespace field; idempotent.
            {ok, Role};
        {ok, #{?namespace := Other}} ->
            {error, namespace_mismatch_msg(Other, Namespace)};
        {error, _} = Error ->
            Error
    end.

namespace_mismatch_msg(RoleNs, FieldNs) ->
    iolist_to_binary([
        <<"namespace mismatch: role is scoped to namespace '">>,
        RoleNs,
        <<"' but the namespace field is '">>,
        FieldNs,
        <<"'">>
    ]).

%% The namespace an effective role string resolves to (`?global_ns' or a binary).
role_namespace(Role) ->
    case emqx_dashboard_rbac:parse_api_role(Role) of
        {ok, #{?namespace := Ns}} -> Ns;
        _ -> ?global_ns
    end.

%% A global caller may create keys in any namespace (including global); a
%% namespaced caller may only create keys in its own namespace.
authorize_target_namespace(?global_ns, _TargetNs) ->
    ok;
authorize_target_namespace(CallerNs, CallerNs) ->
    ok;
authorize_target_namespace(_CallerNs, _TargetNs) ->
    {error, forbidden, <<"You may only manage API keys within your own namespace">>}.

%% A global caller sees every key; a namespaced caller sees only keys in its
%% own namespace. `KeyApp' is a formatted-or-raw map carrying a `namespace' key.
namespace_accessible(?global_ns, _KeyApp) ->
    true;
namespace_accessible(CallerNs, #{namespace := CallerNs}) ->
    true;
namespace_accessible(_CallerNs, _KeyApp) ->
    false.

%% Read a key only if it is visible to the caller's namespace. A key in a
%% different namespace is reported as `not_found' so its existence is not
%% leaked to a namespaced administrator.
read_in_namespace(CallerNs, Name) ->
    case emqx_mgmt_auth:read(Name) of
        {ok, App} ->
            case namespace_accessible(CallerNs, App) of
                true -> {ok, App};
                false -> {error, not_found}
            end;
        {error, not_found} ->
            {error, not_found}
    end.

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

app_extend_fields() ->
    [
        {role,
            hoconsc:mk(binary(), #{
                desc => ?DESC(role),
                default => ?ROLE_API_DEFAULT,
                example => ?ROLE_API_DEFAULT,
                validator => fun(Role) ->
                    maybe
                        {ok, _} ?= emqx_dashboard_rbac:parse_api_role(Role),
                        ok
                    end
                end
            })},
        {namespace,
            hoconsc:mk(binary(), #{
                desc => ?DESC(namespace_field),
                required => false,
                example => <<"ns1">>
            })}
    ].
