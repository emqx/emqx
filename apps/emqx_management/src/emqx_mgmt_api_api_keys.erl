%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mgmt_api_api_keys).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_dashboard/include/emqx_dashboard_rbac.hrl").
-include_lib("emqx/include/emqx_api_key_scopes.hrl").

-export([api_spec/0, fields/1, paths/0, schema/1, namespace/0]).
-export([api_key/2, api_key_by_name/2, api_key_scopes/2]).
-export([validate_name/1]).

-export([scopes/0]).

-define(TAGS, [<<"API Keys">>]).

namespace() -> "api_key".

scopes() -> ?SCOPE_DENIED.

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true, translate_body => true}).

paths() ->
    ["/api_key", "/api_key/:name", "/api_key/scopes"].

schema("/api_key") ->
    #{
        'operationId' => api_key,
        get => #{
            description => ?DESC(api_key_list),
            tags => ?TAGS,
            security => [#{'bearerAuth' => []}],
            responses => #{
                200 => delete([api_secret], fields(app))
            }
        },
        post => #{
            description => ?DESC(create_new_api_key),
            tags => ?TAGS,
            security => [#{'bearerAuth' => []}],
            'requestBody' => delete([created_at, api_key, api_secret], fields(app)),
            responses => #{
                200 => hoconsc:ref(app),
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
                200 => delete([api_secret], fields(app)),
                404 => emqx_dashboard_swagger:error_codes(['NOT_FOUND'])
            }
        },
        put => #{
            description => ?DESC(update_api_key),
            tags => ?TAGS,
            parameters => [hoconsc:ref(name)],
            'requestBody' => delete([created_at, api_key, api_secret, name], fields(app)),
            responses => #{
                200 => delete([api_secret], fields(app)),
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
schema("/api_key/scopes") ->
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
                    desc => ?DESC(api_key_scopes),
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
    Scopes = maps:get(<<"scopes">>, App, undefined),
    case validate_scopes(Scopes) of
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
    Scopes = maps:get(<<"scopes">>, Body, undefined),
    case validate_scopes(Scopes) of
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

ensure_expired_at(#{<<"expired_at">> := ExpiredAt}) when is_integer(ExpiredAt) -> ExpiredAt;
ensure_expired_at(_) -> infinity.

api_key_scopes(get, _) ->
    {200, #{
        scopes => emqx_mgmt_api_key_scopes:scope_catalogue()
    }}.

validate_scopes(undefined) ->
    ok;
validate_scopes(Scopes) when is_list(Scopes) ->
    emqx_mgmt_api_key_scopes:validate_scopes(Scopes);
validate_scopes(_) ->
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
            })}
    ].
