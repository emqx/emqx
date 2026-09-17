%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mgmt_api_api_keys).

-behaviour(minirest_api).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_dashboard/include/emqx_dashboard_rbac.hrl").

-export([api_spec/0, fields/1, paths/0, schema/1, namespace/0]).
-export([api_key/2, api_key_by_name/2]).
-export([validate_name/1]).
-define(TAGS, [<<"API Keys">>]).

namespace() -> "api_key".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true, translate_body => true}).

paths() ->
    ["/api_key", "/api_key/:name"].

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
            'requestBody' => delete([created_at, api_key, api_secret], fields(app_create)),
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
                    %% No `default' on purpose, matching `role' below: a schema default is
                    %% filled into the request body before it reaches the handler, which
                    %% would make an omitted `expired_at' indistinguishable from an
                    %% explicit one and let a partial update silently rewrite the key's
                    %% expiry. A key created without it still never expires.
                    required => false
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
        {expired, hoconsc:mk(boolean(), #{desc => "Expired", required => false})}
    ] ++ app_extend_fields();
%% The `POST' request body documents the values the create path applies to a field the
%% caller left out, so `expired_at' (and `role' in the enterprise edition) keep their
%% `default' here.
%%
%% It must not be shared with the update path: a declared `default' is filled into the
%% request body *before* the handler sees it, so on an update an omitted field would
%% overwrite the stored value with the default. That is exactly what used to escalate a
%% `viewer' key to `administrator' and made an expiring key immortal. `PUT' therefore
%% uses `fields(app)', which declares no defaults.
fields(app_create) ->
    Defaults = create_defaults(),
    [
        {Field, maybe_add_default(Schema, maps:find(Field, Defaults))}
     || {Field, Schema} <- fields(app)
    ];
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
    ].

%% Values the create path applies to an omitted field. `role' only exists in the
%% enterprise edition, so it is contributed the same way `app_extend_fields/0' does it,
%% which also keeps this module free of an unreachable clause in the community edition.
create_defaults() ->
    maps:merge(#{expired_at => infinity}, app_extend_create_defaults()).

-if(?EMQX_RELEASE_EDITION == ee).
app_extend_create_defaults() -> #{role => ?ROLE_API_DEFAULT}.
-else.
app_extend_create_defaults() -> #{}.
-endif.

maybe_add_default(Schema, {ok, Default}) -> Schema#{default => Default};
maybe_add_default(Schema, error) -> Schema.

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
    ExpiredAt = new_expired_at(App),
    Desc = unicode:characters_to_binary(Desc0, unicode),
    Role = maps:get(<<"role">>, App, ?ROLE_API_DEFAULT),
    %% create api_key with random api_key and api_secret from Dashboard
    case emqx_mgmt_auth:create(Name, Enable, ExpiredAt, Desc, Role) of
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
    ExpiredAt = update_expired_at(Body),
    Desc = maps:get(<<"desc">>, Body, undefined),
    %% `undefined' means the request body carried no role; the update path then keeps
    %% the stored one. `role' deliberately has no schema default: the request body is
    %% filled with schema defaults before it reaches this handler, which would make
    %% "omitted" indistinguishable from an explicit value and silently escalate the key.
    Role = maps:get(<<"role">>, Body, undefined),
    case emqx_mgmt_auth:update(Name, Enable, ExpiredAt, Desc, Role) of
        {ok, App} ->
            {200, emqx_mgmt_auth:format(App)};
        {error, not_found} ->
            {404, ?NOT_FOUND_RESPONSE};
        {error, Reason} ->
            {400, #{
                code => 'BAD_REQUEST',
                message => iolist_to_binary(io_lib:format("~p", [Reason]))
            }}
    end.

%% The create path has no previous value to keep, so a key created without `expired_at'
%% simply never expires.
new_expired_at(#{<<"expired_at">> := ExpiredAt}) when is_integer(ExpiredAt) -> ExpiredAt;
new_expired_at(#{<<"expired_at">> := infinity}) -> infinity;
new_expired_at(_) -> infinity.

%% An update that does not mention `expired_at' must not extend the lifetime of the
%% key, so an omitted field stays `undefined' and keeps the stored value, just like
%% `desc', `enable' and `role'. `infinity' is an explicit request to clear the expiry
%% and must therefore be passed through rather than treated as absent.
update_expired_at(#{<<"expired_at">> := ExpiredAt}) when
    is_integer(ExpiredAt); ExpiredAt =:= infinity
->
    ExpiredAt;
update_expired_at(_) ->
    undefined.

-if(?EMQX_RELEASE_EDITION == ee).

app_extend_fields() ->
    [
        {role,
            hoconsc:mk(binary(), #{
                desc => ?DESC(role),
                required => false,
                example => ?ROLE_API_DEFAULT,
                validator => fun emqx_dashboard_rbac:valid_api_role/1
            })}
    ].

-else.

app_extend_fields() ->
    [].

-endif.
