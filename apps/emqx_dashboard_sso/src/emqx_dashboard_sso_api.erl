%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_sso_api).

-behaviour(minirest_api).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("typerefl/include/types.hrl").

-import(hoconsc, [
    mk/2,
    array/1,
    enum/1,
    ref/1,
    union/1
]).

-export([
    api_spec/0,
    fields/1,
    paths/0,
    schema/1,
    namespace/0
]).

-export([
    running/2,
    login/2,
    sso/2,
    backend/2
]).

-define(BAD_USERNAME_OR_PWD, 'BAD_USERNAME_OR_PWD').
-define(BAD_REQUEST, 'BAD_REQUEST').

-define(BACKEND_NOT_FOUND, 'BACKEND_NOT_FOUND').
-define(TAGS, <<"Dashboard Single Sign-on">>).

namespace() -> "dashboard_sso".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true, translate_body => true}).

paths() ->
    [
        "/sso",
        "/sso/login",
        "/sso/running",
        "/sso/:backend"
    ].

schema("/sso") ->
    #{
        'operationId' => sso,
        get => #{
            tags => [?TAGS],
            desc => ?DESC(get_sso),
            responses => #{
                200 => array(ref(backend_status))
            }
        }
    };
schema("/sso/login") ->
    #{
        'operationId' => login,
        post => #{
            tags => [?TAGS],
            desc => ?DESC(login),
            'requestBody' => login_union(),
            responses => #{
                200 => emqx_dashboard_api:fields([token, version, license]),
                401 => response_schema(401),
                404 => response_schema(404)
            }
        }
    };
schema("/sso/running") ->
    #{
        'operationId' => running,
        get => #{
            tags => [?TAGS],
            desc => ?DESC(get_running),
            responses => #{
                200 => array(enum(emqx_dashboard_sso:types()))
            }
        }
    };
schema("/sso/:backend") ->
    #{
        'operationId' => backend,
        get => #{
            tags => [?TAGS],
            desc => ?DESC(get_backend),
            parameters => backend_name_in_path(),
            responses => #{
                200 => backend_union(),
                404 => response_schema(404)
            }
        },
        post => #{
            tags => [?TAGS],
            desc => ?DESC(create_backend),
            parameters => backend_name_in_path(),
            'requestBody' => backend_union(),
            responses => #{
                200 => backend_union()
            }
        },
        put => #{
            tags => [?TAGS],
            desc => ?DESC(update_backend),
            parameters => backend_name_in_path(),
            'requestBody' => backend_union(),
            responses => #{
                200 => backend_union(),
                404 => response_schema(404)
            }
        },
        delete => #{
            tags => [?TAGS],
            desc => ?DESC(delete_backend),
            parameters => backend_name_in_path(),
            responses => #{
                204 => <<"Delete successfully">>,
                404 => response_schema(404)
            }
        }
    }.

fields(backend_status) ->
    emqx_dashboard_sso_schema:common_backend_schema(enum(emqx_dashboard_sso:types())).

%% -------------------------------------------------------------------------------------------------
%% API
running(get, _Request) ->
    {200, emqx_dashboard_sso_manager:running()}.

login(post, #{backend := Backend} = Request) ->
    case emqx_dashboard_sso_manager:lookup_state(Backend) of
        undefined ->
            {404, ?BACKEND_NOT_FOUND};
        State ->
            Provider = emqx_dashboard_sso:provider(Backend),
            case Provider:login(Request, State) of
                {ok, Token} ->
                    ?SLOG(info, #{msg => "Dashboard SSO login successfully", request => Request}),
                    Version = iolist_to_binary(proplists:get_value(version, emqx_sys:info())),
                    {200, #{
                        token => Token,
                        version => Version,
                        license => #{edition => emqx_release:edition()}
                    }};
                {error, Reason} ->
                    ?SLOG(info, #{
                        msg => "Dashboard SSO login failed", request => Request, reason => Reason
                    }),
                    {401, ?BAD_USERNAME_OR_PWD, <<"Auth failed">>}
            end
    end.

sso(get, _Request) ->
    SSO = emqx:get_config([dashboard_sso], #{}),
    {200,
        lists:map(
            fun(Backend) ->
                maps:with([backend, enabled], Backend)
            end,
            maps:values(SSO)
        )}.

backend(get, #{bindings := #{backend := Type}}) ->
    case emqx:get_config([dashboard_sso, Type], undefined) of
        undefined ->
            {404, ?BACKEND_NOT_FOUND};
        Backend ->
            {200, Backend}
    end;
backend(create, #{bindings := #{backend := Backend}, body := Config}) ->
    on_backend_update(Backend, Config, fun emqx_dashboard_sso_manager:create/2);
backend(put, #{bindings := #{backend := Backend}, body := Config}) ->
    on_backend_update(Backend, Config, fun emqx_dashboard_sso_manager:update/2);
backend(delete, #{bindings := #{backend := Backend}, body := Config}) ->
    on_backend_update(Backend, Config, fun emqx_dashboard_sso_manager:delete/2).

%% -------------------------------------------------------------------------------------------------
%% internal
response_schema(401) ->
    emqx_dashboard_swagger:error_codes([?BAD_USERNAME_OR_PWD], ?DESC(login_failed401));
response_schema(404) ->
    emqx_dashboard_swagger:error_codes([?BACKEND_NOT_FOUND], ?DESC(backend_not_found)).

backend_union() ->
    hoconsc:union([Mod:hocon_ref() || Mod <- emqx_dashboard_sso:modules()]).

login_union() ->
    hoconsc:union([Mod:login_ref() || Mod <- emqx_dashboard_sso:modules()]).

backend_name_in_path() ->
    [
        {name,
            mk(
                binary(),
                #{
                    in => path,
                    desc => ?DESC(backend_name_in_qs),
                    example => <<"ldap">>
                }
            )}
    ].

on_backend_update(Backend, Config, Fun) ->
    Result = valid_config(Backend, Config, Fun),
    handle_backend_update_result(Result, Config).

valid_config(Backend, Config, Fun) ->
    case maps:get(backend, Config, undefined) of
        Backend ->
            Fun(Backend, Config);
        _ ->
            {error, invalid_config}
    end.

handle_backend_update_result({ok, _}, Config) ->
    {200, Config};
handle_backend_update_result(ok, _) ->
    204;
handle_backend_update_result({error, not_exists}, _) ->
    {404, ?BACKEND_NOT_FOUND};
handle_backend_update_result({error, already_exists}, _) ->
    {400, ?BAD_REQUEST, <<"Backend already exists.">>};
handle_backend_update_result({error, Reason}, _) ->
    {400, ?BAD_REQUEST, Reason}.
