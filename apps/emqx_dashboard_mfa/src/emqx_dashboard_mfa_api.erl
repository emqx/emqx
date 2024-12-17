%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_mfa_api).

-behaviour(minirest_api).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").

-import(hoconsc, [
    mk/2,
    array/1,
    enum/1,
    ref/1
]).

-export([
    api_spec/0,
    paths/0,
    schema/1,
    namespace/0
]).

-export([
    mfa/2,
    login/2,
    setup_mfa/2
]).

-export([login_union/1, login_resp_union/0, setup_union/0]).

-define(BAD_REQUEST, 'BAD_REQUEST').
-define(USER_NOT_FOUND, 'USER_NOT_FOUND').
-define(ERROR_PWD_NOT_MATCH, 'ERROR_PWD_NOT_MATCH').
-define(BAD_USERNAME_OR_PWD, 'BAD_USERNAME_OR_PWD').

-define(TAGS, <<"dashboard">>).
-define(MOD_KEY_PATH, [dashboard, mfa]).

namespace() -> "dashboard".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true}).

paths() ->
    [
        "/login",
        "/mfa",
        "/users/:username/setup_mfa"
    ].

schema("/login") ->
    #{
        'operationId' => login,
        post => #{
            tags => [<<"dashboard">>],
            desc => ?DESC(emqx_dashboard_api, login_api),
            summary => <<"Dashboard authentication">>,
            'requestBody' => login_union(
                hoconsc:ref(emqx_dashboard_api, login)
            ),
            responses => #{
                100 => login_resp_union(),
                200 => emqx_dashboard_api:fields([
                    role, token, version, license, password_expire_in_seconds
                ]),
                401 => response_schema(401)
            },
            security => []
        }
    };
schema("/mfa") ->
    #{
        'operationId' => mfa,
        get => #{
            tags => [?TAGS],
            desc => ?DESC(get_mfa),
            responses => #{
                200 => hoconsc:array(config_union())
            }
        },
        post => #{
            tags => [?TAGS],
            desc => ?DESC(post_mfa),
            'requestBody' => config_union(),
            responses => #{
                200 => config_union(),
                400 => response_schema(400)
            }
        }
    };
schema("/users/:username/setup_mfa") ->
    #{
        'operationId' => setup_mfa,
        post => #{
            tags => [<<"dashboard">>],
            desc => ?DESC(setup_mfa),
            parameters => emqx_dashboard_api:fields([username_in_path]),
            'requestBody' => setup_union(),
            responses => #{
                200 => setup_resp_union(),
                404 => response_schema(404),
                400 => response_schema(400)
            }
        }
    }.

config_union() ->
    hoconsc:union([
        emqx_dashboard_mfa:api_ref(Mod, api_config)
     || Mod <- emqx_dashboard_mfa:modules()
    ]).

%% MFA second phase login parameters
login_union(Ref) ->
    hoconsc:union([
        Ref
        | [emqx_dashboard_mfa:api_ref(Mod, second_login) || Mod <- emqx_dashboard_mfa:modules()]
    ]).

%% MFA first phase login response
login_resp_union() ->
    hoconsc:union([
        emqx_dashboard_mfa:api_ref(Mod, first_login_resp)
     || Mod <- emqx_dashboard_mfa:modules()
    ]).

%% MFA setup schema
setup_union() ->
    hoconsc:union([emqx_dashboard_mfa:api_ref(Mod, setup) || Mod <- emqx_dashboard_mfa:modules()]).

setup_resp_union() ->
    hoconsc:union([
        emqx_dashboard_mfa:api_ref(Mod, setup_resp)
     || Mod <- emqx_dashboard_mfa:modules()
    ]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

login(post, #{body := #{<<"username">> := Username, <<"password">> := Password}}) ->
    minirest_handler:update_log_meta(#{log_from => dashboard, log_source => Username}),
    case emqx_dashboard_mfa:sign_token(Username, Password) of
        {ok, Result} ->
            ?SLOG(info, #{msg => "dashboard_login_successful", username => Username}),
            Version = iolist_to_binary(proplists:get_value(version, emqx_sys:info())),
            {200, Result#{
                version => Version,
                license => #{edition => emqx_release:edition()}
            }};
        {error, R} ->
            ?SLOG(info, #{msg => "dashboard_login_failed", username => Username, reason => R}),
            {401, ?BAD_USERNAME_OR_PWD, <<"Auth failed">>}
    end;
login(post, #{body := Params}) ->
    case emqx_dashboard_mfa:verify(Params) of
        {ok, Result} ->
            ?SLOG(info, #{msg => "dashboard_mfa_login_successful"}),
            Version = iolist_to_binary(proplists:get_value(version, emqx_sys:info())),
            {200, Result#{
                version => Version,
                license => #{edition => emqx_release:edition()}
            }};
        {error, Reason} ->
            ?SLOG(info, #{msg => "dashboard_mfa_login_failed", reason => Reason}),
            {401, ?BAD_USERNAME_OR_PWD, <<"Auth failed">>}
    end.

mfa(get, _Request) ->
    Methods = emqx:get_config(?MOD_KEY_PATH, #{}),
    {200,
        lists:map(
            fun({Method, Config}) ->
                Config#{method => Method}
            end,
            maps:to_list(Methods)
        )};
mfa(post, #{body := #{<<"method">> := Method} = Params}) ->
    {ok, Method2} = emqx_utils:safe_to_existing_atom(Method),
    Config = emqx_utils_maps:safe_atom_key_map(maps:without([<<"method">>], Params)),
    case emqx_conf:update([dashboard, mfa, Method2], Config, #{override_to => cluster}) of
        {ok, _Result} ->
            {200, Params};
        {error, Reason} ->
            {400, #{code => ?BAD_REQUEST, message => Reason}}
    end.

setup_mfa(
    post,
    #{
        bindings := #{username := Username},
        body := #{<<"password">> := Password} = Params
    } = _Req
) ->
    LogMeta = #{msg => "dashboard_setup_mfa", username => binary_to_list(Username)},
    case emqx_dashboard_admin:check(Username, Password) of
        {ok, _User} ->
            case emqx_dashboard_mfa:setup_user_mfa(Username, Params) of
                {ok, Result} ->
                    ?SLOG(info, LogMeta#{result => success}),
                    {200, Result};
                {error, Reason} ->
                    ?SLOG(error, LogMeta#{result => failed, reason => Reason}),
                    {400, ?BAD_REQUEST, Reason}
            end;
        {error, <<"username_not_found">>} ->
            ?SLOG(error, LogMeta#{result => failed, reason => "username not found"}),
            {404, ?USER_NOT_FOUND, <<"User not found">>};
        {error, <<"password_error">>} ->
            ?SLOG(error, LogMeta#{result => failed, reason => "wrong password"}),
            {400, ?ERROR_PWD_NOT_MATCH, <<"Wrong password">>};
        {error, Reason} ->
            ?SLOG(error, LogMeta#{result => failed, reason => Reason}),
            {400, ?BAD_REQUEST, Reason}
    end.

%%--------------------------------------------------------------------
%% internal
%%--------------------------------------------------------------------
response_schema(400) ->
    emqx_dashboard_swagger:error_codes([?BAD_REQUEST], ?DESC(bad_request_response400));
response_schema(Other) ->
    emqx_dashboard_api:response_schema(Other).
