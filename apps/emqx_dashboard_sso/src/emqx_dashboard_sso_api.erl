%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_sso_api).

-behaviour(minirest_api).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_api_key_scopes.hrl").
-include_lib("emqx_dashboard/include/emqx_dashboard.hrl").

-import(hoconsc, [
    mk/2,
    array/1,
    enum/1,
    ref/1
]).

-import(emqx_dashboard_sso, [provider/1]).

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
    backend/2,
    token_exchange/2
]).

-export([sso_parameters/1, login_meta/4]).
-export([scopes/0]).

-define(REDIRECT, 'REDIRECT').
-define(BAD_USERNAME_OR_PWD, 'BAD_USERNAME_OR_PWD').
-define(BAD_REQUEST, 'BAD_REQUEST').
-define(BACKEND_NOT_FOUND, 'BACKEND_NOT_FOUND').
-define(TAGS, <<"Dashboard Single Sign-On">>).
-define(MOD_KEY_PATH, [dashboard, sso]).

namespace() -> "dashboard_sso".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => true, translate_body => true}).

scopes() -> ?SCOPE_DENIED.

paths() ->
    [
        "/sso",
        "/sso/:backend",
        "/sso/running",
        "/sso/login/:backend",
        "/sso/token_exchange"
    ].

schema("/sso/running") ->
    #{
        'operationId' => running,
        get => #{
            tags => [?TAGS],
            desc => ?DESC(list_running),
            responses => #{
                200 => array(enum(emqx_dashboard_sso:types()))
            },
            security => []
        }
    };
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
%% Visit "/sso/login/saml" to start the saml authentication process -- first check to see if
%% we are already logged in, otherwise we will make an AuthnRequest and send it to
%% our IDP
schema("/sso/login/:backend") ->
    #{
        'operationId' => login,
        post => #{
            tags => [?TAGS],
            desc => ?DESC(login),
            parameters => backend_name_in_path(),
            'requestBody' => login_union(),
            responses => #{
                200 => hoconsc:union([
                    ref(login_success_response),
                    ref(mfa_setup_response),
                    ref(mfa_verify_response)
                ]),
                %% Redirect to IDP for saml
                302 => response_schema(302),
                401 => response_schema(401),
                404 => response_schema(404)
            },
            security => []
        }
    };
schema("/sso/token_exchange") ->
    #{
        'operationId' => token_exchange,
        post => #{
            tags => [?TAGS],
            desc => ?DESC(token_exchange),
            'requestBody' => ref(token_exchange_request),
            responses => #{
                200 => hoconsc:union([
                    ref(login_success_response),
                    ref(mfa_setup_response),
                    ref(mfa_verify_response)
                ]),
                400 => response_schema(400)
            },
            security => []
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
    emqx_dashboard_sso_schema:common_backend_schema(emqx_dashboard_sso:types()) ++
        [
            {running,
                mk(
                    boolean(), #{
                        desc => ?DESC(running)
                    }
                )},
            {last_error,
                mk(
                    binary(), #{
                        desc => ?DESC(last_error)
                    }
                )}
        ];
fields(login_success_response) ->
    emqx_dashboard_api:fields([role, token, version, license]) ++
        [
            {username, mk(binary(), #{desc => ?DESC(sso_username)})},
            {backend, mk(binary(), #{desc => ?DESC(sso_backend)})}
        ];
fields(mfa_setup_response) ->
    [
        {action, mk(binary(), #{desc => ?DESC(mfa_action_setup), example => <<"mfa_setup">>})},
        {setup_token, mk(binary(), #{desc => ?DESC(setup_token_response)})},
        {mechanism, mk(binary(), #{desc => ?DESC(mfa_mechanism), example => <<"totp">>})},
        {username, mk(binary(), #{desc => ?DESC(sso_username)})},
        {backend, mk(binary(), #{desc => ?DESC(sso_backend)})}
    ];
fields(mfa_verify_response) ->
    [
        {action, mk(binary(), #{desc => ?DESC(mfa_action_verify), example => <<"mfa_verify">>})},
        {verify_token, mk(binary(), #{desc => ?DESC(verify_token_response)})},
        {username, mk(binary(), #{desc => ?DESC(sso_username)})},
        {backend, mk(binary(), #{desc => ?DESC(sso_backend)})}
    ];
fields(token_exchange_request) ->
    [
        {code, mk(binary(), #{desc => ?DESC(sso_code), required => true})},
        {username, mk(binary(), #{desc => ?DESC(sso_username), required => true})},
        {backend, mk(binary(), #{desc => ?DESC(sso_backend), required => true})}
    ].

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

running(get, _Request) ->
    {200, emqx_dashboard_sso_manager:running()}.

token_exchange(post, #{
    body := #{<<"code">> := Code, <<"username">> := Username, <<"backend">> := BackendBin}
}) ->
    case parse_backend(BackendBin) of
        {ok, BackendAtom} ->
            case emqx_dashboard_sso_code:exchange_code(BackendAtom, Username, Code) of
                {ok, #{action := <<"login">>}} ->
                    %% Sign JWT now at exchange time
                    SsoUsername = ?SSO_USERNAME(BackendAtom, Username),
                    case emqx_dashboard_admin:lookup_user(SsoUsername) of
                        [User] ->
                            {ok, Role, Token, _Namespace} = emqx_dashboard_token:sign(User),
                            {200, login_meta(Username, Role, Token, BackendAtom)};
                        [] ->
                            {400, #{code => ?BAD_REQUEST, message => <<"User not found">>}}
                    end;
                {ok, Payload} ->
                    %% MFA setup/verify — return as-is
                    {200, Payload};
                {error, _Reason} ->
                    {400, #{code => ?BAD_REQUEST, message => <<"Invalid or expired SSO code">>}}
            end;
        {error, _} ->
            {400, #{code => ?BAD_REQUEST, message => <<"Unknown SSO backend">>}}
    end.

login(post, #{bindings := #{backend := Backend}, body := Body} = Request) ->
    minirest_handler:update_log_meta(#{log_from => dashboard, log_source => Backend}),
    case emqx_dashboard_sso_manager:lookup_state(Backend) of
        undefined ->
            {404, #{code => ?BACKEND_NOT_FOUND, message => <<"Backend not found">>}};
        State ->
            case emqx_dashboard_sso:login(provider(Backend), Request, State) of
                {ok, login} ->
                    ?SLOG(info, #{
                        msg => "dashboard_sso_login_successful",
                        request => emqx_utils:redact(Request)
                    }),
                    Username = maps:get(<<"username">>, Body),
                    minirest_handler:update_log_meta(#{log_source => Username}),
                    SsoUsername = ?SSO_USERNAME(Backend, Username),
                    case emqx_dashboard_admin:lookup_user(SsoUsername) of
                        [User] ->
                            {ok, Role, Token, _Namespace} = emqx_dashboard_token:sign(User),
                            {200, login_meta(Username, Role, Token, Backend)};
                        [] ->
                            {400, #{code => ?BAD_REQUEST, message => <<"User not found">>}}
                    end;
                {ok, Role, Token, _Namespace} ->
                    %% Legacy path — kept for backends that still sign directly
                    ?SLOG(info, #{
                        msg => "dashboard_sso_login_successful",
                        request => emqx_utils:redact(Request)
                    }),
                    Username = maps:get(<<"username">>, Body),
                    minirest_handler:update_log_meta(#{log_source => Username}),
                    {200, login_meta(Username, Role, Token, Backend)};
                {mfa_setup, SetupToken, _QRInfo} ->
                    ?SLOG(info, #{
                        msg => "dashboard_sso_login_mfa_setup_required",
                        request => emqx_utils:redact(Request)
                    }),
                    Username = maps:get(<<"username">>, Body),
                    {200, #{
                        action => <<"mfa_setup">>,
                        setup_token => SetupToken,
                        mechanism => totp,
                        username => Username,
                        backend => Backend
                    }};
                {mfa_verify, VerifyToken} ->
                    ?SLOG(info, #{
                        msg => "dashboard_sso_login_mfa_verify_required",
                        request => emqx_utils:redact(Request)
                    }),
                    Username = maps:get(<<"username">>, Body),
                    {200, #{
                        action => <<"mfa_verify">>,
                        verify_token => VerifyToken,
                        username => Username,
                        backend => Backend
                    }};
                {redirect, Redirect} ->
                    ?SLOG(info, #{
                        msg => "dashboard_sso_login_redirect",
                        request => emqx_utils:redact(Request)
                    }),
                    Redirect;
                {error, Reason0} ->
                    Reason = emqx_utils:redact(Reason0),
                    ?SLOG(info, #{
                        msg => "dashboard_sso_login_failed",
                        request => emqx_utils:redact(Request),
                        reason => Reason
                    }),
                    {401, #{
                        code => ?BAD_USERNAME_OR_PWD,
                        message => <<"Auth failed">>,
                        reason => Reason
                    }}
            end
    end.

sso(get, _Request) ->
    SSO = emqx:get_config(?MOD_KEY_PATH, #{}),
    {200,
        lists:map(
            fun(#{backend := Backend, enable := Enable}) ->
                Status = emqx_dashboard_sso_manager:get_backend_status(Backend, Enable),
                Status#{
                    backend => Backend,
                    enable => Enable
                }
            end,
            maps:values(SSO)
        )}.

backend(get, #{bindings := #{backend := Type}}) ->
    case emqx:get_config(?MOD_KEY_PATH ++ [Type], undefined) of
        undefined ->
            {404, #{code => ?BACKEND_NOT_FOUND, message => <<"Backend not found">>}};
        Backend ->
            {200, to_redacted_json(Backend)}
    end;
backend(put, #{bindings := #{backend := Backend}, body := Config}) ->
    ?SLOG(info, #{
        msg => "update_sso_backend",
        backend => Backend,
        config => emqx_utils:redact(Config)
    }),
    on_backend_update(Backend, Config, fun emqx_dashboard_sso_manager:update/2);
backend(delete, #{bindings := #{backend := Backend}}) ->
    ?SLOG(info, #{msg => "delete_sso_backend", backend => Backend}),
    handle_backend_update_result(emqx_dashboard_sso_manager:delete(Backend), undefined).

sso_parameters(Params) ->
    backend_name_as_arg(query, [local], <<"local">>) ++ Params.

%%--------------------------------------------------------------------
%% internal
%%--------------------------------------------------------------------

response_schema(302) ->
    emqx_dashboard_swagger:error_codes([?REDIRECT], ?DESC(redirect));
response_schema(400) ->
    emqx_dashboard_swagger:error_codes([?BAD_REQUEST], ?DESC(bad_request));
response_schema(401) ->
    emqx_dashboard_swagger:error_codes([?BAD_USERNAME_OR_PWD], ?DESC(login_failed401));
response_schema(404) ->
    emqx_dashboard_swagger:error_codes([?BACKEND_NOT_FOUND], ?DESC(backend_not_found)).

backend_union() ->
    hoconsc:union([emqx_dashboard_sso:hocon_ref(Mod) || Mod <- emqx_dashboard_sso:modules()]).

login_union() ->
    hoconsc:union([emqx_dashboard_sso:login_ref(Mod) || Mod <- emqx_dashboard_sso:modules()]).

backend_name_in_path() ->
    backend_name_as_arg(path, [], <<"ldap">>).

backend_name_as_arg(In, Extra, Default) ->
    [
        {backend,
            mk(
                enum(Extra ++ emqx_dashboard_sso:types()),
                #{
                    in => In,
                    desc => ?DESC(backend_name_in_qs),
                    required => false,
                    example => Default
                }
            )}
    ].

on_backend_update(Backend, Config, Fun) ->
    Result = valid_config(Backend, Config, Fun),
    handle_backend_update_result(Result, Config).

valid_config(Backend, #{<<"backend">> := Backend} = Config, Fun) ->
    Fun(Backend, Config);
valid_config(_, _, _) ->
    {error, invalid_config}.

handle_backend_update_result({ok, #{backend := saml} = State}, _Config) ->
    {200, to_redacted_json(maps:without([idp_meta, sp], State))};
handle_backend_update_result({ok, _State}, Config) ->
    {200, to_redacted_json(Config)};
handle_backend_update_result(ok, _) ->
    204;
handle_backend_update_result({error, not_exists}, _) ->
    {404, #{code => ?BACKEND_NOT_FOUND, message => <<"Backend not found">>}};
handle_backend_update_result({error, failed_to_load_metadata}, _) ->
    {400, #{code => ?BAD_REQUEST, message => <<"Failed to load metadata">>}};
handle_backend_update_result({error, Reason}, _) when is_binary(Reason) ->
    {400, #{code => ?BAD_REQUEST, message => Reason}};
handle_backend_update_result({error, Reason}, _) ->
    {400, #{code => ?BAD_REQUEST, message => emqx_dashboard_sso:format(["Reason: ", Reason])}}.

to_redacted_json(Data) ->
    emqx_utils_maps:jsonable_map(
        emqx_utils:redact(Data),
        fun(K, V) ->
            {K, emqx_utils_maps:binary_string(V)}
        end
    ).

login_meta(Username, Role, Token, Backend) ->
    #{
        username => Username,
        role => Role,
        token => Token,
        version => iolist_to_binary(proplists:get_value(version, emqx_sys:info())),
        license => #{edition => emqx_release:edition()},
        backend => Backend
    }.

parse_backend(BackendBin) ->
    emqx_dashboard_sso:parse_backend(BackendBin).
