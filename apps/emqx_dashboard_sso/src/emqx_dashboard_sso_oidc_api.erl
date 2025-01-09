%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_sso_oidc_api).

-behaviour(minirest_api).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_dashboard/include/emqx_dashboard.hrl").

-import(hoconsc, [
    mk/2,
    array/1,
    enum/1,
    ref/1
]).

-import(emqx_dashboard_sso_api, [login_meta/3]).

-export([
    api_spec/0,
    paths/0,
    schema/1,
    namespace/0
]).

-export([code_callback/2, make_callback_url/1]).

-define(BAD_REQUEST, 'BAD_REQUEST').
-define(BAD_USERNAME_OR_PWD, 'BAD_USERNAME_OR_PWD').
-define(BACKEND_NOT_FOUND, 'BACKEND_NOT_FOUND').

-define(REDIRECT_HEADERS(TARGET), #{
    <<"cache-control">> => <<"no-cache">>,
    <<"pragma">> => <<"no-cache">>,
    <<"content-type">> => <<"text/plain">>,
    <<"location">> => TARGET
}).

-define(REDIRECT_BODY, <<"Redirecting...">>).

-define(TAGS, <<"Dashboard Single Sign-On">>).
-define(BACKEND, oidc).
-define(BASE_PATH, "/api/v5").
-define(CALLBACK_PATH, "/sso/oidc/callback").

namespace() -> "dashboard_sso".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => false, translate_body => false}).

paths() ->
    [
        ?CALLBACK_PATH
    ].

%% Handles Authorization Code callback from the OP.
schema("/sso/oidc/callback") ->
    #{
        'operationId' => code_callback,
        get => #{
            tags => [?TAGS],
            desc => ?DESC(code_callback),
            responses => #{
                200 => emqx_dashboard_api:fields([token, version, license]),
                400 => response_schema(400),
                401 => response_schema(401),
                404 => response_schema(404)
            },
            security => [],
            log_meta => emqx_dashboard_audit:importance(high)
        }
    }.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
code_callback(get, #{query_string := QS}) ->
    minirest_handler:update_log_meta(#{log_from => oidc}),
    case ensure_sso_state(QS) of
        {ok, Target} ->
            ?SLOG(info, #{
                msg => "dashboard_sso_login_successful"
            }),
            {302, ?REDIRECT_HEADERS(Target), ?REDIRECT_BODY};
        {error, invalid_query_string_param} ->
            {400, #{code => ?BAD_REQUEST, message => <<"Invalid query string">>}};
        {error, invalid_backend} ->
            {404, #{code => ?BACKEND_NOT_FOUND, message => <<"Backend not found">>}};
        {error, Reason} ->
            ?SLOG(info, #{
                msg => "dashboard_sso_login_failed",
                reason => emqx_utils:redact(Reason)
            }),
            {401, #{code => ?BAD_USERNAME_OR_PWD, message => reason_to_message(Reason)}}
    end.

%%--------------------------------------------------------------------
%% internal
%%--------------------------------------------------------------------
response_schema(400) ->
    emqx_dashboard_swagger:error_codes([?BAD_REQUEST], <<"Bad Request">>);
response_schema(401) ->
    emqx_dashboard_swagger:error_codes(
        [?BAD_USERNAME_OR_PWD], ?DESC(emqx_dashboard_api, login_failed401)
    );
response_schema(404) ->
    emqx_dashboard_swagger:error_codes([?BACKEND_NOT_FOUND], <<"Backend not found">>).

reason_to_message(Bin) when is_binary(Bin) ->
    Bin;
reason_to_message(Term) ->
    erlang:iolist_to_binary(io_lib:format("~p", [Term])).

ensure_sso_state(QS) ->
    case emqx_dashboard_sso_manager:lookup_state(?BACKEND) of
        undefined ->
            {error, invalid_backend};
        Cfg ->
            ensure_oidc_state(QS, Cfg)
    end.

ensure_oidc_state(#{<<"state">> := State} = QS, Cfg) ->
    case emqx_dashboard_sso_oidc_session:lookup(State) of
        {ok, Data} ->
            emqx_dashboard_sso_oidc_session:delete(State),
            retrieve_token(QS, Cfg, Data);
        _ ->
            {error, session_not_exists}
    end;
ensure_oidc_state(_, _Cfg) ->
    {error, invalid_query_string_param}.

retrieve_token(
    #{<<"code">> := Code},
    #{
        name := Name,
        client_jwks := ClientJwks,
        config := #{
            clientid := ClientId,
            secret := Secret,
            preferred_auth_methods := AuthMethods
        }
    } = Cfg,
    Data
) ->
    case
        oidcc:retrieve_token(
            Code,
            Name,
            ClientId,
            emqx_secret:unwrap(Secret),
            Data#{
                redirect_uri => make_callback_url(Cfg),
                client_jwks => ClientJwks,
                preferred_auth_methods => AuthMethods
            }
        )
    of
        {ok, Token} ->
            retrieve_userinfo(Token, Cfg);
        {error, _Reason} = Error ->
            Error
    end.

retrieve_userinfo(
    Token,
    #{
        name := Name,
        client_jwks := ClientJwks,
        config := #{clientid := ClientId, secret := Secret},
        name_tokens := NameTks
    } = Cfg
) ->
    case
        oidcc:retrieve_userinfo(
            Token,
            Name,
            ClientId,
            emqx_secret:unwrap(Secret),
            #{client_jwks => ClientJwks}
        )
    of
        {ok, UserInfo} ->
            ?SLOG(debug, #{
                msg => "sso_oidc_login_user_info",
                user_info => UserInfo
            }),
            Username = emqx_placeholder:proc_tmpl(NameTks, UserInfo),
            minirest_handler:update_log_meta(#{log_source => Username}),
            ensure_user_exists(Cfg, Username);
        {error, _Reason} = Error ->
            Error
    end.

-dialyzer({nowarn_function, ensure_user_exists/2}).
ensure_user_exists(_Cfg, <<>>) ->
    {error, <<"Username can not be empty">>};
ensure_user_exists(_Cfg, <<"undefined">>) ->
    {error, <<"Username can not be undefined">>};
ensure_user_exists(Cfg, Username) ->
    case emqx_dashboard_admin:lookup_user(?BACKEND, Username) of
        [User] ->
            case emqx_dashboard_token:sign(User, <<>>) of
                {ok, Role, Token} ->
                    {ok, login_redirect_target(Cfg, Username, Role, Token)};
                Error ->
                    Error
            end;
        [] ->
            case emqx_dashboard_admin:add_sso_user(?BACKEND, Username, ?ROLE_VIEWER, <<>>) of
                {ok, _} ->
                    ensure_user_exists(Cfg, Username);
                Error ->
                    Error
            end
    end.

make_callback_url(#{config := #{dashboard_addr := Addr}}) ->
    list_to_binary(binary_to_list(Addr) ++ ?BASE_PATH ++ ?CALLBACK_PATH).

login_redirect_target(#{config := #{dashboard_addr := Addr}}, Username, Role, Token) ->
    LoginMeta = emqx_dashboard_sso_api:login_meta(Username, Role, Token, oidc),
    MetaBin = base64:encode(emqx_utils_json:encode(LoginMeta)),
    <<Addr/binary, "/?login_meta=", MetaBin/binary>>.
