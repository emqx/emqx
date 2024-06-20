%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_sso_oidc).

-include_lib("emqx_dashboard/include/emqx_dashboard.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-behaviour(emqx_dashboard_sso).

-export([
    namespace/0,
    fields/1,
    desc/1
]).

-export([
    hocon_ref/0,
    login_ref/0,
    login/2,
    create/1,
    update/2,
    destroy/1,
    convert_certs/2
]).

-define(PROVIDER_SVR_NAME, ?MODULE).
-define(RESPHEADERS, #{
    <<"cache-control">> => <<"no-cache">>,
    <<"pragma">> => <<"no-cache">>,
    <<"content-type">> => <<"text/plain">>
}).
-define(REDIRECT_BODY, <<"Redirecting...">>).
-define(PKCE_VERIFIER_LEN, 60).

%%------------------------------------------------------------------------------
%% Hocon Schema
%%------------------------------------------------------------------------------

namespace() ->
    "sso".

hocon_ref() ->
    hoconsc:ref(?MODULE, oidc).

login_ref() ->
    hoconsc:ref(?MODULE, login).

fields(oidc) ->
    emqx_dashboard_sso_schema:common_backend_schema([oidc]) ++
        [
            {issuer,
                ?HOCON(
                    binary(),
                    #{desc => ?DESC(issuer), required => true}
                )},
            {clientid,
                ?HOCON(
                    binary(),
                    #{desc => ?DESC(clientid), required => true}
                )},
            {secret,
                ?HOCON(
                    binary(),
                    #{desc => ?DESC(secret), required => true}
                )},
            {scopes,
                ?HOCON(
                    ?ARRAY(binary()),
                    #{desc => ?DESC(scopes), default => [<<"openid">>]}
                )},
            {name_var,
                ?HOCON(
                    binary(),
                    #{desc => ?DESC(name_var), default => <<"${sub}">>}
                )},
            {dashboard_addr,
                ?HOCON(binary(), #{
                    desc => ?DESC(dashboard_addr),
                    default => <<"http://127.0.0.1:18083">>
                })},
            {session_expiry,
                ?HOCON(emqx_schema:timeout_duration_ms(), #{
                    desc => ?DESC(session_expiry),
                    default => <<"30s">>
                })},
            {require_pkce,
                ?HOCON(boolean(), #{
                    desc => ?DESC(require_pkce),
                    default => false
                })}
        ];
fields(login) ->
    [
        emqx_dashboard_sso_schema:backend_schema([oidc])
    ].

desc(oidc) ->
    "OIDC";
desc(_) ->
    undefined.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

create(#{name_var := NameVar} = Config) ->
    case
        emqx_dashboard_sso_oidc_session:start(
            ?PROVIDER_SVR_NAME,
            Config
        )
    of
        {error, _} = Error ->
            Error;
        _ ->
            %% Note: the oidcc maintains an ETS with the same name of the provider gen_server,
            %% we should use this name in each API calls not the PID,
            %% or it would backoff to sync calls to the gen_server
            {ok, #{
                name => ?PROVIDER_SVR_NAME,
                config => Config,
                name_tokens => emqx_placeholder:preproc_tmpl(NameVar)
            }}
    end.

update(Config, State) ->
    destroy(State),
    create(Config).

destroy(_) ->
    emqx_dashboard_sso_oidc_session:stop().

login(
    _Req,
    #{
        config := #{
            clientid := ClientId,
            secret := Secret,
            scopes := Scopes,
            require_pkce := RequirePKCE
        }
    } = Cfg
) ->
    Nonce = emqx_dashboard_sso_oidc_session:random_bin(),
    Opts = maybe_require_pkce(RequirePKCE, #{
        scopes => Scopes,
        nonce => Nonce,
        redirect_uri => emqx_dashboard_sso_oidc_api:make_callback_url(Cfg)
    }),

    Data = maps:with([nonce, require_pkce, pkce_verifier], Opts),
    State = emqx_dashboard_sso_oidc_session:new(Data),

    case
        oidcc:create_redirect_url(
            ?PROVIDER_SVR_NAME,
            ClientId,
            Secret,
            Opts#{state => State}
        )
    of
        {ok, [Base, Delimiter, Params]} ->
            RedirectUri = <<Base/binary, Delimiter/binary, Params/binary>>,
            Redirect = {302, ?RESPHEADERS#{<<"location">> => RedirectUri}, ?REDIRECT_BODY},
            {redirect, Redirect};
        {error, _Reason} = Error ->
            Error
    end.

convert_certs(_Dir, Conf) ->
    Conf.

maybe_require_pkce(false, Opts) ->
    Opts;
maybe_require_pkce(true, Opts) ->
    Opts#{
        require_pkce => true,
        pkce_verifier => emqx_dashboard_sso_oidc_session:random_bin(?PKCE_VERIFIER_LEN)
    }.
