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

-define(PROVIDER_SVR_NAME, sso_oidc_provider).
-define(RESPHEADERS, #{
    <<"cache-control">> => <<"no-cache">>,
    <<"pragma">> => <<"no-cache">>,
    <<"content-type">> => <<"text/plain">>
}).
-define(REDIRECT_BODY, <<"Redirecting...">>).

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

create(#{issuer := Issuer, name_var := NameVar} = Config) ->
    case
        oidcc_provider_configuration_worker:start_link(#{
            issuer => Issuer,
            name => {local, ?PROVIDER_SVR_NAME}
        })
    of
        {ok, Pid} ->
            {ok, #{
                pid => Pid,
                config => Config,
                name_tokens => emqx_placeholder:preproc_tmpl(NameVar)
            }};
        {error, _} = Error ->
            Error
    end.

update(Config, State) ->
    destroy(State),
    create(Config).

destroy(#{pid := Pid}) ->
    _ = catch gen_server:stop(Pid),
    ok.

login(
    _Req,
    #{
        config := #{
            clientid := ClientId,
            secret := Secret,
            scopes := Scopes
        }
    } = State
) ->
    case
        oidcc:create_redirect_url(
            ?PROVIDER_SVR_NAME,
            ClientId,
            Secret,
            #{
                scopes => Scopes,
                state => random_bin(),
                nonce => random_bin(),
                redirect_uri => emqx_dashboard_sso_oidc_api:make_callback_url(State)
            }
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

random_bin() ->
    emqx_utils_conv:bin(emqx_utils:gen_id(16)).
