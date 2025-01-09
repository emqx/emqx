%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_sso_oidc).

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
-define(REDIRECT_HEADERS(LOCATION), #{
    <<"cache-control">> => <<"no-cache">>,
    <<"pragma">> => <<"no-cache">>,
    <<"content-type">> => <<"text/plain">>,
    <<"location">> => LOCATION
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
                emqx_schema_secret:mk(
                    maps:merge(#{desc => ?DESC(secret), required => true}, #{})
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
                ?HOCON(emqx_schema:timeout_duration_s(), #{
                    desc => ?DESC(session_expiry),
                    default => <<"30s">>
                })},
            {require_pkce,
                ?HOCON(boolean(), #{
                    desc => ?DESC(require_pkce),
                    default => false
                })},
            {preferred_auth_methods,
                ?HOCON(
                    ?ARRAY(
                        ?ENUM([
                            private_key_jwt,
                            client_secret_jwt,
                            client_secret_post,
                            client_secret_basic,
                            none
                        ])
                    ),
                    #{
                        desc => ?DESC(preferred_auth_methods),
                        default => [
                            client_secret_post,
                            client_secret_basic,
                            none
                        ]
                    }
                )},
            {provider,
                ?HOCON(?ENUM([okta, generic]), #{
                    mapping => "oidcc.provider",
                    desc => ?DESC(provider),
                    default => generic
                })},
            {fallback_methods,
                ?HOCON(?ARRAY(binary()), #{
                    mapping => "oidcc.fallback_methods",
                    desc => ?DESC(fallback_methods),
                    default => [<<"RS256">>]
                })},
            {client_jwks,
                %% TODO: add url JWKS
                ?HOCON(?UNION([none, ?R_REF(client_file_jwks)]), #{
                    desc => ?DESC(client_jwks),
                    default => none
                })}
        ];
fields(client_file_jwks) ->
    [
        {type,
            ?HOCON(?ENUM([file]), #{
                desc => ?DESC(client_file_jwks_type),
                required => true
            })},
        {file,
            ?HOCON(binary(), #{
                desc => ?DESC(client_file_jwks_file),
                required => true
            })}
    ];
fields(login) ->
    [
        emqx_dashboard_sso_schema:backend_schema([oidc])
    ].

desc(oidc) ->
    "OIDC";
desc(client_file_jwks) ->
    ?DESC(client_file_jwks);
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
            ClientJwks = init_client_jwks(Config),
            {ok, #{
                name => ?PROVIDER_SVR_NAME,
                config => Config,
                client_jwks => ClientJwks,
                name_tokens => emqx_placeholder:preproc_tmpl(NameVar)
            }}
    end.

update(Config, State) ->
    destroy(State, false),
    create(Config).

destroy(State) ->
    destroy(State, true).

destroy(State, TryDelete) ->
    emqx_dashboard_sso_oidc_session:stop(),
    _ = TryDelete andalso try_delete_jwks_file(State),
    ok.

-dialyzer({nowarn_function, login/2}).
login(
    _Req,
    #{
        client_jwks := ClientJwks,
        config := #{
            clientid := ClientId,
            secret := Secret,
            scopes := Scopes,
            require_pkce := RequirePKCE,
            preferred_auth_methods := AuthMethods
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
    case emqx_dashboard_sso_oidc_session:new(Data) of
        {ok, State} ->
            case
                oidcc:create_redirect_url(
                    ?PROVIDER_SVR_NAME,
                    ClientId,
                    emqx_secret:unwrap(Secret),
                    Opts#{
                        state => State,
                        client_jwks => ClientJwks,
                        preferred_auth_methods => AuthMethods
                    }
                )
            of
                {ok, [Base, Delimiter, Params]} ->
                    RedirectUri = <<Base/binary, Delimiter/binary, Params/binary>>,
                    Redirect = {302, ?REDIRECT_HEADERS(RedirectUri), ?REDIRECT_BODY},
                    {redirect, Redirect};
                {error, _Reason} = Error ->
                    Error
            end;
        Error ->
            Error
    end.

convert_certs(
    Dir,
    #{
        <<"client_jwks">> := #{
            <<"type">> := file,
            <<"file">> := Content
        } = Jwks
    } = Conf
) ->
    case save_jwks_file(Dir, Content) of
        {ok, Path} ->
            Conf#{<<"client_jwks">> := Jwks#{<<"file">> := Path}};
        {error, Reason} ->
            ?SLOG(error, #{msg => "failed_to_save_client_jwks", reason => Reason}),
            throw("Failed to save client jwks")
    end;
convert_certs(_Dir, Conf) ->
    Conf.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

save_jwks_file(Dir, Content) ->
    case filelib:is_file(Content) of
        true ->
            {ok, Content};
        _ ->
            Path = filename:join([emqx_tls_lib:pem_dir(Dir), "client_jwks"]),
            write_jwks_file(Path, Content)
    end.

write_jwks_file(Path, Content) ->
    case filelib:ensure_dir(Path) of
        ok ->
            case file:write_file(Path, Content) of
                ok ->
                    {ok, Path};
                {error, Reason} ->
                    {error, #{failed_to_write_file => Reason, file_path => Path}}
            end;
        {error, Reason} ->
            {error, #{failed_to_create_dir_for => Path, reason => Reason}}
    end.

try_delete_jwks_file(#{config := #{client_jwks := #{type := file, file := File}}}) ->
    _ = file:delete(File),
    ok;
try_delete_jwks_file(_) ->
    ok.

maybe_require_pkce(false, Opts) ->
    Opts;
maybe_require_pkce(true, Opts) ->
    Opts#{
        require_pkce => true,
        pkce_verifier => emqx_dashboard_sso_oidc_session:random_bin(?PKCE_VERIFIER_LEN)
    }.

init_client_jwks(#{client_jwks := #{type := file, file := File}}) ->
    try
        case jose_jwk:from_file(File) of
            {error, Reason} ->
                ?SLOG(error, #{msg => "failed_to_initialize_jwks", reason => Reason}),
                none;
            Jwks ->
                Jwks
        end
    catch
        _:CReason ->
            ?SLOG(error, #{msg => "failed_to_initialize_jwks", reason => CReason}),
            none
    end;
init_client_jwks(_) ->
    none.
