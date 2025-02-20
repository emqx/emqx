%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_sso_saml).

-include_lib("emqx_dashboard/include/emqx_dashboard.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("esaml/include/esaml.hrl").

-behaviour(emqx_dashboard_sso).

-export([
    namespace/0,
    hocon_ref/0,
    login_ref/0,
    fields/1,
    desc/1
]).

%% emqx_dashboard_sso callbacks
-export([
    create/1,
    update/2,
    destroy/1,
    convert_certs/2
]).

-export([login/2, callback/2]).

-dialyzer({nowarn_function, do_create/1}).

-define(RESPHEADERS, #{
    <<"cache-control">> => <<"no-cache">>,
    <<"pragma">> => <<"no-cache">>,
    <<"content-type">> => <<"text/plain">>
}).
-define(REDIRECT_BODY, <<"Redirecting...">>).

-define(DIR, <<"saml_sp_certs">>).

%%------------------------------------------------------------------------------
%% Hocon Schema
%%------------------------------------------------------------------------------

namespace() -> "dashboard".

hocon_ref() ->
    hoconsc:ref(?MODULE, saml).

login_ref() ->
    hoconsc:ref(?MODULE, login).

fields(saml) ->
    emqx_dashboard_sso_schema:common_backend_schema([saml]) ++
        [
            {dashboard_addr, fun dashboard_addr/1},
            {idp_metadata_url, fun idp_metadata_url/1},
            {sp_sign_request, fun sp_sign_request/1},
            {sp_public_key, fun sp_public_key/1},
            {sp_private_key, fun sp_private_key/1}
        ];
fields(login) ->
    [
        emqx_dashboard_sso_schema:backend_schema([saml])
    ].

dashboard_addr(type) -> binary();
%% without any path
dashboard_addr(desc) -> ?DESC(dashboard_addr);
dashboard_addr(default) -> <<"https://127.0.0.1:18083">>;
dashboard_addr(_) -> undefined.

%% TODO: support raw xml metadata in hocon (maybe?🤔)
idp_metadata_url(type) -> binary();
idp_metadata_url(desc) -> ?DESC(idp_metadata_url);
idp_metadata_url(default) -> <<"https://idp.example.com">>;
idp_metadata_url(_) -> undefined.

sp_sign_request(type) -> boolean();
sp_sign_request(desc) -> ?DESC(sign_request);
sp_sign_request(default) -> false;
sp_sign_request(_) -> undefined.

sp_public_key(type) -> binary();
sp_public_key(desc) -> ?DESC(sp_public_key);
sp_public_key(default) -> <<"Pub Key">>;
sp_public_key(_) -> undefined.

sp_private_key(type) -> binary();
sp_private_key(desc) -> ?DESC(sp_private_key);
sp_private_key(required) -> false;
sp_private_key(format) -> <<"password">>;
sp_private_key(sensitive) -> true;
sp_private_key(_) -> undefined.

desc(saml) ->
    "saml";
desc(_) ->
    undefined.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

create(#{enable := false} = _Config) ->
    {ok, undefined};
create(#{sp_sign_request := true} = Config) ->
    try
        do_create(Config)
    catch
        Kind:Error ->
            Msg = failed_to_ensure_cert_and_key,
            ?SLOG(error, #{msg => Msg, kind => Kind, error => Error}),
            {error, Msg}
    end;
create(#{sp_sign_request := false} = Config) ->
    do_create(Config#{sp_private_key => undefined, sp_public_key => undefined}).

update(Config0, State) ->
    destroy(State),
    create(Config0).

destroy(_State) ->
    _ = file:del_dir_r(emqx_tls_lib:pem_dir(?DIR)),
    _ = application:stop(esaml),
    ok.

login(
    #{headers := Headers} = _Req,
    #{sp := SP, idp_meta := #esaml_idp_metadata{login_location = IDP}} = _State
) ->
    SignedXml = esaml_sp:generate_authn_request(IDP, SP),
    Target = esaml_binding:encode_http_redirect(IDP, SignedXml, <<>>),
    Redirect =
        case is_msie(Headers) of
            true ->
                Html = esaml_binding:encode_http_post(IDP, SignedXml, <<>>),
                {200, ?RESPHEADERS, Html};
            false ->
                {302, ?MAPPEND(?RESPHEADERS, #{<<"location">> => Target}), ?REDIRECT_BODY}
        end,
    {redirect, Redirect}.

callback(_Req = #{body := Body}, #{sp := SP, dashboard_addr := DashboardAddr} = _State) ->
    case do_validate_assertion(SP, fun esaml_util:check_dupe_ets/2, Body) of
        {ok, Assertion, _RelayState} ->
            Subject = Assertion#esaml_assertion.subject,
            Username = iolist_to_binary(Subject#esaml_subject.name),
            gen_redirect_response(DashboardAddr, Username);
        {error, Reason0} ->
            Reason = [
                "Access denied, assertion failed validation:\n", io_lib:format("~p\n", [Reason0])
            ],
            {error, iolist_to_binary(Reason)}
    end.

convert_certs(
    Dir,
    #{<<"sp_sign_request">> := true, <<"sp_public_key">> := Cert, <<"sp_private_key">> := Key} =
        Conf
) ->
    case
        emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir(
            Dir, #{enable => true, certfile => Cert, keyfile => Key}, #{}
        )
    of
        {ok, #{certfile := CertPath, keyfile := KeyPath}} ->
            Conf#{<<"sp_public_key">> => bin(CertPath), <<"sp_private_key">> => bin(KeyPath)};
        {error, Reason} ->
            ?SLOG(error, #{msg => "failed_to_save_sp_sign_keys", reason => Reason}),
            throw("Failed to save sp signing key(s)")
    end;
convert_certs(_Dir, Conf) ->
    Conf.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

bin(X) -> iolist_to_binary(X).

do_create(
    #{
        dashboard_addr := DashboardAddr,
        idp_metadata_url := IDPMetadataURL,
        sp_sign_request := SpSignRequest,
        sp_private_key := KeyPath,
        sp_public_key := CertPath
    } = Config
) ->
    {ok, _} = application:ensure_all_started(esaml),
    BaseURL = binary_to_list(DashboardAddr) ++ "/api/v5",
    SP = esaml_sp:setup(#esaml_sp{
        key = maybe_load_cert_or_key(KeyPath, fun esaml_util:load_private_key/1),
        certificate = maybe_load_cert_or_key(CertPath, fun esaml_util:load_certificate/1),
        sp_sign_requests = SpSignRequest,
        trusted_fingerprints = [],
        consume_uri = BaseURL ++ "/sso/saml/acs",
        metadata_uri = BaseURL ++ "/sso/saml/metadata",
        %% TODO: support conf org and contact
        org = #esaml_org{
            name = "EMQX",
            displayname = "EMQX Dashboard",
            url = DashboardAddr
        },
        tech = #esaml_contact{
            name = "EMQX",
            email = "contact@emqx.io"
        }
    }),
    try
        IdpMeta = esaml_util:load_metadata(binary_to_list(IDPMetadataURL)),
        State = Config,
        {ok, State#{idp_meta => IdpMeta, sp => SP}}
    catch
        Kind:Error ->
            Reason = failed_to_load_metadata,
            ?SLOG(error, #{msg => Reason, kind => Kind, error => Error}),
            {error, Reason}
    end.

do_validate_assertion(SP, DuplicateFun, Body) ->
    PostVals = cow_qs:parse_qs(Body),
    SAMLEncoding = proplists:get_value(<<"SAMLEncoding">>, PostVals),
    SAMLResponse = proplists:get_value(<<"SAMLResponse">>, PostVals),
    RelayState = proplists:get_value(<<"RelayState">>, PostVals),
    case (catch esaml_binding:decode_response(SAMLEncoding, SAMLResponse)) of
        {'EXIT', Reason} ->
            {error, {bad_decode, Reason}};
        Xml ->
            case esaml_sp:validate_assertion(Xml, DuplicateFun, SP) of
                {ok, A} -> {ok, A, RelayState};
                {error, E} -> {error, E}
            end
    end.

gen_redirect_response(DashboardAddr, Username) ->
    case ensure_user_exists(Username) of
        {ok, Role, Token} ->
            Target = login_redirect_target(DashboardAddr, Username, Role, Token),
            Response = {302, ?MAPPEND(?RESPHEADERS, #{<<"location">> => Target}), ?REDIRECT_BODY},
            {redirect, Username, Response};
        {error, Reason} ->
            {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% Helpers functions
%%------------------------------------------------------------------------------

%% TODO: unify with emqx_dashboard_sso_manager:ensure_user_exists/1
ensure_user_exists(Username) ->
    case emqx_dashboard_admin:lookup_user(saml, Username) of
        [User] ->
            emqx_dashboard_token:sign(User, <<>>);
        [] ->
            case emqx_dashboard_admin:add_sso_user(saml, Username, ?ROLE_VIEWER, <<>>) of
                {ok, _} ->
                    ensure_user_exists(Username);
                Error ->
                    Error
            end
    end.

maybe_load_cert_or_key(undefined, _) ->
    undefined;
maybe_load_cert_or_key(Path, Func) ->
    Func(Path).

is_msie(Headers) ->
    UA = maps:get(<<"user-agent">>, Headers, <<"">>),
    not (binary:match(UA, <<"MSIE">>) =:= nomatch).

login_redirect_target(DashboardAddr, Username, Role, Token) ->
    LoginMeta = emqx_dashboard_sso_api:login_meta(Username, Role, Token, saml),
    <<DashboardAddr/binary, "/?login_meta=", (base64_login_meta(LoginMeta))/binary>>.

base64_login_meta(LoginMeta) ->
    base64:encode(emqx_utils_json:encode(LoginMeta)).
