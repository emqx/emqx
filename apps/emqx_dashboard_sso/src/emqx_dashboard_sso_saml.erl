%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_sso_saml).

-include_lib("emqx_dashboard/include/emqx_dashboard.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("esaml/include/esaml.hrl").

-behaviour(emqx_dashboard_sso).

-export([
    hocon_ref/0,
    login_ref/0,
    fields/1,
    desc/1
]).

%% emqx_dashboard_sso callbacks
-export([
    create/1,
    update/2,
    destroy/1
]).

-export([login/2, callback/2]).

-dialyzer({nowarn_function, create/1}).

%%------------------------------------------------------------------------------
%% Hocon Schema
%%------------------------------------------------------------------------------

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

%% TOOD: support raw xml metadata in hocon (maybe?🤔)
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

create(
    #{
        dashboard_addr := DashboardAddr,
        idp_metadata_url := IDPMetadataURL,
        sp_sign_request := SignRequest
    } = Config
) ->
    BaseURL = binary_to_list(DashboardAddr) ++ "/api/v5",
    %% {Config, State} = parse_config(Config),
    SP = esaml_sp:setup(#esaml_sp{
        %% TODO: save cert and key then return path
        %% TODO: #esaml_sp.key #esaml_sp.certificate support
        %% key = PrivKey,
        %% certificate = Cert,
        sp_sign_requests = SignRequest,
        trusted_fingerprints = [],
        consume_uri = BaseURL ++ "/sso/saml/acs",
        metadata_uri = BaseURL ++ "/sso/saml/metadata",
        org = #esaml_org{
            name = "EMQX Team",
            displayname = "EMQX Dashboard",
            url = DashboardAddr
        },
        tech = #esaml_contact{
            name = "EMQX Team",
            email = "contact@emqx.io"
        }
    }),
    try
        IdpMeta = esaml_util:load_metadata(binary_to_list(IDPMetadataURL)),
        {ok, Config#{idp_meta => IdpMeta, sp => SP}}
    catch
        Kind:Error ->
            ?SLOG(error, #{msg => failed_to_load_metadata, kind => Kind, error => Error}),
            {error, failed_to_load_metadata}
    end.

update(_Config0, State) ->
    {ok, State}.

destroy(_State) ->
    ok.

login(_Req, #{sp := SP, idp_meta := #esaml_idp_metadata{login_location = IDP}} = _State) ->
    SignedXml = esaml_sp:generate_authn_request(IDP, SP),
    Target = esaml_binding:encode_http_redirect(IDP, SignedXml, <<>>),
    %% TODO: _Req acutally is HTTP request body, not fully request
    RedirectFun = fun(Headers) ->
        RespHeaders = #{<<"Cache-Control">> => <<"no-cache">>, <<"Pragma">> => <<"no-cache">>},
        case is_msie(Headers) of
            true ->
                Html = esaml_binding:encode_http_post(IDP, SignedXml, <<>>),
                {200, RespHeaders, Html};
            false ->
                RespHeaders1 = RespHeaders#{<<"Location">> => Target},
                {302, RespHeaders1, <<"Redirecting...">>}
        end
    end,
    {redirect, RedirectFun}.

callback(Req, #{sp := SP} = _State) ->
    case esaml_cowboy:validate_assertion(SP, fun esaml_util:check_dupe_ets/2, Req) of
        {ok, Assertion, _RelayState, _Req2} ->
            Subject = Assertion#esaml_assertion.subject,
            Username = iolist_to_binary(Subject#esaml_subject.name),
            ensure_user_exists(Username);
        {error, Reason0, _Req2} ->
            Reason = [
                "Access denied, assertion failed validation:\n", io_lib:format("~p\n", [Reason0])
            ],
            {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

%% -define(DIR, <<"SAML_SSO_sp_certs">>).
%% -define(RSA_KEYS_A, [sp_public_key, sp_private_key]).

is_msie(Headers) ->
    UA = maps:get(<<"user-agent">>, Headers, <<"">>),
    not (binary:match(UA, <<"MSIE">>) =:= nomatch).

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
