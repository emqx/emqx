%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_sso_saml_api).

-behaviour(minirest_api).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").

-import(hoconsc, [
    mk/2,
    array/1,
    enum/1,
    ref/1
]).

-import(emqx_dashboard_sso, [provider/1]).

-export([
    api_spec/0,
    check_api_schema/2,
    paths/0,
    schema/1,
    namespace/0
]).

-export([
    sp_saml_metadata/2,
    sp_saml_callback/2
]).

-define(REDIRECT, 'REDIRECT').
-define(BAD_USERNAME_OR_PWD, 'BAD_USERNAME_OR_PWD').
-define(BACKEND_NOT_FOUND, 'BACKEND_NOT_FOUND').
-define(TAGS, <<"Dashboard Single Sign-On">>).

namespace() -> "dashboard_sso".

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{
        translate_body => false,
        check_schema => fun ?MODULE:check_api_schema/2
    }).

check_api_schema(Params, Meta) ->
    emqx_dashboard_swagger:validate_content_type(Params, Meta, <<"application/xml">>).

paths() ->
    [
        "/sso/saml/acs",
        "/sso/saml/metadata"
    ].

%% Handles HTTP-POST bound assertions coming back from the IDP.
schema("/sso/saml/acs") ->
    #{
        'operationId' => sp_saml_callback,
        post => #{
            tags => [?TAGS],
            desc => ?DESC(saml_sso_acs),
            %% 'requestbody' => urlencoded_request_body(),
            responses => #{
                302 => response_schema(302),
                401 => response_schema(401),
                404 => response_schema(404)
            },
            security => []
        }
    };
schema("/sso/saml/metadata") ->
    #{
        'operationId' => sp_saml_metadata,
        get => #{
            tags => [?TAGS],
            desc => ?DESC(sp_saml_metadata),
            'requestbody' => saml_metadata_response(),
            responses => #{
                200 => emqx_dashboard_api:fields([token, version, license]),
                404 => response_schema(404)
            }
        }
    }.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

sp_saml_metadata(get, _Req) ->
    case emqx_dashboard_sso_manager:lookup_state(saml) of
        #{enable := true, sp := SP} = _State ->
            SignedXml = esaml_sp:generate_metadata(SP),
            Metadata = xmerl:export([SignedXml], xmerl_xml),
            {200, #{<<"content-type">> => <<"text/xml">>}, erlang:iolist_to_binary(Metadata)};
        _ ->
            {404, #{code => ?BACKEND_NOT_FOUND, message => <<"Backend not found">>}}
    end.

sp_saml_callback(post, Req) ->
    minirest_handler:update_log_meta(#{log_from => saml}),
    case emqx_dashboard_sso_manager:lookup_state(saml) of
        State = #{enable := true} ->
            case (provider(saml)):callback(Req, State) of
                {redirect, Username, Redirect} ->
                    ?SLOG(info, #{
                        msg => "dashboard_saml_sso_login_successful",
                        redirect => "SAML login successful. Redirecting with LoginMeta."
                    }),
                    minirest_handler:update_log_meta(#{log_source => Username}),
                    Redirect;
                {error, Reason} ->
                    ?SLOG(info, #{
                        msg => "dashboard_saml_sso_login_failed",
                        request => Req,
                        reason => Reason
                    }),
                    {403, #{code => <<"UNAUTHORIZED">>, message => Reason}}
            end;
        _ ->
            {404, #{code => ?BACKEND_NOT_FOUND, message => <<"Backend not found">>}}
    end.

%%--------------------------------------------------------------------
%% internal
%%--------------------------------------------------------------------

response_schema(302) ->
    emqx_dashboard_swagger:error_codes([?REDIRECT], ?DESC(redirect));
response_schema(401) ->
    emqx_dashboard_swagger:error_codes([?BAD_USERNAME_OR_PWD], ?DESC(login_failed401));
response_schema(404) ->
    emqx_dashboard_swagger:error_codes([?BACKEND_NOT_FOUND], ?DESC(backend_not_found)).

saml_metadata_response() ->
    #{
        'content' => #{
            'application/xml' => #{
                schema => #{
                    type => <<"string">>,
                    format => <<"binary">>
                }
            }
        }
    }.
