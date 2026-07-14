%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQX Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_connector_oauth2_schema).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").

-import(hoconsc, [mk/2, enum/1, ref/2]).

-export([
    namespace/0,
    fields/1,
    desc/1,
    oauth2_field/0,
    validate/2,
    validate_oauth2/1,
    validate_no_auth_header_conflict/2
]).

-define(ENABLED_HEADERS, [<<"authorization">>]).

namespace() -> "connector_oauth2".

%% A self-contained `oauth2` field (name + schema + validator) to be appended
%% right after the `headers` field in the HTTP connector / authn_http / authz_http
%% schemas.
oauth2_field() ->
    {oauth2,
        mk(
            ref(?MODULE, oauth2),
            #{
                required => false,
                desc => ?DESC("oauth2"),
                validator => fun ?MODULE:validate_oauth2/1
            }
        )}.

fields(oauth2) ->
    [
        {enable,
            mk(boolean(), #{
                default => false,
                required => true,
                desc => ?DESC("oauth2_enable")
            })},
        {grant_type,
            mk(enum([client_credentials]), #{
                default => client_credentials,
                required => true,
                desc => ?DESC("oauth2_grant_type"),
                importance => ?IMPORTANCE_HIDDEN
            })},
        {token_endpoint,
            mk(binary(), #{
                required => false,
                desc => ?DESC("oauth2_token_endpoint")
            })},
        {client_id,
            mk(binary(), #{
                required => false,
                desc => ?DESC("oauth2_client_id")
            })},
        {client_secret,
            emqx_schema_secret:mk(#{
                required => false,
                desc => ?DESC("oauth2_client_secret")
            })},
        {scope,
            mk(binary(), #{
                required => false,
                desc => ?DESC("oauth2_scope")
            })},
        {timeout,
            mk(emqx_schema:timeout_duration_ms(), #{
                default => <<"5s">>,
                desc => ?DESC("oauth2_timeout")
            })}
    ].

desc(oauth2) ->
    ?DESC("oauth2");
desc(_) ->
    undefined.

%%------------------------------------------------------------------------------
%% Validations
%%------------------------------------------------------------------------------

%% Combined check used from the HTTP connector / authn_http / authz_http config
%% assembly points: validates both the internal consistency of the `oauth2'
%% block (required fields when enabled) and that it does not conflict with a
%% manually configured `authorization' header.
-spec validate(Headers, Oauth2) -> ok | {error, term()} when
    Headers :: map() | undefined,
    Oauth2 :: map() | undefined.
validate(Headers, Oauth2) ->
    case validate_oauth2(Oauth2) of
        ok ->
            validate_no_auth_header_conflict(Headers, Oauth2);
        {error, _} = Error ->
            Error
    end.

%% Validates the internal consistency of the `oauth2` block.
%% When `enable = true`, the credentials needed to obtain a token must be
%% present.  Returns `ok` or `{error, iodata()}`.
-spec validate_oauth2(hocon:config() | undefined) -> ok | {error, term()}.
validate_oauth2(undefined) ->
    ok;
validate_oauth2(#{enable := false}) ->
    ok;
validate_oauth2(#{enable := true} = Oauth2) ->
    Required = [token_endpoint, client_id, client_secret],
    Missing = [
        K
     || K <- Required,
        maps:get(K, Oauth2, undefined) =:= undefined
    ],
    case Missing of
        [] ->
            ok;
        _ ->
            {error, #{
                reason => oauth2_missing_fields,
                missing => Missing,
                message =>
                    <<
                        "OAuth2 is enabled but the following fields are missing: "
                        "token_endpoint, client_id and client_secret are all required."
                    >>
            }}
    end;
validate_oauth2(_Other) ->
    ok.

%% Checks that the user did not configure an HTTP header that OAuth2 owns.
%% When OAuth2 is enabled, the connector injects `Authorization: Bearer <token>`
%% on every request; a manually configured `authorization` header would
%% therefore be a conflict and must be rejected at config-check time.
-spec validate_no_auth_header_conflict(Headers, Oauth2) -> ok | {error, term()} when
    Headers :: map() | undefined,
    Oauth2 :: map() | undefined.
validate_no_auth_header_conflict(_Headers, undefined) ->
    ok;
validate_no_auth_header_conflict(_Headers, #{enable := false}) ->
    ok;
validate_no_auth_header_conflict(Headers, #{enable := true}) when
    Headers =:= undefined; Headers =:= #{}
->
    ok;
validate_no_auth_header_conflict(Headers, #{enable := true}) when is_map(Headers) ->
    Conflicting = [
        K
     || K <- maps:keys(Headers),
        lists:member(lower_bin(K), ?ENABLED_HEADERS)
    ],
    case Conflicting of
        [] ->
            ok;
        _ ->
            {error, #{
                reason => oauth2_auth_header_conflict,
                headers => Conflicting,
                message =>
                    <<
                        "The 'authorization' header conflicts with OAuth2: when OAuth2 "
                        "is enabled, the access token is injected as the Authorization "
                        "header automatically. Please remove the 'authorization' header."
                    >>
            }}
    end;
validate_no_auth_header_conflict(_, _) ->
    ok.

%%------------------------------------------------------------------------------
%% Internal helpers
%%------------------------------------------------------------------------------

lower_bin(K) when is_binary(K) ->
    try
        iolist_to_binary(string:lowercase(K))
    catch
        _:_ -> K
    end;
lower_bin(K) when is_list(K) ->
    lower_bin(iolist_to_binary(K));
lower_bin(K) when is_atom(K) ->
    lower_bin(atom_to_binary(K, utf8)).
