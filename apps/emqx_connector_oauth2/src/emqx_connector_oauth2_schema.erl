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
    validate_no_auth_header_conflict/2
]).

-define(ENABLED_HEADERS, [<<"authorization">>]).

namespace() -> "connector_oauth2".

%% A self-contained `oauth2` field to be appended
%% right after the `headers` field in the HTTP connector / authn_http / authz_http
%% schemas.
oauth2_field() ->
    {oauth2,
        mk(
            hoconsc:union(fun oauth2_union_member_selector/1),
            #{
                required => false,
                desc => ?DESC("oauth2")
            }
        )}.

fields(oauth2_disabled) ->
    [
        {enable,
            mk(false, #{
                default => false,
                required => true,
                desc => ?DESC("oauth2_enable")
            })}
    ];
fields(client_credentials) ->
    [
        {enable,
            mk(true, #{
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
                required => true,
                validator => fun validate_token_endpoint/1,
                desc => ?DESC("oauth2_token_endpoint")
            })},
        {client_id,
            mk(binary(), #{
                required => true,
                desc => ?DESC("oauth2_client_id")
            })},
        {client_secret,
            emqx_schema_secret:mk(#{
                required => true,
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
            })},
        {ssl,
            mk(ref(emqx_schema, "ssl_client_opts"), #{
                default => #{<<"enable">> => true},
                desc => ?DESC("oauth2_ssl")
            })}
    ].

desc(Name) when Name =:= oauth2_disabled; Name =:= client_credentials ->
    ?DESC("oauth2");
desc(_) ->
    undefined.

%%------------------------------------------------------------------------------
%% Validations
%%------------------------------------------------------------------------------

%% Check used from the HTTP connector / authn_http / authz_http config assembly
%% points.  Structural constraints are enforced by the union schema above; this
%% cross-field check rejects a manually configured `authorization' header.
-spec validate(Headers, Oauth2) -> ok | {error, term()} when
    Headers :: map() | undefined,
    Oauth2 :: map() | undefined.
validate(Headers, Oauth2) ->
    validate_no_auth_header_conflict(Headers, Oauth2).

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

oauth2_union_member_selector(all_union_members) ->
    [ref(?MODULE, oauth2_disabled), ref(?MODULE, client_credentials)];
oauth2_union_member_selector({value, Value}) ->
    case conf_get(enable, Value, false) of
        false ->
            [ref(?MODULE, oauth2_disabled)];
        true ->
            select_grant_type(conf_get(grant_type, Value, client_credentials))
    end.

select_grant_type(client_credentials) ->
    [ref(?MODULE, client_credentials)];
select_grant_type(<<"client_credentials">>) ->
    [ref(?MODULE, client_credentials)];
select_grant_type(GrantType) ->
    throw(#{field_name => grant_type, expected => [client_credentials], got => GrantType}).

conf_get(Key, Conf, Default) ->
    maps:get(Key, Conf, maps:get(atom_to_binary(Key), Conf, Default)).

validate_token_endpoint(Endpoint) ->
    try emqx_utils_uri:parse(Endpoint) of
        #{
            scheme := Scheme,
            authority := #{host := Host, userinfo := undefined},
            fragment := undefined
        } when Scheme =:= <<"http">>; Scheme =:= <<"https">> ->
            case emqx_utils_ssrf:check_host(Host) of
                ok -> ok;
                {error, Error} -> {error, emqx_utils_ssrf:format_error(Error)}
            end;
        #{scheme := Scheme} when Scheme =/= <<"http">>, Scheme =/= <<"https">> ->
            {error, unsupported_scheme};
        _ ->
            {error, invalid_token_endpoint}
    catch
        _:_ ->
            {error, invalid_token_endpoint}
    end.

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
