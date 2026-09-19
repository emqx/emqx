%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_rbac).

-include_lib("emqx_utils/include/emqx_api_key_scopes.hrl").
-include_lib("emqx_dashboard/include/emqx_dashboard.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
    check_rbac/3,
    check_login_user_scopes/2,
    role/1,
    valid_dashboard_role/1,
    valid_api_role/1
]).

-dialyzer({nowarn_function, role/1}).
%%=====================================================================
%% API
check_rbac(Req, Username, Extra) ->
    Role = role(Extra),
    Backend = backend(Extra),
    Method = cowboy_req:method(Req),
    case relative_path(Req) of
        {ok, Path} ->
            check_rbac(Role, Method, Path, Username, Backend, Req);
        _ ->
            false
    end.

%% Look up the login user's `scopes' from the admin record's extra map
%% and cross-reference against the path-to-scope mapping built from all
%% minirest_api modules' scopes/0 callbacks. Semantics:
%%
%%   * scopes absent  (undefined)        -> fall back to RBAC default
%%                                          (already passed at this
%%                                          point), so allow.
%%   * scopes = [...]  (list)            -> path must map to one of
%%                                          the listed scopes; unmapped
%%                                          paths fail-open (allow).
%%
%% The unmapped-path fail-open is consistent with API key scope
%% semantics (emqx_mgmt_auth:check_path_in_scopes/2). CT
%% t_all_endpoints_covered_by_scopes guards against accidentally
%% leaving a non-public path unmapped.
%%
%% IMPORTANT: this predicate is for dashboard LOGIN users only. It must
%% NOT be invoked from API-key authorisation paths because:
%%   1. API keys have their own scope mechanism via
%%      emqx_mgmt_auth:check_path_in_scopes/2 — invoking this on top
%%      is redundant.
%%   2. If an API-key string value collided with a dashboard username,
%%      this lookup would resolve against that user's extra.scopes and
%%      produce a wrong authorisation decision for the API key.
%% Callers MUST ensure `Username' is the dashboard admin record's
%% primary key (binary for local users, ?SSO_USERNAME tuple for SSO
%% users). The dashboard token verifier reconstructs the SSO tuple via
%% emqx_dashboard_token:resolve_admin_key/1 before invoking us.
check_login_user_scopes(Username, Req) when is_map(Req) ->
    AbsPath = cowboy_req:path(Req),
    case emqx_dashboard_swagger:get_relative_uri(AbsPath) of
        {ok, Path} -> check_login_user_scopes_for_path(Username, Path);
        _ -> false
    end;
check_login_user_scopes(Username, Path) when is_binary(Path) ->
    check_login_user_scopes_for_path(Username, Path).

check_login_user_scopes_for_path(Username, Path) ->
    %% Self-service endpoints — the user's own change_pwd / mfa —
    %% bypass the scope check: they are gated by RBAC's self rule
    %% and, for MFA, by emqx_dashboard_api:authorize_mfa_change/3
    %% (admin_override decision, mfa_management self-exemption).
    %% Locking viewers out of changing their own password / setting
    %% up their own MFA via the scope check would defeat the
    %% scope's purpose, which is to gate management of OTHER users.
    %%
    %% The bypass is intentionally restricted to those two actions.
    %% PUT/DELETE on /users/<self> itself MUST still be scope-
    %% checked — otherwise an admin who explicitly set
    %% `scopes = []' could PUT their own record to add admin-only
    %% scopes back, defeating the explicit self-restriction.
    case is_self_service_endpoint(Path, Username) of
        true -> true;
        false -> check_login_user_scopes_strict(Username, Path)
    end.

check_login_user_scopes_strict(Username, Path) ->
    case emqx_mgmt_api_key_scopes:classify_path(Path) of
        %% Explicitly public endpoint — allow regardless of scopes.
        public ->
            true;
        %% Path maps to no known scope. Fail closed only for users that
        %% carry an explicit scope list (deliberately restricted), so a
        %% catalog gap cannot silently grant them an unmapped endpoint.
        %% Users with no explicit scopes are not scope-restricted and stay
        %% governed by role-based RBAC alone, so they are not locked out.
        not_found ->
            emqx_dashboard_admin:scopes_of(Username) =:= undefined;
        {scope, PathScope} ->
            %% Work on the effective scope list (role-default expanded) so
            %% administrators with no explicit scopes implicitly hold the
            %% full catalog and viewers implicitly hold the common scopes.
            %% Explicit [] is honoured as "no permissions".
            Scopes = emqx_dashboard_admin:effective_scopes_of(Username),
            lists:member(PathScope, Scopes)
    end.

%% Whitelist of self-service paths that may skip the login-user
%% scope check. Currently only the own password and MFA endpoints —
%% extending this whitelist requires careful thought because it
%% creates a hole where an admin who self-restricted via explicit
%% scopes can no longer be reliably restricted.
%%
%% Match /users/<self>/change_pwd or /users/<self>/mfa (with
%% %-encoded segments) regardless of whether Username is a bare
%% binary (local) or a ?SSO_USERNAME(Backend, Name) tuple (SSO;
%% the sub-path uses just Name).
is_self_service_endpoint(<<"/users/", SubPath/binary>>, Username) ->
    case binary:split(SubPath, <<"/">>, [global]) of
        [SelfSeg, Action] when
            Action =:= <<"change_pwd">>;
            Action =:= <<"mfa">>
        ->
            Decoded = uri_string:percent_decode(SelfSeg),
            is_same_user(Decoded, Username);
        _ ->
            false
    end;
is_self_service_endpoint(_Path, _Username) ->
    false.

is_same_user(Decoded, Decoded) -> true;
is_same_user(Decoded, {_Backend, Decoded}) -> true;
is_same_user(_, _) -> false.

%% `cowboy_req:path/1' is the raw request target, while `cowboy_router' dispatches
%% on the percent-decoded, dot-segment-collapsed path (`cowboy_router:split_path/1')
%% and does not expose that path back to the request. Resolve it the same way
%% before matching, otherwise an equivalent alias such as `/%63onfigs' would reach
%% the same handler while missing a rule here.
%%
%% `uri_string:normalize/1' decodes the unreserved percent-escapes and removes the
%% dot segments exactly like the router, but it keeps the trailing empty segment
%% that the router drops, hence `without_trailing_slashes/1'.
relative_path(Req) ->
    RawPath = cowboy_req:path(Req),
    Path =
        try
            uri_string:normalize(RawPath)
        catch
            _:_ -> RawPath
        end,
    case emqx_dashboard_swagger:get_relative_uri(Path) of
        {ok, RelPath} -> {ok, without_trailing_slashes(RelPath)};
        Error -> Error
    end.

%% `cowboy_router:split_path/1' ignores the trailing empty segment, so
%% `/configs/' and `/configs/.' reach the same handler as `/configs'.
without_trailing_slashes(<<>>) ->
    <<>>;
without_trailing_slashes(Path) ->
    case binary:last(Path) of
        $/ -> without_trailing_slashes(binary:part(Path, 0, byte_size(Path) - 1));
        _ -> Path
    end.

%% For compatibility
role(#?ADMIN{role = undefined}) ->
    ?ROLE_SUPERUSER;
role(#?ADMIN{role = Role}) ->
    Role;
%% For compatibility
role([]) ->
    ?ROLE_SUPERUSER;
role(#{role := Role}) ->
    Role;
role(Role) when is_binary(Role) ->
    Role.

backend(#{backend := Backend}) ->
    Backend;
backend(_) ->
    ?BACKEND_LOCAL.

valid_dashboard_role(Role) ->
    valid_role(dashboard, Role).

valid_api_role(Role) ->
    valid_role(api, Role).

%% ===================================================================

valid_role(Type, Role) ->
    case lists:member(Role, role_list(Type)) of
        true ->
            ok;
        _ ->
            {error, <<"Role does not exist">>}
    end.

%% ===================================================================
check_rbac(?ROLE_SUPERUSER, _, _, _, _, _) ->
    true;
%% `GET /configs' negotiated to `text/plain' is the cleartext HOCON dump which
%% pairs with `PUT /configs' (export/reload), so it is reserved to administrators.
%% The JSON variant of the same endpoint is redacted and stays available to viewers.
check_rbac(?ROLE_VIEWER, <<"GET">>, <<"/configs">>, _Username, _Backend, Req) ->
    not wants_plaintext_config_dump(Req);
check_rbac(?ROLE_VIEWER, <<"GET">>, _, _, _, _) ->
    true;
check_rbac(?ROLE_API_PUBLISHER, <<"POST">>, <<"/publish">>, _, _, _) ->
    true;
check_rbac(?ROLE_API_PUBLISHER, <<"POST">>, <<"/publish/bulk">>, _, _, _) ->
    true;
%% everyone should allow to logout
check_rbac(?ROLE_VIEWER, <<"POST">>, <<"/logout">>, _, _, _) ->
    true;
%% viewer should allow to change self password and (re)setup multi-factor auth for self,
%% superuser should allow to change any user
check_rbac(?ROLE_VIEWER, <<"POST">>, <<"/users/", SubPath/binary>>, Username, _, _) ->
    case decode_path_segments(SubPath) of
        [Username, <<"change_pwd">>] -> true;
        [Username, <<"mfa">>] -> true;
        _ -> false
    end;
check_rbac(?ROLE_VIEWER, <<"DELETE">>, <<"/users/", SubPath/binary>>, Username, _Backend, _) ->
    %% RBAC decides only that viewer may DELETE its OWN mfa endpoint.
    %% Policy state (admin_override lock and mfa_management self-
    %% exemption) is decided in emqx_dashboard_api:authorize_mfa_change/3.
    %% RBAC must not consult the live backend force_mfa flag here —
    %% doing so would bypass admin_override and prevent mfa_management
    %% scope holders from self-exempting.
    case decode_path_segments(SubPath) of
        [Username, <<"mfa">>] -> true;
        _ -> false
    end;
check_rbac(_, _, _, _, _, _) ->
    false.

decode_path_segments(SubPath) ->
    [uri_string:percent_decode(Segment) || Segment <- binary:split(SubPath, <<"/">>, [global])].

%% Keeps in sync with the `Accept' negotiation in
%% `emqx_mgmt_api_configs:configs/3': it prefers `text/plain', and a missing
%% `Accept' header (or `*/*') resolves to the first preference, so both count
%% as a request for the plaintext HOCON dump.
wants_plaintext_config_dump(Req) ->
    Accept = cowboy_req:header(<<"accept">>, Req, <<"*/*">>),
    Accepts = [
        begin
            [T | _] = binary:split(string:trim(S), <<";">>),
            T
        end
     || S <- re:split(Accept, ",")
    ],
    lists:member(<<"*/*">>, Accepts) orelse lists:member(<<"text/plain">>, Accepts).

role_list(dashboard) ->
    [?ROLE_VIEWER, ?ROLE_SUPERUSER];
role_list(api) ->
    [?ROLE_API_VIEWER, ?ROLE_API_PUBLISHER, ?ROLE_API_SUPERUSER].

%% ===================================================================
%% Unit tests
%% ===================================================================
-ifdef(TEST).

wants_plaintext_config_dump_test_() ->
    [
        %% No Accept header (or `*/*') negotiates to the first preference of
        %% `GET /configs', which is `text/plain'.
        ?_assert(wants_plaintext_config_dump(fake_req(<<"GET">>, <<"/configs">>))),
        ?_assert(wants_plaintext_config_dump(fake_req(<<"GET">>, <<"/configs">>, <<"*/*">>))),
        ?_assert(
            wants_plaintext_config_dump(fake_req(<<"GET">>, <<"/configs">>, <<"text/plain">>))
        ),
        ?_assert(
            wants_plaintext_config_dump(
                fake_req(<<"GET">>, <<"/configs">>, <<"application/json, */*;q=0.8">>)
            )
        ),
        ?_assert(
            wants_plaintext_config_dump(
                fake_req(<<"GET">>, <<"/configs">>, <<"text/html, text/plain;q=0.9">>)
            )
        ),
        ?_assertNot(
            wants_plaintext_config_dump(
                fake_req(<<"GET">>, <<"/configs">>, <<"application/json">>)
            )
        ),
        ?_assertNot(
            wants_plaintext_config_dump(
                fake_req(<<"GET">>, <<"/configs">>, <<"application/xml">>)
            )
        )
    ].

check_rbac_configs_test_() ->
    Get = <<"GET">>,
    Put = <<"PUT">>,
    User = <<"u">>,
    [
        %% The administrator keeps the cleartext dump: it is the export half of
        %% the `GET /configs' -> `PUT /configs' (export/reload) workflow.
        ?_assert(
            check_rbac(?ROLE_SUPERUSER, Get, <<"/configs">>, User, undefined, plaintext_req())
        ),
        ?_assert(
            check_rbac(?ROLE_SUPERUSER, Get, <<"/configs">>, User, undefined, no_accept_req())
        ),
        ?_assert(check_rbac(?ROLE_SUPERUSER, Get, <<"/configs">>, User, undefined, json_req())),
        ?_assert(check_rbac(?ROLE_SUPERUSER, Get, <<"/other">>, User, undefined, plaintext_req())),
        ?_assert(
            check_rbac(?ROLE_SUPERUSER, Put, <<"/configs">>, User, undefined, plaintext_req())
        ),
        %% A viewer is denied the cleartext dump only; the redacted JSON path and
        %% the other endpoints are untouched.
        ?_assertNot(
            check_rbac(?ROLE_VIEWER, Get, <<"/configs">>, User, undefined, plaintext_req())
        ),
        ?_assertNot(
            check_rbac(?ROLE_VIEWER, Get, <<"/configs">>, User, undefined, no_accept_req())
        ),
        ?_assertNot(
            check_rbac(
                ?ROLE_VIEWER,
                Get,
                <<"/configs">>,
                User,
                undefined,
                fake_req(<<"GET">>, <<"/configs">>, <<"application/json, */*;q=0.8">>)
            )
        ),
        ?_assert(check_rbac(?ROLE_VIEWER, Get, <<"/configs">>, User, undefined, json_req())),
        ?_assert(check_rbac(?ROLE_VIEWER, Get, <<"/other">>, User, undefined, plaintext_req())),
        ?_assertNot(
            check_rbac(?ROLE_VIEWER, Put, <<"/configs">>, User, undefined, plaintext_req())
        ),
        %% The new clause must narrow the viewer only: no other role gains access
        %% to `GET /configs' by matching it.
        ?_assertNot(
            check_rbac(?ROLE_API_PUBLISHER, Get, <<"/configs">>, User, undefined, plaintext_req())
        ),
        ?_assertNot(
            check_rbac(?ROLE_API_PUBLISHER, Get, <<"/configs">>, User, undefined, no_accept_req())
        ),
        ?_assertNot(
            check_rbac(?ROLE_API_PUBLISHER, Get, <<"/configs">>, User, undefined, json_req())
        ),
        ?_assertNot(
            check_rbac(?ROLE_API_PUBLISHER, Get, <<"/other">>, User, undefined, plaintext_req())
        )
    ].

plaintext_req() ->
    fake_req(<<"GET">>, <<"/configs">>, <<"text/plain">>).

no_accept_req() ->
    fake_req(<<"GET">>, <<"/configs">>).

json_req() ->
    fake_req(<<"GET">>, <<"/configs">>, <<"application/json">>).

fake_req(Method, Path) ->
    fake_req(Method, Path, undefined).

fake_req(Method, Path, Accept) ->
    Headers =
        case Accept of
            undefined -> #{};
            _ -> #{<<"accept">> => Accept}
        end,
    #{method => Method, path => <<"/api/v5", Path/binary>>, headers => Headers}.

%% The exported entry point must evaluate the path `cowboy_router' matched, so a
%% percent-encoded or dot-segment alias of `/configs' must not bypass the rule.
check_rbac_path_aliases_test_() ->
    Viewer = #{role => ?ROLE_VIEWER},
    [
        ?_assertNot(
            check_rbac(fake_req(<<"GET">>, <<"/%63onfigs">>, <<"text/plain">>), <<"u">>, Viewer)
        ),
        ?_assertNot(
            check_rbac(fake_req(<<"GET">>, <<"/conf%69gs">>, <<"text/plain">>), <<"u">>, Viewer)
        ),
        ?_assertNot(
            check_rbac(fake_req(<<"GET">>, <<"/./configs">>, <<"text/plain">>), <<"u">>, Viewer)
        ),
        ?_assertNot(
            check_rbac(fake_req(<<"GET">>, <<"/x/../configs">>, <<"text/plain">>), <<"u">>, Viewer)
        ),
        %% `cowboy_router' drops the trailing empty segment as well.
        ?_assertNot(
            check_rbac(fake_req(<<"GET">>, <<"/configs/">>, <<"text/plain">>), <<"u">>, Viewer)
        ),
        ?_assertNot(
            check_rbac(fake_req(<<"GET">>, <<"/configs/.">>, <<"text/plain">>), <<"u">>, Viewer)
        ),
        %% The redacted JSON variant stays reachable through the same alias.
        ?_assert(
            check_rbac(
                fake_req(<<"GET">>, <<"/%63onfigs">>, <<"application/json">>), <<"u">>, Viewer
            )
        )
    ].

-endif.
