%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_rbac).

-include_lib("emqx_utils/include/emqx_api_key_scopes.hrl").
-include_lib("emqx_dashboard/include/emqx_dashboard.hrl").

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
    AbsPath = cowboy_req:path(Req),
    case emqx_dashboard_swagger:get_relative_uri(AbsPath) of
        {ok, Path} ->
            check_rbac(Role, Method, Path, Username, Backend);
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
    %% Self-targeted user endpoints (own change_pwd / own MFA) bypass
    %% the scope check — they are governed by RBAC's self-check (above)
    %% and, for MFA, by emqx_dashboard_api:authorize_mfa_change/3
    %% (force_mfa snapshot, admin_required, mfa_management self-
    %% exemption). Subjecting them to user_management / mfa_management
    %% scope here would lock viewers out of changing their own
    %% password / setting up their own MFA — exactly the opposite of
    %% what the scope was designed to gate (managing OTHER users).
    case is_self_user_endpoint(Path, Username) of
        true -> true;
        false -> check_login_user_scopes_strict(Username, Path)
    end.

check_login_user_scopes_strict(Username, Path) ->
    %% Always work on the effective scope list (role-default expanded)
    %% so administrators with no explicit scopes implicitly hold the
    %% full catalog and viewers implicitly hold the common scopes.
    %% Explicit [] is honoured as "no permissions".
    Scopes = emqx_dashboard_admin:effective_scopes_of(Username),
    case emqx_mgmt_api_key_scopes:path_to_scope(Path) of
        undefined -> true;
        PathScope -> lists:member(PathScope, Scopes)
    end.

%% Match /users/<self>/anything (with %-encoded segments) regardless
%% of whether Username is a bare binary (local) or a
%% ?SSO_USERNAME(Backend, Name) tuple (SSO; sub-path uses just Name).
is_self_user_endpoint(<<"/users/", SubPath/binary>>, Username) ->
    case binary:split(SubPath, <<"/">>, [global]) of
        [SelfSeg | _] ->
            Decoded = uri_string:percent_decode(SelfSeg),
            is_same_user(Decoded, Username);
        _ ->
            false
    end;
is_self_user_endpoint(_Path, _Username) ->
    false.

is_same_user(Decoded, Decoded) -> true;
is_same_user(Decoded, {_Backend, Decoded}) -> true;
is_same_user(_, _) -> false.

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
check_rbac(?ROLE_SUPERUSER, _, _, _, _) ->
    true;
check_rbac(?ROLE_VIEWER, <<"GET">>, _, _, _) ->
    true;
check_rbac(?ROLE_API_PUBLISHER, <<"POST">>, <<"/publish">>, _, _) ->
    true;
check_rbac(?ROLE_API_PUBLISHER, <<"POST">>, <<"/publish/bulk">>, _, _) ->
    true;
%% everyone should allow to logout
check_rbac(?ROLE_VIEWER, <<"POST">>, <<"/logout">>, _, _) ->
    true;
%% viewer should allow to change self password and (re)setup multi-factor auth for self,
%% superuser should allow to change any user
check_rbac(?ROLE_VIEWER, <<"POST">>, <<"/users/", SubPath/binary>>, Username, _) ->
    case decode_path_segments(SubPath) of
        [Username, <<"change_pwd">>] -> true;
        [Username, <<"mfa">>] -> true;
        _ -> false
    end;
check_rbac(?ROLE_VIEWER, <<"DELETE">>, <<"/users/", SubPath/binary>>, Username, _Backend) ->
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
check_rbac(_, _, _, _, _) ->
    false.

decode_path_segments(SubPath) ->
    [uri_string:percent_decode(Segment) || Segment <- binary:split(SubPath, <<"/">>, [global])].

role_list(dashboard) ->
    [?ROLE_VIEWER, ?ROLE_SUPERUSER];
role_list(api) ->
    [?ROLE_API_VIEWER, ?ROLE_API_PUBLISHER, ?ROLE_API_SUPERUSER].
