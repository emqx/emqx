%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_rbac).

-include_lib("emqx_dashboard/include/emqx_dashboard.hrl").
-include_lib("emqx_dashboard/include/emqx_dashboard_rbac.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-export([
    check_rbac/3,
    parse_dashboard_role/1,
    parse_api_role/1
]).

-export_type([actor_context/0]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-type actor_context() :: #{
    ?actor := username() | api_key(),
    ?role := role(),
    ?namespace := ?global_ns | namespace()
}.

-type username() :: binary().
-type api_key() :: binary().
-type role() :: binary().
-type namespace() :: binary().

-define(API(MOD, METHOD, FN), #{method := METHOD, module := MOD, function := FN}).
-define(DASHBOARD_API(METHOD, FN), ?API(emqx_dashboard_api, METHOD, FN)).
-define(CONNECTOR_API(METHOD, FN), ?API(emqx_connector_api, METHOD, FN)).
-define(BRIDGE_V2_API(METHOD, FN), ?API(emqx_bridge_v2_api, METHOD, FN)).

%%=====================================================================
%% API
-spec check_rbac(emqx_dashboard:request(), emqx_dashboard:handler_info(), actor_context()) ->
    {ok, actor_context()} | false.
check_rbac(Req, HandlerInfo, ActorContext) ->
    maybe
        true ?= do_check_rbac(ActorContext, Req, HandlerInfo),
        {ok, ActorContext}
    end.

parse_dashboard_role(Role) ->
    parse_role(dashboard, Role).

parse_api_role(Role) ->
    parse_role(api, Role).

%% ===================================================================

parse_role(Type, Role0) ->
    maybe
        {ok, #{?role := Role} = ParsedRole} ?= do_parse_role(Role0),
        true ?= lists:member(Role, role_list(Type)),
        {ok, ParsedRole}
    else
        false ->
            {error, <<"Role does not exist">>};
        Error ->
            Error
    end.

do_parse_role(Role0) when is_binary(Role0) ->
    maybe
        [NsTag, Role] ?= binary:split(Role0, <<"::">>),
        {ok, Ns} ?= parse_namespace_tag(NsTag),
        {ok, #{?role => Role, ?namespace => Ns}}
    else
        [Role1] ->
            {ok, #{?role => Role1, ?namespace => ?global_ns}};
        {error, _} = Error ->
            Error;
        _ ->
            {error, <<"Role does not exist">>}
    end;
do_parse_role(_) ->
    {error, <<"Invalid role">>}.

parse_namespace_tag(NsTag) ->
    case binary:split(NsTag, <<":">>) of
        [<<"ns">>, Ns] ->
            {ok, Ns};
        _ ->
            {error, <<"Invalid namespace tag">>}
    end.

%% ===================================================================
-spec do_check_rbac(actor_context(), emqx_dashboard:request(), emqx_dashboard:handler_info()) ->
    boolean().
do_check_rbac(#{?role := ?ROLE_SUPERUSER, ?namespace := ?global_ns}, _, _) ->
    %% Global administrator
    true;
do_check_rbac(#{?role := ?ROLE_SUPERUSER}, _, #{method := get}) ->
    %% Namespaced administrator; It's fine for such admins to `GET` anything, even outside
    %% their namespace.  Namespaces are mostly to avoid accidentally mutating the wrong
    %% resources rather than hiding information.
    true;
do_check_rbac(#{?role := ?ROLE_VIEWER}, _, #{method := get}) ->
    true;
do_check_rbac(
    #{?role := ?ROLE_API_PUBLISHER},
    _,
    #{method := post, module := emqx_mgmt_api_publish, function := Fn}
) when Fn == publish; Fn == publish_batch ->
    %% emqx_mgmt_api_publish:publish
    %% emqx_mgmt_api_publish:publish_batch
    true;
%% everyone should allow to logout
do_check_rbac(#{}, _, ?DASHBOARD_API(post, logout)) ->
    %% emqx_dashboard_api:logout
    true;
%% viewer should allow to change self password and (re)setup multi-factor auth for self,
%% superuser should allow to change any user
do_check_rbac(
    #{?role := ?ROLE_VIEWER, ?actor := Username},
    Req,
    ?DASHBOARD_API(post, Fn)
) when Fn == change_pwd; Fn == change_mfa ->
    %% emqx_dashboard_api:change_pwd
    %% emqx_dashboard_api:change_mfa
    case Req of
        #{bindings := #{username := Username}} ->
            true;
        _ ->
            false
    end;
do_check_rbac(
    #{?role := ?ROLE_VIEWER, ?actor := Username},
    Req,
    ?DASHBOARD_API(delete, change_mfa)
) ->
    %% emqx_dashboard_api:change_mfa
    case Req of
        #{bindings := #{username := Username}} ->
            true;
        _ ->
            false
    end;
do_check_rbac(#{?role := ?ROLE_SUPERUSER, ?namespace := Namespace}, _Req, ?CONNECTOR_API(_, _)) when
    is_binary(Namespace)
->
    %% Namespaced connector API; may only alter resources in its own namespace.
    %% This is enforced by the handlers themselves, by only fetching/acting on the
    %% appropriate namespace.
    true;
do_check_rbac(#{?role := ?ROLE_SUPERUSER, ?namespace := Namespace}, _Req, ?BRIDGE_V2_API(_, _)) when
    is_binary(Namespace)
->
    %% Namespaced action/source APIs; may only alter resources in its own namespace.  This
    %% is enforced by the handlers themselves, by only fetching/acting on the appropriate
    %% namespace.
    true;
do_check_rbac(_, _, _) ->
    false.

role_list(dashboard) ->
    [?ROLE_VIEWER, ?ROLE_SUPERUSER];
role_list(api) ->
    [?ROLE_API_VIEWER, ?ROLE_API_PUBLISHER, ?ROLE_API_SUPERUSER].
