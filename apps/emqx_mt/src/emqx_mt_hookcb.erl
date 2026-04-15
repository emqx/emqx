%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc This module implelements the hook callback for multi-tenancy.
-module(emqx_mt_hookcb).

-export([
    register_hooks/0,
    unregister_hooks/0,
    on_session_created/2,
    on_authenticate/2,
    on_post_authn/1,
    on_api_actor_will_be_created/2,
    on_namespace_resource_pre_create/2
]).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_dashboard/include/emqx_dashboard_rbac.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-define(TRACE(MSG, META), ?TRACE("MULTI_TENANCY", MSG, META)).
-define(SESSION_HOOK, {?MODULE, on_session_created, []}).
-define(AUTHN_HOOK, {?MODULE, on_authenticate, []}).
-define(POST_AUTHN_HOOK, {?MODULE, on_post_authn, []}).
-define(LIMITER_HOOK, {emqx_mt_limiter, adjust_limiter, []}).
-define(USER_CREATION_HOOK, {?MODULE, on_api_actor_will_be_created, []}).

register_hooks() ->
    ok = emqx_hooks:add('session.created', ?SESSION_HOOK, ?HP_HIGHEST),
    ok = emqx_hooks:add('client.authenticate', ?AUTHN_HOOK, ?HP_HIGHEST),
    ok = emqx_hooks:add('client.post_authn', ?POST_AUTHN_HOOK, ?HP_HIGHEST),
    ok = emqx_hooks:add('channel.limiter_adjustment', ?LIMITER_HOOK, ?HP_HIGHEST),
    ok = emqx_hooks:add('api_actor.pre_create', ?USER_CREATION_HOOK, ?HP_HIGHEST),
    ok = emqx_hooks:add(
        'namespace.resource_pre_create',
        {?MODULE, on_namespace_resource_pre_create, []},
        ?HP_HIGHEST
    ),
    ok.

unregister_hooks() ->
    ok = emqx_hooks:del('session.created', ?SESSION_HOOK),
    ok = emqx_hooks:del('client.authenticate', ?AUTHN_HOOK),
    ok = emqx_hooks:del('client.post_authn', ?POST_AUTHN_HOOK),
    ok = emqx_hooks:del('channel.limiter_adjustment', ?LIMITER_HOOK),
    ok = emqx_hooks:del('api_actor.pre_create', ?USER_CREATION_HOOK),
    ok = emqx_hooks:del(
        'namespace.resource_pre_create', {?MODULE, on_namespace_resource_pre_create}
    ),
    ok.

on_session_created(
    #{
        clientid := ClientId,
        client_attrs := #{?CLIENT_ATTR_NAME_TNS := Tns}
    },
    _SessionInfo
) ->
    ?TRACE("session_registered_in_namespace", #{}),
    ok = emqx_mt_pool:add(Tns, ClientId, self());
on_session_created(_ClientInfo, _SessionInfo) ->
    %% not a multi-tenant client
    ok.

on_authenticate(ClientInfo, DefaultResult) ->
    case emqx_mt_config:get_post_auth_tns_expression() of
        undefined ->
            do_on_authenticate(ClientInfo, DefaultResult);
        _Compiled ->
            %% Namespace will be (re)derived by the `client.post_authn' hook
            %% from authn-response client_attrs.  Defer all tns-based gating
            %% (namespace config errors, quota, managed-ns membership) to
            %% that hook so that pre-auth tns does not cause spurious
            %% rejections.
            ?TRACE("defer_tns_gate_to_post_authn", #{}),
            DefaultResult
    end.

do_on_authenticate(
    #{clientid := ClientId, client_attrs := #{?CLIENT_ATTR_NAME_TNS := Tns}}, DefaultResult
) ->
    case emqx_config:get_namespace_config_errors(Tns) of
        undefined ->
            decide(ClientId, Tns, DefaultResult);
        #{} ->
            ?TRACE("deny_due_to_namespace_config_errors", #{tns => Tns}),
            {stop, {error, server_unavailable}}
    end;
do_on_authenticate(_, DefaultResult) ->
    AllowOnlyManagedNSs = emqx_mt_config:get_allow_only_managed_namespaces(),
    case AllowOnlyManagedNSs of
        true ->
            ?TRACE("deny_due_to_no_tenant_namespace", #{}),
            {stop, {error, not_authorized}};
        false ->
            ?TRACE("no_tenant_namespace", #{}),
            DefaultResult
    end.

%% Pure namespace/quota decision shared between the pre-auth `client.authenticate'
%% and post-auth `client.post_authn' callbacks. `OnPass' is whatever the caller
%% wants returned when the check passes (e.g. the auth-hook's DefaultResult, or a
%% {ok, ClientInfo} for the post-authn path).
decide(ClientId, Tns, OnPass) ->
    case emqx_mt_state:is_known_client(Tns, ClientId) of
        {true, Node} ->
            %% the client is re-connecting
            %% allow it to continue without checking the session count
            %% because the session count is already checked when the client is registered
            ?TRACE("existing_session_found", #{reside_in => Node}),
            OnPass;
        false ->
            case emqx_mt_state:count_clients(Tns) of
                {ok, Count} ->
                    Max = emqx_mt_config:get_max_sessions(Tns),
                    case Max =/= infinity andalso Count >= Max of
                        true ->
                            ?TRACE("session_count_quota_exceeded", #{}),
                            {stop, {error, quota_exceeded}};
                        false ->
                            ?TRACE("session_count_quota_available", #{}),
                            OnPass
                    end;
                {error, not_found} ->
                    AllowOnlyManagedNSs = emqx_mt_config:get_allow_only_managed_namespaces(),
                    case AllowOnlyManagedNSs of
                        true ->
                            ?TRACE("deny_due_to_no_tenant_namespace", #{}),
                            {stop, {error, not_authorized}};
                        false ->
                            ?TRACE("first_clientid_in_namespace", #{}),
                            OnPass
                    end
            end
    end.

%% Invoked on the `client.post_authn' hook. If the operator has configured
%% `multi_tenancy.post_auth_tns_expression', evaluate it against the merged
%% ClientInfo (which already contains pre-auth + authn-response client_attrs),
%% write the rendered value into `client_attrs.tns', and run namespace/quota
%% checks against that value.
%%
%% The accumulator is a `post_authn_context()' map (currently `#{client_info
%% := ClientInfo}'). Returns:
%%   * `ok' to accept without modification;
%%   * `{ok, NewCtx}' to replace the accumulator (with rewritten tns);
%%   * `{stop, {error, Reason}}' to reject the client with a CONNACK error.
on_post_authn(#{client_info := #{clientid := ClientId} = ClientInfo} = Ctx) ->
    case emqx_mt_config:get_post_auth_tns_expression() of
        undefined -> ok;
        Compiled -> eval_post_auth_tns_expression(Compiled, ClientId, ClientInfo, Ctx)
    end.

eval_post_auth_tns_expression(Compiled, ClientId, ClientInfo, Ctx) ->
    case emqx_variform:render(Compiled, ClientInfo) of
        {ok, <<>>} ->
            ?TRACE("post_auth_tns_expression_rendered_empty", #{}),
            ok;
        {ok, Tns} ->
            decide_with_rewritten_tns(ClientId, Tns, ClientInfo, Ctx);
        {error, Reason} ->
            ?SLOG(
                warning,
                #{msg => "post_auth_tns_expression_error", reason => Reason},
                #{clientid => ClientId}
            ),
            ok
    end.

decide_with_rewritten_tns(ClientId, Tns, ClientInfo, Ctx) ->
    case emqx_config:get_namespace_config_errors(Tns) of
        undefined ->
            decide(ClientId, Tns, {ok, Ctx#{client_info := set_tns(ClientInfo, Tns)}});
        #{} ->
            ?TRACE("deny_due_to_namespace_config_errors", #{tns => Tns}),
            {stop, {error, server_unavailable}}
    end.

set_tns(ClientInfo, Tns) ->
    Attrs = maps:get(client_attrs, ClientInfo, #{}),
    ClientInfo#{client_attrs => Attrs#{?CLIENT_ATTR_NAME_TNS => Tns}}.

on_api_actor_will_be_created(#{?namespace := ?global_ns}, Ok) ->
    Ok;
on_api_actor_will_be_created(#{?namespace := Namespace}, Ok) ->
    case emqx_mt_config:is_known_managed_ns(Namespace) of
        true ->
            Ok;
        false ->
            {stop, {error, #{reason => unknown_namespace, namespace => Namespace}}}
    end.

on_namespace_resource_pre_create(#{?namespace := ?global_ns}, ResCtx) ->
    {stop, ResCtx#{exists := true}};
on_namespace_resource_pre_create(#{?namespace := Namespace}, ResCtx) when is_binary(Namespace) ->
    case emqx_mt_config:is_known_managed_ns(Namespace) of
        true ->
            {stop, ResCtx#{exists := true}};
        false ->
            {stop, ResCtx#{exists := false}}
    end.
