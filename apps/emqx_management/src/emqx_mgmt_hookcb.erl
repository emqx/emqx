%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mgmt_hookcb).

%% API
-export([
    register_hooks/0,
    unregister_hooks/0,
    on_namespace_delete/1
]).

-include_lib("emqx/include/emqx_hooks.hrl").

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(NS_DELETE_HOOK, {?MODULE, on_namespace_delete, []}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

register_hooks() ->
    ok = emqx_hooks:add('namespace.delete', ?NS_DELETE_HOOK, ?HP_HIGHEST),
    ok.

unregister_hooks() ->
    ok = emqx_hooks:del('namespace.delete', ?NS_DELETE_HOOK),
    ok.

on_namespace_delete(Namespace) ->
    ok = emqx_mgmt_auth:delete_all_keys_from_namespace(Namespace),
    ok = purge_builtin_auth_data(Namespace),
    ok.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

-doc """
Deletes all built-in database authentication users (password-based and SCRAM) and
authorization rules belonging to the deleted namespace.

The built-in database tables exist only while the `emqx_auth_mnesia` application is
running; its top supervisor creates them.
""".
purge_builtin_auth_data(Namespace) ->
    case is_pid(whereis(emqx_auth_mnesia_sup)) of
        true ->
            ok = emqx_authn_mnesia:purge_namespace(Namespace),
            ok = emqx_authn_scram_mnesia:purge_namespace(Namespace),
            ok = emqx_authz_mnesia:purge_rules(Namespace);
        false ->
            ok
    end.
