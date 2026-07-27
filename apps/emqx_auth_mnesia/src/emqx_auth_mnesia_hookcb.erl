%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_auth_mnesia_hookcb).

-moduledoc """
Hook callbacks for the built-in database authentication/authorization backends.
""".

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

-doc """
Deletes all built-in database authentication users (password-based and SCRAM) and
authorization rules belonging to the deleted namespace.
""".
on_namespace_delete(Namespace) ->
    ok = emqx_authn_mnesia:purge_namespace(Namespace),
    ok = emqx_authn_scram_mnesia:purge_namespace(Namespace),
    ok = emqx_authz_mnesia:purge_rules(Namespace),
    ok.
