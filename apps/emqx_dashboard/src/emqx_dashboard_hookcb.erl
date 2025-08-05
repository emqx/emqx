%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_dashboard_hookcb).

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
    ok = emqx_dashboard_admin:remove_all_users_from_namespace(Namespace),
    ok.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------
