%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_zerobus_hookcb).

%% API
-export([
    register_hooks/0,
    unregister_hooks/0
]).

%% Hooks
-export([schema_registry_serde_updated/1]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-include("emqx_bridge_zerobus.hrl").

-define(PRIO, 500).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

register_hooks() ->
    ok = emqx_hooks:add(
        'schema_registry.serde_updated',
        {?MODULE, schema_registry_serde_updated, []},
        ?PRIO
    ),
    ok.

unregister_hooks() ->
    ok = emqx_hooks:del(
        'schema_registry.serde_updated', {?MODULE, schema_registry_serde_updated}
    ),
    ok.

%%------------------------------------------------------------------------------
%% Hooks
%%------------------------------------------------------------------------------

schema_registry_serde_updated(Ctx) ->
    #{name := SerdeName} = Ctx,
    notify_actions_updated_serde(SerdeName),
    ok.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

notify_actions_updated_serde(SerdeName) ->
    Writers = emqx_bridge_zerobus_utils:list_all_stream_writer_pids(),
    lists:foreach(
        fun(Writer) ->
            emqx_bridge_zerobus_stream_writer_worker:serde_updated(Writer, SerdeName)
        end,
        Writers
    ).
