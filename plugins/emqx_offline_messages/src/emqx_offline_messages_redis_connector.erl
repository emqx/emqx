%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_offline_messages_redis_connector).

%% The plugin is compiled separately from EMQX, so the behaviour cannot be found.
%% -behaviour(emqx_resource).

%% callbacks of behaviour emqx_resource
-export([
    resource_type/0,
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_batch_query/3,
    on_get_status/2
]).

resource_type() ->
    emqx_redis:resource_type().

callback_mode() ->
    emqx_redis:callback_mode().

on_start(InstId, Config) ->
    emqx_redis:on_start(InstId, Config).

on_stop(InstId, State) ->
    emqx_redis:on_stop(InstId, State).

on_query(InstId, Query, State) ->
    emqx_redis:on_query(InstId, Query, State).

on_batch_query(InstId, Batch, State) ->
    Cmds = lists:flatmap(
        fun(Query) ->
            case Query of
                {cmd, Cmd} -> [Cmd];
                {cmds, Cmds} -> Cmds
            end
        end,
        Batch
    ),
    emqx_redis:on_query(InstId, {cmds, Cmds}, State).

on_get_status(InstId, State) ->
    emqx_redis:on_get_status(InstId, State).
