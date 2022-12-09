%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ee_connector_redis).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-behaviour(emqx_resource).

%% callbacks of behaviour emqx_resource
-export([
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_batch_query/3,
    on_get_status/2
]).

%% -------------------------------------------------------------------------------------------------
%% resource callbacks
%% -------------------------------------------------------------------------------------------------

callback_mode() -> always_sync.

on_start(InstId, #{command_template := CommandTemplate} = Config) ->
    case emqx_connector_redis:on_start(InstId, Config) of
        {ok, RedisConnSt} ->
            ?tp(
                redis_ee_connector_start_success,
                #{}
            ),
            {ok, #{
                conn_st => RedisConnSt,
                command_template => preproc_command_template(CommandTemplate)
            }};
        {error, _} = Error ->
            ?tp(
                redis_ee_connector_start_error,
                #{error => Error}
            ),
            Error
    end.

on_stop(InstId, #{conn_st := RedisConnSt}) ->
    emqx_connector_redis:on_stop(InstId, RedisConnSt).

on_get_status(InstId, #{conn_st := RedisConnSt}) ->
    emqx_connector_redis:on_get_status(InstId, RedisConnSt).

on_query(
    InstId,
    {send_message, Data},
    _State = #{
        command_template := CommandTemplate, conn_st := RedisConnSt
    }
) ->
    Cmd = proc_command_template(CommandTemplate, Data),
    ?tp(
        redis_ee_connector_cmd,
        #{cmd => Cmd, batch => false, mode => sync}
    ),
    Result = query(InstId, {cmd, Cmd}, RedisConnSt),
    ?tp(
        redis_ee_connector_send_done,
        #{cmd => Cmd, batch => false, mode => sync, result => Result}
    ),
    Result;
on_query(
    InstId,
    Query,
    _State = #{conn_st := RedisConnSt}
) ->
    ?tp(
        redis_ee_connector_query,
        #{query => Query, batch => false, mode => sync}
    ),
    Result = query(InstId, Query, RedisConnSt),
    ?tp(
        redis_ee_connector_send_done,
        #{query => Query, batch => false, mode => sync, result => Result}
    ),
    Result.

on_batch_query(
    InstId, BatchData, _State = #{command_template := CommandTemplate, conn_st := RedisConnSt}
) ->
    Cmds = process_batch_data(BatchData, CommandTemplate),
    ?tp(
        redis_ee_connector_send,
        #{batch_data => BatchData, batch => true, mode => sync}
    ),
    Result = query(InstId, {cmds, Cmds}, RedisConnSt),
    ?tp(
        redis_ee_connector_send_done,
        #{
            batch_data => BatchData,
            batch_size => length(BatchData),
            batch => true,
            mode => sync,
            result => Result
        }
    ),
    Result.

%% -------------------------------------------------------------------------------------------------
%% private helpers
%% -------------------------------------------------------------------------------------------------

query(InstId, Query, RedisConnSt) ->
    case emqx_connector_redis:on_query(InstId, Query, RedisConnSt) of
        {ok, _} = Ok -> Ok;
        {error, no_connection} -> {error, {recoverable_error, no_connection}};
        {error, _} = Error -> Error
    end.

process_batch_data(BatchData, CommandTemplate) ->
    lists:map(
        fun({send_message, Data}) ->
            proc_command_template(CommandTemplate, Data)
        end,
        BatchData
    ).

proc_command_template(CommandTemplate, Msg) ->
    lists:map(
        fun(ArgTks) ->
            emqx_plugin_libs_rule:proc_tmpl(ArgTks, Msg, #{return => full_binary})
        end,
        CommandTemplate
    ).

preproc_command_template(CommandTemplate) ->
    lists:map(
        fun emqx_plugin_libs_rule:preproc_tmpl/1,
        CommandTemplate
    ).
