%%--------------------------------------------------------------------
%% Copyright (c) 2022-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_redis_connector).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-behaviour(emqx_resource).

%% callbacks of behaviour emqx_resource
-export([
    callback_mode/0,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channels/1,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_batch_query/3,
    on_get_status/2,
    on_get_channel_status/3,
    on_format_query_result/1
]).

%% -------------------------------------------------------------------------------------------------
%% resource callbacks
%% -------------------------------------------------------------------------------------------------

callback_mode() -> always_sync.

on_add_channel(
    _InstanceId,
    State = #{channels := Channels},
    ChannelId,
    #{
        parameters := #{
            command_template := Template
        }
    }
) ->
    Channels2 = Channels#{
        ChannelId => #{template => preproc_command_template(Template)}
    },
    {ok, State#{channels => Channels2}}.

on_remove_channel(_InstanceId, State = #{channels := Channels}, ChannelId) ->
    {ok, State#{channels => maps:remove(ChannelId, Channels)}}.

on_get_channels(InstanceId) ->
    emqx_bridge_v2:get_channels_for_connector(InstanceId).

on_get_channel_status(_ConnectorResId, ChannelId, #{channels := Channels}) ->
    case maps:is_key(ChannelId, Channels) of
        true -> ?status_connected;
        false -> ?status_disconnected
    end.

on_start(InstId, Config) ->
    case emqx_redis:on_start(InstId, Config) of
        {ok, RedisConnSt} ->
            ?tp(
                redis_bridge_connector_start_success,
                #{}
            ),
            {ok, #{
                conn_st => RedisConnSt,
                channels => #{}
            }};
        {error, {start_pool_failed, _, #{type := authentication_error, reason := Reason}}} = Error ->
            ?tp(
                redis_bridge_connector_start_error,
                #{error => Error}
            ),
            throw({unhealthy_target, Reason});
        {error, _} = Error ->
            ?tp(
                redis_bridge_connector_start_error,
                #{error => Error}
            ),
            Error
    end.

on_stop(InstId, #{conn_st := RedisConnSt}) ->
    Res = emqx_redis:on_stop(InstId, RedisConnSt),
    ?tp(redis_bridge_stopped, #{instance_id => InstId}),
    Res;
on_stop(InstId, undefined = _State) ->
    Res = emqx_redis:on_stop(InstId, undefined),
    ?tp(redis_bridge_stopped, #{instance_id => InstId}),
    Res.

on_get_status(InstId, #{conn_st := RedisConnSt}) ->
    emqx_redis:on_get_status(InstId, RedisConnSt).

%% raw cmd without template, for CI test
on_query(InstId, {cmd, Cmd}, #{conn_st := RedisConnSt}) ->
    ?tp(
        redis_bridge_connector_cmd,
        #{cmd => Cmd, batch => false, mode => sync}
    ),
    Result = query(InstId, {cmd, Cmd}, RedisConnSt),
    ?tp(
        redis_bridge_connector_send_done,
        #{instance_id => InstId, cmd => Cmd, batch => false, mode => sync, result => Result}
    ),
    Result;
on_query(
    InstId,
    {MessageTag, _Data} = Msg,
    #{channels := Channels, conn_st := RedisConnSt}
) ->
    case try_render_message([Msg], Channels) of
        {ok, [Cmd]} ->
            ?tp(
                redis_bridge_connector_cmd,
                #{cmd => Cmd, batch => false, mode => sync}
            ),
            emqx_trace:rendered_action_template(
                MessageTag,
                #{command => Cmd, batch => false}
            ),
            Result = query(InstId, {cmd, Cmd}, RedisConnSt),
            ?tp(
                redis_bridge_connector_send_done,
                #{instance_id => InstId, cmd => Cmd, batch => false, mode => sync, result => Result}
            ),
            Result;
        Error ->
            Error
    end.

on_batch_query(
    InstId, BatchData, _State = #{channels := Channels, conn_st := RedisConnSt}
) ->
    case try_render_message(BatchData, Channels) of
        {ok, Cmds} ->
            ?tp(
                redis_bridge_connector_send,
                #{batch_data => BatchData, batch => true, mode => sync}
            ),
            [{ChannelID, _} | _] = BatchData,
            emqx_trace:rendered_action_template(
                ChannelID,
                #{commands => Cmds, batch => ture}
            ),
            Result = query(InstId, {cmds, Cmds}, RedisConnSt),
            ?tp(
                redis_bridge_connector_send_done,
                #{
                    instance_id => InstId,
                    batch_data => BatchData,
                    batch_size => length(BatchData),
                    batch => true,
                    mode => sync,
                    result => Result
                }
            ),
            Result;
        Error ->
            Error
    end.

on_format_query_result({ok, Msg}) ->
    #{result => ok, message => Msg};
on_format_query_result(Res) ->
    Res.

%% -------------------------------------------------------------------------------------------------
%% private helpers
%% -------------------------------------------------------------------------------------------------

try_render_message(Datas, Channels) ->
    try_render_message(Datas, Channels, []).

try_render_message([{MessageTag, Data} | T], Channels, Acc) ->
    case maps:find(MessageTag, Channels) of
        {ok, #{template := Template}} ->
            Msg = proc_command_template(Template, Data),
            try_render_message(T, Channels, [Msg | Acc]);
        _ ->
            {error, {unrecoverable_error, {invalid_message_tag, MessageTag}}}
    end;
try_render_message([], _Channels, Acc) ->
    {ok, lists:reverse(Acc)}.

query(InstId, Query, RedisConnSt) ->
    case emqx_redis:on_query(InstId, Query, RedisConnSt) of
        {ok, _} = Ok -> Ok;
        {error, no_connection} -> {error, {recoverable_error, no_connection}};
        {error, _} = Error -> Error
    end.

proc_command_template(CommandTemplate, Msg) ->
    lists:map(
        fun(ArgTks) ->
            emqx_placeholder:proc_tmpl(ArgTks, Msg, #{return => full_binary})
        end,
        CommandTemplate
    ).

preproc_command_template(CommandTemplate) ->
    lists:map(
        fun emqx_placeholder:preproc_tmpl/1,
        CommandTemplate
    ).
