%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_mysql_connector).

-behaviour(emqx_resource).

-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% `emqx_resource' API
-export([
    on_remove_channel/3,
    resource_type/0,
    callback_mode/0,
    on_add_channel/4,
    on_batch_query/3,
    on_get_channel_status/3,
    on_get_channels/1,
    on_get_status/2,
    on_query/3,
    on_start/2,
    on_stop/2
]).

%%========================================================================================
%% `emqx_resource' API
%%========================================================================================
resource_type() -> emqx_mysql:resource_type().

callback_mode() -> emqx_mysql:callback_mode().

on_add_channel(
    _InstanceId,
    #{channels := Channels, connector_state := ConnectorState} = State0,
    ChannelId,
    ChannelConfig0
) ->
    ChannelConfig1 = emqx_utils_maps:unindent(parameters, ChannelConfig0),
    QueryTemplates = emqx_mysql:parse_prepare_sql(ChannelId, ChannelConfig1),
    case validate_sql_type(ChannelId, ChannelConfig1, QueryTemplates) of
        ok ->
            ChannelConfig2 = maps:merge(ChannelConfig1, QueryTemplates),
            ChannelConfig = set_prepares(ChannelConfig2, ConnectorState),
            case maps:get(prepares, ChannelConfig) of
                {error, {Code, ErrState, Msg}} ->
                    Context = #{
                        code => Code,
                        state => ErrState,
                        message => Msg
                    },
                    {error, {prepare_statement, Context}};
                {error, undefined_table} ->
                    {error, {unhealthy_target, <<"Undefined table">>}};
                ok ->
                    State = State0#{
                        channels => maps:put(ChannelId, ChannelConfig, Channels),
                        connector_state => ConnectorState
                    },
                    {ok, State}
            end;
        {error, Error} ->
            {error, Error}
    end.

on_get_channel_status(_InstanceId, ChannelId, #{channels := Channels}) ->
    case maps:get(ChannelId, Channels) of
        #{prepares := ok} ->
            ?status_connected;
        #{prepares := {error, _}} ->
            ?status_connecting
    end.

on_get_channels(InstanceId) ->
    emqx_bridge_v2:get_channels_for_connector(InstanceId).

on_get_status(InstanceId, #{connector_state := ConnectorState}) ->
    emqx_mysql:on_get_status(InstanceId, ConnectorState).

on_query(InstId, {TypeOrKey, SQLOrKey}, State) ->
    on_query(InstId, {TypeOrKey, SQLOrKey, [], default_timeout}, State);
on_query(InstId, {TypeOrKey, SQLOrKey, Params}, State) ->
    on_query(InstId, {TypeOrKey, SQLOrKey, Params, default_timeout}, State);
on_query(
    InstanceId,
    {Channel, _Message, _Params, _Timeout} = Request,
    #{channels := Channels, connector_state := ConnectorState}
) when is_binary(Channel) ->
    ChannelConfig = maps:get(Channel, Channels),
    MergedState0 = maps:merge(ConnectorState, ChannelConfig),
    MergedState1 = MergedState0#{channel_id => Channel},
    Result = emqx_mysql:on_query(
        InstanceId,
        Request,
        MergedState1
    ),
    ?tp(mysql_connector_on_query_return, #{instance_id => InstanceId, result => Result}),
    Result;
on_query(InstanceId, Request, _State = #{channels := _Channels, connector_state := ConnectorState}) ->
    emqx_mysql:on_query(InstanceId, Request, ConnectorState).

on_batch_query(
    InstanceId,
    [Req | _] = BatchRequest,
    #{channels := Channels, connector_state := ConnectorState}
) when is_binary(element(1, Req)) ->
    Channel = element(1, Req),
    ChannelConfig = maps:get(Channel, Channels),
    MergedState0 = maps:merge(ConnectorState, ChannelConfig),
    MergedState1 = MergedState0#{channel_id => Channel},
    Result = emqx_mysql:on_batch_query(
        InstanceId,
        BatchRequest,
        MergedState1,
        ChannelConfig
    ),
    ?tp(mysql_connector_on_batch_query_return, #{instance_id => InstanceId, result => Result}),
    Result;
on_batch_query(InstanceId, BatchRequest, _State = #{connector_state := ConnectorState}) ->
    emqx_mysql:on_batch_query(InstanceId, BatchRequest, ConnectorState, #{}).

on_remove_channel(
    _InstanceId, #{channels := Channels, connector_state := ConnectorState} = State, ChannelId
) when is_map_key(ChannelId, Channels) ->
    ChannelConfig = maps:get(ChannelId, Channels),
    emqx_mysql:unprepare_sql(ChannelId, maps:merge(ChannelConfig, ConnectorState)),
    NewState = State#{channels => maps:remove(ChannelId, Channels)},
    {ok, NewState};
on_remove_channel(_InstanceId, State, _ChannelId) ->
    {ok, State}.

-spec on_start(binary(), hocon:config()) ->
    {ok, #{connector_state := emqx_mysql:state(), channels := map()}} | {error, _}.
on_start(InstanceId, Config) ->
    case emqx_mysql:on_start(InstanceId, Config) of
        {ok, ConnectorState} ->
            State = #{
                connector_state => ConnectorState,
                channels => #{}
            },
            {ok, State};
        {error, Reason} ->
            {error, Reason}
    end.

on_stop(InstanceId, _State = #{connector_state := ConnectorState}) ->
    ok = emqx_mysql:on_stop(InstanceId, ConnectorState),
    ?tp(mysql_connector_stopped, #{instance_id => InstanceId}),
    ok.

%%========================================================================================
%% Helper fns
%%========================================================================================
set_prepares(ChannelConfig, ConnectorState) ->
    #{prepares := Prepares} =
        emqx_mysql:init_prepare(maps:merge(ConnectorState, ChannelConfig)),
    ChannelConfig#{prepares => Prepares}.

validate_sql_type(ChannelId, ChannelConfig, #{query_templates := QueryTemplates}) ->
    Batch =
        case emqx_utils_maps:deep_get([resource_opts, batch_size], ChannelConfig) of
            N when N > 1 -> batch;
            _ -> single
        end,
    BatchKey = {ChannelId, batch},
    SingleKey = {ChannelId, prepstmt},
    case {QueryTemplates, Batch} of
        {#{BatchKey := _}, batch} ->
            ok;
        {#{SingleKey := _}, single} ->
            ok;
        {_, batch} ->
            %% try to provide helpful info
            SQL = maps:get(sql, ChannelConfig),
            Type = emqx_utils_sql:get_statement_type(SQL),
            ErrorContext0 = #{
                reason => failed_to_prepare_statement,
                statement_type => Type,
                operation_type => Batch
            },
            ErrorContext = emqx_utils_maps:put_if(
                ErrorContext0,
                hint,
                <<"UPDATE statements are not supported for batch operations">>,
                Type =:= update
            ),
            {error, ErrorContext};
        _ ->
            SQL = maps:get(sql, ChannelConfig),
            Type = emqx_utils_sql:get_statement_type(SQL),
            ErrorContext = #{
                reason => failed_to_prepare_statement,
                statement_type => Type,
                operation_type => Batch
            },
            {error, ErrorContext}
    end.
