%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_tablestore_connector).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-behaviour(emqx_resource).

%% callbacks of behaviour emqx_resource
-export([
    resource_type/0,
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channel_status/3,
    on_get_channels/1,
    on_query/3,
    on_batch_query/3,
    on_get_status/2
]).

-export([
    mk_tablestore_data/3,
    mk_tablestore_batch_data/2
]).

-define(OTS_CLIENT_NAME(ID), list_to_binary("tablestore:" ++ str(ID))).
-define(TKS(STR), {tmpl_tokens, STR}).
-define(LOG_T(LEVEL, LOG), ?SLOG(LEVEL, LOG, #{tag => "TABLE_STORE"})).

%%--------------------------------------------------------------------
%% resource callback

resource_type() -> tablestore.

callback_mode() -> always_sync.

on_add_channel(_InstId, #{channels := Channels} = OldState, ChannelId, Conf) ->
    #{parameters := #{storage_model_type := timeseries} = Params} = Conf,
    Channels1 = maps:get(ChannelId, Channels, #{}),
    NewState = OldState#{
        channels => Channels1#{
            ChannelId => #{
                timestamp => maybe_preproc(maps:get(timestamp, Params, <<"now">>)),
                table_name => maybe_preproc(maps:get(table_name, Params)),
                measurement => maybe_preproc(maps:get(measurement, Params)),
                tags => preproc_tags(maps:get(tags, Params)),
                fields => preproc_fields(maps:get(fields, Params)),
                data_source => maybe_preproc(maps:get(data_source, Params, <<>>)),
                meta_update_model => maps:get(meta_update_model, Params)
            }
        }
    },
    {ok, NewState}.

on_remove_channel(_InstId, #{channels := Channels} = State, ChannelId) ->
    {ok, State#{
        channels => maps:remove(ChannelId, Channels)
    }}.

on_get_channel_status(InstId, _ChannelId, State) ->
    on_get_status(InstId, State).

on_get_channels(InstId) ->
    emqx_bridge_influxdb_connector:on_get_channels(InstId).

on_start(InstId, Config) ->
    BaseOpts = [
        {instance, maps:get(instance_name, Config)},
        {pool, ?OTS_CLIENT_NAME(InstId)},
        {endpoint, maps:get(endpoint, Config)},
        {pool_size, maps:get(pool_size, Config)}
    ],
    SecretOpts = [
        {access_key, maps:get(access_key_id, Config)},
        {access_secret, maps:get(access_key_secret, Config)}
    ],
    ?LOG_T(info, #{msg => ots_start, ots_opts => BaseOpts}),
    {ok, ClientRef} = start_ots_ts_client(InstId, BaseOpts ++ SecretOpts),
    case list_ots_tables(ClientRef) of
        {ok, _} ->
            {ok, #{client_ref => ClientRef, channels => #{}, ots_opts => BaseOpts}};
        {error, Reason} ->
            _ = ots_ts_client:stop(ClientRef),
            {error, Reason}
    end.

on_stop(_InstId, #{client_ref := ClientRef} = State) ->
    ots_ts_client:stop(ClientRef),
    State.

on_get_status(_InstId, #{client_ref := ClientRef}) ->
    case list_ots_tables(ClientRef) of
        {ok, _} -> ?status_connected;
        _ -> ?status_connecting
    end.

on_query(_InstId, {ChannelId, Message}, #{client_ref := ClientRef, channels := Channels}) ->
    case maps:find(ChannelId, Channels) of
        {ok, #{table_name := TableName0, meta_update_model := MetaUpdateMode} = ChannelState} ->
            try
                Row = mk_tablestore_data_row(Message, ChannelState),
                TableName = render_table_name(TableName0, Message),
                mk_tablestore_data(TableName, MetaUpdateMode, [Row])
            of
                Data ->
                    LogMsg = #{msg => ots_query, channel => ChannelId, data => Data},
                    ?LOG_T(debug, LogMsg),
                    case ots_ts_client:put(ClientRef, Data) of
                        {ok, _Res} -> ok;
                        {error, Reason} -> {error, {unrecoverable_error, Reason}}
                    end
            catch
                throw:{bad_ots_data, _} = Reason ->
                    {error, {unrecoverable_error, Reason}}
            end;
        error ->
            {error, {unrecoverable_error, channel_not_found}}
    end.

on_batch_query(_, [{ChannelId, _} | _] = MsgList, #{client_ref := ClientRef, channels := Channels}) ->
    case maps:find(ChannelId, Channels) of
        {ok, ChannelState} ->
            try mk_tablestore_batch_data(MsgList, ChannelState) of
                BatchDataList ->
                    send_batch_data(BatchDataList, ClientRef, ChannelId)
            catch
                throw:{bad_ots_data, _} = Reason ->
                    {error, {unrecoverable_error, Reason}}
            end;
        error ->
            {error, {unrecoverable_error, channel_not_found}}
    end.

send_batch_data(BatchDataList, ClientRef, ChannelId) ->
    LogMsg = #{msg => ots_batch_query, channel => ChannelId, batch_data_list => BatchDataList},
    ?LOG_T(debug, LogMsg),
    Res = [ots_ts_client:put(ClientRef, BatchData) || BatchData <- BatchDataList],
    Filter = fun
        ({error, _}) -> true;
        (_) -> false
    end,
    case lists:filter(Filter, Res) of
        [] -> ok;
        Errors -> {error, {unrecoverable_error, Errors}}
    end.

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------
start_ots_ts_client(_, OtsOpts) ->
    ots_ts_client:start(OtsOpts).

list_ots_tables(ClientRef) ->
    try
        ots_ts_client:list_tables(ClientRef)
    catch
        Err:Reason:ST ->
            {error, #{error => Err, reason => Reason, stacktrace => ST}}
    end.

preproc_tags(Tags) ->
    maps:fold(
        fun(K, V, AccIn) ->
            AccIn#{maybe_preproc(bin(K)) => maybe_preproc(V)}
        end,
        #{},
        Tags
    ).

preproc_fields(Fields) ->
    lists:map(
        fun(#{column := C, value := V} = Row0) ->
            #{
                column => maybe_preproc(C),
                value => maybe_preproc(V),
                isint => maybe_preproc(maps:get(isint, Row0, undefined)),
                isbinary => maybe_preproc(maps:get(isbinary, Row0, undefined))
            }
        end,
        Fields
    ).

render_table_name(TableName, Message) ->
    case render_tmpl(TableName, Message) of
        undefined -> throw({bad_ots_data, no_table_name});
        TName -> TName
    end.

render_tags(Tags, Message) ->
    maps:fold(
        fun(K, V, AccIn) ->
            case {render_tmpl(K, Message), render_tmpl(V, Message)} of
                {Key, Value} when Key =/= undefined, Value =/= undefined ->
                    AccIn#{Key => Value};
                _ ->
                    AccIn
            end
        end,
        #{},
        Tags
    ).

render_fields(Fields, Message) ->
    lists:filtermap(
        fun(#{column := Column, value := Value} = Row) ->
            case {render_tmpl(Column, Message), render_tmpl(Value, Message)} of
                {Col, Val} when Col =/= undefined, Val =/= undefined ->
                    {true, {Col, Val, field_opts([isint, isbinary], Row, Message, #{})}};
                {_Col, _Val} ->
                    false
            end
        end,
        Fields
    ).

field_opts([Key | Keys], Row, Message, Opts) ->
    case render_tmpl(maps:get(Key, Row, undefined), Message) of
        undefined ->
            field_opts(Keys, Row, Message, Opts);
        Val ->
            field_opts(Keys, Row, Message, Opts#{Key => Val})
    end;
field_opts([], _, _, Opts) ->
    Opts.

maybe_preproc(Str) when is_binary(Str) ->
    case string:find(Str, "${") of
        nomatch -> Str;
        _ -> ?TKS(emqx_placeholder:preproc_tmpl(Str))
    end;
maybe_preproc(Any) ->
    Any.

mk_tablestore_batch_data(MsgList, #{table_name := TableName0} = ChannelState) ->
    #{meta_update_model := MetaUpdateMode} = ChannelState,
    GrpRows = lists:foldr(
        fun({_, Message}, Res) ->
            TableName = render_table_name(TableName0, Message),
            Row = mk_tablestore_data_row(Message, ChannelState),
            Res#{TableName => [Row | maps:get(TableName, Res, [])]}
        end,
        #{},
        MsgList
    ),
    [
        mk_tablestore_data(TableName, MetaUpdateMode, Rows)
     || {TableName, Rows} <- maps:to_list(GrpRows)
    ].

mk_tablestore_data(TableName, MetaUpdateMode, Rows) ->
    #{
        table_name => TableName,
        rows_data => Rows,
        meta_update_mode => MetaUpdateMode
    }.

mk_tablestore_data_row(Message, #{measurement := Measurement0} = ChannelState) ->
    Measurement = render_tmpl(Measurement0, Message),
    do_mk_tablestore_data_row(Message, ChannelState, Measurement).

do_mk_tablestore_data_row(_, _, undefined) ->
    throw({bad_ots_data, no_measurement});
do_mk_tablestore_data_row(Message, ChannelState, Measurement) ->
    #{tags := Tags0, fields := Fields0, data_source := DataSource0, timestamp := Ts0} =
        ChannelState,
    DataSource = trans_data_source(render_tmpl(DataSource0, Message)),
    Tags = render_tags(Tags0, Message),
    Fields = render_fields(Fields0, Message),
    Timestamp =
        case render_tmpl(Ts0, Message) of
            <<"now">> -> os:system_time(microsecond);
            <<"NOW">> -> os:system_time(microsecond);
            undefined -> os:system_time(microsecond);
            Ts1 when is_integer(Ts1) -> Ts1;
            Ts2 -> throw({bad_ots_data, {bad_timestamp, Ts2}})
        end,
    #{
        measurement => Measurement,
        data_source => DataSource,
        tags => Tags,
        fields => Fields,
        time => Timestamp
    }.

trans_data_source(undefined) -> <<>>;
trans_data_source(DataSource) -> DataSource.

render_tmpl(?TKS(Tokens), Message) ->
    do_render_tmpl(Tokens, Message);
render_tmpl(Val, _) ->
    Val.

do_render_tmpl(Tokens, Message) ->
    RawResult = emqx_placeholder:proc_tmpl(Tokens, Message, #{return => rawlist}),
    filter_vars(RawResult).

filter_vars([undefined]) ->
    undefined;
filter_vars([RawResult]) ->
    RawResult;
filter_vars(RawResult) when is_list(RawResult) ->
    erlang:iolist_to_binary([str(R) || R <- RawResult]).

str(Data) ->
    emqx_utils_conv:str(Data).

bin(Data) ->
    emqx_utils_conv:bin(Data).
