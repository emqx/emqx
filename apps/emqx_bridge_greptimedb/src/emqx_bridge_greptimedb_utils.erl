%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_greptimedb_utils).

%% API
-export([
    parse_write_syntax/2,
    parse_batch_data/5
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-include_lib("emqx/include/logger.hrl").

-define(DEFAULT_TIMESTAMP_TMPL, "${timestamp}").

-type ts_precision() :: ns | us | ms | s.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

parse_write_syntax(Lines, Precision) ->
    do_parse_write_syntax(Lines, [], Precision).

parse_batch_data(ConnResId, DbName, BatchData, SyntaxLines, DriverType) when
    DriverType == erlang;
    DriverType == native
->
    do_parse_batch_data(ConnResId, DbName, BatchData, SyntaxLines, DriverType).

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

do_parse_batch_data(ConnResId, DbName, BatchData, SyntaxLines, DriverType) ->
    {Points0, Errors} = lists:foldl(
        fun({_, Data}, {ListOfPoints, ErrAccIn}) ->
            case data_to_points(Data, DbName, SyntaxLines, DriverType) of
                {ok, Points} ->
                    {[Points | ListOfPoints], ErrAccIn};
                {error, ErrorPoints} ->
                    log_error_points(ConnResId, ErrorPoints),
                    {ListOfPoints, [ErrorPoints | ErrAccIn]}
            end
        end,
        {[], []},
        BatchData
    ),
    case Errors of
        [] ->
            Points = lists:reverse(Points0),
            {ok, lists:flatten(Points)};
        [[{error, LastError} | _] | _] ->
            ?SLOG(error, #{
                msg => "greptimedb_trans_point_failed",
                last_error => LastError,
                connector => ConnResId,
                reason => points_trans_failed
            }),
            {error, {points_trans_failed, #{last_error => LastError}}};
        [LastError | _] ->
            ?SLOG(error, #{
                msg => "greptimedb_trans_point_failed",
                last_error => LastError,
                connector => ConnResId,
                reason => points_trans_failed
            }),
            {error, {points_trans_failed, #{last_error => LastError}}}
    end.

-spec data_to_points(
    map(),
    binary(),
    [
        #{
            fields := [{binary(), binary()}],
            measurement := binary(),
            tags := [{binary(), binary()}],
            timestamp := emqx_placeholder:tmpl_token() | integer(),
            precision := {From :: ts_precision(), To :: ts_precision()}
        }
    ],
    _DriverType :: erlang | native
) -> {ok, [map()]} | {error, term()}.
data_to_points(Data, DbName, SyntaxLines, DriverType) ->
    lines_to_points(Data, DbName, DriverType, SyntaxLines, [], []).

%% When converting multiple rows data into Greptimedb Line Protocol, they are considered to be strongly correlated.
%% And once a row fails to convert, all of them are considered to have failed.
lines_to_points(_Data, _DbName, _DriverType, [], Points, ErrorPoints) ->
    case ErrorPoints of
        [] ->
            {ok, Points};
        _ ->
            %% ignore trans succeeded points
            {error, ErrorPoints}
    end;
lines_to_points(
    Data, DbName, DriverType, [#{timestamp := Ts} = Item | Rest], ResultPointsAcc, ErrorPointsAcc
) when
    is_list(Ts)
->
    TransOptions = #{return => rawlist, var_trans => fun data_filter/1},
    case parse_timestamp(emqx_placeholder:proc_tmpl(Ts, Data, TransOptions)) of
        {ok, TsInt} ->
            Item1 = Item#{timestamp => TsInt},
            continue_lines_to_points(
                Data, DbName, DriverType, Item1, Rest, ResultPointsAcc, ErrorPointsAcc
            );
        {error, BadTs} ->
            lines_to_points(Data, DbName, DriverType, Rest, ResultPointsAcc, [
                {error, {bad_timestamp, BadTs}} | ErrorPointsAcc
            ])
    end;
lines_to_points(
    Data, DbName, DriverType, [#{timestamp := Ts} = Item | Rest], ResultPointsAcc, ErrorPointsAcc
) when
    is_integer(Ts)
->
    continue_lines_to_points(Data, DbName, DriverType, Item, Rest, ResultPointsAcc, ErrorPointsAcc).

continue_lines_to_points(Data, DbName, DriverType, Item, Rest, ResultPointsAcc, ErrorPointsAcc) ->
    case line_to_point(Data, DbName, DriverType, Item) of
        {_, [#{fields := Fields}]} when map_size(Fields) =:= 0 ->
            %% greptimedb client doesn't like empty field maps...
            ErrorPointsAcc1 = [{error, no_fields} | ErrorPointsAcc],
            lines_to_points(Data, DbName, DriverType, Rest, ResultPointsAcc, ErrorPointsAcc1);
        Point ->
            lines_to_points(
                Data, DbName, DriverType, Rest, [Point | ResultPointsAcc], ErrorPointsAcc
            )
    end.

line_to_point(
    Data,
    DbName,
    DriverType,
    #{
        measurement := Measurement,
        tags := Tags,
        fields := Fields,
        timestamp := Ts,
        precision := {_, ToPrecision} = Precision
    } = Item
) ->
    FoldFn = fun(K, V, Acc) ->
        maps_config_to_data(K, V, Acc, DriverType)
    end,
    {_, EncodedTags} = maps:fold(FoldFn, {Data, #{}}, Tags),
    {_, EncodedFields} = maps:fold(FoldFn, {Data, #{}}, Fields),
    TableName = emqx_placeholder:proc_tmpl(Measurement, Data),
    Metric = #{dbname => DbName, table => TableName, timeunit => ToPrecision},
    {Metric, [
        maps:without([precision, measurement], Item#{
            tags => EncodedTags,
            fields => EncodedFields,
            timestamp => maybe_convert_time_unit(Ts, Precision)
        })
    ]}.

do_parse_write_syntax([], Acc, _Precision) ->
    lists:reverse(Acc);
do_parse_write_syntax([Item0 | Rest], Acc, Precision) ->
    Ts0 = maps:get(timestamp, Item0, ?DEFAULT_TIMESTAMP_TMPL),
    {Ts, FromPrecision, ToPrecision} = preproc_tmpl_timestamp(Ts0, Precision),
    Item = #{
        measurement => emqx_placeholder:preproc_tmpl(maps:get(measurement, Item0)),
        timestamp => Ts,
        precision => {FromPrecision, ToPrecision},
        tags => to_kv_config(maps:get(tags, Item0)),
        fields => to_kv_config(maps:get(fields, Item0))
    },
    do_parse_write_syntax(Rest, [Item | Acc], Precision).

%% pre-process the timestamp template
%% returns a tuple of three elements:
%% 1. The timestamp template itself.
%% 2. The source timestamp precision (ms if the template ${timestamp} is used).
%% 3. The target timestamp precision (configured for the client).
preproc_tmpl_timestamp(undefined, Precision) ->
    %% not configured, we default it to the message timestamp
    preproc_tmpl_timestamp(?DEFAULT_TIMESTAMP_TMPL, Precision);
preproc_tmpl_timestamp(Ts, Precision) when is_integer(Ts) ->
    %% a const value is used which is very much unusual, but we have to add a special handling
    {Ts, Precision, Precision};
preproc_tmpl_timestamp(Ts, Precision) when is_list(Ts) ->
    preproc_tmpl_timestamp(iolist_to_binary(Ts), Precision);
preproc_tmpl_timestamp(<<?DEFAULT_TIMESTAMP_TMPL>> = Ts, Precision) ->
    {emqx_placeholder:preproc_tmpl(Ts), ms, Precision};
preproc_tmpl_timestamp(Ts, Precision) when is_binary(Ts) ->
    %% a placehold is in use. e.g. ${payload.my_timestamp}
    %% we can only hope it the value will be of the same precision in the configs
    {emqx_placeholder:preproc_tmpl(Ts), Precision, Precision}.

to_kv_config(KVfields) ->
    lists:foldl(
        fun({K, V}, Acc) -> to_maps_config(K, V, Acc) end,
        #{},
        KVfields
    ).

to_maps_config(K, V, Res) ->
    NK = emqx_placeholder:preproc_tmpl(bin(K)),
    NV = preproc_quoted(V),
    Res#{NK => NV}.

maps_config_to_data(K, V, {Data, Res}, DriverType) ->
    KTransOptions = #{return => rawlist, var_trans => fun key_filter/1},
    VTransOptions = #{return => rawlist, var_trans => fun data_filter/1},
    NK0 = emqx_placeholder:proc_tmpl(K, Data, KTransOptions),
    NV = proc_quoted(V, Data, VTransOptions),
    case {NK0, NV} of
        {[undefined], _} ->
            {Data, Res};
        %% undefined value in normal format [undefined] or int/uint format [undefined, <<"i">>]
        {_, [undefined | _]} ->
            {Data, Res};
        _ ->
            NK = list_to_binary(NK0),
            case DriverType of
                erlang ->
                    {Data, Res#{NK => value_type(NV)}};
                native ->
                    {Data, Res#{NK => to_raw_value(NV)}}
            end
    end.

maybe_convert_time_unit(Ts, {FromPrecision, ToPrecision}) ->
    erlang:convert_time_unit(Ts, time_unit(FromPrecision), time_unit(ToPrecision)).

time_unit(s) -> second;
time_unit(ms) -> millisecond;
time_unit(us) -> microsecond;
time_unit(ns) -> nanosecond.

key_filter(undefined) -> undefined;
key_filter(Value) -> emqx_utils_conv:bin(Value).

data_filter(undefined) -> undefined;
data_filter(Int) when is_integer(Int) -> Int;
data_filter(Number) when is_number(Number) -> Number;
data_filter(Bool) when is_boolean(Bool) -> Bool;
data_filter(Data) -> bin(Data).

parse_timestamp([TsInt]) when is_integer(TsInt) ->
    {ok, TsInt};
parse_timestamp([TsBin]) ->
    try
        {ok, binary_to_integer(TsBin)}
    catch
        _:_ ->
            {error, TsBin}
    end.

to_raw_value([N, Suffix]) when is_number(N), is_binary(Suffix) ->
    N;
to_raw_value([<<"t">>]) ->
    true;
to_raw_value([<<"T">>]) ->
    true;
to_raw_value([true]) ->
    true;
to_raw_value([<<"TRUE">>]) ->
    true;
to_raw_value([<<"True">>]) ->
    true;
to_raw_value([<<"f">>]) ->
    false;
to_raw_value([<<"F">>]) ->
    false;
to_raw_value([false]) ->
    false;
to_raw_value([<<"FALSE">>]) ->
    false;
to_raw_value([<<"False">>]) ->
    false;
to_raw_value([Float]) when is_float(Float) ->
    Float;
to_raw_value(Val0) ->
    case unicode:characters_to_binary(Val0, utf8) of
        Val1 when is_binary(Val1) ->
            Val1;
        _Error ->
            throw({unrecoverable_error, {non_utf8_string_value, Val0}})
    end.

value_type([Int, <<"i">>]) when
    is_integer(Int)
->
    greptimedb_values:int64_value(Int);
value_type([UInt, <<"u">>]) when
    is_integer(UInt)
->
    greptimedb_values:uint64_value(UInt);
value_type([<<"t">>]) ->
    greptimedb_values:boolean_value(true);
value_type([<<"T">>]) ->
    greptimedb_values:boolean_value(true);
value_type([true]) ->
    greptimedb_values:boolean_value(true);
value_type([<<"TRUE">>]) ->
    greptimedb_values:boolean_value(true);
value_type([<<"True">>]) ->
    greptimedb_values:boolean_value(true);
value_type([<<"f">>]) ->
    greptimedb_values:boolean_value(false);
value_type([<<"F">>]) ->
    greptimedb_values:boolean_value(false);
value_type([false]) ->
    greptimedb_values:boolean_value(false);
value_type([<<"FALSE">>]) ->
    greptimedb_values:boolean_value(false);
value_type([<<"False">>]) ->
    greptimedb_values:boolean_value(false);
value_type([Float]) when is_float(Float) ->
    Float;
value_type(Val0) ->
    Val =
        case unicode:characters_to_binary(Val0, utf8) of
            Val1 when is_binary(Val1) ->
                Val1;
            _Error ->
                throw({unrecoverable_error, {non_utf8_string_value, Val0}})
        end,
    greptimedb_values:string_value(Val).

bin(Data) -> emqx_utils_conv:bin(Data).

preproc_quoted({quoted, V}) ->
    {quoted, emqx_placeholder:preproc_tmpl(bin(V))};
preproc_quoted(V) ->
    emqx_placeholder:preproc_tmpl(bin(V)).

proc_quoted({quoted, V}, Data, TransOpts) ->
    {quoted, emqx_placeholder:proc_tmpl(V, Data, TransOpts)};
proc_quoted(V, Data, TransOpts) ->
    emqx_placeholder:proc_tmpl(V, Data, TransOpts).

log_error_points(InstId, Errs) ->
    lists:foreach(
        fun({error, Reason}) ->
            ?SLOG(error, #{
                msg => "greptimedb_trans_point_failed",
                connector => InstId,
                reason => Reason
            })
        end,
        Errs
    ).
