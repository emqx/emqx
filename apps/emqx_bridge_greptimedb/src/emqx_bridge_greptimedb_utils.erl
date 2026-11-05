%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_greptimedb_utils).

%% API
-export([
    parse_write_syntax/2,
    parse_action_parameters/2,
    parse_batch_data/5,
    parse_single_data/5
]).

-ifdef(TEST).
-export([
    convert_timestamp/2,
    data_to_points/4,
    value_type/1,
    value_type/3,
    value_type_for_kind/2
]).
-endif.

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-include_lib("emqx/include/logger.hrl").

-define(DEFAULT_TIMESTAMP_TMPL, "${timestamp}").

%% Bounds greptimedb accepts for the `i' and `u' typed suffixes.
-define(INT64_MIN, -16#8000000000000000).
-define(INT64_MAX, 16#7FFFFFFFFFFFFFFF).
-define(UINT64_MAX, 16#FFFFFFFFFFFFFFFF).

-type ts_precision() :: ns | us | ms | s.
-type point_config() :: #{
    measurement := term(),
    tags := map(),
    fields := map(),
    timestamp := term(),
    missing_timestamp => default | error | ignore,
    precision := {From :: ts_precision(), To :: ts_precision()}
}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

parse_write_syntax(Lines, Precision) ->
    do_parse_write_syntax(Lines, [], Precision).

parse_action_parameters(#{write := Points}, Precision) ->
    parse_structured_write(Points, Precision);
parse_action_parameters(#{write_syntax := Lines}, Precision) ->
    parse_write_syntax(Lines, Precision).

parse_batch_data(ConnResId, DbName, BatchData, SyntaxLines, DriverType) when
    DriverType == erlang;
    DriverType == native
->
    do_parse_batch_data(ConnResId, DbName, BatchData, SyntaxLines, DriverType).

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

%% One message on its own: the caller wants the error points, not the batch
%% marker, so the failure names the values that could not be transformed.
-spec parse_single_data(binary(), binary(), map(), list(), erlang | native) ->
    {ok, [map()]} | {error, term()}.
parse_single_data(ConnResId, DbName, Data, SyntaxLines, DriverType) ->
    case data_to_points(Data, DbName, SyntaxLines, DriverType) of
        {ok, _Points} = Ok ->
            Ok;
        {error, ErrorPoints} ->
            log_error_points(ConnResId, ErrorPoints),
            {error, {points_trans_failed, #{last_error => last_error([ErrorPoints])}}}
    end.

%% A message whose points do not all transform is reported on its own, and the
%% batch keeps the points it could build: failing the whole batch would drop
%% valid messages because of one bad neighbour.
do_parse_batch_data(ConnResId, DbName, BatchData, SyntaxLines, DriverType) ->
    {Points0, BatchResults0, Errors} = lists:foldl(
        fun({_, Data}, {ListOfPoints, BatchResultsAcc, ErrAccIn}) ->
            case data_to_points(Data, DbName, SyntaxLines, DriverType) of
                {ok, Points} ->
                    {[Points | ListOfPoints], [valid | BatchResultsAcc], ErrAccIn};
                {error, ErrorPoints} ->
                    log_error_points(ConnResId, ErrorPoints),
                    Error = {error, {unrecoverable_error, points_trans_failed}},
                    {ListOfPoints, [Error | BatchResultsAcc], [ErrorPoints | ErrAccIn]}
            end
        end,
        {[], [], []},
        BatchData
    ),
    Points = lists:flatten(lists:reverse(Points0)),
    case {Errors, Points} of
        {[], _} ->
            {ok, Points};
        {_, []} ->
            %% Nothing transformed: report the failure, as this branch always has.
            ?SLOG(error, #{
                msg => "greptimedb_trans_point_failed",
                last_error => last_error(Errors),
                connector => ConnResId,
                reason => points_trans_failed
            }),
            {error, {points_trans_failed, #{last_error => last_error(Errors)}}};
        {_, _} ->
            %% Some messages transformed: send those, and report the rest
            %% alongside them instead of dropping the whole batch.
            ?SLOG(error, #{
                msg => "greptimedb_trans_point_failed",
                last_error => last_error(Errors),
                connector => ConnResId,
                reason => points_trans_failed
            }),
            {ok, Points, lists:reverse(BatchResults0)}
    end.

last_error([[{error, LastError} | _] | _]) -> LastError;
last_error([LastError | _]) -> LastError.

-spec data_to_points(
    map(),
    binary(),
    [point_config()],
    _DriverType :: erlang | native
) -> {ok, [map()]} | {error, term()}.
data_to_points(Data, DbName, SyntaxLines, DriverType) ->
    lines_to_points(Data, DbName, DriverType, SyntaxLines, [], []).

%% Points configured for one message are strongly correlated. If any configured
%% point fails to convert, all points for that message are treated as failed.
lines_to_points(_Data, _DbName, _DriverType, [], Points, ErrorPoints) ->
    case ErrorPoints of
        [] ->
            {ok, lists:reverse(Points)};
        _ ->
            %% ignore trans succeeded points
            {error, ErrorPoints}
    end;
lines_to_points(
    Data,
    DbName,
    DriverType,
    [#{timestamp := Ts, precision := Precision} = Item | Rest],
    ResultPointsAcc,
    ErrorPointsAcc
) when
    is_list(Ts)
->
    MissingTimestamp = maps:get(missing_timestamp, Item, default),
    TransOptions = #{return => rawlist, var_trans => fun data_filter/1},
    case
        parse_timestamp(
            emqx_placeholder:proc_tmpl(Ts, Data, TransOptions),
            MissingTimestamp,
            Precision
        )
    of
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
    continue_lines_to_points(Data, DbName, DriverType, Item, Rest, ResultPointsAcc, ErrorPointsAcc);
lines_to_points(
    Data,
    DbName,
    DriverType,
    [#{timestamp := undefined} = Item | Rest],
    ResultPointsAcc,
    ErrorPointsAcc
) ->
    continue_lines_to_points(Data, DbName, DriverType, Item, Rest, ResultPointsAcc, ErrorPointsAcc).

continue_lines_to_points(Data, DbName, DriverType, Item, Rest, ResultPointsAcc, ErrorPointsAcc) ->
    case line_to_point(Data, DbName, DriverType, Item) of
        {ok, {_, [#{fields := Fields}]}} when map_size(Fields) =:= 0 ->
            %% greptimedb client doesn't like empty field maps...
            ErrorPointsAcc1 = [{error, no_fields} | ErrorPointsAcc],
            lines_to_points(Data, DbName, DriverType, Rest, ResultPointsAcc, ErrorPointsAcc1);
        {ok, Point} ->
            lines_to_points(
                Data, DbName, DriverType, Rest, [Point | ResultPointsAcc], ErrorPointsAcc
            );
        {error, _} = Error ->
            lines_to_points(
                Data, DbName, DriverType, Rest, ResultPointsAcc, [Error | ErrorPointsAcc]
            )
    end.

line_to_point(Data, DbName, DriverType, Item) ->
    try line_to_point_1(Data, DbName, DriverType, Item) of
        Result -> Result
    catch
        throw:Reason ->
            {error, Reason};
        Class:Reason when Class =:= error; Class =:= exit ->
            {error, {point_conversion_failed, Reason}}
    end.

line_to_point_1(
    Data,
    DbName,
    DriverType,
    #{
        measurement := Measurement,
        tags := Tags,
        fields := Fields,
        precision := {_, ToPrecision} = Precision
    } = Item
) ->
    FoldTagFn = fun(K, V, Acc) ->
        maps_config_to_tag_data(K, V, Acc, DriverType)
    end,
    FoldFieldFn = fun(K, V, Acc) ->
        maps_config_to_data(K, V, Acc, DriverType)
    end,
    maybe
        {ok, {_, EncodedTags}} ?= maps:fold(FoldTagFn, {ok, {Data, #{}}}, Tags),
        {ok, {_, EncodedFields}} ?= maps:fold(FoldFieldFn, {ok, {Data, #{}}}, Fields),
        {ok, TimestampFields} ?=
            timestamp_fields(
                maps:get(timestamp, Item, undefined),
                Precision,
                maps:get(missing_timestamp, Item, default)
            ),
        TableName = render_measurement(Measurement, Data),
        Metric = #{dbname => DbName, table => TableName, timeunit => ToPrecision},
        Point = maps:merge(
            maps:without(
                [precision, measurement, missing_timestamp, timestamp],
                Item#{tags => EncodedTags, fields => EncodedFields}
            ),
            TimestampFields
        ),
        {ok, {Metric, [Point]}}
    end.

do_parse_write_syntax([], Acc, _Precision) ->
    lists:reverse(Acc);
do_parse_write_syntax([Item0 | Rest], Acc, Precision) ->
    Ts0 = maps:get(timestamp, Item0, ?DEFAULT_TIMESTAMP_TMPL),
    {Ts, FromPrecision, ToPrecision} = preproc_tmpl_timestamp(Ts0, Precision),
    Item = #{
        measurement => emqx_placeholder:preproc_tmpl(maps:get(measurement, Item0)),
        timestamp => Ts,
        precision => {FromPrecision, ToPrecision},
        tags => to_kv_config(maps:get(tags, Item0), tag),
        fields => to_kv_config(maps:get(fields, Item0), field)
    },
    do_parse_write_syntax(Rest, [Item | Acc], Precision).

parse_structured_point(Point, TargetPrecision) ->
    {Timestamp, SourcePrecision, MissingTimestamp} = structured_timestamp(
        maps:get(timestamp, Point, undefined), TargetPrecision
    ),
    #{
        measurement => {structured, emqx_placeholder:preproc_tmpl(maps:get(measurement, Point))},
        timestamp => Timestamp,
        missing_timestamp => MissingTimestamp,
        precision => {SourcePrecision, TargetPrecision},
        tags => structured_tags(maps:get(tags, Point, [])),
        fields => structured_fields(maps:get(fields, Point))
    }.

parse_structured_write(Points, Precision) ->
    [parse_structured_point(Point, Precision) || Point <- Points].

structured_timestamp(false, _TargetPrecision) ->
    {undefined, ms, ignore};
structured_timestamp(undefined, TargetPrecision) ->
    {Ts, SourcePrecision, _} = preproc_tmpl_timestamp(undefined, TargetPrecision),
    {Ts, SourcePrecision, default};
structured_timestamp(#{value := Value, source_precision := SourcePrecision}, _TargetPrecision) ->
    {emqx_placeholder:preproc_tmpl(Value), SourcePrecision, error}.

structured_tags(Tags) ->
    lists:foldl(
        fun(#{key := Key, value := Value}, Acc) ->
            maps:put(
                emqx_placeholder:preproc_tmpl(Key),
                {structured_tag, emqx_placeholder:preproc_tmpl(Value)},
                Acc
            )
        end,
        #{},
        Tags
    ).

structured_fields(Fields) ->
    lists:foldl(
        fun(#{key := Key, type := Type, value := Value}, Acc) ->
            maps:put(
                emqx_placeholder:preproc_tmpl(Key),
                {typed, Type, emqx_placeholder:preproc_tmpl(Value)},
                Acc
            )
        end,
        #{},
        Fields
    ).

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

to_kv_config(KVfields, Kind) ->
    lists:foldl(
        fun({K, V}, Acc) -> to_maps_config(K, V, Kind, Acc) end,
        #{},
        KVfields
    ).

to_maps_config(K, V, tag, Res) ->
    NK = emqx_placeholder:preproc_tmpl(bin(K)),
    Res#{NK => {legacy_tag, preproc_quoted(V)}};
to_maps_config(K, V, field, Res) ->
    NK = emqx_placeholder:preproc_tmpl(bin(K)),
    NV = preproc_quoted(V),
    Res#{NK => NV}.

maps_config_to_data(_K, _V, {error, _} = Error, _DriverType) ->
    Error;
maps_config_to_data(K, {typed, Type, Tmpl}, {ok, {Data, Res}}, DriverType) ->
    KTransOptions = #{return => rawlist, var_trans => fun key_filter/1},
    VTransOptions = #{return => rawlist, var_trans => fun data_filter/1},
    NK = emqx_placeholder:proc_tmpl(K, Data, KTransOptions),
    NV = emqx_placeholder:proc_tmpl(Tmpl, Data, VTransOptions),
    case has_undefined(NK) orelse has_undefined(NV) of
        true ->
            {ok, {Data, Res}};
        false ->
            case typed_value(DriverType, Type, NV) of
                {ok, Value} ->
                    Key = render_template_text(NK),
                    {ok, {Data, put_structured_kv(field_key, Key, Value, Res)}};
                {error, _} = Error ->
                    Error
            end
    end;
maps_config_to_data(K, V, {ok, {Data, Res}}, DriverType) ->
    KTransOptions = #{return => rawlist, var_trans => fun key_filter/1},
    VTransOptions = #{return => rawlist, var_trans => fun data_filter/1},
    NK0 = emqx_placeholder:proc_tmpl(K, Data, KTransOptions),
    NV = proc_quoted(V, Data, VTransOptions),
    SuffixType = tmpl_suffix_type(V),
    case {NK0, NV} of
        {[undefined], _} ->
            {ok, {Data, Res}};
        %% the value starts with an unset placeholder: [undefined], or [undefined, <<"i">>]
        %% when a literal suffix follows it
        {_, [undefined | _]} ->
            {ok, {Data, Res}};
        %% a quoted value is a single string, so it is skipped when any placeholder is unset
        {_, {quoted, Tokens}} when is_list(Tokens) ->
            case lists:member(undefined, Tokens) of
                true ->
                    {ok, {Data, Res}};
                false ->
                    convert_config_value(NK0, NV, SuffixType, DriverType, field, Data, Res)
            end;
        _ ->
            convert_config_value(NK0, NV, SuffixType, DriverType, field, Data, Res)
    end.

convert_config_value(NK0, NV, SuffixType, DriverType, Kind, Data, Res) ->
    NK = list_to_binary(NK0),
    case value_type(NV, SuffixType, Kind) of
        {ok, Value} ->
            {ok, {Data, Res#{NK => driver_value(DriverType, Value)}}};
        {error, _} = Error ->
            Error
    end.

%% The Erlang driver takes the typed value; the native driver takes the bare term. A float
%% token is returned bare by `value_type/1`, so the typed-map clause does not cover it.
driver_value(erlang, Value) ->
    Value;
driver_value(native, #{value_data := {_Type, Value}}) ->
    Value;
driver_value(native, Float) when is_float(Float) ->
    Float.

maps_config_to_tag_data(_K, _V, {error, _} = Error, _DriverType) ->
    Error;
maps_config_to_tag_data(K, Config, {ok, {Data, Res}}, DriverType) ->
    Tmpl = tag_template(Config),
    KTransOptions = #{return => rawlist, var_trans => fun key_filter/1},
    VTransOptions = #{return => rawlist, var_trans => fun data_filter/1},
    NK0 = emqx_placeholder:proc_tmpl(K, Data, KTransOptions),
    NV = unwrap_quoted_tag(proc_quoted(Tmpl, Data, VTransOptions)),
    SuffixType = tmpl_suffix_type(Tmpl),
    case {NK0, NV} of
        {[undefined], _} ->
            {ok, {Data, Res}};
        {_, [undefined | _]} ->
            {ok, {Data, Res}};
        {_, {quoted, Tokens}} when is_list(Tokens) ->
            case lists:member(undefined, Tokens) of
                true ->
                    {ok, {Data, Res}};
                false ->
                    convert_config_value(NK0, NV, SuffixType, DriverType, tag, Data, Res)
            end;
        _ ->
            convert_config_value(NK0, NV, SuffixType, DriverType, tag, Data, Res)
    end.

%% Earlier releases stored legacy tags without the `legacy_tag' wrapper.
%% Structured tags use their own wrapper. Both forms follow the same tag typing rules.
tag_template({legacy_tag, Tmpl}) ->
    Tmpl;
tag_template({structured_tag, Tmpl}) ->
    Tmpl;
tag_template(Tmpl) ->
    Tmpl.
typed_value(erlang, auto, Values) ->
    value_type(Values);
typed_value(native, auto, Values) ->
    native_auto_value(Values);
typed_value(DriverType, string, Values) ->
    Value = render_template_text(Values),
    case DriverType of
        erlang -> {ok, greptimedb_values:string_value(Value)};
        native -> {ok, Value}
    end;
typed_value(DriverType, integer, [Value]) ->
    maybe
        {ok, Int} ?= typed_integer(Value),
        true ?= Int >= ?INT64_MIN andalso Int =< ?INT64_MAX,
        typed_integer_result(DriverType, Int)
    else
        _ -> {error, {invalid_integer_value, Value}}
    end;
typed_value(DriverType, float, [Value]) ->
    maybe
        {ok, Float} ?= typed_float(Value),
        typed_float_result(DriverType, Float)
    else
        _ -> {error, {invalid_float_value, Value}}
    end;
typed_value(DriverType, boolean, [Value]) ->
    maybe
        {ok, Bool} ?= typed_boolean(Value),
        typed_boolean_result(DriverType, Bool)
    else
        _ -> {error, {invalid_boolean_value, Value}}
    end;
typed_value(_DriverType, Type, Value) ->
    {error, {invalid_field_value, #{type => Type, value => Value}}}.

typed_integer(Value) when is_integer(Value) ->
    {ok, Value};
typed_integer(Value) when is_binary(Value) ->
    try
        {ok, binary_to_integer(Value)}
    catch
        error:badarg -> error
    end;
typed_integer(_Value) ->
    error.

typed_float(Value) when is_float(Value) ->
    {ok, Value};
typed_float(Value) when is_integer(Value) ->
    try
        {ok, float(Value)}
    catch
        error:badarg -> error
    end;
typed_float(Value) when is_binary(Value) ->
    try
        {ok, binary_to_float(Value)}
    catch
        error:badarg ->
            try
                {ok, float(binary_to_integer(Value))}
            catch
                error:badarg ->
                    typed_float_string(Value)
            end
    end;
typed_float(_Value) ->
    error.

typed_float_string(Value) ->
    case
        re:run(
            Value,
            <<"^[+-]?(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+)(?:[eE][+-]?[0-9]+)?$">>,
            [{capture, none}]
        )
    of
        match ->
            try
                {ok, binary_to_float(normalize_float_binary(Value))}
            catch
                error:badarg -> error
            end;
        nomatch ->
            error
    end.

normalize_float_binary(Value0) ->
    Value1 = binary:replace(Value0, <<"E">>, <<"e">>, [global]),
    {Mantissa0, Exponent} =
        case binary:split(Value1, <<"e">>) of
            [Mantissa, Exp] -> {Mantissa, <<"e", Exp/binary>>};
            [Mantissa] -> {Mantissa, <<>>}
        end,
    Mantissa1 = normalize_float_leading_zero(Mantissa0),
    Mantissa2 = normalize_float_fraction(Mantissa1),
    <<Mantissa2/binary, Exponent/binary>>.

normalize_float_leading_zero(<<".", Rest/binary>>) ->
    <<"0.", Rest/binary>>;
normalize_float_leading_zero(<<"-.", Rest/binary>>) ->
    <<"-0.", Rest/binary>>;
normalize_float_leading_zero(<<"+", Rest/binary>>) ->
    normalize_float_leading_zero(Rest);
normalize_float_leading_zero(Value) ->
    Value.

normalize_float_fraction(Value) ->
    Last = binary:last(Value),
    case {Last, binary:match(Value, <<".">>)} of
        {_Dot, nomatch} -> <<Value/binary, ".0">>;
        {$., _} -> <<Value/binary, "0">>;
        _ -> Value
    end.

typed_boolean(true) ->
    {ok, true};
typed_boolean(false) ->
    {ok, false};
typed_boolean(Value) when is_binary(Value) ->
    try
        case string:lowercase(Value) of
            <<"true">> -> {ok, true};
            <<"t">> -> {ok, true};
            <<"false">> -> {ok, false};
            <<"f">> -> {ok, false};
            _ -> error
        end
    catch
        error:_ -> error
    end;
typed_boolean(_Value) ->
    error.

typed_integer_result(erlang, Value) ->
    {ok, greptimedb_values:int64_value(Value)};
typed_integer_result(native, Value) ->
    {ok, Value}.

typed_float_result(erlang, Value) ->
    {ok, greptimedb_values:float64_value(Value)};
typed_float_result(native, Value) ->
    {ok, Value}.

typed_boolean_result(erlang, Value) ->
    {ok, greptimedb_values:boolean_value(Value)};
typed_boolean_result(native, Value) ->
    {ok, Value}.

render_measurement({structured, Tmpl}, Data) ->
    TransOptions = #{return => rawlist, var_trans => fun data_filter/1},
    Parts = emqx_placeholder:proc_tmpl(Tmpl, Data, TransOptions),
    case {has_undefined(Parts), render_template_text(Parts)} of
        {true, _} ->
            throw(missing_measurement);
        {false, <<>>} ->
            throw(empty_measurement);
        {false, Measurement} ->
            Measurement
    end;
render_measurement(Tmpl, Data) ->
    emqx_placeholder:proc_tmpl(Tmpl, Data).

render_template_text(Values) ->
    iolist_to_binary([bin(Value) || Value <- Values]).

has_undefined(Values) ->
    lists:member(undefined, Values).

put_structured_kv(KeyType, Key, Value, Acc) ->
    case {Key, maps:is_key(Key, Acc)} of
        {<<>>, _} ->
            throw({empty_rendered_key, KeyType});
        {_, true} ->
            throw({duplicate_rendered_key, #{type => KeyType, key => Key}});
        {_, false} ->
            Acc#{Key => Value}
    end.

%% greptimedb stores timestamps as int64, so a value that overflows after the
%% unit conversion is rejected rather than written wrapped.
timestamp_fields(undefined, {_FromPrecision, ToPrecision}, ignore) ->
    case convert_timestamp(emqx_message:timestamp_now(), {ms, ToPrecision}) of
        {ok, Timestamp} -> {ok, #{timestamp => Timestamp}};
        {error, _} = Error -> Error
    end;
timestamp_fields(undefined, _Precision, _MissingTimestamp) ->
    {ok, #{}};
timestamp_fields(Ts, Precision, _MissingTimestamp) ->
    case convert_timestamp(Ts, Precision) of
        {ok, Timestamp} -> {ok, #{timestamp => Timestamp}};
        {error, _} = Error -> Error
    end.

convert_timestamp(undefined, _Precision) ->
    {ok, undefined};
convert_timestamp(Ts, Precision) ->
    Timestamp = maybe_convert_time_unit(Ts, Precision),
    case Timestamp >= ?INT64_MIN andalso Timestamp =< ?INT64_MAX of
        true -> {ok, Timestamp};
        false -> {error, {bad_timestamp, Ts}}
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

parse_timestamp([undefined], error, _Precision) ->
    {error, missing_timestamp};
parse_timestamp([undefined], default, {FromPrecision, _}) ->
    {ok,
        maybe_convert_time_unit(
            emqx_message:timestamp_now(),
            {ms, FromPrecision}
        )};
parse_timestamp([TsInt], _MissingTimestamp, _Precision) when is_integer(TsInt) ->
    {ok, TsInt};
parse_timestamp([TsBin], _MissingTimestamp, _Precision) ->
    try
        {ok, binary_to_integer(TsBin)}
    catch
        _:_ ->
            {error, TsBin}
    end;
parse_timestamp(InvalidTs, _MissingTimestamp, _Precision) ->
    {error, {unsupported_placeholder_usage_for_timestamp, InvalidTs}}.

to_raw_value([Int, <<"i">>]) when
    is_integer(Int), Int >= ?INT64_MIN, Int =< ?INT64_MAX
->
    Int;
to_raw_value([Int, <<"i">>]) ->
    throw({invalid_integer_value, Int});
to_raw_value([UInt, <<"u">>]) when
    is_integer(UInt), UInt >= 0, UInt =< ?UINT64_MAX
->
    UInt;
to_raw_value([UInt, <<"u">>]) ->
    throw({invalid_unsigned_integer_value, UInt});
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
to_raw_value([Int]) when is_integer(Int) ->
    float(Int);
to_raw_value(Val0) ->
    case unicode:characters_to_binary(Val0, utf8) of
        Val1 when is_binary(Val1) ->
            Val1;
        _Error ->
            throw({unrecoverable_error, {non_utf8_string_value, Val0}})
    end.

native_auto_value([Int, <<"i">>]) ->
    try to_raw_value([Int, <<"i">>]) of
        Value -> {ok, Value}
    catch
        throw:Reason -> {error, Reason}
    end;
native_auto_value([UInt, <<"u">>]) ->
    try to_raw_value([UInt, <<"u">>]) of
        Value -> {ok, Value}
    catch
        throw:Reason -> {error, Reason}
    end;
native_auto_value(Values) ->
    {ok, to_raw_value(Values)}.
value_type([<<"t">>]) ->
    {ok, greptimedb_values:boolean_value(true)};
value_type([<<"T">>]) ->
    {ok, greptimedb_values:boolean_value(true)};
value_type([true]) ->
    {ok, greptimedb_values:boolean_value(true)};
value_type([<<"TRUE">>]) ->
    {ok, greptimedb_values:boolean_value(true)};
value_type([<<"True">>]) ->
    {ok, greptimedb_values:boolean_value(true)};
value_type([<<"f">>]) ->
    {ok, greptimedb_values:boolean_value(false)};
value_type([<<"F">>]) ->
    {ok, greptimedb_values:boolean_value(false)};
value_type([false]) ->
    {ok, greptimedb_values:boolean_value(false)};
value_type([<<"FALSE">>]) ->
    {ok, greptimedb_values:boolean_value(false)};
value_type([<<"False">>]) ->
    {ok, greptimedb_values:boolean_value(false)};
value_type([Float]) when is_float(Float) ->
    {ok, Float};
value_type([Int]) when is_integer(Int) ->
    try
        {ok, greptimedb_values:float64_value(float(Int))}
    catch
        error:badarg ->
            {error, {invalid_float_value, Int}}
    end;
%% A quoted field value is one string: every token is rendered as text.
value_type({quoted, Tokens}) when is_list(Tokens) ->
    text_value(Tokens);
%% A list value that is not a single typed value is a template: every token is rendered as text.
value_type(Val0) when is_list(Val0) ->
    text_value(Val0);
value_type(Val0) ->
    string_value(Val0).

%% A line protocol type suffix is the literal `i`/`u` token after one value placeholder; the same
%% text rendered by a placeholder is plain text.
tmpl_suffix_type({quoted, _Tokens}) ->
    none;
tmpl_suffix_type([{var, _}, {str, <<"i">>}]) ->
    int64;
tmpl_suffix_type([{var, _}, {str, <<"u">>}]) ->
    uint64;
tmpl_suffix_type(_Tokens) ->
    none.

%% `proc_tmpl/3' renders one list element per template token, so an `int64'/`uint64' template
%% always renders as `[Value, Suffix]`.
value_type([Int, <<"i">>], int64, _Kind) when
    is_integer(Int), Int >= ?INT64_MIN, Int =< ?INT64_MAX
->
    {ok, greptimedb_values:int64_value(Int)};
value_type([Invalid, <<"i">>], int64, _Kind) ->
    {error, {invalid_integer_value, Invalid}};
value_type([UInt, <<"u">>], uint64, _Kind) when
    is_integer(UInt), UInt >= 0, UInt =< ?UINT64_MAX
->
    {ok, greptimedb_values:uint64_value(UInt)};
value_type([Invalid, <<"u">>], uint64, _Kind) ->
    {error, {invalid_unsigned_integer_value, Invalid}};
value_type(NV, none, Kind) ->
    value_type_for_kind(NV, Kind).

%% Line protocol tag values are text: token types are dropped, except a boolean from the payload,
%% which stays typed. A typed `i`/`u` suffix never reaches here; `value_type/3' handles it first.
value_type_for_kind([Bool], tag) when is_boolean(Bool) ->
    value_type([Bool]);
value_type_for_kind(NV, tag) when is_list(NV) ->
    text_value(NV);
value_type_for_kind(NV, _Kind) ->
    value_type(NV).

text_value(Tokens) ->
    string_value([render_token(Token) || Token <- Tokens]).

string_value(Val0) ->
    try unicode:characters_to_binary(Val0, utf8) of
        Val1 when is_binary(Val1) ->
            {ok, greptimedb_values:string_value(Val1)};
        _Error ->
            {error, {invalid_string_value, Val0}}
    catch
        error:badarg ->
            {error, {invalid_string_value, Val0}}
    end.

render_token(Token) when is_boolean(Token) ->
    atom_to_binary(Token, utf8);
render_token(Token) when is_integer(Token) ->
    integer_to_binary(Token);
render_token(Token) when is_float(Token) ->
    float_to_binary(Token, [short]);
render_token(Token) ->
    Token.

bin(Data) -> emqx_utils_conv:bin(Data).

preproc_quoted({quoted, V}) ->
    {quoted, emqx_placeholder:preproc_tmpl(bin(V))};
preproc_quoted(V) ->
    emqx_placeholder:preproc_tmpl(bin(V)).

proc_quoted({quoted, V}, Data, TransOpts) ->
    {quoted, emqx_placeholder:proc_tmpl(V, Data, TransOpts)};
proc_quoted(V, Data, TransOpts) ->
    emqx_placeholder:proc_tmpl(V, Data, TransOpts).

unwrap_quoted_tag({quoted, Values}) ->
    Values;
unwrap_quoted_tag(Values) ->
    Values.

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
