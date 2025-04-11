%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_iceberg_logic_tests).

-feature(maybe_expr, enable).

-include_lib("eunit/include/eunit.hrl").

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

load_json_data(Dir, Ext) ->
    Pattern = "*" ++ Ext,
    Files = filelib:wildcard(filename:join(Dir, Pattern)),
    lists:map(
        fun(Filename) ->
            {ok, Bin} = file:read_file(filename:join(Dir, Filename)),
            {filename:basename(Filename, Ext), emqx_utils_json:decode(Bin)}
        end,
        Files
    ).

test_conversion(Input, Expected) ->
    {ok, Output} = emqx_bridge_iceberg_logic:convert_iceberg_schema_to_avro(Input),
    ?assertEqual(
        Expected,
        Output,
        #{
            input => Input,
            intersection_removed => rm_intersection(Expected, Output)
        }
    ).

rm_intersection(#{} = A, #{} = B) ->
    Acc0 = maps:without(maps:keys(A), B),
    Acc = maps:map(fun(_K, V) -> {undef, V} end, Acc0),
    maps:fold(
        fun(K, V, AccIn) ->
            case B of
                #{K := V} ->
                    AccIn;
                #{K := OtherV} ->
                    AccIn#{K => rm_intersection(V, OtherV)}
            end
        end,
        Acc,
        A
    );
rm_intersection(As, Bs) when is_list(As), is_list(Bs) ->
    lists:map(
        fun({A, B}) ->
            case rm_intersection(A, B) of
                Res when map_size(Res) == 0 ->
                    '=';
                Res when length(Res) == 0 ->
                    '=';
                Res ->
                    Res
            end
        end,
        lists:zip(As, Bs)
    );
rm_intersection(A, A) ->
    '=';
rm_intersection(A, B) ->
    {A, B}.

test_index_fields_by_id(IceSchema) ->
    FieldIndex = emqx_bridge_iceberg_logic:index_fields_by_id(IceSchema),
    %% ?debugFmt("index:\n  ~p\n", [FieldIndex]),
    Errors = maps:filter(
        fun
            (K, _V) when not is_integer(K) ->
                %% All keys should be ints
                true;
            (_K, #{type := IceType, can_be_pk := CanBePK, path := Path}) ->
                %% Fields that may be partition keys cannot live inside maps nor
                %% lists.  These have atoms (`'$N'`, `'$K'`, `'$V') in their paths.
                maybe
                    true ?= is_binary(IceType) orelse is_map(IceType),
                    true ?= CanBePK == true orelse CanBePK == false,
                    true ?= is_list(Path),
                    true ?= CanBePK == lists:all(fun is_binary/1, Path),
                    false
                else
                    _ -> true
                end;
            (_K, _V) ->
                %% Weird type
                true
        end,
        FieldIndex
    ),
    case map_size(Errors) > 0 of
        true ->
            error({bad_index, Errors});
        false ->
            ok
    end.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

avro_conversion_test_() ->
    BaseDir = code:lib_dir(emqx_bridge_iceberg),
    TestDataDir = filename:join([BaseDir, "test", "sample_iceberg_schemas"]),
    SampleInputs = load_json_data(TestDataDir, "json"),
    ExpectedOutputs = load_json_data(TestDataDir, "avsc"),
    Cases = lists:zip(SampleInputs, ExpectedOutputs),
    [
        {Name, ?_test(test_conversion(Input, Expected))}
     || {{Name, Input}, {_, Expected}} <- Cases
    ].

%% Checks that we can traverse iceberg schemas and index them by id.  This just verifies
%% that, once indexed, all values are either a primitive type, or a map with a `type` key
%% (struct, list or map).  All keys are field ids (ints).
index_fields_by_id_test_() ->
    BaseDir = code:lib_dir(emqx_bridge_iceberg),
    TestDataDir = filename:join([BaseDir, "test", "sample_iceberg_schemas"]),
    SampleInputs = load_json_data(TestDataDir, "json"),
    [
        {Name, ?_test(test_index_fields_by_id(Input))}
     || {Name, Input} <- SampleInputs
    ].
