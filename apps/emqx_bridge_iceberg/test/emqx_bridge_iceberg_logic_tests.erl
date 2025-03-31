%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_iceberg_logic_tests).

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
    Output = emqx_bridge_iceberg_logic:convert_iceberg_schema_to_avro(Input),
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
