%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_s3tables_logic_tests).

-feature(maybe_expr, enable).

-compile([nowarn_export_all, export_all]).

-include_lib("eunit/include/eunit.hrl").
-include("../src/emqx_bridge_s3tables.hrl").

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

load_iceberg_schema(File) ->
    BaseDir = code:lib_dir(emqx_bridge_s3tables),
    TestDataDir = filename:join([BaseDir, "test", "sample_iceberg_schemas"]),
    Filename = filename:join(TestDataDir, File),
    {ok, Bin} = file:read_file(Filename),
    emqx_utils_json:decode(Bin).

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
    {ok, Output} = emqx_bridge_s3tables_logic:convert_iceberg_schema_to_avro(Input),
    ?assertEqual(
        Expected,
        Output,
        #{
            input => Input,
            intersection_removed => rm_intersection(Expected, Output)
        }
    ),
    %% Must be able to re-parse it from json.
    NamedOutput = Output#{<<"name">> => <<"root">>},
    _ = avro:decode_schema(emqx_utils_json:encode(NamedOutput)),
    ok.

rm_intersection(#{} = A, #{} = B) ->
    Acc0 = maps:without(maps:keys(A), B),
    Acc = maps:map(fun(_K, V) -> {undef, V} end, Acc0),
    maps:fold(
        fun(K, V, AccIn) ->
            case B of
                #{K := V} ->
                    AccIn;
                #{K := OtherV} ->
                    AccIn#{K => rm_intersection(V, OtherV)};
                #{} ->
                    AccIn#{K => {V, undef}}
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
    FieldIndex = emqx_bridge_s3tables_logic:index_fields_by_id(IceSchema),
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

fmt(Fmt, Args) ->
    iolist_to_binary(io_lib:format(Fmt, Args)).

%% Returns table metadata for an empty table (no snapshots, single schema, by default).
mk_loaded_table(Opts) ->
    BaseDir = code:lib_dir(emqx_bridge_s3tables),
    File = maps:get(file, Opts, "empty_table.json"),
    Filename = filename:join([BaseDir, "test", "sample_metadata", File]),
    {ok, Raw} = file:read_file(Filename),
    Metadata0 = emqx_utils_json:decode(Raw),
    Metadata1 =
        case Opts of
            #{schemas := IceSchemas} ->
                emqx_utils_maps:deep_put(
                    [<<"metadata">>, <<"schemas">>],
                    Metadata0,
                    IceSchemas
                );
            #{schema := IceSchema} ->
                emqx_utils_maps:deep_put(
                    [<<"metadata">>, <<"schemas">>],
                    Metadata0,
                    [IceSchema]
                );
            _ ->
                Metadata0
        end,
    case Opts of
        #{partition_spec := undefined} ->
            emqx_utils_maps:deep_put(
                [<<"metadata">>, <<"partition-specs">>],
                Metadata1,
                []
            );
        #{partition_spec := PartitionSpec} ->
            emqx_utils_maps:deep_put(
                [<<"metadata">>, <<"partition-specs">>],
                Metadata1,
                [PartitionSpec]
            );
        _ ->
            Metadata1
    end.

manifest_file_avro_schema() ->
    {ok, ScJSON} = file:read_file(
        filename:join([code:lib_dir(emqx_bridge_s3tables), "priv", "manifest-file.avsc"])
    ),
    avro:decode_schema(ScJSON).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

avro_conversion_test_() ->
    BaseDir = code:lib_dir(emqx_bridge_s3tables),
    TestDataDir = filename:join([BaseDir, "test", "sample_iceberg_schemas"]),
    SampleInputs = load_json_data(TestDataDir, "json"),
    ExpectedOutputs = load_json_data(TestDataDir, "avsc"),
    Cases = lists:zip(SampleInputs, ExpectedOutputs),
    [
        {Name, ?_test(test_conversion(Input, Expected))}
     || {{Name, Input}, {_, Expected}} <- Cases
    ].

avro_conversion_unsupported_type_test() ->
    BadSchema = #{
        <<"schema-id">> => 0,
        <<"type">> => <<"struct">>,
        <<"fields">> => [
            #{
                <<"name">> => <<"bah">>,
                <<"id">> => 1,
                <<"type">> => <<"foobar">>
            }
        ]
    },
    LoadedTable = mk_loaded_table(#{schema => BadSchema}),
    ?assertEqual(
        {error, {unsupported_type, <<"foobar">>}},
        emqx_bridge_s3tables_logic:parse_loaded_table(LoadedTable)
    ),
    ok.

%% Checks that we can traverse iceberg schemas and index them by id.  This just verifies
%% that, once indexed, all values are either a primitive type, or a map with a `type` key
%% (struct, list or map).  All keys are field ids (ints).
index_fields_by_id_test_() ->
    BaseDir = code:lib_dir(emqx_bridge_s3tables),
    TestDataDir = filename:join([BaseDir, "test", "sample_iceberg_schemas"]),
    SampleInputs = load_json_data(TestDataDir, "json"),
    [
        {Name, ?_test(test_index_fields_by_id(Input))}
     || {Name, Input} <- SampleInputs
    ].

partition_keys_to_segments_test_() ->
    MkSchema = fun(Field0) ->
        Default = #{
            <<"id">> => 1,
            <<"name">> => <<"bar">>,
            <<"required">> => false,
            <<"type">> => <<"string">>
        },
        Field = maps:merge(Default, Field0),
        #{
            <<"type">> => <<"struct">>,
            <<"schema-id">> => 0,
            <<"fields">> => [Field]
        }
    end,
    MkPartSpec = fun(PartField0) ->
        Default = #{
            <<"field-id">> => 1001,
            <<"source-id">> => 1,
            <<"name">> => <<"bar-ps">>,
            <<"transform">> => <<"identity">>
        },
        PartField = maps:merge(Default, PartField0),
        #{
            <<"spec-id">> => 0,
            <<"fields">> => [PartField]
        }
    end,
    MkCase = fun(
        #{name := Name, field := Field, spec := Spec, input := Record, expected := Expected}
    ) ->
        {Name,
            ?_test(begin
                Schema = MkSchema(Field),
                LoadedTable = mk_loaded_table(#{schema => Schema, partition_spec => Spec}),
                {ok, #{?partition_spec := #partitioned{fields = PartitionFields} = PartSpec}} =
                    emqx_bridge_s3tables_logic:parse_loaded_table(LoadedTable),
                case emqx_bridge_s3tables_logic:record_to_partition_keys(Record, PartitionFields) of
                    {ok, PKs} ->
                        Segments = emqx_bridge_s3tables_logic:partition_keys_to_segments(
                            PKs, PartSpec
                        ),
                        ?assertEqual(Expected, Segments);
                    {error, Reason} ->
                        ?assertEqual(Expected, {error, Reason})
                end
            end)}
    end,
    IdentityCases = [
        #{
            name => "transform = identity; input = string",
            field => #{<<"type">> => <<"string">>},
            spec => MkPartSpec(#{<<"name">> => <<"bar ps/to be & escaped?">>}),
            input => #{<<"bar">> => <<"human/readable with spaces & slashes?">>},
            expected => [
                <<"bar+ps%2Fto+be+%26+escaped%3F=human%2Freadable+with+spaces+%26+slashes%3F">>
            ]
        },
        #{
            name => "transform = identity; input = binary (non-printable)",
            field => #{<<"type">> => <<"binary">>},
            spec => MkPartSpec(#{}),
            input => #{<<"bar">> => <<1, 2, 3, 4>>},
            expected => [<<"bar-ps=AQIDBA%3D%3D">>]
        },
        #{
            name => "transform = identity; input = float",
            field => #{<<"type">> => <<"double">>},
            spec => MkPartSpec(#{}),
            input => #{<<"bar">> => 123.456},
            expected => [<<"bar-ps=123.45600000000000306954">>]
        },
        #{
            name => "transform = identity; input = int",
            field => #{<<"type">> => <<"long">>},
            spec => MkPartSpec(#{}),
            input => #{<<"bar">> => 1234567890},
            expected => [<<"bar-ps=1234567890">>]
        },
        #{
            name => "transform = identity; input = boolean true",
            field => #{<<"type">> => <<"boolean">>},
            spec => MkPartSpec(#{}),
            input => #{<<"bar">> => true},
            expected => [<<"bar-ps=true">>]
        },
        #{
            name => "transform = identity; input = boolean false",
            field => #{<<"type">> => <<"boolean">>},
            spec => MkPartSpec(#{}),
            input => #{<<"bar">> => false},
            expected => [<<"bar-ps=false">>]
        },
        #{
            name => "transform = identity; input = null",
            field => #{<<"type">> => <<"timestamptz_ns">>},
            spec => MkPartSpec(#{}),
            input => #{<<"bar">> => null},
            expected => [<<"bar-ps=null">>]
        },
        #{
            name => "transform = void; input = whatever",
            field => #{<<"type">> => <<"int">>},
            spec => #{
                <<"spec-id">> => 0,
                <<"fields">> => [
                    #{
                        <<"field-id">> => 1001,
                        <<"source-id">> => 1,
                        <<"name">> => <<"bar-ps">>,
                        <<"transform">> => <<"void">>
                    }
                ]
            },
            input => #{<<"bar">> => 12345},
            expected => [<<"bar-ps=null">>]
        },
        #{
            name => "transform = bucket[4]; input = binary",
            field => #{<<"type">> => <<"binary">>},
            spec => #{
                <<"spec-id">> => 0,
                <<"fields">> => [
                    #{
                        <<"field-id">> => 1001,
                        <<"source-id">> => 1,
                        <<"name">> => <<"bar-ps">>,
                        <<"transform">> => <<"bucket[4]">>
                    }
                ]
            },
            input => #{
                <<"bar">> =>
                    <<16, 30, 176, 81, 184, 107, 66, 100, 174, 241, 76, 63, 1, 147, 64, 134>>
            },
            expected => [<<"bar-ps=2">>]
        },
        #{
            name => "spec field order is preserved 1",
            field => #{<<"type">> => <<"string">>},
            spec => #{
                <<"spec-id">> => 0,
                <<"fields">> => [
                    #{
                        <<"field-id">> => 1001,
                        <<"source-id">> => 1,
                        <<"name">> => <<"z">>,
                        <<"transform">> => <<"identity">>
                    },
                    #{
                        <<"field-id">> => 1002,
                        <<"source-id">> => 1,
                        <<"name">> => <<"a">>,
                        <<"transform">> => <<"void">>
                    }
                ]
            },
            input => #{<<"bar">> => <<"hello">>},
            expected => [<<"z=hello">>, <<"a=null">>]
        },
        #{
            name => "spec field order is preserved 1",
            field => #{<<"type">> => <<"string">>},
            spec => #{
                <<"spec-id">> => 0,
                <<"fields">> => [
                    #{
                        <<"field-id">> => 1002,
                        <<"source-id">> => 1,
                        <<"name">> => <<"a">>,
                        <<"transform">> => <<"void">>
                    },
                    #{
                        <<"field-id">> => 1001,
                        <<"source-id">> => 1,
                        <<"name">> => <<"z">>,
                        <<"transform">> => <<"identity">>
                    }
                ]
            },
            input => #{<<"bar">> => <<"hello">>},
            expected => [<<"a=null">>, <<"z=hello">>]
        },
        #{
            name => "incompatible value",
            field => #{<<"type">> => <<"boolean">>},
            spec => #{
                <<"spec-id">> => 0,
                <<"fields">> => [
                    #{
                        <<"field-id">> => 1001,
                        <<"source-id">> => 1,
                        <<"name">> => <<"bar-ps">>,
                        <<"transform">> => <<"bucket[4]">>
                    }
                ]
            },
            input => #{<<"bar">> => true},
            expected => {error, {incompatible_value, <<"bar-ps">>, true}}
        }
    ],
    MkUnsupportedCase = fun(
        #{name := Name, field := Field, spec := Spec, expected := Expected}
    ) ->
        {Name,
            ?_test(begin
                Schema = MkSchema(Field),
                LoadedTable = mk_loaded_table(#{schema => Schema, partition_spec => Spec}),
                ?assertEqual(Expected, emqx_bridge_s3tables_logic:parse_loaded_table(LoadedTable))
            end)}
    end,
    UnsupportedTransformCases = [
        #{
            name => "year",
            field => #{<<"type">> => <<"string">>},
            spec => MkPartSpec(#{<<"transform">> => <<"year">>}),
            note => <<"This is not implemented yet">>,
            input => #{<<"bar">> => 55},
            would_be_expected => [<<"bar+ps=2025">>],
            expected => {error, {unsupported_transform, <<"year">>}}
        },
        #{
            name => "month",
            field => #{<<"type">> => <<"string">>},
            spec => MkPartSpec(#{<<"transform">> => <<"month">>}),
            note => <<"This is not implemented yet">>,
            input => #{<<"bar">> => 664},
            would_be_expected => [<<"bar+ps=2025-05">>],
            expected => {error, {unsupported_transform, <<"month">>}}
        },
        #{
            name => "day",
            field => #{<<"type">> => <<"string">>},
            spec => MkPartSpec(#{<<"transform">> => <<"day">>}),
            note => <<"This is not implemented yet">>,
            input => #{<<"bar">> => 20_202},
            would_be_expected => [<<"bar+ps=2025-04-24">>],
            expected => {error, {unsupported_transform, <<"day">>}}
        },
        #{
            name => "hour",
            field => #{<<"type">> => <<"string">>},
            spec => MkPartSpec(#{<<"transform">> => <<"hour">>}),
            note => <<"This is not implemented yet">>,
            input => #{<<"bar">> => 490_000},
            would_be_expected => [<<"bar+ps=2025-11-24-16">>],
            expected => {error, {unsupported_transform, <<"hour">>}}
        },
        #{
            name => "truncate[10]",
            field => #{<<"type">> => <<"int">>},
            spec => MkPartSpec(#{<<"transform">> => <<"truncate[10]">>}),
            note => <<"This is not implemented yet">>,
            input => #{<<"bar">> => -1},
            would_be_expected => [<<"bar+ps=-10">>],
            expected => {error, {unsupported_transform, <<"truncate[10]">>}}
        }
    ],
    lists:flatten([
        lists:map(MkCase, IdentityCases),
        lists:map(MkUnsupportedCase, UnsupportedTransformCases)
    ]).

%% Checks the behavior of the possible partition transform functions.
transform_fns_test_() ->
    AllTransforms =
        lists:map(
            fun(TransformName) ->
                {ok, TransformFn} = emqx_bridge_s3tables_logic:mk_transform_fn(TransformName),
                {TransformName, TransformFn}
            end,
            [
                <<"identity">>,
                <<"void">>,
                <<"bucket[3]">>,
                <<"bucket[5]">>
            ]
        ),
    {_, [
        Identity,
        Void,
        Bucket3,
        Bucket5
    ]} = lists:unzip(AllTransforms),
    CommonValues = [
        {"int", 123},
        {"float", 123.456},
        {"binary str", <<"abcd">>},
        {"binary", <<0, 1, 2>>},
        {"boolean true", true},
        {"boolean false", false},
        {"fixed[5]", <<1, 2, 3, 4, 5>>},
        {"uuid", <<16, 30, 176, 81, 184, 107, 66, 100, 174, 241, 76, 63, 1, 147, 64, 134>>}
    ],
    NullInputCases = [
        {fmt("null input (~s)", [Name]), ?_assertEqual(null, Fn(null))}
     || {Name, Fn} <- AllTransforms
    ],
    IdentityCases = [
        {fmt("identity (~s)", [N]), ?_assertEqual(V, Identity(V))}
     || {N, V} <- CommonValues
    ],
    VoidCases = [
        {fmt("void (~s)", [N]), ?_assertEqual(null, Void(V))}
     || {N, V} <- CommonValues
    ],
    AssertBetween = fun(Fn, V, Min, Max) ->
        Res = Fn(V),
        ?assert(
            Min =< Res andalso Res =< Max,
            #{
                result => Res,
                max => Max,
                min => Min
            }
        )
    end,
    IsValidBucketValue = fun(Name) ->
        case Name of
            "boolean" ++ _ -> false;
            "float" ++ _ -> false;
            _ -> true
        end
    end,
    Bucket3Cases = [
        {fmt("bucket[3] (~s)", [N]), ?_test(AssertBetween(Bucket3, V, 0, 2))}
     || {N, V} <- CommonValues,
        IsValidBucketValue(N)
    ],
    Bucket5Cases = [
        {fmt("bucket[5] (~s)", [N]), ?_test(AssertBetween(Bucket5, V, 0, 4))}
     || {N, V} <- CommonValues,
        IsValidBucketValue(N)
    ],
    lists:concat([
        NullInputCases,
        IdentityCases,
        VoidCases,
        Bucket3Cases,
        Bucket5Cases
    ]).

find_current_schema_test_() ->
    LoadedTable = mk_loaded_table(#{}),
    SchemaId = 0,
    UnknownSchemaId = -999,
    Schema0 = #{
        <<"type">> => <<"struct">>,
        <<"schema-id">> => 0,
        <<"fields">> => [#{<<"type">> => <<"string">>, <<"id">> => 0}]
    },
    LoadedTableMultipleSchemas = mk_loaded_table(#{
        schemas => [
            Schema0#{<<"schema-id">> := 1},
            Schema0#{<<"schema-id">> := 2},
            Schema0,
            Schema0#{<<"schema-id">> := 3}
        ]
    }),
    %% Missing `spec-id` field.
    SchemaBroken = #{
        <<"type">> => <<"struct">>,
        <<"fields">> => [#{<<"type">> => <<"string">>, <<"id">> => 0}]
    },
    LoadedTableBrokenSchema = mk_loaded_table(#{
        schemas => [
            Schema0#{<<"schema-id">> := 1},
            Schema0#{<<"schema-id">> := 2},
            SchemaBroken,
            Schema0#{<<"schema-id">> := 3}
        ]
    }),
    [
        ?_assertMatch(
            {ok, #{<<"schema-id">> := SchemaId}},
            emqx_bridge_s3tables_logic:find_current_schema(LoadedTable, SchemaId)
        ),
        ?_assertMatch(
            {ok, #{<<"schema-id">> := SchemaId}},
            emqx_bridge_s3tables_logic:find_current_schema(LoadedTableMultipleSchemas, SchemaId)
        ),
        ?_assertMatch(
            {error, schema_not_found},
            emqx_bridge_s3tables_logic:find_current_schema(LoadedTableBrokenSchema, UnknownSchemaId)
        ),
        ?_assertMatch(
            {error, schema_not_found},
            emqx_bridge_s3tables_logic:find_current_schema(LoadedTable, UnknownSchemaId)
        )
    ].

find_partition_spec_test_() ->
    LoadedTable = mk_loaded_table(#{}),
    {ok, #{?iceberg_schema := IceSchema}} =
        emqx_bridge_s3tables_logic:parse_loaded_table(LoadedTable),
    SpecId = 0,
    UnknownSpecId = -999,
    LoadedTableBogusSpecId = emqx_utils_maps:deep_put(
        [<<"metadata">>, <<"default-spec-id">>],
        LoadedTable,
        UnknownSpecId
    ),
    PartitionSpecNoFields = #{<<"spec-id">> => 0, <<"fields">> => []},
    LoadedTableNotPartitioned = mk_loaded_table(#{partition_spec => PartitionSpecNoFields}),
    %% No `fields` field.
    PartitionSpecBroken = #{<<"spec-id">> => 0},
    LoadedTableBrokenSpec = mk_loaded_table(#{partition_spec => PartitionSpecBroken}),
    [
        ?_assertMatch(
            {ok, {SpecId, #unpartitioned{}}},
            emqx_bridge_s3tables_logic:find_partition_spec(LoadedTableNotPartitioned, IceSchema)
        ),
        ?_assertMatch(
            {ok, {SpecId, #partitioned{fields = [#{}]}}},
            emqx_bridge_s3tables_logic:find_partition_spec(LoadedTable, IceSchema)
        ),
        ?_assertMatch(
            {error, partition_spec_not_found},
            emqx_bridge_s3tables_logic:find_partition_spec(LoadedTableBogusSpecId, IceSchema)
        ),
        ?_assertMatch(
            {error, invalid_spec},
            emqx_bridge_s3tables_logic:find_partition_spec(LoadedTableBrokenSpec, IceSchema)
        )
    ].

find_current_snapshot_test_() ->
    LoadedTable = mk_loaded_table(#{file => "two_commits.json"}),
    SnapshotId = 1431434461335186214,
    PreviousSnapshotId = 665534022530477666,
    Default = {error, not_found},
    UnknownSnapshotId = -999,
    LoadedTableUnknownSnapId = emqx_utils_maps:deep_put(
        [<<"metadata">>, <<"current-snapshot-id">>],
        LoadedTable,
        UnknownSnapshotId
    ),
    LoadedTablePrevId = emqx_utils_maps:deep_put(
        [<<"metadata">>, <<"current-snapshot-id">>],
        LoadedTable,
        PreviousSnapshotId
    ),
    LoadedTableEmpty = mk_loaded_table(#{file => "empty_table.json"}),
    LoadedTableEmptyNull = emqx_utils_maps:deep_put(
        [<<"metadata">>, <<"current-snapshot-id">>],
        LoadedTable,
        null
    ),
    [
        ?_assertMatch(
            #{<<"snapshot-id">> := SnapshotId},
            emqx_bridge_s3tables_logic:find_current_snapshot(LoadedTable, Default)
        ),
        ?_assertMatch(
            #{<<"snapshot-id">> := PreviousSnapshotId},
            emqx_bridge_s3tables_logic:find_current_snapshot(LoadedTablePrevId, Default)
        ),
        ?_assertMatch(
            Default,
            emqx_bridge_s3tables_logic:find_current_snapshot(LoadedTableEmpty, Default)
        ),
        ?_assertMatch(
            Default,
            emqx_bridge_s3tables_logic:find_current_snapshot(LoadedTableEmptyNull, Default)
        ),
        ?_assertMatch(
            Default,
            emqx_bridge_s3tables_logic:find_current_snapshot(LoadedTableUnknownSnapId, Default)
        )
    ].

parse_partition_field_bad_fields_test_() ->
    InnermostSourceId = 3,
    NestedStructsSchema = load_iceberg_schema("nested_structs.json"),
    NestedSchema = load_iceberg_schema("nested_with_struct_key_map.json"),
    MkSpec = fun(SourceId) ->
        #{
            <<"spec-id">> => 0,
            <<"fields">> => [
                #{
                    <<"field-id">> => 1001,
                    <<"source-id">> => SourceId,
                    <<"name">> => <<"a">>,
                    <<"transform">> => <<"void">>
                }
            ]
        }
    end,
    MkLT = fun(Sc, SourceId) ->
        LT = mk_loaded_table(#{schema => Sc, partition_spec => MkSpec(SourceId)}),
        emqx_utils_maps:deep_put(
            [<<"metadata">>, <<"current-schema-id">>],
            LT,
            1
        )
    end,
    %% Cannot be partition keys because they are nested inside lists or maps.
    CannotBePKCases =
        [
            #{
                source_id => 5,
                note => "element-id of qux"
            },
            #{
                source_id => 26,
                note => "id of x struct field inside points, a list"
            },
            #{
                source_id => 18,
                note => "key-id of location"
            },
            #{
                source_id => 18,
                note => "value-id of location"
            },
            #{
                source_id => 21,
                note => "id of address struct field inside location, a map"
            },
            #{
                source_id => 9,
                note => "key-id of the value inside quux"
            },
            #{
                source_id => 10,
                note => "value-id of the value inside quux"
            }
        ],
    lists:flatten([
        {"nested structs may be PKs",
            ?_assertMatch(
                {ok, #{
                    ?partition_spec := #partitioned{
                        fields = [
                            #{
                                ?id := 1001,
                                ?name := <<"a">>,
                                ?raw := #{},
                                ?result_type := #{},
                                ?source_type := #{},
                                ?get_fn := _,
                                ?transform_fn := _
                            }
                        ]
                    }
                }},
                emqx_bridge_s3tables_logic:parse_loaded_table(
                    MkLT(NestedStructsSchema, InnermostSourceId)
                )
            )},
        {"missing source-id in schema",
            ?_assertMatch(
                {error, #{
                    reason := <<"could not find field-id in schema">>,
                    msg := <<"bad_partition_spec">>,
                    source_field_id := 999,
                    partition_field_name := <<"a">>
                }},
                emqx_bridge_s3tables_logic:parse_loaded_table(
                    MkLT(NestedSchema, _BadSourceId = 999)
                )
            )},
        lists:map(
            fun(#{note := Note, source_id := SourceId}) ->
                {Note,
                    ?_assertMatch(
                        {error, #{
                            reason := <<"partition spec uses invalid field-id">>,
                            msg := <<"bad_partition_spec">>,
                            source_field_id := SourceId,
                            partition_field_name := <<"a">>
                        }},
                        emqx_bridge_s3tables_logic:parse_loaded_table(
                            MkLT(NestedSchema, SourceId)
                        )
                    )}
            end,
            CannotBePKCases
        )
    ]).

%% Checks that we transform bad manifest file schemas from Athena.
prepare_previous_manifest_files_test() ->
    BaseDir = code:lib_dir(emqx_bridge_s3tables),
    Dir = filename:join([BaseDir, "test", "sample_metadata"]),
    {ok, BadBin} = file:read_file(filename:join([Dir, "bad_athena_manifest_list.avro"])),
    ManifestFileSc = manifest_file_avro_schema(),
    %% Simply not throwing an error asserts the schema has been fixed.  Without fixing the
    %% original schema, this would break as required fields are missing.
    FixedBins = emqx_bridge_s3tables_logic:prepare_previous_manifest_files(
        BadBin, ManifestFileSc
    ),
    ?assertMatch([_, _], FixedBins),
    Decode = fun(Obj) ->
        Opts = avro:make_decoder_options([{map_type, map}, {record_type, map}]),
        avro_binary_decoder:decode(Obj, <<"manifest_file">>, ManifestFileSc, Opts)
    end,
    %% Correct fields are populated
    ?assertMatch(
        [
            #{
                <<"added_files_count">> := 0,
                <<"deleted_files_count">> := 1,
                <<"existing_files_count">> := 0
            },
            #{
                <<"added_files_count">> := 0,
                <<"deleted_files_count">> := 1,
                <<"existing_files_count">> := 0
            }
        ],
        lists:map(Decode, FixedBins)
    ),
    ok.

parse_base_endpoint_test_() ->
    Parse = fun emqx_bridge_s3tables_connector_schema:parse_base_endpoint/1,
    [
        {"only hostname (defaults to https)",
            ?_assertMatch(
                {ok, #{
                    host := "s3tables.sa-east-1.amazonaws.com",
                    base_uri := "https://s3tables.sa-east-1.amazonaws.com",
                    base_path := []
                }},
                Parse(<<"s3tables.sa-east-1.amazonaws.com">>)
            )},
        {"only hostname and path",
            ?_assertMatch(
                {ok, #{
                    host := "s3tables.sa-east-1.amazonaws.com",
                    base_uri := "https://s3tables.sa-east-1.amazonaws.com",
                    base_path := ["iceberg", "v1"]
                }},
                Parse(<<"s3tables.sa-east-1.amazonaws.com/iceberg/v1">>)
            )},
        {"only hostname and scheme",
            ?_assertMatch(
                {ok, #{
                    host := "s3tables.sa-east-1.amazonaws.com",
                    base_uri := "http://s3tables.sa-east-1.amazonaws.com",
                    base_path := []
                }},
                Parse(<<"http://s3tables.sa-east-1.amazonaws.com">>)
            )},
        {"hostname, port and scheme",
            ?_assertMatch(
                {ok, #{
                    host := "s3tables.sa-east-1.amazonaws.com",
                    base_uri := "http://s3tables.sa-east-1.amazonaws.com:9000",
                    base_path := []
                }},
                Parse(<<"http://s3tables.sa-east-1.amazonaws.com:9000">>)
            )},
        {"complete",
            ?_assertMatch(
                {ok, #{
                    host := "s3tables.sa-east-1.amazonaws.com",
                    base_uri := "http://s3tables.sa-east-1.amazonaws.com:9000",
                    base_path := ["iceberg", "v1"]
                }},
                Parse(<<"http://s3tables.sa-east-1.amazonaws.com:9000/iceberg/v1">>)
            )},
        {"bad scheme",
            ?_assertMatch(
                {error, {bad_scheme, <<"pulsar">>}},
                Parse(<<"pulsar://s3tables.sa-east-1.amazonaws.com/iceberg/v1">>)
            )},
        {"bad endpoint",
            ?_assertMatch(
                {error, {bad_endpoint, <<"htt:/aaa\naaa">>}},
                Parse(<<"htt:/aaa\naaa">>)
            )}
    ].
