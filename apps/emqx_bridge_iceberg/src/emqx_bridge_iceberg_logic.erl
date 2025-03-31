%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_iceberg_logic).

-feature(maybe_expr, enable).

%% API
-export([
    convert_iceberg_schema_to_avro/1,
    compute_update_table_request/1,
    find_current_schema/1,
    find_current_schema/2,
    find_current_snapshot/2
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(REQUIRED_BYTES_PT_KEY, {?MODULE, required_bytes}).
-define(REF_NAME, <<"main">>).

-type loaded_table() :: map().

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec find_current_schema(loaded_table()) -> {ok, map()} | {error, not_found}.
find_current_schema(LoadedTable) ->
    #{<<"metadata">> := #{<<"current-schema-id">> := SchemaId}} = LoadedTable,
    find_current_schema(LoadedTable, SchemaId).

-spec find_current_schema(loaded_table(), integer()) -> {ok, map()} | {error, not_found}.
find_current_schema(LoadedTable, SchemaId) ->
    #{<<"metadata">> := #{<<"schemas">> := IceSchemas}} = LoadedTable,
    find(
        IceSchemas,
        {error, not_found},
        fun(IceSchema) ->
            case IceSchema of
                #{<<"schema-id">> := SchemaId} ->
                    {halt, {ok, IceSchema}};
                _ ->
                    cont
            end
        end
    ).

-spec find_current_snapshot(loaded_table(), Default) -> map() | Default when
    Default :: term().
find_current_snapshot(LoadedTable, IfMissing) ->
    #{<<"metadata">> := #{<<"snapshots">> := Snapshots}} = LoadedTable,
    CurrentSnapshotId = current_snapshot_id(LoadedTable),
    find(
        Snapshots,
        IfMissing,
        fun(Snapshot) ->
            case Snapshot of
                #{<<"snapshot-id">> := CurrentSnapshotId} ->
                    {halt, Snapshot};
                _ ->
                    cont
            end
        end
    ).

convert_iceberg_schema_to_avro(#{<<"type">> := <<"struct">>} = IceSc) ->
    iceberg_struct_to_avro(IceSc).

compute_update_table_request(CommitContext) ->
    #{
        loaded_table := LoadedTable,
        manifest_file_path := ManifestFileS3Path,
        namespace := NamespaceList,
        new_snapshot_id := NewSnapshotId,
        now_ms := NowMS,
        schema_id := SchemaId,
        seq_num := SeqNum,
        table := Table,
        table_uuid := TableUUID
    } = CommitContext,
    ParentSnapshotId = current_snapshot_id(LoadedTable),
    Summary = mk_summary(CommitContext),
    Snapshot0 = #{
        <<"snapshot-id">> => NewSnapshotId,
        <<"sequence-number">> => SeqNum,
        <<"timestamp-ms">> => NowMS,
        <<"manifest-list">> => ManifestFileS3Path,
        <<"summary">> => Summary,
        <<"schema-id">> => SchemaId
    },
    HasParentSnapshot = ParentSnapshotId /= undefined,
    Snapshot = emqx_utils_maps:put_if(
        Snapshot0,
        <<"parent-snapshot-id">>,
        ParentSnapshotId,
        HasParentSnapshot
    ),
    Requirements0 = [#{<<"type">> => <<"assert-table-uuid">>, <<"uuid">> => TableUUID}],
    Requirements =
        case HasParentSnapshot of
            true ->
                [
                    #{
                        <<"type">> => <<"assert-ref-snapshot-id">>,
                        <<"ref">> => ?REF_NAME,
                        <<"snapshot-id">> => ParentSnapshotId
                    }
                    | Requirements0
                ];
            false ->
                [
                    #{
                        <<"type">> => <<"assert-ref-snapshot-id">>,
                        <<"ref">> => ?REF_NAME
                    }
                    | Requirements0
                ]
        end,
    #{
        <<"identifier">> => #{
            <<"namespace">> => NamespaceList,
            <<"name">> => Table
        },
        <<"requirements">> => Requirements,
        <<"updates">> => [
            #{
                <<"action">> => <<"add-snapshot">>,
                <<"snapshot">> => Snapshot
            },
            #{
                <<"action">> => <<"set-snapshot-ref">>,
                <<"ref-name">> => ?REF_NAME,
                <<"type">> => <<"branch">>,
                <<"snapshot-id">> => NewSnapshotId
            }
        ]
    }.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

iceberg_struct_to_avro(#{<<"type">> := <<"struct">>} = IceSc) ->
    IceFields = maps:get(<<"fields">>, IceSc),
    Fields = lists:map(fun iceberg_field_to_avro/1, IceFields),
    #{
        <<"type">> => <<"record">>,
        <<"fields">> => Fields
    }.

iceberg_field_to_avro(IceField) ->
    #{
        <<"id">> := Id,
        <<"name">> := Name,
        <<"type">> := IceType
    } = IceField,
    IsRequired = maps:get(<<"required">>, IceField, false),
    Type0 = iceberg_type_to_avro(IceType, IceField),
    Type1 =
        case Type0 of
            #{<<"type">> := <<"record">>} ->
                Type0#{<<"name">> => <<"r", (integer_to_binary(Id))/binary>>};
            _ ->
                Type0
        end,
    Type =
        case IsRequired of
            true ->
                Type1;
            false ->
                [<<"null">>, Type1]
        end,
    Field0 = #{
        <<"name">> => Name,
        <<"field-id">> => Id,
        <<"type">> => Type
    },
    Default = maps:get(<<"write-default">>, IceField, undefined),
    Field1 = emqx_utils_maps:put_if(
        Field0,
        <<"default">>,
        null,
        not IsRequired
    ),
    Field2 = emqx_utils_maps:put_if(
        Field1,
        <<"default">>,
        Default,
        Default /= undefined
    ),
    Doc = maps:get(<<"doc">>, IceField, undefined),
    emqx_utils_maps:put_if(
        Field2,
        <<"doc">>,
        Doc,
        Doc /= undefined
    ).

iceberg_type_to_avro(<<"string">>, _IceParent) ->
    <<"string">>;
iceberg_type_to_avro(<<"int">>, _IceParent) ->
    <<"int">>;
iceberg_type_to_avro(<<"long">>, _IceParent) ->
    <<"long">>;
iceberg_type_to_avro(<<"float">>, _IceParent) ->
    <<"float">>;
iceberg_type_to_avro(<<"double">>, _IceParent) ->
    <<"double">>;
iceberg_type_to_avro(<<"boolean">>, _IceParent) ->
    <<"boolean">>;
iceberg_type_to_avro(#{<<"type">> := <<"list">>} = IceType, _IceParent) ->
    ElementIceType = maps:get(<<"element">>, IceType),
    ElementType0 = iceberg_type_to_avro(ElementIceType, IceType),
    ElementId = maps:get(<<"element-id">>, IceType),
    ElementType =
        case ElementType0 of
            #{<<"type">> := <<"record">>} ->
                Name = <<"r", (integer_to_binary(ElementId))/binary>>,
                ElementType0#{<<"name">> => Name};
            _ ->
                ElementType0
        end,
    #{
        <<"type">> => <<"array">>,
        <<"element-id">> => ElementId,
        <<"items">> => ElementType
    };
iceberg_type_to_avro(#{<<"type">> := <<"map">>} = IceType, _IceParent) ->
    %% Note: in pyiceberg's `schema_conversion.ConvertSchemaToAvro.map`, there's an
    %% apparently unreachable clause when the key type should be `string`.  It's
    %% unreachable due to the types involved during conversion.  Since it's apparently
    %% impossible to observe this function output a `type: map` with real invocations, we
    %% port only the alternative behavior here.
    %%
    %% Note: in the original implementation, when we have a nested map type, it seems that
    %% the inner key field id ends up being used for both the inner and outer key field
    %% ids, which sounds wrong.
    %%
    %% From https://iceberg.apache.org/docs/1.8.1/schemas/ :
    %% > Iceberg tracks each field in a table schema using an ID that is never reused in a
    %%   table.
    KeyIceType = maps:get(<<"key">>, IceType),
    KeyId = maps:get(<<"key-id">>, IceType),
    KeyType = iceberg_type_to_avro(KeyIceType, IceType),
    ValueIceType = maps:get(<<"value">>, IceType),
    ValueId = maps:get(<<"value-id">>, IceType),
    ValueType = iceberg_type_to_avro(ValueIceType, IceType),
    KVName = iolist_to_binary([
        ["k", integer_to_binary(KeyId)],
        "_",
        ["v", integer_to_binary(ValueId)]
    ]),
    #{
        <<"type">> => <<"array">>,
        <<"items">> => #{
            <<"type">> => <<"record">>,
            <<"name">> => KVName,
            <<"fields">> => [
                #{
                    <<"name">> => <<"key">>,
                    <<"type">> => KeyType,
                    <<"field-id">> => KeyId
                },
                #{
                    <<"name">> => <<"value">>,
                    <<"type">> => ValueType,
                    <<"field-id">> => ValueId
                }
            ]
        },
        <<"logicalType">> => <<"map">>
    };
iceberg_type_to_avro(#{<<"type">> := <<"struct">>} = IceType, _IceParent) ->
    iceberg_struct_to_avro(IceType);
iceberg_type_to_avro(<<"decimal(", PrecScaleBin/binary>>, _IceParent) ->
    %% Assert
    <<")">> = binary:part(PrecScaleBin, {byte_size(PrecScaleBin), -1}),
    [PrecBin, ScaleBin] = binary:split(PrecScaleBin, [<<",">>, <<" ">>, <<")">>], [global, trim_all]),
    Precision = binary_to_integer(PrecBin),
    Scale = binary_to_integer(ScaleBin),
    Name = <<"decimal_", PrecBin/binary, "_", ScaleBin/binary>>,
    RequiredBytes = decimal_required_bytes(Precision),
    #{
        <<"type">> => <<"fixed">>,
        <<"size">> => RequiredBytes,
        <<"logicalType">> => <<"decimal">>,
        <<"precision">> => Precision,
        <<"scale">> => Scale,
        <<"name">> => Name
    };
iceberg_type_to_avro(<<"date">>, _IceParent) ->
    #{
        <<"type">> => <<"int">>,
        <<"logicalType">> => <<"date">>
    };
iceberg_type_to_avro(<<"time">>, _IceParent) ->
    #{
        <<"type">> => <<"long">>,
        <<"logicalType">> => <<"time-micros">>
    };
iceberg_type_to_avro(<<"timestamp">>, _IceParent) ->
    #{
        <<"type">> => <<"long">>,
        <<"logicalType">> => <<"timestamp-micros">>,
        <<"adjust-to-utc">> => false
    };
iceberg_type_to_avro(<<"timestamp_ns">>, _IceParent) ->
    #{
        <<"type">> => <<"long">>,
        <<"logicalType">> => <<"timestamp-nanos">>,
        <<"adjust-to-utc">> => false
    };
iceberg_type_to_avro(<<"timestamptz">>, _IceParent) ->
    #{
        <<"type">> => <<"long">>,
        <<"logicalType">> => <<"timestamp-micros">>,
        <<"adjust-to-utc">> => true
    };
iceberg_type_to_avro(<<"timestamptz_ns">>, _IceParent) ->
    #{
        <<"type">> => <<"long">>,
        <<"logicalType">> => <<"timestamp-nanos">>,
        <<"adjust-to-utc">> => true
    };
iceberg_type_to_avro(<<"uuid">>, _IceParent) ->
    #{
        <<"type">> => <<"fixed">>,
        <<"size">> => 16,
        <<"logicalType">> => <<"uuid">>,
        <<"name">> => <<"uuid_fixed">>
    };
iceberg_type_to_avro(<<"fixed[", SizeBin0/binary>>, _IceParent) ->
    %% Assert
    <<"]">> = binary:part(SizeBin0, {byte_size(SizeBin0), -1}),
    [SizeBin] = binary:split(SizeBin0, [<<"]">>], [global, trim_all]),
    Size = binary_to_integer(SizeBin),
    Name = <<"fixed_", SizeBin/binary>>,
    #{
        <<"type">> => <<"fixed">>,
        <<"size">> => Size,
        <<"name">> => Name
    };
iceberg_type_to_avro(<<"binary">>, _IceParent) ->
    <<"bytes">>;
iceberg_type_to_avro(Type, _IceParent) ->
    throw({unsupported_type, Type}).

memoized_required_bytes() ->
    maybe
        undefined ?= persistent_term:get(?REQUIRED_BYTES_PT_KEY, undefined),
        memoize_required_bytes()
    end.

%% See `pyiceberg.utils.decimal.decimal_required_bytes`.
memoize_required_bytes() ->
    MaxPrecision =
        lists:map(
            fun(Pos) ->
                math:floor(
                    math:log10(
                        abs(
                            math:pow(2, 8 * Pos - 1) - 1
                        )
                    )
                )
            end,
            lists:seq(0, 23)
        ),
    RequiredLength =
        lists:foldl(
            fun(P, Acc) ->
                [Max | _] =
                    lists:filtermap(
                        fun({Pos, MaxPrec}) ->
                            case MaxPrec >= P of
                                true -> {true, Pos};
                                false -> false
                            end
                        end,
                        lists:enumerate(0, MaxPrecision)
                    ),
                Acc#{P => Max}
            end,
            #{},
            lists:seq(0, 39)
        ),
    persistent_term:put(?REQUIRED_BYTES_PT_KEY, RequiredLength),
    RequiredLength.

decimal_required_bytes(Precision) ->
    RequiredLength = memoized_required_bytes(),
    maps:get(Precision, RequiredLength).

mk_summary(CommitContext) ->
    #{
        data_size := DataSize,
        num_records := NumRecords,
        loaded_table := LoadedTable
    } = CommitContext,
    CurrentSnapshot = find_current_snapshot(LoadedTable, #{}),
    PrevSummary = maps:get(<<"summary">>, CurrentSnapshot, #{}),
    Summary0 = #{
        <<"operation">> => <<"append">>,
        %% Currently, we always upload just a single data file and single manifest entry.
        <<"added-data-files">> => 1,
        <<"total-data-files">> => 1,
        <<"total-delete-files">> => 0,
        <<"total-equality-deletes">> => 0,
        <<"total-position-deletes">> => 0,
        <<"added-files-size">> => DataSize,
        <<"total-files-size">> => DataSize,
        <<"added-records">> => NumRecords,
        <<"total-records">> => NumRecords
    },
    Summary1 = merge_summaries(PrevSummary, Summary0),
    maps:map(
        fun
            (_K, V) when is_integer(V) ->
                integer_to_binary(V);
            (_K, V) ->
                V
        end,
        Summary1
    ).

get_summary_value(Summary, Key) ->
    ValBin = maps:get(Key, Summary, <<"0">>),
    binary_to_integer(ValBin).

merge_summaries(PrevSummary, Summary0) ->
    maps:map(
        fun
            (<<"total-", _/binary>> = K, V) when is_integer(V) ->
                PrevV = get_summary_value(PrevSummary, K),
                V + PrevV;
            (_K, V) ->
                V
        end,
        Summary0
    ).

find(Xs, Acc0, Predicate) ->
    emqx_utils:foldl_while(
        fun(X, Acc) ->
            case Predicate(X) of
                {halt, Res} ->
                    {halt, Res};
                cont ->
                    {cont, Acc}
            end
        end,
        Acc0,
        Xs
    ).

current_snapshot_id(LoadedTable) ->
    #{<<"metadata">> := #{<<"current-snapshot-id">> := Id}} = LoadedTable,
    case Id of
        -1 ->
            undefined;
        null ->
            undefined;
        _ ->
            Id
    end.
