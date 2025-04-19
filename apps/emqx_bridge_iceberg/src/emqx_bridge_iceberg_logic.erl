%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_iceberg_logic).

-feature(maybe_expr, enable).

%% API
-export([
    parse_loaded_table/1,
    convert_iceberg_schema_to_avro/1,
    compute_update_table_request/1,
    find_current_schema/1,
    find_current_schema/2,
    find_current_snapshot/2,
    partition_keys_to_segments/2,
    manifest_entry_avro_schema/2
]).

-ifdef(TEST).
-export([index_fields_by_id/1]).
-endif.

-export_type([partition_field/0, partition_spec_parsed/0]).

-include("emqx_bridge_iceberg.hrl").

-elvis([{elvis_style, atom_naming_convention, disable}]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(REQUIRED_BYTES_PT_KEY, {?MODULE, required_bytes}).
-define(REF_NAME, <<"main">>).
%% 2^31 - 1
-define(INTEGER_MAX_VALUE, 2147483647).

-type loaded_table() :: map().

-type partitioned() :: #partitioned{fields :: [partition_field()]}.
-type unpartitioned() :: #unpartitioned{}.

-type parsed_table() :: #{
    ?avro_schema := avro:avro_type(),
    ?iceberg_schema := map(),
    ?loaded_table := loaded_table(),
    ?partition_spec := partition_spec_parsed(),
    ?partition_spec_id := integer()
}.

-type partition_spec_parsed() :: partitioned() | unpartitioned().

-type partition_field() :: #{
    get_fn := get_fn(),
    id := integer(),
    name := binary(),
    raw := map(),
    result_type := ice_type(),
    source_type := ice_type(),
    transform_fn := transform_fn()
}.

-type partition_spec_id() :: integer().

-type ice_schema() :: map().
-type ice_type() :: binary() | ice_schema().
-type get_fn() :: fun((map()) -> null | term()).
-type transform_fn() :: fun((null | term()) -> term()).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec parse_loaded_table(loaded_table()) ->
    {ok, parsed_table()}
    | {error, schema_not_found}
    | {error, partition_spec_not_found}
    | {error, invalid_spec}.
parse_loaded_table(LoadedTable) ->
    maybe
        {ok, IceSchema} ?= find_current_schema(LoadedTable),
        {ok, AvroSchema0} ?= convert_iceberg_schema_to_avro(IceSchema),
        AvroSchema1 = AvroSchema0#{<<"name">> => <<"root">>},
        AvroSchema = avro:decode_schema(emqx_utils_json:encode(AvroSchema1)),
        {ok, {PartitionSpecId, PartSpec}} ?= find_partition_spec(LoadedTable, IceSchema),
        {ok, #{
            ?avro_schema => AvroSchema,
            ?iceberg_schema => IceSchema,
            ?loaded_table => LoadedTable,
            ?partition_spec => PartSpec,
            ?partition_spec_id => PartitionSpecId
        }}
    end.

-spec find_current_schema(loaded_table()) -> {ok, map()} | {error, schema_not_found}.
find_current_schema(LoadedTable) ->
    #{<<"metadata">> := #{<<"current-schema-id">> := SchemaId}} = LoadedTable,
    find_current_schema(LoadedTable, SchemaId).

-spec find_current_schema(loaded_table(), integer()) -> {ok, map()} | {error, schema_not_found}.
find_current_schema(LoadedTable, SchemaId) ->
    #{<<"metadata">> := #{<<"schemas">> := IceSchemas}} = LoadedTable,
    find(
        IceSchemas,
        {error, schema_not_found},
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

-spec find_partition_spec(loaded_table(), ice_schema()) ->
    {ok, {partition_spec_id(), unpartitioned() | partitioned()}}
    | {error, partition_spec_not_found}
    | {error, invalid_spec}.
find_partition_spec(LoadedTable, IceSchema) ->
    #{
        <<"metadata">> := #{
            <<"default-spec-id">> := DefaultSpecId,
            <<"partition-specs">> := PartitionSpecs
        }
    } = LoadedTable,
    find(
        PartitionSpecs,
        {error, partition_spec_not_found},
        fun(PartitionSpec) ->
            case PartitionSpec of
                #{<<"spec-id">> := DefaultSpecId, <<"fields">> := []} ->
                    {halt, {ok, {DefaultSpecId, #unpartitioned{}}}};
                #{<<"spec-id">> := DefaultSpecId, <<"fields">> := [_ | _]} ->
                    {halt, parse_partition_spec(IceSchema, PartitionSpec)};
                #{<<"spec-id">> := DefaultSpecId} ->
                    {halt, {error, invalid_spec}};
                _ ->
                    cont
            end
        end
    ).

convert_iceberg_schema_to_avro(#{<<"type">> := <<"struct">>} = IceSc) ->
    try
        {ok, iceberg_struct_to_avro(IceSc)}
    catch
        throw:{unsupported_type, Type} ->
            {error, {unsupported_type, Type}}
    end.

partition_keys_to_segments(PartitionKeys, #partitioned{fields = PartitionFields}) ->
    lists:map(
        fun({PK, #{name := PFName, source_type := IceType}}) ->
            K = partition_url_quote(PFName),
            V0 = human_readable_partition_value(PK, IceType),
            V = partition_url_quote(V0),
            <<K/binary, "=", V/binary>>
        end,
        lists:zip(PartitionKeys, PartitionFields)
    ).

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

-doc """
Returns the manifest entry Avro schema to be used.

The schema itself is different based on the partition spec used by the table, which needs
to be injected into `data_file.partition`.

This structure is generated by parsing the base Avro schema contained in
`priv/manfiest-entry.avsc`.
""".
manifest_entry_avro_schema(#partitioned{fields = PartitionFields}, IcebergSchema) ->
    do_manifest_entry_avro_schema(PartitionFields, IcebergSchema);
manifest_entry_avro_schema(#unpartitioned{}, IcebergSchema) ->
    do_manifest_entry_avro_schema([], IcebergSchema).

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
    Type0 = iceberg_type_to_avro(IceType),
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

iceberg_type_to_avro(<<"string">>) ->
    <<"string">>;
iceberg_type_to_avro(<<"int">>) ->
    <<"int">>;
iceberg_type_to_avro(<<"long">>) ->
    <<"long">>;
iceberg_type_to_avro(<<"float">>) ->
    <<"float">>;
iceberg_type_to_avro(<<"double">>) ->
    <<"double">>;
iceberg_type_to_avro(<<"boolean">>) ->
    <<"boolean">>;
iceberg_type_to_avro(#{<<"type">> := <<"list">>} = IceType) ->
    ElementIceType = maps:get(<<"element">>, IceType),
    ElementType0 = iceberg_type_to_avro(ElementIceType),
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
iceberg_type_to_avro(#{<<"type">> := <<"map">>} = IceType) ->
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
    KeyType = iceberg_type_to_avro(KeyIceType),
    ValueIceType = maps:get(<<"value">>, IceType),
    ValueId = maps:get(<<"value-id">>, IceType),
    ValueType = iceberg_type_to_avro(ValueIceType),
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
iceberg_type_to_avro(#{<<"type">> := <<"struct">>} = IceType) ->
    iceberg_struct_to_avro(IceType);
iceberg_type_to_avro(<<"decimal(", PrecScaleBin/binary>>) ->
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
iceberg_type_to_avro(<<"date">>) ->
    #{
        <<"type">> => <<"int">>,
        <<"logicalType">> => <<"date">>
    };
iceberg_type_to_avro(<<"time">>) ->
    #{
        <<"type">> => <<"long">>,
        <<"logicalType">> => <<"time-micros">>
    };
iceberg_type_to_avro(<<"timestamp">>) ->
    #{
        <<"type">> => <<"long">>,
        <<"logicalType">> => <<"timestamp-micros">>,
        <<"adjust-to-utc">> => false
    };
iceberg_type_to_avro(<<"timestamp_ns">>) ->
    #{
        <<"type">> => <<"long">>,
        <<"logicalType">> => <<"timestamp-nanos">>,
        <<"adjust-to-utc">> => false
    };
iceberg_type_to_avro(<<"timestamptz">>) ->
    #{
        <<"type">> => <<"long">>,
        <<"logicalType">> => <<"timestamp-micros">>,
        <<"adjust-to-utc">> => true
    };
iceberg_type_to_avro(<<"timestamptz_ns">>) ->
    #{
        <<"type">> => <<"long">>,
        <<"logicalType">> => <<"timestamp-nanos">>,
        <<"adjust-to-utc">> => true
    };
iceberg_type_to_avro(<<"uuid">>) ->
    #{
        <<"type">> => <<"fixed">>,
        <<"size">> => 16,
        <<"logicalType">> => <<"uuid">>,
        <<"name">> => <<"uuid_fixed">>
    };
iceberg_type_to_avro(<<"fixed[", SizeBin0/binary>>) ->
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
iceberg_type_to_avro(<<"binary">>) ->
    <<"bytes">>;
iceberg_type_to_avro(Type) ->
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

parse_partition_spec(IceSchema, PartitionSpec) ->
    #{
        <<"spec-id">> := PartitionSpecId,
        <<"fields">> := PartitionFieldsRaw
    } = PartitionSpec,
    FieldIndex = index_fields_by_id(IceSchema),
    PartitionFields0 =
        emqx_utils:foldl_while(
            fun(PartitionFieldRaw, {ok, Acc}) ->
                case parse_partition_field(PartitionFieldRaw, FieldIndex) of
                    {ok, PartitionField} ->
                        {cont, {ok, [PartitionField | Acc]}};
                    {error, _} = Error ->
                        {halt, Error}
                end
            end,
            {ok, []},
            PartitionFieldsRaw
        ),
    maybe
        {ok, PartitionFields} ?= PartitionFields0,
        {ok, {PartitionSpecId, #partitioned{fields = lists:reverse(PartitionFields)}}}
    end.

-spec index_fields_by_id(loaded_table()) ->
    #{
        integer() => #{
            type := binary() | map(),
            can_be_pk := boolean(),
            path := [binary() | '$N' | '$K' | '$V']
        }
    }.
index_fields_by_id(#{<<"type">> := <<"struct">>} = IceSchema) ->
    %% Source columns for partition keys must be primitive types, cannot be contained in
    %% maps or lists, but otherwise nested inside structs.
    CanBePK = true,
    {_Path, _CanBePK, FieldIndex} = do_index_fields_by_id(IceSchema, {[], CanBePK, #{}}),
    FieldIndex.

do_index_fields_by_id(#{<<"type">> := <<"struct">>} = IceType, {Path, CanBePK, Index}) ->
    #{<<"fields">> := Fields} = IceType,
    lists:foldl(
        fun(IceTypeIn, {_, _, IndexAcc}) ->
            do_index_fields_by_id(IceTypeIn, {Path, CanBePK, IndexAcc})
        end,
        {Path, CanBePK, Index},
        Fields
    );
do_index_fields_by_id(#{<<"id">> := Id} = IceType, {Path0, CanBePK, Index0}) ->
    #{<<"type">> := InnerType, <<"name">> := Name} = IceType,
    Path = Path0 ++ [Name],
    Index = Index0#{Id => #{type => IceType, can_be_pk => CanBePK, path => Path}},
    do_index_fields_by_id(InnerType, {Path, CanBePK, Index});
do_index_fields_by_id(#{<<"type">> := <<"list">>} = IceType, {Path0, _CanBePK, Index0}) ->
    #{<<"element-id">> := ElementId, <<"element">> := ElementType} = IceType,
    Path = Path0 ++ ['$N'],
    %% Note [PK types]
    %% This type is either primitive, or a composite that is not a struct (a map or list),
    %% so any inner type cannot be a partition key.
    Index = Index0#{ElementId => #{type => ElementType, can_be_pk => false, path => Path}},
    do_index_fields_by_id(ElementType, {Path, false, Index});
do_index_fields_by_id(#{<<"type">> := <<"map">>} = IceType, {Path0, _CanBePK, Index0}) ->
    #{
        <<"key-id">> := KeyId,
        <<"key">> := KeyType,
        <<"value-id">> := ValId,
        <<"value">> := ValType
    } = IceType,
    PathK = Path0 ++ ['$K'],
    PathV = Path0 ++ ['$V'],
    %% See Note [PK types] about these `false`s
    Index1 = Index0#{
        KeyId => #{type => KeyType, can_be_pk => false, path => PathK},
        ValId => #{type => ValType, can_be_pk => false, path => PathV}
    },
    {_, _, Index2} = do_index_fields_by_id(KeyType, {PathK, false, Index1}),
    do_index_fields_by_id(ValType, {PathV, false, Index2});
do_index_fields_by_id(_PrimitiveType, {Path, CanBePK, Index}) ->
    {Path, CanBePK, Index}.

transform_result_type(<<"identity">>, SourceIceType) ->
    SourceIceType;
transform_result_type(<<"void">>, SourceIceType) ->
    SourceIceType;
%% TODO
transform_result_type(TransformName, _SourceIceType) ->
    throw({unsupported_transform, TransformName}).

mk_transform_fn(<<"identity">>) ->
    fmap(fun(X) -> X end);
mk_transform_fn(<<"void">>) ->
    fun(_) -> null end;
mk_transform_fn(<<"bucket[", Rest/binary>>) ->
    %% Assert
    <<"]">> = binary:part(Rest, {byte_size(Rest), -1}),
    [NBin] = binary:split(Rest, [<<"]">>], [global, trim_all]),
    N = binary_to_integer(NBin),
    fmap(fun(X) ->
        Hash = murmerl3:hash_32(X),
        (Hash band ?INTEGER_MAX_VALUE) rem N
    end);
%% TODO:
%%  - year
%%  - month
%%  - day
%%  - hour
%%  - truncate[W]
mk_transform_fn(TransformName) ->
    throw({unsupported_transform, TransformName}).

fmap(F) ->
    fun
        (null) -> null;
        (X) -> F(X)
    end.

parse_partition_field(PartitionField, FieldIndex) ->
    #{
        <<"field-id">> := PartitionFieldId,
        <<"source-id">> := SourceFieldId,
        <<"name">> := PartitionFieldName,
        <<"transform">> := TransformName
    } = PartitionField,
    %% Source columns must be primitive types, cannot be contained in maps or lists, but
    %% otherwise nested inside structs.
    case FieldIndex of
        #{SourceFieldId := #{can_be_pk := false}} ->
            {error, #{
                msg => <<"bad_partition_spec">>,
                reason => <<"partition spec uses invalid field-id">>,
                source_field_id => SourceFieldId,
                partition_field_name => PartitionFieldName
            }};
        #{SourceFieldId := #{path := Path, can_be_pk := true, type := IceType}} ->
            {ok, #{
                get_fn => fun(M) -> emqx_utils_maps:deep_get(Path, M, null) end,
                id => PartitionFieldId,
                name => PartitionFieldName,
                raw => PartitionField,
                result_type => transform_result_type(TransformName, IceType),
                source_type => IceType,
                transform_fn => mk_transform_fn(TransformName)
            }};
        #{} ->
            {error, #{
                msg => <<"bad_partition_spec">>,
                reason => <<"could not find field-id in schema">>,
                source_field_id => SourceFieldId,
                partition_field_name => PartitionFieldName
            }}
    end.

partition_url_quote(X) ->
    Escaped = uri_string:quote(X, " "),
    binary:replace(Escaped, <<" ">>, <<"+">>, [global]).

human_readable_partition_value(null, _IceType) ->
    <<"null">>;
human_readable_partition_value(Bin, _IceType) when is_binary(Bin) ->
    %% maybe port Elixir's `String.printable?/1` ?
    case io_lib:printable_list(binary_to_list(Bin)) of
        true ->
            Bin;
        false ->
            base64:encode(Bin)
    end;
human_readable_partition_value(Bool, _IceType) when is_boolean(Bool) ->
    atom_to_binary(Bool);
human_readable_partition_value(I, _IceType) when is_integer(I) ->
    %% use ice type to detect datetimes
    integer_to_binary(I);
human_readable_partition_value(F, _IceType) when is_float(F) ->
    %% any particular format/precision?
    float_to_binary(F);
human_readable_partition_value(_X, _IceType) ->
    error(todo).

do_manifest_entry_avro_schema(PartitionFields, IcebergSchema) ->
    AvroPartitionFields = lists:map(
        fun(#{id := Id, name := N, source_type := #{<<"type">> := T}}) ->
            #{
                <<"field-id">> => Id,
                <<"name">> => N,
                <<"type">> => [<<"null">>, iceberg_type_to_avro(T)],
                <<"default">> => null
            }
        end,
        PartitionFields
    ),
    ScJSON = manifest_entry_avro_schema_json(AvroPartitionFields),
    ManifestEntrySc = avro:decode_schema(emqx_utils_json:encode(ScJSON)),
    %% Some implementation need this to be able to plan/scan/parse the manifests.
    ManifestEntryMeta = [
        {<<"schema">>, emqx_utils_json:encode(IcebergSchema)},
        {<<"partition-spec">>, emqx_utils_json:encode([R || #{raw := R} <- PartitionFields])}
    ],
    ManifestEntryHeader = avro_ocf:make_header(ManifestEntrySc, ManifestEntryMeta),
    {ManifestEntryHeader, ManifestEntrySc}.

%% Moved the meat of the implementation here just to avoid bloating the main API section.
manifest_entry_avro_schema_json(PartitionFieldsAvroSchema) when
    is_list(PartitionFieldsAvroSchema)
->
    #{
        <<"fields">> =>
            [
                #{
                    <<"field-id">> => 0,
                    <<"name">> => <<"status">>,
                    <<"type">> => <<"int">>
                },
                #{
                    <<"default">> => null,
                    <<"field-id">> => 1,
                    <<"name">> => <<"snapshot_id">>,
                    <<"type">> => [<<"null">>, <<"long">>]
                },
                #{
                    <<"default">> => null,
                    <<"field-id">> => 3,
                    <<"name">> => <<"sequence_number">>,
                    <<"type">> => [<<"null">>, <<"long">>]
                },
                #{
                    <<"default">> => null,
                    <<"field-id">> => 4,
                    <<"name">> => <<"file_sequence_number">>,
                    <<"type">> => [<<"null">>, <<"long">>]
                },
                #{
                    <<"field-id">> => 2,
                    <<"name">> => <<"data_file">>,
                    <<"type">> =>
                        #{
                            <<"fields">> =>
                                [
                                    #{
                                        <<"doc">> => <<"File format name: avro, orc, or parquet">>,
                                        <<"field-id">> => 134,
                                        <<"name">> => <<"content">>,
                                        <<"type">> => <<"int">>
                                    },
                                    #{
                                        <<"doc">> => <<"Location URI with FS scheme">>,
                                        <<"field-id">> => 100,
                                        <<"name">> => <<"file_path">>,
                                        <<"type">> => <<"string">>
                                    },
                                    #{
                                        <<"doc">> => <<"File format name: avro, orc, or parquet">>,
                                        <<"field-id">> => 101,
                                        <<"name">> => <<"file_format">>,
                                        <<"type">> => <<"string">>
                                    },
                                    #{
                                        <<"doc">> =>
                                            <<"Partition data tuple, schema based on the partition spec">>,
                                        <<"field-id">> => 102,
                                        <<"name">> => <<"partition">>,
                                        <<"type">> =>
                                            #{
                                                <<"fields">> => PartitionFieldsAvroSchema,
                                                <<"name">> => <<"r102">>,
                                                <<"type">> => <<"record">>
                                            }
                                    },
                                    #{
                                        <<"doc">> => <<"Number of records in the file">>,
                                        <<"field-id">> => 103,
                                        <<"name">> => <<"record_count">>,
                                        <<"type">> => <<"long">>
                                    },
                                    #{
                                        <<"doc">> => <<"Total file size in bytes">>,
                                        <<"field-id">> => 104,
                                        <<"name">> => <<"file_size_in_bytes">>,
                                        <<"type">> => <<"long">>
                                    },
                                    #{
                                        <<"default">> => null,
                                        <<"doc">> => <<"Map of column id to total size on disk">>,
                                        <<"field-id">> => 108,
                                        <<"name">> => <<"column_sizes">>,
                                        <<"type">> =>
                                            [
                                                <<"null">>,
                                                #{
                                                    <<"items">> =>
                                                        #{
                                                            <<"fields">> =>
                                                                [
                                                                    #{
                                                                        <<"field-id">> => 117,
                                                                        <<"name">> => <<"key">>,
                                                                        <<"type">> => <<"int">>
                                                                    },
                                                                    #{
                                                                        <<"field-id">> => 118,
                                                                        <<"name">> => <<"value">>,
                                                                        <<"type">> => <<"long">>
                                                                    }
                                                                ],
                                                            <<"name">> => <<"k117_v118">>,
                                                            <<"type">> => <<"record">>
                                                        },
                                                    <<"logicalType">> => <<"map">>,
                                                    <<"type">> => <<"array">>
                                                }
                                            ]
                                    },
                                    #{
                                        <<"default">> => null,
                                        <<"doc">> =>
                                            <<"Map of column id to total count, including null and NaN">>,
                                        <<"field-id">> => 109,
                                        <<"name">> => <<"value_counts">>,
                                        <<"type">> =>
                                            [
                                                <<"null">>,
                                                #{
                                                    <<"items">> =>
                                                        #{
                                                            <<"fields">> =>
                                                                [
                                                                    #{
                                                                        <<"field-id">> => 119,
                                                                        <<"name">> => <<"key">>,
                                                                        <<"type">> => <<"int">>
                                                                    },
                                                                    #{
                                                                        <<"field-id">> => 120,
                                                                        <<"name">> => <<"value">>,
                                                                        <<"type">> => <<"long">>
                                                                    }
                                                                ],
                                                            <<"name">> => <<"k119_v120">>,
                                                            <<"type">> => <<"record">>
                                                        },
                                                    <<"logicalType">> => <<"map">>,
                                                    <<"type">> => <<"array">>
                                                }
                                            ]
                                    },
                                    #{
                                        <<"default">> => null,
                                        <<"doc">> => <<"Map of column id to null value count">>,
                                        <<"field-id">> => 110,
                                        <<"name">> => <<"null_value_counts">>,
                                        <<"type">> =>
                                            [
                                                <<"null">>,
                                                #{
                                                    <<"items">> =>
                                                        #{
                                                            <<"fields">> =>
                                                                [
                                                                    #{
                                                                        <<"field-id">> => 121,
                                                                        <<"name">> => <<"key">>,
                                                                        <<"type">> => <<"int">>
                                                                    },
                                                                    #{
                                                                        <<"field-id">> => 122,
                                                                        <<"name">> => <<"value">>,
                                                                        <<"type">> => <<"long">>
                                                                    }
                                                                ],
                                                            <<"name">> => <<"k121_v122">>,
                                                            <<"type">> => <<"record">>
                                                        },
                                                    <<"logicalType">> => <<"map">>,
                                                    <<"type">> => <<"array">>
                                                }
                                            ]
                                    },
                                    #{
                                        <<"default">> => null,
                                        <<"doc">> =>
                                            <<"Map of column id to number of NaN values in the column">>,
                                        <<"field-id">> => 137,
                                        <<"name">> => <<"nan_value_counts">>,
                                        <<"type">> =>
                                            [
                                                <<"null">>,
                                                #{
                                                    <<"items">> =>
                                                        #{
                                                            <<"fields">> =>
                                                                [
                                                                    #{
                                                                        <<"field-id">> => 138,
                                                                        <<"name">> => <<"key">>,
                                                                        <<"type">> => <<"int">>
                                                                    },
                                                                    #{
                                                                        <<"field-id">> => 139,
                                                                        <<"name">> => <<"value">>,
                                                                        <<"type">> => <<"long">>
                                                                    }
                                                                ],
                                                            <<"name">> => <<"k138_v139">>,
                                                            <<"type">> => <<"record">>
                                                        },
                                                    <<"logicalType">> => <<"map">>,
                                                    <<"type">> => <<"array">>
                                                }
                                            ]
                                    },
                                    #{
                                        <<"default">> => null,
                                        <<"doc">> => <<"Map of column id to lower bound">>,
                                        <<"field-id">> => 125,
                                        <<"name">> => <<"lower_bounds">>,
                                        <<"type">> =>
                                            [
                                                <<"null">>,
                                                #{
                                                    <<"items">> =>
                                                        #{
                                                            <<"fields">> =>
                                                                [
                                                                    #{
                                                                        <<"field-id">> => 126,
                                                                        <<"name">> => <<"key">>,
                                                                        <<"type">> => <<"int">>
                                                                    },
                                                                    #{
                                                                        <<"field-id">> => 127,
                                                                        <<"name">> => <<"value">>,
                                                                        <<"type">> => <<"bytes">>
                                                                    }
                                                                ],
                                                            <<"name">> => <<"k126_v127">>,
                                                            <<"type">> => <<"record">>
                                                        },
                                                    <<"logicalType">> => <<"map">>,
                                                    <<"type">> => <<"array">>
                                                }
                                            ]
                                    },
                                    #{
                                        <<"default">> => null,
                                        <<"doc">> => <<"Map of column id to upper bound">>,
                                        <<"field-id">> => 128,
                                        <<"name">> => <<"upper_bounds">>,
                                        <<"type">> =>
                                            [
                                                <<"null">>,
                                                #{
                                                    <<"items">> =>
                                                        #{
                                                            <<"fields">> =>
                                                                [
                                                                    #{
                                                                        <<"field-id">> => 129,
                                                                        <<"name">> => <<"key">>,
                                                                        <<"type">> => <<"int">>
                                                                    },
                                                                    #{
                                                                        <<"field-id">> => 130,
                                                                        <<"name">> => <<"value">>,
                                                                        <<"type">> => <<"bytes">>
                                                                    }
                                                                ],
                                                            <<"name">> => <<"k129_v130">>,
                                                            <<"type">> => <<"record">>
                                                        },
                                                    <<"logicalType">> => <<"map">>,
                                                    <<"type">> => <<"array">>
                                                }
                                            ]
                                    },
                                    #{
                                        <<"default">> => null,
                                        <<"doc">> => <<"Encryption key metadata blob">>,
                                        <<"field-id">> => 131,
                                        <<"name">> => <<"key_metadata">>,
                                        <<"type">> => [<<"null">>, <<"bytes">>]
                                    },
                                    #{
                                        <<"default">> => null,
                                        <<"doc">> => <<"Splittable offsets">>,
                                        <<"field-id">> => 132,
                                        <<"name">> => <<"split_offsets">>,
                                        <<"type">> =>
                                            [
                                                <<"null">>,
                                                #{
                                                    <<"element-id">> => 133,
                                                    <<"items">> => <<"long">>,
                                                    <<"type">> => <<"array">>
                                                }
                                            ]
                                    },
                                    #{
                                        <<"default">> => null,
                                        <<"doc">> =>
                                            <<"Field ids used to determine row equality in equality delete files.">>,
                                        <<"field-id">> => 135,
                                        <<"name">> => <<"equality_ids">>,
                                        <<"type">> =>
                                            [
                                                <<"null">>,
                                                #{
                                                    <<"element-id">> => 136,
                                                    <<"items">> => <<"long">>,
                                                    <<"type">> => <<"array">>
                                                }
                                            ]
                                    },
                                    #{
                                        <<"default">> => null,
                                        <<"doc">> => <<"ID representing sort order for this file">>,
                                        <<"field-id">> => 140,
                                        <<"name">> => <<"sort_order_id">>,
                                        <<"type">> => [<<"null">>, <<"int">>]
                                    }
                                ],
                            <<"name">> => <<"r2">>,
                            <<"type">> => <<"record">>
                        }
                }
            ],
        <<"name">> => <<"manifest_entry">>,
        <<"type">> => <<"record">>
    }.
