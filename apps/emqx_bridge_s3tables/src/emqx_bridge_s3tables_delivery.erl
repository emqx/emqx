%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_s3tables_delivery).

-behaviour(emqx_connector_aggreg_delivery).

%% API
-export([
    load_table/3,
    validate_table/3
]).

%% `emqx_connector_aggreg_delivery' API
-export([
    init_transfer_state_and_container_opts/2,
    process_append/2,
    process_write/1,
    process_complete/1
]).

-include("emqx_bridge_s3tables.hrl").
-include_lib("emqx_connector_aggregator/include/emqx_connector_aggregator.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(MAX_RETRY_ATTEMPTS, 10).
-define(MEGABYTE, 1024 * 1024).
-define(GIGABYTE, 1024 * ?MEGABYTE).
%% 2^63 - 1
-define(LONG_MAX_VALUE, 9223372036854775807).

%% Manifest constants
-define(DATA_FILE_CONTENT_DATA, 0).
-define(DATA_FILE_FORMAT_AVRO, <<"AVRO">>).
-define(MANIFEST_ENTRY_STATUS_ADDED, 1).
-define(MANIFEST_LIST_CONTENT_DATA, 0).

-record(single_transfer, {
    data_file_key,
    data_size = 0,
    num_records = 0,
    container,
    state
}).
-record(partitioned_transfer, {container, state}).

-type iceberg_client() :: emqx_bridge_s3tables_impl:iceberg_client().
-type location_client() :: emqx_bridge_s3tables_impl:location_client().
-type namespace() :: emqx_bridge_s3tables_impl:namespace().
-type partition_key() :: emqx_bridge_s3tables_aggreg_partitioned:partition_key().
-type table_name() :: emqx_bridge_s3tables_impl:table_name().

-type inner_transfer() :: single_transfer() | partitioned_transfer().

-type single_transfer() :: #single_transfer{
    data_file_key :: binary(),
    data_size :: non_neg_integer(),
    num_records :: non_neg_integer(),
    state :: emqx_s3_upload:t()
}.

-type partitioned_transfer() :: #partitioned_transfer{
    state :: #{
        [partition_key()] => #{
            ?data_file_key := binary(),
            ?data_size := non_neg_integer(),
            ?num_records := non_neg_integer(),
            ?s3_transfer_state := emqx_s3_upload:t()
        }
    }
}.

-type transfer_opts() :: #{
    upload_options := #{
        ?action_res_id := action_resource_id(),
        ?client := iceberg_client(),
        ?location_client := location_client(),
        ?namespace := namespace(),
        ?table := table_name()
    }
}.

-type transfer_state() :: #{
    ?action_res_id := action_resource_id(),
    ?base_path := binary(),
    ?bucket := binary(),
    ?client := iceberg_client(),
    ?iceberg_schema := map(),
    ?inner_transfer := inner_transfer(),
    ?loaded_table := map(),
    ?n_attempt := non_neg_integer(),
    ?namespace := namespace(),
    ?partition_spec := emqx_bridge_s3tables_logic:partition_spec_parsed(),
    ?partition_spec_id := integer(),
    ?s3_client := emqx_s3_client:client(),
    ?schema_id := integer(),
    ?seq_num := integer(),
    ?table := table_name(),
    ?table_uuid := binary(),
    ?write_uuid := binary()
}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

load_table(Client, Namespace, Table) ->
    emqx_bridge_s3tables_client_s3t:load_table(Client, Namespace, Table).

validate_table(Client, Namespace, Table) ->
    maybe
        {ok, LoadedTable} ?= load_table(Client, Namespace, Table),
        emqx_bridge_s3tables_logic:parse_loaded_table(LoadedTable)
    end.

%%------------------------------------------------------------------------------
%% `emqx_connector_aggreg_delivery' API
%%------------------------------------------------------------------------------

-spec init_transfer_state_and_container_opts(buffer(), transfer_opts()) ->
    {ok, transfer_state(), map()} | {error, term()}.
init_transfer_state_and_container_opts(_Buffer, Opts) ->
    #{
        upload_options := #{
            ?action_res_id := ActionResId,
            ?client := Client,
            ?location_client := #{?s3_client_config := S3ClientConfig},
            ?namespace := Namespace,
            ?table := Table
        }
    } = Opts,
    maybe
        {ok, #{
            ?loaded_table := LoadedTable,
            ?avro_schema := AvroSchema,
            ?iceberg_schema := IcebergSchema,
            ?partition_spec := PartSpec,
            ?partition_spec_id := PartitionSpecId
        }} ?=
            validate_table(Client, Namespace, Table),
        #{
            <<"metadata">> := #{
                <<"current-schema-id">> := SchemaId,
                <<"last-sequence-number">> := LastSeqNum,
                <<"location">> := Location,
                <<"table-uuid">> := TableUUID
            }
        } = LoadedTable,
        #{bucket := Bucket, base_path := BasePath} = parse_location(Location),
        WriteUUID = uuid4(),
        S3Client = emqx_s3_client:create(binary_to_list(Bucket), S3ClientConfig),
        InnerOpts = #{
            ?avro_schema => AvroSchema,
            ?base_path => BasePath,
            ?s3_client => S3Client,
            ?write_uuid => WriteUUID
        },
        {ContainerOpts, InnerTransferState} =
            mk_container_opts_and_inner_transfer_state(PartSpec, InnerOpts),
        TransferState = #{
            ?action_res_id => ActionResId,
            ?base_path => BasePath,
            ?bucket => Bucket,
            ?client => Client,
            ?iceberg_schema => IcebergSchema,
            ?inner_transfer => InnerTransferState,
            ?loaded_table => LoadedTable,
            ?n_attempt => 0,
            ?namespace => Namespace,
            ?partition_spec => PartSpec,
            ?partition_spec_id => PartitionSpecId,
            ?s3_client => S3Client,
            ?schema_id => SchemaId,
            ?seq_num => LastSeqNum + 1,
            ?table => Table,
            ?table_uuid => TableUUID,
            ?write_uuid => WriteUUID
        },
        {ok, TransferState, ContainerOpts}
    end.

-spec process_append([emqx_connector_aggregator:record()], transfer_state()) ->
    transfer_state().
process_append(
    Records, #{?inner_transfer := #single_transfer{container = Container0}} = TransferState0
) ->
    {IOData, #{num_records := N1}, Container} =
        emqx_bridge_s3tables_aggreg_avro:fill(Records, Container0),
    #{
        ?inner_transfer := #single_transfer{
            data_size = S0,
            num_records = N0,
            state = S3TransferState0
        } = Inner0
    } = TransferState0,
    S1 = iolist_size(IOData),
    {ok, S3TransferState} = emqx_s3_upload:append(IOData, S3TransferState0),
    TransferState0#{
        ?inner_transfer := Inner0#single_transfer{
            container = Container,
            data_size = S0 + S1,
            num_records = N0 + N1,
            state = S3TransferState
        }
    };
process_append(Records, #{?inner_transfer := #partitioned_transfer{}} = TransferState0) ->
    #{
        ?base_path := BasePath,
        ?inner_transfer := #partitioned_transfer{container = Container0, state = PartSt0} = Inner0,
        ?partition_spec := PartSpec,
        ?s3_client := S3Client,
        ?write_uuid := WriteUUID
    } = TransferState0,
    {IOMap, WriteMetadata, Container} =
        emqx_bridge_s3tables_aggreg_partitioned:fill(Records, Container0),
    PartSt =
        maps:fold(
            fun
                (PK, IOData, AccSt0) when is_map_key(PK, AccSt0) ->
                    #{PK := St0} = AccSt0,
                    #{
                        ?data_size := S0,
                        ?num_records := N0,
                        ?s3_transfer_state := S3TransferState0
                    } = St0,
                    #{PK := #{num_records := N1}} = WriteMetadata,
                    S1 = iolist_size(IOData),
                    {ok, S3TransferState} = emqx_s3_upload:append(IOData, S3TransferState0),
                    St = St0#{
                        ?data_size := S0 + S1,
                        ?num_records := N0 + N1,
                        ?s3_transfer_state := S3TransferState
                    },
                    AccSt0#{PK := St};
                (PK, IOData, AccSt0) ->
                    %% New partition key
                    #{PK := #{num_records := N0}} = WriteMetadata,
                    N = map_size(AccSt0),
                    Segments =
                        emqx_bridge_s3tables_logic:partition_keys_to_segments(
                            PK,
                            PartSpec
                        ),
                    {DataFileKey, S3TransferState0} =
                        init_inner_s3_transfer_state(BasePath, Segments, N, S3Client, WriteUUID),
                    S1 = iolist_size(IOData),
                    {ok, S3TransferState} = emqx_s3_upload:append(IOData, S3TransferState0),
                    St = #{
                        ?data_file_key => DataFileKey,
                        ?data_size => S1,
                        ?num_records => N0,
                        ?s3_transfer_state => S3TransferState
                    },
                    AccSt0#{PK => St}
            end,
            PartSt0,
            IOMap
        ),
    TransferState0#{
        ?inner_transfer := Inner0#partitioned_transfer{
            container = Container,
            state = PartSt
        }
    }.

-spec process_write(transfer_state()) ->
    {ok, transfer_state()} | {error, term()}.
process_write(#{?inner_transfer := #single_transfer{}} = TransferState0) ->
    #{
        ?inner_transfer := #single_transfer{state = S3TransferState0} = Inner0
    } = TransferState0,
    case emqx_s3_upload:write(S3TransferState0) of
        {ok, S3TransferState} ->
            TransferState = TransferState0#{
                ?inner_transfer := Inner0#single_transfer{state = S3TransferState}
            },
            {ok, TransferState};
        {cont, S3TransferState} ->
            TransferState = TransferState0#{
                ?inner_transfer := Inner0#single_transfer{state = S3TransferState}
            },
            process_write(TransferState);
        {error, Reason} ->
            _ = emqx_s3_upload:abort(S3TransferState0),
            {error, Reason}
    end;
process_write(#{?inner_transfer := #partitioned_transfer{}} = TransferState0) ->
    #{
        ?inner_transfer := #partitioned_transfer{state = ParSt0}
    } = TransferState0,
    PKs = maps:keys(ParSt0),
    do_process_write_partitioned(PKs, TransferState0).

-spec process_complete(transfer_state()) ->
    {ok, term()} | no_return().
process_complete(#{?inner_transfer := #single_transfer{}} = TransferState0) ->
    #{
        ?inner_transfer := #single_transfer{state = S3TransferState}
    } = TransferState0,
    case emqx_s3_upload:complete(S3TransferState) of
        {ok, _S3Completed} ->
            ?tp("s3tables_upload_manifests_enter", #{}),
            upload_manifests(TransferState0);
        {error, Reason} ->
            _ = emqx_s3_upload:abort(S3TransferState),
            exit({upload_failed, {data_file, Reason}})
    end;
process_complete(#{?inner_transfer := #partitioned_transfer{}} = TransferState0) ->
    #{
        ?inner_transfer := #partitioned_transfer{state = ParSt0}
    } = TransferState0,
    PKs = maps:keys(ParSt0),
    do_process_complete_partitioned(PKs, TransferState0).

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

bin(X) -> emqx_utils_conv:bin(X).

-spec uuid4() -> binary().
uuid4() ->
    %% Note: this useless `bin` is to trick dialyzer due to incorrect typespec....
    bin(uuid:uuid_to_string(uuid:get_v4(), binary_standard)).

mk_container_opts_and_inner_transfer_state(#unpartitioned{}, Opts) ->
    #{
        ?avro_schema := AvroSchema,
        ?base_path := BasePath,
        ?s3_client := S3Client,
        ?write_uuid := WriteUUID
    } = Opts,
    {DataFileKey, S3TransferState} =
        init_inner_s3_transfer_state(BasePath, [], 0, S3Client, WriteUUID),
    InnerContainerOpts = #{
        schema => AvroSchema,
        root_type => ?ROOT_AVRO_TYPE
    },
    Container = emqx_bridge_s3tables_aggreg_avro:new(InnerContainerOpts),
    %% We manage the container here, since we require extra metadata that the
    %% `emqx_connector_aggreg_container` API does not provide.
    ContainerOpts = #{type => noop},
    InnerTransferState = #single_transfer{
        container = Container,
        data_file_key = DataFileKey,
        data_size = 0,
        num_records = 0,
        state = S3TransferState
    },
    {ContainerOpts, InnerTransferState};
mk_container_opts_and_inner_transfer_state(#partitioned{fields = PartitionFields}, Opts) ->
    #{
        ?avro_schema := AvroSchema
    } = Opts,
    InnerContainerOpts = #{
        inner_container_opts => #{
            type => avro,
            schema => AvroSchema,
            root_type => ?ROOT_AVRO_TYPE
        },
        partition_fields => PartitionFields
    },
    Container = emqx_bridge_s3tables_aggreg_partitioned:new(InnerContainerOpts),
    %% We manage the container here, since we require extra metadata that the
    %% `emqx_connector_aggreg_container` API does not provide.
    ContainerOpts = #{type => noop},
    InnerTransferState = #partitioned_transfer{
        container = Container,
        state = #{}
    },
    {ContainerOpts, InnerTransferState}.

init_inner_s3_transfer_state(BasePath, Segments, N, S3Client, WriteUUID) ->
    %% fixme: might need fixing if using partitions and if
    %% `write.object-storage.partitioned-paths` is true (default is true).....
    DataFileKey = mk_data_file_key(BasePath, Segments, N, WriteUUID, "avro"),
    %% todo: make configurable?
    S3UploaderConfig = #{
        min_part_size => 5 * ?MEGABYTE,
        max_part_size => 5 * ?GIGABYTE
    },
    S3UploadOpts = #{},
    S3TransferState = emqx_s3_upload:new(S3Client, DataFileKey, S3UploadOpts, S3UploaderConfig),
    {DataFileKey, S3TransferState}.

do_process_complete_partitioned([PK | PKs], TransferState0) ->
    #{?inner_transfer := #partitioned_transfer{} = ParSt0} = TransferState0,
    #partitioned_transfer{state = #{PK := #{?s3_transfer_state := S3TransferState0}} = St0} =
        ParSt0,
    case emqx_s3_upload:complete(S3TransferState0) of
        {ok, _S3Completed} ->
            do_process_complete_partitioned(PKs, TransferState0);
        {error, Reason} ->
            maps:foreach(
                fun(_PK, #{?s3_transfer_state := S3TransferState}) ->
                    _ = emqx_s3_upload:abort(S3TransferState)
                end,
                St0
            ),
            %% TODO: prettify partition key
            exit({upload_failed, {data_file, PK, Reason}})
    end;
do_process_complete_partitioned([], TransferState0) ->
    ?tp("s3tables_upload_manifests_enter", #{}),
    upload_manifests(TransferState0).

-spec load_previous_manifest_file(emqx_s3_client:client(), map()) ->
    {ok, [iodata()]} | {error, term()}.
load_previous_manifest_file(S3Client, LoadedTable) ->
    maybe
        #{<<"manifest-list">> := ManifestListLocation} ?=
            emqx_bridge_s3tables_logic:find_current_snapshot(LoadedTable, no_snapshot),
        #{base_path := Key} = parse_location(ManifestListLocation),
        %% TODO: retry on errors??
        {ok, #{content := PrevManifestBin}} ?= emqx_s3_client:get_object(S3Client, Key),
        #{schema := ManifestFileSc} = persistent_term:get(?MANIFEST_FILE_PT_KEY),
        BlocksBin = emqx_bridge_s3tables_logic:fix_previous_manifest_files(
            PrevManifestBin, ManifestFileSc
        ),
        {ok, BlocksBin}
    else
        no_snapshot -> {ok, []};
        Error -> Error
    end.

parse_location(Location) ->
    #{authority := #{host := Bucket}, path := BasePath0} = emqx_utils_uri:parse(Location),
    BasePath =
        case BasePath0 of
            <<"/", BP/binary>> -> BP;
            _ -> BasePath0
        end,
    #{bucket => Bucket, base_path => bin(BasePath)}.

do_process_write_partitioned([PK | PKs], TransferState0) ->
    #{?inner_transfer := #partitioned_transfer{} = ParSt0} = TransferState0,
    #partitioned_transfer{state = #{PK := #{?s3_transfer_state := S3TransferState0} = InSt0} = St0} =
        ParSt0,
    case emqx_s3_upload:write(S3TransferState0) of
        {ok, S3TransferState} ->
            InSt = InSt0#{?s3_transfer_state := S3TransferState},
            St = St0#{PK := InSt},
            ParSt = ParSt0#partitioned_transfer{state = St},
            TransferState = TransferState0#{?inner_transfer := ParSt},
            do_process_write_partitioned(PKs, TransferState);
        {cont, S3TransferState} ->
            InSt = InSt0#{?s3_transfer_state := S3TransferState},
            St = St0#{PK := InSt},
            ParSt = ParSt0#partitioned_transfer{state = St},
            TransferState = TransferState0#{?inner_transfer := ParSt},
            do_process_write_partitioned([PK | PKs], TransferState);
        {error, Reason} ->
            maps:foreach(
                fun(_PK, #{?s3_transfer_state := S3TransferState}) ->
                    _ = emqx_s3_upload:abort(S3TransferState)
                end,
                St0
            ),
            %% TODO: prettify partition key
            {error, {PK, Reason}}
    end;
do_process_write_partitioned([], TransferState) ->
    {ok, TransferState}.

-spec upload_manifests(transfer_state()) -> {ok, term()} | no_return().
upload_manifests(TransferState) ->
    #{
        ?action_res_id := ActionResId,
        ?base_path := BasePath,
        ?bucket := Bucket,
        ?client := Client,
        ?loaded_table := LoadedTable,
        ?n_attempt := NAttempt,
        ?namespace := Namespace,
        ?partition_spec_id := PartitionSpecId,
        ?s3_client := S3Client,
        ?schema_id := SchemaId,
        ?seq_num := SeqNum,
        ?table := Table,
        ?table_uuid := TableUUID,
        ?write_uuid := WriteUUID
    } = TransferState,

    NewSnapshotId = gen_snapshot_id(),

    #{
        key := ManifestEntryKey,
        size := ManifestSize,
        num_records := NumRecords
    } = upload_manifest_entries(NewSnapshotId, TransferState),

    #{
        schema := ManifestFileSc,
        header := ManifestFileHeader
    } = persistent_term:get(?MANIFEST_FILE_PT_KEY),

    ManifestEntryS3Path = make_s3_path(Bucket, ManifestEntryKey),
    ManifestFile = #{
        <<"manifest_path">> => ManifestEntryS3Path,
        <<"manifest_length">> => ManifestSize,
        <<"partition_spec_id">> => PartitionSpecId,
        <<"content">> => ?MANIFEST_LIST_CONTENT_DATA,
        <<"sequence_number">> => SeqNum,
        %% TODO: check all manifests, including ones not written here?
        <<"min_sequence_number">> => SeqNum,
        <<"added_snapshot_id">> => NewSnapshotId,
        <<"added_files_count">> => 1,
        <<"existing_files_count">> => 0,
        <<"deleted_files_count">> => 0,
        <<"added_rows_count">> => NumRecords,
        <<"existing_rows_count">> => 0,
        <<"deleted_rows_count">> => 0,
        %% TODO: apparently not strictly required, but contains statistics for readers to
        %% plan stuff...
        <<"partitions">> => []
    },
    %% TODO: handle errors, retries...
    {ok, PrevManifestListBin} = load_previous_manifest_file(S3Client, LoadedTable),
    ManifestFileBin0 = avro_binary_encoder:encode(
        ManifestFileSc, <<"manifest_file">>, ManifestFile
    ),
    ManifestFileBin = [ManifestFileBin0 | PrevManifestListBin],
    ManifestFileOCF = avro_ocf:make_ocf(ManifestFileHeader, ManifestFileBin),
    ManifestFileKey = mk_manifest_file_key(BasePath, NewSnapshotId, WriteUUID, NAttempt),
    %% TODO: handle errors
    ?SLOG(info, #{
        msg => "s3tables_uploading_manifest_list",
        action => ActionResId,
        key => ManifestFileKey,
        snapshot_id => NewSnapshotId,
        record_count => NumRecords
    }),
    ok = emqx_s3_client:put_object(S3Client, ManifestFileKey, ManifestFileOCF),

    ManifestFileS3Path = make_s3_path(Bucket, ManifestFileKey),
    CommitContext = #{
        data_size => ManifestSize,
        num_records => NumRecords,
        loaded_table => LoadedTable,
        manifest_file_path => ManifestFileS3Path,
        namespace => split_ns(Namespace),
        new_snapshot_id => NewSnapshotId,
        now_ms => now_ms(),
        schema_id => SchemaId,
        seq_num => SeqNum,
        table => Table,
        table_uuid => TableUUID
    },
    Request = emqx_bridge_s3tables_logic:compute_update_table_request(CommitContext),
    %% todo: will need to abstract this once we support more locations...
    %% TODO: handle errors
    ?tp("s3tables_about_to_commit", #{}),
    Response = emqx_bridge_s3tables_client_s3t:update_table(Client, Namespace, Table, Request),
    case Response of
        {ok, Result} ->
            {ok, Result};
        {error, conflict} ->
            ?SLOG(info, #{
                msg => "s3tables_commit_conflict",
                action => ActionResId,
                snapshot_id => NewSnapshotId,
                num_attempt => NAttempt,
                write_uuid => WriteUUID
            }),
            retry_upload_manifests(TransferState);
        {error, Reason} ->
            exit({upload_failed, {commit, Reason}})
    end.

retry_upload_manifests(TransferState0) ->
    #{
        ?client := Client,
        ?n_attempt := NAttempt0,
        ?namespace := Namespace,
        ?table := Table
    } = TransferState0,
    NAttempt = NAttempt0 + 1,
    case NAttempt > ?MAX_RETRY_ATTEMPTS of
        true ->
            exit({upload_failed, {commit, <<"too many conflicts; retries exhausted">>}});
        false ->
            ok
    end,
    SleepMS = rand:uniform((1 bsl NAttempt) * 100),
    timer:sleep(SleepMS),
    %% TODO: handle transient errors...
    {ok,
        #{
            <<"metadata">> := #{
                <<"last-sequence-number">> := LastSeqNum
            }
        } = LoadedTable} = load_table(Client, Namespace, Table),
    %% TODO: check if schema and partitions changed since start...
    {ok, _} = emqx_bridge_s3tables_logic:parse_loaded_table(LoadedTable),
    TransferState = TransferState0#{
        ?loaded_table := LoadedTable,
        ?n_attempt := NAttempt,
        ?seq_num := LastSeqNum + 1
    },
    upload_manifests(TransferState).

upload_manifest_entries(NewSnapshotId, #{?inner_transfer := #single_transfer{}} = TransferState) ->
    #{
        ?iceberg_schema := IcebergSchema,
        ?inner_transfer := #single_transfer{
            data_size = DataSize,
            data_file_key = DataFileKey,
            num_records = NumRecords
        },
        ?partition_spec := #unpartitioned{} = PartSpec
    } = TransferState,
    {ManifestEntryHeader, ManifestEntrySc} =
        manifest_entry_avro_schema(PartSpec, IcebergSchema),
    %% No partition keys
    PKs = [],
    DataFiles = #{
        PKs => #{
            ?data_file_key => DataFileKey,
            ?data_size => DataSize,
            ?num_records => NumRecords
        }
    },
    PartitionFields = [],
    do_upload_manifest_entries(
        NewSnapshotId,
        DataFiles,
        PartitionFields,
        ManifestEntryHeader,
        ManifestEntrySc,
        TransferState
    );
upload_manifest_entries(
    NewSnapshotId, #{?inner_transfer := #partitioned_transfer{}} = TransferState
) ->
    #{
        ?iceberg_schema := IcebergSchema,
        ?inner_transfer := #partitioned_transfer{state = PartSt},
        ?partition_spec := #partitioned{fields = PartitionFields} = PartSpec
    } = TransferState,
    {ManifestEntryHeader, ManifestEntrySc} =
        manifest_entry_avro_schema(PartSpec, IcebergSchema),
    do_upload_manifest_entries(
        NewSnapshotId, PartSt, PartitionFields, ManifestEntryHeader, ManifestEntrySc, TransferState
    ).

do_upload_manifest_entries(
    NewSnapshotId, DataFiles, PartitionFields, ManifestEntryHeader, ManifestEntrySc, TransferState
) ->
    #{
        ?action_res_id := ActionResId,
        ?base_path := BasePath,
        ?bucket := Bucket,
        ?n_attempt := NAttempt,
        ?s3_client := S3Client,
        ?write_uuid := WriteUUID
    } = TransferState,
    {ManifestEntriesBins, NumRecords} =
        maps:fold(
            fun(PKs, St, {AccBin, AccN}) ->
                #{
                    ?data_file_key := DataFileKey,
                    ?data_size := DataSize,
                    ?num_records := NumRecords
                } = St,
                %% todo: will need to abstract this once we support more locations...
                DataS3Path = make_s3_path(Bucket, DataFileKey),
                Partition = partition_keys_to_record(PKs, PartitionFields),
                ManifestEntry = #{
                    <<"status">> => ?MANIFEST_ENTRY_STATUS_ADDED,
                    <<"snapshot_id">> => NewSnapshotId,
                    <<"data_file">> => #{
                        <<"content">> => ?DATA_FILE_CONTENT_DATA,
                        <<"partition">> => Partition,
                        <<"file_path">> => DataS3Path,
                        <<"file_format">> => ?DATA_FILE_FORMAT_AVRO,
                        <<"record_count">> => NumRecords,
                        <<"file_size_in_bytes">> => DataSize
                    }
                },
                ManifestEntryBin = avro_binary_encoder:encode(
                    ManifestEntrySc, <<"manifest_entry">>, ManifestEntry
                ),
                {[ManifestEntryBin | AccBin], AccN + NumRecords}
            end,
            {[], 0},
            DataFiles
        ),
    ManifestEntryKey = mk_manifest_entry_key(BasePath, NAttempt, WriteUUID),
    ManifestEntryOCF = avro_ocf:make_ocf(ManifestEntryHeader, ManifestEntriesBins),
    %% TODO: handle errors
    ?SLOG(info, #{
        msg => "s3tables_uploading_manifest_entry",
        action => ActionResId,
        key => ManifestEntryKey,
        snapshot_id => NewSnapshotId,
        record_count => NumRecords
    }),
    ok = emqx_s3_client:put_object(S3Client, ManifestEntryKey, ManifestEntryOCF),
    ManifestSize = iolist_size(ManifestEntryOCF),
    #{
        key => ManifestEntryKey,
        size => ManifestSize,
        num_records => NumRecords
    }.

partition_keys_to_record(PKs, PartitionFields) ->
    lists:foldl(
        fun({PK, #{name := Name}}, Acc) ->
            Acc#{Name => PK}
        end,
        #{},
        lists:zip(PKs, PartitionFields)
    ).

-spec make_key(binary(), [binary() | string()]) -> binary().
make_key(<<"">>, Segments) ->
    filename:join(Segments);
make_key(BasePath, Segments) ->
    filename:join([BasePath | Segments]).

mk_data_file_key(BasePath, ExtraSegments, N, WriteUUID, Ext) ->
    K = iolist_to_binary(["00000-", bin(N), "-", WriteUUID, ".", Ext]),
    make_key(BasePath, ["data"] ++ ExtraSegments ++ [K]).

%% N.B.: in the original pyiceberg implementation, there's no attempt number in the
%% filename.  However, when running against real s3tables, it has the (as far as I know)
%% undocumented behavior which is equivalent to having an implicit `If-None-Match` header
%% in the `PutObject` request when uploading metadata, which means that retrying to upload
%% the manifest entries/lists with the exact same key always fails.  Attempting to delete
%% the already uploaded file is also forbidden, apparently.  So, to circumvent that, we
%% just add the attempt number to the filename...
mk_manifest_entry_key(BasePath, NAttempt, WriteUUID) ->
    K = iolist_to_binary([WriteUUID, "-", bin(NAttempt), "-m0.avro"]),
    make_key(BasePath, ["metadata", K]).

mk_manifest_file_key(BasePath, SnapshotId, WriteUUID, NAttempt) ->
    K = iolist_to_binary(["snap-", bin(SnapshotId), "-", bin(NAttempt), "-", WriteUUID, ".avro"]),
    make_key(BasePath, ["metadata", K]).

make_s3_path(Bucket, Key) ->
    %% Key must not begin with `/`...
    iolist_to_binary([<<"s3://">>, Bucket, <<"/">>, Key]).

-spec gen_snapshot_id() -> integer().
gen_snapshot_id() ->
    <<MSB:64, LSB:64>> = uuid:get_v4(),
    (MSB bxor LSB) band ?LONG_MAX_VALUE.

now_ms() ->
    erlang:system_time(millisecond).

split_ns(Namespace) ->
    binary:split(Namespace, [<<".">>], [global, trim_all]).

manifest_entry_avro_schema(PartSpec, IcebergSchema) ->
    #{json_schema := AvroSchemaJSON} = persistent_term:get(?MANIFEST_ENTRY_PT_KEY),
    emqx_bridge_s3tables_logic:manifest_entry_avro_schema(PartSpec, AvroSchemaJSON, IcebergSchema).
