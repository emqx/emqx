-module(emqx_bridge_iceberg_impl).

-feature(maybe_expr, enable).

-behaviour(emqx_resource).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx/include/emqx_trace.hrl").
-include("emqx_bridge_iceberg.hrl").
-include_lib("emqx_connector_aggregator/include/emqx_connector_aggregator.hrl").

%% `emqx_resource' API
-export([
    resource_type/0,
    callback_mode/0,

    on_start/2,
    on_stop/2,
    on_get_status/2,

    on_get_channels/1,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channel_status/3,

    on_query/3,
    on_batch_query/3
]).

%% `emqx_connector_aggreg_delivery' API
-export([
    init_transfer_state_and_container_opts/2,
    process_append/3,
    process_write/1,
    process_complete/1
]).

%% API
-export([
    load_and_memoize_schema_files/0,
    forget_schema_files/0
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(AGGREG_SUP, emqx_bridge_iceberg_sup).

%% Allocatable resources

%% Connector fields
-define(installed_channels, installed_channels).
-define(client, client).
-define(location_client, location_client).

%% Channel fields
-define(aggreg_id, aggreg_id).
-define(supervisor, supervisor).

%% Aggregated delivery / transfer state fields
-define(action_res_id, action_res_id).
-define(base_path, base_path).
-define(bucket, bucket).
-define(data_file_key, data_file_key).
-define(data_size, data_size).
-define(inner_transfer, inner_transfer).
-define(n_attempt, n_attempt).
-define(namespace, namespace).
-define(s3_client, s3_client).
-define(s3_client_config, s3_client_config).
-define(s3_transfer_state, s3_transfer_state).
-define(schema_id, schema_id).
-define(seq_num, seq_num).
-define(table, table).
-define(table_uuid, table_uuid).
-define(write_uuid, write_uuid).

-define(MEGABYTE, 1024 * 1024).
-define(GIGABYTE, 1024 * ?MEGABYTE).
%% 2^63 - 1
-define(LONG_MAX_VALUE, 9223372036854775807).

-define(MAX_RETRY_ATTEMPTS, 10).

%% Manifest constants
-define(DATA_FILE_CONTENT_DATA, 0).
-define(DATA_FILE_FORMAT_AVRO, <<"AVRO">>).
-define(MANIFEST_ENTRY_STATUS_ADDED, 1).
-define(MANIFEST_LIST_CONTENT_DATA, 0).

-define(MANIFEST_ENTRY_PT_KEY, {?MODULE, manifest_entry}).
-define(MANIFEST_FILE_PT_KEY, {?MODULE, manifest_file}).

-record(single_transfer, {data_file_key, data_size = 0, num_records = 0, state}).
-record(partitioned_transfer, {state}).

-type connector_config() :: #{}.
-type connector_state() :: #{
    ?client := iceberg_client(),
    ?installed_channels := #{channel_id() => channel_state()},
    ?location_client := location_client()
}.

-type channel_config() :: #{
    parameters := #{}
}.
-type channel_state() :: #{
    ?aggreg_id := aggreg_id(),
    ?supervisor := pid()
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
    ?partition_spec := emqx_bridge_iceberg_logic:partition_spec_parsed(),
    ?partition_spec_id := integer(),
    ?s3_client := emqx_s3_client:client(),
    ?schema_id := integer(),
    ?seq_num := integer(),
    ?table := table_name(),
    ?table_uuid := binary(),
    ?write_uuid := binary()
}.

-type query() :: {_Tag :: channel_id(), _Data :: map()}.

-type aggreg_id() :: {binary(), binary()}.

-type iceberg_client() :: emqx_bridge_iceberg_client_s3t:t().
-type location_client() :: map().

-type write_metadata() :: emqx_connector_aggreg_container:write_metadata().

-type namespace() :: binary().
-type table_name() :: binary().

-type partition_key() :: emqx_bridge_iceberg_aggreg_partitioned:partition_key().

%%------------------------------------------------------------------------------
%% `emqx_resource' API
%%------------------------------------------------------------------------------

-spec resource_type() -> atom().
resource_type() ->
    iceberg.

-spec callback_mode() -> callback_mode().
callback_mode() ->
    always_sync.

-spec on_start(connector_resource_id(), connector_config()) ->
    {ok, connector_state()} | {error, _Reason}.
on_start(ConnResId, ConnConfig) ->
    maybe
        {ok, Client} ?= make_client(ConnConfig),
        {ok, LocClient} ?= init_location_client(ConnResId, ConnConfig),
        ConnState = #{
            ?client => Client,
            ?location_client => LocClient,
            ?installed_channels => #{}
        },
        {ok, ConnState}
    end.

-spec on_stop(connector_resource_id(), connector_state()) -> ok.
on_stop(ConnResId, _ConnState) ->
    Res = emqx_s3_client_http:stop_pool(ConnResId),
    ?tp("iceberg_connector_stop", #{instance_id => ConnResId}),
    Res.

-spec on_get_status(connector_resource_id(), connector_state()) ->
    ?status_connected | {?status_disconnected, term()}.
on_get_status(
    _ConnResId, #{?location_client := #{?s3_client_config := S3ClientConfig}} = _ConnState
) ->
    emqx_bridge_s3_connector:do_on_get_status(S3ClientConfig).

-spec on_get_channels(connector_resource_id()) ->
    [{action_resource_id(), channel_config()}].
on_get_channels(ConnResId) ->
    emqx_bridge_v2:get_channels_for_connector(ConnResId).

-spec on_add_channel(
    connector_resource_id(),
    connector_state(),
    action_resource_id(),
    channel_config()
) ->
    {ok, connector_state()}.
on_add_channel(_ConnResId, ConnState0, ChanResId, ActionConfig) ->
    maybe
        {ok, ChanState} ?= create_channel(ChanResId, ActionConfig, ConnState0),
        ConnState = emqx_utils_maps:deep_put(
            [?installed_channels, ChanResId], ConnState0, ChanState
        ),
        {ok, ConnState}
    end.

-spec on_remove_channel(
    connector_resource_id(),
    connector_state(),
    action_resource_id()
) ->
    {ok, connector_state()}.
on_remove_channel(
    _ConnResId, ConnState0 = #{?installed_channels := InstalledChans0}, ChanResId
) when
    is_map_key(ChanResId, InstalledChans0)
->
    {ChanState, InstalledChans} = maps:take(ChanResId, InstalledChans0),
    ok = destroy_channel(ChanState),
    ConnState = ConnState0#{?installed_channels := InstalledChans},
    {ok, ConnState};
on_remove_channel(_ConnResId, ConnState, _ChanResId) ->
    {ok, ConnState}.

-spec on_get_channel_status(
    connector_resource_id(),
    action_resource_id(),
    connector_state()
) ->
    ?status_connected | ?status_disconnected.
on_get_channel_status(
    ConnResId,
    ChanResId,
    _ConnState = #{?installed_channels := InstalledChans}
) when is_map_key(ChanResId, InstalledChans) ->
    ChanState = maps:get(ChanResId, InstalledChans),
    channel_status(ConnResId, ChanResId, ChanState);
on_get_channel_status(_ConnResId, _ChanResId, _ConnState) ->
    ?status_disconnected.

-spec on_query(connector_resource_id(), query(), connector_state()) ->
    {ok, _Result} | {error, _Reason}.
on_query(
    _ConnResId,
    {ChanResId, #{} = Data},
    #{?installed_channels := InstalledChans} = _ConnState
) when
    is_map_key(ChanResId, InstalledChans)
->
    ChanState = maps:get(ChanResId, InstalledChans),
    run_aggregated_action([Data], ChanState);
on_query(_ConnResId, Query, _ConnState) ->
    {error, {unrecoverable_error, {invalid_query, Query}}}.

-spec on_batch_query(connector_resource_id(), [query()], connector_state()) ->
    ok | {error, term()}.
on_batch_query(
    _ConnResId,
    [{ChanResId, _Data} | _] = Queries,
    #{?installed_channels := InstalledChans} = _ConnState
) when
    is_map_key(ChanResId, InstalledChans)
->
    ChanState = maps:get(ChanResId, InstalledChans),
    Batch = [Data || {_, Data} <- Queries],
    run_aggregated_action(Batch, ChanState);
on_batch_query(_ConnResId, Queries, _ConnectorState) ->
    {error, {unrecoverable_error, {invalid_batch, Queries}}}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec load_and_memoize_schema_files() -> ok.
load_and_memoize_schema_files() ->
    {ok, ScJSON} = file:read_file(
        filename:join([code:lib_dir(emqx_bridge_iceberg), "priv", "manifest-file.avsc"])
    ),
    Sc = avro:decode_schema(ScJSON),
    Header = avro_ocf:make_header(Sc),
    persistent_term:put(?MANIFEST_FILE_PT_KEY, #{schema => Sc, header => Header}),
    %% Entry schema varies with partition spec.
    {ok, EntryScJSONRaw} = file:read_file(
        filename:join([code:lib_dir(emqx_bridge_iceberg), "priv", "manifest-entry.avsc"])
    ),
    EntryScJSON = emqx_utils_json:decode(EntryScJSONRaw),
    persistent_term:put(?MANIFEST_ENTRY_PT_KEY, #{json_schema => EntryScJSON}),
    ok.

-spec forget_schema_files() -> ok.
forget_schema_files() ->
    _ = persistent_term:erase(?MANIFEST_FILE_PT_KEY),
    _ = persistent_term:erase(?MANIFEST_ENTRY_PT_KEY),
    ok.

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

-spec process_append(
    iodata() | #{[partition_key()] => iodata()}, write_metadata(), transfer_state()
) ->
    transfer_state().
process_append(IOData, WriteMetadata, #{?inner_transfer := #single_transfer{}} = TransferState0) ->
    #{num_records := N1} = WriteMetadata,
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
            data_size = S0 + S1,
            num_records = N0 + N1,
            state = S3TransferState
        }
    };
process_append(
    IOMap, WriteMetadata, #{?inner_transfer := #partitioned_transfer{}} = TransferState0
) ->
    #{
        ?base_path := BasePath,
        ?inner_transfer := #partitioned_transfer{state = PartSt0} = Inner0,
        ?partition_spec := PartSpec,
        ?s3_client := S3Client,
        ?write_uuid := WriteUUID
    } = TransferState0,
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
                        emqx_bridge_iceberg_logic:partition_keys_to_segments(
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
        ?inner_transfer := Inner0#partitioned_transfer{state = PartSt}
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
            ?tp("iceberg_upload_manifests_enter", #{}),
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

make_client(#{parameters := #{location_type := s3tables} = Params}) ->
    emqx_bridge_iceberg_client_s3t:new(Params).

init_location_client(ConnResId, #{parameters := #{location_type := s3tables} = Params}) ->
    maybe
        #{s3_client := S3Config0} = Params,
        S3Config = S3Config0#{url_expire_time => 0},
        S3ClientConfig = emqx_s3_profile_conf:client_config(S3Config, ConnResId),
        _ = emqx_s3_client_http:stop_pool(ConnResId),
        ok ?= emqx_s3_client_http:start_pool(ConnResId, S3Config),
        {ok, #{?s3_client_config => S3ClientConfig}}
    end.

run_aggregated_action(Batch, #{?aggreg_id := AggregId}) ->
    Timestamp = erlang:system_time(second),
    case emqx_connector_aggregator:push_records(AggregId, Timestamp, Batch) of
        ok ->
            ok;
        {error, Reason} ->
            {error, {unrecoverable_error, Reason}}
    end.

create_channel(ChanResId, ActionConfig, ConnState) ->
    #{
        ?client := Client,
        ?location_client := LocClient
    } = ConnState,
    #{
        bridge_name := Name,
        parameters := #{
            aggregation := #{
                max_records := MaxRecords,
                time_interval := TimeInterval
            },
            namespace := Namespace,
            table := Table
        }
    } = ActionConfig,
    Type = ?ACTION_TYPE_BIN,
    AggregId = {Type, Name},
    WorkDir = work_dir(Type, Name),
    AggregOpts = #{
        max_records => MaxRecords,
        time_interval => TimeInterval,
        work_dir => WorkDir
    },
    TransferOpts = #{
        action => Name,
        action_res_id => ChanResId,
        ?location_client => LocClient,
        ?client => Client,
        ?namespace => Namespace,
        ?table => Table
    },
    DeliveryOpts = #{
        callback_module => ?MODULE,
        upload_options => TransferOpts
    },
    _ = ?AGGREG_SUP:delete_child(AggregId),
    maybe
        {ok, _} ?= validate_table(Client, Namespace, Table),
        {ok, SupPid} ?=
            ?AGGREG_SUP:start_child(#{
                id => AggregId,
                start =>
                    {emqx_connector_aggreg_upload_sup, start_link, [
                        AggregId, AggregOpts, DeliveryOpts
                    ]},
                type => supervisor,
                restart => permanent
            }),
        {ok, #{
            ?aggreg_id => AggregId,
            ?supervisor => SupPid
        }}
    else
        {error, not_found} ->
            {error, {unhealthy_target, <<"Namespace or table does not exist">>}};
        {error, timeout} ->
            {error, <<"Timeout loading table">>};
        {error, {unsupported_format_version, FormatVsn}} ->
            Msg = iolist_to_binary(
                io_lib:format("Table uses unsupported Iceberg format version: ~p", [FormatVsn])
            ),
            {error, {unhealthy_target, Msg}};
        {error, {unsupported_type, IceType}} ->
            IceTypeFormatted =
                case is_binary(IceType) of
                    true -> IceType;
                    false -> emqx_utils_json:encode(IceType)
                end,
            Msg = iolist_to_binary(
                io_lib:format("Schema contains unsupported data type: ~s", [IceTypeFormatted])
            ),
            {error, {unhealthy_target, Msg}};
        {error, schema_not_found} ->
            {error, {unhealthy_target, <<"Current schema could not be found">>}};
        {error, partition_spec_not_found} ->
            {error, {unhealthy_target, <<"Current partition spec could not be found">>}};
        {error, invalid_spec} ->
            {error, {unhealthy_target, <<"Partition spec is invalid">>}};
        Error ->
            Error
    end.

destroy_channel(ChanState) ->
    #{?aggreg_id := AggregId} = ChanState,
    ?AGGREG_SUP:delete_child(AggregId).

channel_status(_ConnResId, _ChanResId, ChanState) ->
    #{
        ?aggreg_id := AggregId
    } = ChanState,
    Timestamp = erlang:system_time(second),
    ok = emqx_connector_aggregator:tick(AggregId, Timestamp),
    maybe
        ok ?= check_aggreg_upload_errors(AggregId),
        ?status_connected
    end.

validate_table(Client, Namespace, Table) ->
    maybe
        {ok, LoadedTable} ?= load_table(Client, Namespace, Table),
        emqx_bridge_iceberg_logic:parse_loaded_table(LoadedTable)
    end.

work_dir(Type, Name) ->
    filename:join([emqx:data_dir(), bridge, Type, Name]).

-spec gen_snapshot_id() -> integer().
gen_snapshot_id() ->
    <<MSB:64, LSB:64>> = uuid:get_v4(),
    (MSB bxor LSB) band ?LONG_MAX_VALUE.

-spec make_key(binary(), [binary() | string()]) -> binary().
make_key(<<"">>, Segments) ->
    filename:join(Segments);
make_key(BasePath, Segments) ->
    filename:join([BasePath | Segments]).

parse_location(Location) ->
    #{authority := #{host := Bucket}, path := BasePath0} = emqx_utils_uri:parse(Location),
    BasePath =
        case BasePath0 of
            <<"/", BP/binary>> -> BP;
            _ -> BasePath0
        end,
    #{bucket => Bucket, base_path => bin(BasePath)}.

make_s3_path(Bucket, Key) ->
    %% Key must not begin with `/`...
    iolist_to_binary([<<"s3://">>, Bucket, <<"/">>, Key]).

split_ns(Namespace) ->
    binary:split(Namespace, [<<".">>], [global, trim_all]).

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
        msg => "iceberg_uploading_manifest_list",
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
    Request = emqx_bridge_iceberg_logic:compute_update_table_request(CommitContext),
    %% todo: will need to abstract this once we support more locations...
    %% TODO: handle errors
    ?tp("iceberg_about_to_commit", #{}),
    Response = emqx_bridge_iceberg_client_s3t:update_table(Client, Namespace, Table, Request),
    case Response of
        {ok, Result} ->
            {ok, Result};
        {error, conflict} ->
            ?SLOG(info, #{
                msg => "iceberg_commit_conflict",
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
    {ok, _} = emqx_bridge_iceberg_logic:parse_loaded_table(LoadedTable),
    TransferState = TransferState0#{
        ?loaded_table := LoadedTable,
        ?n_attempt := NAttempt,
        ?seq_num := LastSeqNum + 1
    },
    upload_manifests(TransferState).

%% TODO: will need to abstract this once we support more locations...
load_table(Client, Namespace, Table) ->
    emqx_bridge_iceberg_client_s3t:load_table(Client, Namespace, Table).

bin(X) -> emqx_utils_conv:bin(X).

-spec uuid4() -> binary().
uuid4() ->
    %% Note: this useless `bin` is to trick dialyzer due to incorrect typespec....
    bin(uuid:uuid_to_string(uuid:get_v4(), binary_standard)).

mk_data_file_key(BasePath, ExtraSegments, N, WriteUUID, Ext) ->
    K = iolist_to_binary(["00000-", bin(N), "-", WriteUUID, ".", Ext]),
    make_key(BasePath, ["data"] ++ ExtraSegments ++ [K]).

%% N.B.: in the original pyiceberg implementation, there's no attempt number in the
%% filename.  However, when running against real s3tables, it has the (as fas as I know)
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

now_ms() ->
    erlang:system_time(millisecond).

-spec load_previous_manifest_file(emqx_s3_client:client(), map()) ->
    {ok, [iodata()]} | {error, term()}.
load_previous_manifest_file(S3Client, LoadedTable) ->
    maybe
        #{<<"manifest-list">> := ManifestListLocation} ?=
            emqx_bridge_iceberg_logic:find_current_snapshot(LoadedTable, no_snapshot),
        #{base_path := Key} = parse_location(ManifestListLocation),
        %% TODO: retry on errors??
        {ok, #{content := PrevManifestBin}} ?= emqx_s3_client:get_object(S3Client, Key),
        #{schema := ManifestFileSc} = persistent_term:get(?MANIFEST_FILE_PT_KEY),
        BlocksBin = emqx_bridge_iceberg_logic:prepare_previous_manifest_files(
            PrevManifestBin, ManifestFileSc
        ),
        {ok, BlocksBin}
    else
        no_snapshot -> {ok, []};
        Error -> Error
    end.

check_aggreg_upload_errors(AggregId) ->
    case emqx_connector_aggregator:take_error(AggregId) of
        [Error] ->
            %% This approach means that, for example, 3 upload failures will cause
            %% the channel to be marked as unhealthy for 3 consecutive health checks.
            {?status_disconnected, {unhealthy_target, emqx_s3_utils:map_error_details(Error)}};
        [] ->
            ok
    end.

mk_container_opts_and_inner_transfer_state(#unpartitioned{}, Opts) ->
    #{
        ?avro_schema := AvroSchema,
        ?base_path := BasePath,
        ?s3_client := S3Client,
        ?write_uuid := WriteUUID
    } = Opts,
    {DataFileKey, S3TransferState} =
        init_inner_s3_transfer_state(BasePath, [], 0, S3Client, WriteUUID),
    ContainerOpts = #{
        type => avro,
        schema => AvroSchema,
        root_type => ?ROOT_AVRO_TYPE
    },
    InnerTransferState = #single_transfer{
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
    PartContOpts = #{
        inner_container_opts => #{
            type => avro,
            schema => AvroSchema,
            root_type => ?ROOT_AVRO_TYPE
        },
        partition_fields => PartitionFields
    },
    ContainerOpts = #{
        type => custom,
        module => emqx_bridge_iceberg_aggreg_partitioned,
        opts => PartContOpts
    },
    InnerTransferState = #partitioned_transfer{
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
    ?tp("iceberg_upload_manifests_enter", #{}),
    upload_manifests(TransferState0).

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
        msg => "iceberg_uploading_manifest_entry",
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

manifest_entry_avro_schema(PartSpec, IcebergSchema) ->
    #{json_schema := AvroSchemaJSON} = persistent_term:get(?MANIFEST_ENTRY_PT_KEY),
    emqx_bridge_iceberg_logic:manifest_entry_avro_schema(PartSpec, AvroSchemaJSON, IcebergSchema).
