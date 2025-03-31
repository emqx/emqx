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
-define(base_path, base_path).
-define(bucket, bucket).
-define(data_file_key, data_file_key).
-define(data_size, data_size).
-define(loaded_table, loaded_table).
-define(n_attempt, n_attempt).
-define(namespace, namespace).
-define(num_records, num_records).
-define(s3_client, s3_client).
-define(s3_client_config, s3_client_config).
-define(s3_transfer_state, s3_transfer_state).
-define(schema_id, schema_id).
-define(seq_num, seq_num).
-define(table, table).
-define(table_uuid, table_uuid).
-define(write_uuid, write_uuid).

-define(ROOT_SC_TYPE, <<"root">>).
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
        ?client := iceberg_client(),
        ?location_client := location_client(),
        ?namespace := namespace(),
        ?table := table_name()
    }
}.

-type transfer_state() :: #{
    ?base_path := binary(),
    ?bucket := binary(),
    ?client := iceberg_client(),
    ?data_file_key := binary(),
    ?data_size := non_neg_integer(),
    ?loaded_table := map(),
    ?n_attempt := non_neg_integer(),
    ?namespace := namespace(),
    ?num_records := non_neg_integer(),
    ?s3_client := emqx_s3_client:client(),
    ?s3_transfer_state := emqx_s3_upload:t(),
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
    do_load_and_memoize_schema_files(?MANIFEST_ENTRY_PT_KEY, "manifest-entry.avsc"),
    do_load_and_memoize_schema_files(?MANIFEST_FILE_PT_KEY, "manifest-file.avsc"),
    ok.

do_load_and_memoize_schema_files(PTKey, File) ->
    {ok, ScJSON} = file:read_file(
        filename:join([code:lib_dir(emqx_bridge_iceberg), "priv", File])
    ),
    Sc = avro:decode_schema(ScJSON),
    Header = avro_ocf:make_header(Sc),
    persistent_term:put(PTKey, #{schema => Sc, header => Header}).

-spec forget_schema_files() -> ok.
forget_schema_files() ->
    _ = persistent_term:erase(?MANIFEST_ENTRY_PT_KEY),
    _ = persistent_term:erase(?MANIFEST_FILE_PT_KEY),
    ok.

%%------------------------------------------------------------------------------
%% `emqx_connector_aggreg_delivery' API
%%------------------------------------------------------------------------------

-spec init_transfer_state_and_container_opts(buffer(), transfer_opts()) ->
    {ok, transfer_state(), map()} | {error, term()}.
init_transfer_state_and_container_opts(_Buffer, Opts) ->
    #{
        upload_options := #{
            ?client := Client,
            ?location_client := #{?s3_client_config := S3ClientConfig},
            ?namespace := Namespace,
            ?table := Table
        }
    } = Opts,
    maybe
        {ok,
            #{
                <<"metadata">> := #{
                    <<"current-schema-id">> := SchemaId,
                    <<"last-sequence-number">> := LastSeqNum,
                    <<"location">> := Location,
                    <<"table-uuid">> := TableUUID
                }
            } = LoadedTable} ?= load_table(Client, Namespace, Table),
        #{bucket := Bucket, base_path := BasePath} = parse_location(Location),
        WriteUUID = uuid4(),
        %% fixme: might need fixing if using partitions and if
        %% `write.object-storage.partitioned-paths` is true (default is true).....
        DataKey = mk_data_file_key(BasePath, WriteUUID, "avro"),
        S3Client = emqx_s3_client:create(binary_to_list(Bucket), S3ClientConfig),
        %% todo: make configurable?
        S3UploaderConfig = #{
            min_part_size => 5 * ?MEGABYTE,
            max_part_size => 5 * ?GIGABYTE
        },
        S3UploadOpts = #{},
        S3TransferState = emqx_s3_upload:new(S3Client, DataKey, S3UploadOpts, S3UploaderConfig),
        {ok, IceSchema} ?= emqx_bridge_iceberg_logic:find_current_schema(LoadedTable, SchemaId),
        {ok, AvroSchema} ?= ice_schema_to_avro(IceSchema),
        ContainerOpts = #{
            type => avro,
            schema => AvroSchema,
            root_type => ?ROOT_SC_TYPE
        },
        TransferState = #{
            ?base_path => BasePath,
            ?bucket => Bucket,
            ?client => Client,
            ?data_file_key => DataKey,
            ?data_size => 0,
            ?loaded_table => LoadedTable,
            ?n_attempt => 0,
            ?namespace => Namespace,
            ?num_records => 0,
            ?s3_client => S3Client,
            ?s3_transfer_state => S3TransferState,
            ?schema_id => SchemaId,
            ?seq_num => LastSeqNum + 1,
            ?table => Table,
            ?table_uuid => TableUUID,
            ?write_uuid => WriteUUID
        },
        {ok, TransferState, ContainerOpts}
    end.

-spec process_append(iodata(), write_metadata(), transfer_state()) ->
    transfer_state().
process_append(IOData, WriteMetadata, TransferState0) ->
    #{num_records := N1} = WriteMetadata,
    #{
        ?num_records := N0,
        ?data_size := S0,
        ?s3_transfer_state := S3TransferState0
    } = TransferState0,
    S1 = iolist_size(IOData),
    {ok, S3TransferState} = emqx_s3_upload:append(IOData, S3TransferState0),
    TransferState0#{
        ?num_records := N0 + N1,
        ?data_size := S0 + S1,
        ?s3_transfer_state := S3TransferState
    }.

-spec process_write(transfer_state()) ->
    {ok, transfer_state()} | {error, term()}.
process_write(TransferState0) ->
    #{
        ?s3_transfer_state := S3TransferState0
    } = TransferState0,
    case emqx_s3_upload:write(S3TransferState0) of
        {ok, S3TransferState} ->
            TransferState = TransferState0#{
                ?s3_transfer_state := S3TransferState
            },
            {ok, TransferState};
        {cont, S3TransferState} ->
            TransferState = TransferState0#{
                ?s3_transfer_state := S3TransferState
            },
            process_write(TransferState);
        {error, Reason} ->
            _ = emqx_s3_upload:abort(S3TransferState0),
            {error, Reason}
    end.

-spec process_complete(transfer_state()) ->
    {ok, term()} | no_return().
process_complete(TransferState0) ->
    #{
        ?s3_transfer_state := S3TransferState
    } = TransferState0,
    case emqx_s3_upload:complete(S3TransferState) of
        {ok, _S3Completed} ->
            ?tp("iceberg_upload_manifests_enter", #{}),
            upload_manifests(TransferState0);
        {error, Reason} ->
            _ = emqx_s3_upload:abort(S3TransferState),
            exit({upload_failed, {data_file, Reason}})
    end.

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
        ok ?= validate_table(Client, Namespace, Table),
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
        {error, {unsupported_type, Type}} ->
            Msg = iolist_to_binary(
                io_lib:format("Schema contains unsupported data type: ~p", [Type])
            ),
            {error, {unhealthy_target, Msg}};
        {error, not_found} ->
            {error, {unhealthy_target, <<"Current schema could not be found">>}};
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
        {ok, IceSchema} ?= emqx_bridge_iceberg_logic:find_current_schema(LoadedTable),
        {ok, _} ?= ice_schema_to_avro(IceSchema),
        ok
    end.

work_dir(Type, Name) ->
    filename:join([emqx:data_dir(), bridge, Type, Name]).

ice_schema_to_avro(IceSchema) ->
    try emqx_bridge_iceberg_logic:convert_iceberg_schema_to_avro(IceSchema) of
        AvroSchema0 ->
            AvroSchema1 = AvroSchema0#{<<"name">> => <<"root">>},
            AvroSchema = avro:decode_schema(emqx_utils_json:encode(AvroSchema1)),
            {ok, AvroSchema}
    catch
        throw:{unsupported_type, Type} ->
            {error, {unsupported_type, Type}}
    end.

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
        ?base_path := BasePath,
        ?bucket := Bucket,
        ?client := Client,
        ?data_file_key := DataFileKey,
        ?data_size := DataSize,
        ?loaded_table := LoadedTable,
        ?n_attempt := NAttempt,
        ?namespace := Namespace,
        ?num_records := NumRecords,
        ?s3_client := S3Client,
        ?schema_id := SchemaId,
        ?seq_num := SeqNum,
        ?table := Table,
        ?table_uuid := TableUUID,
        ?write_uuid := WriteUUID
    } = TransferState,

    #{
        schema := ManifestEntrySc,
        header := ManifestEntryHeader
    } = persistent_term:get(?MANIFEST_ENTRY_PT_KEY),

    NewSnapshotId = gen_snapshot_id(),

    %% todo: will need to abstract this once we support more locations...
    DataS3Path = make_s3_path(Bucket, DataFileKey),
    ManifestEntry = #{
        <<"status">> => ?MANIFEST_ENTRY_STATUS_ADDED,
        <<"snapshot_id">> => NewSnapshotId,
        <<"data_file">> => #{
            <<"content">> => ?DATA_FILE_CONTENT_DATA,
            %% FIXME: TODO: Support partitions...
            <<"partition">> => #{},
            <<"file_path">> => DataS3Path,
            <<"file_format">> => ?DATA_FILE_FORMAT_AVRO,
            <<"record_count">> => NumRecords,
            <<"file_size_in_bytes">> => DataSize
        }
    },
    ManifestEntryBin = avro_binary_encoder:encode(
        ManifestEntrySc, <<"manifest_entry">>, ManifestEntry
    ),
    ManifestEntryKey = mk_manifest_entry_key(BasePath, WriteUUID),
    ManifestEntryOCF = avro_ocf:make_ocf(ManifestEntryHeader, [ManifestEntryBin]),
    %% TODO: handle errors
    ok = emqx_s3_client:put_object(S3Client, ManifestEntryKey, ManifestEntryOCF),

    #{
        schema := ManifestFileSc,
        header := ManifestFileHeader
    } = persistent_term:get(?MANIFEST_FILE_PT_KEY),

    ManifestEntryS3Path = make_s3_path(Bucket, ManifestEntryKey),
    ManifestLength = iolist_size(ManifestEntryOCF),
    ManifestFile = #{
        <<"manifest_path">> => ManifestEntryS3Path,
        <<"manifest_length">> => ManifestLength,
        %% TODO: FIXME: support partitions....
        <<"partition_spec_id">> => 0,
        <<"content">> => ?MANIFEST_LIST_CONTENT_DATA,
        <<"sequence_number">> => SeqNum,
        <<"min_sequence_number">> => SeqNum,
        <<"added_snapshot_id">> => NewSnapshotId,
        <<"added_files_count">> => 1,
        <<"existing_files_count">> => 0,
        <<"deleted_files_count">> => 0,
        <<"added_rows_count">> => NumRecords,
        <<"existing_rows_count">> => 0,
        <<"deleted_rows_count">> => 0,
        %% TODO: FIXME: support partitions....
        <<"partitions">> => []
    },
    %% TODO: handle errors, retries...
    {ok, PrevManifestList} = load_previous_manifest_file(S3Client, LoadedTable),
    ManifestFileBin =
        lists:map(
            fun(ManifestFileIn) ->
                avro_binary_encoder:encode(ManifestFileSc, <<"manifest_file">>, ManifestFileIn)
            end,
            [ManifestFile | PrevManifestList]
        ),
    ManifestFileKey = mk_manifest_file_key(BasePath, NewSnapshotId, WriteUUID, NAttempt),
    ManifestFileOCF = avro_ocf:make_ocf(ManifestFileHeader, ManifestFileBin),
    %% TODO: handle errors
    ok = emqx_s3_client:put_object(S3Client, ManifestFileKey, ManifestFileOCF),

    ManifestFileS3Path = make_s3_path(Bucket, ManifestFileKey),
    CommitContext = #{
        data_size => DataSize,
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

mk_data_file_key(BasePath, WriteUUID, Ext) ->
    K = iolist_to_binary(["00000-0-", WriteUUID, ".", Ext]),
    make_key(BasePath, ["data", K]).

mk_manifest_entry_key(BasePath, WriteUUID) ->
    K = iolist_to_binary([WriteUUID, "-m0.avro"]),
    make_key(BasePath, ["metadata", K]).

mk_manifest_file_key(BasePath, SnapshotId, WriteUUID, NAttempt) ->
    K = iolist_to_binary(["snap-", bin(SnapshotId), "-", bin(NAttempt), "-", WriteUUID, ".avro"]),
    make_key(BasePath, ["metadata", K]).

now_ms() ->
    erlang:system_time(millisecond).

-spec load_previous_manifest_file(emqx_s3_client:client(), map()) ->
    {ok, [map()]} | {error, term()}.
load_previous_manifest_file(S3Client, LoadedTable) ->
    maybe
        #{<<"manifest-list">> := ManifestListLocation} ?=
            emqx_bridge_iceberg_logic:find_current_snapshot(LoadedTable, no_snapshot),
        #{base_path := Key} = parse_location(ManifestListLocation),
        %% TODO: retry on errors??
        {ok, #{content := PrevManifestBin}} ?= emqx_s3_client:get_object(S3Client, Key),
        DecodeOpts = avro:make_decoder_options([{map_type, map}, {record_type, map}]),
        {_Header, _Schema, Blocks} = avro_ocf:decode_binary(PrevManifestBin, DecodeOpts),
        {ok, Blocks}
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
