%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_s3tables_impl).

-feature(maybe_expr, enable).

-behaviour(emqx_resource).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx/include/emqx_trace.hrl").
-include("emqx_bridge_s3tables.hrl").
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

%% API
-export([
    load_and_memoize_schema_files/0,
    forget_schema_files/0
]).

-export_type([
    iceberg_client/0,
    location_client/0,
    namespace/0,
    table_name/0
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(AGGREG_SUP, emqx_bridge_s3tables_sup).

%% Allocatable resources

%% Connector fields
-define(installed_channels, installed_channels).

%% Channel fields
-define(aggreg_id, aggreg_id).
-define(supervisor, supervisor).

-type connector_config() :: #{
    access_key_id := string(),
    secret_access_key := emqx_secret:t(binary()),
    s3tables_arn := binary(),
    base_endpoint => string(),
    bucket => string(),
    account_id => string(),
    any() => term()
}.
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

-type query() :: {_Tag :: channel_id(), _Data :: map()}.

-type aggreg_id() :: {binary(), binary()}.

-type iceberg_client() :: emqx_bridge_s3tables_client_s3t:t().
-type location_client() :: map().

-type namespace() :: binary().
-type table_name() :: binary().

%%------------------------------------------------------------------------------
%% `emqx_resource' API
%%------------------------------------------------------------------------------

-spec resource_type() -> atom().
resource_type() ->
    s3tables.

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
    ?tp("s3tables_connector_stop", #{instance_id => ConnResId}),
    Res.

-spec on_get_status(connector_resource_id(), connector_state()) ->
    ?status_connected | {?status_disconnected, term()}.
on_get_status(
    _ConnResId, #{?location_client := #{?s3_client_config := S3ClientConfig}} = _ConnState
) ->
    emqx_s3_client:get_status(S3ClientConfig).

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
    run_aggregated_action([Data], ChanResId, ChanState);
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
    run_aggregated_action(Batch, ChanResId, ChanState);
on_batch_query(_ConnResId, Queries, _ConnectorState) ->
    {error, {unrecoverable_error, {invalid_batch, Queries}}}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec load_and_memoize_schema_files() -> ok.
load_and_memoize_schema_files() ->
    {ok, ScJSON} = file:read_file(
        filename:join([code:lib_dir(emqx_bridge_s3tables), "priv", "manifest-file.avsc"])
    ),
    Sc = avro:decode_schema(ScJSON),
    Header = avro_ocf:make_header(Sc),
    persistent_term:put(?MANIFEST_FILE_PT_KEY, #{schema => Sc, header => Header}),
    %% Entry schema varies with partition spec.
    {ok, EntryScJSONRaw} = file:read_file(
        filename:join([code:lib_dir(emqx_bridge_s3tables), "priv", "manifest-entry.avsc"])
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
%% Internal fns
%%------------------------------------------------------------------------------

make_client(#{base_endpoint := BaseEndpoint, bucket := Bucket} = Params) when
    is_binary(BaseEndpoint),
    is_binary(Bucket)
->
    emqx_bridge_s3tables_client_s3t:new(Params);
make_client(#{s3tables_arn := ARN} = Params0) ->
    {ok, #{
        region := Region,
        account_id := AccountId,
        bucket := Bucket
    }} = emqx_bridge_s3tables_connector_schema:parse_arn(ARN),
    BaseEndpoint0 = [<<"https://s3tables.">>, Region, <<".amazonaws.com/iceberg">>],
    BaseEndpoint = iolist_to_binary(BaseEndpoint0),
    Params = Params0#{
        account_id => AccountId,
        bucket => Bucket,
        base_endpoint => BaseEndpoint
    },
    emqx_bridge_s3tables_client_s3t:new(Params).

init_location_client(ConnResId, Params) ->
    maybe
        #{
            access_key_id := AccessKeyId,
            secret_access_key := SecretAccessKey,
            s3tables_arn := ARN,
            s3_client := S3Config0
        } = Params,
        S3Config1 = S3Config0#{
            access_key_id => iolist_to_list(AccessKeyId),
            secret_access_key => SecretAccessKey,
            url_expire_time => 0
        },
        S3Config = infer_s3_host_and_port(S3Config1, ARN),
        S3ClientConfig = emqx_s3_profile_conf:client_config(S3Config, ConnResId),
        _ = emqx_s3_client_http:stop_pool(ConnResId),
        ok ?= emqx_s3_client_http:start_pool(ConnResId, S3Config),
        {ok, #{?s3_client_config => S3ClientConfig}}
    end.

run_aggregated_action(Batch, ChanResId, #{?aggreg_id := AggregId}) ->
    Timestamp = erlang:system_time(second),
    emqx_trace:rendered_action_template(ChanResId, #{records => Batch}),
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
        callback_module => emqx_bridge_s3tables_delivery,
        upload_options => TransferOpts
    },
    _ = ?AGGREG_SUP:delete_child(AggregId),
    maybe
        {ok, _} ?= emqx_bridge_s3tables_delivery:validate_table(Client, Namespace, Table),
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

work_dir(Type, Name) ->
    filename:join([emqx:data_dir(), bridge, Type, Name]).

check_aggreg_upload_errors(AggregId) ->
    case emqx_connector_aggregator:take_error(AggregId) of
        [Error] ->
            %% This approach means that, for example, 3 upload failures will cause
            %% the channel to be marked as unhealthy for 3 consecutive health checks.
            {?status_disconnected, {unhealthy_target, emqx_s3_utils:map_error_details(Error)}};
        [] ->
            ok
    end.

iolist_to_list(IOList) ->
    binary_to_list(iolist_to_binary(IOList)).

infer_s3_host_and_port(#{host := _, port := _} = S3Config, _ARN) ->
    S3Config;
infer_s3_host_and_port(S3Config0, ARN) ->
    {ok, #{region := Region}} = emqx_bridge_s3tables_connector_schema:parse_arn(ARN),
    Host0 = [<<"s3.">>, Region, <<".amazonaws.com">>],
    Host = iolist_to_list(Host0),
    Port = 443,
    S3Config0#{host => Host, port => Port}.
