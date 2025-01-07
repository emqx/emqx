%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_hstreamdb_connector).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include("emqx_bridge_hstreamdb.hrl").

-import(hoconsc, [mk/2]).

-behaviour(emqx_resource).

%% callbacks of behaviour emqx_resource
-export([
    resource_type/0,
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_batch_query/3,
    on_get_status/2,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channels/1,
    on_get_channel_status/3
]).

-export([
    on_flush_result/1
]).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

%% Allocatable resources
-define(hstreamdb_client, hstreamdb_client).

%% -------------------------------------------------------------------------------------------------
%% resource callback
resource_type() -> hstreamdb.

callback_mode() -> always_sync.

on_start(InstId, Config) ->
    try
        do_on_start(InstId, Config)
    catch
        E:R:S ->
            Error = #{
                msg => "start_hstreamdb_connector_error",
                connector => InstId,
                error => E,
                reason => R,
                stack => S
            },
            ?SLOG(error, Error),
            {error, Error}
    end.

on_add_channel(
    _InstId,
    #{
        installed_channels := InstalledChannels,
        client_options := ClientOptions
    } = OldState,
    ChannelId,
    ChannelConfig
) ->
    %{ok, ChannelState} = create_channel_state(ChannelId, PoolName, ChannelConfig),
    Parameters0 = maps:get(parameters, ChannelConfig),
    Parameters = Parameters0#{client_options => ClientOptions},
    PartitionKey = emqx_placeholder:preproc_tmpl(maps:get(partition_key, Parameters, <<"">>)),
    try
        ChannelState = #{
            producer => start_producer(ChannelId, Parameters),
            record_template => record_template(Parameters),
            partition_key => PartitionKey
        },
        NewInstalledChannels = maps:put(ChannelId, ChannelState, InstalledChannels),
        %% Update state
        NewState = OldState#{installed_channels => NewInstalledChannels},
        {ok, NewState}
    catch
        Error:Reason:Stack ->
            {error, {Error, Reason, Stack}}
    end.

on_remove_channel(
    _InstId,
    #{
        installed_channels := InstalledChannels
    } = OldState,
    ChannelId
) ->
    #{
        producer := Producer
    } = maps:get(ChannelId, InstalledChannels),
    _ = hstreamdb:stop_producer(Producer),
    NewInstalledChannels = maps:remove(ChannelId, InstalledChannels),
    %% Update state
    NewState = OldState#{installed_channels => NewInstalledChannels},
    {ok, NewState}.

on_get_channel_status(
    _ResId,
    _ChannelId,
    _State
) ->
    ?status_connected.

on_get_channels(ResId) ->
    emqx_bridge_v2:get_channels_for_connector(ResId).

on_stop(InstId, _State) ->
    ?tp(
        hstreamdb_connector_on_stop,
        #{instance_id => InstId}
    ).

-define(FAILED_TO_APPLY_HRECORD_TEMPLATE,
    {error, {unrecoverable_error, failed_to_apply_hrecord_template}}
).

on_query(
    InstId,
    {ChannelID, Data},
    #{installed_channels := Channels} = _State
) ->
    #{
        producer := Producer, partition_key := PartitionKey, record_template := HRecordTemplate
    } = maps:get(ChannelID, Channels),
    try
        KeyAndRawRecord = to_key_and_raw_record(PartitionKey, HRecordTemplate, Data),
        emqx_trace:rendered_action_template(ChannelID, #{record => KeyAndRawRecord}),
        Record = to_record(KeyAndRawRecord),
        append_record(InstId, Producer, Record, false)
    catch
        _:_ -> ?FAILED_TO_APPLY_HRECORD_TEMPLATE
    end.

on_batch_query(
    InstId,
    [{ChannelID, _Data} | _] = BatchList,
    #{installed_channels := Channels} = _State
) ->
    #{
        producer := Producer, partition_key := PartitionKey, record_template := HRecordTemplate
    } = maps:get(ChannelID, Channels),
    try
        KeyAndRawRecordList = to_multi_part_key_and_partition_key(
            PartitionKey, HRecordTemplate, BatchList
        ),
        emqx_trace:rendered_action_template(ChannelID, #{records => KeyAndRawRecordList}),
        Records = [to_record(Item) || Item <- KeyAndRawRecordList],
        append_record(InstId, Producer, Records, true)
    catch
        _:_ -> ?FAILED_TO_APPLY_HRECORD_TEMPLATE
    end.

on_get_status(_InstId, State) ->
    case check_status(State) of
        ok ->
            ?status_connected;
        Error ->
            %% We set it to ?status_connecting so that the channels are not deleted.
            %% The producers in the channels contains buffers so we don't want to delete them.
            {?status_connecting, Error}
    end.

%% -------------------------------------------------------------------------------------------------
%% hstreamdb batch callback
%% TODO: maybe remove it after disk cache is ready

on_flush_result({{flush, _Stream, _Records}, {ok, _Resp}}) ->
    ok;
on_flush_result({{flush, _Stream, _Records}, {error, _Reason}}) ->
    ok.

%% -------------------------------------------------------------------------------------------------
%% schema
namespace() -> connector_hstreamdb.

roots() ->
    fields(config).

fields(config) ->
    [
        {url,
            mk(binary(), #{
                required => true, desc => ?DESC("url"), default => <<"http://127.0.0.1:6570">>
            })},
        {stream, mk(binary(), #{required => true, desc => ?DESC("stream_name")})},
        {partition_key, mk(binary(), #{required => false, desc => ?DESC("partition_key")})},
        {pool_size, fun emqx_connector_schema_lib:pool_size/1},
        {grpc_timeout, fun grpc_timeout/1}
    ] ++ emqx_connector_schema_lib:ssl_fields().

grpc_timeout(type) -> emqx_schema:timeout_duration_ms();
grpc_timeout(desc) -> ?DESC("grpc_timeout");
grpc_timeout(default) -> ?DEFAULT_GRPC_TIMEOUT_RAW;
grpc_timeout(required) -> false;
grpc_timeout(_) -> undefined.

desc(config) ->
    ?DESC("config").

%% -------------------------------------------------------------------------------------------------
%% internal functions

do_on_start(InstId, Config) ->
    ?SLOG(info, #{
        msg => "starting_hstreamdb_connector_client",
        connector => InstId,
        config => Config
    }),
    {ok, _} = application:ensure_all_started(hstreamdb_erl),
    ClientOptions = client_options(Config),
    State = #{
        client_options => ClientOptions,
        installed_channels => #{}
    },
    case check_status(State) of
        ok ->
            ?SLOG(info, #{
                msg => "hstreamdb_connector_client_started",
                connector => InstId
            }),
            {ok, State};
        Error ->
            ?tp(
                hstreamdb_connector_start_failed,
                #{error => client_not_alive}
            ),
            ?SLOG(error, #{
                msg => "hstreamdb_connector_client_not_alive",
                connector => InstId,
                error => Error
            }),
            {error, {connect_failed, Error}}
    end.

client_options(#{url := ServerURL, ssl := SSL, grpc_timeout := GRPCTimeout}) ->
    RpcOpts =
        case maps:get(enable, SSL) of
            false ->
                #{pool_size => 1};
            true ->
                #{
                    pool_size => 1,
                    gun_opts => #{
                        transport => tls,
                        transport_opts =>
                            emqx_tls_lib:to_client_opts(SSL)
                    }
                }
        end,
    ClientOptions = #{
        url => to_string(ServerURL),
        grpc_timeout => GRPCTimeout,
        rpc_options => RpcOpts
    },
    ClientOptions.

check_status(ConnectorState) ->
    try start_client(ConnectorState) of
        {ok, Client} ->
            check_status_with_client(Client);
        {error, _} = StartClientError ->
            StartClientError
    catch
        ErrorType:Reason:_ST ->
            {error, {ErrorType, Reason}}
    end.

check_status_with_client(Client) ->
    try hstreamdb_client:echo(Client) of
        ok -> ok;
        {error, _} = ErrorEcho -> ErrorEcho
    after
        _ = hstreamdb:stop_client(Client)
    end.

start_client(Opts) ->
    ClientOptions = maps:get(client_options, Opts),
    case hstreamdb:start_client(ClientOptions) of
        {ok, Client} ->
            {ok, Client};
        {error, Error} ->
            {error, Error}
    end.

start_producer(
    ActionId,
    #{
        stream := Stream,
        batch_size := BatchSize,
        batch_interval := Interval
    } = Opts
) ->
    MaxBatches = maps:get(max_batches, Opts, ?DEFAULT_MAX_BATCHES),
    AggPoolSize = maps:get(aggregation_pool_size, Opts, ?DEFAULT_AGG_POOL_SIZE),
    WriterPoolSize = maps:get(writer_pool_size, Opts, ?DEFAULT_WRITER_POOL_SIZE),
    GRPCTimeout = maps:get(grpc_flush_timeout, Opts, ?DEFAULT_GRPC_FLUSH_TIMEOUT),
    ClientOptions = maps:get(client_options, Opts),
    ProducerOptions = #{
        stream => to_string(Stream),
        buffer_options => #{
            interval => Interval,
            callback => {?MODULE, on_flush_result, [ActionId]},
            max_records => BatchSize,
            max_batches => MaxBatches
        },
        buffer_pool_size => AggPoolSize,
        writer_options => #{
            grpc_timeout => GRPCTimeout
        },
        writer_pool_size => WriterPoolSize,
        client_options => ClientOptions
    },
    Name = produce_name(ActionId),
    ensure_start_producer(Name, ProducerOptions).

ensure_start_producer(ProducerName, ProducerOptions) ->
    case hstreamdb:start_producer(ProducerName, ProducerOptions) of
        ok ->
            ok;
        {error, {already_started, _Pid}} ->
            %% HStreamDB producer already started, restart it
            _ = hstreamdb:stop_producer(ProducerName),
            %% the pool might have been leaked after relup
            _ = ecpool:stop_sup_pool(ProducerName),
            ok = hstreamdb:start_producer(ProducerName, ProducerOptions);
        {error, {
            {shutdown,
                {failed_to_start_child, {pool_sup, Pool},
                    {shutdown,
                        {failed_to_start_child, worker_sup,
                            {shutdown, {failed_to_start_child, _, {badarg, _}}}}}}},
            _
        }} ->
            %% HStreamDB producer was not properly cleared, restart it
            %% the badarg error in gproc maybe caused by the pool is leaked after relup
            _ = ecpool:stop_sup_pool(Pool),
            ok = hstreamdb:start_producer(ProducerName, ProducerOptions);
        {error, Reason} ->
            %% HStreamDB start producer failed
            throw({start_producer_failed, Reason})
    end,
    ProducerName.

produce_name(ActionId) ->
    list_to_binary("backend_hstream_producer:" ++ to_string(ActionId)).

to_key_and_raw_record(PartitionKeyTmpl, HRecordTmpl, Data) ->
    PartitionKey = emqx_placeholder:proc_tmpl(PartitionKeyTmpl, Data),
    RawRecord = emqx_placeholder:proc_tmpl(HRecordTmpl, Data),
    #{partition_key => PartitionKey, raw_record => RawRecord}.

to_record(#{partition_key := PartitionKey, raw_record := RawRecord}) when is_binary(PartitionKey) ->
    to_record(#{partition_key => binary_to_list(PartitionKey), raw_record => RawRecord});
to_record(#{partition_key := PartitionKey, raw_record := RawRecord}) ->
    hstreamdb:to_record(PartitionKey, raw, RawRecord).

to_multi_part_key_and_partition_key(PartitionKeyTmpl, HRecordTmpl, BatchList) ->
    lists:map(
        fun({_, Data}) ->
            to_key_and_raw_record(PartitionKeyTmpl, HRecordTmpl, Data)
        end,
        BatchList
    ).

append_record(ResourceId, Producer, MultiPartsRecords, MaybeBatch) when
    is_list(MultiPartsRecords)
->
    lists:foreach(
        fun(Record) -> append_record(ResourceId, Producer, Record, MaybeBatch) end,
        MultiPartsRecords
    );
append_record(ResourceId, Producer, Record, MaybeBatch) when is_tuple(Record) ->
    do_append_records(ResourceId, Producer, Record, MaybeBatch).

%% TODO: only sync request supported. implement async request later.
do_append_records(ResourceId, Producer, Record, true = IsBatch) ->
    Result = hstreamdb:append(Producer, Record),
    handle_result(ResourceId, Result, Record, IsBatch);
do_append_records(ResourceId, Producer, Record, false = IsBatch) ->
    Result = hstreamdb:append_flush(Producer, Record),
    handle_result(ResourceId, Result, Record, IsBatch).

handle_result(ResourceId, ok = Result, Record, IsBatch) ->
    handle_result(ResourceId, {ok, Result}, Record, IsBatch);
handle_result(ResourceId, {ok, Result}, Record, IsBatch) ->
    ?tp(
        hstreamdb_connector_query_append_return,
        #{result => Result, is_batch => IsBatch, instance_id => ResourceId}
    ),
    ?SLOG(debug, #{
        msg => "hstreamdb_producer_sync_append_success",
        record => Record,
        is_batch => IsBatch
    });
handle_result(ResourceId, {error, Reason} = Err, Record, IsBatch) ->
    ?tp(
        hstreamdb_connector_query_append_return,
        #{error => Reason, is_batch => IsBatch, instance_id => ResourceId}
    ),
    ?SLOG(error, #{
        msg => "hstreamdb_producer_sync_append_failed",
        reason => Reason,
        record => Record,
        is_batch => IsBatch
    }),
    Err.

record_template(#{record_template := RawHRecordTemplate}) ->
    emqx_placeholder:preproc_tmpl(RawHRecordTemplate);
record_template(_) ->
    emqx_placeholder:preproc_tmpl(<<"${payload}">>).

to_string(List) when is_list(List) -> List;
to_string(Bin) when is_binary(Bin) -> binary_to_list(Bin);
to_string(Atom) when is_atom(Atom) -> atom_to_list(Atom).
