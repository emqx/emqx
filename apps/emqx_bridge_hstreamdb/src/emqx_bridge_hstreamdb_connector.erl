%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_hstreamdb_connector).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(hoconsc, [mk/2, enum/1]).

-behaviour(emqx_resource).

%% callbacks of behaviour emqx_resource
-export([
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_batch_query/3,
    on_get_status/2
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

-define(DEFAULT_GRPC_TIMEOUT, timer:seconds(30)).
-define(DEFAULT_GRPC_TIMEOUT_RAW, <<"30s">>).

%% -------------------------------------------------------------------------------------------------
%% resource callback
callback_mode() -> always_sync.

on_start(InstId, Config) ->
    start_client(InstId, Config).

on_stop(InstId, _State) ->
    case emqx_resource:get_allocated_resources(InstId) of
        #{?hstreamdb_client := #{client := Client, producer := Producer}} ->
            StopClientRes = hstreamdb:stop_client(Client),
            StopProducerRes = hstreamdb:stop_producer(Producer),
            ?SLOG(info, #{
                msg => "stop_hstreamdb_connector",
                connector => InstId,
                client => Client,
                producer => Producer,
                stop_client => StopClientRes,
                stop_producer => StopProducerRes
            });
        _ ->
            ok
    end.

-define(FAILED_TO_APPLY_HRECORD_TEMPLATE,
    {error, {unrecoverable_error, failed_to_apply_hrecord_template}}
).

on_query(
    _InstId,
    {send_message, Data},
    _State = #{
        producer := Producer, partition_key := PartitionKey, record_template := HRecordTemplate
    }
) ->
    try to_record(PartitionKey, HRecordTemplate, Data) of
        Record -> append_record(Producer, Record, false)
    catch
        _:_ -> ?FAILED_TO_APPLY_HRECORD_TEMPLATE
    end.

on_batch_query(
    _InstId,
    BatchList,
    _State = #{
        producer := Producer, partition_key := PartitionKey, record_template := HRecordTemplate
    }
) ->
    try to_multi_part_records(PartitionKey, HRecordTemplate, BatchList) of
        Records -> append_record(Producer, Records, true)
    catch
        _:_ -> ?FAILED_TO_APPLY_HRECORD_TEMPLATE
    end.

on_get_status(_InstId, #{client := Client}) ->
    case is_alive(Client) of
        true ->
            connected;
        false ->
            disconnected
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
start_client(InstId, Config) ->
    try
        do_start_client(InstId, Config)
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

do_start_client(InstId, Config = #{url := Server, pool_size := PoolSize, ssl := SSL}) ->
    ?SLOG(info, #{
        msg => "starting_hstreamdb_connector_client",
        connector => InstId,
        config => Config
    }),
    ClientName = client_name(InstId),
    RpcOpts =
        case maps:get(enable, SSL) of
            false ->
                #{pool_size => PoolSize};
            true ->
                #{
                    pool_size => PoolSize,
                    gun_opts => #{
                        transport => tls,
                        transport_opts => emqx_tls_lib:to_client_opts(SSL)
                    }
                }
        end,
    ClientOptions = [
        {url, binary_to_list(Server)},
        {rpc_options, RpcOpts}
    ],
    case hstreamdb:start_client(ClientName, ClientOptions) of
        {ok, Client} ->
            case is_alive(Client) of
                true ->
                    ?SLOG(info, #{
                        msg => "hstreamdb_connector_client_started",
                        connector => InstId,
                        client => Client
                    }),
                    start_producer(InstId, Client, Config);
                _ ->
                    ?tp(
                        hstreamdb_connector_start_failed,
                        #{error => client_not_alive}
                    ),
                    ?SLOG(error, #{
                        msg => "hstreamdb_connector_client_not_alive",
                        connector => InstId
                    }),
                    {error, connect_failed}
            end;
        {error, {already_started, Pid}} ->
            ?SLOG(info, #{
                msg => "starting_hstreamdb_connector_client_find_old_client_restart_client",
                old_client_pid => Pid,
                old_client_name => ClientName
            }),
            _ = hstreamdb:stop_client(ClientName),
            start_client(InstId, Config);
        {error, Error} ->
            ?SLOG(error, #{
                msg => "hstreamdb_connector_client_failed",
                connector => InstId,
                reason => Error
            }),
            {error, Error}
    end.

is_alive(Client) ->
    hstreamdb_client:echo(Client) =:= ok.

start_producer(
    InstId,
    Client,
    Options = #{stream := Stream, pool_size := PoolSize}
) ->
    %% TODO: change these batch options after we have better disk cache.
    BatchSize = maps:get(batch_size, Options, 100),
    Interval = maps:get(batch_interval, Options, 1000),
    ProducerOptions = [
        {stream, Stream},
        {callback, {?MODULE, on_flush_result, []}},
        {max_records, BatchSize},
        {interval, Interval},
        {pool_size, PoolSize},
        {grpc_timeout, maps:get(grpc_timeout, Options, ?DEFAULT_GRPC_TIMEOUT)}
    ],
    Name = produce_name(InstId),
    ?SLOG(info, #{
        msg => "starting_hstreamdb_connector_producer",
        connector => InstId
    }),
    case hstreamdb:start_producer(Client, Name, ProducerOptions) of
        {ok, Producer} ->
            ?SLOG(info, #{
                msg => "hstreamdb_connector_producer_started"
            }),
            State = #{
                client => Client,
                producer => Producer,
                enable_batch => maps:get(enable_batch, Options, false),
                partition_key => emqx_placeholder:preproc_tmpl(
                    maps:get(partition_key, Options, <<"">>)
                ),
                record_template => record_template(Options)
            },
            ok = emqx_resource:allocate_resource(InstId, ?hstreamdb_client, #{
                client => Client, producer => Producer
            }),
            {ok, State};
        {error, {already_started, Pid}} ->
            ?SLOG(info, #{
                msg =>
                    "starting_hstreamdb_connector_producer_find_old_producer_restart_producer",
                old_producer_pid => Pid,
                old_producer_name => Name
            }),
            _ = hstreamdb:stop_producer(Name),
            start_producer(InstId, Client, Options);
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "starting_hstreamdb_connector_producer_failed",
                reason => Reason
            }),
            {error, Reason}
    end.

to_record(PartitionKeyTmpl, HRecordTmpl, Data) ->
    PartitionKey = emqx_placeholder:proc_tmpl(PartitionKeyTmpl, Data),
    RawRecord = emqx_placeholder:proc_tmpl(HRecordTmpl, Data),
    to_record(PartitionKey, RawRecord).

to_record(PartitionKey, RawRecord) when is_binary(PartitionKey) ->
    to_record(binary_to_list(PartitionKey), RawRecord);
to_record(PartitionKey, RawRecord) ->
    hstreamdb:to_record(PartitionKey, raw, RawRecord).

to_multi_part_records(PartitionKeyTmpl, HRecordTmpl, BatchList) ->
    lists:map(
        fun({send_message, Data}) ->
            to_record(PartitionKeyTmpl, HRecordTmpl, Data)
        end,
        BatchList
    ).

append_record(Producer, MultiPartsRecords, MaybeBatch) when is_list(MultiPartsRecords) ->
    lists:foreach(
        fun(Record) -> append_record(Producer, Record, MaybeBatch) end, MultiPartsRecords
    );
append_record(Producer, Record, MaybeBatch) when is_tuple(Record) ->
    do_append_records(Producer, Record, MaybeBatch).

%% TODO: only sync request supported. implement async request later.
do_append_records(Producer, Record, true = IsBatch) ->
    Result = hstreamdb:append(Producer, Record),
    handle_result(Result, Record, IsBatch);
do_append_records(Producer, Record, false = IsBatch) ->
    Result = hstreamdb:append_flush(Producer, Record),
    handle_result(Result, Record, IsBatch).

handle_result(ok = Result, Record, IsBatch) ->
    handle_result({ok, Result}, Record, IsBatch);
handle_result({ok, Result}, Record, IsBatch) ->
    ?tp(
        hstreamdb_connector_query_append_return,
        #{result => Result, is_batch => IsBatch}
    ),
    ?SLOG(debug, #{
        msg => "hstreamdb_producer_sync_append_success",
        record => Record,
        is_batch => IsBatch
    });
handle_result({error, Reason} = Err, Record, IsBatch) ->
    ?tp(
        hstreamdb_connector_query_append_return,
        #{error => Reason, is_batch => IsBatch}
    ),
    ?SLOG(error, #{
        msg => "hstreamdb_producer_sync_append_failed",
        reason => Reason,
        record => Record,
        is_batch => IsBatch
    }),
    Err.

client_name(InstId) ->
    "client:" ++ to_string(InstId).

produce_name(ActionId) ->
    list_to_atom("producer:" ++ to_string(ActionId)).

record_template(#{record_template := RawHRecordTemplate}) ->
    emqx_placeholder:preproc_tmpl(RawHRecordTemplate);
record_template(_) ->
    emqx_placeholder:preproc_tmpl(<<"${payload}">>).

to_string(List) when is_list(List) -> List;
to_string(Bin) when is_binary(Bin) -> binary_to_list(Bin);
to_string(Atom) when is_atom(Atom) -> atom_to_list(Atom).
