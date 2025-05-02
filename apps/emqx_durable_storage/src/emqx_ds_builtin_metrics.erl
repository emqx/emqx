%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_builtin_metrics).

%% DS-facing API:
-export([child_spec/0, metric_id/1, init_for_db/1, init_for_buffer/2, init_for_beamformer/2]).

%% Prometheus-facing API:
-export([prometheus_meta/0, prometheus_collect/1]).

-export([
    inc_buffer_batches/1,
    inc_buffer_batches_retry/1,
    inc_buffer_batches_failed/1,
    inc_buffer_messages/2,
    inc_buffer_bytes/2,

    observe_buffer_flush_time/2,
    observe_buffer_latency/2,

    observe_store_batch_time/2,

    observe_next_time/2,

    observe_sharing/2,
    set_subs_count/3,
    inc_handover/1,
    dec_handover/1,
    inc_beams_sent/2,
    inc_subs_stuck_total/1,
    inc_subs_unstuck_total/1,
    observe_beamformer_fulfill_time/2,
    observer_beamformer_cmds_time/2,
    observe_beamformer_scan_time/2,
    observe_beamsplitter_fanout_time/2,

    inc_lts_seek_counter/2,
    inc_lts_next_counter/2,
    inc_lts_collision_counter/2,

    collect_shard_counter/3
]).

%% behavior callbacks:
-export([]).

%% internal exports:
-export([]).

-export_type([shard_metrics_id/0]).

-include("emqx_ds_metrics.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(WORKER, ?MODULE).

%% Prometheus type | Metrics worker type | Metric ID
-define(COMMON_DB_METRICS, [
    {gauge, slide, ?DS_SUBS_FANOUT_TIME},
    {counter, counter, ?DS_SUBS_STUCK_TOTAL},
    {counter, counter, ?DS_SUBS_UNSTUCK_TOTAL}
]).

-define(STORAGE_LAYER_METRICS, [
    {counter, hist, ?DS_STORE_BATCH_TIME},
    {counter, counter, ?DS_BITFIELD_LTS_SEEK_COUNTER},
    {counter, counter, ?DS_BITFIELD_LTS_NEXT_COUNTER},
    {counter, counter, ?DS_BITFIELD_LTS_COLLISION_COUNTER},
    {counter, counter, ?DS_SKIPSTREAM_LTS_SEEK},
    {counter, counter, ?DS_SKIPSTREAM_LTS_NEXT},
    {counter, counter, ?DS_SKIPSTREAM_LTS_HASH_COLLISION},
    {counter, counter, ?DS_SKIPSTREAM_LTS_HIT},
    {counter, counter, ?DS_SKIPSTREAM_LTS_MISS},
    {counter, counter, ?DS_SKIPSTREAM_LTS_FUTURE},
    {counter, counter, ?DS_SKIPSTREAM_LTS_EOS}
]).

-define(FETCH_METRICS, [
    {counter, slide, ?DS_BUILTIN_NEXT_TIME}
]).

-define(DB_METRICS, ?COMMON_DB_METRICS ++ ?STORAGE_LAYER_METRICS ++ ?FETCH_METRICS).

-define(BUFFER_METRICS, [
    {counter, counter, ?DS_BUFFER_BATCHES},
    {counter, counter, ?DS_BUFFER_BATCHES_RETRY},
    {counter, counter, ?DS_BUFFER_BATCHES_FAILED},
    {counter, counter, ?DS_BUFFER_MESSAGES},
    {counter, counter, ?DS_BUFFER_BYTES},
    {gauge, hist, ?DS_BUFFER_FLUSH_TIME},
    {gauge, slide, ?DS_BUFFER_LATENCY}
]).

-define(BEAMFORMER_METRICS, [
    {gauge, counter, ?DS_SUBS},
    {counter, counter, ?DS_SUBS_HANDOVER},
    {counter, counter, ?DS_SUBS_BEAMS_SENT_TOTAL},
    {gauge, slide, ?DS_SUBS_REQUEST_SHARING},
    {gauge, slide, ?DS_SUBS_FULFILL_TIME},
    {gauge, slide, ?DS_SUBS_PROCESS_COMMANDS_TIME},
    {gauge, slide, ?DS_SUBS_SCAN_TIME}
]).

-type shard_metrics_id() :: binary().

-define(CATCH(BODY),
    try
        BODY
    catch
        _:_ -> ok
    end
).

-elvis([{elvis_style, dont_repeat_yourself, disable}]).
-elvis([{elvis_style, no_if_expression, disable}]).

%%================================================================================
%% API functions
%%================================================================================

%% @doc Transform a proplist of prometheus labels with their values
%% into a metric ID expected by `emqx_metrics_worker':
metric_id(Proplist) ->
    Binaries = [
        if
            is_binary(Value) ->
                Value;
            is_integer(Value) ->
                integer_to_binary(Value);
            is_atom(Value) ->
                atom_to_binary(Value)
        end
     || {_Label, Value} <- lists:sort(Proplist)
    ],
    iolist_to_binary(lists:join("/", Binaries)).

-spec child_spec() -> supervisor:child_spec().
child_spec() ->
    emqx_metrics_worker:child_spec(?WORKER).

%% @doc Initialize metrics that are global for a DS database
-spec init_for_db(emqx_ds:db()) -> ok.
init_for_db(DB) ->
    Metrics = [{Type, MetricId} || {_PromType, Type, MetricId} <- ?DB_METRICS],
    emqx_metrics_worker:create_metrics(?WORKER, metric_id([{db, DB}]), Metrics, []).

%% @doc Initialize metrics that are specific for the buffer.
-spec init_for_buffer(emqx_ds:db(), binary()) -> ok.
init_for_buffer(DB, Shard) ->
    Metrics = [{Type, MetricId} || {_PromType, Type, MetricId} <- ?BUFFER_METRICS],
    emqx_metrics_worker:create_metrics(?WORKER, metric_id([{db, DB}, {shard, Shard}]), Metrics, []).

%% @doc Initialize metrics that are specific for the buffer.
-spec init_for_beamformer(emqx_ds:db(), binary()) -> ok.
init_for_beamformer(DB, Shard) ->
    Metrics = [{Type, MetricId} || {_PromType, Type, MetricId} <- ?BEAMFORMER_METRICS],
    ok = emqx_metrics_worker:create_metrics(
        ?WORKER, metric_id([{db, DB}, {shard, Shard}, {type, rt}]), Metrics, []
    ),
    ok = emqx_metrics_worker:create_metrics(
        ?WORKER, metric_id([{db, DB}, {shard, Shard}, {type, catchup}]), Metrics, []
    ).

%% @doc Increase the number of successfully flushed batches
-spec inc_buffer_batches(shard_metrics_id()) -> ok.
inc_buffer_batches(Id) ->
    ?CATCH(emqx_metrics_worker:inc(?WORKER, Id, ?DS_BUFFER_BATCHES)).

%% @doc Increase the number of time the buffer worker had to retry
%% flushing the batch
-spec inc_buffer_batches_retry(shard_metrics_id()) -> ok.
inc_buffer_batches_retry(Id) ->
    ?CATCH(emqx_metrics_worker:inc(?WORKER, Id, ?DS_BUFFER_BATCHES_RETRY)).

%% @doc Increase the number of time the buffer worker encountered an
%% unrecoverable error while trying to flush the batch
-spec inc_buffer_batches_failed(shard_metrics_id()) -> ok.
inc_buffer_batches_failed(Id) ->
    ?CATCH(emqx_metrics_worker:inc(?WORKER, Id, ?DS_BUFFER_BATCHES_FAILED)).

%% @doc Increase the number of messages successfully saved to the shard
-spec inc_buffer_messages(shard_metrics_id(), non_neg_integer()) -> ok.
inc_buffer_messages(Id, NMessages) ->
    ?CATCH(emqx_metrics_worker:inc(?WORKER, Id, ?DS_BUFFER_MESSAGES, NMessages)).

%% @doc Increase the number of messages successfully saved to the shard
-spec inc_buffer_bytes(shard_metrics_id(), non_neg_integer()) -> ok.
inc_buffer_bytes(Id, NMessages) ->
    ?CATCH(emqx_metrics_worker:inc(?WORKER, Id, ?DS_BUFFER_BYTES, NMessages)).

%% @doc Add a sample of elapsed time spent flushing the buffer to the
%% backend (in microseconds)
-spec observe_buffer_flush_time(shard_metrics_id(), non_neg_integer()) -> ok.
observe_buffer_flush_time(Id, FlushTime) ->
    ?CATCH(emqx_metrics_worker:observe(?WORKER, Id, ?DS_BUFFER_FLUSH_TIME, FlushTime)).

%% @doc Add a sample of latency induced by the buffer (milliseconds).
%% Latency is calculated as difference between timestamp of the oldest
%% message in the flushed batch and current time.
-spec observe_buffer_latency(shard_metrics_id(), non_neg_integer()) -> ok.
observe_buffer_latency(Id, FlushTime) ->
    ?CATCH(emqx_metrics_worker:observe(?WORKER, Id, ?DS_BUFFER_LATENCY, FlushTime)).

-spec observe_store_batch_time({emqx_ds:db(), emqx_ds:shard()}, non_neg_integer()) -> ok.
observe_store_batch_time({DB, _}, StoreTime) ->
    ?CATCH(emqx_metrics_worker:observe(?WORKER, DB, ?DS_STORE_BATCH_TIME, StoreTime)).

%% @doc Add a sample of elapsed time spent waiting for a batch
%% `emqx_ds_replication_layer:next'
-spec observe_next_time(emqx_ds:db(), non_neg_integer()) -> ok.
observe_next_time(DB, NextTime) ->
    ?CATCH(emqx_metrics_worker:observe(?WORKER, DB, ?DS_BUILTIN_NEXT_TIME, NextTime)).

observe_sharing(Id, Sharing) ->
    ?CATCH(emqx_metrics_worker:observe(?WORKER, Id, ?DS_SUBS_REQUEST_SHARING, Sharing)).

%% @doc Set number of subscribers handled by the `rt' beamformer workers:
set_subs_count(Id, Worker, Len) ->
    ?CATCH(emqx_metrics_worker:set_gauge(?WORKER, Id, Worker, ?DS_SUBS, Len)).

inc_handover(Id) ->
    ?CATCH(emqx_metrics_worker:inc(?WORKER, Id, ?DS_SUBS_HANDOVER)).

dec_handover(Id) ->
    ?CATCH(emqx_metrics_worker:inc(?WORKER, Id, ?DS_SUBS_HANDOVER, -1)).

inc_beams_sent(Id, N) ->
    ?CATCH(emqx_metrics_worker:inc(?WORKER, Id, ?DS_SUBS_BEAMS_SENT_TOTAL, N)).

inc_subs_stuck_total(Id) ->
    ?CATCH(emqx_metrics_worker:inc(?WORKER, Id, ?DS_SUBS_STUCK_TOTAL, 1)).

inc_subs_unstuck_total(Id) ->
    ?CATCH(emqx_metrics_worker:inc(?WORKER, Id, ?DS_SUBS_UNSTUCK_TOTAL, 1)).

%% @doc Add a sample of elapsed time spent forming and casting a beam
observe_beamformer_fulfill_time(Id, Time) ->
    ?CATCH(emqx_metrics_worker:observe(?WORKER, Id, ?DS_SUBS_FULFILL_TIME, Time)).

%% @doc Add a sample of elapsed time spent forming and casting a beam
observer_beamformer_cmds_time(Id, Time) ->
    ?CATCH(emqx_metrics_worker:observe(?WORKER, Id, ?DS_SUBS_PROCESS_COMMANDS_TIME, Time)).

%% @doc Add a sample of elapsed time spent scanning the DB stream
observe_beamformer_scan_time(Id, Time) ->
    ?CATCH(emqx_metrics_worker:observe(?WORKER, Id, ?DS_SUBS_SCAN_TIME, Time)).

observe_beamsplitter_fanout_time(DB, Time) ->
    ?CATCH(emqx_metrics_worker:observe(?WORKER, DB, ?DS_SUBS_FANOUT_TIME, Time)).

-spec inc_lts_seek_counter({emqx_ds:db(), emqx_ds:shard()}, non_neg_integer()) -> ok.
inc_lts_seek_counter({DB, _}, Inc) ->
    ?CATCH(emqx_metrics_worker:inc(?WORKER, DB, ?DS_BITFIELD_LTS_SEEK_COUNTER, Inc)).

-spec inc_lts_next_counter({emqx_ds:db(), emqx_ds:shard()}, non_neg_integer()) -> ok.
inc_lts_next_counter({DB, _}, Inc) ->
    ?CATCH(emqx_metrics_worker:inc(?WORKER, DB, ?DS_BITFIELD_LTS_NEXT_COUNTER, Inc)).

-spec inc_lts_collision_counter({emqx_ds:db(), emqx_ds:shard()}, non_neg_integer()) -> ok.
inc_lts_collision_counter({DB, _}, Inc) ->
    ?CATCH(emqx_metrics_worker:inc(?WORKER, DB, ?DS_BITFIELD_LTS_COLLISION_COUNTER, Inc)).

-spec collect_shard_counter({emqx_ds:db(), emqx_ds:shard()}, atom(), non_neg_integer()) -> ok.
collect_shard_counter({DB, _}, Key, Inc) ->
    ?CATCH(emqx_metrics_worker:inc(?WORKER, DB, Key, Inc)).

prometheus_meta() ->
    lists:map(
        fun({Type, _, A}) -> {A, Type, A} end,
        ?DB_METRICS ++ ?BUFFER_METRICS ++ ?BEAMFORMER_METRICS
    ).

prometheus_collect(NodeOrAggr) ->
    collect_beamformer_metrics(
        NodeOrAggr, collect_buffer_metrics(NodeOrAggr, collect_db_metrics(NodeOrAggr))
    ).

collect_db_metrics(NodeOrAggr) ->
    Instances = [[{db, DB}] || {DB, _Backend} <- emqx_ds:which_dbs()],
    collect(NodeOrAggr, Instances, #{}).

collect_buffer_metrics(NodeOrAggr, Acc) ->
    Instances = [[{db, DB}, {shard, Shard}] || {DB, Shard} <- emqx_ds_buffer:ls()],
    collect(NodeOrAggr, Instances, Acc).

collect_beamformer_metrics(NodeOrAggr, Acc) ->
    Instances = [
        [{db, DB}, {shard, Shard}, {type, Type}]
     || {DB, Shard} <- emqx_ds_buffer:ls(),
        Type <- [rt, catchup]
    ],
    collect(NodeOrAggr, Instances, Acc).

%% This function returns the data in the following format:
%% ```
%% #{emqx_ds_buffer_batches =>
%%       [{[{db,messages},{shard,<<"1">>}],99408},
%%        {[{db,messages},{shard,<<"0">>}],99409}],
%%   emqx_ds_buffer_batches_retry =>
%%       [{[{db,messages},{shard,<<"1">>}],0},
%%        {[{db,messages},{shard,<<"0">>}],0}],
%%   emqx_ds_buffer_messages =>
%%        ...
%%  }
%% '''
%%
%% If `NodeOrAggr' = `node' then node name is appended to the list of
%% labels.
collect(NodeOrAggr, Instances, Acc0) ->
    lists:foldl(
        fun(Labels, Acc) ->
            PromLabels =
                case NodeOrAggr of
                    node -> Labels;
                    _ -> [{node, node()} | Labels]
                end,
            do_collect(metric_id(Labels), PromLabels, Acc)
        end,
        Acc0,
        Instances
    ).

do_collect(Id, PromLabels, Acc0) ->
    #{counters := CC, slides := SS, gauges := GG} = emqx_metrics_worker:get_metrics(?WORKER, Id),
    %% Collect counters and gauges:
    Acc1 = maps:fold(
        fun(MetricId, Value, Acc) ->
            append_to_key(MetricId, {PromLabels, Value}, Acc)
        end,
        Acc0,
        maps:merge(CC, GG)
    ),
    %% Collect slides:
    maps:fold(
        fun(MetricId, Value, Acc) ->
            append_to_key(
                MetricId,
                slide_value(last5m, Value, PromLabels),
                %% Add current value of the slide to the accumulator:
                append_to_key(MetricId, slide_value(current, Value, PromLabels), Acc)
            )
        end,
        Acc1,
        SS
    ).

-spec append_to_key(K, V, #{K => [V]}) -> #{K => [V]}.
append_to_key(Key, Value, Map) ->
    maps:update_with(
        Key,
        fun(L) ->
            [Value | L]
        end,
        [Value],
        Map
    ).

slide_value(Interval, Value, Labels0) ->
    Labels = [{interval, Interval} | Labels0],
    {Labels, maps:get(Interval, Value, 0)}.

%%================================================================================
%% Internal functions
%%================================================================================
