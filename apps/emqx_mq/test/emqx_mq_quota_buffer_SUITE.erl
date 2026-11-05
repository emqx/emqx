%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_quota_buffer_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include("../gen_src/MessageQueue.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([ecpool], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(Case, Config) ->
    ok = meck:new(emqx_mq_quota_buffer, [passthrough, no_link]),
    Table = ets:new(?MODULE, [public]),
    Options = #{pool_size => 1, cbm => ?MODULE},
    BufferOptions =
        case Case of
            t_keep_legacy_indices -> Options#{keep_legacy_indices => true};
            _ -> Options
        end,
    ok = emqx_mq_quota_buffer:start(?MODULE, BufferOptions),
    [{table, Table} | Config].

end_per_testcase(_Case, _Config) ->
    ok = emqx_mq_quota_buffer:stop(?MODULE),
    ok = meck:unload(emqx_mq_quota_buffer).

%% Check that flushing removes legacy lists only when the capability check reports support on all nodes.
t_upgrade_gate(Config) ->
    Table = ?config(table, Config),
    Opts = #{bytes => 10000, count => 1000, threshold_percentage => 10},
    Message = emqx_message:make(<<"publisher">>, 0, <<"t">>, <<"payload">>),
    Info = #{message => Message, message_size => 10},
    TsUs = erlang:convert_time_unit(emqx_message:timestamp(Message), millisecond, microsecond),

    %% Save separate byte and message-count lists for one message, without common buckets.
    LegacyBin = 'MessageQueue':encode('StorageIndex', #'StorageIndex'{
        maxTsUs = TsUs,
        bytes = [#'StorageIndexRecord'{tsUs = TsUs, value = 10}],
        count = [#'StorageIndexRecord'{tsUs = TsUs, value = 1}]
    }),
    true = ets:insert(Table, {queue, LegacyBin}),

    %% Check that flushing updates legacy lists and common buckets while old nodes remain.
    ok = meck:expect(emqx_mq_quota_buffer, do_all_nodes_support_combined_indices, fun() ->
        false
    end),
    ok = emqx_mq_quota_buffer:add(?MODULE, Table, queue, Opts, undefined, Info),
    ok = emqx_mq_quota_buffer:flush(?MODULE),
    [{queue, Bin1}] = ets:lookup(Table, queue),
    ?assertMatch(
        #'StorageIndex'{
            bytes = [#'StorageIndexRecord'{tsUs = TsUs, value = 20}],
            count = [#'StorageIndexRecord'{tsUs = TsUs, value = 2}],
            common = [#'StorageIndexBucket'{tsUs = TsUs, bytes = 20, count = 2}]
        },
        'MessageQueue':decode('StorageIndex', Bin1)
    ),

    %% Check that flushing updates common counts and removes legacy lists once all nodes support common buckets.
    ok = meck:expect(emqx_mq_quota_buffer, do_all_nodes_support_combined_indices, fun() -> true end),
    ok = emqx_mq_quota_buffer:add(?MODULE, Table, queue, Opts, undefined, Info),
    ok = emqx_mq_quota_buffer:flush(?MODULE),
    [{queue, Bin2}] = ets:lookup(Table, queue),
    ?assertMatch(
        #'StorageIndex'{
            bytes = asn1_NOVALUE,
            count = asn1_NOVALUE,
            common = [#'StorageIndexBucket'{tsUs = TsUs, bytes = 30, count = 3}]
        },
        'MessageQueue':decode('StorageIndex', Bin2)
    ).

%% Check that keep_legacy_indices preserves legacy buckets despite cluster-wide common-index support.
t_keep_legacy_indices(Config) ->
    %% Enable cluster support and flush a message with legacy retention enabled.
    ok = meck:expect(emqx_mq_quota_buffer, do_all_nodes_support_combined_indices, fun() -> true end),
    Table = ?config(table, Config),
    Opts = #{bytes => 10000, count => 1000, threshold_percentage => 10},
    Message = emqx_message:make(<<"publisher">>, 0, <<"t">>, <<"payload">>),
    Info = #{message => Message, message_size => 10},
    TsUs = erlang:convert_time_unit(emqx_message:timestamp(Message), millisecond, microsecond),
    ok = emqx_mq_quota_buffer:add(?MODULE, Table, queue, Opts, undefined, Info),
    ok = emqx_mq_quota_buffer:flush(?MODULE),

    %% Check that the persisted index contains both legacy counters.
    [{queue, Bin}] = ets:lookup(Table, queue),
    ?assertMatch(
        #'StorageIndex'{
            bytes = [#'StorageIndexRecord'{tsUs = TsUs, value = 10}],
            count = [#'StorageIndexRecord'{tsUs = TsUs, value = 1}]
        },
        'MessageQueue':decode('StorageIndex', Bin)
    ).

quota_buffer_max_size() ->
    100.

quota_buffer_flush_interval() ->
    60000.

quota_buffer_notify_queue_size(_WorkerId, _QueueSize) ->
    ok.

quota_buffer_flush_transaction(_TxKey, Fun) ->
    Fun().

quota_buffer_flush_tx_read(Table, Key, Opts) ->
    case ets:lookup(Table, Key) of
        [] -> undefined;
        [{Key, Bin}] -> emqx_mq_quota_index:decode(Opts, Bin)
    end.

quota_buffer_flush_tx_write(Table, Key, Index) ->
    Bin = emqx_mq_quota_index:encode(Index),
    true = ets:insert(Table, {Key, Bin}),
    ok.
