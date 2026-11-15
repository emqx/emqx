%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_mq_ds_queue_deleter_SUITE).
-compile(export_all).
-compile(nowarn_export_all).
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx_utils/include/emqx_ds_dbs.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).
init_per_testcase(Case, Config) -> emqx_mq_batch_delete_SUITE:init_per_testcase(Case, Config).
end_per_testcase(Case, Config) -> emqx_mq_batch_delete_SUITE:end_per_testcase(Case, Config).

-doc "Delete multiple batches while leaving unrelated mailbox messages in order".
t_batches_and_mailbox(_Config) ->
    DB = ?MQ_MESSAGE_LASTVALUE_DB,
    Topic0 = [<<"queue-deleter">>],
    [Shard0 | _] = emqx_ds:list_shards(DB),
    emqx_mq_batch_delete_SUITE:seed(DB, Shard0, 1, Topic0, lists:seq(1, 1001)),
    Ref0 = make_ref(),
    self() ! {unrelated, 1},
    self() ! {'DOWN', Ref0, process, self(), unrelated},
    self() ! {unrelated, 2},
    ?assertEqual(ok, emqx_mq_ds_queue_deleter:delete(DB, [Topic0])),
    ?assertEqual([], emqx_ds:dirty_read(DB, Topic0)),
    ?assertEqual(
        {unrelated, 1},
        receive
            Msg0 -> Msg0
        after 0 -> missing
        end
    ),
    ?assertEqual(
        {'DOWN', Ref0, process, self(), unrelated},
        receive
            Msg1 -> Msg1
        after 0 -> missing
        end
    ),
    ?assertEqual(
        {unrelated, 2},
        receive
            Msg2 -> Msg2
        after 0 -> missing
        end
    ).

-doc "A recoverable failure retries only its entry and stops after five errors".
t_retries(_Config) ->
    DB = ?MQ_MESSAGE_LASTVALUE_DB,
    [Shard0 | _] = emqx_ds:list_shards(DB),
    Topic0 = [<<"retry">>],
    Topic1 = [<<"healthy">>],
    [emqx_mq_batch_delete_SUITE:seed(DB, Shard0, 1, T, [1]) || T <- [Topic0, Topic1]],
    emqx_mq_batch_delete_SUITE:mock_ds(),
    put(errors, 0),
    meck:expect(emqx_ds, del_batch, fun(DB0, Stream, Topic, From, To, Opts) ->
        case Topic of
            Topic0 ->
                put(errors, get(errors) + 1),
                {error, recoverable, unavailable};
            _ ->
                meck:passthrough([DB0, Stream, Topic, From, To, Opts])
        end
    end),
    ?assertEqual(
        {error, [{{Shard0, 1}, Topic0, {error, recoverable, unavailable}}]},
        emqx_mq_ds_queue_deleter:delete(DB, [Topic0, Topic1])
    ),
    ?assertEqual(5, get(errors)),
    ?assertEqual([], emqx_ds:dirty_read(DB, Topic1)),
    put(errors, 0),
    meck:expect(emqx_ds, del_batch, fun(DB0, Stream, Topic, From, To, Opts) ->
        N = get(errors),
        put(errors, N + 1),
        case N of
            0 -> {error, recoverable, unavailable};
            _ -> meck:passthrough([DB0, Stream, Topic, From, To, Opts])
        end
    end),
    ?assertEqual(ok, emqx_mq_ds_queue_deleter:delete(DB, [Topic0])),
    ?assertEqual(2, get(errors)).

-doc "A slab removed after discovery completes without an error".
t_storage_not_found(_Config) ->
    DB = ?MQ_MESSAGE_LASTVALUE_DB,
    [Shard0 | _] = emqx_ds:list_shards(DB),
    Topic0 = [<<"missing-storage">>],
    emqx_mq_batch_delete_SUITE:seed(DB, Shard0, 1, Topic0, [1]),
    emqx_mq_batch_delete_SUITE:mock_ds(),
    meck:expect(emqx_ds, del_batch, fun(_, _, _, _, _, _) ->
        {error, unrecoverable, {storage_not_found, test}}
    end),
    ?assertEqual(ok, emqx_mq_ds_queue_deleter:delete(DB, [Topic0])).
