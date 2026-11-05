%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_streams_batch_delete_SUITE).
-compile(export_all).
-compile(nowarn_export_all).
-include_lib("eunit/include/eunit.hrl").

all() -> [{group, streams}, t_delete].
groups() -> emqx_mq_batch_delete_SUITE:groups().
init_per_group(Group, Config) -> emqx_mq_batch_delete_SUITE:init_per_group(Group, Config).
end_per_group(_, _) -> ok.

init_per_testcase(TestCase, Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_durable_storage,
            {emqx,
                emqx_streams_test_utils:cth_config(emqx, #{
                    <<"durable_storage">> => #{<<"streams_messages">> => #{<<"n_shards">> => 2}}
                })},
            {emqx_mq, emqx_mq_test_utils:cth_config(emqx_mq)},
            {emqx_streams, emqx_streams_test_utils:cth_config(emqx_streams)}
        ],
        #{work_dir => emqx_cth_suite:work_dir(TestCase, Config)}
    ),
    snabbkaffe:start_trace(),
    [{apps, Apps} | Config].

end_per_testcase(TestCase, Config) -> emqx_mq_batch_delete_SUITE:end_per_testcase(TestCase, Config).

-doc "GC drains multiple batches while preserving newer data".
t_gc_batches(Config) -> emqx_mq_batch_delete_SUITE:t_gc_batches(Config).
-doc "GC respects each shard's quota cutoff".
t_gc_quota_cutoffs(Config) -> emqx_mq_batch_delete_SUITE:t_gc_quota_cutoffs(Config).
-doc "Drop drains batches across generations and preserves neighboring streams".
t_drop_batches(Config) -> emqx_mq_batch_delete_SUITE:t_drop_batches(Config).
-doc "Drop reports incomplete stream discovery".
t_drop_discovery_error(Config) -> emqx_mq_batch_delete_SUITE:t_drop_discovery_error(Config).
-doc "Drop can resume after a failed batch".
t_drop_batch_error(Config) -> emqx_mq_batch_delete_SUITE:t_drop_batch_error(Config).
-doc "GC skips an empty deletion interval".
t_gc_zero_cutoff(Config) -> emqx_mq_batch_delete_SUITE:t_gc_zero_cutoff(Config).

-doc "A failed slab does not stop another slab worker".
t_gc_slab_isolation(Config) -> emqx_mq_batch_delete_SUITE:t_gc_slab_isolation(Config).

-doc "A failed request is retried for that stream only".
t_gc_queue_retry(Config) -> emqx_mq_batch_delete_SUITE:t_gc_queue_retry(Config).

-doc "A stream whose requests keep failing is given up after three failures".
t_gc_queue_give_up(Config) -> emqx_mq_batch_delete_SUITE:t_gc_queue_give_up(Config).

-doc "The worker stops after five shard failures".
t_gc_slab_abort(Config) -> emqx_mq_batch_delete_SUITE:t_gc_slab_abort(Config).

-doc "Deleting a stream removes its data before the call returns".
t_delete(_Config) ->
    Spec0 = #{
        name => <<"delete">>,
        topic_filter => <<"delete/#">>,
        is_lastvalue => true,
        key_expression => <<"message.topic">>
    },
    Stream0 = emqx_streams_test_utils:ensure_stream_created(Spec0),
    Message0 = emqx_message:make(<<"test">>, 1, <<"delete/1">>, <<"data">>),
    ok = emqx_streams_message_db:insert(Stream0, Message0),
    ?assertNotEqual([], emqx_streams_message_db:dirty_read_all(Stream0)),
    ok = emqx_streams_registry:delete(<<"delete">>),
    ?assertEqual([], emqx_streams_message_db:dirty_read_all(Stream0)),
    ?assertEqual(not_found, emqx_streams_registry:find(<<"delete">>)).
