%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_quota_buffer_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").
-include("../src/emqx_mq_internal.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_durable_storage,
            {emqx,
                emqx_mq_test_utils:cth_config(emqx, #{
                    <<"durable_storage">> => #{<<"mq_messages">> => #{<<"n_shards">> => 1}}
                })},
            {emqx_mq,
                emqx_mq_test_utils:cth_config(emqx_mq, #{
                    <<"mq">> => #{<<"quota">> => #{<<"buffer_flush_interval">> => <<"1h">>}}
                })}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

-doc "A replacement is subtracted by DS timestamp, the legacy index is dropped, and queue deletion removes the fence".
t_replacement_metadata(_Config) ->
    ?assert(emqx_mq_quota_index:is_serial_index_supported()),
    MQ =
        #{id := Id, topic_filter := TopicFilter} = emqx_mq_test_utils:ensure_mq_created(#{
            name => <<"fenced">>,
            topic_filter => <<"fenced/#">>,
            is_lastvalue => true,
            key_expression => <<"message.topic">>,
            limits => #{max_shard_message_count => 10, max_shard_message_bytes => infinity}
        }),
    IndexTopic = [<<"topic">>, TopicFilter, Id, <<"index">>],
    FenceTopic = [<<"topic">>, TopicFilter, Id, <<"fence">>],
    ReadOpts = #{db => ?MQ_MESSAGE_LASTVALUE_DB, shard => <<"0">>, generation => 1},
    Opts = emqx_mq_prop:quota_index_opts(MQ),

    %% Count a message whose publication clock is far ahead of DS time.
    Message0 = emqx_message:make(<<"publisher">>, 1, <<"fenced/key">>, <<"old">>),
    Message1 = Message0#message{timestamp = erlang:system_time(millisecond) + 86400000},
    ok = emqx_mq_message_db:insert(MQ, Message1),
    ok = emqx_mq_quota_buffer:flush(?MQ_QUOTA_BUFFER),
    [{_, FenceTS, _}] = emqx_ds:dirty_read(ReadOpts, FenceTopic),

    %% Replacing it must decrement the old DS bucket despite the publication clock change.
    Message2 = Message0#message{timestamp = 1, payload = <<"new">>},
    ok = emqx_mq_message_db:insert(MQ, Message2),
    ok = emqx_mq_quota_buffer:flush(?MQ_QUOTA_BUFFER),
    [{_, ?QUOTA_INDEX_TS, Bin}] = emqx_ds:dirty_read(ReadOpts, IndexTopic),
    Index = emqx_mq_quota_index:decode(Opts, Bin),
    ?assertMatch(
        #{
            legacy := undefined,
            serial := #{
                total := #{count := 1},
                records := [
                    #{ts_us := FenceTS, count := 1}, #{ts_us := 0, bytes := 0, count := 0}
                ]
            }
        },
        emqx_mq_quota_index:inspect(Index)
    ),

    %% Queue deletion removes the pending fence along with the index.
    ?assertMatch([_], emqx_ds:dirty_read(ReadOpts, FenceTopic)),
    ok = emqx_mq_message_db:drop(MQ),
    ?assertEqual([], emqx_ds:dirty_read(ReadOpts, FenceTopic)),
    ?assertEqual([], emqx_ds:dirty_read(ReadOpts, IndexTopic)).
