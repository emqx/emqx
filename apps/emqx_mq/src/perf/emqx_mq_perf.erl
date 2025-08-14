%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_perf).

-moduledoc """
Performance test utilities for the MQ application.
""".

-include_lib("../emqx_mq_internal.hrl").

-export([
    create_mq_regular/0,
    mq_regular/0,
    populate_regular/1,
    cleanup_mq_regular_consumption_progress/0,
    subsctriber_info/0
]).

-define(MQ_TOPIC_REGULAR, <<"test/#">>).
-define(MQ_POPULATE_BATCH_SIZE, 1000).
-define(MQ_REGULAR, #{
    topic_filter => ?MQ_TOPIC_REGULAR,
    is_compacted => false,
    consumer_max_inactive_ms => 1000,
    ping_interval_ms => 1000,
    redispatch_interval_ms => 100,
    dispatch_strategy => random,
    local_max_inflight => 20,
    busy_session_retry_interval => 100,
    stream_max_buffer_size => 100,
    stream_max_unacked => 100
}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

create_mq_regular() ->
    _ = emqx_mq_registry:delete(?MQ_TOPIC_REGULAR),
    {ok, MQ} = emqx_mq_registry:create(?MQ_REGULAR),
    ok = wait_for_mq_created(MQ),
    MQ.

mq_regular() ->
    emqx_mq_registry:find(?MQ_TOPIC_REGULAR).

cleanup_mq_regular_consumption_progress() ->
    ok = stop_all_consumers(),
    {ok, MQ} = emqx_mq_registry:find(?MQ_TOPIC_REGULAR),
    ok = emqx_mq_consumer_db:drop_consumer_data(MQ).

populate_regular(N) ->
    MQ = create_mq_regular(),
    populate_regular(MQ, N, 0, now_ms_monotonic()).

subsctriber_info() ->
    lists:map(
        fun(ChanPid) -> emqx_mq_util:mq_info(ChanPid) end,
        emqx_cm:all_channels()
    ).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

populate_regular(#{topic_filter := TopicFilter} = MQ, N, NGenerated, StartTime) when
    NGenerated < N
->
    BatchSize = min(?MQ_POPULATE_BATCH_SIZE, N - NGenerated),
    Messages = generate_regular_messages(NGenerated, NGenerated + BatchSize - 1),
    ok = emqx_mq_message_db:insert(MQ, Messages),
    ?tp(warning, populate_regular, #{
        topic_filter => TopicFilter,
        n => N,
        n_generated => NGenerated,
        elapsed => now_ms_monotonic() - StartTime
    }),
    populate_regular(MQ, N, NGenerated + BatchSize, StartTime);
populate_regular(_MQ, _N, _NGenerated, _StartTime) ->
    ok.

generate_regular_messages(FromN, ToN) ->
    lists:map(
        fun(I) ->
            IBin = integer_to_binary(I),
            Payload = <<"payload-", IBin/binary>>,
            Topic = <<"test/", IBin/binary>>,
            ClientId = <<"client-", IBin/binary>>,
            emqx_message:make(ClientId, Topic, Payload)
        end,
        lists:seq(FromN, ToN)
    ).

wait_for_mq_created(#{topic_filter := TopicFilter} = MQ) ->
    case emqx_mq_registry:find(TopicFilter) of
        {ok, MQ} ->
            ok;
        not_found ->
            timer:sleep(100),
            wait_for_mq_created(MQ)
    end.

now_ms_monotonic() ->
    erlang:monotonic_time(millisecond).

stop_all_consumers() ->
    ConsumerPids = [Pid || {_, Pid, _, _} <- supervisor:which_children(emqx_mq_consumer_sup)],
    ok = lists:foreach(
        fun(Pid) ->
            ok = emqx_mq_consumer:stop(Pid)
        end,
        ConsumerPids
    ).
