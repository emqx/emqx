%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_perf).

-moduledoc """
Performance test utilities for the MQ application.
""".

-include_lib("../emqx_mq_internal.hrl").

-export([
    create_mq_regular/1,
    mq_regular/1,
    populate_regular/3,
    cleanup_mq_regular_consumption_progress/1,
    subsctriber_info/1,
    consumer_info_regular/1,
    info_regular/1
]).

-define(MQ_REGULAR, #{
    is_lastvalue => false,
    consumer_max_inactive => 1000,
    ping_interval => 10000,
    redispatch_interval => 100,
    dispatch_strategy => random,
    local_max_inflight => 20,
    busy_session_retry_interval => 100,
    stream_max_buffer_size => 100,
    stream_max_unacked => 100,
    consumer_persistence_interval => 10000,
    data_retention_period => 3600_000
}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

create_mq_regular(TopicFilter) ->
    _ = emqx_mq_registry:delete(TopicFilter),
    {ok, MQ} = emqx_mq_registry:create(maps:merge(?MQ_REGULAR, #{topic_filter => TopicFilter})),
    ok = wait_for_mq_created(MQ),
    MQ.

mq_regular(TopicFilter) ->
    emqx_mq_registry:find(TopicFilter).

cleanup_mq_regular_consumption_progress(TopicFilter) when is_binary(TopicFilter) ->
    ok = stop_all_consumers(),
    {ok, MQ} = emqx_mq_registry:find(TopicFilter),
    ok = emqx_mq_consumer_db:drop_consumer_data(MQ);
cleanup_mq_regular_consumption_progress(TopicFilters) when is_list(TopicFilters) ->
    lists:foreach(
        fun(TopicFilter) ->
            cleanup_mq_regular_consumption_progress(TopicFilter)
        end,
        TopicFilters
    ).

populate_regular(TopicFilter, N, BatchSize) ->
    MQ = create_mq_regular(TopicFilter),
    populate_regular(MQ, N, 0, now_ms_monotonic(), BatchSize).

info_regular(TopicFilter) ->
    #{
        subscribers => subsctriber_info(TopicFilter),
        consumer => consumer_info_regular(TopicFilter)
    }.

subsctriber_info(TopicFilter) ->
    lists:flatmap(
        fun(ChanPid) ->
            case emqx_mq_utils:mq_info(ChanPid, TopicFilter) of
                undefined ->
                    [];
                Info ->
                    [Info]
            end
        end,
        emqx_cm:all_channels()
    ).

consumer_info_regular(TopicFilter) ->
    {ok, MQ} = emqx_mq_registry:find(TopicFilter),
    case emqx_mq_consumer_db:find_consumer(MQ, now_ms()) of
        {ok, Pid} ->
            try
                emqx_mq_consumer:info(Pid, 1000)
            catch
                exit:{timeout, _} ->
                    #{error => timeout, pid => Pid}
            end;
        Other ->
            Other
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

populate_regular(#{topic_filter := TopicFilter} = MQ, N, NGenerated, StartTime, BatchSize) when
    NGenerated < N
->
    ActualBatchSize = min(BatchSize, N - NGenerated),
    Messages = generate_regular_messages(NGenerated, NGenerated + ActualBatchSize - 1),
    ok = emqx_mq_message_db:insert(MQ, Messages),
    io:format("populate_regular tf=~p generated ~p(~p), elapsed ~p~n", [
        TopicFilter, N, NGenerated, now_ms_monotonic() - StartTime
    ]),
    populate_regular(MQ, N, NGenerated + BatchSize, StartTime, BatchSize);
populate_regular(_MQ, _N, _NGenerated, _StartTime, _BatchSize) ->
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

now_ms() ->
    erlang:system_time(millisecond).

stop_all_consumers() ->
    ConsumerPids = [Pid || {_, Pid, _, _} <- supervisor:which_children(emqx_mq_consumer_sup)],
    ok = lists:foreach(
        fun(Pid) ->
            ok = emqx_mq_consumer:stop(Pid)
        end,
        ConsumerPids
    ).
