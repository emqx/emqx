%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_extsub_test_st_handler).

-behaviour(emqx_extsub_handler).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    handle_subscribe/4,
    handle_unsubscribe/3,
    handle_terminate/1,
    handle_delivered/4,
    handle_info/3
]).

-record(fake_msg, {
    n :: integer()
}).
-record(push_messages, {}).

handle_subscribe(
    _SubscribeType,
    #{send_after := SendAfterFn, send := SendFn} = _SubscribeCtx,
    undefined,
    <<"extsub_st_test/", Rest/binary>> = TopicFilter
) ->
    ?tp(debug, handle_subscribe, #{
        subscribe_type => _SubscribeType,
        subscribe_ctx => _SubscribeCtx,
        state => undefined,
        topic_filter => TopicFilter
    }),
    try
        [_Tag, BatchCountBin, BatchSizeBin, IntervalMsBin] = binary:split(Rest, <<"/">>, [global]),
        BatchCount = binary_to_integer(BatchCountBin),
        BatchSize = binary_to_integer(BatchSizeBin),
        IntervalMs = binary_to_integer(IntervalMsBin),
        ok = lists:foreach(
            fun(I) ->
                SendAfterFn(IntervalMs * I, #fake_msg{n = I})
            end,
            lists:seq(0, BatchCount - 1)
        ),
        {ok, #{
            buffer => buffer_new(),
            topic_filter => TopicFilter,
            batch_size => BatchSize,
            send => SendFn
        }}
    catch
        Class:Reason:Stacktrace ->
            ?tp(debug, handle_init_error, #{
                class => Class,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            ignore
    end;
handle_subscribe(_SubscribeType, _SubscribeCtx, undefined, TopicFilter) ->
    ?tp(debug, handle_init_ignore, #{topic_filter => TopicFilter}),
    ignore.

handle_unsubscribe(_UnsubscribeType, State, _TopicFilter) ->
    ?tp(debug, handle_terminate, #{state => State, topic_filter => _TopicFilter}),
    State.

handle_terminate(_State) ->
    ?tp(debug, handle_terminate, #{state => _State}),
    ok.

handle_delivered(
    #{send := SendFn} = State,
    #{desired_message_count := DesiredCount} = _AckCtx,
    _Message,
    _Ack
) ->
    case DesiredCount of
        0 ->
            ok;
        _ ->
            SendFn(#push_messages{})
    end,
    State.

handle_info(
    #{buffer := Buffer0} = State,
    #{desired_message_count := DesiredCount} = _InfoCtx,
    #fake_msg{} = Msg
) ->
    Buffer = buffer_in(Buffer0, make_messages(State, Msg)),
    push_messages(State#{buffer => Buffer}, DesiredCount);
handle_info(State, #{desired_message_count := DesiredCount} = _InfoCtx, #push_messages{}) ->
    push_messages(State, DesiredCount);
handle_info(State, _InfoCtx, Info) ->
    ?tp(warning, handle_info_unknown, #{info => Info, state => State}),
    {ok, State}.

push_messages(#{buffer := Buffer0} = State, DesiredCount) ->
    case DesiredCount of
        0 ->
            {ok, State};
        _ ->
            {Messages, Buffer} = buffer_out(Buffer0, DesiredCount),
            case Messages of
                [] ->
                    {ok, State#{buffer => Buffer}};
                _ ->
                    {ok, State#{buffer => Buffer}, Messages}
            end
    end.

%% Fake message generation functions

make_messages(#{batch_size := BatchSize} = State, #fake_msg{n = BatchN}) ->
    lists:reverse(
        lists:map(
            fun(I) ->
                make_message(State, BatchN, I, BatchSize)
            end,
            lists:seq(0, BatchSize - 1)
        )
    ).

make_message(#{topic_filter := TopicFilter} = _State, BatchN, I, _BatchSize) ->
    Body = iolist_to_binary(io_lib:format("fake msg batch_n=~p, n in batch=~p", [BatchN, I])),
    emqx_message:make(<<"from">>, ?QOS_1, TopicFilter, Body).

%% Toy buffer functions

buffer_new() ->
    queue:new().

buffer_in(Q, Messages) ->
    lists:foldl(
        fun(Msg, QAcc) ->
            queue:in(Msg, QAcc)
        end,
        Q,
        Messages
    ).

buffer_out(Q, N) ->
    buffer_out(Q, N, []).

buffer_out(Q, 0, Acc) ->
    {lists:reverse(Acc), Q};
buffer_out(Q, N, Acc) ->
    case queue:out(Q) of
        {{value, MessageEntry}, Q1} ->
            buffer_out(Q1, N - 1, [MessageEntry | Acc]);
        {empty, Q1} ->
            {lists:reverse(Acc), Q1}
    end.
