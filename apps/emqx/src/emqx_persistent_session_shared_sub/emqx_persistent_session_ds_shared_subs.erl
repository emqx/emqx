%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_persistent_session_ds_shared_subs).

-include("emqx_mqtt.hrl").
-include("../emqx_persistent_session_ds.hrl").
-include("logger.hrl").

-include_lib("snabbkaffe/include/trace.hrl").

-record(state, {
    sub_data = #{} :: #{
        emqx_persistent_session_ds:subscription_id() => shared_sub_data()
    },
    send :: fun((pid(), term()) -> term()),
    send_after :: fun((non_neg_integer(), pid(), term()) -> reference())
}).

-define(connecting, connecting).
-define(replaying, replaying).
-define(updating, updating).

-type state() :: #state{}.
-type shared_sub_data() ::
    {?connecting, connecting_data()}
    | {?replaying, replaying_data()}
    | {?updating, updating_data()}.

-type connecting_data() :: #{}.
-type replaying_data() :: #{
    emqx_persistent_session_ds_state:stream_key() => emqx_persistent_session_ds:stream_state()
}.
-type updating_data() :: #{}.

-type shared_topic_filter() :: #share{}.

-type opts() :: #{
    send_funs := #{
        send := fun((pid(), term()) -> term()),
        send_after := fun((non_neg_integer(), pid(), term()) -> reference())
    }
}.

-export([
    open/2,
    new/1,

    on_subscribe/3,
    on_unsubscribe/4,
    on_session_drop/2,

    on_info/3,

    find_new_streams/2,
    renew_streams/2,
    put_stream/3,

    to_map/2
]).

-export_type([
    state/0,
    shared_topic_filter/0
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec new(opts()) -> state().
new(#{
    send_funs := #{
        send := Send,
        send_after := SendAfter
    }
}) ->
    #state{sub_data = #{}, send = Send, send_after = SendAfter}.

-spec open(emqx_persistent_session_ds_state:t(), opts()) ->
    {ok, emqx_persistent_session_ds_state:t(), state()}.
open(S, #{
    send_funs := #{
        send := Send,
        send_after := SendAfter
    }
}) ->
    %% TODO: Restore subscriptions from S
    {ok, S, #state{sub_data = #{}, send = Send, send_after = SendAfter}}.

-spec on_subscribe(
    shared_topic_filter(), emqx_types:subopts(), emqx_persistent_session_ds:session()
) ->
    {ok, emqx_persistent_session_ds_state:t(), state()} | {error, ?RC_QUOTA_EXCEEDED}.
on_subscribe(#share{topic = TopicFilter} = SharedTopicFilter, SubOpts, #{
    id := SessionId, s := S0, shared_sub_s := SharedSubS0, props := Props
}) ->
    ?SLOG(warning, #{
        msg => on_subscribe,
        session_id => SessionId,
        topic_filter => SharedTopicFilter,
        subopts => SubOpts,
        props => Props
    }),

    #{upgrade_qos := UpgradeQoS, max_subscriptions := MaxSubscriptions} = Props,
    case emqx_persistent_session_ds_state:get_subscription(SharedTopicFilter, S0) of
        undefined ->
            %% This is a new subscription:
            case emqx_persistent_session_ds_state:n_subscriptions(S0) < MaxSubscriptions of
                true ->
                    %% TODO: The group leaedr should add the route.
                    ok = emqx_persistent_session_ds_router:do_add_route(TopicFilter, SessionId),

                    {SubId, S1} = emqx_persistent_session_ds_state:new_id(S0),
                    {SStateId, S2} = emqx_persistent_session_ds_state:new_id(S1),
                    SState = #{
                        parent_subscription => SubId, upgrade_qos => UpgradeQoS, subopts => SubOpts
                    },
                    S3 = emqx_persistent_session_ds_state:put_subscription_state(
                        SStateId, SState, S2
                    ),
                    Subscription = #{
                        id => SubId,
                        current_state => SStateId,
                        start_time => now_ms()
                    },
                    S = emqx_persistent_session_ds_state:put_subscription(
                        SharedTopicFilter, Subscription, S3
                    ),
                    ?tp(persistent_session_ds_shared_subscription_added, #{
                        topic_filter => SharedTopicFilter, session => SessionId
                    }),
                    {ok, S, init_subscription(SharedTopicFilter, S, SharedSubS0)};
                false ->
                    {error, ?RC_QUOTA_EXCEEDED}
            end;
        Sub0 = #{current_state := SStateId0, id := SubId} ->
            SState = #{parent_subscription => SubId, upgrade_qos => UpgradeQoS, subopts => SubOpts},
            case emqx_persistent_session_ds_state:get_subscription_state(SStateId0, S0) of
                SState ->
                    %% Client resubscribed with the same parameters:
                    {ok, S0, SharedSubS0};
                _ ->
                    %% Subsription parameters changed:
                    {SStateId, S1} = emqx_persistent_session_ds_state:new_id(S0),
                    S2 = emqx_persistent_session_ds_state:put_subscription_state(
                        SStateId, SState, S1
                    ),
                    Sub = Sub0#{current_state => SStateId},
                    S = emqx_persistent_session_ds_state:put_subscription(
                        SharedTopicFilter, Sub, S2
                    ),
                    {ok, S, SharedSubS0}
            end
    end.

-spec on_unsubscribe(
    emqx_persistent_session_ds:id(),
    shared_topic_filter(),
    emqx_persistent_session_ds_state:t(),
    state()
) ->
    {ok, emqx_persistent_session_ds_state:t(), state(), emqx_persistent_session_ds:subscription()}
    | {error, ?RC_NO_SUBSCRIPTION_EXISTED}.
on_unsubscribe(SessionId, #share{topic = TopicFilter} = SharedTopicFilter, S0, SharedSubS0) ->
    case lookup(SharedTopicFilter, S0) of
        undefined ->
            {error, ?RC_NO_SUBSCRIPTION_EXISTED};
        #{id := SubId} = Subscription ->
            ?tp(persistent_session_ds_shared_subscription_delete, #{
                session_id => SessionId, topic_filter => SharedTopicFilter
            }),
            %% TODO: the group leader should delete the route.
            ok = emqx_persistent_session_ds_router:do_delete_route(TopicFilter, SessionId),
            SharedSubS1 = del_sub_data(SubId, SharedSubS0),
            {ok, emqx_persistent_session_ds_state:del_subscription(SharedTopicFilter, S0),
                SharedSubS1, Subscription}
    end.

-spec on_session_drop(emqx_persistent_session_ds:id(), emqx_persistent_session_ds_state:t()) -> ok.
on_session_drop(_SessionId, _S) -> ok.

-spec renew_streams(emqx_persistent_session_ds_state:t(), state()) ->
    {emqx_persistent_session_ds_state:t(), state()}.
renew_streams(S, SharedSubS0) ->
    renew_streams_stub0(S, SharedSubS0).

-spec find_new_streams(emqx_persistent_session_ds_state:t(), state()) ->
    [{emqx_persistent_session_ds_state:stream_key(), emqx_persistent_session_ds:stream_state()}].
find_new_streams(S, SharedSubS) ->
    find_new_streams_stub(S, SharedSubS).

-spec put_stream(
    emqx_persistent_session_ds_state:stream_key(),
    emqx_persistent_session_ds:stream_state(),
    state()
) -> state().
put_stream({SubId, Stream}, StreamState, SharedSubS) ->
    {?replaying, SubData0} = get_sub_data(SubId, SharedSubS),
    SubData1 = maps:put(Stream, StreamState, SubData0),
    put_sub_data(SubId, SubData1, SharedSubS).

-spec to_map(emqx_persistent_session_ds_state:t(), state()) -> map().
to_map(_SessionS, _S) -> #{}.

-spec on_info(_Message :: term(), emqx_persistent_session_ds_state:t(), state()) ->
    {emqx_persistent_session_ds_state:t(), state()}.
on_info(renew_streams, S, SharedSubS) ->
    renew_streams_stub1(S, SharedSubS);
on_info(Message, S, SharedSubS) ->
    ?SLOG(warning, #{
        msg => on_info,
        message => Message
    }),
    {S, SharedSubS}.

%% @doc Fold over active subscriptions:
-spec fold(
    fun((emqx_types:topic(), emqx_persistent_session_ds:subscription(), Acc) -> Acc),
    Acc,
    emqx_persistent_session_ds_state:t()
) ->
    Acc.
fold(Fun, Acc, S) ->
    emqx_persistent_session_ds_state:fold_subscriptions(
        fun
            (#share{} = SharedTopicFilter, Sub, Acc0) -> Fun(SharedTopicFilter, Sub, Acc0);
            (_SharedTopicFilter, _Sub, Acc0) -> Acc0
        end,
        Acc,
        S
    ).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

init_subscription(SharedTopicFilter, S, SharedSubS0) ->
    init_subscription_stub(SharedTopicFilter, S, SharedSubS0).

lookup(SharedTopicFilter, S) ->
    case emqx_persistent_session_ds_state:get_subscription(SharedTopicFilter, S) of
        Sub = #{current_state := SStateId} ->
            case emqx_persistent_session_ds_state:get_subscription_state(SStateId, S) of
                #{subopts := SubOpts} ->
                    Sub#{subopts => SubOpts};
                undefined ->
                    undefined
            end;
        undefined ->
            undefined
    end.

now_ms() ->
    erlang:system_time(millisecond).

get_sub_data(SubId, #state{sub_data = SubData}) ->
    maps:get(SubId, SubData).

get_sub_data(SubId, #state{sub_data = SubData}, Default) ->
    maps:get(SubId, SubData, Default).

put_sub_data(SubId, NewStatus, NewData, #state{sub_data = SubData0} = State) ->
    SubData1 = maps:put(SubId, {NewStatus, NewData}, SubData0),
    State#state{sub_data = SubData1}.

del_sub_data(SubId, #state{sub_data = SubData0} = State) ->
    SubData1 = maps:remove(SubId, SubData0),
    State#state{sub_data = SubData1}.

put_sub_data(SubId, NewData, #state{sub_data = SubData0} = State) ->
    {Status, _OldData} = maps:get(SubId, SubData0),
    SubData1 = maps:put(SubId, {Status, NewData}, SubData0),
    State#state{sub_data = SubData1}.

%%--------------------------------------------------------------------
%% Stub implementations
%% Just to verify that we interact with session correctly
%%--------------------------------------------------------------------

init_subscription_stub(_SharedTopicFilter, _S, SharedSubS0) ->
    SharedSubS0.

renew_streams_stub0(S, #state{send_after = SendAfter} = SharedSubS) ->
    %% Handle asynchronously just to check message sending and receiving
    _ = SendAfter(0, self(), renew_streams),
    {S, SharedSubS}.

renew_streams_stub1(S, SharedSubS0) ->
    SharedSubS1 = fold(
        fun(SharedTopicFilter, _Sub, SharedSubSAcc) ->
            find_streams(SharedTopicFilter, S, SharedSubSAcc)
        end,
        SharedSubS0,
        S
    ),
    {S, SharedSubS1}.

find_new_streams_stub(S, SharedSubS) ->
    Comm1 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_1), S),
    Comm2 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_2), S),
    StreamStates = fold(
        fun(_SharedTopicFilter, #{id := SubId}, StreamAcc) ->
            {?replaying, SubData} = get_sub_data(SubId, SharedSubS),
            lists:filtermap(
                fun({Stream, StreamState}) ->
                    case
                        is_fully_acked(Comm1, Comm2, StreamState) andalso
                            not is_fully_replayed(Comm1, Comm2, StreamState)
                    of
                        true ->
                            {true, {{SubId, Stream}, StreamState}};
                        false ->
                            false
                    end
                end,
                maps:to_list(SubData)
            ) ++ StreamAcc
        end,
        [],
        S
    ),
    StreamStates.

%% It's a stub, no rank management, no progress persistence
find_streams(#share{topic = TopicFilter} = SharedTopicFilter, S, SharedSubS) ->
    #{start_time := StartTime, id := SubId, current_state := SStateId} =
        emqx_persistent_session_ds_state:get_subscription(SharedTopicFilter, S),
    TopicFilterWords = emqx_topic:words(TopicFilter),
    Streams = emqx_ds:get_streams(?PERSISTENT_MESSAGE_DB, TopicFilterWords, StartTime),
    ?SLOG(warning, #{
        msg => find_streams,
        topic_filter => SharedTopicFilter,
        streams => Streams
    }),
    {Status, SubData0} = get_sub_data(SubId, SharedSubS, {?replaying, #{}}),
    SubData1 = ensure_iterators(Streams, SStateId, TopicFilterWords, StartTime, SubData0),
    put_sub_data(SubId, Status, SubData1, SharedSubS).

ensure_iterators(
    Streams, SStateId, TopicFilterWords, StartTime, SubData0
) ->
    lists:foldl(
        fun({{RankX, RankY}, Stream}, SubData) ->
            case maps:get(Stream, SubData, undefined) of
                undefined ->
                    {ok, Iterator} = emqx_ds:make_iterator(
                        ?PERSISTENT_MESSAGE_DB, Stream, TopicFilterWords, StartTime
                    ),
                    NewStreamState = #srs{
                        rank_x = RankX,
                        rank_y = RankY,
                        it_begin = Iterator,
                        it_end = Iterator,
                        sub_state_id = SStateId,
                        stream_owner = ?owner_shared_sub
                    },
                    ?SLOG(warning, #{
                        msg => new_stream,
                        topic_filter => TopicFilterWords,
                        stream => Stream
                    }),
                    SubData#{Stream => NewStreamState};
                #srs{} ->
                    SubData
            end
        end,
        SubData0,
        Streams
    ).

is_fully_acked(_, _, #srs{
    first_seqno_qos1 = Q1, last_seqno_qos1 = Q1, first_seqno_qos2 = Q2, last_seqno_qos2 = Q2
}) ->
    %% Streams where the last chunk doesn't contain any QoS1 and 2
    %% messages are considered fully acked:
    true;
is_fully_acked(Comm1, Comm2, #srs{last_seqno_qos1 = S1, last_seqno_qos2 = S2}) ->
    (Comm1 >= S1) andalso (Comm2 >= S2).

is_fully_replayed(Comm1, Comm2, S = #srs{it_end = It}) ->
    It =:= end_of_stream andalso is_fully_acked(Comm1, Comm2, S).
