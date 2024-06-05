%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_agent).

-include_lib("emqx/include/emqx_persistent_message.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").

-export([
    new/1,
    open/2,

    on_subscribe/3,
    on_unsubscribe/2,
    on_stream_progress/2,
    on_info/2,

    renew_streams/1
]).

-behaviour(emqx_persistent_session_ds_shared_subs_agent).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

new(Opts) ->
    init_state(Opts).

open(TopicSubscriptions, Opts) ->
    State0 = init_state(Opts),
    State1 = lists:foldl(
        fun({ShareTopicFilter, #{start_time := StartTime}}, State) ->
            add_subscription(State, ShareTopicFilter, StartTime)
        end,
        State0,
        TopicSubscriptions
    ),
    State1.

on_subscribe(State0, TopicFilter, _SubOpts) ->
    StartTime = now_ms(),
    State1 = add_subscription(State0, TopicFilter, StartTime),
    {ok, State1}.

on_unsubscribe(State, TopicFilter) ->
    delete_subscription(State, TopicFilter).

renew_streams(State0) ->
    State1 = do_renew_streams(State0),
    {State2, StreamLeases} = stream_leases(State1),
    {StreamLeases, [], State2}.

on_stream_progress(State, _StreamProgress) ->
    State.

on_info(State, _Info) ->
    State.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

init_state(Opts) ->
    SessionId = maps:get(session_id, Opts),
    SendFuns = maps:get(send_funs, Opts),
    Send = maps:get(send, SendFuns),
    SendAfter = maps:get(send_after, SendFuns),
    #{
        session_id => SessionId,
        send => Send,
        end_after => SendAfter,
        subscriptions => #{}
    }.

% send(State, Pid, Msg) ->
%     Send = maps:get(send, State),
%     Send(Pid, Msg).

% send_after(State, Time, Pid, Msg) ->
%     SendAfter = maps:get(send_after, State),
%     SendAfter(Time, Pid, Msg).

do_renew_streams(#{subscriptions := Subs0} = State0) ->
    Subs1 = maps:map(
        fun(
            ShareTopicFilter,
            #{start_time := StartTime, streams := Streams0, stream_leases := StreamLeases} = Sub
        ) ->
            #share{topic = TopicFilterRaw} = ShareTopicFilter,
            TopicFilter = emqx_topic:words(TopicFilterRaw),
            {_, NewStreams} = lists:unzip(
                emqx_ds:get_streams(?PERSISTENT_MESSAGE_DB, TopicFilter, StartTime)
            ),
            {Streams1, NewLeases} = lists:foldl(
                fun(Stream, {StreamsAcc, LeasesAcc}) ->
                    case StreamsAcc of
                        #{Stream := _} ->
                            {StreamsAcc, LeasesAcc};
                        _ ->
                            {ok, It} = emqx_ds:make_iterator(
                                ?PERSISTENT_MESSAGE_DB, Stream, TopicFilter, StartTime
                            ),
                            StreamLease = #{
                                topic_filter => ShareTopicFilter,
                                stream => Stream,
                                iterator => It
                            },
                            {StreamsAcc#{Stream => It}, [StreamLease | LeasesAcc]}
                    end
                end,
                {Streams0, []},
                NewStreams
            ),
            Sub#{streams => Streams1, stream_leases => StreamLeases ++ NewLeases}
        end,
        Subs0
    ),
    State0#{subscriptions => Subs1}.

delete_subscription(#{session_id := SessionId, subscriptions := Subs0} = State0, ShareTopicFilter) ->
    #share{topic = TopicFilter} = ShareTopicFilter,
    ok = emqx_persistent_session_ds_router:do_delete_route(TopicFilter, SessionId),
    Subs1 = maps:remove(ShareTopicFilter, Subs0),
    State0#{subscriptions => Subs1}.

stream_leases(#{subscriptions := Subs0} = State0) ->
    {Subs1, StreamLeases} = lists:foldl(
        fun({TopicFilter, #{stream_leases := Leases} = Sub}, {SubsAcc, LeasesAcc}) ->
            {SubsAcc#{TopicFilter => Sub#{stream_leases => []}}, [Leases | LeasesAcc]}
        end,
        {Subs0, []},
        maps:to_list(Subs0)
    ),
    State1 = State0#{subscriptions => Subs1},
    {State1, lists:concat(StreamLeases)}.

now_ms() ->
    erlang:system_time(millisecond).

add_subscription(
    #{subscriptions := Subs0, session_id := SessionId} = State0, ShareTopicFilter, StartTime
) ->
    #share{topic = TopicFilter} = ShareTopicFilter,
    ok = emqx_persistent_session_ds_router:do_add_route(TopicFilter, SessionId),
    Subs1 = Subs0#{
        ShareTopicFilter => #{
            start_time => StartTime,
            streams => #{},
            stream_leases => []
        }
    },
    State1 = State0#{subscriptions => Subs1},
    State1.
