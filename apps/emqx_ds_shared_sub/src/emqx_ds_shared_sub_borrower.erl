%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc State machine for a single subscription of a shared subscription agent.
%% Implements Shared subscriber described in
%% https://github.com/emqx/eip/blob/main/active/0028-durable-shared-subscriptions.md

-module(emqx_ds_shared_sub_borrower).

-include_lib("emqx/include/logger.hrl").
-include("emqx_ds_shared_sub_config.hrl").
-include("emqx_ds_shared_sub_proto.hrl").
-include("emqx_ds_shared_sub_format.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    new/1,

    %% Self-initiated/leader messages
    on_info/2,

    %% Callbacks from the enclosing session
    on_stream_progress/2,
    on_disconnect/2,
    on_unsubscribe/1
]).

-export_type([
    t/0,
    options/0
]).

-type options() :: #{
    session_id := emqx_persistent_session_ds:id(),
    id := emqx_ds_shared_sub_proto:borrower_id(),
    share_topic_filter := emqx_persistent_session_ds:share_topic_filter(),
    send_after := fun((non_neg_integer(), term()) -> reference())
}.

-type progress() :: emqx_persistent_session_ds_shared_subs:progress().

%% Events that SSsubscriber propagates to the Agent which in turn
%% updates the session.

-type to_agent_events() ::
    #{
        type := lease,
        stream := emqx_ds:stream(),
        progress := progress()
    }
    | #{
        type := revoke,
        stream := emqx_ds:stream()
    }.

%% Borrower states

-define(connecting, connecting).
-define(connected, connected).
-define(unsubscribing, unsubscribing).

-type status() :: ?connecting | ?connected | ?unsubscribing.

-define(is_connecting(St), (map_get(status, St) =:= ?connecting)).
-define(is_connected(St), (map_get(status, St) =:= ?connected)).
-define(is_unsubscribing(St), (map_get(status, St) =:= ?unsubscribing)).

%% Possible status transitions are:
%% connecting -> connected -> unsubscribing

%% Individual stream states

%% Stream is granted by the leader, and we are consuming it
-define(stream_granted, stream_granted).
%% Stream is being revoked by the leader, and we are waiting for the session
%% to reach a consistent state of stream consumption.
-define(stream_revoking, stream_revoking).

%% Possible stream transitions are as follows:
%%
%% [no stream] -> stream_granted -> stream_revoking -> [no stream]

-type stream_status() :: ?stream_granted | ?stream_revoking.

-type stream_data() :: #{
    %% See description of the statuses above
    status := stream_status(),
    %% Progress of the stream, basically, DS iterator
    progress := progress(),
    %% We set this to true when the stream is being revoked and
    %% the enclosing session informed us that it has finished
    %% consuming the stream.
    use_finished := boolean()
}.

%% Timers

%% This timer is used in the `connecting` state only. After it
%% expires, the subscriber will request a leader again.
-define(find_leader_timer, find_leader_timer).

%% This timer is used in the `connected` and `unsubscribing` states.
%% It is used to issue a ping to the leader to mutually confirm
%% that the both are still alive.
-define(ping_leader_timer, ping_leader_timer).

%% This timer is active after a ping request is issued.
%% If it expires, we consider the leader lost and will reset the
%% subscriber.
-define(ping_leader_timeout_timer, ping_leader_timeout_timer).

%% This timer is used in the `unsubscribing` state only. It is used
%% to wait for the session to reach a consistent state of stream
%% consumption so that we can safely report stream progress and
%% disconnect from the leader.
-define(unsubscribe_timer, unsubscribe_timer).

-type t() :: #{
    session_id := emqx_persistent_session_ds:id(),
    share_topic_filter := emqx_persistent_session_ds:share_topic_filter(),
    id := emqx_persistent_session_ds:id(),
    send_after := fun((non_neg_integer(), term()) -> reference()),

    status := status(),
    leader => emqx_ds_shared_sub_proto:leader(),
    streams := #{emqx_ds:stream() => stream_data()},
    timers := #{
        ?find_leader_timer => emqx_maybe:t(reference()),
        ?ping_leader_timer => emqx_maybe:t(reference()),
        ?ping_leader_timeout_timer => emqx_maybe:t(reference()),
        ?unsubscribe_timer => emqx_maybe:t(reference())
    }
}.

-type message() :: term().

-type response() ::
    {ok, list(to_agent_events()), t()}
    | {stop, list(to_agent_events())}
    | {reset, list(to_agent_events())}.

%%-----------------------------------------------------------------------
%% API
%%-----------------------------------------------------------------------

-spec new(options()) -> t().
new(#{
    session_id := SessionId,
    id := Id,
    share_topic_filter := ShareTopicFilter,
    send_after := SendAfter
}) ->
    St = #{
        session_id => SessionId,
        share_topic_filter => ShareTopicFilter,
        id => Id,
        send_after => SendAfter,

        status => ?connecting,
        streams => #{},
        timers => #{}
    },
    ok = emqx_ds_shared_sub_registry:leader_wanted(Id, ShareTopicFilter),
    ensure_timer(St, ?find_leader_timer).

%%-----------------------------------------------------------------------
%% Event Handlers
%%-----------------------------------------------------------------------

-spec on_info(t(), message()) -> response().
%%
%% Obviously a leader election error
%%
on_info(#{leader := Leader} = St, ?leader_message_match(OtherLeader)) when Leader =/= OtherLeader ->
    {ok, [], St};
%%
%% Connecting
%%
on_info(St0, ?leader_connect_response_match(Leader)) when ?is_connecting(St0) ->
    St1 = St0#{status => ?connected, leader => Leader},
    St2 = cancel_timer(St1, ?find_leader_timer),
    St3 = ensure_timer(St2, ?ping_leader_timer),
    {ok, [], St3};
on_info(#{id := Id, share_topic_filter := ShareTopicFilter} = St, ?find_leader_timer) when
    ?is_connecting(St)
->
    ?tp(debug, ds_shared_sub_borrower_find_leader_timeout, #{
        borrower_id => ?format_borrower_id(Id),
        share_topic_filter => ShareTopicFilter
    }),
    ok = emqx_ds_shared_sub_registry:leader_wanted(Id, ShareTopicFilter),
    {ok, [], ensure_timer(St, ?find_leader_timer)};
%%
%% Ping leader, status independent
%%
on_info(St0, ?leader_ping_response_match(_)) ->
    St1 = cancel_timer(St0, ?ping_leader_timeout_timer),
    St2 = ensure_timer(St1, ?ping_leader_timer),
    {ok, [], St2};
on_info(#{session_id := SessionId, id := Id} = St, ?ping_leader_timer) ->
    ?tp(debug, ds_shared_sub_borrower_ping_leader, #{
        session_id => SessionId,
        borrower_id => ?format_borrower_id(Id)
    }),
    ok = notify_ping(St),
    {ok, [], ensure_timer(St, ?ping_leader_timeout_timer)};
on_info(#{session_id := SessionId, id := Id} = St, ?ping_leader_timeout_timer) ->
    ?tp(warning, ds_shared_sub_borrower_ping_leader_timeout, #{
        session_id => SessionId,
        borrower_id => ?format_borrower_id(Id)
    }),
    reset(St, revoke_all_events(St));
%%
%% Grant stream
%%
on_info(St0, ?leader_grant_match(_Leader, _StreamProgress)) when ?is_unsubscribing(St0) ->
    %% If unsubscribing, ignore the grant
    {ok, [], St0};
on_info(St0, ?leader_grant_match(Leader, _StreamProgress) = Msg) when ?is_connecting(St0) ->
    St1 = St0#{leader => Leader, status => ?connected},
    St2 = cancel_timer(St1, ?find_leader_timer),
    St3 = ensure_timer(St2, ?ping_leader_timer),
    on_info(St3, Msg);
on_info(
    #{streams := Streams0, session_id := SessionId, id := Id} = St0,
    ?leader_grant_match(_Leader, #{stream := Stream, progress := Progress0})
) when ?is_connected(St0) ->
    ?tp(debug, ds_shared_sub_borrower_leader_grant, #{
        session_id => SessionId,
        borrower_id => ?format_borrower_id(Id)
    }),
    {Events, Streams1} =
        case Streams0 of
            #{Stream := _} ->
                {[], Streams0};
            #{} ->
                {
                    [lease_event(Stream, Progress0)],
                    Streams0#{Stream => new_stream_data(Progress0)}
                }
        end,
    St1 = St0#{streams => Streams1},
    ok = notify_progress(St1, Stream),
    {ok, Events, St1};
%%
%% Revoke stream
%%
on_info(#{session_id := SessionId, id := Id} = St, ?leader_revoke_match(_Leader, Stream)) when
    ?is_connecting(St)
->
    %% Should never happen.
    ?tp(warning, ds_shared_sub_borrower_leader_revoke_while_connecting, #{
        session_id => SessionId,
        borrower_id => ?format_borrower_id(Id),
        stream => ?format_stream(Stream)
    }),
    reset(St);
on_info(St0, ?leader_revoke_match(_Leader, _Stream)) when ?is_unsubscribing(St0) ->
    %% If unsubscribing, ignore the revoke — we are revoking everything ourselves.
    {ok, [], St0};
on_info(
    #{streams := Streams0, session_id := SessionId, id := Id} = St0,
    ?leader_revoke_match(_Leader, Stream)
) when
    ?is_connected(St0)
->
    case Streams0 of
        #{Stream := #{status := ?stream_revoking}} ->
            ok = notify_progress(St0, Stream),
            {ok, [], St0};
        #{Stream := #{status := ?stream_granted} = StreamData} ->
            Streams1 = Streams0#{
                Stream => StreamData#{status => ?stream_revoking}
            },
            St1 = St0#{streams => Streams1},
            ok = notify_progress(St1, Stream),
            {ok, [revoke_event(Stream)], St1};
        #{} ->
            %% Should never happen.
            ?tp(warning, ds_shared_sub_borrower_leader_revoke_unknown_stream, #{
                session_id => SessionId,
                borrower_id => ?format_borrower_id(Id),
                stream => ?format_stream(Stream)
            }),
            reset(St0, revoke_all_events(St0))
    end;
%%
%% Revoke finalization
%%
on_info(#{session_id := SessionId, id := Id} = St, ?leader_revoked_match(_Leader, Stream)) when
    ?is_connecting(St)
->
    %% Should never happen.
    ?tp(warning, ds_shared_sub_borrower_leader_revoked_while_connecting, #{
        session_id => SessionId,
        borrower_id => ?format_borrower_id(Id),
        stream => ?format_stream(Stream)
    }),
    reset(St);
on_info(St, ?leader_revoked_match(_Leader, _Stream)) when ?is_unsubscribing(St) ->
    {ok, [], St};
on_info(#{streams := Streams0} = St0, ?leader_revoked_match(_Leader, Stream)) when
    ?is_connected(St0)
->
    {Events, Streams1} =
        case Streams0 of
            #{Stream := #{status := ?stream_revoking}} ->
                {[], maps:remove(Stream, Streams0)};
            #{Stream := #{status := ?stream_granted}} ->
                {[revoke_event(Stream)], maps:remove(Stream, Streams0)};
            #{} ->
                {[], Streams0}
        end,
    St1 = St0#{streams => Streams1},
    ok = notify_revoke_finished(St1, Stream),
    {ok, Events, St1};
%%
%% Invalidate
%%
on_info(#{session_id := SessionId, id := Id} = St, ?leader_invalidate_match(_Leader)) ->
    ?tp(warning, ds_shared_sub_borrower_leader_invalidate, #{
        session_id => SessionId,
        borrower_id => ?format_borrower_id(Id)
    }),
    reset(St, revoke_all_events(St));
%%
%% Unsubscribe timeout
%%
on_info(St, ?unsubscribe_timer) when ?is_unsubscribing(St) ->
    ok = notify_disconnect(St),
    stop(St).

on_disconnect(St0, Progresses) ->
    St1 = update_progresses(St0, Progresses),
    ok = notify_disconnect(St1).

on_unsubscribe(St) when ?is_connecting(St) ->
    stop(St);
on_unsubscribe(St) when ?is_unsubscribing(St) ->
    {ok, [], St};
on_unsubscribe(St0) when ?is_connected(St0) ->
    case has_unfinished_streams(St0) of
        true ->
            St1 = St0#{status => ?unsubscribing},
            St2 = ensure_timer(St1, ?unsubscribe_timer),
            {ok, [], St2};
        false ->
            ok = notify_disconnect(St0),
            stop(St0)
    end.

on_stream_progress(St, _StreamProgresses) when ?is_connecting(St) ->
    {ok, [], St};
on_stream_progress(St0, StreamProgresses) when ?is_connected(St0) ->
    St1 = update_progresses(St0, StreamProgresses),
    ok = lists:foreach(
        fun(#{stream := Stream}) ->
            ok = notify_progress(St1, Stream)
        end,
        StreamProgresses
    ),
    {ok, [], St1};
on_stream_progress(St0, StreamProgresses) when ?is_unsubscribing(St0) ->
    St1 = update_progresses(St0, StreamProgresses),
    case has_unfinished_streams(St1) of
        true ->
            {ok, [], St1};
        false ->
            ok = notify_disconnect(St1),
            stop(St1)
    end.

%%-----------------------------------------------------------------------
%% Helpers
%%-----------------------------------------------------------------------

new_stream_data(Progress) ->
    #{
        status => ?stream_granted,
        progress => Progress,
        use_finished => false
    }.

lease_event(Stream, Progress) ->
    #{
        type => lease,
        stream => Stream,
        progress => Progress
    }.

revoke_event(Stream) ->
    #{
        type => revoke,
        stream => Stream
    }.

revoke_all_events(#{streams := Streams}) ->
    [revoke_event(Stream) || {Stream, #{status := ?stream_granted}} <- maps:to_list(Streams)].

has_unfinished_streams(#{streams := Streams}) ->
    lists:any(fun(#{use_finished := UseFinished}) -> not UseFinished end, maps:values(Streams)).

update_progresses(St0, Progresses) ->
    lists:foldl(
        fun(StreamProgress, St) -> update_progress(St, StreamProgress) end,
        St0,
        Progresses
    ).

update_progress(#{streams := Streams0} = St, #{
    stream := Stream, progress := Progress, use_finished := UseFinished
}) ->
    Streams1 =
        case Streams0 of
            #{Stream := StreamData} ->
                Streams0#{
                    Stream => StreamData#{
                        progress := Progress,
                        use_finished := UseFinished
                    }
                };
            #{} ->
                Streams0
        end,
    St#{streams => Streams1}.

stop(St) ->
    _ = cancel_all_timers(St),
    {stop, []}.

reset(St) ->
    reset(St, []).

reset(St, Events) ->
    _ = cancel_all_timers(St),
    {reset, Events}.

%%-----------------------------------------------------------------------
%% Messages to the leader
%%-----------------------------------------------------------------------

notify_progress(#{id := Id, leader := Leader, streams := Streams}, Stream) ->
    case Streams of
        #{Stream := #{progress := Progress, use_finished := UseFinished}} ->
            StreamProgress = #{
                stream => Stream,
                progress => Progress,
                use_finished => UseFinished
            },
            ok = emqx_ds_shared_sub_proto:send_to_leader(
                Leader, ?borrower_update_progress(Id, StreamProgress)
            );
        #{} ->
            ok
    end.

notify_revoke_finished(#{id := Id, leader := Leader}, Stream) ->
    ok = emqx_ds_shared_sub_proto:send_to_leader(Leader, ?borrower_revoke_finished(Id, Stream)).

notify_disconnect(St) when ?is_connecting(St) ->
    %% We have no leader to notify
    ok;
notify_disconnect(#{id := Id, leader := Leader, streams := Streams}) ->
    StreamProgresses = [
        #{
            stream => Stream,
            progress => Progress,
            use_finished => UseFinished
        }
     || {Stream, #{progress := Progress, use_finished := UseFinished}} <- maps:to_list(Streams)
    ],
    ok = emqx_ds_shared_sub_proto:send_to_leader(
        Leader, ?borrower_disconnect(Id, StreamProgresses)
    ).

notify_ping(#{id := Id, leader := Leader}) ->
    ok = emqx_ds_shared_sub_proto:send_to_leader(Leader, ?borrower_ping(Id)).

%%-----------------------------------------------------------------------
%% Timers
%%-----------------------------------------------------------------------

ensure_timer(#{send_after := SendAfter} = State0, TimerName) ->
    Timeout = timer_timeout(TimerName),
    State1 = cancel_timer(State0, TimerName),
    #{timers := Timers} = State1,
    TimerRef = SendAfter(Timeout, TimerName),
    State1#{
        timers := Timers#{TimerName => TimerRef}
    }.

cancel_timer(#{timers := Timers} = St, TimerName) ->
    case Timers of
        #{TimerName := MaybeTimerRef} ->
            ok = emqx_utils:cancel_timer(MaybeTimerRef);
        _ ->
            ok
    end,
    St#{
        timers := Timers#{TimerName => undefined}
    }.

cancel_all_timers(#{timers := Timers} = St) ->
    lists:foldl(
        fun(TimerName, StAcc) -> cancel_timer(StAcc, TimerName) end,
        St,
        maps:keys(Timers)
    ).

timer_timeout(?find_leader_timer) ->
    ?dq_config(session_find_leader_timeout, 5000);
timer_timeout(?ping_leader_timer) ->
    ?dq_config(session_ping_leader_interval, 4000);
timer_timeout(?ping_leader_timeout_timer) ->
    ?dq_config(session_ping_leader_timeout, 4000);
timer_timeout(?unsubscribe_timer) ->
    ?dq_config(session_unsubscribe_timeout, 1000).
