%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc State machine for a single subscription of a shared subscription agent.
%% Implements Shared subscriber described in
%% https://github.com/emqx/eip/blob/main/active/0028-durable-shared-subscriptions.md

%% Referred as `ssubscriber` outside of this module
-module(emqx_ds_shared_sub_subscriber).

-include_lib("emqx/include/logger.hrl").
-include("emqx_ds_shared_sub_config.hrl").
-include("emqx_ds_shared_sub_proto.hrl").
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
    id := emqx_ds_shared_sub_proto:agent(),
    share_topic_filter := emqx_persistent_session_ds:share_topic_filter(),
    send_after := fun((non_neg_integer(), term()) -> reference())
}.

-type progress() :: emqx_persistent_session_ds_shared_subs:progress().

-type stream_leader_event() ::
    #{
        type := lease,
        stream := emqx_ds:stream(),
        progress := progress()
    }
    | #{
        type := revoke,
        stream := emqx_ds:stream()
    }.

%% SSubscriber states

-define(connecting, connecting).
-define(connected, connected).
-define(unsubscribing, unsubscribing).

%% Individual stream states

%% Stream is granted by the leader, and we are consuming it
-define(stream_granted, stream_granted).
%% Stream is being revoked by the leader, and we are waiting for the session
%% to reach a consistent state of stream consumption
-define(stream_revoke_fininshing, stream_revoke_fininshing).
%% Stream is being revoked by the leader, and we stopped consuming it.
%% We are waiting for the leader to finish the revocation process
-define(stream_revoke_finished, stream_revoke_finished).

%% Possible stream transitions are:
%%
%% [no stream] -> stream_granted
%% stream_granted -> stream_revoke_fininshing
%% stream_revoke_fininshing -> [no streamm]

-type status() :: ?connecting | ?connected | ?unsubscribing.

%% Possible transitions are:
%% connecting -> connected
%% connected -> unsubscribing

-type stream_status() :: ?stream_granted | ?stream_revoke_fininshing | ?stream_revoke_finished.

-type stream_data() :: #{
    status := stream_status(),
    progress := progress()
}.

%% Timers

-define(find_leader_timer, find_leader_timer).
-define(ping_leader_timer, ping_leader_timer).
-define(ping_leader_timeout_timer, ping_leader_timeout_timer).
-define(unsubscribe_timer, unsubscribe_timer).

-type t() :: #{
    %% Persistent state
    session_id := emqx_persistent_session_ds:id(),
    share_topic_filter := emqx_persistent_session_ds:share_topic_filter(),
    id := emqx_persistent_session_ds:id(),
    send_after := fun((non_neg_integer(), term()) -> reference()),

    status := status(),
    leader => emqx_ds_shared_sub_proto:leader(),
    streams => #{emqx_ds:stream() => stream_data()},
    timers => #{
        ?find_leader_timer => emqx_maybe:t(reference()),
        ?ping_leader_timer => emqx_maybe:t(reference()),
        ?ping_leader_timeout_timer => emqx_maybe:t(reference()),
        ?unsubscribe_timer => emqx_maybe:t(reference())
    }
}.

-type message() :: term().

-type response() ::
    {ok, list(stream_leader_event()), t()}
    | {stop, list(stream_leader_event())}
    | {reset, list(stream_leader_event())}.

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
        timers => #{}
    },
    ok = emqx_ds_shared_sub_registry:leader_wanted(Id, ShareTopicFilter),
    ensure_timer(St, ?find_leader_timer).

%%-----------------------------------------------------------------------
%% Event Handlers
%%-----------------------------------------------------------------------

-spec on_info(t(), message()) -> response().
on_info(#{leader := Leader} = St, ?leader_message_match(OtherLeader)) when Leader =/= OtherLeader ->
    {ok, [], St};
on_info(#{status := ?connecting} = St0, ?leader_connect_response_match(Leader)) ->
    St1 = St0#{
        status => ?connected,
        leader => Leader
    },
    {ok, [], ensure_timer(St1, ?ping_leader_timer)};
on_info(
    #{status := ?connecting, id := Id, share_topic_filter := ShareTopicFilter} = St0,
    ?find_leader_timer
) ->
    ok = emqx_ds_shared_sub_registry:leader_wanted(Id, ShareTopicFilter),
    {ok, [], ensure_timer(St0, ?find_leader_timer)};
on_info(St0, ?leader_ping_response_match(_)) ->
    St1 = cancel_timer(St0, ?ping_leader_timeout_timer),
    St2 = ensure_timer(St1, ?ping_leader_timer),
    {ok, [], St2};
on_info(#{status := ?connected, id := Id, leader := Leader} = St, ?ping_leader_timer) ->
    ok = emqx_ds_shared_sub_proto:ping_leader_v3(Leader, Id),
    {ok, [], ensure_timer(St, ?ping_leader_timer)};
on_info(#{status := ?connected} = St, ?ping_leader_timeout_timer) ->
    {reset, [], St}.

on_disconnect(_St, _Progresses) ->
    %% TODO
    ok.

on_unsubscribe(#{status := ?connected} = St) ->
    %% TODO
    {ok, [], St#{
        status => ?unsubscribing
    }};
on_unsubscribe(#{status := ?connecting} = St) ->
    {stop, St}.

%% TODO
on_stream_progress(#{status := ?connecting} = St, _Progress) ->
    {ok, [], St};
on_stream_progress(#{status := ?unsubscribing} = St, _Progress) ->
    {ok, [], St}.

%%-----------------------------------------------------------------------
%% Helpers
%%-----------------------------------------------------------------------

%%-----------------------------------------------------------------------
%% Timers

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

timer_timeout(?find_leader_timer) ->
    ?dq_config(session_find_leader_timeout_ms, 5000);
timer_timeout(?ping_leader_timer) ->
    ?dq_config(session_ping_leader_interval_ms, 5000);
timer_timeout(?ping_leader_timeout_timer) ->
    ?dq_config(session_ping_leader_timeout_ms, 5000);
timer_timeout(?unsubscribe_timer) ->
    ?dq_config(session_unsubscribe_timeout_ms, 1000).
