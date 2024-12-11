%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc Asynchronous messages between shared sub subscriber and shared sub leader.

%% NOTE
%% We do not need any kind of request/response identification,
%% because the protocol is fully event-based.

%% SSubscriber messages sent to the leader.
%% Leader talks to many SSubscribers, `ssubscriber_id` field is used to identify the sender.

-define(ssubscriber_connect(FromSSubscriberId, ShareTopicFilter), #{
    message_type => ssubscriber_connect,
    from_ssubscriber_id => FromSSubscriberId,
    share_topic_filter => ShareTopicFilter
}).

-define(ssubscriber_connect_match(FromSSubscriberId, ShareTopicFilter), #{
    message_type := ssubscriber_connect,
    from_ssubscriber_id := FromSSubscriberId,
    share_topic_filter := ShareTopicFilter
}).

-define(ssubscriber_ping(FromSSubscriberId), #{
    message_type => ssubscriber_ping,
    from_ssubscriber_id => FromSSubscriberId
}).

-define(ssubscriber_ping_match(FromSSubscriberId), #{
    message_type := ssubscriber_ping,
    from_ssubscriber_id := FromSSubscriberId
}).

-define(ssubscriber_update_progress_match(FromSSubscriberId, Stream, Progress), #{
    message_type := ssubscriber_update_progresses,
    from_ssubscriber_id := FromSSubscriberId,
    stream := Stream,
    progress := Progress
}).

-define(ssubscriber_update_progress(FromSSubscriberId, Stream, Progress), #{
    message_type => ssubscriber_update_progresses,
    from_ssubscriber_id => FromSSubscriberId,
    stream => Stream,
    progress => Progress
}).

-define(ssubscriber_revoke_finished_match(FromSSubscriberId, Stream), #{
    message_type := ssubscriber_revoke_finished,
    from_ssubscriber_id := FromSSubscriberId,
    stream := Stream
}).

-define(ssubscriber_revoke_finished(FromSSubscriberId, Stream), #{
    message_type => ssubscriber_revoke_finished,
    from_ssubscriber_id => FromSSubscriberId,
    stream => Stream
}).

-define(ssubscriber_disconnect_match(FromSSubscriberId, Progresses), #{
    message_type := ssubscriber_unsubscribe,
    from_ssubscriber_id := FromSSubscriberId,
    progresses := Progresses
}).

-define(ssubscriber_disconnect(FromSSubscriberId, Progresses), #{
    message_type => ssubscriber_unsubscribe,
    from_ssubscriber_id => FromSSubscriberId,
    progresses => Progresses
}).

%% Leader messages sent to the SSubscriber.

%% A common matcher for leader messages.
-define(leader_message_match(FromLeader), #{
    from_leader := FromLeader
}).

%% Respond to the SSubscriber's connection request.

-define(leader_connect_response(FromLeader), #{
    message_type => leader_connect_response,
    from_leader => FromLeader
}).

-define(leader_connect_response_match(FromLeader), #{
    message_type := leader_connect_response,
    from_leader := FromLeader
}).

%% Respond to the SSubscriber's ping request.

-define(leader_ping_response(FromLeader), #{
    message_type => leader_ping_response,
    from_leader => FromLeader
}).

-define(leader_ping_response_match(FromLeader), #{
    message_type := leader_ping_response,
    from_leader := FromLeader
}).

%% Grant a stream to the SSubscriber.

-define(leader_grant_match(FromLeader, Stream, Progress), #{
    message_type := leader_grant,
    from_leader := FromLeader,
    stream := Stream,
    progress := Progress
}).

-define(leader_grant(FromLeader, Stream, Progress), #{
    message_type => leader_grant,
    from_leader => FromLeader,
    stream => Stream,
    progress => Progress
}).

%% Start a revoke process for a stream from the SSubscriber.

-define(leader_revoke_match(FromLeader, Stream), #{
    message_type := leader_revoke,
    from_leader := FromLeader,
    stream := Stream
}).

-define(leader_revoke(FromLeader, Stream), #{
    message_type => leader_revoke,
    from_leader => FromLeader,
    stream => Stream
}).

%% Confirm that the leader obtained the progress of the stream,
%% allow the ssubscriber to clean the data

-define(leader_revoked_match(FromLeader, Stream), #{
    message_type := leader_revoked,
    from_leader := FromLeader,
    stream := Stream
}).

-define(leader_revoked(FromLeader, Stream), #{
    message_type => leader_revoked,
    from_leader => FromLeader,
    stream => Stream
}).

%% Notify the SSubscriber that it is in unexpected state and should reconnect.

-define(leader_invalidate_match(FromLeader), #{
    message_type := leader_invalidate,
    from_leader := FromLeader
}).

-define(leader_invalidate(FromLeader), #{
    message_type => leader_invalidate,
    from_leader => FromLeader
}).

%% Respond to the SSubscriber's disconnect request

-define(leader_disconnect_response_match(FromLeader), #{
    message_type := leader_disconnect_response,
    from_leader := FromLeader
}).

-define(leader_disconnect_response(FromLeader), #{
    message_type => leader_disconnect_response,
    from_leader => FromLeader
}).

%% SSubscriber/Leader Id helpers

-define(ssubscriber_id(SessionId, SubscriptionId, PidRef), {SessionId, SubscriptionId, PidRef}).
-define(ssubscriber_pidref(SSubscriberId), element(1, SSubscriberId)).
-define(ssubscriber_subscription_id(SSubscriberId), element(2, SSubscriberId)).
-define(ssubscriber_node(SSubscriberId), node(element(1, SSubscriberId))).
-define(is_local_ssubscriber(SSubscriberId), (?ssubscriber_node(SSubscriberId) =:= node())).
-define(leader_node(Leader), node(Leader)).
-define(is_local_leader(Leader), (?leader_node(Leader) =:= node())).

%% Logging helpers

-ifdef(TEST).

-define(format_ssubscriber_msg(Msg), emqx_ds_shared_sub_proto_format:format_agent_msg(Msg)).
-define(format_leader_msg(Msg), emqx_ds_shared_sub_proto_format:format_leader_msg(Msg)).

%% -ifdef(TEST).
-else.

-define(format_ssubscriber_msg(Msg), Msg).
-define(format_leader_msg(Msg), Msg).

%% -ifdef(TEST).
-endif.

-define(log_ssubscriber_msg(ToLeader, Msg),
    ?tp(debug, ssubscriber_to_leader, #{
        to_leader => ToLeader,
        proto_msg => ?format_ssubscriber_msg(Msg)
    })
).

-define(log_leader_msg(ToSSubscriberId, Msg),
    ?tp(debug, leader_to_ssubscriber, #{
        to_ssubscriber => ToSSubscriberId,
        proto_msg => ?format_leader_msg(Msg)
    })
).
