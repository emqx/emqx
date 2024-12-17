%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

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

-define(ssubscriber_update_progress_match(FromSSubscriberId, StreamProgress), #{
    message_type := ssubscriber_update_progresses,
    from_ssubscriber_id := FromSSubscriberId,
    stream_progress := StreamProgress
}).

-define(ssubscriber_update_progress(FromSSubscriberId, StreamProgress), #{
    message_type => ssubscriber_update_progresses,
    from_ssubscriber_id => FromSSubscriberId,
    stream_progress => StreamProgress
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

-define(ssubscriber_disconnect_match(FromSSubscriberId, StreamProgresses), #{
    message_type := ssubscriber_unsubscribe,
    from_ssubscriber_id := FromSSubscriberId,
    stream_progresses := StreamProgresses
}).

-define(ssubscriber_disconnect(FromSSubscriberId, StreamProgresses), #{
    message_type => ssubscriber_unsubscribe,
    from_ssubscriber_id => FromSSubscriberId,
    stream_progresses => StreamProgresses
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

-define(leader_grant_match(FromLeader, StreamProgress), #{
    message_type := leader_grant,
    from_leader := FromLeader,
    stream_progress := StreamProgress
}).

-define(leader_grant(FromLeader, StreamProgress), #{
    message_type => leader_grant,
    from_leader => FromLeader,
    stream_progress => StreamProgress
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

%% SSubscriber/Leader Id helpers

-define(ssubscriber_id(SessionId, SubscriptionId, PidRef), {SessionId, SubscriptionId, PidRef}).
-define(ssubscriber_pidref(SSubscriberId), element(3, SSubscriberId)).
-define(ssubscriber_subscription_id(SSubscriberId), element(2, SSubscriberId)).
-define(ssubscriber_node(SSubscriberId), node(?ssubscriber_pidref(SSubscriberId))).
-define(is_local_ssubscriber(SSubscriberId), (?ssubscriber_node(SSubscriberId) =:= node())).
-define(leader_node(Leader), node(Leader)).
-define(is_local_leader(Leader), (?leader_node(Leader) =:= node())).
