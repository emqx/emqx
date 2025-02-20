%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Borrower messages sent to the leader.
%% Leader talks to many Borrowers, `borrower_id` field is used to identify the sender.

-define(borrower_connect(FromBorrowerId, ShareTopicFilter), #{
    message_type => borrower_connect,
    from_borrower_id => FromBorrowerId,
    share_topic_filter => ShareTopicFilter
}).

-define(borrower_connect_match(FromBorrowerId, ShareTopicFilter), #{
    message_type := borrower_connect,
    from_borrower_id := FromBorrowerId,
    share_topic_filter := ShareTopicFilter
}).

-define(borrower_ping(FromBorrowerId), #{
    message_type => borrower_ping,
    from_borrower_id => FromBorrowerId
}).

-define(borrower_ping_match(FromBorrowerId), #{
    message_type := borrower_ping,
    from_borrower_id := FromBorrowerId
}).

-define(borrower_update_progress_match(FromBorrowerId, StreamProgress), #{
    message_type := borrower_update_progresses,
    from_borrower_id := FromBorrowerId,
    stream_progress := StreamProgress
}).

-define(borrower_update_progress(FromBorrowerId, StreamProgress), #{
    message_type => borrower_update_progresses,
    from_borrower_id => FromBorrowerId,
    stream_progress => StreamProgress
}).

-define(borrower_revoke_finished_match(FromBorrowerId, Stream), #{
    message_type := borrower_revoke_finished,
    from_borrower_id := FromBorrowerId,
    stream := Stream
}).

-define(borrower_revoke_finished(FromBorrowerId, Stream), #{
    message_type => borrower_revoke_finished,
    from_borrower_id => FromBorrowerId,
    stream => Stream
}).

-define(borrower_disconnect_match(FromBorrowerId, StreamProgresses), #{
    message_type := borrower_unsubscribe,
    from_borrower_id := FromBorrowerId,
    stream_progresses := StreamProgresses
}).

-define(borrower_disconnect(FromBorrowerId, StreamProgresses), #{
    message_type => borrower_unsubscribe,
    from_borrower_id => FromBorrowerId,
    stream_progresses => StreamProgresses
}).

%% Leader messages sent to the Borrower.

%% A common matcher for leader messages.
-define(leader_message_match(FromLeader), #{
    from_leader := FromLeader
}).

%% Respond to the Borrower's connection request.

-define(leader_connect_response(FromLeader), #{
    message_type => leader_connect_response,
    from_leader => FromLeader
}).

-define(leader_connect_response_match(FromLeader), #{
    message_type := leader_connect_response,
    from_leader := FromLeader
}).

%% Respond to the Borrower's ping request.

-define(leader_ping_response(FromLeader), #{
    message_type => leader_ping_response,
    from_leader => FromLeader
}).

-define(leader_ping_response_match(FromLeader), #{
    message_type := leader_ping_response,
    from_leader := FromLeader
}).

%% Grant a stream to the Borrower.

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

%% Start a revoke process for a stream from the Borrower.

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
%% allow the borrower to clean the data

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

%% Notify the Borrower that it is in unexpected state and should reconnect.

-define(leader_invalidate_match(FromLeader), #{
    message_type := leader_invalidate,
    from_leader := FromLeader
}).

-define(leader_invalidate(FromLeader), #{
    message_type => leader_invalidate,
    from_leader => FromLeader
}).

%% Borrower/Leader Id helpers

-define(borrower_id(SessionId, SubscriptionId, PidRef), {SessionId, SubscriptionId, PidRef}).
-define(borrower_pidref(BorrowerId), element(3, BorrowerId)).
-define(borrower_subscription_id(BorrowerId), element(2, BorrowerId)).
-define(borrower_node(BorrowerId), node(?borrower_pidref(BorrowerId))).
-define(is_local_borrower(BorrowerId), (?borrower_node(BorrowerId) =:= node())).
-define(leader_node(Leader), node(Leader)).
-define(is_local_leader(Leader), (?leader_node(Leader) =:= node())).
