%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-ifndef(EMQX_DS_SHARED_SUB_PROTO_HRL).
-define(EMQX_DS_SHARED_SUB_PROTO_HRL, true).

%% Borrower messages sent to the leader.
%% Leader talks to many Borrowers, `borrower_id` field is used to identify the sender.

-define(borrower_connect(FROM_BORROWER_ID, SHARE_TOPIC_FILTER), #{
    message_type => borrower_connect,
    from_borrower_id => FROM_BORROWER_ID,
    share_topic_filter => SHARE_TOPIC_FILTER
}).

-define(borrower_connect_match(FROM_BORROWER_ID, SHARE_TOPIC_FILTER), #{
    message_type := borrower_connect,
    from_borrower_id := FROM_BORROWER_ID,
    share_topic_filter := SHARE_TOPIC_FILTER
}).

-define(borrower_ping(FROM_BORROWER_ID), #{
    message_type => borrower_ping,
    from_borrower_id => FROM_BORROWER_ID
}).

-define(borrower_ping_match(FROM_BORROWER_ID), #{
    message_type := borrower_ping,
    from_borrower_id := FROM_BORROWER_ID
}).

-define(borrower_update_progress_match(FROM_BORROWER_ID, STREAM_PROGRESS), #{
    message_type := borrower_update_progresses,
    from_borrower_id := FROM_BORROWER_ID,
    stream_progress := STREAM_PROGRESS
}).

-define(borrower_update_progress(FROM_BORROWER_ID, STREAM_PROGRESS), #{
    message_type => borrower_update_progresses,
    from_borrower_id => FROM_BORROWER_ID,
    stream_progress => STREAM_PROGRESS
}).

-define(borrower_revoke_finished_match(FROM_BORROWER_ID, STREAM), #{
    message_type := borrower_revoke_finished,
    from_borrower_id := FROM_BORROWER_ID,
    stream := STREAM
}).

-define(borrower_revoke_finished(FROM_BORROWER_ID, STREAM), #{
    message_type => borrower_revoke_finished,
    from_borrower_id => FROM_BORROWER_ID,
    stream => STREAM
}).

-define(borrower_disconnect_match(FROM_BORROWER_ID, STREAM_PROGRESSES), #{
    message_type := borrower_unsubscribe,
    from_borrower_id := FROM_BORROWER_ID,
    stream_progresses := STREAM_PROGRESSES
}).

-define(borrower_disconnect(FROM_BORROWER_ID, STREAM_PROGRESSES), #{
    message_type => borrower_unsubscribe,
    from_borrower_id => FROM_BORROWER_ID,
    stream_progresses => STREAM_PROGRESSES
}).

%% Leader messages sent to the Borrower.

%% A common matcher for leader messages.
-define(leader_message_match(FROM_LEADER), #{
    from_leader := FROM_LEADER
}).

%% Respond to the Borrower's connection request.

-define(leader_connect_response(FROM_LEADER), #{
    message_type => leader_connect_response,
    from_leader => FROM_LEADER
}).

-define(leader_connect_response_match(FROM_LEADER), #{
    message_type := leader_connect_response,
    from_leader := FROM_LEADER
}).

%% Respond to the Borrower's ping request.

-define(leader_ping_response(FROM_LEADER), #{
    message_type => leader_ping_response,
    from_leader => FROM_LEADER
}).

-define(leader_ping_response_match(FROM_LEADER), #{
    message_type := leader_ping_response,
    from_leader := FROM_LEADER
}).

%% Grant a stream to the Borrower.

-define(leader_grant_match(FROM_LEADER, STREAM_PROGRESS), #{
    message_type := leader_grant,
    from_leader := FROM_LEADER,
    stream_progress := STREAM_PROGRESS
}).

-define(leader_grant(FROM_LEADER, STREAM_PROGRESS), #{
    message_type => leader_grant,
    from_leader => FROM_LEADER,
    stream_progress => STREAM_PROGRESS
}).

%% Start a revoke process for a stream from the Borrower.

-define(leader_revoke_match(FROM_LEADER, STREAM), #{
    message_type := leader_revoke,
    from_leader := FROM_LEADER,
    stream := STREAM
}).

-define(leader_revoke(FROM_LEADER, STREAM), #{
    message_type => leader_revoke,
    from_leader => FROM_LEADER,
    stream => STREAM
}).

%% Confirm that the leader obtained the progress of the stream,
%% allow the borrower to clean the data

-define(leader_revoked_match(FROM_LEADER, STREAM), #{
    message_type := leader_revoked,
    from_leader := FROM_LEADER,
    stream := STREAM
}).

-define(leader_revoked(FROM_LEADER, STREAM), #{
    message_type => leader_revoked,
    from_leader => FROM_LEADER,
    stream => STREAM
}).

%% Notify the Borrower that it is in unexpected state and should reconnect.

-define(leader_invalidate_match(FROM_LEADER), #{
    message_type := leader_invalidate,
    from_leader := FROM_LEADER
}).

-define(leader_invalidate(FROM_LEADER), #{
    message_type => leader_invalidate,
    from_leader => FROM_LEADER
}).

%% Borrower/Leader Id helpers

-define(borrower_id(SESSION_ID, SUBSCRIPTION_ID, PID_REF), {SESSION_ID, SUBSCRIPTION_ID, PID_REF}).
-define(borrower_pidref(BORROWER_ID), element(3, BORROWER_ID)).
-define(borrower_subscription_id(BORROWER_ID), element(2, BORROWER_ID)).
-define(borrower_node(BORROWER_ID), node(?borrower_pidref(BORROWER_ID))).
-define(is_local_borrower(BORROWER_ID), (?borrower_node(BORROWER_ID) =:= node())).
-define(leader_node(LEADER), node(LEADER)).
-define(is_local_leader(LEADER), (?leader_node(LEADER) =:= node())).

-endif.
