%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_DS_SHARED_SUB_FORMAT_HRL).
-define(EMQX_DS_SHARED_SUB_FORMAT_HRL, true).

%% Logging helpers

-ifdef(TEST).

-define(format_borrower_msg(Msg), emqx_ds_shared_sub_format:format_borrower_msg(Msg)).
-define(format_leader_msg(Msg), emqx_ds_shared_sub_format:format_leader_msg(Msg)).
-define(format_borrower(Subscriber), emqx_ds_shared_sub_format:format_borrower(Subscriber)).
-define(format_stream(Stream), emqx_ds_shared_sub_format:format_stream(Stream)).
-define(format_borrower_id(BorrowerId),
    emqx_ds_shared_sub_format:format_borrower_id(BorrowerId)
).
-define(format_borrower_ids(SubscriptionIds),
    emqx_ds_shared_sub_format:format_borrower_ids(SubscriptionIds)
).
-define(format_borrower_map(Map), emqx_ds_shared_sub_format:format_borrower_map(Map)).
-define(format_stream_map(Map), emqx_ds_shared_sub_format:format_stream_map(Map)).
-define(format_deep(Value), emqx_ds_shared_sub_format:format_deep(Value)).
-define(format_streams(Streams), emqx_ds_shared_sub_format:format_streams(Streams)).

%% -ifdef(TEST).
-else.

-define(format_borrower_msg(Msg), Msg).
-define(format_leader_msg(Msg), Msg).
-define(format_borrower(Subscriber), Subscriber).
-define(format_stream(Stream), Stream).
-define(format_borrower_id(BorrowerId), BorrowerId).
-define(format_borrower_ids(SubscriptionIds), SubscriptionIds).
-define(format_borrower_map(Map), Map).
-define(format_stream_map(Map), Map).
-define(format_deep(Value), Value).
-define(format_streams(Streams), Streams).

%% -ifdef(TEST).
-endif.

-define(log_borrower_msg(ToLeader, Msg),
    ?tp(debug, ds_shared_sub_borrower_to_leader, #{
        to_leader => ToLeader,
        proto_msg => ?format_borrower_msg(Msg)
    })
).

-define(log_leader_msg(ToBorrowerId, Msg),
    ?tp(debug, ds_shared_sub_leader_to_borrower, #{
        to_borrower => ?format_borrower_id(ToBorrowerId),
        proto_msg => ?format_leader_msg(Msg)
    })
).

-define(tp_unknown_event, ds_shared_sub_unknown_event).
-define(tp_leader_started, ds_shared_sub_leader_started).
-define(tp_leader_terminate, ds_shared_sub_leader_terminate).
-define(tp_leader_borrower_connect, ds_shared_sub_leader_borrower_connect).
-define(tp_leader_borrower_disconnect, ds_shared_sub_leader_borrower_disconnect).
-define(tp_leader_disconnect_borrower, ds_shared_sub_leader_disconnect_borrower).
-define(tp_leader_realloc, ds_shared_sub_leader_realloc).

-endif.
