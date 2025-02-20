%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

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
    ?tp(debug, borrower_to_leader, #{
        to_leader => ToLeader,
        proto_msg => ?format_borrower_msg(Msg)
    })
).

-define(log_leader_msg(ToBorrowerId, Msg),
    ?tp(debug, leader_to_borrower, #{
        to_borrower => ?format_borrower_id(ToBorrowerId),
        proto_msg => ?format_leader_msg(Msg)
    })
).
