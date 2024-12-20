%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Logging helpers

-ifdef(TEST).

-define(format_ssubscriber_msg(Msg), emqx_ds_shared_sub_format:format_ssubscriber_msg(Msg)).
-define(format_leader_msg(Msg), emqx_ds_shared_sub_format:format_leader_msg(Msg)).
-define(format_ssubscriber(Subscriber), emqx_ds_shared_sub_format:format_ssubscriber(Subscriber)).
-define(format_stream(Stream), emqx_ds_shared_sub_format:format_stream(Stream)).
-define(format_ssubscriber_id(SSubscriberId),
    emqx_ds_shared_sub_format:format_ssubscriber_id(SSubscriberId)
).
-define(format_ssubscriber_ids(SubscriptionIds),
    emqx_ds_shared_sub_format:format_ssubscriber_ids(SubscriptionIds)
).
-define(format_ssubscriber_map(Map), emqx_ds_shared_sub_format:format_ssubscriber_map(Map)).
-define(format_stream_map(Map), emqx_ds_shared_sub_format:format_stream_map(Map)).
-define(format_deep(Value), emqx_ds_shared_sub_format:format_deep(Value)).
-define(format_streams(Streams), emqx_ds_shared_sub_format:format_streams(Streams)).

%% -ifdef(TEST).
-else.

-define(format_ssubscriber_msg(Msg), Msg).
-define(format_leader_msg(Msg), Msg).
-define(format_ssubscriber(Subscriber), Subscriber).
-define(format_stream(Stream), Stream).
-define(format_ssubscriber_id(SSubscriberId), SSubscriberId).
-define(format_ssubscriber_ids(SubscriptionIds), SubscriptionIds).
-define(format_ssubscriber_map(Map), Map).
-define(format_stream_map(Map), Map).
-define(format_deep(Value), Value).
-define(format_streams(Streams), Streams).

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
        to_ssubscriber => ?format_ssubscriber_id(ToSSubscriberId),
        proto_msg => ?format_leader_msg(Msg)
    })
).
