%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Logging helpers

-ifdef(TEST).

-define(format_ssubscriber_msg(Msg), emqx_ds_shared_sub_format:format_agent_msg(Msg)).
-define(format_leader_msg(Msg), emqx_ds_shared_sub_format:format_leader_msg(Msg)).
-define(format_ssubscriber(Subscriber), emqx_ds_shared_sub_format:format_ssubscriber(Subscriber)).
-define(format_stream(Stream), emqx_ds_shared_sub_format:format_stream(Stream)).
-define(format_progress(Progress), emqx_ds_shared_sub_format:format_progress(Progress)).

%% -ifdef(TEST).
-else.

-define(format_ssubscriber_msg(Msg), Msg).
-define(format_leader_msg(Msg), Msg).
-define(format_ssubscriber(Subscriber), Subscriber).
-define(format_stream(Stream), Stream).
-define(format_progress(Progress), Progress).

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
