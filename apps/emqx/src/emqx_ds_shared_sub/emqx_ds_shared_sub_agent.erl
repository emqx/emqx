%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_agent).

-include("emqx_ds_shared_sub_proto.hrl").
-include("emqx_ds_shared_sub_borrower.hrl").

-export([
    send_to_borrower/2
]).

-include("emqx_session.hrl").
-include("../emqx_persistent_session_ds/session_internals.hrl").

send_to_borrower(BorrowerId, Msg) ->
    SubscriptionId = emqx_ds_shared_sub_proto:borrower_subscription_id(BorrowerId),
    send(
        emqx_ds_shared_sub_proto:borrower_pidref(BorrowerId),
        SubscriptionId,
        #message_to_borrower{
            borrower_id = BorrowerId,
            message = Msg
        }
    ).

-spec send(pid() | reference(), emqx_persistent_session_ds:subscription_id(), term()) -> term().
send(Dest, SubscriptionId, Msg) ->
    erlang:send(Dest, ?session_message(?shared_sub_message(SubscriptionId, Msg))).
