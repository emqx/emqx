%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_proto_v3).

-behaviour(emqx_bpapi).

-include_lib("emqx/include/bpapi.hrl").

-export([
    introduced_in/0,
    send_to_leader/3,
    send_to_ssubscriber/3
]).

introduced_in() ->
    "5.8.0".

-spec send_to_leader(
    node(),
    emqx_ds_shared_sub_proto:leader(),
    emqx_ds_shared_sub_proto:to_leader_msg()
) -> ok.
send_to_leader(Node, ToLeader, Msg) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, send_to_leader, [ToLeader, Msg]).

-spec send_to_ssubscriber(
    node(),
    emqx_ds_shared_sub_proto:ssubscriber_id(),
    emqx_ds_shared_sub_proto:to_ssubscriber_msg()
) -> ok.
send_to_ssubscriber(Node, ToSSubscriber, Msg) ->
    erpc:cast(Node, emqx_ds_shared_sub_proto, send_to_ssubscriber, [ToSSubscriber, Msg]).
