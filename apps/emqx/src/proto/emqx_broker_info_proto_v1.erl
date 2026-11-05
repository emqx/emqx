%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_broker_info_proto_v1).

-behaviour(emqx_bpapi).

-include("bpapi.hrl").

-export([
    introduced_in/0,

    forward_async/3
]).

introduced_in() ->
    "7.0.0".

-spec forward_async(node(), emqx_types:topic(), term()) -> true.
forward_async(Node, Topic, Info) when is_binary(Topic) ->
    emqx_rpc:cast(Topic, Node, emqx_broker, do_dispatch_info, [Topic, Info]).
