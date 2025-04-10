%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_plugins_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    get_tar/3
]).

-include_lib("emqx/include/bpapi.hrl").

-type name_vsn() :: binary() | string().

introduced_in() ->
    "5.0.21".

-spec get_tar(node(), name_vsn(), timeout()) -> {ok, binary()} | {error, any}.
get_tar(Node, NameVsn, Timeout) ->
    rpc:call(Node, emqx_plugins, get_tar, [NameVsn], Timeout).
