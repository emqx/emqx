%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_plugins_proto_v2).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    get_tar/3,
    get_config/5
]).

-include("emqx_plugins.hrl").
-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "5.7.0".

-spec get_tar(node(), name_vsn(), timeout()) -> {ok, binary()} | {error, any()}.
get_tar(Node, NameVsn, Timeout) ->
    rpc:call(Node, emqx_plugins, get_tar, [NameVsn], Timeout).

-spec get_config(
    node(), name_vsn(), ?CONFIG_FORMAT_MAP, any(), timeout()
) -> {ok, map() | any()} | {error, any()}.
get_config(Node, NameVsn, Opt, Default, Timeout) ->
    rpc:call(Node, emqx_plugins, get_config, [NameVsn, Opt, Default], Timeout).
