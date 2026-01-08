%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_prometheus_proto_v3).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0
]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "6.1.0".
