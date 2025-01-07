%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_proto_v2).

-behaviour(emqx_bpapi).

-include_lib("emqx/include/bpapi.hrl").

-export([introduced_in/0]).

-export([
    remote_connection_counts/1
]).

-define(TIMEOUT, 500).
-define(BACKUP_TIMEOUT, 15_000).

introduced_in() ->
    "e5.0.0".

-spec remote_connection_counts(list(node())) -> list({atom(), term()}).
remote_connection_counts(Nodes) ->
    erpc:multicall(Nodes, emqx_license_resources, local_connection_count, [], ?TIMEOUT).
