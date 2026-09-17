%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mgmt_data_backup_proto_v4).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    peek_sensitive_table_sets/3
]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "6.0.4".

%% Lists the sensitive mnesia table sets a stored backup contains. A node that
%% does not support this version has no target function and answers `badrpc'.
-spec peek_sensitive_table_sets(node(), binary(), timeout()) ->
    {ok, [binary()]} | {error, _} | {badrpc, _}.
peek_sensitive_table_sets(Node, FileName, Timeout) ->
    rpc:call(Node, emqx_mgmt_data_backup, peek_sensitive_table_sets, [FileName], Timeout).
