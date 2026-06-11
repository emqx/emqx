%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mgmt_data_backup_proto_v2).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    import_file/5,
    peek_sensitive_table_sets/3
]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "6.0.0".

-spec import_file(node(), node(), binary(), map(), timeout()) ->
    emqx_mgmt_data_backup:import_res() | {badrpc, _}.
import_file(Node, FileNode, FileName, Opts, Timeout) ->
    erpc:call(
        Node, emqx_mgmt_data_backup, maybe_copy_and_import, [FileNode, FileName, Opts], Timeout
    ).

-spec peek_sensitive_table_sets(node(), binary(), timeout()) ->
    {ok, [binary()]} | {error, _} | {badrpc, _}.
peek_sensitive_table_sets(Node, FileName, Timeout) ->
    rpc:call(Node, emqx_mgmt_data_backup, peek_sensitive_table_sets, [FileName], Timeout).
