%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mgmt_data_backup_proto_v2).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    import_file/5
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
