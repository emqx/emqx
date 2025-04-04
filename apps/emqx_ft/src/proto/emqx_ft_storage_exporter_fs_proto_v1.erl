%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ft_storage_exporter_fs_proto_v1).

-behaviour(emqx_bpapi).

-export([introduced_in/0]).

-export([list_exports/2]).
-export([read_export_file/3]).

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "5.0.17".

-spec list_exports([node()], emqx_ft_storage:query(_LocalCursor)) ->
    emqx_rpc:erpc_multicall(
        {ok, [emqx_ft_storage:file_info()]}
        | {error, file:posix() | disabled | {invalid_storage_type, _}}
    ).
list_exports(Nodes, Query) ->
    erpc:multicall(
        Nodes,
        emqx_ft_storage_exporter_fs_proxy,
        list_exports_local,
        [Query]
    ).

-spec read_export_file(node(), file:name(), pid()) ->
    {ok, emqx_ft_storage:reader()}
    | {error, term()}
    | no_return().
read_export_file(Node, Filepath, CallerPid) ->
    erpc:call(
        Node,
        emqx_ft_storage_exporter_fs_proxy,
        read_export_file_local,
        [Filepath, CallerPid]
    ).
