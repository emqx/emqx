%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
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
