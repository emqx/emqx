%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ft_storage_fs_proto_v1).

-behaviour(emqx_bpapi).

-export([introduced_in/0]).

-export([multilist/3]).
-export([pread/5]).
-export([ready_transfers/1]).
-export([get_ready_transfer/3]).

-type offset() :: emqx_ft:offset().
-type transfer() :: emqx_ft:transfer().
-type filefrag() :: emqx_ft_storage_fs:filefrag().

-include_lib("emqx/include/bpapi.hrl").

introduced_in() ->
    "5.0.17".

-spec multilist([node()], transfer(), fragment | result) ->
    emqx_rpc:erpc_multicall({ok, [filefrag()]} | {error, term()}).
multilist(Nodes, Transfer, What) ->
    erpc:multicall(Nodes, emqx_ft_storage_fs_proxy, list_local, [Transfer, What]).

-spec pread(node(), transfer(), filefrag(), offset(), _Size :: non_neg_integer()) ->
    {ok, [filefrag()]} | {error, term()} | no_return().
pread(Node, Transfer, Frag, Offset, Size) ->
    erpc:call(Node, emqx_ft_storage_fs_proxy, pread_local, [Transfer, Frag, Offset, Size]).

-spec ready_transfers([node()]) ->
    [
        {ok, [{emqx_ft_storage:ready_transfer_id(), emqx_ft_storage:ready_transfer_info()}]}
        | {error, term()}
        | {exit, term()}
        | {throw, term()}
    ].
ready_transfers(Nodes) ->
    erpc:multicall(Nodes, emqx_ft_storage_fs_proxy, ready_transfers_local, []).

-spec get_ready_transfer(node(), pid(), emqx_ft_storage:ready_transfer_id()) ->
    {ok, emqx_ft_storage:ready_transfer_data()}
    | {error, term()}
    | no_return().
get_ready_transfer(Node, CallerPid, ReadyTransferId) ->
    erpc:call(Node, emqx_ft_storage_fs_proxy, get_ready_transfer_local, [CallerPid, ReadyTransferId]).
