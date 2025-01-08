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

-module(emqx_persistent_session_ds_proto_v1).

-behaviour(emqx_bpapi).

-export([
    introduced_in/0,
    deprecated_since/0,

    open_iterator/4,
    close_iterator/2,
    close_all_iterators/2
]).

-include_lib("emqx/include/bpapi.hrl").

-define(TIMEOUT, 30_000).

introduced_in() ->
    "5.3.0".

deprecated_since() ->
    "5.4.0".

-spec open_iterator(
    [node()],
    emqx_types:words(),
    emqx_ds:time(),
    emqx_ds:iterator_id()
) ->
    emqx_rpc:erpc_multicall(ok).
open_iterator(Nodes, TopicFilter, StartMS, IteratorID) ->
    erpc:multicall(
        Nodes,
        emqx_persistent_session_ds,
        do_open_iterator,
        [TopicFilter, StartMS, IteratorID],
        ?TIMEOUT
    ).

-spec close_iterator(
    [node()],
    emqx_ds:iterator_id()
) ->
    emqx_rpc:erpc_multicall(ok).
close_iterator(Nodes, IteratorID) ->
    erpc:multicall(
        Nodes,
        emqx_persistent_session_ds,
        do_ensure_iterator_closed,
        [IteratorID],
        ?TIMEOUT
    ).

-spec close_all_iterators(
    [node()],
    emqx_persistent_session_ds:id()
) ->
    emqx_rpc:erpc_multicall(ok).
close_all_iterators(Nodes, DSSessionID) ->
    erpc:multicall(
        Nodes,
        emqx_persistent_session_ds,
        do_ensure_all_iterators_closed,
        [DSSessionID],
        ?TIMEOUT
    ).
