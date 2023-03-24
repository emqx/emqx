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

-module(emqx_api_lib).

-export([
    with_node/2,
    with_node_or_cluster/2
]).

-include("emqx_api_lib.hrl").

-define(NODE_NOT_FOUND(NODE), ?NOT_FOUND(<<"Node not found: ", NODE/binary>>)).

%%--------------------------------------------------------------------
%% exported API
%%--------------------------------------------------------------------
-spec with_node(binary(), fun((atom()) -> {ok, term()} | {error, term()})) ->
    ?OK(term()) | ?NOT_FOUND(binary()) | ?BAD_REQUEST(term()).
with_node(BinNode, Fun) ->
    case lookup_node(BinNode) of
        {ok, Node} ->
            handle_result(Fun(Node));
        not_found ->
            ?NODE_NOT_FOUND(BinNode)
    end.

-spec with_node_or_cluster(binary(), fun((atom()) -> {ok, term()} | {error, term()})) ->
    ?OK(term()) | ?NOT_FOUND(iolist()) | ?BAD_REQUEST(term()).
with_node_or_cluster(<<"all">>, Fun) ->
    handle_result(Fun(all));
with_node_or_cluster(Node, Fun) ->
    with_node(Node, Fun).

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

-spec lookup_node(binary()) -> {ok, atom()} | not_found.
lookup_node(BinNode) ->
    case emqx_misc:safe_to_existing_atom(BinNode, utf8) of
        {ok, Node} ->
            case lists:member(Node, mria:running_nodes()) of
                true ->
                    {ok, Node};
                false ->
                    not_found
            end;
        _Error ->
            not_found
    end.

handle_result({ok, Result}) ->
    ?OK(Result);
handle_result({error, Reason}) ->
    ?BAD_REQUEST(Reason).
