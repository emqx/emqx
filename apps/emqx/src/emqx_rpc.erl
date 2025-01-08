%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_rpc).

%% Note: please don't forget to add new API functions to
%% `emqx_bpapi_trans:extract_mfa'

-export([
    call/4,
    call/5,
    call/6,
    cast/4,
    cast/5,
    multicall/4,
    multicall/5,
    multicall_on_running/5,
    on_running/3,

    unwrap_erpc/1
]).

-export_type([
    badrpc/0,
    call_result/1,
    call_result/0,
    multicall_result/1,
    multicall_result/0,
    erpc/1,
    erpc_multicall/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-compile(
    {inline, [
        rpc_node/1,
        rpc_nodes/1
    ]}
).

-define(DefaultClientNum, 10).

-type badrpc() :: {badrpc, term()} | {badtcp, term()}.

-type call_result(Result) :: Result | badrpc().

-type call_result() :: call_result(term()).

-type cast_result() :: true.

-type multicall_result(Result) :: {[call_result(Result)], _BadNodes :: [node()]}.

-type multicall_result() :: multicall_result(term()).

-type erpc(Ret) ::
    {ok, Ret}
    | {throw, _Err}
    | {exit, {exception | signal, _Reason}}
    | {error, {exception, _Reason, _Stack :: list()}}
    | {error, {erpc, _Reason}}.

-type erpc_multicall(Ret) :: [erpc(Ret)].

-spec call(node(), module(), atom(), list()) -> call_result().
call(Node, Mod, Fun, Args) ->
    maybe_badrpc(gen_rpc:call(rpc_node(Node), Mod, Fun, Args)).

-spec call(term(), node(), module(), atom(), list()) -> call_result().
call(Key, Node, Mod, Fun, Args) ->
    maybe_badrpc(gen_rpc:call(rpc_node({Key, Node}), Mod, Fun, Args)).

-spec call(term(), node(), module(), atom(), list(), timeout()) -> call_result().
call(Key, Node, Mod, Fun, Args, Timeout) ->
    maybe_badrpc(gen_rpc:call(rpc_node({Key, Node}), Mod, Fun, Args, Timeout)).

-spec multicall([node()], module(), atom(), list()) -> multicall_result().
multicall(Nodes, Mod, Fun, Args) ->
    gen_rpc:multicall(rpc_nodes(Nodes), Mod, Fun, Args).

-spec multicall(term(), [node()], module(), atom(), list()) -> multicall_result().
multicall(Key, Nodes, Mod, Fun, Args) ->
    gen_rpc:multicall(rpc_nodes([{Key, Node} || Node <- Nodes]), Mod, Fun, Args).

-spec multicall_on_running([node()], module(), atom(), list(), timeout()) -> [term() | {error, _}].
multicall_on_running(Nodes, Mod, Fun, Args, Timeout) ->
    unwrap_erpc(erpc:multicall(Nodes, emqx_rpc, on_running, [Mod, Fun, Args], Timeout)).

-spec on_running(module(), atom(), list()) -> term().
on_running(Mod, Fun, Args) ->
    case emqx:is_running() of
        true -> apply(Mod, Fun, Args);
        false -> error(emqx_down)
    end.

-spec cast(node(), module(), atom(), list()) -> cast_result().
cast(Node, Mod, Fun, Args) ->
    %% Note: using a non-ordered cast here, since the generated key is
    %% random anyway:
    gen_rpc:cast(rpc_node(Node), Mod, Fun, Args).

-spec cast(term(), node(), module(), atom(), list()) -> cast_result().
cast(Key, Node, Mod, Fun, Args) ->
    gen_rpc:ordered_cast(rpc_node({Key, Node}), Mod, Fun, Args).

rpc_node(Node) when is_atom(Node) ->
    {Node, rand:uniform(max_client_num())};
rpc_node({Key, Node}) when is_atom(Node) ->
    {Node, erlang:phash2(Key, max_client_num()) + 1}.

rpc_nodes(Nodes) ->
    rpc_nodes(Nodes, []).

rpc_nodes([], Acc) ->
    Acc;
rpc_nodes([Node | Nodes], Acc) ->
    rpc_nodes(Nodes, [rpc_node(Node) | Acc]).

maybe_badrpc({Error, Reason}) when Error =:= badrpc; Error =:= badtcp ->
    {badrpc, Reason};
maybe_badrpc(Delivery) ->
    Delivery.

max_client_num() ->
    emqx:get_config([rpc, client_num], ?DefaultClientNum).

-spec unwrap_erpc(emqx_rpc:erpc(A) | [emqx_rpc:erpc(A)]) -> A | {error, _Err} | list().
unwrap_erpc(Res) when is_list(Res) ->
    [unwrap_erpc(R) || R <- Res];
unwrap_erpc({ok, A}) ->
    A;
unwrap_erpc({throw, A}) ->
    {error, A};
unwrap_erpc({error, {exception, Err, _Stack}}) ->
    {error, Err};
unwrap_erpc({exit, Err}) ->
    {error, Err};
unwrap_erpc({error, {erpc, Err}}) ->
    {error, Err}.

-ifdef(TEST).

badrpc_call_test_() ->
    application:ensure_all_started(gen_rpc),
    Node = node(),
    [
        {"throw", fun() ->
            ?assertEqual(foo, call(Node, erlang, throw, [foo]))
        end},
        {"error", fun() ->
            ?assertMatch({badrpc, {'EXIT', {foo, _}}}, call(Node, erlang, error, [foo]))
        end},
        {"exit", fun() ->
            ?assertEqual({badrpc, {'EXIT', foo}}, call(Node, erlang, exit, [foo]))
        end},
        {"timeout", fun() ->
            ?assertEqual({badrpc, timeout}, call(key, Node, timer, sleep, [1000], 100))
        end},
        {"noconnection", fun() ->
            %% mute crash report from gen_rpc
            logger:set_primary_config(level, critical),
            try
                ?assertEqual(
                    {badrpc, nxdomain}, call(key, 'no@such.node', foo, bar, [])
                )
            after
                logger:set_primary_config(level, notice)
            end
        end}
    ].

multicall_test() ->
    application:ensure_all_started(gen_rpc),
    logger:set_primary_config(level, critical),
    BadNode = 'no@such.node',
    ThisNode = node(),
    Nodes = [ThisNode, BadNode],
    Call4 = fun(M, F, A) -> multicall(Nodes, M, F, A) end,
    Call5 = fun(Key, M, F, A) -> multicall(Key, Nodes, M, F, A) end,
    try
        ?assertMatch({[foo], [{BadNode, _}]}, Call4(erlang, throw, [foo])),
        ?assertMatch({[], [{ThisNode, _}, {BadNode, _}]}, Call4(erlang, error, [foo])),
        ?assertMatch({[], [{ThisNode, _}, {BadNode, _}]}, Call4(erlang, exit, [foo])),
        ?assertMatch({[], [{ThisNode, _}, {BadNode, _}]}, Call5(key, foo, bar, []))
    after
        logger:set_primary_config(level, notice)
    end.

unwrap_erpc_test_() ->
    Nodes = [node()],
    MultiC = fun(M, F, A) -> unwrap_erpc(erpc:multicall(Nodes, M, F, A, 100)) end,
    [
        {"throw", fun() ->
            ?assertEqual([{error, foo}], MultiC(erlang, throw, [foo]))
        end},
        {"error", fun() ->
            ?assertEqual([{error, foo}], MultiC(erlang, error, [foo]))
        end},
        {"exit", fun() ->
            ?assertEqual([{error, {exception, foo}}], MultiC(erlang, exit, [foo]))
        end},
        {"noconnection", fun() ->
            ?assertEqual(
                [{error, noconnection}], unwrap_erpc(erpc:multicall(['no@such.node'], foo, bar, []))
            )
        end}
    ].

-endif.
