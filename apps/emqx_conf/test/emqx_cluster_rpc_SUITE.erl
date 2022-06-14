%%--------------------------------------------------------------------
%% Copyright (c) 2018-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_cluster_rpc_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_conf.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-define(NODE1, emqx_cluster_rpc).
-define(NODE2, emqx_cluster_rpc2).
-define(NODE3, emqx_cluster_rpc3).

all() ->
    [
        t_base_test,
        t_commit_fail_test,
        t_commit_crash_test,
        t_commit_ok_but_apply_fail_on_other_node,
        t_commit_ok_apply_fail_on_other_node_then_recover,
        t_del_stale_mfa,
        t_skip_failed_commit,
        t_fast_forward_commit
    ].
suite() -> [{timetrap, {minutes, 3}}].
groups() -> [].

init_per_suite(Config) ->
    application:load(emqx_conf),
    ok = ekka:start(),
    ok = mria_rlog:wait_for_shards([?CLUSTER_RPC_SHARD], infinity),
    ok = emqx_config:put([node, cluster_call, retry_interval], 1000),
    meck:new(emqx_alarm, [non_strict, passthrough, no_link]),
    meck:expect(emqx_alarm, activate, 3, ok),
    meck:expect(emqx_alarm, deactivate, 3, ok),
    Config.

end_per_suite(_Config) ->
    ekka:stop(),
    mria:stop(),
    mria_mnesia:delete_schema(),
    meck:unload(emqx_alarm),
    ok.

init_per_testcase(_TestCase, Config) ->
    start(),
    Config.

end_per_testcase(_Config) ->
    stop(),
    ok.

t_base_test(_Config) ->
    emqx_cluster_rpc:reset(),
    ?assertEqual(emqx_cluster_rpc:status(), {atomic, []}),
    Pid = self(),
    MFA = {M, F, A} = {?MODULE, echo, [Pid, test]},
    {ok, TnxId, ok} = multicall(M, F, A),
    {atomic, Query} = emqx_cluster_rpc:query(TnxId),
    ?assertEqual(MFA, maps:get(mfa, Query)),
    ?assertEqual(node(), maps:get(initiator, Query)),
    ?assert(maps:is_key(created_at, Query)),
    ?assertEqual(ok, receive_msg(3, test)),
    ?assertEqual({ok, 2, ok}, multicall(M, F, A)),
    {atomic, Status} = emqx_cluster_rpc:status(),
    case length(Status) =:= 3 of
        true ->
            ?assert(lists:all(fun(I) -> maps:get(tnx_id, I) =:= 2 end, Status));
        false ->
            %% wait for mnesia to write in.
            ct:sleep(42),
            {atomic, Status1} = emqx_cluster_rpc:status(),
            ct:pal("status: ~p", Status),
            ct:pal("status1: ~p", Status1),
            ?assertEqual(3, length(Status1)),
            ?assert(lists:all(fun(I) -> maps:get(tnx_id, I) =:= 2 end, Status))
    end,
    ok.

t_commit_fail_test(_Config) ->
    emqx_cluster_rpc:reset(),
    {atomic, []} = emqx_cluster_rpc:status(),
    {M, F, A} = {?MODULE, failed_on_node, [erlang:whereis(?NODE2)]},
    {init_failure, "MFA return not ok"} = multicall(M, F, A),
    ?assertEqual({atomic, []}, emqx_cluster_rpc:status()),
    ok.

t_commit_crash_test(_Config) ->
    emqx_cluster_rpc:reset(),
    {atomic, []} = emqx_cluster_rpc:status(),
    {M, F, A} = {?MODULE, no_exist_function, []},
    {init_failure, {error, Meta}} = multicall(M, F, A),
    ?assertEqual(undef, maps:get(reason, Meta)),
    ?assertEqual(error, maps:get(exception, Meta)),
    ?assertEqual(true, maps:is_key(stacktrace, Meta)),
    ?assertEqual({atomic, []}, emqx_cluster_rpc:status()),
    ok.

t_commit_ok_but_apply_fail_on_other_node(_Config) ->
    emqx_cluster_rpc:reset(),
    {atomic, []} = emqx_cluster_rpc:status(),
    MFA = {M, F, A} = {?MODULE, failed_on_node, [erlang:whereis(?NODE1)]},
    {ok, _, ok} = multicall(M, F, A, 1, 1000),
    {atomic, [Status]} = emqx_cluster_rpc:status(),
    ?assertEqual(MFA, maps:get(mfa, Status)),
    ?assertEqual(node(), maps:get(node, Status)),
    erlang:send(?NODE2, test),
    Call = emqx_cluster_rpc:make_initiate_call_req(M, F, A),
    Res = gen_server:call(?NODE2, Call),
    ?assertEqual({init_failure, "MFA return not ok"}, Res),
    ok.

t_catch_up_status_handle_next_commit(_Config) ->
    emqx_cluster_rpc:reset(),
    {atomic, []} = emqx_cluster_rpc:status(),
    {M, F, A} = {?MODULE, failed_on_node_by_odd, [erlang:whereis(?NODE1)]},
    {ok, 1, ok} = multicall(M, F, A, 1, 1000),
    Call = emqx_cluster_rpc:make_initiate_call_req(M, F, A),
    {ok, 2} = gen_server:call(?NODE2, Call),
    ok.

t_commit_ok_apply_fail_on_other_node_then_recover(_Config) ->
    emqx_cluster_rpc:reset(),
    {atomic, []} = emqx_cluster_rpc:status(),
    ets:new(test, [named_table, public]),
    ets:insert(test, {other_mfa_result, failed}),
    ct:pal("111:~p~n", [ets:tab2list(cluster_rpc_commit)]),
    {M, F, A} = {?MODULE, failed_on_other_recover_after_retry, [erlang:whereis(?NODE1)]},
    {ok, 1, ok} = multicall(M, F, A, 1, 1000),
    ct:pal("222:~p~n", [ets:tab2list(cluster_rpc_commit)]),
    ct:pal("333:~p~n", [emqx_cluster_rpc:status()]),
    {atomic, [_Status | L]} = emqx_cluster_rpc:status(),
    ?assertEqual([], L),
    ets:insert(test, {other_mfa_result, ok}),
    {ok, 2, ok} = multicall(io, format, ["test"], 1, 1000),
    ct:sleep(1000),
    {atomic, NewStatus} = emqx_cluster_rpc:status(),
    ?assertEqual(3, length(NewStatus)),
    Pid = self(),
    MFAEcho = {M1, F1, A1} = {?MODULE, echo, [Pid, test]},
    {ok, TnxId, ok} = multicall(M1, F1, A1),
    {atomic, Query} = emqx_cluster_rpc:query(TnxId),
    ?assertEqual(MFAEcho, maps:get(mfa, Query)),
    ?assertEqual(node(), maps:get(initiator, Query)),
    ?assert(maps:is_key(created_at, Query)),
    ?assertEqual(ok, receive_msg(3, test)),
    ok.

t_del_stale_mfa(_Config) ->
    emqx_cluster_rpc:reset(),
    {atomic, []} = emqx_cluster_rpc:status(),
    MFA = {M, F, A} = {io, format, ["test"]},
    Keys = lists:seq(1, 50),
    Keys2 = lists:seq(51, 150),
    Ids =
        [
            begin
                {ok, TnxId, ok} = multicall(M, F, A),
                TnxId
            end
         || _ <- Keys
        ],
    ?assertEqual(Keys, Ids),
    Ids2 =
        [
            begin
                {ok, TnxId, ok} = multicall(M, F, A),
                TnxId
            end
         || _ <- Keys2
        ],
    ?assertEqual(Keys2, Ids2),
    ct:sleep(1200),
    [
        begin
            ?assertEqual({aborted, not_found}, emqx_cluster_rpc:query(I))
        end
     || I <- lists:seq(1, 50)
    ],
    [
        begin
            {atomic, Map} = emqx_cluster_rpc:query(I),
            ?assertEqual(MFA, maps:get(mfa, Map)),
            ?assertEqual(node(), maps:get(initiator, Map)),
            ?assert(maps:is_key(created_at, Map))
        end
     || I <- lists:seq(51, 150)
    ],
    ok.

t_skip_failed_commit(_Config) ->
    emqx_cluster_rpc:reset(),
    {atomic, []} = emqx_cluster_rpc:status(),
    {ok, 1, ok} = multicall(io, format, ["test~n"], all, 1000),
    ct:sleep(180),
    {atomic, List1} = emqx_cluster_rpc:status(),
    Node = node(),
    ?assertEqual(
        [{Node, 1}, {{Node, ?NODE2}, 1}, {{Node, ?NODE3}, 1}],
        tnx_ids(List1)
    ),
    {M, F, A} = {?MODULE, failed_on_node, [erlang:whereis(?NODE1)]},
    {ok, 2, ok} = multicall(M, F, A, 1, 1000),
    2 = gen_server:call(?NODE2, skip_failed_commit, 5000),
    {atomic, List2} = emqx_cluster_rpc:status(),
    ?assertEqual(
        [{Node, 2}, {{Node, ?NODE2}, 2}, {{Node, ?NODE3}, 1}],
        tnx_ids(List2)
    ),
    ok.

t_fast_forward_commit(_Config) ->
    emqx_cluster_rpc:reset(),
    {atomic, []} = emqx_cluster_rpc:status(),
    {ok, 1, ok} = multicall(io, format, ["test~n"], all, 1000),
    ct:sleep(180),
    {atomic, List1} = emqx_cluster_rpc:status(),
    Node = node(),
    ?assertEqual(
        [{Node, 1}, {{Node, ?NODE2}, 1}, {{Node, ?NODE3}, 1}],
        tnx_ids(List1)
    ),
    {M, F, A} = {?MODULE, failed_on_node, [erlang:whereis(?NODE1)]},
    {ok, 2, ok} = multicall(M, F, A, 1, 1000),
    {ok, 3, ok} = multicall(M, F, A, 1, 1000),
    {ok, 4, ok} = multicall(M, F, A, 1, 1000),
    {ok, 5, ok} = multicall(M, F, A, 1, 1000),
    {peers_lagging, 6, ok, _} = multicall(M, F, A, 2, 1000),
    3 = gen_server:call(?NODE2, {fast_forward_to_commit, 3}, 5000),
    4 = gen_server:call(?NODE2, {fast_forward_to_commit, 4}, 5000),
    6 = gen_server:call(?NODE2, {fast_forward_to_commit, 7}, 5000),
    2 = gen_server:call(?NODE3, {fast_forward_to_commit, 2}, 5000),
    {atomic, List2} = emqx_cluster_rpc:status(),
    ?assertEqual(
        [{Node, 6}, {{Node, ?NODE2}, 6}, {{Node, ?NODE3}, 2}],
        tnx_ids(List2)
    ),
    ok.

t_handler_unexpected_msg(_Config) ->
    Handler = emqx_cluster_rpc_handler,
    OldPid = erlang:whereis(Handler),
    ok = gen_server:cast(Handler, unexpected_cast_msg),
    ignore = gen_server:call(Handler, unexpected_cast_msg),
    erlang:send(Handler, unexpected_info_msg),
    NewPid = erlang:whereis(Handler),
    ?assertEqual(OldPid, NewPid),
    ok.

tnx_ids(Status) ->
    lists:sort(
        lists:map(
            fun(#{tnx_id := TnxId, node := Node}) ->
                {Node, TnxId}
            end,
            Status
        )
    ).

start() ->
    {ok, Pid1} = emqx_cluster_rpc:start_link(),
    {ok, Pid2} = emqx_cluster_rpc:start_link({node(), ?NODE2}, ?NODE2, 500),
    {ok, Pid3} = emqx_cluster_rpc:start_link({node(), ?NODE3}, ?NODE3, 500),
    {ok, Pid4} = emqx_cluster_rpc_handler:start_link(100, 500),
    true = erlang:register(emqx_cluster_rpc_handler, Pid4),
    {ok, [Pid1, Pid2, Pid3, Pid4]}.

stop() ->
    [
        begin
            case erlang:whereis(N) of
                undefined ->
                    ok;
                P ->
                    erlang:unlink(P),
                    erlang:exit(P, kill)
            end
        end
     || N <- [?NODE1, ?NODE2, ?NODE3]
    ],
    gen_server:stop(emqx_cluster_rpc_handler, normal, 5000).

receive_msg(0, _Msg) ->
    ok;
receive_msg(Count, Msg) when Count > 0 ->
    receive
        Msg ->
            receive_msg(Count - 1, Msg)
    after 800 ->
        timeout
    end.

echo(Pid, Msg) ->
    erlang:send(Pid, Msg),
    ok.

failed_on_node(Pid) ->
    case Pid =:= self() of
        true -> ok;
        false -> "MFA return not ok"
    end.

failed_on_node_by_odd(Pid) ->
    case Pid =:= self() of
        true ->
            ok;
        false ->
            catch ets:new(test, [named_table, set, public]),
            Num = ets:update_counter(test, self(), {2, 1}, {self(), 1}),
            case Num rem 2 =:= 0 of
                false -> "MFA return not ok";
                true -> ok
            end
    end.

failed_on_other_recover_after_retry(Pid) ->
    case Pid =:= self() of
        true ->
            ok;
        false ->
            [{_, Res}] = ets:lookup(test, other_mfa_result),
            Res
    end.

multicall(M, F, A, N, T) ->
    emqx_cluster_rpc:do_multicall(M, F, A, N, T).

multicall(M, F, A) ->
    multicall(M, F, A, all, timer:minutes(2)).
