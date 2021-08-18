%%--------------------------------------------------------------------
%% Copyright (c) 2018-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-define(Node1, emqx_cluster_rpc).
-define(Node2, emqx_cluster_rpc2).
-define(Node3, emqx_cluster_rpc3).

all() -> [
    t_base_test,
    t_commit_fail_test,
    t_commit_ok_but_apply_fail_on_other_node,
    t_commit_ok_apply_fail_on_other_node_then_recover,
    t_del_stale_mfa
].
suite() -> [{timetrap, {minutes, 3}}].
groups() -> [].

init_per_suite(Config) ->
    application:load(emqx),
    ok = ekka:start(),
    %% Mock timeout
    ok = meck:new(emqx_cluster_rpc, [passthrough, no_history, no_link]),
    ok = meck:expect(emqx_cluster_rpc, del_mfa_after, fun(_) -> {timeout, 1000, del_stale_mfa} end),
    ok = meck:expect(emqx_cluster_rpc, catch_up_after, fun(_) -> {state_timeout, 1000, catch_up_delay} end),
    %%dbg:tracer(),
    %%dbg:p(all, c),
    %%dbg:tpl(emqx_cluster_rpc, cx),
    %%dbg:tpl(gen_statem, loop_receive, cx),
    %%dbg:tpl(gen_statem, loop_state_callback, cx),
    %%dbg:tpl(gen_statem, loop_callback_mode_result, cx),
    Config.

end_per_suite(_Config) ->
    meck:unload(emqx_cluster_rpc),
    ekka:stop(),
    ekka_mnesia:ensure_stopped(),
    ekka_mnesia:delete_schema(),
    %%dbg:stop(),
    ok.

init_per_testcase(_TestCase, Config) ->
    start(),
    Config.

end_per_testcase(_Config) ->
    stop(),
    ok.

t_base_test(_Config) ->
    ?assertEqual(emqx_cluster_rpc:status(), {atomic, []}),
    Pid = self(),
    MFA = {?MODULE, echo, [Pid, test]},
    {ok, TnxId} = gen_server:call(?Node1, {commit, MFA}),
    {atomic, Query} = emqx_cluster_rpc:query(TnxId),
    ?assertEqual(MFA,maps:get(mfa, Query)),
    ?assertEqual({node(), ?Node1},maps:get(initiator, Query)),
    ?assert(maps:is_key(created_at, Query)),
    ?assertEqual(ok,receive_msg(3, test)),
    sleep(400),
    {atomic, Status} = emqx_cluster_rpc:status(),
    ?assertEqual(3, length(Status)),
    ?assert(lists:all(fun(I) -> maps:get(tnx_id, I) =:= 1 end, Status)),
    ok.

t_commit_fail_test(_Config) ->
    emqx_cluster_rpc:reset(),
    {atomic, []} = emqx_cluster_rpc:status(),
    MFA = {?MODULE, failed_on_node, [erlang:whereis(?Node2)]},
    {error, "MFA return not ok"} = gen_server:call(?Node1, {commit, MFA}),
    ?assertEqual({atomic, []}, emqx_cluster_rpc:status()),
    ok.

t_commit_ok_but_apply_fail_on_other_node(_Config) ->
    emqx_cluster_rpc:reset(),
    {atomic, []} = emqx_cluster_rpc:status(),
    MFA = {?MODULE, failed_on_node, [erlang:whereis(?Node1)]},
    {ok, _TnxId} = gen_server:call(?Node1, {commit, MFA}),
    {atomic, [Status]} = emqx_cluster_rpc:status(),
    ?assertEqual(MFA, maps:get(mfa, Status)),
    ?assertEqual({node(), ?Node1}, maps:get(node, Status)),
    ?assertEqual(idle, element(1, sys:get_state(?Node1))),
    ?assertEqual(catch_up, element(1, sys:get_state(?Node2))),
    ?assertEqual(catch_up, element(1, sys:get_state(?Node3))),
    ok.

t_commit_ok_apply_fail_on_other_node_then_recover(_Config) ->
    emqx_cluster_rpc:reset(),
    {atomic, []} = emqx_cluster_rpc:status(),
    Now = erlang:system_time(second),
    MFA = {?MODULE, failed_on_other_recover_after_5_second, [erlang:whereis(?Node1), Now]},
    {ok, _TnxId} = gen_server:call(?Node1, {commit, MFA}),
    {atomic, [Status]} = emqx_cluster_rpc:status(),
    ?assertEqual(MFA, maps:get(mfa, Status)),
    ?assertEqual({node(), ?Node1}, maps:get(node, Status)),
    ?assertEqual(idle, element(1, sys:get_state(?Node1))),
    ?assertEqual(catch_up, element(1, sys:get_state(?Node2))),
    ?assertEqual(catch_up, element(1, sys:get_state(?Node3))),
    sleep(4000),
    {atomic, [Status1]} = emqx_cluster_rpc:status(),
    ?assertEqual(Status, Status1),
    sleep(2000),
    {atomic, NewStatus} = emqx_cluster_rpc:status(),
    ?assertEqual(3, length(NewStatus)),
    ?assertEqual(idle, element(1, sys:get_state(?Node1))),
    ?assertEqual(idle, element(1, sys:get_state(?Node2))),
    ?assertEqual(idle, element(1, sys:get_state(?Node3))),
    Pid = self(),
    MFAEcho = {?MODULE, echo, [Pid, test]},
    {ok, TnxId} = gen_server:call(?Node1, {commit, MFAEcho}),
    {atomic, Query} = emqx_cluster_rpc:query(TnxId),
    ?assertEqual(MFAEcho, maps:get(mfa, Query)),
    ?assertEqual({node(), ?Node1}, maps:get(initiator, Query)),
    ?assert(maps:is_key(created_at, Query)),
    ?assertEqual(ok, receive_msg(3, test)),
    ok.

t_del_stale_mfa(_Config) ->
    emqx_cluster_rpc:reset(),
    {atomic, []} = emqx_cluster_rpc:status(),
    MFA = {io, format, ["test"]},
    Keys = lists:seq(1, 50),
    Keys2 = lists:seq(51, 150),
    Ids =
        [begin
             {ok, TnxId} = gen_server:call(?Node1, {commit, MFA}),
             TnxId end ||_<- Keys],
    ?assertEqual(Keys, Ids),
    Ids2 =
        [begin
         {ok, TnxId} = gen_server:call(?Node1, {commit, MFA}),
         TnxId end ||_<- Keys2],
    ?assertEqual(Keys2, Ids2),
    erlang:send(?Node1, del_mfa_after),
    sleep(11500),
    [begin
         ?assertEqual({aborted, not_found}, emqx_cluster_rpc:query(I))
     end||I<- lists:seq(1, 50)],
    [begin
         {atomic,Map} = emqx_cluster_rpc:query(I),
         ?assertEqual(MFA, maps:get(mfa, Map)),
         ?assertEqual({node(), ?Node1}, maps:get(initiator, Map)),
         ?assert(maps:is_key(created_at, Map))
     end||I<- lists:seq(51, 150)],
    ok.

start() ->
    {ok, Pid1} = emqx_cluster_rpc:start_link({node(), ?Node1}, ?Node1),
    {ok, Pid2} = emqx_cluster_rpc:start_link({node(), ?Node2}, ?Node2),
    {ok, Pid3} = emqx_cluster_rpc:start_link({node(), ?Node3}, ?Node3),
    {ok, [Pid1, Pid2, Pid3]}.

stop() ->
    [begin
         case erlang:whereis(N) of
             undefined -> ok;
             P ->
                 erlang:unlink(P),
                 erlang:exit(P, kill)
         end end || N <- [?Node1, ?Node2, ?Node3]].

receive_msg(0, _Msg) -> ok;
receive_msg(Count, Msg) when Count > 0 ->
    receive Msg ->
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

failed_on_other_recover_after_5_second(Pid, CreatedAt) ->
    Now = erlang:system_time(second),
    case Pid =:= self() of
        true -> ok;
        false ->
            case Now < CreatedAt + 5 of
                true -> "MFA return not ok";
                false -> ok
            end
    end.

sleep(Second) ->
    receive _ -> ok
    after Second -> timeout
    end.
