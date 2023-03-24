%%--------------------------------------------------------------------
%% Copyright (c) 2018-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_cm_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(CM, emqx_cm).
-define(ChanInfo,#{conninfo =>
                   #{socktype => tcp,
                     peername => {{127,0,0,1}, 5000},
                     sockname => {{127,0,0,1}, 1883},
                     peercert => nossl,
                     conn_mod => emqx_connection,
                     receive_maximum => 100}}).

-define(WAIT(PATTERN, TIMEOUT, RET),
        fun() ->
                receive PATTERN -> RET
                after TIMEOUT -> error({timeout, ?LINE}) end
        end()).

%%--------------------------------------------------------------------
%% CT callbacks
%%--------------------------------------------------------------------
suite() -> [{timetrap, {minutes, 2}}].

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:boot_modules(all),
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

%%--------------------------------------------------------------------
%% TODO: Add more test cases
%%--------------------------------------------------------------------

t_reg_unreg_channel(_) ->
    #{conninfo := ConnInfo} = ?ChanInfo,
    ok = emqx_cm:register_channel(<<"clientid">>, self(), ConnInfo),
    ok = emqx_cm:insert_channel_info(<<"clientid">>, ?ChanInfo, []),
    ?assertEqual([self()], emqx_cm:lookup_channels(<<"clientid">>)),
    ok = emqx_cm:unregister_channel(<<"clientid">>),
    ?assertEqual([], emqx_cm:lookup_channels(<<"clientid">>)).

t_get_set_chan_info(_) ->
    Info = #{conninfo := ConnInfo} = ?ChanInfo,
    ok = emqx_cm:register_channel(<<"clientid">>, self(), ConnInfo),
    ok = emqx_cm:insert_channel_info(<<"clientid">>, ?ChanInfo, []),

    ?assertEqual(Info, emqx_cm:get_chan_info(<<"clientid">>)),
    Info1 = Info#{proto_ver => 5},
    true = emqx_cm:set_chan_info(<<"clientid">>, Info1),
    ?assertEqual(Info1, emqx_cm:get_chan_info(<<"clientid">>)),
    ok = emqx_cm:unregister_channel(<<"clientid">>),
    ?assertEqual(undefined, emqx_cm:get_chan_info(<<"clientid">>)).

t_get_set_chan_stats(_) ->
    Stats = [{recv_oct, 10}, {send_oct, 8}],
    Info = #{conninfo := ConnInfo} = ?ChanInfo,
    ok = emqx_cm:register_channel(<<"clientid">>, self(), ConnInfo),
    ok = emqx_cm:insert_channel_info(<<"clientid">>, Info, Stats),

    ?assertEqual(Stats, emqx_cm:get_chan_stats(<<"clientid">>)),
    Stats1 = [{recv_oct, 10}|Stats],
    true = emqx_cm:set_chan_stats(<<"clientid">>, Stats1),
    ?assertEqual(Stats1, emqx_cm:get_chan_stats(<<"clientid">>)),
    ok = emqx_cm:unregister_channel(<<"clientid">>),
    ?assertEqual(undefined, emqx_cm:get_chan_stats(<<"clientid">>)).

t_open_session(_) ->
    ok = meck:new(emqx_connection, [passthrough, no_history]),
    ok = meck:expect(emqx_connection, call, fun(_, _) -> ok end),
    ok = meck:expect(emqx_connection, call, fun(_, _, _) -> ok end),

    ClientInfo = #{zone => external,
                   clientid => <<"clientid">>,
                   username => <<"username">>,
                   peerhost => {127,0,0,1}},
    ConnInfo = #{socktype => tcp,
                 peername => {{127,0,0,1}, 5000},
                 sockname => {{127,0,0,1}, 1883},
                 peercert => nossl,
                 conn_mod => emqx_connection,
                 receive_maximum => 100},
    {ok, #{session := Session1, present := false}}
        = emqx_cm:open_session(true, ClientInfo, ConnInfo),
    ?assertEqual(100, emqx_session:info(inflight_max, Session1)),
    {ok, #{session := Session2, present := false}}
        = emqx_cm:open_session(true, ClientInfo, ConnInfo),
    ?assertEqual(100, emqx_session:info(inflight_max, Session2)),

    emqx_cm:unregister_channel(<<"clientid">>),
    ok = meck:unload(emqx_connection).

rand_client_id() ->
    list_to_binary("client-id-" ++ integer_to_list(erlang:system_time())).

t_open_session_race_condition(_) ->
    ClientId = rand_client_id(),
    ClientInfo = #{zone => external,
                   clientid => ClientId,
                   username => <<"username">>,
                   peerhost => {127,0,0,1}},
    ConnInfo = #{socktype => tcp,
                 peername => {{127,0,0,1}, 5000},
                 sockname => {{127,0,0,1}, 1883},
                 peercert => nossl,
                 conn_mod => emqx_connection,
                 receive_maximum => 100},

    Parent = self(),
    OpenASession = fun() ->
                     timer:sleep(rand:uniform(100)),
                     OpenR = (emqx_cm:open_session(true, ClientInfo, ConnInfo)),
                     Parent ! OpenR,
                     case OpenR of
                         {ok, _} ->
                             receive
                                 {'$gen_call', From, discard} ->
                                     gen_server:reply(From, ok), ok
                             end;
                         {error, Reason} ->
                             exit(Reason)
                     end
                   end,
    N = 1000,
    Pids = lists:flatten([[spawn_monitor(OpenASession), spawn_monitor(OpenASession)] ||
                          _ <- lists:seq(1, N)]),

    WaitingRecv = fun _Wr(N1, N2, 0) ->
                          {N1, N2};
                      _Wr(N1, N2, Rest) ->
                          receive
                              {ok, _} -> _Wr(N1+1, N2, Rest-1);
                              {error, _} -> _Wr(N1, N2+1, Rest-1)
                          end
                  end,

    {Succeeded, Failed} = WaitingRecv(0, 0, 2 * N),
    ct:pal("Race condition status: succeeded=~p failed=~p~n", [Succeeded, Failed]),
    ?assertEqual(2 * N, length(Pids)),
    WaitForDowns =
        fun _Wd([{Pid, _Ref}]) -> Pid;
            _Wd(Pids0) ->
                receive
                    {'DOWN', DownRef, process, DownPid, _} ->
                        ?assert(lists:member({DownPid, DownRef}, Pids0)),
                        _Wd(lists:delete({DownPid, DownRef}, Pids0))
                after
                    10000 ->
                        exit(timeout)
                end
        end,
    Winner = WaitForDowns(Pids),

    ?assertMatch([_], ets:lookup(emqx_channel, ClientId)),
    ?assertEqual([Winner], emqx_cm:lookup_channels(ClientId)),
    ?assertMatch([_], ets:lookup(emqx_channel_conn, {ClientId, Winner})),
    ?assertMatch([_], ets:lookup(emqx_channel_registry, ClientId)),

    exit(Winner, kill),
    receive {'DOWN', _, process, Winner, _} -> ok end,
    ignored = gen_server:call(?CM, ignore, infinity), %% sync
    ok = flush_emqx_pool(),
    ?assertEqual([], emqx_cm:lookup_channels(ClientId)).

t_stepdown_sessiondiscard_normal(_) ->
    test_stepdown_session(discard, normal).

t_stepdown_sessiondiscard_shutdown(_) ->
    test_stepdown_session(discard, shutdown).

t_stepdown_sessiondiscard_shutdown_with_reason(_) ->
    test_stepdown_session(discard, {shutdown, discard}).

t_stepdown_sessiondiscard_timeout(_) ->
    test_stepdown_session(discard, timeout).

t_stepdown_sessiondiscard_noproc(_) ->
    test_stepdown_session(discard, noproc).

t_stepdown_sessionkick_normal(_) ->
    test_stepdown_session(kick, normal).

t_stepdown_sessionkick_shutdown(_) ->
    test_stepdown_session(kick, shutdown).

t_stepdown_sessionkick_shutdown_with_reason(_) ->
    test_stepdown_session(kick, {shutdown, discard}).

t_stepdown_sessionkick_timeout(_) ->
    test_stepdown_session(kick, timeout).

t_stepdown_sessionkick_noproc(_) ->
    test_stepdown_session(discard, noproc).

t_stepdown_sessiontakeover_begin_normal(_) ->
    test_stepdown_session({takeover, 'begin'}, normal).

t_stepdown_sessiontakeover_begin_shutdown(_) ->
    test_stepdown_session({takeover, 'begin'}, shutdown).

t_stepdown_sessiontakeover_begin_shutdown_with_reason(_) ->
    test_stepdown_session({takeover, 'begin'}, {shutdown, discard}).

t_stepdown_sessiontakeover_begin_timeout(_) ->
    test_stepdown_session({takeover, 'begin'}, timeout).

t_stepdown_sessiontakeover_begin_noproc(_) ->
    test_stepdown_session({takeover, 'begin'}, noproc).

t_stepdown_sessiontakeover_end_normal(_) ->
    test_stepdown_session({takeover, 'end'}, normal).

t_stepdown_sessiontakeover_end_shutdown(_) ->
    test_stepdown_session({takeover, 'end'}, shutdown).

t_stepdown_sessiontakeover_end_shutdown_with_reason(_) ->
    test_stepdown_session({takeover, 'end'}, {shutdown, discard}).

t_stepdown_sessiontakeover_end_timeout(_) ->
    test_stepdown_session({takeover, 'end'}, timeout).

t_stepdown_sessiontakeover_end_noproc(_) ->
    test_stepdown_session({takeover, 'end'}, noproc).

test_stepdown_session(Action, Reason) ->
    ClientId = rand_client_id(),
    #{conninfo := ConnInfo} = ?ChanInfo,
    FakeSessionFun =
        fun Loop() ->
            receive
                {'$gen_call', From, A} when A =:= kick orelse
                                            A =:= discard orelse
                                            A =:= {takeover, 'begin'} orelse
                                            A =:= {takeover, 'end'} ->
                    case Reason of
                        normal when A =:= kick orelse A =:= discard ->
                            gen_server:reply(From, ok);
                        timeout ->
                            %% no response to the call
                            Loop();
                        _ ->
                            exit(Reason)
                    end;
                Msg ->
                    ct:pal("(~p) fake_session_discarded ~p", [Action, Msg]),
                    Loop()
            end
        end,
    {Pid1, _} = spawn_monitor(FakeSessionFun),
    {Pid2, _} = spawn_monitor(FakeSessionFun),
    ok = emqx_cm:register_channel(ClientId, Pid1, ConnInfo),
    ok = emqx_cm:register_channel(ClientId, Pid1, ConnInfo),
    ok = emqx_cm:register_channel(ClientId, Pid2, ConnInfo),
    ?assertEqual([Pid1, Pid2], lists:sort(emqx_cm:lookup_channels(ClientId))),
    case Reason of
        noproc -> exit(Pid1, kill), exit(Pid2, kill);
        _ -> ok
    end,
    _ = case Action of
            kick -> emqx_cm:kick_session(ClientId);
            discard -> emqx_cm:discard_session(ClientId);
            {takeover, _} -> emqx_cm:takeover_session(ClientId)
        end,
    case Reason =:= timeout orelse Reason =:= noproc of
        true ->
            ?assertEqual(killed, ?WAIT({'DOWN', _, process, Pid1, R}, 2_000, R)),
            ?assertEqual(killed, ?WAIT({'DOWN', _, process, Pid2, R}, 2_000, R));
        false ->
            ?assertEqual(Reason, ?WAIT({'DOWN', _, process, Pid1, R}, 2_000, R)),
            ?assertEqual(Reason, ?WAIT({'DOWN', _, process, Pid2, R}, 2_000, R))
    end,
    ignored = gen_server:call(?CM, ignore, infinity), %% sync
    ok = flush_emqx_pool(),
    ?assertEqual([], emqx_cm:lookup_channels(ClientId)).

%% Channel deregistration is delegated to emqx_pool as a sync tasks.
%% The emqx_pool is pool of workers, and there is no way to know
%% which worker was picked for the last deregistration task.
%% This help function creates a large enough number of async tasks
%% to sync with the pool workers.
%% The number of tasks should be large enough to ensure all workers have
%% the chance to work on at least one of the tasks.
flush_emqx_pool() ->
    Ref = make_ref(),
    Self = self(),
    L = lists:seq(1, 1000),
    lists:foreach(fun(I) -> emqx_pool:async_submit(fun() -> Self ! {done, I, Ref} end, []) end, L),
    lists:foreach(fun(I) -> receive {done, I, Ref} -> ok end end, L).

t_discard_session_race(_) ->
    ClientId = rand_client_id(),
    ConnMod = emqx_ws_connection,
    ?check_trace(
       begin
         #{conninfo := ConnInfo0} = ?ChanInfo,
         ConnInfo = ConnInfo0#{conn_mod := ConnMod},
         {Pid, Ref} = spawn_monitor(fun() -> receive stop -> exit(normal) end end),
         ok = emqx_cm:register_channel(ClientId, Pid, ConnInfo),
         Pid ! stop,
         receive {'DOWN', Ref, process, Pid, normal} -> ok end,
         %% Here we simulate the situation where we are going to emqx_cm:discard_session/1
         %% but the session has died.
         emqx_cm:request_stepdown(discard, ConnMod, Pid),
         {ok, _} = ?block_until(#{?snk_kind := "session_already_gone", stale_pid := Pid}, 2000)
       end,
       fun(_, _) ->
               true
       end).

t_takeover_session(_) ->
    #{conninfo := ConnInfo} = ?ChanInfo,
    {error, not_found} = emqx_cm:takeover_session(<<"clientid">>),
    Dummy = emqx_session:dummy(),
    Downgraded = emqx_session:downgrade(Dummy),
    erlang:spawn_link(fun() ->
        ok = emqx_cm:register_channel(<<"clientid">>, self(), ConnInfo),
        receive
            {'$gen_call', From, {takeover, 'begin'}} ->
                gen_server:reply(From, Dummy), ok
        end
    end),
    timer:sleep(100),
    {ok, emqx_connection, _, Downgraded} = emqx_cm:takeover_session(<<"clientid">>),
    emqx_cm:unregister_channel(<<"clientid">>).

t_all_channels(_) ->
    ?assertEqual(true, is_list(emqx_cm:all_channels())).

t_lock_clientid(_) ->
    {true, Nodes} = emqx_cm_locker:lock(<<"clientid">>),
    ?assertEqual({true, Nodes}, emqx_cm_locker:lock(<<"clientid">>)),
    ?assertEqual({true, Nodes}, emqx_cm_locker:unlock(<<"clientid">>)),
    ?assertEqual({true, Nodes}, emqx_cm_locker:unlock(<<"clientid">>)).

t_message(_) ->
    ?CM ! testing,
    gen_server:cast(?CM, testing),
    gen_server:call(?CM, testing).
