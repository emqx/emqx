%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include("emqx.hrl").
-include_lib("eunit/include/eunit.hrl").

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
    ok = emqx_cm:register_channel(<<"clientid">>, ?ChanInfo, []),
    ?assertEqual([self()], emqx_cm:lookup_channels(<<"clientid">>)),
    ok = emqx_cm:unregister_channel(<<"clientid">>),
    ?assertEqual([], emqx_cm:lookup_channels(<<"clientid">>)).

t_get_set_chan_info(_) ->
    Info = ?ChanInfo,
    ok = emqx_cm:register_channel(<<"clientid">>, Info, []),
    ?assertEqual(Info, emqx_cm:get_chan_info(<<"clientid">>)),
    Info1 = Info#{proto_ver => 5},
    true = emqx_cm:set_chan_info(<<"clientid">>, Info1),
    ?assertEqual(Info1, emqx_cm:get_chan_info(<<"clientid">>)),
    ok = emqx_cm:unregister_channel(<<"clientid">>),
    ?assertEqual(undefined, emqx_cm:get_chan_info(<<"clientid">>)).

t_get_set_chan_stats(_) ->
    Stats = [{recv_oct, 10}, {send_oct, 8}],
    ok = emqx_cm:register_channel(<<"clientid">>, ?ChanInfo, Stats),
    ?assertEqual(Stats, emqx_cm:get_chan_stats(<<"clientid">>)),
    Stats1 = [{recv_oct, 10}|Stats],
    true = emqx_cm:set_chan_stats(<<"clientid">>, Stats1),
    ?assertEqual(Stats1, emqx_cm:get_chan_stats(<<"clientid">>)),
    ok = emqx_cm:unregister_channel(<<"clientid">>),
    ?assertEqual(undefined, emqx_cm:get_chan_stats(<<"clientid">>)).

t_open_session(_) ->
    ok = meck:new(emqx_connection, [passthrough, no_history]),
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

t_open_session_race_condition(_) ->
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
    [spawn(
      fun() ->
              spawn(OpenASession),
              spawn(OpenASession)
      end) || _ <- lists:seq(1, 1000)],

    WaitingRecv = fun _Wr(N1, N2, 0) ->
                          {N1, N2};
                      _Wr(N1, N2, Rest) ->
                          receive
                              {ok, _} -> _Wr(N1+1, N2, Rest-1);
                              {error, _} -> _Wr(N1, N2+1, Rest-1)
                          end
                  end,

    ct:pal("Race condition status: ~p~n", [WaitingRecv(0, 0, 2000)]),

    ?assertEqual(1, ets:info(emqx_channel, size)),
    ?assertEqual(1, ets:info(emqx_channel_conn, size)),
    ?assertEqual(1, ets:info(emqx_channel_registry, size)),

    [Pid] = emqx_cm:lookup_channels(<<"clientid">>),
    exit(Pid, kill), timer:sleep(100),
    ?assertEqual([], emqx_cm:lookup_channels(<<"clientid">>)).

t_kick_session_discard_normal(_) ->
    test_kick_session(discard, normal).

t_kick_session_discard_shutdown(_) ->
    test_kick_session(discard, shutdown).

t_kick_session_discard_shutdown_with_reason(_) ->
    test_kick_session(discard, {shutdown, discard}).

t_kick_session_discard_timeout(_) ->
    test_kick_session(discard, timeout).

t_kick_session_discard_noproc(_) ->
    test_kick_session(discard, noproc).

t_kick_session_kick_normal(_) ->
    test_kick_session(discard, normal).

t_kick_session_kick_shutdown(_) ->
    test_kick_session(discard, shutdown).

t_kick_session_kick_shutdown_with_reason(_) ->
    test_kick_session(discard, {shutdown, discard}).

t_kick_session_kick_timeout(_) ->
    test_kick_session(discard, timeout).

t_kick_session_kick_noproc(_) ->
    test_kick_session(discard, noproc).

test_kick_session(Action, Reason) ->
    ClientId = rand_client_id(),
    #{conninfo := ConnInfo} = ?ChanInfo,
    FakeSessionFun =
        fun Loop() ->
                     receive
                         {'$gen_call', From, A} when A =:= kick orelse
                                                     A =:= discard ->
                             case Reason of
                                 normal ->
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
    ok = case Action of
             kick -> emqx_cm:kick_session(ClientId);
             discard -> emqx_cm:discard_session(ClientId)
         end,
    case Reason =:= timeout orelse Reason =:= noproc of
        true ->
            ?assertEqual(killed, ?WAIT({'DOWN', _, process, Pid1, R}, 2_000, R)),
            ?assertEqual(killed, ?WAIT({'DOWN', _, process, Pid2, R}, 2_000, R));
        false ->
            ?assertEqual(Reason, ?WAIT({'DOWN', _, process, Pid1, R}, 2_000, R)),
            ?assertEqual(Reason, ?WAIT({'DOWN', _, process, Pid2, R}, 2_000, R))
    end,
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
    Self = self(),
    L = lists:seq(1, 1000),
    lists:foreach(fun(I) -> emqx_pool:async_submit(fun() -> Self ! {done, I} end, []) end, L),
    lists:foreach(fun(I) -> receive {done, I} -> ok end end, L).

t_takeover_session(_) ->
    {error, not_found} = emqx_cm:takeover_session(<<"clientid">>),
    erlang:spawn(fun() ->
                     ok = emqx_cm:register_channel(<<"clientid">>, ?ChanInfo, []),
                     receive
                         {'$gen_call', From, {takeover, 'begin'}} ->
                             gen_server:reply(From, test), ok
                     end
                 end),
    timer:sleep(100),
    {ok, emqx_connection, _, test} = emqx_cm:takeover_session(<<"clientid">>),
    emqx_cm:unregister_channel(<<"clientid">>).

t_all_channels(_) ->
    ?assertEqual(true, is_list(emqx_cm:all_channels())).

t_lock_clientid(_) ->
    {true, _Nodes} = emqx_cm_locker:lock(<<"clientid">>),
    {true, _Nodes} = emqx_cm_locker:lock(<<"clientid">>),
    {true, _Nodes} = emqx_cm_locker:unlock(<<"clientid">>),
    {true, _Nodes} = emqx_cm_locker:unlock(<<"clientid">>).

t_message(_) ->
    ?CM ! testing,
    gen_server:cast(?CM, testing),
    gen_server:call(?CM, testing).
