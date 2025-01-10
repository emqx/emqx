%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("emqx/include/emqx_cm.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(CM, emqx_cm).
-define(ChanInfo, #{
    conninfo =>
        #{
            socktype => tcp,
            peername => {{127, 0, 0, 1}, 5000},
            sockname => {{127, 0, 0, 1}, 1883},
            peercert => nossl,
            conn_mod => emqx_connection,
            receive_maximum => 100
        }
}).

-define(WAIT(PATTERN, TIMEOUT, RET),
    fun() ->
        receive
            PATTERN -> RET
        after TIMEOUT -> error({timeout, ?LINE})
        end
    end()
).

%%--------------------------------------------------------------------
%% CT callbacks
%%--------------------------------------------------------------------
suite() -> [{timetrap, {minutes, 2}}].

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(proplists:get_value(apps, Config)).

%%--------------------------------------------------------------------
%% Helper fns
%%--------------------------------------------------------------------

open_session(CleanStart, ClientInfo, ConnInfo) ->
    emqx_cm:open_session(CleanStart, ClientInfo, ConnInfo, _WillMsg = undefined).

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
    ?assertEqual(undefined, emqx_cm:get_chan_info(<<"clientid">>)),

    ?assertError(
        function_clause,
        emqx_cm:insert_channel_info(undefined, #{}, [])
    ).

t_get_set_chan_stats(_) ->
    Stats = [{recv_oct, 10}, {send_oct, 8}],
    Info = #{conninfo := ConnInfo} = ?ChanInfo,
    ok = emqx_cm:register_channel(<<"clientid">>, self(), ConnInfo),
    ok = emqx_cm:insert_channel_info(<<"clientid">>, Info, Stats),

    ?assertEqual(Stats, emqx_cm:get_chan_stats(<<"clientid">>)),
    Stats1 = [{recv_oct, 10} | Stats],
    true = emqx_cm:set_chan_stats(<<"clientid">>, Stats1),
    ?assertEqual(Stats1, emqx_cm:get_chan_stats(<<"clientid">>)),

    ?assertError(
        function_clause,
        emqx_cm:set_chan_stats(undefined, [])
    ),

    ok = emqx_cm:unregister_channel(<<"clientid">>),
    ?assertEqual(undefined, emqx_cm:get_chan_stats(<<"clientid">>)).

t_open_session(_) ->
    ok = meck:new(emqx_connection, [passthrough, no_history]),
    ok = meck:expect(emqx_connection, call, fun(_, _) -> ok end),
    ok = meck:expect(emqx_connection, call, fun(_, _, _) -> ok end),

    ClientInfo = #{
        zone => default,
        listener => {tcp, default},
        clientid => <<"clientid">>,
        username => <<"username">>,
        peerhost => {127, 0, 0, 1}
    },
    ConnInfo = #{
        socktype => tcp,
        peername => {{127, 0, 0, 1}, 5000},
        sockname => {{127, 0, 0, 1}, 1883},
        peercert => nossl,
        conn_mod => emqx_connection,
        expiry_interval => 0,
        receive_maximum => 100
    },
    {ok, #{session := Session1, present := false}} =
        open_session(true, ClientInfo, ConnInfo),
    ?assertEqual(100, emqx_session:info(inflight_max, Session1)),
    {ok, #{session := Session2, present := false}} =
        open_session(true, ClientInfo, ConnInfo),
    ?assertEqual(100, emqx_session:info(inflight_max, Session2)),

    emqx_cm:unregister_channel(<<"clientid">>),
    ok = meck:unload(emqx_connection).

rand_client_id() ->
    list_to_binary("client-id-" ++ integer_to_list(erlang:system_time())).

t_open_session_race_condition(_) ->
    ClientId = rand_client_id(),
    ClientInfo = #{
        zone => default,
        listener => {tcp, default},
        clientid => ClientId,
        username => <<"username">>,
        peerhost => {127, 0, 0, 1}
    },
    ConnInfo = #{
        socktype => tcp,
        peername => {{127, 0, 0, 1}, 5000},
        sockname => {{127, 0, 0, 1}, 1883},
        peercert => nossl,
        conn_mod => emqx_connection,
        expiry_interval => 0,
        receive_maximum => 100
    },

    Parent = self(),
    OpenASession = fun() ->
        timer:sleep(rand:uniform(100)),
        OpenR = open_session(true, ClientInfo, ConnInfo),
        Parent ! OpenR,
        case OpenR of
            {ok, _} ->
                receive
                    {'$gen_call', From, discard} ->
                        gen_server:reply(From, ok),
                        ok
                end;
            {error, Reason} ->
                exit(Reason)
        end
    end,
    N = 1000,
    Pids = lists:flatten([
        [spawn_monitor(OpenASession), spawn_monitor(OpenASession)]
     || _ <- lists:seq(1, N)
    ]),

    WaitingRecv = fun
        _Wr(N1, N2, 0) ->
            {N1, N2};
        _Wr(N1, N2, Rest) ->
            receive
                {ok, _} -> _Wr(N1 + 1, N2, Rest - 1);
                {error, _} -> _Wr(N1, N2 + 1, Rest - 1)
            end
    end,

    {Succeeded, Failed} = WaitingRecv(0, 0, 2 * N),
    ct:pal("Race condition status: succeeded=~p failed=~p~n", [Succeeded, Failed]),
    ?assertEqual(2 * N, length(Pids)),
    WaitForDowns =
        fun
            _Wd([{Pid, _Ref}]) ->
                Pid;
            _Wd(Pids0) ->
                receive
                    {'DOWN', DownRef, process, DownPid, _} ->
                        ?assert(lists:member({DownPid, DownRef}, Pids0)),
                        _Wd(lists:delete({DownPid, DownRef}, Pids0))
                after 10000 ->
                    exit(timeout)
                end
        end,
    Winner = WaitForDowns(Pids),

    ?assertMatch([_], ets:lookup(?CHAN_TAB, ClientId)),
    ?assertEqual([Winner], emqx_cm:lookup_channels(ClientId)),
    ?assertMatch([_], ets:lookup(?CHAN_CONN_TAB, Winner)),
    ?assertMatch([_], ets:lookup(?CHAN_REG_TAB, ClientId)),

    exit(Winner, kill),
    receive
        {'DOWN', _, process, Winner, _} -> ok
    end,
    %% sync
    ignored = gen_server:call(?CM, ignore, infinity),
    ok = emqx_pool:flush_async_tasks(?CM_POOL),
    ?assertEqual([], emqx_cm:lookup_channels(ClientId)).

t_kick_session_discard_normal(_) ->
    test_stepdown_session(discard, normal).

t_kick_session_discard_shutdown(_) ->
    test_stepdown_session(discard, shutdown).

t_kick_session_discard_shutdown_with_reason(_) ->
    test_stepdown_session(discard, {shutdown, discard}).

t_kick_session_discard_timeout(_) ->
    test_stepdown_session(discard, timeout).

t_kick_session_discard_noproc(_) ->
    test_stepdown_session(discard, noproc).

t_kick_session_kick_normal(_) ->
    test_stepdown_session(kick, normal).

t_kick_session_kick_shutdown(_) ->
    test_stepdown_session(kick, shutdown).

t_kick_session_kick_shutdown_with_reason(_) ->
    test_stepdown_session(kick, {shutdown, kicked}).

t_kick_session_kick_timeout(_) ->
    test_stepdown_session(kick, timeout).

t_kick_session_kick_noproc(_) ->
    test_stepdown_session(kick, noproc).

t_stepdown_session_takeover_begin_normal(_) ->
    test_stepdown_session({takeover, 'begin'}, normal).

t_stepdown_session_takeover_begin_shutdown(_) ->
    test_stepdown_session({takeover, 'begin'}, shutdown).

t_stepdown_session_takeover_begin_shutdown_with_reason(_) ->
    test_stepdown_session({takeover, 'begin'}, {shutdown, kicked}).

t_stepdown_session_takeover_begin_timeout(_) ->
    test_stepdown_session({takeover, 'begin'}, timeout).

t_stepdown_session_takeover_begin_noproc(_) ->
    test_stepdown_session({takeover, 'begin'}, noproc).

t_stepdown_session_takeover_end_normal(_) ->
    test_stepdown_session({takeover, 'end'}, normal).

t_stepdown_session_takeover_end_shutdown(_) ->
    test_stepdown_session({takeover, 'end'}, shutdown).

t_stepdown_session_takeover_end_shutdown_with_reason(_) ->
    test_stepdown_session({takeover, 'end'}, {shutdown, kicked}).

t_stepdown_session_takeover_end_timeout(_) ->
    test_stepdown_session({takeover, 'end'}, timeout).

t_stepdown_session_takeover_end_noproc(_) ->
    test_stepdown_session({takeover, 'end'}, noproc).

test_stepdown_session(Action, Reason) ->
    ClientId = rand_client_id(),
    #{conninfo := ConnInfo} = ?ChanInfo,
    FakeSessionFun =
        fun Loop() ->
            receive
                {'$gen_call', From, A} when
                    A =:= kick orelse
                        A =:= discard orelse
                        A =:= {takeover, 'begin'} orelse
                        A =:= {takeover, 'end'}
                ->
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
    ?assertEqual(lists:sort([Pid1, Pid2]), lists:sort(emqx_cm:lookup_channels(ClientId))),
    case Reason of
        noproc ->
            exit(Pid1, kill),
            exit(Pid2, kill);
        _ ->
            ok
    end,
    ok =
        case Action of
            kick ->
                emqx_cm:kick_session(ClientId);
            discard ->
                emqx_cm:discard_session(ClientId);
            {takeover, _} ->
                none = emqx_cm:takeover_session_begin(ClientId),
                ok
        end,
    case Reason =:= timeout orelse Reason =:= noproc of
        true ->
            ?assertEqual(killed, ?WAIT({'DOWN', _, process, Pid1, R}, 2_000, R)),
            ?assertEqual(killed, ?WAIT({'DOWN', _, process, Pid2, R}, 2_000, R));
        false ->
            ?assertEqual(Reason, ?WAIT({'DOWN', _, process, Pid1, R}, 2_000, R)),
            ?assertEqual(Reason, ?WAIT({'DOWN', _, process, Pid2, R}, 2_000, R))
    end,
    % sync
    ignored = gen_server:call(?CM, ignore, infinity),
    ok = emqx_pool:flush_async_tasks(?CM_POOL),
    ?assertEqual([], emqx_cm:lookup_channels(ClientId)).

t_discard_session_race(_) ->
    ClientId = rand_client_id(),
    ?check_trace(
        #{timetrap => 60000},
        begin
            #{conninfo := ConnInfo0} = ?ChanInfo,
            ConnInfo = ConnInfo0#{conn_mod := emqx_ws_connection},
            {Pid, Ref} = spawn_monitor(fun() ->
                receive
                    stop -> exit(normal)
                end
            end),
            ok = emqx_cm:register_channel(ClientId, Pid, ConnInfo),
            Pid ! stop,
            receive
                {'DOWN', Ref, process, Pid, normal} -> ok
            end,
            ?assertMatch(ok, emqx_cm:discard_session(ClientId))
        end,
        []
    ).

t_takeover_session(_) ->
    #{conninfo := ConnInfo} = ?ChanInfo,
    ClientId = <<"clientid">>,
    none = emqx_cm:takeover_session_begin(ClientId),
    Parent = self(),
    ChanPid = erlang:spawn_link(fun() ->
        ok = emqx_cm:register_channel(ClientId, self(), ConnInfo),
        Parent ! registered,
        receive
            {'$gen_call', From1, {takeover, 'begin'}} ->
                gen_server:reply(From1, test),
                receive
                    {'$gen_call', From2, {takeover, 'end'}} ->
                        gen_server:reply(From2, _Pendings = [])
                end
        end
    end),
    receive
        registered -> ok
    end,
    {ok, test, State = {emqx_connection, ChanPid}} = emqx_cm:takeover_session_begin(ClientId),
    {ok, []} = emqx_cm:takeover_session_end(State),
    emqx_cm:unregister_channel(ClientId).

t_takeover_session_process_gone(_) ->
    #{conninfo := ConnInfo} = ?ChanInfo,
    ClientIDTcp = <<"clientidTCP">>,
    ClientIDWs = <<"clientidWs">>,
    ClientIDRpc = <<"clientidRPC">>,
    none = emqx_cm:takeover_session_begin(ClientIDTcp),
    none = emqx_cm:takeover_session_begin(ClientIDWs),
    meck:new(emqx_connection, [passthrough, no_history]),
    meck:expect(
        emqx_connection,
        call,
        fun
            (Pid, {takeover, 'begin'}, _) ->
                exit({noproc, {gen_server, call, [Pid, takeover_session]}});
            (Pid, What, Args) ->
                meck:passthrough([Pid, What, Args])
        end
    ),
    ok = emqx_cm:register_channel(ClientIDTcp, self(), ConnInfo),
    none = emqx_cm:takeover_session_begin(ClientIDTcp),
    meck:expect(
        emqx_connection,
        call,
        fun
            (_Pid, {takeover, 'begin'}, _) ->
                exit(noproc);
            (Pid, What, Args) ->
                meck:passthrough([Pid, What, Args])
        end
    ),
    ok = emqx_cm:register_channel(ClientIDWs, self(), ConnInfo),
    none = emqx_cm:takeover_session_begin(ClientIDWs),
    meck:expect(
        emqx_connection,
        call,
        fun
            (Pid, {takeover, 'begin'}, _) ->
                exit({noproc, {gen_server, call, [Pid, takeover_session]}});
            (Pid, What, Args) ->
                meck:passthrough([Pid, What, Args])
        end
    ),
    ok = emqx_cm:register_channel(ClientIDRpc, self(), ConnInfo),
    none = emqx_cm:takeover_session_begin(ClientIDRpc),
    emqx_cm:unregister_channel(ClientIDTcp),
    emqx_cm:unregister_channel(ClientIDWs),
    emqx_cm:unregister_channel(ClientIDRpc),
    meck:unload(emqx_connection).

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

t_live_connection_stream(_) ->
    Chans1 = spawn_dummy_chann(emqx_connection, 50),
    Chans2 = spawn_dummy_chann(emqx_ws_connection, 50),
    lists:foreach(
        fun({ClientId, Pid, ChanInfo}) ->
            ok = emqx_cm:register_channel(ClientId, Pid, ChanInfo)
        end,
        Chans1
    ),
    lists:foreach(
        fun({ClientId, Pid, ChanInfo}) ->
            ok = emqx_cm:register_channel(ClientId, Pid, ChanInfo)
        end,
        Chans2
    ),
    Stream = emqx_cm:live_connection_stream([emqx_connection, emqx_ws_connection]),
    Pids = emqx_utils_stream:fold(fun(Pid, Acc) -> [Pid | Acc] end, [], Stream),
    StreamedPids = lists:sort(Pids),
    ExpectedPids = lists:sort(lists:map(fun({_, Pid, _}) -> Pid end, Chans1 ++ Chans2)),
    lists:foreach(
        fun(Pid) ->
            unlink(Pid),
            exit(Pid, kill)
        end,
        ExpectedPids
    ),
    ?assertEqual(100, length(StreamedPids)),
    ?assertEqual(ExpectedPids, StreamedPids),
    ok.

spawn_dummy_chann(Mod, Count) ->
    #{conninfo := ConnInfo0} = ?ChanInfo,
    ConnInfo = ConnInfo0#{conn_mod => Mod},
    lists:map(
        fun(I) ->
            ClientId = list_to_binary(atom_to_list(Mod) ++ integer_to_list(I)),
            Pid = spawn_link(fun() -> timer:sleep(1000000) end),
            {ClientId, Pid, ConnInfo}
        end,
        lists:seq(1, Count)
    ).
