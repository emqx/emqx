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

t_discard_session(_) ->
    #{conninfo := ConnInfo} = ?ChanInfo,
    ok = emqx_cm:register_channel(<<"clientid">>, self(), ConnInfo),

    ok = meck:new(emqx_connection, [passthrough, no_history]),
    ok = meck:expect(emqx_connection, call, fun(_, _) -> ok end),
    ok = meck:expect(emqx_connection, call, fun(_, _, _) -> ok end),
    ok = emqx_cm:discard_session(<<"clientid">>),
    ok = emqx_cm:register_channel(<<"clientid">>, self(), ConnInfo),
    ok = emqx_cm:discard_session(<<"clientid">>),
    ok = emqx_cm:unregister_channel(<<"clientid">>),
    ok = emqx_cm:register_channel(<<"clientid">>, self(), ConnInfo),
    ok = emqx_cm:discard_session(<<"clientid">>),
    ok = meck:expect(emqx_connection, call, fun(_, _) -> error(testing) end),
    ok = meck:expect(emqx_connection, call, fun(_, _, _) -> error(testing) end),
    ok = emqx_cm:discard_session(<<"clientid">>),
    ok = emqx_cm:unregister_channel(<<"clientid">>),
    ok = meck:unload(emqx_connection).

t_discard_session_race(_) ->
    ?check_trace(
       begin
         #{conninfo := ConnInfo0} = ?ChanInfo,
         ConnInfo = ConnInfo0#{conn_mod := emqx_ws_connection},
         {Pid, Ref} = spawn_monitor(fun() -> receive stop -> exit(normal) end end),
         ok = emqx_cm:register_channel(<<"clientid">>, Pid, ConnInfo),
         Pid ! stop,
         receive {'DOWN', Ref, process, Pid, normal} -> ok end,
         ok = emqx_cm:discard_session(<<"clientid">>),
         {ok, _} = ?block_until(#{?snk_kind := "session_already_gone", pid := Pid}, 1000)
       end,
       fun(_, _) ->
               true
       end).

t_takeover_session(_) ->
    #{conninfo := ConnInfo} = ?ChanInfo,
    {error, not_found} = emqx_cm:takeover_session(<<"clientid">>),
    erlang:spawn_link(fun() ->
        ok = emqx_cm:register_channel(<<"clientid">>, self(), ConnInfo),
        receive
            {'$gen_call', From, {takeover, 'begin'}} ->
                gen_server:reply(From, test), ok
        end
    end),
    timer:sleep(100),
    {ok, emqx_connection, _, test} = emqx_cm:takeover_session(<<"clientid">>),
    emqx_cm:unregister_channel(<<"clientid">>).

t_kick_session(_) ->
    Info = #{conninfo := ConnInfo} = ?ChanInfo,
    ok = meck:new(emqx_connection, [passthrough, no_history]),
    ok = meck:expect(emqx_connection, call, fun(_, _) -> test end),
    ok = meck:expect(emqx_connection, call, fun(_, _, _) -> test end),
    {error, not_found} = emqx_cm:kick_session(<<"clientid">>),
    ok = emqx_cm:register_channel(<<"clientid">>, self(), ConnInfo),
    ok = emqx_cm:insert_channel_info(<<"clientid">>, Info, []),
    test = emqx_cm:kick_session(<<"clientid">>),
    erlang:spawn_link(
        fun() ->
            ok = emqx_cm:register_channel(<<"clientid">>, self(), ConnInfo),
            ok = emqx_cm:insert_channel_info(<<"clientid">>, Info, []),

            timer:sleep(1000)
        end),
    ct:sleep(100),
    test = emqx_cm:kick_session(<<"clientid">>),
    ok = emqx_cm:unregister_channel(<<"clientid">>),
    ok = meck:unload(emqx_connection).

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
