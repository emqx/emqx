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

-module(emqx_stats_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

t_cast_useless_msg(_)->
    emqx_stats:setstat('notExis', 1),
    with_proc(fun() ->
        emqx_stats ! useless,
        ?assertEqual(ok, gen_server:cast(emqx_stats, useless))
        end).

t_get_error_state(_) ->
    Conns = emqx_stats:getstats(),
    ?assertEqual([], Conns).

t_get_state(_) ->
    with_proc(fun() ->
        ?assertEqual(undefined, emqx_stats:getstat('notExist')),
        SetConnsCount = emqx_stats:statsfun('connections.count'),
        SetConnsCount(1),
        ?assertEqual(1, emqx_stats:getstat('connections.count')),
        emqx_stats:setstat('connections.count', 2),
        ?assertEqual(2, emqx_stats:getstat('connections.count')),
        emqx_stats:setstat('connections.count', 'connections.max', 3),
        timer:sleep(100),
        ?assertEqual(3, emqx_stats:getstat('connections.count')),
        ?assertEqual(3, emqx_stats:getstat('connections.max')),
        emqx_stats:setstat('connections.count', 'connections.max', 2),
        timer:sleep(100),
        ?assertEqual(2, emqx_stats:getstat('connections.count')),
        ?assertEqual(3, emqx_stats:getstat('connections.max')),
        SetConns = emqx_stats:statsfun('connections.count', 'connections.max'),
        SetConns(4),
        timer:sleep(100),
        ?assertEqual(4, emqx_stats:getstat('connections.count')),
        ?assertEqual(4, emqx_stats:getstat('connections.max')),
        Conns = emqx_stats:getstats(),
        ?assertEqual(4, proplists:get_value('connections.count', Conns)),
        ?assertEqual(4, proplists:get_value('connections.max', Conns))
    end).

t_update_interval(_) ->
    TickMs = 200,
    with_proc(fun() ->
        SleepMs = TickMs * 2 + TickMs div 2, %% sleep for 2.5 ticks
        emqx_stats:cancel_update(cm_stats),
        UpdFun = fun() -> emqx_stats:setstat('connections.count',  1) end,
        ok = emqx_stats:update_interval(stats_test, UpdFun),
        timer:sleep(SleepMs),
        ok = emqx_stats:update_interval(stats_test, UpdFun),
        timer:sleep(SleepMs),
        ?assertEqual(1, emqx_stats:getstat('connections.count'))
    end, TickMs).

t_helper(_) ->
    TickMs = 200,
    TestF =
        fun(CbModule, CbFun) ->
                SleepMs = TickMs + TickMs div 2, %% sleep for 1.5 ticks
                Ref = make_ref(),
                Tester = self(),
                UpdFun =
                    fun() ->
                            CbModule:CbFun(),
                            Tester ! Ref,
                            ok
                    end,
                    ok = emqx_stats:update_interval(stats_test, UpdFun),
                    timer:sleep(SleepMs),
                    receive Ref -> ok after 2000 -> error(timeout) end
        end,
    MkTestFun =
        fun(CbModule, CbFun) ->
                fun() ->
                        with_proc(fun() -> TestF(CbModule, CbFun) end, TickMs)
                end
        end,
    [{"emqx_broker", MkTestFun(emqx_broker, stats_fun)},
     {"emqx_sm", MkTestFun(emqx_sm, stats_fun)},
     {"emqx_router_helper", MkTestFun(emqx_router_helper, stats_fun)},
     {"emqx_cm", MkTestFun(emqx_cm, stats_fun)}
    ].

with_proc(F) ->
    {ok, _Pid} = emqx_stats:start_link(),
    with_stop(F).

with_proc(F, TickMs) ->
    {ok, _Pid} = emqx_stats:start_link(#{tick_ms => TickMs}),
    with_stop(F).

with_stop(F) ->
    try
        %% make a synced call to the gen_server so we know it has
        %% started running, hence it is safe to continue with less risk of race condition
        ?assertEqual(ignored, gen_server:call(emqx_stats, ignored)),
        F()
    after
        ok = emqx_stats:stop()
    end.
