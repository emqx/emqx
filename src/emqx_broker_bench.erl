%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_broker_bench).

-ifdef(EMQX_BENCHMARK).

-export([start/1, run1/0, run1/2]).

run1() -> run1(4, 1000).

run1(Factor, Limit) ->
    start(#{factor => Factor,
            limit => Limit,
            sub_ptn => <<"device/{{id}}/+/{{num}}/#">>,
            pub_ptn => <<"device/{{id}}/foo/{{num}}/bar">>
           }).

%% setting fields:
%% - factor: spawn broker-pool-size * factor number of callers
%% - limit: limit the total number of topics for each caller
%% - sub_ptn: subscribe topic pattern like a/+/b/+/c/#
%%            or a/+/{{id}}/{{num}}/# to generate pattern with {{id}}
%%            replaced by worker id and {{num}} replaced by topic number.
%% - pub_ptn: topic pattern used to benchmark publish (match) performance
%%            e.g. a/x/{{id}}/{{num}}/foo/bar
start(#{factor := Factor, limit := Limit} = Settings) ->
    T1 = erlang:system_time(),
    BrokerPoolSize = emqx_vm:schedulers() * 2,
    WorkersCnt = BrokerPoolSize * Factor,
    Pids = start_callers(WorkersCnt, Settings),

    lists:foreach(fun(Pid) -> Pid ! start_subscribe end, Pids),
    SubsTime = collect_results(Pids, subscribe_time),
    T2 = erlang:system_time(),
    Span1 = sec_span(T2, T1),
    io:format(user, "InsertTotalTime(seconds): ~p~n", [Span1]),
    io:format(user, "InsertTimeAverage: ~p~n", [SubsTime / 1000000 / WorkersCnt]),
    io:format(user, "InsertRps: ~p~n", [WorkersCnt * Limit / Span1]),

    lists:foreach(fun(Pid) -> Pid ! start_lookup end, Pids),
    LkupTime = collect_results(Pids, lookup_time),
    T3 = erlang:system_time(),
    Span2 = sec_span(T3, T2),
    io:format(user, "LookupTotalTime(seconds): ~p~n", [Span2]),
    io:format(user, "LookupTimeAverage: ~p~n", [LkupTime / 1000000 / WorkersCnt]),
    io:format(user, "LookupRps: ~p~n", [WorkersCnt * Limit / Span2]),

    io:format(user, "mnesia table(s) RAM: ~p~n", [ram_bytes()]),
    io:format(user, "erlang memory: ~p~n", [erlang:memory()]),
    lists:foreach(fun(Pid) -> Pid ! stop end, Pids).

sec_span(T2, T1) -> abs(T2 - T1) / 1000000000.

ram_bytes() ->
    Wordsize = erlang:system_info(wordsize),
    mnesia:table_info(emqx_trie, memory) * Wordsize +
    case lists:member(emqx_trie_node, ets:all()) of
        true ->
            %% before 4.3
            mnesia:table_info(emqx_trie_node, memory) * Wordsize;
        false ->
            0
    end.

start_callers(0, _) -> [];
start_callers(N, Settings) ->
    [start_caller(Settings#{id => N}) | start_callers(N - 1, Settings)].

collect_results(Pids, Tag) ->
    collect_results(Pids, Tag, 0).

collect_results([], _Tag, R) -> R;
collect_results([Pid | Pids], Tag, R) ->
    receive
        {Pid, Tag, N} ->
            collect_results(Pids, Tag, N + R)
    end.

start_caller(#{id := Id, limit := N, sub_ptn := SubPtn, pub_ptn := PubPtn}) ->
    Parent = self(),
    proc_lib:spawn_link(
        fun() ->
                SubTopics = make_topics(SubPtn, Id, N),
                receive
                    start_subscribe ->
                        ok
                end,
                {Ts, _} = timer:tc(fun() -> subscribe(SubTopics) end),
                _ = erlang:send(Parent, {self(), subscribe_time, Ts}),
                PubTopics = make_topics(PubPtn, Id, N),
                receive
                    start_lookup ->
                        ok
                end,
                {Tm, _} = timer:tc(fun() -> match(PubTopics) end),
                _ = erlang:send(Parent, {self(), lookup_time, Tm}),
                receive
                    stop ->
                        ok
                end
        end).

match([]) -> ok;
match([Topic | Topics]) ->
    _ = emqx_router:match_routes(Topic),
    match(Topics).

subscribe([]) -> ok;
subscribe([Topic | Rest]) ->
    ok = emqx_broker:subscribe(Topic),
    subscribe(Rest).

make_topics(SubPtn0, Id, Limit) ->
    SubPtn = emqx_topic:words(SubPtn0),
    F = fun(N) -> render(Id, N, SubPtn) end,
    lists:map(F, lists:seq(1, Limit)).

render(ID, N, Ptn) ->
    render(ID, N, Ptn, []).

render(_ID, _N, [], Acc) ->
    emqx_topic:join(lists:reverse(Acc));
render(ID, N, [<<"{{id}}">> | T], Acc) ->
    render(ID, N, T, [integer_to_binary(ID) | Acc]);
render(ID, N, [<<"{{num}}">> | T], Acc) ->
    render(ID, N, T, [integer_to_binary(N) | Acc]);
render(ID, N, [H | T], Acc) ->
    render(ID, N, T, [H | Acc]).

-endif.
