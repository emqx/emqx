%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([run/1, run1/0, run1/4]).

-define(T(Expr), timer:tc(fun() -> Expr end)).

run1() -> run1(80, 1000, 80, 10000).

run1(Subs, SubOps, Pubs, PubOps) ->
    run(#{subscribers => Subs,
          publishers => Pubs,
          sub_ops => SubOps,
          pub_ops => PubOps,
          sub_ptn => <<"device/{{id}}/+/{{num}}/#">>,
          pub_ptn => <<"device/{{id}}/foo/{{num}}/bar/1/2/3/4/5">>
         }).

%% setting fields:
%% - subscribers: spawn this number of subscriber workers
%% - publishers: spawn this number of publisher workers
%% - sub_ops: the number of subscribes (route insert) each subscriber runs
%% - pub_ops: the number of publish (route lookups) each publisher runs
%% - sub_ptn: subscribe topic pattern like a/+/b/+/c/#
%%            or a/+/{{id}}/{{num}}/# to generate pattern with {{id}}
%%            replaced by worker id and {{num}} replaced by topic number.
%% - pub_ptn: topic pattern used to benchmark publish (match) performance
%%            e.g. a/x/{{id}}/{{num}}/foo/bar
run(#{subscribers := Subs,
      publishers := Pubs,
      sub_ops := SubOps,
      pub_ops := PubOps
     } = Settings) ->
    SubsPids = start_callers(Subs, fun  start_subscriber/1, Settings),
    PubsPids = start_callers(Pubs, fun start_publisher/1, Settings),
    _ = collect_results(SubsPids, subscriber_ready),
    io:format(user, "subscribe ...~n", []),
    {T1, SubsTime} =
        ?T(begin
               lists:foreach(fun(Pid) -> Pid ! start_subscribe end, SubsPids),
               collect_results(SubsPids, subscribe_time)
           end),
    io:format(user, "InsertTotalTime: ~s~n", [ns(T1)]),
    io:format(user, "InsertTimeAverage: ~s~n", [ns(SubsTime / Subs)]),
    io:format(user, "InsertRps: ~p~n", [rps(Subs * SubOps, T1)]),

    io:format(user, "lookup ...~n", []),
    {T2, PubsTime} =
        ?T(begin
               lists:foreach(fun(Pid) -> Pid ! start_lookup end, PubsPids),
               collect_results(PubsPids, lookup_time)
           end),
    io:format(user, "LookupTotalTime: ~s~n", [ns(T2)]),
    io:format(user, "LookupTimeAverage: ~s~n", [ns(PubsTime / Pubs)]),
    io:format(user, "LookupRps: ~p~n", [rps(Pubs * PubOps, T2)]),

    io:format(user, "mnesia table(s) RAM: ~p~n", [ram_bytes()]),

    io:format(user, "unsubscribe ...~n", []),
    {T3, ok} =
        ?T(begin
               lists:foreach(fun(Pid) -> Pid ! stop end, SubsPids),
               wait_until_empty()
           end),
    io:format(user, "TimeToUnsubscribeAll: ~s~n", [ns(T3)]).

wait_until_empty() ->
    case emqx_trie:empty() of
        true -> ok;
        false ->
            timer:sleep(5),
            wait_until_empty()
    end.

rps(N, NanoSec) -> N * 1_000_000 / NanoSec.

ns(T) when T > 1_000_000 -> io_lib:format("~p(s)", [T / 1_000_000]);
ns(T) when T > 1_000 -> io_lib:format("~p(ms)", [T / 1_000]);
ns(T) -> io_lib:format("~p(ns)", [T]).

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

start_callers(N, F, Settings) ->
    start_callers(N, F, Settings, []).

start_callers(0, _F, _Settings, Acc) ->
    lists:reverse(Acc);
start_callers(N, F, Settings, Acc) ->
    start_callers(N - 1, F, Settings, [F(Settings#{id => N}) | Acc]).

collect_results(Pids, Tag) ->
    collect_results(Pids, Tag, 0).

collect_results([], _Tag, R) -> R;
collect_results([Pid | Pids], Tag, R) ->
    receive
        {Pid, Tag, N} ->
            collect_results(Pids, Tag, N + R)
    end.

start_subscriber(#{id := Id, sub_ops := N, sub_ptn := SubPtn}) ->
    Parent = self(),
    proc_lib:spawn_link(
        fun() ->
                SubTopics = make_topics(SubPtn, Id, N),
                Parent ! {self(), subscriber_ready, 0},
                receive
                    start_subscribe ->
                        ok
                end,
                {Ts, _} = ?T(subscribe(SubTopics)),
                _ = erlang:send(Parent, {self(), subscribe_time, Ts/ N}),
                %% subscribers should not exit before publish test is done
                receive
                    stop ->
                        ok
                end
        end).

start_publisher(#{id := Id, pub_ops := N, pub_ptn := PubPtn, subscribers := Subs}) ->
    Parent = self(),
    proc_lib:spawn_link(
      fun() ->
              L = lists:seq(1, N),
              [Topic] = make_topics(PubPtn, (Id rem Subs) + 1, 1),
              receive
                  start_lookup ->
                      ok
              end,
              {Tm, ok} = ?T(lists:foreach(fun(_) -> match(Topic) end, L)),
              _ = erlang:send(Parent, {self(), lookup_time, Tm / N}),
              ok
      end).

match(Topic) ->
    [_] = emqx_router:match_routes(Topic).

subscribe([]) -> ok;
subscribe([Topic | Rest]) ->
    ok = emqx_broker:subscribe(Topic),
    subscribe(Rest).

make_topics(Ptn0, Id, Limit) ->
    Ptn = emqx_topic:words(Ptn0),
    F = fun(N) -> render(Id, N, Ptn) end,
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
