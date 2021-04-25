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
            pub_ptn => <<"device/{{id}}/xays/{{num}}/foo/bar/baz">>}).

%% setting fields:
%% - factor: spawn broker-pool-size * factor number of callers
%% - limit: limit the total number of topics for each caller
%% - sub_ptn: subscribe topic pattern like a/+/b/+/c/#
%%            or a/+/{{id}}/{{num}}/# to generate pattern with {{id}}
%%            replaced by worker id and {{num}} replaced by topic number.
%% - pub_ptn: topic pattern used to benchmark publish (match) performance
%%            e.g. a/x/{{id}}/{{num}}/foo/bar
start(#{factor := Factor} = Settings) ->
    BrokerPoolSize = emqx_vm:schedulers() * 2,
    Pids = start_callers(BrokerPoolSize * Factor, Settings),
    R = collect_results(Pids, #{subscribe => 0, match => 0}),
    io:format(user, "mnesia table(s) RAM: ~p~n", [ram_bytes()]),
    io:format(user, "~p~n", [erlang:memory()]),
    io:format(user, "~p~n", [R]),
    lists:foreach(fun(Pid) -> Pid ! stop end, Pids).

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

collect_results([], R) -> R;
collect_results([Pid | Pids], Acc = #{subscribe := Sr, match := Mr}) ->
    receive
        {Pid, #{subscribe := Srd, match := Mrd}} ->
            collect_results(Pids, Acc#{subscribe := Sr + Srd, match := Mr + Mrd})
    end.

%% ops per second
rps(T, N) -> round(N / (T / 1000000)).

start_caller(#{id := Id, limit := N, sub_ptn := SubPtn, pub_ptn := PubPtn}) ->
    Parent = self(),
    proc_lib:spawn_link(
        fun() ->
                SubTopics = make_topics(SubPtn, Id, N),
                {Ts, _} = timer:tc(fun() -> subscribe(SubTopics) end),
                PubTopics = make_topics(PubPtn, Id, N),
                {Tm, _} = timer:tc(fun() -> match(PubTopics) end),
                _ = erlang:send(Parent, {self(), #{subscribe => rps(Ts, N), match => rps(Tm, N)}}),
                receive
                    stop ->
                        ok
                end
        end).

match([]) -> ok;
match([Topic | Topics]) ->
    _ = emqx_router:lookup_routes(Topic),
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
