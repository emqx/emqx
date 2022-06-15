%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ratelimiter_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-define(APP, emqx).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(BASE_CONF, <<
    ""
    "\n"
    "limiter {\n"
    "  bytes_in {\n"
    "    bucket.default {\n"
    "      rate = infinity\n"
    "      capacity = infinity\n"
    "    }\n"
    "  }\n"
    "\n"
    "  message_in {\n"
    "    bucket.default {\n"
    "      rate = infinity\n"
    "      capacity = infinity\n"
    "    }\n"
    "  }\n"
    "\n"
    "  connection {\n"
    "    bucket.default {\n"
    "      rate = infinity\n"
    "      capacity = infinity\n"
    "    }\n"
    "  }\n"
    "\n"
    "  message_routing {\n"
    "    bucket.default {\n"
    "      rate = infinity\n"
    "      capacity = infinity\n"
    "    }\n"
    "  }\n"
    "\n"
    "  batch {\n"
    "    bucket.retainer {\n"
    "      rate = infinity\n"
    "      capacity = infinity\n"
    "    }\n"
    "  }\n"
    "}\n"
    "\n"
    ""
>>).

-record(client, {
    counter :: counters:counter_ref(),
    start :: pos_integer(),
    endtime :: pos_integer(),
    obtained :: pos_integer(),
    rate :: float(),
    client :: emqx_htb_limiter:client()
}).

-define(LOGT(Format, Args), ct:pal("TEST_SUITE: " ++ Format, Args)).
-define(RATE(Rate), to_rate(Rate)).
-define(NOW, erlang:system_time(millisecond)).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------
all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    ok = emqx_common_test_helpers:load_config(emqx_limiter_schema, ?BASE_CONF),
    emqx_common_test_helpers:start_apps([?APP]),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([?APP]).

init_per_testcase(_TestCase, Config) ->
    Config.

load_conf() ->
    emqx_common_test_helpers:load_config(emqx_limiter_schema, ?BASE_CONF).

init_config() ->
    emqx_config:init_load(emqx_limiter_schema, ?BASE_CONF).

%%--------------------------------------------------------------------
%% Test Cases Bucket Level
%%--------------------------------------------------------------------
t_consume(_) ->
    Cfg = fun(Cfg) ->
        Cfg#{
            rate := 100,
            capacity := 100,
            initial := 100,
            max_retry_time := 1000,
            failure_strategy := force
        }
    end,
    Case = fun() ->
        Client = connect(default),
        {ok, L2} = emqx_htb_limiter:consume(50, Client),
        {ok, _L3} = emqx_htb_limiter:consume(150, L2)
    end,
    with_per_client(default, Cfg, Case).

t_retry(_) ->
    Cfg = fun(Cfg) ->
        Cfg#{
            rate := 50,
            capacity := 200,
            initial := 0,
            max_retry_time := 1000,
            failure_strategy := force
        }
    end,
    Case = fun() ->
        Client = connect(default),
        {ok, Client} = emqx_htb_limiter:retry(Client),
        {_, _, Retry, L2} = emqx_htb_limiter:check(150, Client),
        L3 = emqx_htb_limiter:set_retry(Retry, L2),
        timer:sleep(500),
        {ok, _L4} = emqx_htb_limiter:retry(L3)
    end,
    with_per_client(default, Cfg, Case).

t_restore(_) ->
    Cfg = fun(Cfg) ->
        Cfg#{
            rate := 1,
            capacity := 200,
            initial := 50,
            max_retry_time := 100,
            failure_strategy := force
        }
    end,
    Case = fun() ->
        Client = connect(default),
        {_, _, Retry, L2} = emqx_htb_limiter:check(150, Client),
        timer:sleep(200),
        {ok, L3} = emqx_htb_limiter:check(Retry, L2),
        Avaiable = emqx_htb_limiter:available(L3),
        ?assert(Avaiable >= 50)
    end,
    with_per_client(default, Cfg, Case).

t_max_retry_time(_) ->
    Cfg = fun(Cfg) ->
        Cfg#{
            rate := 1,
            capacity := 1,
            max_retry_time := 500,
            failure_strategy := drop
        }
    end,
    Case = fun() ->
        Client = connect(default),
        Begin = ?NOW,
        Result = emqx_htb_limiter:consume(101, Client),
        ?assertMatch({drop, _}, Result),
        Time = ?NOW - Begin,
        ?assert(Time >= 500 andalso Time < 550)
    end,
    with_per_client(default, Cfg, Case).

t_divisible(_) ->
    Cfg = fun(Cfg) ->
        Cfg#{
            divisible := true,
            rate := ?RATE("1000/1s"),
            initial := 600,
            capacity := 600
        }
    end,
    Case = fun() ->
        Client = connect(default),
        Result = emqx_htb_limiter:check(1000, Client),
        ?assertMatch(
            {partial, 400,
                #{
                    continuation := _,
                    diff := 400,
                    start := _,
                    need := 1000
                },
                _},
            Result
        )
    end,
    with_per_client(default, Cfg, Case).

t_low_watermark(_) ->
    Cfg = fun(Cfg) ->
        Cfg#{
            low_watermark := 400,
            rate := ?RATE("1000/1s"),
            initial := 1000,
            capacity := 1000
        }
    end,
    Case = fun() ->
        Client = connect(default),
        Result = emqx_htb_limiter:check(500, Client),
        ?assertMatch({ok, _}, Result),
        {_, Client2} = Result,
        Result2 = emqx_htb_limiter:check(101, Client2),
        ?assertMatch(
            {pause, _,
                #{
                    continuation := undefined,
                    diff := 0
                },
                _},
            Result2
        )
    end,
    with_per_client(default, Cfg, Case).

t_infinity_client(_) ->
    Fun = fun(#{per_client := Cli} = Bucket) ->
        Bucket2 = Bucket#{
            rate := infinity,
            capacity := infinity
        },
        Cli2 = Cli#{rate := infinity, capacity := infinity},
        Bucket2#{per_client := Cli2}
    end,
    Case = fun() ->
        Client = connect(default),
        InfVal = emqx_limiter_schema:infinity_value(),
        ?assertMatch(#{bucket := #{rate := InfVal}}, Client),
        Result = emqx_htb_limiter:check(100000, Client),
        ?assertEqual({ok, Client}, Result)
    end,
    with_bucket(default, Fun, Case).

t_try_restore_agg(_) ->
    Fun = fun(#{per_client := Cli} = Bucket) ->
        Bucket2 = Bucket#{
            rate := 1,
            capacity := 200,
            initial := 50
        },
        Cli2 = Cli#{
            rate := infinity,
            capacity := infinity,
            divisible := true,
            max_retry_time := 100,
            failure_strategy := force
        },
        Bucket2#{per_client := Cli2}
    end,
    Case = fun() ->
        Client = connect(default),
        {_, _, Retry, L2} = emqx_htb_limiter:check(150, Client),
        timer:sleep(200),
        {ok, L3} = emqx_htb_limiter:check(Retry, L2),
        Avaiable = emqx_htb_limiter:available(L3),
        ?assert(Avaiable >= 50)
    end,
    with_bucket(default, Fun, Case).

t_short_board(_) ->
    Fun = fun(#{per_client := Cli} = Bucket) ->
        Bucket2 = Bucket#{
            rate := ?RATE("100/1s"),
            initial := 0,
            capacity := 100
        },
        Cli2 = Cli#{
            rate := ?RATE("600/1s"),
            capacity := 600,
            initial := 600
        },
        Bucket2#{per_client := Cli2}
    end,
    Case = fun() ->
        Counter = counters:new(1, []),
        start_client(default, ?NOW + 2000, Counter, 20),
        timer:sleep(2100),
        check_average_rate(Counter, 2, 100)
    end,
    with_bucket(default, Fun, Case).

t_rate(_) ->
    Fun = fun(#{per_client := Cli} = Bucket) ->
        Bucket2 = Bucket#{
            rate := ?RATE("100/100ms"),
            initial := 0,
            capacity := infinity
        },
        Cli2 = Cli#{
            rate := infinity,
            capacity := infinity,
            initial := 0
        },
        Bucket2#{per_client := Cli2}
    end,
    Case = fun() ->
        Client = connect(default),
        Ts1 = erlang:system_time(millisecond),
        C1 = emqx_htb_limiter:available(Client),
        timer:sleep(1000),
        Ts2 = erlang:system_time(millisecond),
        C2 = emqx_htb_limiter:available(Client),
        ShouldInc = floor((Ts2 - Ts1) / 100) * 100,
        Inc = C2 - C1,
        ?assert(in_range(Inc, ShouldInc - 100, ShouldInc + 100), "test bucket rate")
    end,
    with_bucket(default, Fun, Case).

t_capacity(_) ->
    Capacity = 600,
    Fun = fun(#{per_client := Cli} = Bucket) ->
        Bucket2 = Bucket#{
            rate := ?RATE("100/100ms"),
            initial := 0,
            capacity := 600
        },
        Cli2 = Cli#{
            rate := infinity,
            capacity := infinity,
            initial := 0
        },
        Bucket2#{per_client := Cli2}
    end,
    Case = fun() ->
        Client = connect(default),
        timer:sleep(1000),
        C1 = emqx_htb_limiter:available(Client),
        ?assertEqual(Capacity, C1, "test bucket capacity")
    end,
    with_bucket(default, Fun, Case).

%%--------------------------------------------------------------------
%% Test Cases Global Level
%%--------------------------------------------------------------------
t_collaborative_alloc(_) ->
    GlobalMod = fun(Cfg) ->
        Cfg#{rate := ?RATE("600/1s")}
    end,

    Bucket1 = fun(#{per_client := Cli} = Bucket) ->
        Bucket2 = Bucket#{
            rate := ?RATE("400/1s"),
            initial := 0,
            capacity := 600
        },
        Cli2 = Cli#{
            rate := ?RATE("50"),
            capacity := 100,
            initial := 100
        },
        Bucket2#{per_client := Cli2}
    end,

    Bucket2 = fun(Bucket) ->
        Bucket2 = Bucket1(Bucket),
        Bucket2#{rate := ?RATE("200/1s")}
    end,

    Case = fun() ->
        C1 = counters:new(1, []),
        C2 = counters:new(1, []),
        start_client(b1, ?NOW + 2000, C1, 20),
        start_client(b2, ?NOW + 2000, C2, 30),
        timer:sleep(2100),
        check_average_rate(C1, 2, 300),
        check_average_rate(C2, 2, 300)
    end,

    with_global(
        GlobalMod,
        [{b1, Bucket1}, {b2, Bucket2}],
        Case
    ).

t_burst(_) ->
    GlobalMod = fun(Cfg) ->
        Cfg#{
            rate := ?RATE("200/1s"),
            burst := ?RATE("400/1s")
        }
    end,

    Bucket = fun(#{per_client := Cli} = Bucket) ->
        Bucket2 = Bucket#{
            rate := ?RATE("200/1s"),
            initial := 0,
            capacity := 200
        },
        Cli2 = Cli#{
            rate := ?RATE("50/1s"),
            capacity := 200,
            divisible := true
        },
        Bucket2#{per_client := Cli2}
    end,

    Case = fun() ->
        C1 = counters:new(1, []),
        C2 = counters:new(1, []),
        C3 = counters:new(1, []),
        start_client(b1, ?NOW + 2000, C1, 20),
        start_client(b2, ?NOW + 2000, C2, 30),
        start_client(b3, ?NOW + 2000, C3, 30),
        timer:sleep(2100),

        Total = lists:sum([counters:get(X, 1) || X <- [C1, C2, C3]]),
        in_range(Total / 2, 300)
    end,

    with_global(
        GlobalMod,
        [{b1, Bucket}, {b2, Bucket}, {b3, Bucket}],
        Case
    ).

t_limit_global_with_unlimit_other(_) ->
    GlobalMod = fun(Cfg) ->
        Cfg#{rate := ?RATE("600/1s")}
    end,

    Bucket = fun(#{per_client := Cli} = Bucket) ->
        Bucket2 = Bucket#{
            rate := infinity,
            initial := 0,
            capacity := infinity
        },
        Cli2 = Cli#{
            rate := infinity,
            capacity := infinity,
            initial := 0
        },
        Bucket2#{per_client := Cli2}
    end,

    Case = fun() ->
        C1 = counters:new(1, []),
        start_client(b1, ?NOW + 2000, C1, 20),
        timer:sleep(2100),
        check_average_rate(C1, 2, 600)
    end,

    with_global(
        GlobalMod,
        [{b1, Bucket}],
        Case
    ).

%%--------------------------------------------------------------------
%% Test Cases container
%%--------------------------------------------------------------------
t_new_container(_) ->
    C1 = emqx_limiter_container:new(),
    C2 = emqx_limiter_container:new([message_routing]),
    C3 = emqx_limiter_container:update_by_name(message_routing, default, C1),
    ?assertMatch(
        #{
            message_routing := _,
            retry_ctx := undefined,
            {retry, message_routing} := _
        },
        C2
    ),
    ?assertMatch(
        #{
            message_routing := _,
            retry_ctx := undefined,
            {retry, message_routing} := _
        },
        C3
    ),
    ok.

t_check_container(_) ->
    Cfg = fun(Cfg) ->
        Cfg#{
            rate := ?RATE("1000/1s"),
            initial := 1000,
            capacity := 1000
        }
    end,
    Case = fun() ->
        C1 = emqx_limiter_container:new(
            [message_routing],
            #{message_routing => default}
        ),
        {ok, C2} = emqx_limiter_container:check(1000, message_routing, C1),
        {pause, Pause, C3} = emqx_limiter_container:check(1000, message_routing, C2),
        timer:sleep(Pause),
        {ok, C4} = emqx_limiter_container:retry(message_routing, C3),
        Context = test,
        C5 = emqx_limiter_container:set_retry_context(Context, C4),
        RetryData = emqx_limiter_container:get_retry_context(C5),
        ?assertEqual(Context, RetryData)
    end,
    with_per_client(default, Cfg, Case).

%%--------------------------------------------------------------------
%% Test Cases misc
%%--------------------------------------------------------------------
t_limiter_manager(_) ->
    {error, _} = emqx_limiter_manager:start_server(message_routing),
    ignore = gen_server:call(emqx_limiter_manager, unexpected_call),
    ok = gen_server:cast(emqx_limiter_manager, unexpected_cast),
    erlang:send(erlang:whereis(emqx_limiter_manager), unexpected_info),
    ok = emqx_limiter_manager:format_status(normal, ok),
    ok.

t_limiter_app(_) ->
    try
        _ = emqx_limiter_app:start(undefined, undefined)
    catch
        _:_ ->
            ok
    end,
    ok = emqx_limiter_app:stop(undefined),
    ok.

t_limiter_server(_) ->
    State = emqx_limiter_server:info(message_routing),
    ?assertMatch(
        #{
            root := _,
            counter := _,
            index := _,
            buckets := _,
            type := message_routing
        },
        State
    ),

    Name = emqx_limiter_server:name(message_routing),
    ignored = gen_server:call(Name, unexpected_call),
    ok = gen_server:cast(Name, unexpected_cast),
    erlang:send(erlang:whereis(Name), unexpected_info),
    ok = emqx_limiter_server:format_status(normal, ok),
    ok.

t_decimal(_) ->
    ?assertEqual(infinity, emqx_limiter_decimal:add(infinity, 3)),
    ?assertEqual(5, emqx_limiter_decimal:add(2, 3)),
    ?assertEqual(infinity, emqx_limiter_decimal:sub(infinity, 3)),
    ?assertEqual(-1, emqx_limiter_decimal:sub(2, 3)),
    ?assertEqual(infinity, emqx_limiter_decimal:mul(infinity, 3)),
    ?assertEqual(6, emqx_limiter_decimal:mul(2, 3)),
    ?assertEqual(infinity, emqx_limiter_decimal:floor_div(infinity, 3)),
    ?assertEqual(2, emqx_limiter_decimal:floor_div(7, 3)),
    ok.

t_schema_unit(_) ->
    M = emqx_limiter_schema,
    ?assertEqual(limiter, M:namespace()),

    %% infinity
    ?assertEqual({ok, infinity}, M:to_rate(" infinity ")),

    %% xMB
    ?assertMatch({ok, _}, M:to_rate("100")),
    ?assertMatch({ok, _}, M:to_rate("  100   ")),
    ?assertMatch({ok, _}, M:to_rate("100MB")),

    %% xMB/s
    ?assertMatch({ok, _}, M:to_rate("100/s")),
    ?assertMatch({ok, _}, M:to_rate("100MB/s")),

    %% xMB/ys
    ?assertMatch({ok, _}, M:to_rate("100/10s")),
    ?assertMatch({ok, _}, M:to_rate("100MB/10s")),

    ?assertMatch({error, _}, M:to_rate("infini")),
    ?assertMatch({error, _}, M:to_rate("0")),
    ?assertMatch({error, _}, M:to_rate("MB")),
    ?assertMatch({error, _}, M:to_rate("10s")),
    ?assertMatch({error, _}, M:to_rate("100MB/")),
    ?assertMatch({error, _}, M:to_rate("100MB/xx")),
    ?assertMatch({error, _}, M:to_rate("100MB/1")),
    ?assertMatch({error, _}, M:to_rate("100/10x")),

    ?assertEqual({ok, emqx_limiter_schema:infinity_value()}, M:to_capacity("infinity")),
    ?assertEqual({ok, 100}, M:to_capacity("100")),
    ?assertEqual({ok, 100 * 1024}, M:to_capacity("100KB")),
    ?assertEqual({ok, 100 * 1024 * 1024}, M:to_capacity("100MB")),
    ?assertEqual({ok, 100 * 1024 * 1024 * 1024}, M:to_capacity("100GB")),
    ok.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
start_client(Name, EndTime, Counter, Number) ->
    lists:foreach(
        fun(_) ->
            spawn(fun() ->
                start_client(Name, EndTime, Counter)
            end)
        end,
        lists:seq(1, Number)
    ).

start_client(Name, EndTime, Counter) ->
    #{per_client := PerClient} =
        emqx_config:get([limiter, message_routing, bucket, Name]),
    #{rate := Rate} = PerClient,
    Client = #client{
        start = ?NOW,
        endtime = EndTime,
        counter = Counter,
        obtained = 0,
        rate = Rate,
        client = connect(Name)
    },
    client_loop(Client).

%% the simulated client will try to reach the configured rate as much as possible
%% note this client will not considered the capacity, so must make sure rate < capacity
client_loop(
    #client{
        start = Start,
        endtime = EndTime,
        obtained = Obtained,
        rate = Rate
    } = State
) ->
    Now = ?NOW,
    Period = emqx_limiter_schema:default_period(),
    MinPeriod = erlang:ceil(0.25 * Period),
    if
        Now >= EndTime ->
            stop;
        Now - Start < MinPeriod ->
            timer:sleep(client_random_val(MinPeriod)),
            client_loop(State);
        Obtained =< 0 ->
            Rand = client_random_val(Rate),
            client_try_check(Rand, State);
        true ->
            Span = Now - Start,
            CurrRate = Obtained * Period / Span,
            if
                CurrRate < Rate ->
                    Rand = client_random_val(Rate),
                    client_try_check(Rand, State);
                true ->
                    LeftTime = EndTime - Now,
                    CanSleep = erlang:min(LeftTime, client_random_val(MinPeriod div 2)),
                    timer:sleep(CanSleep),
                    client_loop(State)
            end
    end.

client_try_check(
    Need,
    #client{
        counter = Counter,
        endtime = EndTime,
        obtained = Obtained,
        client = Client
    } = State
) ->
    case emqx_htb_limiter:check(Need, Client) of
        {ok, Client2} ->
            case Need of
                #{need := Val} -> ok;
                Val -> ok
            end,
            counters:add(Counter, 1, Val),
            client_loop(State#client{obtained = Obtained + Val, client = Client2});
        {_, Pause, Retry, Client2} ->
            LeftTime = EndTime - ?NOW,
            if
                LeftTime =< 0 ->
                    stop;
                true ->
                    timer:sleep(erlang:min(Pause, LeftTime)),
                    client_try_check(Retry, State#client{client = Client2})
            end
    end.

%% XXX not a god test, because client's rate maybe bigger than global rate
%% so if client' rate = infinity
%% client's divisible should be true or capacity must be bigger than number of each consume
client_random_val(infinity) ->
    1000;
%% random in 0.5Range ~ 1Range
client_random_val(Range) ->
    Half = erlang:floor(Range) div 2,
    Rand = rand:uniform(Half + 1) + Half,
    erlang:max(1, Rand).

to_rate(Str) ->
    {ok, Rate} = emqx_limiter_schema:to_rate(Str),
    Rate.

with_global(Modifier, BuckeTemps, Case) ->
    Fun = fun(Cfg) ->
        #{bucket := #{default := BucketCfg}} = Cfg2 = Modifier(Cfg),
        Fun = fun({Name, BMod}, Acc) ->
            Acc#{Name => BMod(BucketCfg)}
        end,
        Buckets = lists:foldl(Fun, #{}, BuckeTemps),
        Cfg2#{bucket := Buckets}
    end,

    with_config([limiter, message_routing], Fun, Case).

with_bucket(Bucket, Modifier, Case) ->
    Path = [limiter, message_routing, bucket, Bucket],
    with_config(Path, Modifier, Case).

with_per_client(Bucket, Modifier, Case) ->
    Path = [limiter, message_routing, bucket, Bucket, per_client],
    with_config(Path, Modifier, Case).

with_config(Path, Modifier, Case) ->
    Cfg = emqx_config:get(Path),
    NewCfg = Modifier(Cfg),
    ct:pal("test with config:~p~n", [NewCfg]),
    emqx_config:put(Path, NewCfg),
    emqx_limiter_server:restart(message_routing),
    timer:sleep(500),
    DelayReturn = delay_return(Case),
    emqx_config:put(Path, Cfg),
    DelayReturn().

delay_return(Case) ->
    try
        Return = Case(),
        fun() -> Return end
    catch
        Type:Reason:Trace ->
            fun() -> erlang:raise(Type, Reason, Trace) end
    end.

connect(Name) ->
    {ok, Limiter} = emqx_limiter_server:connect(message_routing, Name),
    Limiter.

check_average_rate(Counter, Second, Rate) ->
    Cost = counters:get(Counter, 1),
    PerSec = Cost / Second,
    ?LOGT("Cost:~p PerSec:~p Rate:~p ~n", [Cost, PerSec, Rate]),
    ?assert(in_range(PerSec, Rate)).

print_average_rate(Counter, Second) ->
    Cost = counters:get(Counter, 1),
    PerSec = Cost / Second,
    ct:pal("Cost:~p PerSec:~p ~n", [Cost, PerSec]).

in_range(Val, Expected) when Val < Expected * 0.5 ->
    ct:pal("Val:~p smaller than min bound", [Val]),
    false;
in_range(Val, Expected) when Val > Expected * 1.8 ->
    ct:pal("Val:~p bigger than max bound", [Val]),
    false;
in_range(_, _) ->
    true.

in_range(Val, Min, _Max) when Val < Min ->
    ct:pal("Val:~p smaller than min bound:~p~n", [Val, Min]),
    false;
in_range(Val, _Min, Max) when Val > Max ->
    ct:pal("Val:~p bigger than max bound:~p~n", [Val, Max]),
    false;
in_range(_, _, _) ->
    true.

apply_modifier(Name, Modifier, Cfg) when is_list(Name) ->
    Pairs = lists:zip(Name, Modifier),
    apply_modifier(Pairs, Cfg);
apply_modifier(Name, Modifier, #{default := Template} = Cfg) ->
    Cfg#{Name => Modifier(Template)}.

apply_modifier(Pairs, #{default := Template}) ->
    Fun = fun({N, M}, Acc) ->
        Acc#{N => M(Template)}
    end,
    lists:foldl(Fun, #{}, Pairs).
