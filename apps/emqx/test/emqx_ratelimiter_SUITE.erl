%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(BASE_CONF, <<"">>).

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
-define(ROOT_COUNTER_IDX, 1).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------
all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    ok = load_conf(),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_TestCase, Config) ->
    ok = emqx_config:erase(limiter),
    ok = load_conf(),
    Config.

end_per_testcase(_TestCase, Config) ->
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
            burst := 0,
            initial := 100,
            max_retry_time := 1000,
            failure_strategy := force
        }
    end,
    Case = fun(BucketCfg) ->
        Client = connect(BucketCfg),
        {ok, L2} = emqx_htb_limiter:consume(50, Client),
        {ok, _L3} = emqx_htb_limiter:consume(150, L2)
    end,
    with_per_client(Cfg, Case).

t_retry(_) ->
    Cfg = fun(Cfg) ->
        Cfg#{
            rate := 50,
            burst := 150,
            initial := 0,
            max_retry_time := 1000,
            failure_strategy := force
        }
    end,
    Case = fun(BucketCfg) ->
        Client = connect(BucketCfg),
        {ok, Client2} = emqx_htb_limiter:retry(Client),
        {_, _, Retry, L2} = emqx_htb_limiter:check(150, Client2),
        L3 = emqx_htb_limiter:set_retry(Retry, L2),
        timer:sleep(500),
        {ok, _L4} = emqx_htb_limiter:retry(L3)
    end,
    with_per_client(Cfg, Case).

t_restore(_) ->
    Cfg = fun(Cfg) ->
        Cfg#{
            rate := 1,
            burst := 199,
            initial := 50,
            max_retry_time := 100,
            failure_strategy := force
        }
    end,
    Case = fun(BucketCfg) ->
        Client = connect(BucketCfg),
        {_, _, Retry, L2} = emqx_htb_limiter:check(150, Client),
        timer:sleep(200),
        {ok, L3} = emqx_htb_limiter:check(Retry, L2),
        Avaiable = emqx_htb_limiter:available(L3),
        ?assert(Avaiable >= 50)
    end,
    with_per_client(Cfg, Case).

t_max_retry_time(_) ->
    Cfg = fun(Cfg) ->
        Cfg#{
            rate := 1,
            burst := 0,
            max_retry_time := 500,
            failure_strategy := drop
        }
    end,
    Case = fun(BucketCfg) ->
        Client = connect(BucketCfg),
        Begin = ?NOW,
        Result = emqx_htb_limiter:consume(101, Client),
        ?assertMatch({drop, _}, Result),
        End = ?NOW,
        Time = End - Begin,
        ?assert(
            Time >= 500 andalso Time < 550,
            lists:flatten(io_lib:format("Begin:~p, End:~p, Time:~p~n", [Begin, End, Time]))
        )
    end,
    with_per_client(Cfg, Case).

t_divisible(_) ->
    Cfg = fun(Cfg) ->
        Cfg#{
            divisible := true,
            rate := ?RATE("1000/1s"),
            initial := 600,
            burst := 0
        }
    end,
    Case = fun(BucketCfg) ->
        Client = connect(BucketCfg),
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
    with_per_client(Cfg, Case).

t_low_watermark(_) ->
    Cfg = fun(Cfg) ->
        Cfg#{
            low_watermark := 400,
            rate := ?RATE("1000/1s"),
            initial := 1000,
            burst := 0
        }
    end,
    Case = fun(BucketCfg) ->
        Client = connect(BucketCfg),
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
    with_per_client(Cfg, Case).

t_infinity_client(_) ->
    Fun = fun(Cfg) -> Cfg end,
    Case = fun(Cfg) ->
        Client = connect(Cfg),
        ?assertMatch(infinity, Client),
        Result = emqx_htb_limiter:check(100000, Client),
        ?assertEqual({ok, Client}, Result)
    end,
    with_per_client(Fun, Case).

t_try_restore_with_bucket(_) ->
    Fun = fun(#{client := Cli} = Bucket) ->
        Bucket2 = Bucket#{
            rate := 100,
            burst := 100,
            initial := 50
        },
        Cli2 = Cli#{
            rate := infinity,
            burst := 0,
            divisible := true,
            max_retry_time := 100,
            failure_strategy := force
        },
        Bucket2#{client := Cli2}
    end,
    Case = fun(Cfg) ->
        Client = connect(Cfg),
        {_, _, Retry, L2} = emqx_htb_limiter:check(150, Client),
        timer:sleep(200),
        {ok, L3} = emqx_htb_limiter:check(Retry, L2),
        Avaiable = emqx_htb_limiter:available(L3),
        ?assert(Avaiable >= 50)
    end,
    with_bucket(Fun, Case).

t_short_board(_) ->
    Fun = fun(#{client := Cli} = Bucket) ->
        Bucket2 = Bucket#{
            rate := ?RATE("100/1s"),
            initial := 0,
            burst := 0
        },
        Cli2 = Cli#{
            rate := ?RATE("600/1s"),
            burst := 0,
            initial := 600
        },
        Bucket2#{client := Cli2}
    end,
    Case = fun(Cfg) ->
        Counter = counters:new(1, []),
        start_client(Cfg, ?NOW + 2000, Counter, 20),
        timer:sleep(2100),
        check_average_rate(Counter, 2, 100)
    end,
    with_bucket(Fun, Case).

t_rate(_) ->
    Fun = fun(#{client := Cli} = Bucket) ->
        Bucket2 = Bucket#{
            rate := ?RATE("100/100ms"),
            initial := 0,
            burst := 0
        },
        Cli2 = Cli#{
            rate := infinity,
            burst := 0,
            initial := 0
        },
        Bucket2#{client := Cli2}
    end,
    Case = fun(Cfg) ->
        Time = 1000,
        Client = connect(Cfg),
        C1 = emqx_htb_limiter:available(Client),
        timer:sleep(1100),
        C2 = emqx_htb_limiter:available(Client),
        ShouldInc = floor(Time / 100) * 100,
        Inc = C2 - C1,
        ?assert(in_range(Inc, ShouldInc - 100, ShouldInc + 100), "test bucket rate")
    end,
    with_bucket(Fun, Case).

t_capacity(_) ->
    Capacity = 1200,
    Fun = fun(#{client := Cli} = Bucket) ->
        Bucket2 = Bucket#{
            rate := ?RATE("100/100ms"),
            initial := 0,
            burst := 200
        },
        Cli2 = Cli#{
            rate := infinity,
            burst := 0,
            initial := 0
        },
        Bucket2#{client := Cli2}
    end,
    Case = fun(Cfg) ->
        Client = connect(Cfg),
        timer:sleep(1500),
        C1 = emqx_htb_limiter:available(Client),
        ?assertEqual(Capacity, C1, "test bucket capacity")
    end,
    with_bucket(Fun, Case).

%%--------------------------------------------------------------------
%% Test Cases Global Level
%%--------------------------------------------------------------------
t_collaborative_alloc(_) ->
    GlobalMod = fun(Cfg) ->
        Cfg#{message_routing => #{rate => ?RATE("600/1s"), burst => 0}}
    end,

    Bucket1 = fun(#{client := Cli} = Bucket) ->
        Bucket2 = Bucket#{
            rate := ?RATE("400/1s"),
            initial := 0,
            burst := 200
        },
        Cli2 = Cli#{
            rate := ?RATE("50"),
            burst := 50,
            initial := 100
        },
        Bucket2#{client := Cli2}
    end,

    Bucket2 = fun(Bucket) ->
        Bucket2 = Bucket1(Bucket),
        Bucket2#{rate := ?RATE("200/1s")}
    end,

    Case = fun() ->
        C1 = counters:new(1, []),
        C2 = counters:new(1, []),
        start_client({b1, Bucket1}, ?NOW + 2000, C1, 20),
        start_client({b2, Bucket2}, ?NOW + 2000, C2, 30),
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
            message_routing => #{
                rate => ?RATE("200/1s"),
                burst => ?RATE("400/1s")
            }
        }
    end,

    Bucket = fun(#{client := Cli} = Bucket) ->
        Bucket2 = Bucket#{
            rate := ?RATE("200/1s"),
            initial := 0,
            burst := 0
        },
        Cli2 = Cli#{
            rate := ?RATE("50/1s"),
            burst := 150,
            divisible := true
        },
        Bucket2#{client := Cli2}
    end,

    Case = fun() ->
        C1 = counters:new(1, []),
        C2 = counters:new(1, []),
        C3 = counters:new(1, []),
        start_client({b1, Bucket}, ?NOW + 2000, C1, 20),
        start_client({b2, Bucket}, ?NOW + 2000, C2, 30),
        start_client({b3, Bucket}, ?NOW + 2000, C3, 30),
        timer:sleep(2100),

        Total = lists:sum([counters:get(X, 1) || X <- [C1, C2, C3]]),
        in_range(Total / 2, 300)
    end,

    with_global(
        GlobalMod,
        [{b1, Bucket}, {b2, Bucket}, {b3, Bucket}],
        Case
    ).

%%--------------------------------------------------------------------
%% Test Cases container
%%--------------------------------------------------------------------
t_check_container(_) ->
    Cfg = fun(Cfg) ->
        Cfg#{
            rate := ?RATE("1000/1s"),
            initial := 1000,
            burst := 0
        }
    end,
    Case = fun(#{client := Client} = BucketCfg) ->
        C1 = emqx_limiter_container:get_limiter_by_types(
            ?MODULE,
            [message_routing],
            #{message_routing => BucketCfg, client => #{message_routing => Client}}
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
    with_per_client(Cfg, Case).

%%--------------------------------------------------------------------
%% Test Cases misc
%%--------------------------------------------------------------------
t_limiter_manager(_) ->
    {error, _} = emqx_limiter_manager:start_server(message_routing),
    ignore = gen_server:call(emqx_limiter_manager, unexpected_call),
    ok = gen_server:cast(emqx_limiter_manager, unexpected_cast),
    erlang:send(erlang:whereis(emqx_limiter_manager), unexpected_info),
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

    ?assertEqual({ok, infinity}, M:to_capacity("infinity")),
    ?assertEqual({ok, 100}, M:to_capacity("100")),
    ?assertEqual({ok, 100 * 1024}, M:to_capacity("100KB")),
    ?assertEqual({ok, 100 * 1024 * 1024}, M:to_capacity("100MB")),
    ?assertEqual({ok, 100 * 1024 * 1024 * 1024}, M:to_capacity("100GB")),
    ok.

t_compatibility_for_capacity(_) ->
    CfgStr = <<
        ""
        "\n"
        "listeners.tcp.default {\n"
        "  bind = \"0.0.0.0:1883\"\n"
        "  max_connections = 1024000\n"
        "  limiter.messages.capacity = infinity\n"
        "  limiter.client.messages.capacity = infinity\n"
        "}\n"
        ""
    >>,
    ?assertMatch(
        #{
            messages := #{burst := 0},
            client := #{messages := #{burst := 0}}
        },
        parse_and_check(CfgStr)
    ).

t_compatibility_for_message_in(_) ->
    CfgStr = <<
        ""
        "\n"
        "listeners.tcp.default {\n"
        "  bind = \"0.0.0.0:1883\"\n"
        "  max_connections = 1024000\n"
        "  limiter.message_in.rate = infinity\n"
        "  limiter.client.message_in.rate = infinity\n"
        "}\n"
        ""
    >>,
    ?assertMatch(
        #{
            messages := #{rate := infinity},
            client := #{messages := #{rate := infinity}}
        },
        parse_and_check(CfgStr)
    ).

t_compatibility_for_bytes_in(_) ->
    CfgStr = <<
        ""
        "\n"
        "listeners.tcp.default {\n"
        "  bind = \"0.0.0.0:1883\"\n"
        "  max_connections = 1024000\n"
        "  limiter.bytes_in.rate = infinity\n"
        "  limiter.client.bytes_in.rate = infinity\n"
        "}\n"
        ""
    >>,
    ?assertMatch(
        #{
            bytes := #{rate := infinity},
            client := #{bytes := #{rate := infinity}}
        },
        parse_and_check(CfgStr)
    ).

t_extract_with_type(_) ->
    IsOnly = fun
        (_Key, Cfg) when map_size(Cfg) =/= 1 ->
            false;
        (Key, Cfg) ->
            maps:is_key(Key, Cfg)
    end,
    Checker = fun
        (Type, #{client := Client} = Cfg) ->
            Cfg2 = maps:remove(client, Cfg),
            IsOnly(Type, Client) andalso
                (IsOnly(Type, Cfg2) orelse
                    map_size(Cfg2) =:= 0);
        (Type, Cfg) ->
            IsOnly(Type, Cfg)
    end,
    ?assertEqual(undefined, emqx_limiter_utils:extract_with_type(messages, undefined)),
    ?assert(
        Checker(
            messages,
            emqx_limiter_utils:extract_with_type(messages, #{
                messages => #{rate => 1}, bytes => #{rate => 1}
            })
        )
    ),
    ?assert(
        Checker(
            messages,
            emqx_limiter_utils:extract_with_type(messages, #{
                messages => #{rate => 1},
                bytes => #{rate => 1},
                client => #{messages => #{rate => 2}}
            })
        )
    ),
    ?assert(
        Checker(
            messages,
            emqx_limiter_utils:extract_with_type(messages, #{
                client => #{messages => #{rate => 2}, bytes => #{rate => 1}}
            })
        )
    ).

t_add_bucket(_) ->
    Checker = fun(Size) ->
        #{buckets := Buckets} = sys:get_state(emqx_limiter_server:whereis(bytes)),
        ?assertEqual(Size, maps:size(Buckets), Buckets)
    end,
    DefBucket = emqx_limiter_utils:default_bucket_config(),
    ?assertEqual(ok, emqx_limiter_server:add_bucket(?FUNCTION_NAME, bytes, undefined)),
    Checker(0),
    ?assertEqual(ok, emqx_limiter_server:add_bucket(?FUNCTION_NAME, bytes, DefBucket)),
    Checker(0),
    ?assertEqual(
        ok, emqx_limiter_server:add_bucket(?FUNCTION_NAME, bytes, DefBucket#{rate := 100})
    ),
    Checker(1),
    ?assertEqual(ok, emqx_limiter_server:del_bucket(?FUNCTION_NAME, bytes)),
    Checker(0),
    ok.

%%--------------------------------------------------------------------
%% Test Cases  Create Instance
%%--------------------------------------------------------------------
t_create_instance_with_infinity_node(_) ->
    emqx_limiter_manager:insert_bucket(?FUNCTION_NAME, bytes, ?FUNCTION_NAME),
    Cases = make_create_test_data_with_infinity_node(?FUNCTION_NAME),
    lists:foreach(
        fun({Cfg, Expected}) ->
            {ok, Result} = emqx_limiter_server:connect(?FUNCTION_NAME, bytes, Cfg),
            IsMatched =
                case is_atom(Expected) of
                    true ->
                        Result =:= Expected;
                    _ ->
                        Expected(Result)
                end,
            ?assert(
                IsMatched,
                lists:flatten(
                    io_lib:format("Got unexpected:~p~n, Cfg:~p~n", [
                        Result, Cfg
                    ])
                )
            )
        end,
        Cases
    ),
    emqx_limiter_manager:delete_bucket(?FUNCTION_NAME, bytes),
    ok.

t_not_exists_instance(_) ->
    Cfg = #{bytes => #{rate => 100, burst => 0, initial => 0}},
    ?assertEqual(
        {error, invalid_bucket},
        emqx_limiter_server:connect(?FUNCTION_NAME, bytes, Cfg)
    ),

    ?assertEqual(
        {ok, infinity},
        emqx_limiter_server:connect(?FUNCTION_NAME, not_exists, Cfg)
    ),
    ok.

t_create_instance_with_node(_) ->
    GlobalMod = fun(Cfg) ->
        Cfg#{
            message_routing => #{rate => ?RATE("200/1s"), burst => 0},
            messages => #{rate => ?RATE("200/1s"), burst => 0}
        }
    end,

    B1 = fun(Bucket) ->
        Bucket#{rate := ?RATE("400/1s")}
    end,

    B2 = fun(Bucket) ->
        Bucket#{rate := infinity}
    end,

    IsRefLimiter = fun
        ({ok, #{tokens := _}}, _IsRoot) ->
            false;
        ({ok, #{bucket := #{index := ?ROOT_COUNTER_IDX}}}, true) ->
            true;
        ({ok, #{bucket := #{index := Index}}}, false) when Index =/= ?ROOT_COUNTER_IDX ->
            true;
        (Result, _IsRoot) ->
            ct:pal("The result is:~p~n", [Result]),
            false
    end,

    Case = fun() ->
        BucketCfg = make_limiter_cfg(),

        ?assert(
            IsRefLimiter(emqx_limiter_server:connect(b1, message_routing, B1(BucketCfg)), false)
        ),
        ?assert(
            IsRefLimiter(emqx_limiter_server:connect(b2, message_routing, B2(BucketCfg)), true)
        ),
        ?assert(IsRefLimiter(emqx_limiter_server:connect(x, messages, undefined), true)),
        ?assertNot(IsRefLimiter(emqx_limiter_server:connect(x, bytes, undefined), false))
    end,

    with_global(
        GlobalMod,
        [{b1, B1}, {b2, B2}],
        Case
    ),
    ok.

%%--------------------------------------------------------------------
%% Test Cases emqx_esockd_htb_limiter
%%--------------------------------------------------------------------
t_create_esockd_htb_limiter(_) ->
    Opts = emqx_esockd_htb_limiter:new_create_options(?FUNCTION_NAME, bytes, undefined),
    ?assertMatch(
        #{module := _, id := ?FUNCTION_NAME, type := bytes, bucket := undefined},
        Opts
    ),

    Limiter = emqx_esockd_htb_limiter:create(Opts),
    ?assertMatch(
        #{module := _, name := bytes, limiter := infinity},
        Limiter
    ),

    ?assertEqual(ok, emqx_esockd_htb_limiter:delete(Limiter)),
    ok.

t_esockd_htb_consume(_) ->
    ClientCfg = default_client_config(),
    Cfg = #{client => #{bytes => ClientCfg#{rate := 50, max_retry_time := 0}}},
    Opts = emqx_esockd_htb_limiter:new_create_options(?FUNCTION_NAME, bytes, Cfg),
    Limiter = emqx_esockd_htb_limiter:create(Opts),

    C1R = emqx_esockd_htb_limiter:consume(51, Limiter),
    ?assertMatch({pause, _Ms, _Limiter2}, C1R),

    timer:sleep(300),
    C2R = emqx_esockd_htb_limiter:consume(50, Limiter),
    ?assertMatch({ok, _}, C2R),
    ok.

%%--------------------------------------------------------------------
%% Test Cases short paths
%%--------------------------------------------------------------------
t_node_short_paths(_) ->
    CfgStr = <<"limiter {max_conn_rate = \"1000\", messages_rate = \"100\", bytes_rate = \"10\"}">>,
    ok = emqx_common_test_helpers:load_config(emqx_limiter_schema, CfgStr),
    Accessor = fun emqx_limiter_utils:get_node_opts/1,
    ?assertMatch(#{rate := 100.0}, Accessor(connection)),
    ?assertMatch(#{rate := 10.0}, Accessor(messages)),
    ?assertMatch(#{rate := 1.0}, Accessor(bytes)),
    ?assertMatch(#{rate := infinity}, Accessor(message_routing)),
    ?assertEqual(undefined, emqx:get_config([limiter, connection], undefined)).

t_compatibility_for_node_short_paths(_) ->
    CfgStr =
        <<"limiter {max_conn_rate = \"1000\", connection.rate = \"500\", bytes.rate = \"200\"}">>,
    ok = emqx_common_test_helpers:load_config(emqx_limiter_schema, CfgStr),
    Accessor = fun emqx_limiter_utils:get_node_opts/1,
    ?assertMatch(#{rate := 100.0}, Accessor(connection)),
    ?assertMatch(#{rate := 20.0}, Accessor(bytes)).

t_listener_short_paths(_) ->
    CfgStr = <<
        ""
        "listeners.tcp.default {max_conn_rate = \"1000\", messages_rate = \"100\", bytes_rate = \"10\"}"
        ""
    >>,
    ok = emqx_common_test_helpers:load_config(emqx_schema, CfgStr),
    ListenerOpt = emqx:get_config([listeners, tcp, default]),
    ?assertMatch(
        #{
            client := #{
                messages := #{rate := 10.0},
                bytes := #{rate := 1.0}
            },
            connection := #{rate := 100.0}
        },
        emqx_limiter_utils:get_listener_opts(ListenerOpt)
    ).

t_compatibility_for_listener_short_paths(_) ->
    CfgStr = <<
        "" "listeners.tcp.default {max_conn_rate = \"1000\", limiter.connection.rate = \"500\"}" ""
    >>,
    ok = emqx_common_test_helpers:load_config(emqx_schema, CfgStr),
    ListenerOpt = emqx:get_config([listeners, tcp, default]),
    ?assertMatch(
        #{
            connection := #{rate := 100.0}
        },
        emqx_limiter_utils:get_listener_opts(ListenerOpt)
    ).

t_no_limiter_for_listener(_) ->
    CfgStr = <<>>,
    ok = emqx_common_test_helpers:load_config(emqx_schema, CfgStr),
    ListenerOpt = emqx:get_config([listeners, tcp, default]),
    ?assertMatch(
        #{connection := #{rate := infinity}},
        emqx_limiter_utils:get_listener_opts(ListenerOpt)
    ).

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
start_client(Cfg, EndTime, Counter, Number) ->
    lists:foreach(
        fun(_) ->
            spawn(fun() ->
                do_start_client(Cfg, EndTime, Counter)
            end)
        end,
        lists:seq(1, Number)
    ).

do_start_client({Name, CfgFun}, EndTime, Counter) ->
    do_start_client(Name, CfgFun(make_limiter_cfg()), EndTime, Counter);
do_start_client(Cfg, EndTime, Counter) ->
    do_start_client(?MODULE, Cfg, EndTime, Counter).

do_start_client(Name, Cfg, EndTime, Counter) ->
    #{client := PerClient} = Cfg,
    #{rate := Rate} = PerClient,
    Client = #client{
        start = ?NOW,
        endtime = EndTime,
        counter = Counter,
        obtained = 0,
        rate = Rate,
        client = connect(Name, Cfg)
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

with_global(Modifier, Buckets, Case) ->
    with_config([limiter], Modifier, Buckets, Case).

with_bucket(Modifier, Case) ->
    Cfg = Modifier(make_limiter_cfg()),
    add_bucket(Cfg),
    Case(Cfg),
    del_bucket().

with_per_client(Modifier, Case) ->
    #{client := Client} = Cfg = make_limiter_cfg(),
    Cfg2 = Cfg#{client := Modifier(Client)},
    add_bucket(Cfg2),
    Case(Cfg2),
    del_bucket().

with_config(Path, Modifier, Buckets, Case) ->
    Cfg = emqx_config:get(Path),
    NewCfg = Modifier(Cfg),
    emqx_config:put(Path, NewCfg),
    emqx_limiter_server:restart(message_routing),
    timer:sleep(500),
    BucketCfg = make_limiter_cfg(),
    lists:foreach(
        fun
            ({Name, BucketFun}) ->
                add_bucket(Name, BucketFun(BucketCfg));
            (BucketFun) ->
                add_bucket(BucketFun(BucketCfg))
        end,
        Buckets
    ),
    DelayReturn = delay_return(Case),
    lists:foreach(
        fun
            ({Name, _Cfg}) ->
                del_bucket(Name);
            (_Cfg) ->
                del_bucket()
        end,
        Buckets
    ),
    emqx_config:put(Path, Cfg),
    emqx_limiter_server:restart(message_routing),
    DelayReturn().

delay_return(Case) ->
    try
        Return = Case(),
        fun() -> Return end
    catch
        Type:Reason:Trace ->
            fun() -> erlang:raise(Type, Reason, Trace) end
    end.

connect({Name, CfgFun}) ->
    connect(Name, CfgFun(make_limiter_cfg()));
connect(Cfg) ->
    connect(?MODULE, Cfg).

connect(Name, Cfg) ->
    {ok, Limiter} = emqx_limiter_server:connect(Name, message_routing, Cfg),
    Limiter.

make_limiter_cfg() ->
    Client = #{
        rate => infinity,
        initial => 0,
        burst => 0,
        low_watermark => 0,
        divisible => false,
        max_retry_time => timer:seconds(5),
        failure_strategy => force
    },
    #{client => Client, rate => infinity, initial => 0, burst => 0}.

add_bucket(Cfg) ->
    add_bucket(?MODULE, Cfg).

add_bucket(Name, Cfg) ->
    emqx_limiter_server:add_bucket(Name, message_routing, Cfg).

del_bucket() ->
    del_bucket(?MODULE).

del_bucket(Name) ->
    emqx_limiter_server:del_bucket(Name, message_routing).

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

parse_and_check(ConfigString) ->
    ok = emqx_common_test_helpers:load_config(emqx_schema, ConfigString),
    emqx:get_config([listeners, tcp, default, limiter]).

make_create_test_data_with_infinity_node(FakeInstnace) ->
    Infinity = emqx_htb_limiter:make_infinity_limiter(),
    ClientCfg = default_client_config(),
    InfinityRef = emqx_limiter_bucket_ref:infinity_bucket(),
    MkC = fun(Rate) ->
        #{client => #{bytes => ClientCfg#{rate := Rate}}}
    end,
    MkB = fun(Rate) ->
        #{bytes => #{rate => Rate, burst => 0, initial => 0}}
    end,

    MkA = fun(Client, Bucket) ->
        maps:merge(MkC(Client), MkB(Bucket))
    end,
    IsRefLimiter = fun(Expected) ->
        fun
            (#{tokens := _}) -> false;
            (#{bucket := Bucket}) -> Bucket =:= Expected;
            (_) -> false
        end
    end,

    IsTokenLimiter = fun(Expected) ->
        fun
            (#{tokens := _, bucket := Bucket}) -> Bucket =:= Expected;
            (_) -> false
        end
    end,

    [
        %% default situation, no limiter setting
        {undefined, Infinity},

        %% client = undefined bucket = undefined
        {#{}, Infinity},
        %% client = undefined bucket = infinity
        {MkB(infinity), Infinity},
        %% client = undefined bucket = other
        {MkB(100), IsRefLimiter(FakeInstnace)},

        %% client = infinity bucket = undefined
        {MkC(infinity), Infinity},
        %% client = infinity bucket = infinity
        {MkA(infinity, infinity), Infinity},

        %% client = infinity bucket = other
        {MkA(infinity, 100), IsRefLimiter(FakeInstnace)},

        %% client = other bucket = undefined
        {MkC(100), IsTokenLimiter(InfinityRef)},

        %% client = other bucket = infinity
        {MkC(100), IsTokenLimiter(InfinityRef)},

        %% client = C bucket = B C < B
        {MkA(100, 1000), IsTokenLimiter(FakeInstnace)},

        %% client = C bucket = B C > B
        {MkA(1000, 100), IsRefLimiter(FakeInstnace)}
    ].

parse_schema(ConfigString) ->
    {ok, RawConf} = hocon:binary(ConfigString, #{format => map}),
    hocon_tconf:check_plain(
        emqx_limiter_schema,
        RawConf,
        #{required => false, atom_key => false}
    ).

default_client_config() ->
    Conf = emqx_limiter_utils:default_client_config(),
    Conf#{divisible := false, max_retry_time := timer:seconds(10)}.
