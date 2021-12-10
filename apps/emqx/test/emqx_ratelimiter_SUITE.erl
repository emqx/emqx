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

-module(emqx_ratelimiter_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-define(APP, emqx).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(BASE_CONF, <<"""
emqx_limiter {
  bytes_in {
    global.rate = infinity
    zone.default.rate = infinity
    bucket.default {
      zone = default
      aggregated.rate = infinity
      aggregated.capacity = infinity
      per_client.rate = \"100MB/1s\"
      per_client.capacity = infinity
    }
  }

  message_in {
    global.rate = infinity
    zone.default.rate = infinity
    bucket.default {
      zone = default
      aggregated.rate = infinity
      aggregated.capacity = infinity
      per_client.rate = infinity
      per_client.capacity = infinity
    }
  }

  connection {
    global.rate = infinity
    zone.default.rate = infinity
    bucket.default {
      zone = default
      aggregated.rate = infinity
      aggregated.capacity = infinity
      per_client.rate = infinity
      per_client.capacity = infinity
                   }
  }

  message_routing {
    global.rate = infinity
    zone.default.rate = infinity
    bucket.default {
      zone = default
      aggregated.rate = infinity
      aggregated.capacity = infinity
      per_client.rate = infinity
      per_client.capacity = infinity
    }
  }
}

""">>).

-record(client, { counter :: counters:counter_ref()
                , start :: pos_integer()
                , endtime :: pos_integer()
                , obtained :: pos_integer()
                , rate :: float()
                , client :: emqx_htb_limiter:client()
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
    ok = emqx_config:init_load(emqx_limiter_schema, ?BASE_CONF),
    emqx_common_test_helpers:start_apps([?APP]),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([?APP]).

init_per_testcase(_TestCase, Config) ->
    Config.

base_conf() ->
    emqx_config:init_load(emqx_limiter_schema, ?BASE_CONF).

%%--------------------------------------------------------------------
%% Test Cases Bucket Level
%%--------------------------------------------------------------------
t_max_retry_time(_) ->
    Cfg = fun(Cfg) ->
              Cfg#{rate := 1,
                   capacity := 1,
                   max_retry_time := 500,
                   failure_strategy := drop}
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
                  Cfg#{divisible := true,
                       rate := ?RATE("1000/1s"),
                       initial := 600,
                       capacity := 600}
          end,
    Case = fun() ->
                   Client = connect(default),
                   Result = emqx_htb_limiter:check(1000, Client),
                   ?assertMatch({partial,
                                 400,
                                 #{continuation := _,
                                   diff := 400,
                                   start := _,
                                   need := 1000},
                                 _}, Result)
           end,
    with_per_client(default, Cfg, Case).

t_low_water_mark(_) ->
    Cfg = fun(Cfg) ->
                  Cfg#{low_water_mark := 400,
                       rate := ?RATE("1000/1s"),
                       initial := 1000,
                       capacity := 1000}
          end,
    Case = fun() ->
                   Client = connect(default),
                   Result = emqx_htb_limiter:check(500, Client),
                   ?assertMatch({ok, _}, Result),
                   {_, Client2} = Result,
                   Result2 = emqx_htb_limiter:check(101, Client2),
                   ?assertMatch({pause,
                                 _,
                                 #{continuation := undefined,
                                   diff := 0},
                                 _}, Result2)
           end,
    with_per_client(default, Cfg, Case).

t_infinity_client(_) ->
    Fun = fun(#{aggregated := Aggr, per_client := Cli} = Bucket) ->
                  Aggr2 = Aggr#{rate := infinity,
                                capacity := infinity},
                  Cli2 = Cli#{rate := infinity, capacity := infinity},
                  Bucket#{aggregated := Aggr2,
                          per_client := Cli2}
          end,
    Case = fun() ->
                   Client = connect(default),
                   ?assertEqual(infinity, Client),
                   Result = emqx_htb_limiter:check(100000, Client),
                   ?assertEqual({ok, Client}, Result)
           end,
    with_bucket(default, Fun, Case).

t_short_board(_) ->
    Fun = fun(#{aggregated := Aggr, per_client := Cli} = Bucket) ->
                  Aggr2 = Aggr#{rate := ?RATE("100/1s"),
                                initial := 0,
                                capacity := 100},
                  Cli2 = Cli#{rate := ?RATE("600/1s"),
                              capacity := 600,
                              initial := 600},
                  Bucket#{aggregated := Aggr2,
                          per_client := Cli2}
          end,
    Case = fun() ->
                   Counter = counters:new(1, [write_concurrency]),
                   start_client(default, ?NOW + 2000, Counter, 20),
                   timer:sleep(2100),
                   check_average_rate(Counter, 2, 100, 20)
           end,
    with_bucket(default, Fun, Case).

t_rate(_) ->
    Fun = fun(#{aggregated := Aggr, per_client := Cli} = Bucket) ->
              Aggr2 = Aggr#{rate := ?RATE("100/100ms"),
                            initial := 0,
                            capacity := infinity},
              Cli2 = Cli#{rate := infinity,
                          capacity := infinity,
                          initial := 0},
              Bucket#{aggregated := Aggr2,
                      per_client := Cli2}
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
    Fun = fun(#{aggregated := Aggr, per_client := Cli} = Bucket) ->
              Aggr2 = Aggr#{rate := ?RATE("100/100ms"),
                            initial := 0,
                            capacity := 600},
              Cli2 = Cli#{rate := infinity,
                          capacity := infinity,
                          initial := 0},
              Bucket#{aggregated := Aggr2,
                      per_client := Cli2}
          end,
    Case = fun() ->
               Client = connect(default),
               timer:sleep(1000),
                   C1 = emqx_htb_limiter:available(Client),
                   ?assertEqual(Capacity, C1, "test bucket capacity")
           end,
    with_bucket(default, Fun, Case).

%%--------------------------------------------------------------------
%% Test Cases Zone Level
%%--------------------------------------------------------------------
t_limit_zone_with_unlimit_bucket(_) ->
    ZoneMod = fun(Cfg) ->
                  Cfg#{rate := ?RATE("600/1s"),
                       burst := ?RATE("60/1s")}
              end,

    Bucket = fun(#{aggregated := Aggr, per_client := Cli} = Bucket) ->
                 Aggr2 = Aggr#{rate := infinity,
                               initial := 0,
                               capacity := infinity},
                 Cli2 = Cli#{rate := infinity,
                             initial := 0,
                             capacity := infinity,
                             divisible := true},
                 Bucket#{aggregated := Aggr2, per_client := Cli2}
             end,

    Case = fun() ->
               C1 = counters:new(1, [write_concurrency]),
               start_client(b1, ?NOW + 2000, C1, 20),
               timer:sleep(2100),
               check_average_rate(C1, 2, 600, 1000)
           end,

    with_zone(default, ZoneMod, [{b1, Bucket}], Case).


%%--------------------------------------------------------------------
%% Test Cases Global Level
%%--------------------------------------------------------------------
t_burst_and_fairness(_) ->
    GlobalMod = fun(Cfg) ->
                    Cfg#{burst := ?RATE("60/1s")}
                                  end,

    ZoneMod = fun(Cfg) ->
                  Cfg#{rate := ?RATE("600/1s"),
                       burst := ?RATE("60/1s")}
              end,

    Bucket = fun(#{aggregated := Aggr, per_client := Cli} = Bucket) ->
                     Aggr2 = Aggr#{rate := ?RATE("500/1s"),
                                   initial := 0,
                                   capacity := 500},
                     Cli2 = Cli#{rate := ?RATE("600/1s"),
                                 capacity := 600,
                                 initial := 600},
                     Bucket#{aggregated := Aggr2,
                             per_client := Cli2}
             end,

    Case = fun() ->
                   C1 = counters:new(1, [write_concurrency]),
                   C2 = counters:new(1, [write_concurrency]),
                   start_client(b1, ?NOW + 2000, C1, 20),
                   start_client(b2, ?NOW + 2000, C2, 30),
                   timer:sleep(2100),
                   check_average_rate(C1, 2, 330, 25),
                   check_average_rate(C2, 2, 330, 25)
           end,

    with_global(GlobalMod,
                default,
                ZoneMod,
                [{b1, Bucket}, {b2, Bucket}],
                Case).

t_limit_global_with_unlimit_other(_) ->
    GlobalMod = fun(Cfg) ->
                    Cfg#{rate := ?RATE("600/1s")}
                end,

    ZoneMod = fun(Cfg) -> Cfg#{rate := infinity} end,

    Bucket = fun(#{aggregated := Aggr, per_client := Cli} = Bucket) ->
                 Aggr2 = Aggr#{rate := infinity,
                               initial := 0,
                               capacity := infinity},
                 Cli2 = Cli#{rate := infinity,
                             capacity := infinity,
                             initial := 0},
                 Bucket#{aggregated := Aggr2,
                         per_client := Cli2}
             end,

    Case = fun() ->
               C1 = counters:new(1, [write_concurrency]),
               start_client(b1, ?NOW + 2000, C1, 20),
               timer:sleep(2100),
               check_average_rate(C1, 2, 600, 100)
           end,

    with_global(GlobalMod,
                default,
                ZoneMod,
                [{b1, Bucket}],
                Case).

t_multi_zones(_) ->
    GlobalMod = fun(Cfg) ->
                    Cfg#{rate := ?RATE("600/1s")}
                end,

    Zone1 = fun(Cfg) ->
                Cfg#{rate := ?RATE("400/1s")}
            end,

    Zone2 = fun(Cfg) ->
                Cfg#{rate := ?RATE("500/1s")}
            end,

    Bucket = fun(Zone, Rate) ->
                 fun(#{aggregated := Aggr, per_client := Cli} = Bucket) ->
                     Aggr2 = Aggr#{rate := infinity,
                                   initial := 0,
                                   capacity := infinity},
                     Cli2 = Cli#{rate := Rate,
                                 capacity := infinity,
                                 initial := 0},
                     Bucket#{aggregated := Aggr2,
                             per_client := Cli2,
                             zone := Zone}
                 end
             end,

    Case = fun() ->
               C1 = counters:new(1, [write_concurrency]),
               C2 = counters:new(1, [write_concurrency]),
               start_client(b1, ?NOW + 2000, C1, 25),
               start_client(b2, ?NOW + 2000, C2, 20),
               timer:sleep(2100),
               check_average_rate(C1, 2, 300, 25),
               check_average_rate(C2, 2, 300, 25)
           end,

    with_global(GlobalMod,
                [z1, z2],
                [Zone1, Zone2],
                [{b1, Bucket(z1, ?RATE("400/1s"))}, {b2, Bucket(z2, ?RATE("500/1s"))}],
                Case).

%% because the simulated client will try to reach the maximum rate
%% when divisiable = true, a large number of divided tokens will be generated
%% so this is not an accurate test
t_multi_zones_with_divisible(_) ->
    GlobalMod = fun(Cfg) ->
                    Cfg#{rate := ?RATE("600/1s")}
                end,

    Zone1 = fun(Cfg) ->
                Cfg#{rate := ?RATE("400/1s")}
            end,

    Zone2 = fun(Cfg) ->
                Cfg#{rate := ?RATE("500/1s")}
            end,

    Bucket = fun(Zone, Rate) ->
                 fun(#{aggregated := Aggr, per_client := Cli} = Bucket) ->
                     Aggr2 = Aggr#{rate := Rate,
                                   initial := 0,
                                   capacity := infinity},
                     Cli2 = Cli#{rate := Rate,
                                 divisible := true,
                                 capacity := infinity,
                                 initial := 0},
                     Bucket#{aggregated := Aggr2,
                             per_client := Cli2,
                             zone := Zone}
                 end
             end,

    Case = fun() ->
               C1 = counters:new(1, [write_concurrency]),
               C2 = counters:new(1, [write_concurrency]),
               start_client(b1, ?NOW + 2000, C1, 25),
               start_client(b2, ?NOW + 2000, C2, 20),
               timer:sleep(2100),
               check_average_rate(C1, 2, 300, 120),
               check_average_rate(C2, 2, 300, 120)
           end,

    with_global(GlobalMod,
                [z1, z2],
                [Zone1, Zone2],
                [{b1, Bucket(z1, ?RATE("400/1s"))}, {b2, Bucket(z2, ?RATE("500/1s"))}],
                Case).

t_zone_hunger_and_fair(_) ->
    GlobalMod = fun(Cfg) ->
                    Cfg#{rate := ?RATE("600/1s")}
                end,

    Zone1 = fun(Cfg) ->
                Cfg#{rate := ?RATE("600/1s")}
            end,

    Zone2 = fun(Cfg) ->
                Cfg#{rate := ?RATE("50/1s")}
            end,

    Bucket = fun(Zone, Rate) ->
                 fun(#{aggregated := Aggr, per_client := Cli} = Bucket) ->
                     Aggr2 = Aggr#{rate := infinity,
                                   initial := 0,
                                   capacity := infinity},
                     Cli2 = Cli#{rate := Rate,
                                 capacity := infinity,
                                 initial := 0},
                     Bucket#{aggregated := Aggr2,
                             per_client := Cli2,
                             zone := Zone}
                 end
             end,

    Case = fun() ->
               C1 = counters:new(1, [write_concurrency]),
               C2 = counters:new(1, [write_concurrency]),
               start_client(b1, ?NOW + 2000, C1, 20),
               start_client(b2, ?NOW + 2000, C2, 20),
               timer:sleep(2100),
               check_average_rate(C1, 2, 550, 25),
               check_average_rate(C2, 2, 50, 25)
           end,

    with_global(GlobalMod,
                [z1, z2],
                [Zone1, Zone2],
                [{b1, Bucket(z1, ?RATE("600/1s"))}, {b2, Bucket(z2, ?RATE("50/1s"))}],
                Case).

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
start_client(Name, EndTime, Counter, Number) ->
    lists:foreach(fun(_) ->
                      spawn(fun() ->
                                start_client(Name, EndTime, Counter)
                            end)
                  end,
                  lists:seq(1, Number)).

start_client(Name, EndTime, Counter) ->
    #{per_client := PerClient} =
        emqx_config:get([emqx_limiter, message_routing, bucket, Name]),
    #{rate := Rate} = PerClient,
    Client = #client{start = ?NOW,
                     endtime = EndTime,
                     counter = Counter,
                     obtained = 0,
                     rate = Rate,
                     client = connect(Name)
                    },
    client_loop(Client).

%% the simulated client will try to reach the configured rate as much as possible
%% note this client will not considered the capacity, so must make sure rate < capacity
client_loop(#client{start = Start,
                    endtime = EndTime,
                    obtained = Obtained,
                    rate = Rate} = State) ->
    Now = ?NOW,
    Period = emqx_limiter_schema:minimum_period(),
    MinPeriod = erlang:ceil(0.25 * Period),
    if Now >= EndTime ->
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
            if CurrRate < Rate ->
                    Rand = client_random_val(Rate),
                    client_try_check(Rand, State);
               true ->
                    LeftTime = EndTime - Now,
                    CanSleep = erlang:min(LeftTime, client_random_val(MinPeriod div 2)),
                    timer:sleep(CanSleep),
                    client_loop(State)
            end
    end.

client_try_check(Need, #client{counter = Counter,
                               endtime = EndTime,
                               obtained = Obtained,
                               client = Client} = State) ->
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
            if LeftTime =< 0 ->
                    stop;
               true ->
                    timer:sleep(erlang:min(Pause, LeftTime)),
                    client_try_check(Retry, State#client{client = Client2})
            end
    end.


%% XXX not a god test, because client's rate maybe bigger than global rate
%% so if client' rate = infinity
%% client's divisible should be true or capacity must be bigger than number of each comsume
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

with_global(Modifier, ZoneName, ZoneModifier, Buckets, Case) ->
    Path = [emqx_limiter, message_routing],
    #{global := Global} = Cfg = emqx_config:get(Path),
    Cfg2 = Cfg#{global := Modifier(Global)},
    with_zone(Cfg2, ZoneName, ZoneModifier, Buckets, Case).

with_zone(Name, Modifier, Buckets, Case) ->
    Path = [emqx_limiter, message_routing],
    Cfg = emqx_config:get(Path),
    with_zone(Cfg, Name, Modifier, Buckets, Case).

with_zone(Cfg, Name, Modifier, Buckets, Case) ->
    Path = [emqx_limiter, message_routing],
    #{zone := ZoneCfgs,
      bucket := BucketCfgs} = Cfg,
    ZoneCfgs2 = apply_modifier(Name, Modifier, ZoneCfgs),
    BucketCfgs2 = apply_modifier(Buckets, BucketCfgs),
    Cfg2 = Cfg#{zone := ZoneCfgs2, bucket := BucketCfgs2},
    with_config(Path, fun(_) -> Cfg2 end, Case).

with_bucket(Bucket, Modifier, Case) ->
    Path = [emqx_limiter, message_routing, bucket, Bucket],
    with_config(Path, Modifier, Case).

with_per_client(Bucket, Modifier, Case) ->
    Path = [emqx_limiter, message_routing, bucket, Bucket, per_client],
    with_config(Path, Modifier, Case).

with_config(Path, Modifier, Case) ->
    Cfg = emqx_config:get(Path),
    NewCfg = Modifier(Cfg),
    ct:pal("test with config:~p~n", [NewCfg]),
    emqx_config:put(Path, NewCfg),
    emqx_limiter_manager:restart_server(message_routing),
    timer:sleep(100),
    DelayReturn
        = try
              Return = Case(),
              fun() -> Return end
          catch Type:Reason:Trace ->
                  fun() -> erlang:raise(Type, Reason, Trace) end
          end,
    emqx_config:put(Path, Cfg),
    DelayReturn().

connect(Name) ->
    emqx_limiter_server:connect(message_routing, Name).

check_average_rate(Counter, Second, Rate, Margin) ->
    Cost = counters:get(Counter, 1),
    PerSec = Cost / Second,
    ?LOGT(">>>> Cost:~p PerSec:~p Rate:~p ~n", [Cost, PerSec, Rate]),
    ?assert(in_range(PerSec, Rate - Margin, Rate + Margin)).

print_average_rate(Counter, Second) ->
    Cost = counters:get(Counter, 1),
    PerSec = Cost / Second,
    ct:pal(">>>> Cost:~p PerSec:~p ~n", [Cost, PerSec]).

in_range(Val, Min, _Max) when Val < Min ->
    ct:pal("Val:~p smaller than min bound:~p~n", [Val, Min]),
    false;
in_range(Val, _Min, Max) when Val > Max->
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
