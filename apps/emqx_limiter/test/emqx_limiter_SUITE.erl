%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_limiter_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-define(APP, emqx_limiter).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(BASE_CONF, <<"""
emqx_limiter {
  bytes_in {global = \"100KB/10s\"
            zone.default = \"100kB/10s\"
            zone.external = \"20kB/10s\"
            bucket.tcp {zone = default
                        aggregated = \"100kB/10s,1Mb\"
                        per_client = \"100KB/10s,10Kb\"}
            bucket.ssl {zone = external
                        aggregated = \"100kB/10s,1Mb\"
                        per_client = \"100KB/10s,10Kb\"}
           }

  message_in {global = \"100/10s\"
              zone.default = \"100/10s\"
              bucket.bucket1 {zone = default
                              aggregated = \"100/10s,1000\"
                              per_client = \"100/10s,100\"}
             }

  connection {global = \"100/10s\"
              zone.default = \"100/10s\"
              bucket.bucket1 {zone = default
                              aggregated = \"100/10s,100\"
                              per_client = \"100/10s,10\"
                             }
             }

  message_routing {global = \"100/10s\"
                    zone.default = \"100/10s\"
                    bucket.bucket1 {zone = default
                                    aggregated = \"100/10s,100\"
                                    per_client = \"100/10s,10\"
                                   }
                  }
}""">>).

-define(LOGT(Format, Args), ct:pal("TEST_SUITE: " ++ Format, Args)).

-record(client_options, { interval :: non_neg_integer()
                        , per_cost :: non_neg_integer()
                        , type :: atom()
                        , bucket :: atom()
                        , lifetime :: non_neg_integer()
                        , rates :: list(tuple())
                        }).

-record(client_state, { client :: emqx_limiter_client:limiter()
                      , pid :: pid()
                      , got :: non_neg_integer()
                      , options :: #client_options{}}).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------
all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    ok = emqx_config:init_load(emqx_limiter_schema, ?BASE_CONF),
    emqx_ct_helpers:start_apps([?APP]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([?APP]).

init_per_testcase(_TestCase, Config) ->
    Config.

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------
t_un_overload(_) ->
    Conf = emqx:get_config([emqx_limiter]),
    Conn = #{global => to_rate("infinity"),
             zone => #{z1 => to_rate("1000/1s"),
                       z2 => to_rate("1000/1s")},
             bucket => #{b1 => #{zone => z1,
                                 aggregated => to_bucket_rate("100/1s, 500"),
                                 per_client => to_bucket_rate("10/1s, 50")},
                         b2 => #{zone => z2,
                                 aggregated => to_bucket_rate("500/1s, 500"),
                                 per_client => to_bucket_rate("100/1s, infinity")
                                }}},
    Conf2 = Conf#{connection => Conn},
    emqx_config:put([emqx_limiter], Conf2),
    {ok, _} = emqx_limiter_manager:restart_server(connection),

    timer:sleep(200),

    B1C = #client_options{interval = 100,
                          per_cost = 1,
                          type = connection,
                          bucket = b1,
                          lifetime = timer:seconds(3),
                          rates = [{fun erlang:'=<'/2, ["1000/1s", "100/1s"]},
                                   {fun erlang:'=:='/2, ["10/1s"]}]},

    B2C = #client_options{interval = 100,
                          per_cost = 10,
                          type = connection,
                          bucket = b2,
                          lifetime = timer:seconds(3),
                          rates = [{fun erlang:'=<'/2, ["1000/1s", "500/1s"]},
                                   {fun erlang:'=:='/2, ["100/1s"]}]},

    lists:foreach(fun(_) -> start_client(B1C) end,
                  lists:seq(1, 10)),


    lists:foreach(fun(_) -> start_client(B2C) end,
                  lists:seq(1, 5)),

    ?assert(check_client_result(10 + 5)).

t_infinity(_) ->
    Conf = emqx:get_config([emqx_limiter]),
    Conn = #{global => to_rate("infinity"),
             zone => #{z1 => to_rate("1000/1s"),
                       z2 => to_rate("infinity")},
             bucket => #{b1 => #{zone => z1,
                                 aggregated => to_bucket_rate("100/1s, infinity"),
                                 per_client => to_bucket_rate("10/1s, 100")},
                         b2 => #{zone => z2,
                                 aggregated => to_bucket_rate("infinity, 600"),
                                 per_client => to_bucket_rate("100/1s, infinity")
                                }}},
    Conf2 = Conf#{connection => Conn},
    emqx_config:put([emqx_limiter], Conf2),
    {ok, _} = emqx_limiter_manager:restart_server(connection),

    timer:sleep(200),

    B1C = #client_options{interval = 100,
                          per_cost = 1,
                          type = connection,
                          bucket = b1,
                          lifetime = timer:seconds(3),
                          rates = [{fun erlang:'=<'/2, ["1000/1s", "100/1s"]},
                                   {fun erlang:'=:='/2, ["10/1s"]}]},

    B2C = #client_options{interval = 100,
                          per_cost = 10,
                          type = connection,
                          bucket = b2,
                          lifetime = timer:seconds(3),
                          rates = [{fun erlang:'=:='/2, ["100/1s"]}]},

    lists:foreach(fun(_) -> start_client(B1C) end,
                  lists:seq(1, 8)),

    lists:foreach(fun(_) -> start_client(B2C) end,
                  lists:seq(1, 4)),

    ?assert(check_client_result(8 + 4)).

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
start_client(Opts) ->
    Pid = self(),
    erlang:spawn(fun() -> enter_client(Opts, Pid) end).

enter_client(#client_options{type = Type,
                             bucket = Bucket,
                             lifetime = Lifetime} = Opts,
             Pid) ->
    erlang:send_after(Lifetime, self(), stop),
    erlang:send(self(), consume),
    Client = emqx_limiter_server:connect(Type, Bucket),
    client_loop(#client_state{client = Client,
                              pid = Pid,
                              got = 0,
                              options = Opts}).

client_loop(#client_state{client = Client,
                          got = Got,
                          pid = Pid,
                          options = #client_options{interval = Interval,
                                                    per_cost = PerCost,
                                                    lifetime = Lifetime,
                                                    rates = Rates}} = State) ->
    receive
        consume ->
            case emqx_limiter_client:consume(PerCost, Client) of
                {ok, Client2} ->
                    erlang:send_after(Interval, self(), consume),
                    client_loop(State#client_state{client = Client2,
                                                   got = Got + PerCost});
                {pause, MS, Client2} ->
                    erlang:send_after(MS, self(), {resume, erlang:system_time(millisecond)}),
                    client_loop(State#client_state{client = Client2})
            end;
        stop ->
            Rate = Got * emqx_limiter_schema:minimum_period() / Lifetime,
            ?LOGT("Got:~p, Rate is:~p Checks:~p~n", [Got, Rate, Rate]),
            Check = check_rates(Rate, Rates),
            erlang:send(Pid, {client, Check});
        {resume, Begin} ->
            case emqx_limiter_client:consume(PerCost, Client) of
                {ok, Client2} ->
                    Now = erlang:system_time(millisecond),
                    Diff = erlang:max(0, Interval - (Now - Begin)),
                    erlang:send_after(Diff, self(), consume),
                    client_loop(State#client_state{client = Client2,
                                                   got = Got + PerCost});
                {pause, MS, Client2} ->
                    erlang:send_after(MS, self(), {resume, Begin}),
                    client_loop(State#client_state{client = Client2})
            end
    end.

check_rates(Rate, [{Fun, Rates} | T]) ->
    case lists:all(fun(E) -> Fun(Rate, to_rate(E)) end, Rates) of
        true ->
            check_rates(Rate, T);
        false ->
            false
    end;
check_rates(_, _) ->
    true.

check_client_result(0) ->
    true;

check_client_result(N) ->
    ?LOGT("check_client_result:~p~n", [N]),
    receive
        {client, true} ->
            check_client_result(N - 1);
        {client, false} ->
            false;
        Any ->
            ?LOGT(">>>> other:~p~n", [Any])

    after 3500 ->
            ?LOGT(">>>> timeout~n", []),
            false
    end.

to_rate(Str) ->
    {ok, Rate} = emqx_limiter_schema:to_rate(Str),
    Rate.

to_bucket_rate(Str) ->
    {ok, Result} = emqx_limiter_schema:to_bucket_rate(Str),
    Result.
