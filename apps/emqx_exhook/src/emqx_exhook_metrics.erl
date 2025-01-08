%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_exhook_metrics).

-include("emqx_exhook.hrl").

%% API
-export([
    init/0,
    succeed/2,
    failed/2,
    update/1,
    new_metrics_info/0,
    servers_metrics/0,
    on_server_deleted/1,
    server_metrics/1,
    hooks_metrics/1,
    metrics_aggregate/1,
    metrics_aggregate_by_key/2,
    metrics_aggregate_by/2
]).

-record(metrics, {
    index :: index(),
    succeed = 0 :: non_neg_integer(),
    failed = 0 :: non_neg_integer(),
    rate = 0 :: non_neg_integer(),
    max_rate = 0 :: non_neg_integer(),
    window_rate :: integer()
}).

-type metrics() :: #metrics{}.
-type server_name() :: emqx_exhook_mgr:server_name().
-type hookpoint() :: emqx_exhook_server:hookpoint().
-type index() :: {server_name(), hookpoint()}.
-type hooks_metrics() :: #{hookpoint() => metrics_info()}.
-type servers_metrics() :: #{server_name() => metrics_info()}.

-type metrics_info() :: #{
    succeed := non_neg_integer(),
    failed := non_neg_integer(),
    rate := number(),
    max_rate := number()
}.

-define(INDEX(ServerName, HookPoint), {ServerName, HookPoint}).
-export_type([metrics_info/0, servers_metrics/0, hooks_metrics/0]).

%%--------------------------------------------------------------------
%%% API
%%--------------------------------------------------------------------
init() ->
    _ = ets:new(
        ?HOOKS_METRICS,
        [
            set,
            named_table,
            public,
            {keypos, #metrics.index},
            {write_concurrency, true},
            {read_concurrency, true}
        ]
    ),
    ok.

-spec new_metric_info() -> metrics_info().
new_metric_info() ->
    #{
        succeed => 0,
        failed => 0,
        rate => 0,
        max_rate => 0
    }.

-spec succeed(server_name(), hookpoint()) -> ok.
succeed(Server, Hook) ->
    inc(
        Server,
        Hook,
        #metrics.succeed,
        #metrics{
            index = {Server, Hook},
            window_rate = 0,
            succeed = 0
        }
    ).

-spec failed(server_name(), hookpoint()) -> ok.
failed(Server, Hook) ->
    inc(
        Server,
        Hook,
        #metrics.failed,
        #metrics{
            index = {Server, Hook},
            window_rate = 0,
            failed = 0
        }
    ).

-spec update(pos_integer()) -> true.
update(Interval) ->
    Fun = fun(
        #metrics{
            rate = Rate,
            window_rate = WindowRate,
            max_rate = MaxRate
        } = Metrics,
        _
    ) ->
        case calc_metric(WindowRate, Interval) of
            Rate ->
                true;
            NewRate ->
                MaxRate2 = erlang:max(MaxRate, NewRate),
                Metrics2 = Metrics#metrics{
                    rate = NewRate,
                    window_rate = 0,
                    max_rate = MaxRate2
                },
                ets:insert(?HOOKS_METRICS, Metrics2)
        end
    end,

    ets:foldl(Fun, true, ?HOOKS_METRICS).

-spec on_server_deleted(server_name()) -> true.
on_server_deleted(Name) ->
    ets:match_delete(
        ?HOOKS_METRICS,
        {metrics, {Name, '_'}, '_', '_', '_', '_', '_'}
    ).

-spec server_metrics(server_name()) -> metrics_info().
server_metrics(SvrName) ->
    Hooks = ets:match_object(
        ?HOOKS_METRICS,
        {metrics, {SvrName, '_'}, '_', '_', '_', '_', '_'}
    ),

    Fold = fun(
        #metrics{
            succeed = Succeed,
            failed = Failed,
            rate = Rate,
            max_rate = MaxRate
        },
        Acc
    ) ->
        [
            #{
                succeed => Succeed,
                failed => Failed,
                rate => Rate,
                max_rate => MaxRate
            }
            | Acc
        ]
    end,

    AllMetrics = lists:foldl(Fold, [], Hooks),
    metrics_aggregate(AllMetrics).

-spec servers_metrics() -> servers_metrics().
servers_metrics() ->
    AllMetrics = ets:tab2list(?HOOKS_METRICS),

    GroupFun = fun(
        #metrics{
            index = ?INDEX(ServerName, _),
            succeed = Succeed,
            failed = Failed,
            rate = Rate,
            max_rate = MaxRate
        },
        Acc
    ) ->
        SvrGroup = maps:get(ServerName, Acc, []),
        Metrics = #{
            succeed => Succeed,
            failed => Failed,
            rate => Rate,
            max_rate => MaxRate
        },
        Acc#{ServerName => [Metrics | SvrGroup]}
    end,

    GroupBySever = lists:foldl(GroupFun, #{}, AllMetrics),

    MapFun = fun(_SvrName, Group) -> metrics_aggregate(Group) end,
    maps:map(MapFun, GroupBySever).

-spec hooks_metrics(server_name()) -> hooks_metrics().
hooks_metrics(SvrName) ->
    Hooks = ets:match_object(
        ?HOOKS_METRICS,
        {metrics, {SvrName, '_'}, '_', '_', '_', '_', '_'}
    ),

    Fold = fun(
        #metrics{
            index = ?INDEX(_, HookPoint),
            succeed = Succeed,
            failed = Failed,
            rate = Rate,
            max_rate = MaxRate
        },
        Acc
    ) ->
        Acc#{
            HookPoint => #{
                succeed => Succeed,
                failed => Failed,
                rate => Rate,
                max_rate => MaxRate
            }
        }
    end,

    lists:foldl(Fold, #{}, Hooks).

-spec metrics_aggregate(list(metrics_info())) -> metrics_info().
metrics_aggregate(MetricsL) ->
    metrics_aggregate_by(fun(X) -> X end, MetricsL).

-spec metrics_aggregate_by_key(Key, list(HasMetrics)) -> metrics_info() when
    Key :: any(),
    HasMetrics :: #{Key => metrics_info()}.
metrics_aggregate_by_key(Key, MetricsL) ->
    metrics_aggregate_by(
        fun(X) -> maps:get(Key, X, new_metrics_info()) end,
        MetricsL
    ).

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
-spec inc(server_name(), hookpoint(), pos_integer(), metrics()) -> ok.
inc(Server, Hook, Pos, Default) ->
    Index = {Server, Hook},
    _ = ets:update_counter(
        ?HOOKS_METRICS,
        Index,
        [{#metrics.window_rate, 1}, {Pos, 1}],
        Default
    ),
    ok.

-spec new_metrics_info() -> metrics_info().
new_metrics_info() ->
    #{
        succeed => 0,
        failed => 0,
        rate => 0,
        max_rate => 0
    }.

-spec calc_metric(non_neg_integer(), non_neg_integer()) -> non_neg_integer().
calc_metric(Val, Interval) ->
    %% the base unit of interval is milliseconds, but the rate is seconds
    erlang:ceil(Val * 1000 / Interval).

-spec metrics_add(metrics_info(), metrics_info()) -> metrics_info().
metrics_add(
    #{succeed := S1, failed := F1, rate := R1, max_rate := M1},
    #{succeed := S2, failed := F2, rate := R2, max_rate := M2} = Acc
) ->
    Acc#{
        succeed := S1 + S2,
        failed := F1 + F2,
        rate := R1 + R2,
        max_rate := M1 + M2
    }.

-spec metrics_aggregate_by(fun((X) -> metrics_info()), list(X)) -> metrics_info() when
    X :: any().
metrics_aggregate_by(_, []) ->
    new_metric_info();
metrics_aggregate_by(Fun, MetricsL) ->
    Fold = fun(E, Acc) -> metrics_add(Fun(E), Acc) end,
    #{
        rate := Rate,
        max_rate := MaxRate
    } = Result = lists:foldl(Fold, new_metric_info(), MetricsL),

    Len = erlang:length(MetricsL),

    Result#{
        rate := Rate div Len,
        max_rate := MaxRate div Len
    }.
