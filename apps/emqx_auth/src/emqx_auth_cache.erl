%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_auth_cache).

-behaviour(gen_server).

-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-export([
    start_link/3,
    child_spec/3,
    with_cache/3,
    reset/1,
    metrics/1
]).

-export([
    reset_v1/1,
    metrics_v1/1
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-record(cache_record, {
    key :: term(),
    value :: term(),
    expire_at :: integer() | '_'
}).

-define(stat_key, stats).
-record(stats, {
    key :: ?stat_key,
    count :: non_neg_integer(),
    memory :: non_neg_integer()
}).

-define(pt_key(NAME), {?MODULE, NAME}).
-define(unlimited, unlimited).

-define(DEFAULT_STAT_UPDATE_INTERVAL, 5000).
-define(DEFAULT_CLEANUP_INTERVAL, 30000).

%%--------------------------------------------------------------------
%% Metrics
%%--------------------------------------------------------------------

-define(metric_hit, hits).
-define(metric_miss, misses).
-define(metric_insert, inserts).
-define(metric_count, count).
-define(metric_memory, memory).

-define(metric_counters, [?metric_hit, ?metric_miss, ?metric_insert]).
-define(metric_gauges, [?metric_count, ?metric_memory]).

%% For gauges we use only one "virtual" worker
-define(worker_id, worker_id).

%%--------------------------------------------------------------------
%% Types
%%--------------------------------------------------------------------

-type cache_key() :: term() | fun(() -> term()).
-type name() :: atom().
-type config_path() :: emqx_config:runtime_config_key_path().
-type callback() :: fun(() -> {cache | nocache, term()}).

-type metrics_worker() :: emqx_metrics_worker:handler_name().

-export_type([
    name/0,
    cache_key/0
]).

%%--------------------------------------------------------------------
%% Messages
%%--------------------------------------------------------------------

-record(cleanup, {}).
-record(update_stats, {}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec start_link(name(), config_path(), metrics_worker()) -> {ok, pid()}.
start_link(Name, ConfigPath, MetricsWorker) ->
    gen_server:start_link(?MODULE, [Name, ConfigPath, MetricsWorker], []).

-spec child_spec(name(), config_path(), metrics_worker()) -> supervisor:child_spec().
child_spec(Name, ConfigPath, MetricsWorker) ->
    #{
        id => {?MODULE, Name},
        start => {?MODULE, start_link, [Name, ConfigPath, MetricsWorker]},
        restart => permanent,
        shutdown => 5000,
        type => worker
    }.

-spec with_cache(name(), cache_key(), callback()) -> term().
with_cache(Name, Key, Fun) ->
    case is_cache_enabled(Name) of
        false ->
            with_cache_disabled(Fun);
        {true, PtState} ->
            with_cache_enabled(PtState, Key, Fun)
    end.

-spec reset(name()) -> ok.
reset(Name) ->
    try
        #{tab := Tab, metrics_worker := MetricsWorker} = persistent_term:get(?pt_key(Name)),
        ets:delete_all_objects(Tab),
        ok = emqx_metrics_worker:reset_metrics(MetricsWorker, Name),
        ?tp(info, auth_cache_reset, #{name => Name, status => ok}),
        ok
    catch
        error:badarg ->
            ?tp(warning, auth_cache_reset, #{name => Name, status => not_found}),
            ok
    end.

-spec metrics(name()) -> map() | no_return().
metrics(Name) ->
    try persistent_term:get(?pt_key(Name)) of
        #{metrics_worker := MetricsWorker} ->
            RawMetrics = emqx_metrics_worker:get_metrics(MetricsWorker, Name),
            Metrics0 = fold_counters(RawMetrics, ?metric_counters),
            Metrics1 = fold_gauges(RawMetrics, ?metric_gauges),
            maps:merge(Metrics0, Metrics1)
    catch
        error:badarg ->
            error({cache_not_found, Name})
    end.

%%--------------------------------------------------------------------
%% RPC Targets
%%--------------------------------------------------------------------

-spec reset_v1(name()) -> ok.
reset_v1(Name) ->
    reset(Name).

-spec metrics_v1(name()) -> {node(), map()}.
metrics_v1(Name) ->
    {node(), metrics(Name)}.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Name, ConfigPath, MetricsWorker]) ->
    Tab = ets:new(emqx_node_cache, [
        public,
        ordered_set,
        {keypos, #cache_record.key},
        {read_concurrency, true},
        {write_concurrency, true}
    ]),
    StatTab = ets:new(emqx_node_cache_tab, [
        public, set, {keypos, #stats.key}, {read_concurrency, true}
    ]),
    ok = create_metrics(Name, MetricsWorker),
    PtState = #{
        name => Name,
        tab => Tab,
        stat_tab => StatTab,
        config_path => ConfigPath,
        metrics_worker => MetricsWorker
    },
    ok = update_stats(PtState),
    _ = persistent_term:put(?pt_key(Name), PtState),
    _ = erlang:send_after(cleanup_interval(PtState), self(), #cleanup{}),
    _ = erlang:send_after(stat_update_interval(PtState), self(), #update_stats{}),
    {ok, #{name => Name}}.

handle_call(Msg, _From, State) ->
    ?tp(warning, auth_cache_unkown_call, #{
        msg => Msg
    }),
    {reply, ok, State}.

handle_cast(Msg, State) ->
    ?tp(warning, auth_cache_unkown_cast, #{
        msg => Msg
    }),
    {noreply, State}.

handle_info(#cleanup{}, State) ->
    PtState = pt_state(State),
    ok = cleanup(PtState),
    erlang:send_after(cleanup_interval(PtState), self(), #cleanup{}),
    {noreply, State};
handle_info(#update_stats{}, State) ->
    PtState = pt_state(State),
    ok = update_stats(PtState),
    erlang:send_after(stat_update_interval(PtState), self(), #update_stats{}),
    {noreply, State};
handle_info(Msg, State) ->
    ?tp(warning, auth_cache_unkown_info, #{
        msg => Msg
    }),
    {noreply, State}.

terminate(_Reason, #{name := Name}) ->
    _ = persistent_term:erase(?pt_key(Name)),
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

is_cache_enabled(Name) ->
    try persistent_term:get(?pt_key(Name)) of
        #{config_path := ConfigPath} = PtState ->
            case config_value(ConfigPath, enable) of
                true -> {true, PtState};
                false -> false
            end
    catch
        error:badarg -> false
    end.

with_cache_disabled(Fun) ->
    dont_cache(Fun()).

create_metrics(Name, MetricsWorker) ->
    ok = emqx_metrics_worker:create_metrics(
        MetricsWorker, Name, ?metric_counters, ?metric_counters
    ).

with_cache_enabled(#{tab := Tab} = PtState, KeyOrFun, Fun) ->
    Key = evaluate_key(KeyOrFun),
    case lookup(Tab, Key) of
        {ok, Value} ->
            ok = inc_metric(PtState, ?metric_hit),
            Value;
        not_found ->
            ok = inc_metric(PtState, ?metric_miss),
            maybe_cache(PtState, Key, Fun());
        error ->
            dont_cache(Fun())
    end.

evaluate_key(Key) when is_function(Key) ->
    Key();
evaluate_key(Key) ->
    Key.

inc_metric(#{name := Name, metrics_worker := MetricsWorker}, Metric) ->
    ok = emqx_metrics_worker:inc(MetricsWorker, Name, Metric).

set_gauge(#{name := Name, metrics_worker := MetricsWorker}, Metric, Value) ->
    ok = emqx_metrics_worker:set_gauge(MetricsWorker, Name, ?worker_id, Metric, Value).

cleanup(#{name := Name, tab := Tab}) ->
    Now = now_ms_monotonic(),
    MS = ets:fun2ms(fun(#cache_record{expire_at = ExpireAt}) when ExpireAt < Now -> true end),
    NumDeleted = ets:select_delete(Tab, MS),
    ?tp(info, auth_cache_cleanup, #{
        name => Name,
        num_deleted => NumDeleted
    }),
    ok.

update_stats(#{tab := Tab, stat_tab := StatTab, name := Name} = PtState) ->
    #{count := Count, memory := Memory} = tab_stats(Tab),
    Stats = #stats{
        key = ?stat_key,
        count = Count,
        memory = Memory
    },
    ok = set_gauge(PtState, ?metric_count, Count),
    ok = set_gauge(PtState, ?metric_memory, Memory),
    ?tp(info, auth_cache_update_stats, #{
        name => Name,
        stats => Stats
    }),
    _ = ets:insert(StatTab, Stats),
    ok.

deadline(ConfigPath) ->
    now_ms_monotonic() + config_value(ConfigPath, cache_ttl).

cleanup_interval(#{config_path := ConfigPath}) ->
    config_value(ConfigPath, cleanup_interval, ?DEFAULT_CLEANUP_INTERVAL).

stat_update_interval(#{config_path := ConfigPath}) ->
    config_value(ConfigPath, stat_update_interval, ?DEFAULT_STAT_UPDATE_INTERVAL).

now_ms_monotonic() ->
    erlang:monotonic_time(millisecond).

config_value(ConfigPath, Key) ->
    maps:get(Key, emqx_config:get(ConfigPath)).

config_value(ConfigPath, Key, Default) ->
    maps:get(Key, emqx_config:get(ConfigPath), Default).

lookup(Tab, Key) ->
    Now = now_ms_monotonic(),
    try ets:lookup(Tab, Key) of
        [#cache_record{value = Value, expire_at = ExpireAt}] when ExpireAt > Now ->
            {ok, Value};
        _ ->
            not_found
    catch
        error:badarg -> error
    end.

maybe_cache(PtState, Key, {cache, Value}) ->
    ok = maybe_insert(PtState, Key, Value),
    Value;
maybe_cache(_PtState, _Key, {nocache, Value}) ->
    Value.

dont_cache({nocache, Value}) -> Value;
dont_cache({cache, Value}) -> Value.

tab_stats(Tab) ->
    try
        Memory = ets:info(Tab, memory) * erlang:system_info(wordsize),
        Count = ets:info(Tab, size),
        #{count => Count, memory => Memory}
    catch
        error:badarg -> not_found
    end.

maybe_insert(#{tab := Tab, stat_tab := StatTab, config_path := ConfigPath} = PtState, Key, Value) ->
    LimitsReached = limits_reached(ConfigPath, StatTab),
    ?tp(auth_cache_insert, #{
        key => Key,
        value => Value,
        limits_reached => LimitsReached
    }),
    case LimitsReached of
        true ->
            ok;
        false ->
            ok = inc_metric(PtState, ?metric_insert),
            insert(Tab, Key, Value, ConfigPath)
    end.

insert(Tab, Key, Value, ConfigPath) ->
    Record = #cache_record{
        key = Key,
        value = Value,
        expire_at = deadline(ConfigPath)
    },
    try ets:insert(Tab, Record) of
        true -> ok
    catch
        error:badarg -> ok
    end.

limits_reached(ConfigPath, StatTab) ->
    MaxCount = config_value(ConfigPath, max_count, ?unlimited),
    MaxMemory = config_value(ConfigPath, max_memory, ?unlimited),
    [#stats{count = Count, memory = Memory}] = ets:lookup(StatTab, ?stat_key),
    ?tp(auth_cache_limits, #{
        count => Count,
        memory => Memory,
        max_count => MaxCount,
        max_memory => MaxMemory
    }),
    case {MaxCount, MaxMemory} of
        {MaxCount, _} when is_integer(MaxCount) andalso Count >= MaxCount -> true;
        {_, MaxMemory} when is_integer(MaxMemory) andalso Memory >= MaxMemory -> true;
        _ -> false
    end.

pt_state(#{name := Name} = _State) ->
    persistent_term:get(?pt_key(Name)).

%%--------------------------------------------------------------------
%% Metric helpers
%%--------------------------------------------------------------------

fold_counters(RawMetrics, Counters) ->
    lists:foldl(
        fun(Counter, Acc) ->
            case RawMetrics of
                #{counters := #{Counter := Value}, rate := #{Counter := Rate}} ->
                    Acc#{Counter => #{value => Value, rate => Rate}};
                _ ->
                    Acc
            end
        end,
        #{},
        Counters
    ).

fold_gauges(RawMetrics, Gauges) ->
    lists:foldl(
        fun(Gauge, Acc) ->
            case RawMetrics of
                #{gauges := #{Gauge := Value}} ->
                    Acc#{Gauge => Value};
                _ ->
                    Acc
            end
        end,
        #{},
        Gauges
    ).
