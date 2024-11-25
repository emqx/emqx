%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    with_cache/3,
    reset/1,
    reset/2,
    stats/1,
    metrics/1
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
    deadline :: integer() | '_'
}).

-define(stat_key, stats).
-record(stats, {
    key :: ?stat_key,
    size :: non_neg_integer(),
    memory :: non_neg_integer()
}).

-define(pt_key(ID), {?MODULE, ID}).
-define(stat_update_interval, 5000).
-define(unlimited, unlimited).

%%--------------------------------------------------------------------
%% Metrics
%%--------------------------------------------------------------------

-define(metric_hit, auth_cache_hit).
-define(metric_miss, auth_cache_miss).
-define(metric_insert, auth_cache_insert).
-define(metric_size, auth_cache_size).
-define(metric_memory, auth_cache_memory).

%% For gauges we use only one "virtual" worker
-define(worker_id, worker_id).

%%--------------------------------------------------------------------
%% Types
%%--------------------------------------------------------------------

-type id() :: binary().
%% We want to cache many records under the same scope (id())
%% The Id may be a user id, a topic, etc.
-type cache_key() :: {id(), _Extra :: term()}.
-type name() :: atom().
-type config_path() :: runtime_config_key_path:runtime_config_key_path().
-type callback() :: fun(() -> {cache | nocache, term()}).

-type metrics_worker() :: emqx_metrics_worker:handler_name().

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
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Name, ConfigPath, MetricsWorker], []).

-spec with_cache(config_path(), cache_key(), callback()) -> term().
with_cache(Name, {Id, _Extra} = Key, Fun) when is_binary(Id) ->
    case is_cache_enabled(Name) of
        false ->
            with_cache_disabled(Fun);
        {true, PtState} ->
            with_cache_enabled(PtState, Key, Fun)
    end.

-spec reset(name()) -> ok.
reset(Name) ->
    try
        #{tab := Tab} = persistent_term:get(?pt_key(Name)),
        ets:delete_all_objects(Tab),
        ok
    catch
        error:badarg -> ok
    end.

-spec reset(name(), id()) -> ok.
reset(Name, Id) ->
    try
        #{tab := Tab} = persistent_term:get(?pt_key(Name)),
        Ms = [{#cache_record{key = {Id, '_'}, _ = '_'}, [], [true]}],
        _ = ets:select_delete(Tab, Ms),
        ok
    catch
        error:badarg -> ok
    end.

-spec stats(config_path()) -> not_found | #{size => non_neg_integer(), memory => non_neg_integer()}.
stats(Name) ->
    try
        #{tab := Tab} = persistent_term:get(?pt_key(Name)),
        tab_stats(Tab)
    catch
        error:badarg -> not_found
    end.

metrics(Name) ->
    try persistent_term:get(?pt_key(Name)) of
        #{name := Name, metrics_worker := MetricsWorker} ->
            {ok, emqx_metrics_worker:get_metrics(MetricsWorker, Name)}
    catch
        error:badarg -> not_found
    end.

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
    State = #{
        name => Name,
        tab => Tab,
        stat_tab => StatTab,
        config_path => ConfigPath,
        metrics_worker => MetricsWorker
    },
    ok = update_stats(State),
    _ = persistent_term:put(?pt_key(Name), State),
    _ = erlang:send_after(cleanup_interval(State), self(), #cleanup{}),
    _ = erlang:send_after(stat_update_interval(State), self(), #update_stats{}),
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
        MetricsWorker,
        Name,
        [
            ?metric_hit,
            ?metric_miss,
            ?metric_insert
        ],
        [
            ?metric_hit,
            ?metric_miss,
            ?metric_insert
        ]
    ).

%% TODO:
%% metrics
with_cache_enabled(#{tab := Tab} = PtState, Key, Fun) ->
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

inc_metric(#{name := Name, metrics_worker := MetricsWorker}, Metric) ->
    ok = emqx_metrics_worker:inc(MetricsWorker, Name, Metric).

set_gauge(#{name := Name, metrics_worker := MetricsWorker}, Metric, Value) ->
    ok = emqx_metrics_worker:set_gauge(MetricsWorker, Name, ?worker_id, Metric, Value).

cleanup(#{tab := Tab}) ->
    Now = now_ms_monotonic(),
    MS = ets:fun2ms(fun(#cache_record{deadline = Deadline}) when Deadline < Now -> true end),
    ?tp(warning, node_cache_cleanup, #{
        now => Now,
        records => ets:tab2list(Tab)
    }),
    NumDeleted = ets:select_delete(Tab, MS),
    ?tp(warning, node_cache_cleanup, #{
        num_deleted => NumDeleted
    }),
    ok.

update_stats(#{tab := Tab, stat_tab := StatTab} = State) ->
    #{size := Size, memory := Memory} = tab_stats(Tab),
    Stats = #stats{
        key = ?stat_key,
        size = Size,
        memory = Memory
    },
    ok = set_gauge(State, ?metric_size, Size),
    ok = set_gauge(State, ?metric_memory, Memory),
    ?tp(warning, update_stats, #{
        stats => Stats
    }),
    _ = ets:insert(StatTab, Stats),
    ok.

deadline(ConfigPath) ->
    now_ms_monotonic() + config_value(ConfigPath, cache_ttl).

cleanup_interval(#{config_path := ConfigPath}) ->
    config_value(ConfigPath, cleanup_interval).

stat_update_interval(#{config_path := ConfigPath}) ->
    config_value(ConfigPath, stat_update_interval, ?stat_update_interval).

now_ms_monotonic() ->
    erlang:monotonic_time(millisecond).

config_value(ConfigPath, Key) ->
    maps:get(Key, emqx_config:get(ConfigPath)).

config_value(ConfigPath, Key, Default) ->
    maps:get(Key, emqx_config:get(ConfigPath), Default).

lookup(Tab, Key) ->
    Now = now_ms_monotonic(),
    try ets:lookup(Tab, Key) of
        [#cache_record{value = Value, deadline = Deadlne}] when Deadlne > Now ->
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
        Size = ets:info(Tab, size),
        #{size => Size, memory => Memory}
    catch
        error:badarg -> not_found
    end.

maybe_insert(#{tab := Tab, stat_tab := StatTab, config_path := ConfigPath} = PtState, Key, Value) ->
    LimitsReached = limits_reached(ConfigPath, StatTab),
    ?tp(warning, node_cache_insert, #{
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
        deadline = deadline(ConfigPath)
    },
    try ets:insert(Tab, Record) of
        true -> ok
    catch
        error:badarg -> ok
    end.

limits_reached(ConfigPath, StatTab) ->
    MaxSize = config_value(ConfigPath, max_size, ?unlimited),
    MaxMemory = config_value(ConfigPath, max_memory, ?unlimited),
    [#stats{size = Size, memory = Memory}] = ets:lookup(StatTab, ?stat_key),
    ?tp(warning, node_cache_limits, #{
        size => Size,
        memory => Memory,
        max_size => MaxSize,
        max_memory => MaxMemory
    }),
    case {MaxSize, MaxMemory} of
        {MaxSize, _} when is_integer(MaxSize) andalso Size >= MaxSize -> true;
        {_, MaxMemory} when is_integer(MaxMemory) andalso Memory >= MaxMemory -> true;
        _ -> false
    end.

pt_state(#{name := Name} = _State) ->
    persistent_term:get(?pt_key(Name)).
