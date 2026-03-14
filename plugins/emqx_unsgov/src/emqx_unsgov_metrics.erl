%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_unsgov_metrics).

-moduledoc """
Collects and aggregates UNS Governance allow/deny metrics and recent drops.
""".

-behaviour(gen_server).

-export([
    start_link/0,
    reset/0,
    record_allowed/0,
    record_allowed/1,
    record_allowed_model/1,
    record_exempt/0,
    record_drop/3,
    record_drop_model/3,
    delete_model/1,
    snapshot/0,
    recent_events/0
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include("emqx_unsgov.hrl").

-define(MAX_RECENT_DROPS, 100).
-define(SNAPSHOT_DEADLINE_MS, 5000).
-define(COUNTERS_TAB, emqx_unsgov_metrics_counters).
-define(RECENT_DROPS_TAB, emqx_unsgov_metrics_recent_drops).
-define(EVENTS_PER_NODE, 10).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

reset() ->
    gen_server:call(?MODULE, reset).

record_allowed() ->
    ok.

record_allowed(ModelId) ->
    record_allowed_model(ModelId).

record_allowed_model(undefined) ->
    ok;
record_allowed_model(ModelId) ->
    safe_ets(fun() -> bump_model_counters(ModelId, [messages_total, messages_allowed]) end).

record_exempt() ->
    safe_ets(fun() -> incr_counter({global, exempt}) end).

record_drop(Topic, ErrorType, ErrorDetail) ->
    ErrorTypeNorm = normalize_error_type(ErrorType),
    Drop = #{
        timestamp_ms => erlang:system_time(millisecond),
        topic => Topic,
        error_type => ErrorTypeNorm,
        error_detail => ErrorDetail
    },
    safe_ets(fun() ->
        maybe_bump_global_drop_counter(ErrorTypeNorm),
        push_recent_drop(Drop)
    end).

record_drop_model(undefined, _ErrorType, _ErrorDetail) ->
    ok;
record_drop_model(ModelId, ErrorType0, ErrorDetail) ->
    _ = ErrorDetail,
    ErrorType = normalize_error_type(ErrorType0),
    safe_ets(fun() ->
        bump_model_counters(ModelId, [messages_total, messages_dropped, ErrorType])
    end).

delete_model(ModelId) when is_binary(ModelId) ->
    safe_ets(fun() -> delete_model_counters(ModelId) end);
delete_model(_ModelId) ->
    ok.

snapshot() ->
    gen_server:call(?MODULE, snapshot).

recent_events() ->
    aggregate_recent_events().

init([]) ->
    _ = ets:new(?COUNTERS_TAB, [
        named_table,
        ordered_set,
        public,
        {read_concurrency, true},
        {write_concurrency, true}
    ]),
    _ = ets:new(?RECENT_DROPS_TAB, [
        named_table,
        ordered_set,
        public,
        {read_concurrency, true},
        {write_concurrency, true}
    ]),
    reset_ets(),
    {ok, #{}}.

handle_call(reset, _From, _State) ->
    reset_ets(),
    {reply, ok, #{}};
handle_call(snapshot, _From, State) ->
    {reply, aggregate_cluster_stats(), State};
handle_call({snapshot_local, DeadlineMs}, _From, State) ->
    case now_millisecond() > DeadlineMs of
        true ->
            %% stale request: intentionally ignore and let caller timeout
            {noreply, State};
        false ->
            {reply, format_stats(), State}
    end;
handle_call(snapshot_local, _From, State) ->
    {reply, format_stats(), State};
handle_call(recent_events_local, _From, State) ->
    {reply, local_recent_events(), State};
handle_call(_Call, _From, State) ->
    {reply, {error, bad_request}, State}.

handle_cast(allowed, State) ->
    {noreply, State};
handle_cast({allowed_model, ModelId}, State) ->
    bump_model_counters(ModelId, [messages_total, messages_allowed]),
    {noreply, State};
handle_cast(exempt, State) ->
    incr_counter({global, exempt}),
    {noreply, State};
handle_cast({drop, Topic, ErrorType0, ErrorDetail}, State) ->
    ErrorType = normalize_error_type(ErrorType0),
    Drop = #{
        timestamp_ms => erlang:system_time(millisecond),
        topic => Topic,
        error_type => ErrorType,
        error_detail => ErrorDetail
    },
    push_recent_drop(Drop),
    {noreply, State};
handle_cast({drop_model, ModelId, ErrorType0, _ErrorDetail}, State) ->
    ErrorType = normalize_error_type(ErrorType0),
    bump_model_counters(ModelId, [messages_total, messages_dropped, ErrorType]),
    {noreply, State};
handle_cast({delete_model, ModelId}, State) ->
    delete_model_counters(ModelId),
    {noreply, State};
handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

reset_ets() ->
    true = ets:delete_all_objects(?COUNTERS_TAB),
    true = ets:delete_all_objects(?RECENT_DROPS_TAB),
    ets:insert(?COUNTERS_TAB, [
        {{global, started_at_ms}, erlang:system_time(millisecond)},
        {{global, exempt}, 0},
        {{global, topic_nomatch}, 0},
        {{global, recent_seq}, 0}
    ]),
    ok.

bump_model_counters(ModelId, Keys) when is_binary(ModelId) ->
    lists:foreach(
        fun(Key) -> incr_counter({model, ModelId, Key}) end,
        Keys
    );
bump_model_counters(_ModelId, _Keys) ->
    ok.

incr_counter(Key) ->
    ets:update_counter(?COUNTERS_TAB, Key, {2, 1}, {Key, 0}),
    ok.

delete_model_counters(ModelId) ->
    ets:match_delete(?COUNTERS_TAB, {{model, ModelId, '_'}, '_'}),
    ok.

safe_ets(Fun) ->
    try Fun() of
        _ -> ok
    catch
        error:badarg ->
            ok
    end.

push_recent_drop(Drop) ->
    Seq = ets:update_counter(?COUNTERS_TAB, {global, recent_seq}, {2, 1}, {{global, recent_seq}, 0}),
    ets:insert(?RECENT_DROPS_TAB, {Seq, Drop}),
    case Seq - ?MAX_RECENT_DROPS of
        N when N > 0 -> ets:delete(?RECENT_DROPS_TAB, N);
        _ -> ok
    end,
    ok.

get_counter(Key) ->
    case ets:lookup(?COUNTERS_TAB, Key) of
        [{_, V}] -> V;
        _ -> 0
    end.

format_per_model_stats() ->
    lists:foldl(
        fun
            ({{model, ModelId, Metric}, Value}, Acc) ->
                ModelStats0 = maps:get(ModelId, Acc, init_model_stats()),
                ModelStats = ModelStats0#{Metric => Value},
                Acc#{ModelId => ModelStats};
            (_Other, Acc) ->
                Acc
        end,
        #{},
        ets:tab2list(?COUNTERS_TAB)
    ).

normalize_error_type(payload_invalid) -> payload_invalid;
normalize_error_type(not_endpoint) -> not_endpoint;
normalize_error_type(topic_nomatch) -> topic_nomatch;
normalize_error_type(topic_invalid) -> topic_invalid;
normalize_error_type(_Other) -> topic_invalid.

init_model_stats() ->
    #{
        messages_total => 0,
        messages_allowed => 0,
        messages_dropped => 0,
        topic_invalid => 0,
        payload_invalid => 0,
        not_endpoint => 0
    }.

format_stats() ->
    StartedAtMs = get_counter({global, started_at_ms}),
    UptimeSec = max(0, (erlang:system_time(millisecond) - StartedAtMs) div 1000),
    PerModel = format_per_model_stats(),
    Exempt = get_counter({global, exempt}),
    TopicNoMatch = get_counter({global, topic_nomatch}),
    TopicInvalid = sum_per_model_metric(PerModel, topic_invalid),
    PayloadInvalid = sum_per_model_metric(PerModel, payload_invalid),
    NotEndpoint = sum_per_model_metric(PerModel, not_endpoint),
    MessagesDropped = sum_per_model_metric(PerModel, messages_dropped),
    MessagesAllowed = sum_per_model_metric(PerModel, messages_allowed) + Exempt,
    MessagesTotal = sum_per_model_metric(PerModel, messages_total) + Exempt + TopicNoMatch,
    RecentDrops = [Drop || {_, Drop} <- ets:tab2list(?RECENT_DROPS_TAB)],
    #{
        messages_total => MessagesTotal,
        messages_allowed => MessagesAllowed,
        messages_dropped => MessagesDropped,
        topic_nomatch => TopicNoMatch,
        topic_invalid => TopicInvalid,
        payload_invalid => PayloadInvalid,
        not_endpoint => NotEndpoint,
        exempt => Exempt,
        uptime_seconds => UptimeSec,
        drop_breakdown => #{
            topic_invalid => TopicInvalid,
            payload_invalid => PayloadInvalid,
            not_endpoint => NotEndpoint
        },
        per_model => PerModel,
        recent_drops => RecentDrops
    }.

aggregate_cluster_stats() ->
    DeadlineMs = now_millisecond() + ?SNAPSHOT_DEADLINE_MS,
    Nodes = emqx:running_nodes(),
    NodeStats = emqx_utils:pmap(
        fun(Node) -> get_node_snapshot(Node, DeadlineMs) end, Nodes, infinity
    ),
    merge_node_stats(NodeStats).

get_node_snapshot(Node, _DeadlineMs) when Node =:= node() ->
    format_stats();
get_node_snapshot(Node, DeadlineMs) ->
    TimeoutMs = erlang:max(1, DeadlineMs - now_millisecond()),
    try gen_server:call({?MODULE, Node}, {snapshot_local, DeadlineMs}, TimeoutMs) of
        Snapshot when is_map(Snapshot) ->
            Snapshot;
        Other ->
            ?LOG(error, #{
                msg => "metrics_snapshot_invalid_response",
                node => Node,
                response => Other
            }),
            zero_stats()
    catch
        Class:Reason:Stacktrace ->
            ?LOG(error, #{
                msg => "metrics_snapshot_call_failed",
                node => Node,
                class => Class,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            zero_stats()
    end.

now_millisecond() ->
    erlang:system_time(millisecond).

merge_node_stats(NodeStats) ->
    Merged0 = lists:foldl(fun merge_two_stats/2, zero_stats(), NodeStats),
    finalize_merged_stats(Merged0).

merge_two_stats(Stats, Acc) ->
    Acc#{
        messages_total => sum_key(messages_total, Acc, Stats),
        messages_allowed => sum_key(messages_allowed, Acc, Stats),
        messages_dropped => sum_key(messages_dropped, Acc, Stats),
        topic_nomatch => sum_key(topic_nomatch, Acc, Stats),
        topic_invalid => sum_key(topic_invalid, Acc, Stats),
        payload_invalid => sum_key(payload_invalid, Acc, Stats),
        not_endpoint => sum_key(not_endpoint, Acc, Stats),
        exempt => sum_key(exempt, Acc, Stats),
        uptime_seconds => erlang:max(
            maps:get(uptime_seconds, Acc, 0),
            maps:get(uptime_seconds, Stats, 0)
        ),
        per_model => merge_per_model(
            maps:get(per_model, Acc, #{}),
            maps:get(per_model, Stats, #{})
        ),
        recent_drops => maps:get(recent_drops, Acc, []) ++
            maps:get(recent_drops, Stats, [])
    }.

sum_key(Key, A, B) ->
    maps:get(Key, A, 0) + maps:get(Key, B, 0).

finalize_merged_stats(Merged0) ->
    Drops0 = maps:get(recent_drops, Merged0, []),
    Drops1 = lists:sort(
        fun(A, B) ->
            maps:get(timestamp_ms, A, 0) >= maps:get(timestamp_ms, B, 0)
        end,
        Drops0
    ),
    Merged0#{
        drop_breakdown => #{
            topic_nomatch => maps:get(topic_nomatch, Merged0, 0),
            topic_invalid => maps:get(topic_invalid, Merged0, 0),
            payload_invalid => maps:get(payload_invalid, Merged0, 0),
            not_endpoint => maps:get(not_endpoint, Merged0, 0)
        },
        recent_drops => lists:sublist(Drops1, ?MAX_RECENT_DROPS)
    }.

merge_per_model(Left, Right) ->
    maps:fold(
        fun(ModelId, StatsR, Acc0) ->
            StatsL = maps:get(ModelId, Acc0, init_model_stats()),
            Acc0#{ModelId => merge_two_model_stats(StatsL, StatsR)}
        end,
        Left,
        Right
    ).

merge_two_model_stats(A, B) ->
    #{
        messages_total => sum_key(messages_total, A, B),
        messages_allowed => sum_key(messages_allowed, A, B),
        messages_dropped => sum_key(messages_dropped, A, B),
        topic_invalid => sum_key(topic_invalid, A, B),
        payload_invalid => sum_key(payload_invalid, A, B),
        not_endpoint => sum_key(not_endpoint, A, B)
    }.

zero_stats() ->
    #{
        messages_total => 0,
        messages_allowed => 0,
        messages_dropped => 0,
        topic_nomatch => 0,
        topic_invalid => 0,
        payload_invalid => 0,
        not_endpoint => 0,
        exempt => 0,
        uptime_seconds => 0,
        drop_breakdown => #{
            topic_nomatch => 0,
            topic_invalid => 0,
            payload_invalid => 0,
            not_endpoint => 0
        },
        per_model => #{},
        recent_drops => []
    }.

sum_per_model_metric(PerModel, Metric) ->
    maps:fold(
        fun(_ModelId, Stats, Acc) ->
            Acc + maps:get(Metric, Stats, 0)
        end,
        0,
        PerModel
    ).

maybe_bump_global_drop_counter(topic_nomatch) ->
    incr_counter({global, topic_nomatch});
maybe_bump_global_drop_counter(_) ->
    ok.

aggregate_recent_events() ->
    Nodes = mria:running_nodes(),
    NodeEvents = emqx_utils:pmap(fun get_node_events/1, Nodes, infinity),
    Events = lists:append(NodeEvents),
    lists:sort(
        fun(A, B) -> maps:get(timestamp_ms, A, 0) >= maps:get(timestamp_ms, B, 0) end, Events
    ).

get_node_events(Node) when Node =:= node() ->
    tag_node(node(), local_recent_events());
get_node_events(Node) ->
    try gen_server:call({?MODULE, Node}, recent_events_local, 5000) of
        Events when is_list(Events) -> tag_node(Node, Events);
        _ -> []
    catch
        _:_ -> []
    end.

local_recent_events() ->
    All = ets:tab2list(?RECENT_DROPS_TAB),
    Sorted = lists:sort(fun({A, _}, {B, _}) -> A >= B end, All),
    [Drop || {_, Drop} <- lists:sublist(Sorted, ?EVENTS_PER_NODE)].

tag_node(Node, Events) ->
    NodeBin = atom_to_binary(Node, utf8),
    [E#{node => NodeBin, timestamp => format_rfc3339(E)} || E <- Events].

format_rfc3339(#{timestamp_ms := Ms}) ->
    list_to_binary(
        calendar:system_time_to_rfc3339(Ms, [{unit, millisecond}, {offset, "Z"}])
    ).
