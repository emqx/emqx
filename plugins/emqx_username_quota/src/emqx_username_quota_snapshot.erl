%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_username_quota_snapshot).

-moduledoc """
Snapshot owner process for paginated username-quota listing.

Uses blue/green ETS tables to avoid data gaps during rebuild. Snapshots are built
asynchronously in a spawned process. Cursors encode `{node, generation, used_gte, last_key}`
so follow-up requests can be routed back to the same node and generation.

Only core nodes own snapshots; replicants forward requests to the leader core node.
""".

-behaviour(gen_server).

-export([
    start_link/0,
    request_page/5,
    request_page_local/5,
    invalidate/0,
    reset/0
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-include("emqx_username_quota.hrl").

-define(SERVER, ?MODULE).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

request_page(RequesterPid, DeadlineMs, Cursor, Limit, UsedGte) ->
    case resolve_target(Cursor) of
        local ->
            request_page_local(RequesterPid, DeadlineMs, Cursor, Limit, UsedGte);
        {forward_cursor, Node} ->
            forward_or_invalid(Node, RequesterPid, DeadlineMs, Cursor, Limit, UsedGte);
        {forward, Node} ->
            forward_or_local(Node, RequesterPid, DeadlineMs, Cursor, Limit, UsedGte)
    end.

request_page_local(RequesterPid, DeadlineMs, Cursor, Limit, UsedGte) ->
    TimeoutMs = erlang:max(1, DeadlineMs - now_ms()),
    try
        gen_server:call(
            ?SERVER,
            {request_page, RequesterPid, DeadlineMs, Cursor, Limit, UsedGte},
            TimeoutMs
        )
    catch
        exit:{timeout, _} -> {error, {busy, pseudo_cursor(node())}};
        exit:{noproc, _} -> {error, {rebuilding_snapshot, pseudo_cursor(node())}}
    end.

invalidate() ->
    gen_server:call(?SERVER, invalidate, infinity).

reset() ->
    try gen_server:call(?SERVER, reset, infinity) of
        ok -> ok
    catch
        exit:_ -> ok
    end.

init([]) ->
    process_flag(trap_exit, true),
    ensure_snapshot_tables(),
    {ok, initial_state()}.

handle_call(reset, _From, State) ->
    {reply, ok, do_reset(State)};
handle_call(invalidate, _From, State0) ->
    {reply, ok, maybe_start_build(State0)};
handle_call({request_page, _RequesterPid, DeadlineMs, Cursor, Limit, UsedGte}, _From, State0) ->
    case deadline_ok(DeadlineMs) of
        false ->
            {reply, rebuilding_reply(), State0};
        true ->
            State = maybe_trigger_rebuild(State0, UsedGte),
            {reply, serve_snapshot(DeadlineMs, Cursor, Limit, State), State}
    end;
handle_call(_Req, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({build_complete, Pid}, State) ->
    case maps:get(building, State) of
        Pid -> {noreply, complete_build(State)};
        _ -> {noreply, State}
    end;
handle_info({'EXIT', Pid, normal}, State) ->
    case maps:get(building, State) of
        Pid -> {noreply, clear_building(State)};
        _ -> {noreply, State}
    end;
handle_info({'EXIT', Pid, Reason}, State) ->
    case maps:get(building, State) of
        Pid ->
            logger:error(#{msg => "snapshot_builder_crashed", reason => Reason}),
            {noreply, clear_building(State)};
        _ ->
            {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Request routing
%%--------------------------------------------------------------------

resolve_target(undefined) ->
    resolve_target_no_cursor();
resolve_target(Cursor) ->
    case decode_cursor(Cursor) of
        {ok, {RemoteNode, _, _, _}} when RemoteNode =/= node() ->
            {forward_cursor, RemoteNode};
        _ ->
            local
    end.

resolve_target_no_cursor() ->
    case is_core_node() of
        true ->
            local;
        false ->
            case leader_node() of
                undefined -> local;
                Leader -> {forward, Leader}
            end
    end.

forward_or_invalid(Node, RequesterPid, DeadlineMs, Cursor, Limit, UsedGte) ->
    case forward_to_node(Node, RequesterPid, DeadlineMs, Cursor, Limit, UsedGte) of
        {error, forward_failed} -> {error, invalid_cursor};
        Result -> Result
    end.

forward_or_local(Node, RequesterPid, DeadlineMs, Cursor, Limit, UsedGte) ->
    case forward_to_node(Node, RequesterPid, DeadlineMs, Cursor, Limit, UsedGte) of
        {error, forward_failed} ->
            request_page_local(RequesterPid, DeadlineMs, Cursor, Limit, UsedGte);
        Result ->
            Result
    end.

forward_to_node(Node, RequesterPid, DeadlineMs, Cursor, Limit, UsedGte) ->
    TimeoutMs = erlang:max(1, DeadlineMs - now_ms()),
    try
        gen_server:call(
            {?SERVER, Node},
            {request_page, RequesterPid, DeadlineMs, Cursor, Limit, UsedGte},
            TimeoutMs
        )
    catch
        exit:{timeout, _} -> {error, forward_failed};
        exit:_ -> {error, forward_failed}
    end.

%%--------------------------------------------------------------------
%% Snapshot serving
%%--------------------------------------------------------------------

serve_snapshot(_DeadlineMs, _Cursor, _Limit, #{current := undefined}) ->
    rebuilding_reply();
serve_snapshot(DeadlineMs, Cursor, Limit, State) ->
    case deadline_ok(DeadlineMs) of
        true -> {ok, page_snapshot(State, Cursor, Limit)};
        false -> rebuilding_reply()
    end.

deadline_ok(DeadlineMs) ->
    DeadlineMs > now_ms().

rebuilding_reply() ->
    {error, {rebuilding_snapshot, pseudo_cursor(node())}}.

page_snapshot(State, Cursor, Limit) ->
    Color = maps:get(current, State),
    Tab = color_to_tab(Color),
    Generation = maps:get(generation, State),
    UsedGte = maps:get(used_gte, State),
    Total = ets:info(Tab, size),
    StartKey = resolve_start_key(Tab, Cursor, Generation),
    {Rows, LastKey, HasMore} = collect_page(Tab, StartKey, Limit, [], undefined),
    NextCursor =
        case HasMore andalso LastKey =/= undefined of
            true -> encode_cursor(node(), Generation, UsedGte, LastKey);
            false -> undefined
        end,
    #{
        total => Total,
        data => Rows,
        snapshot => #{
            node => node(),
            generation => Generation,
            taken_at_ms => maps:get(taken_at_ms, State)
        },
        next_cursor => NextCursor
    }.

resolve_start_key(Tab, Cursor, Generation) ->
    case decode_cursor(Cursor) of
        {ok, {CursorNode, CursorGen, _UsedGte, LastKey}} when
            CursorNode =:= node(), CursorGen =:= Generation
        ->
            ets:next(Tab, LastKey);
        _ ->
            ets:first(Tab)
    end.

collect_page(_Tab, Key, 0, Acc, LastKey) ->
    {lists:reverse(Acc), LastKey, Key =/= '$end_of_table'};
collect_page(_Tab, '$end_of_table', _Limit, Acc, LastKey) ->
    {lists:reverse(Acc), LastKey, false};
collect_page(Tab, {Counter, _Username} = Key, Limit, Acc, _LastKey) ->
    Row = {Key, Counter},
    collect_page(Tab, ets:next(Tab, Key), Limit - 1, [Row | Acc], Key).

%%--------------------------------------------------------------------
%% Build lifecycle
%%--------------------------------------------------------------------

maybe_trigger_rebuild(#{building := Pid} = State, _UsedGte) when is_pid(Pid) ->
    State;
maybe_trigger_rebuild(#{current := undefined} = State, UsedGte) ->
    start_build(State, UsedGte);
maybe_trigger_rebuild(State, UsedGte) ->
    maybe_rebuild_if_stale(State, UsedGte).

maybe_rebuild_if_stale(State, UsedGte) ->
    MinAgeMs = emqx_username_quota_config:snapshot_min_age_ms(),
    Age = now_ms() - maps:get(taken_at_ms, State),
    case Age >= MinAgeMs of
        true -> start_build(State, UsedGte);
        false -> State
    end.

maybe_start_build(#{building := Pid} = State) when is_pid(Pid) ->
    State;
maybe_start_build(State) ->
    UsedGte =
        case maps:get(used_gte, State) of
            undefined -> 1;
            V -> V
        end,
    start_build(State, UsedGte).

start_build(State, UsedGte) ->
    InactiveColor = inactive_color(maps:get(current, State)),
    InactiveTab = color_to_tab(InactiveColor),
    ets:delete_all_objects(InactiveTab),
    Owner = self(),
    YieldInterval = ?SNAPSHOT_BUILD_YIELD_INTERVAL,
    BuilderPid = spawn_link(fun() ->
        ok = emqx_username_quota_state:build_snapshot_into(InactiveTab, UsedGte, YieldInterval),
        Owner ! {build_complete, self()}
    end),
    State#{
        building => BuilderPid,
        building_color => InactiveColor,
        building_used_gte => UsedGte
    }.

complete_build(State) ->
    NewColor = maps:get(building_color, State),
    ok = maybe_clear_table(maps:get(current, State)),
    (clear_building(State))#{
        current => NewColor,
        generation => maps:get(generation, State) + 1,
        taken_at_ms => now_ms(),
        used_gte => maps:get(building_used_gte, State)
    }.

clear_building(State) ->
    State#{
        building => undefined,
        building_color => undefined,
        building_used_gte => undefined
    }.

%%--------------------------------------------------------------------
%% State helpers
%%--------------------------------------------------------------------

initial_state() ->
    #{
        current => undefined,
        generation => 0,
        taken_at_ms => 0,
        used_gte => undefined,
        building => undefined,
        building_color => undefined,
        building_used_gte => undefined
    }.

do_reset(State) ->
    case maps:get(building, State) of
        undefined -> ok;
        BuilderPid -> exit(BuilderPid, kill)
    end,
    clear_table(blue),
    clear_table(green),
    initial_state().

inactive_color(undefined) -> blue;
inactive_color(blue) -> green;
inactive_color(green) -> blue.

color_to_tab(blue) -> ?SNAPSHOT_TAB_BLUE;
color_to_tab(green) -> ?SNAPSHOT_TAB_GREEN.

maybe_clear_table(undefined) -> ok;
maybe_clear_table(Color) -> clear_table(Color).

clear_table(Color) ->
    true = ets:delete_all_objects(color_to_tab(Color)),
    ok.

ensure_snapshot_tables() ->
    ensure_table(?SNAPSHOT_TAB_BLUE),
    ensure_table(?SNAPSHOT_TAB_GREEN).

ensure_table(Tab) ->
    case ets:whereis(Tab) of
        undefined ->
            _ = ets:new(Tab, [ordered_set, named_table, public]),
            ok;
        _ ->
            ok
    end.

is_core_node() ->
    mria_rlog:role() =:= core.

leader_node() ->
    case lists:sort(mria:cluster_nodes(cores)) of
        [Leader | _] -> Leader;
        [] -> undefined
    end.

now_ms() ->
    erlang:system_time(millisecond).

%%--------------------------------------------------------------------
%% Cursor encoding/decoding
%%--------------------------------------------------------------------

encode_cursor(Node, Generation, UsedGte, LastKey) ->
    base64:encode(term_to_binary({Node, Generation, UsedGte, LastKey})).

decode_cursor(undefined) ->
    error;
decode_cursor(Cursor) when is_list(Cursor) ->
    decode_cursor(iolist_to_binary(Cursor));
decode_cursor(Cursor) when is_binary(Cursor) ->
    try binary_to_term(base64:decode(Cursor), [safe]) of
        {Node, Generation, UsedGte, {Counter, Username} = LastKey} when
            is_atom(Node),
            is_integer(Generation),
            is_integer(UsedGte),
            is_integer(Counter),
            is_binary(Username)
        ->
            {ok, {Node, Generation, UsedGte, LastKey}};
        %% Accept old 3-tuple format — will cause generation mismatch → page 1
        {Node, Generation, {Counter, Username} = LastKey} when
            is_atom(Node),
            is_integer(Generation),
            is_integer(Counter),
            is_binary(Username)
        ->
            {ok, {Node, Generation, undefined, LastKey}};
        _ ->
            error
    catch
        _:_ -> error
    end;
decode_cursor(_Other) ->
    error.

pseudo_cursor(Node) ->
    encode_cursor(Node, 0, 1, {0, <<>>}).
