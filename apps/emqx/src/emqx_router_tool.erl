%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_router_tool).

-moduledoc """
Operator-facing diagnostics and repair helpers for the routing tables.

Safe to run on a live cluster:
* Streams via ets:first/next; never builds a whole-table list.
* Yields between batches; chunk size and sleep are tunable.
* Two-pass: any topic flagged "missing" in pass 1 is rechecked in pass 2
  to filter out false positives caused by in-flight (un)subscribes.
* Schema-agnostic: delegates to emqx_router:has_route/2 which handles
  both v2 (emqx_route / emqx_route_filters) and v3 (emqx_route_m /
  emqx_route_filters_m) tables transparently.

Shared subscriptions are NOT reconciled by this tool. Their entries in
emqx_suboption are keyed on a #share{} record (not a plain binary topic),
and their routes carry a {Group, Node} destination rather than a bare
Node. The diagnostics here are scoped to regular per-node routes, so any
non-binary (shared) subscription topic is skipped during the scan.
""".

-include_lib("emqx/include/emqx_router.hrl").

-export([
    cluster_schema_view/0,
    scan_missing_routes/0,
    scan_missing_routes/1,
    reconcile_missing_routes/0,
    reconcile_missing_routes/1
]).

-export_type([scan_opts/0, scan_result/0]).

-type schema_status() :: v1 | v2 | v3 | unknown | starting | {error, term()}.

-type scan_opts() :: #{
    %% rows per batch before sleeping (default 200)
    chunk => pos_integer(),
    %% ms to sleep after each batch (default 50)
    sleep_ms => non_neg_integer()
}.

-type scan_result() :: #{
    node := node(),
    schema := v1 | v2 | v3 | unknown,
    scanned := non_neg_integer(),
    missing := [emqx_types:topic()]
}.

-type reconcile_result() :: #{
    scanned := non_neg_integer(),
    missing := [emqx_types:topic()],
    repaired := [{emqx_types:topic(), ok | {error, term()}}]
}.

-define(DEFAULT_CHUNK, 200).
-define(DEFAULT_SLEEP_MS, 50).

%% In the ordered_set emqx_suboption table the key is `{Topic, SubPid}'.
%% An empty list sorts after every pid in Erlang term order, so
%% `{Topic, []}' is greater than `{Topic, AnySubPid}' but smaller than
%% any `{Topic2, _}' with Topic2 > Topic. Using it as the ets:next/2
%% continuation jumps straight to the first row of the next distinct
%% topic, skipping the remaining subscriber pids of the current topic.
%% This mirrors emqx_broker:foldl_topics/2.
-define(GREATER_THAN_ANY_PID, []).

%%--------------------------------------------------------------------
%% Cluster schema view
%%--------------------------------------------------------------------

-doc """
Return the route storage schema each cluster node is running, keyed by
node. Values are v1 | v2 | v3 | unknown | starting | {error, Reason}.

`starting' means the node is up but its schema persistent_term is not
set yet (router still booting); `{error, Reason}' captures any other RPC
failure. The local node is always included.
""".
-spec cluster_schema_view() -> #{node() => schema_status()}.
cluster_schema_view() ->
    Local = node(),
    Peers = emqx:running_nodes() -- [Local],
    Results = emqx_router_proto_v1:get_routing_schema_vsn(Peers),
    PeerView = maps:from_list(
        lists:zipwith(fun(Node, Res) -> {Node, classify_rpc(Res)} end, Peers, Results)
    ),
    PeerView#{Local => local_schema_vsn()}.

local_schema_vsn() ->
    try
        emqx_router:get_schema_vsn()
    catch
        error:badarg ->
            %% persistent_term not set yet; router still starting.
            starting
    end.

classify_rpc({ok, Vsn}) ->
    Vsn;
classify_rpc({error, {exception, badarg, _Stacktrace}}) ->
    %% Peer up but its schema persistent_term is not defined yet.
    starting;
classify_rpc({error, Reason}) ->
    {error, Reason};
classify_rpc(Other) ->
    {error, Other}.

%%--------------------------------------------------------------------
%% Scan for missing routes
%%--------------------------------------------------------------------

-doc "Equivalent to scan_missing_routes/1 with default options.".
-spec scan_missing_routes() -> scan_result().
scan_missing_routes() ->
    scan_missing_routes(#{}).

-doc """
Scan local subscriptions (emqx_suboption) for topics that have no
matching route entry in the cluster route table for this node.

The walk is streaming (ets:first/next, one distinct topic per step) and
throttled: it sleeps `sleep_ms' milliseconds after every `chunk' topics.
Two passes are run: any topic missing in pass 1 is rechecked in pass 2,
and only topics still missing in pass 2 are reported. This filters out
false positives caused by an unsubscribe (or a not-yet-synced subscribe)
racing the scan.

`scanned' is the number of distinct local topics inspected.
""".
-spec scan_missing_routes(scan_opts()) -> scan_result().
scan_missing_routes(Opts) ->
    Chunk = maps:get(chunk, Opts, ?DEFAULT_CHUNK),
    SleepMs = maps:get(sleep_ms, Opts, ?DEFAULT_SLEEP_MS),
    Node = node(),
    Schema = safe_get_schema_vsn(),
    %% Pass 1: walk emqx_suboption; collect topics whose route is absent.
    {Scanned, Pass1} = walk_subopts(Node, Chunk, SleepMs),
    %% Pass 2: recheck each pass-1 topic; keep only the still-missing ones.
    Missing = [Topic || Topic <- Pass1, not emqx_router:has_route(Topic, Node)],
    #{
        node => Node,
        schema => Schema,
        scanned => Scanned,
        missing => Missing
    }.

walk_subopts(Node, Chunk, SleepMs) ->
    First = ets:first(?SUBOPTION),
    walk_subopts(Node, Chunk, SleepMs, First, _Scanned = 0, _Acc = []).

walk_subopts(_Node, _Chunk, _SleepMs, '$end_of_table', Scanned, Acc) ->
    {Scanned, lists:reverse(Acc)};
walk_subopts(Node, Chunk, SleepMs, {Topic, _SubPid}, Scanned, Acc) ->
    Acc1 = check_topic(Topic, Node, Acc),
    Scanned1 = Scanned + 1,
    ok = maybe_yield(Scanned1, Chunk, SleepMs),
    Next = ets:next(?SUBOPTION, {Topic, ?GREATER_THAN_ANY_PID}),
    walk_subopts(Node, Chunk, SleepMs, Next, Scanned1, Acc1).

check_topic(Topic, Node, Acc) when is_binary(Topic) ->
    case is_shared_topic(Topic) of
        true ->
            %% Shared subs are reconciled elsewhere; see moduledoc.
            Acc;
        false ->
            case emqx_router:has_route(Topic, Node) of
                true -> Acc;
                false -> [Topic | Acc]
            end
    end;
check_topic(_NonBinaryTopic, _Node, Acc) ->
    %% #share{} records and any other non-binary topic: skip.
    Acc.

is_shared_topic(<<"$share/", _/binary>>) -> true;
is_shared_topic(_) -> false.

maybe_yield(Scanned, Chunk, SleepMs) when
    Scanned rem Chunk =:= 0, SleepMs > 0
->
    ok = timer:sleep(SleepMs);
maybe_yield(_Scanned, _Chunk, _SleepMs) ->
    ok.

safe_get_schema_vsn() ->
    try
        emqx_router:get_schema_vsn()
    catch
        error:badarg -> unknown
    end.

%%--------------------------------------------------------------------
%% Reconcile missing routes
%%--------------------------------------------------------------------

-doc "Equivalent to reconcile_missing_routes/1 with default options.".
-spec reconcile_missing_routes() -> reconcile_result().
reconcile_missing_routes() ->
    reconcile_missing_routes(#{}).

-doc """
Run scan_missing_routes/1 and re-add every reported missing route for the
local node via emqx_router:add_route/2 (the worker-serialized public API).

Returns the scan totals plus a `repaired' list pairing each topic with
the result of its add_route call.
""".
-spec reconcile_missing_routes(scan_opts()) -> reconcile_result().
reconcile_missing_routes(Opts) ->
    #{scanned := Scanned, missing := Missing} = scan_missing_routes(Opts),
    Node = node(),
    Repaired = [{Topic, emqx_router:add_route(Topic, Node)} || Topic <- Missing],
    #{
        scanned => Scanned,
        missing => Missing,
        repaired => Repaired
    }.
