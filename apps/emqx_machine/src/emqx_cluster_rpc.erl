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
-module(emqx_cluster_rpc).
-behaviour(gen_statem).

%% API
-export([start_link/0, mnesia/1]).
-export([multicall/3, multicall/4, query/1, reset/0, status/0]).

-export([init/1, format_status/2, handle_event/4, terminate/3,
    code_change/4, callback_mode/0]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-export([start_link/3]).
-endif.

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include("emqx_cluster_rpc.hrl").

-rlog_shard({?COMMON_SHARD, ?CLUSTER_MFA}).
-rlog_shard({?COMMON_SHARD, ?CLUSTER_COMMIT}).

-define(CATCH_UP, catch_up).
-define(REALTIME, realtime).
-define(CATCH_UP_AFTER(_Ms_), {state_timeout, _Ms_, catch_up_delay}).

%%%===================================================================
%%% API
%%%===================================================================
mnesia(boot) ->
    ok = ekka_mnesia:create_table(?CLUSTER_MFA, [
        {type, ordered_set},
        {disc_copies, [node()]},
        {local_content, true},
        {record_name, cluster_rpc_mfa},
        {attributes, record_info(fields, cluster_rpc_mfa)}]),
    ok = ekka_mnesia:create_table(?CLUSTER_COMMIT, [
        {type, set},
        {disc_copies, [node()]},
        {local_content, true},
        {record_name, cluster_rpc_commit},
        {attributes, record_info(fields, cluster_rpc_commit)}]);
mnesia(copy) ->
    ok = ekka_mnesia:copy_table(cluster_rpc_mfa, disc_copies),
    ok = ekka_mnesia:copy_table(cluster_rpc_commit, disc_copies).

start_link() ->
    RetryMs = application:get_env(emqx_machine, cluster_call_retry_interval, 1000),
    start_link(node(), ?MODULE, RetryMs).
start_link(Node, Name, RetryMs) ->
    gen_statem:start_link({local, Name}, ?MODULE, [Node, RetryMs], []).

-spec multicall(Module, Function, Args) -> {ok, TnxId} | {error, Reason} when
    Module :: module(),
    Function :: atom(),
    Args :: [term()],
    TnxId :: pos_integer(),
    Reason :: term().
multicall(M, F, A) ->
    multicall(M, F, A, timer:minutes(2)).

-spec multicall(Module, Function, Args, Timeout) -> {ok, TnxId} |{error, Reason} when
    Module :: module(),
    Function :: atom(),
    Args :: [term()],
    TnxId :: pos_integer(),
    Timeout :: timeout(),
    Reason :: term().
multicall(M, F, A, Timeout) ->
    MFA = {initiate, {M, F, A}},
    case ekka_rlog:role() of
        core ->  gen_statem:call(?MODULE, MFA, Timeout);
        replicant ->
            case ekka_rlog_status:upstream_node(?COMMON_SHARD) of
                {ok, Node} -> gen_statem:call({?MODULE, Node}, MFA, Timeout);
                disconnected -> {error, disconnected}
            end
    end.

-spec query(pos_integer()) -> {'atomic', map()} | {'aborted', Reason :: term()}.
query(TnxId) ->
    transaction(fun trans_query/1, [TnxId]).

-spec reset() -> reset.
reset() -> gen_statem:call(?MODULE, reset).

-spec status() -> {'atomic', [map()]} | {'aborted', Reason :: term()}.
status() ->
    transaction(fun trans_status/0, []).

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

%% @private
init([Node, RetryMs]) ->
    {ok, _} = mnesia:subscribe({table, ?CLUSTER_MFA, simple}),
    {ok, ?CATCH_UP, #{node => Node, retry_interval => RetryMs}, ?CATCH_UP_AFTER(0)}.

callback_mode() ->
    handle_event_function.

%% @private
format_status(Opt, [_PDict, StateName, Data]) ->
    #{state => StateName, data => Data , opt => Opt}.

%% @private
handle_event(state_timeout, catch_up_delay, _State, Data) ->
    catch_up(Data);

handle_event(info, {mnesia_table_event, {write, #cluster_rpc_mfa{} = MFARec, _AId}}, ?REALTIME, Data) ->
    handle_mfa_write_event(MFARec, Data);
handle_event(info, {mnesia_table_event, {write, #cluster_rpc_mfa{}, _ActivityId}}, ?CATCH_UP, _Data) ->
    {keep_state_and_data, [postpone, ?CATCH_UP_AFTER(0)]};
%% we should catch up as soon as possible when we reset all.
handle_event(info, {mnesia_table_event, {delete,{schema, ?CLUSTER_MFA}, _Tid}}, _, _Data) ->
    {keep_state_and_data, [?CATCH_UP_AFTER(0)]};

handle_event({call, From}, reset, _State, _Data) ->
    _ = ekka_mnesia:clear_table(?CLUSTER_COMMIT),
    _ = ekka_mnesia:clear_table(?CLUSTER_MFA),
    {keep_state_and_data, [{reply, From, ok}, ?CATCH_UP_AFTER(0)]};

handle_event({call, From}, {initiate, MFA}, ?REALTIME, Data = #{node := Node}) ->
    case transaction(fun init_mfa/2, [Node, MFA]) of
        {atomic, {ok, TnxId}} ->
            {keep_state, Data, [{reply, From, {ok, TnxId}}]};
        {aborted, Reason} ->
            {keep_state, Data, [{reply, From, {error, Reason}}]}
    end;
handle_event({call, From}, {initiate, _MFA}, ?CATCH_UP, Data = #{retry_interval := RetryMs}) ->
    case catch_up(Data) of
        {next_state, ?REALTIME, Data} ->
            {next_state, ?REALTIME, Data, [{postpone, true}]};
        _ ->
            Reason = "There are still transactions that have not been executed.",
            {keep_state_and_data, [{reply, From, {error, Reason}}, ?CATCH_UP_AFTER(RetryMs)]}
    end;

handle_event(_EventType, _EventContent, ?CATCH_UP, #{retry_interval := RetryMs}) ->
    {keep_state_and_data, [?CATCH_UP_AFTER(RetryMs)]};
handle_event(_EventType, _EventContent, _StateName, _Data) ->
    keep_state_and_data.

terminate(_Reason, _StateName, _Data) ->
    ok.

code_change(_OldVsn, StateName, Data, _Extra) ->
    {ok, StateName, Data}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
catch_up(#{node := Node, retry_interval := RetryMs} = Data) ->
    case transaction(fun get_next_mfa/1, [Node]) of
        {atomic, caught_up} -> {next_state, ?REALTIME, Data};
        {atomic, {still_lagging, NextId, MFA}} ->
            case apply_mfa(NextId, MFA) of
                ok ->
                    case transaction(fun commit/2, [Node, NextId]) of
                        {atomic, ok} -> catch_up(Data);
                        _ -> {next_state, ?CATCH_UP, Data, [?CATCH_UP_AFTER(RetryMs)]}
                    end;
                _ -> {next_state, ?CATCH_UP, Data, [?CATCH_UP_AFTER(RetryMs)]}
            end;
        {aborted, _Reason} -> {next_state, ?CATCH_UP, Data, [?CATCH_UP_AFTER(RetryMs)]}
    end.

get_next_mfa(Node) ->
    NextId =
        case mnesia:wread({?CLUSTER_COMMIT, Node}) of
            [] ->
                LatestId = get_latest_id(),
                TnxId = max(LatestId - 1, 0),
                commit(Node, TnxId),
                ?LOG(notice, "New node(~p) first catch up and start commit at ~p", [Node, TnxId]),
                TnxId;
            [#cluster_rpc_commit{tnx_id = LastAppliedID}] -> LastAppliedID + 1
        end,
    case mnesia:read(?CLUSTER_MFA, NextId) of
        [] -> caught_up;
        [#cluster_rpc_mfa{mfa = MFA}] -> {still_lagging, NextId, MFA}
    end.

do_catch_up(ToTnxId, Node) ->
    case mnesia:wread({?CLUSTER_COMMIT, Node}) of
        [] ->
            commit(Node, ToTnxId),
            caught_up;
        [#cluster_rpc_commit{tnx_id = LastAppliedId}] when ToTnxId =:= LastAppliedId ->
            caught_up;
        [#cluster_rpc_commit{tnx_id = LastAppliedId}] when ToTnxId > LastAppliedId ->
            CurTnxId = LastAppliedId + 1,
            [#cluster_rpc_mfa{mfa = MFA}] = mnesia:read(?CLUSTER_MFA, CurTnxId),
            case apply_mfa(CurTnxId, MFA) of
                ok -> ok = commit(Node, CurTnxId);
                {error, Reason} -> mnesia:abort(Reason);
                Other -> mnesia:abort(Other)
            end;
        [#cluster_rpc_commit{tnx_id = LastAppliedId}]  ->
            Reason = lists:flatten(io_lib:format("~p catch up failed by LastAppliedId(~p) > ToTnxId(~p)",
                [Node, LastAppliedId, ToTnxId])),
            ?LOG(error, Reason),
            {error, Reason}
    end.

commit(Node, TnxId) ->
    ok = mnesia:write(?CLUSTER_COMMIT, #cluster_rpc_commit{node = Node, tnx_id = TnxId}, write).

get_latest_id() ->
    case mnesia:last(?CLUSTER_MFA) of
        '$end_of_table' -> 0;
        Id -> Id
    end.

handle_mfa_write_event(#cluster_rpc_mfa{tnx_id = EventId, mfa = MFA}, Data) ->
    #{node := Node, retry_interval := RetryMs} = Data,
    {atomic, LastAppliedId} = transaction(fun get_last_applied_id/2, [Node, EventId - 1]),
    if LastAppliedId + 1 =:= EventId ->
        case apply_mfa(EventId, MFA) of
            ok ->
                case transaction(fun commit/2, [Node, EventId]) of
                    {atomic, ok} ->
                        {next_state, ?REALTIME, Data};
                    _ -> {next_state, ?CATCH_UP, Data, [?CATCH_UP_AFTER(RetryMs)]}
                end;
            _ -> {next_state, ?CATCH_UP, Data, [?CATCH_UP_AFTER(RetryMs)]}
        end;
        LastAppliedId >= EventId -> %% It's means the initiator receive self event or other receive stale event.
            keep_state_and_data;
        true ->
            ?LOG(error, "LastAppliedId+1<EventId, maybe the mnesia event'order is messed up! restart process:~p",
                [{LastAppliedId, EventId, MFA, Node}]),
            {stop, {"LastAppliedId+1<EventId", {LastAppliedId, EventId, MFA, Node}}}
    end.

get_last_applied_id(Node, Default) ->
    case mnesia:wread({?CLUSTER_COMMIT, Node}) of
        [#cluster_rpc_commit{tnx_id = TnxId}] -> TnxId;
        [] ->
            commit(Node, Default),
            Default
    end.

init_mfa(Node, MFA) ->
    mnesia:write_lock_table(?CLUSTER_MFA),
    LatestId = get_latest_id(),
    ok = do_catch_up_in_one_trans(LatestId, Node),
    TnxId = LatestId + 1,
    MFARec = #cluster_rpc_mfa{tnx_id = TnxId, mfa = MFA, initiator = Node, created_at = erlang:localtime()},
    ok = mnesia:write(?CLUSTER_MFA, MFARec, write),
    ok = commit(Node, TnxId),
    case apply_mfa(TnxId, MFA) of
        ok -> {ok, TnxId};
        {error, Reason} -> mnesia:abort(Reason);
        Other -> mnesia:abort(Other)
    end.

do_catch_up_in_one_trans(LatestId, Node) ->
    case do_catch_up(LatestId, Node) of
        caught_up -> ok;
        ok -> do_catch_up_in_one_trans(LatestId, Node);
        {error, Reason} -> mnesia:abort(Reason)
    end.

transaction(Func, Args) ->
    ekka_mnesia:transaction(?COMMON_SHARD, Func, Args).

trans_status() ->
    mnesia:foldl(fun(Rec, Acc) ->
        #cluster_rpc_commit{node = Node, tnx_id = TnxId} = Rec,
        case mnesia:read(?CLUSTER_MFA, TnxId) of
            [MFARec] ->
                #cluster_rpc_mfa{mfa = MFA, initiator = InitNode, created_at = CreatedAt} = MFARec,
                [#{
                    node => Node,
                    tnx_id => TnxId,
                    initiator => InitNode,
                    mfa => MFA,
                    created_at => CreatedAt
                } | Acc];
            [] -> Acc
        end end, [], ?CLUSTER_COMMIT).

trans_query(TnxId) ->
    case mnesia:read(?CLUSTER_MFA, TnxId) of
        [] -> mnesia:abort(not_found);
        [#cluster_rpc_mfa{mfa = MFA, initiator = InitNode, created_at = CreatedAt}] ->
            #{tnx_id => TnxId, mfa => MFA, initiator => InitNode, created_at => CreatedAt}
    end.

apply_mfa(TnxId, {M, F, A} = MFA) ->
    try
        Res = erlang:apply(M, F, A),
            case Res =:= ok of
                true ->
                    ?SLOG(notice, #{msg => "succeeded to apply MFA", tnx_id => TnxId, mfa => MFA, result => ok});
                false ->
                    ?SLOG(error, #{msg => "failed to apply MFA", tnx_id => TnxId, mfa => MFA, result => Res})
                end,
        Res
    catch
        C : E ->
            ?SLOG(critical, #{msg => "crash to apply MFA", tnx_id => TnxId, mfa => MFA, exception => C, reason => E}),
            {error, lists:flatten(io_lib:format("TnxId(~p) apply MFA(~p) crash", [TnxId, MFA]))}
    end.
