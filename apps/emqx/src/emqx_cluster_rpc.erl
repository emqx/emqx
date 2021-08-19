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
-export([start_link/2]).
-endif.

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

-include("emqx.hrl").
-include("logger.hrl").
-include("emqx_cluster_rpc.hrl").

-rlog_shard({?COMMON_SHARD, ?CLUSTER_MFA}).
-rlog_shard({?COMMON_SHARD, ?CLUSTER_COMMIT}).

-define(CATCH_UP, catch_up).
-define(REALTIME, realtime).
-define(CATCH_UP_AFTER(_Sec_), {state_timeout, timer:seconds(_Sec_), catch_up_delay}).

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
    start_link(node(), ?MODULE).
start_link(Node, Name) ->
    gen_statem:start_link({local, Name}, ?MODULE, [Node], []).

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
    Fun = fun() ->
        case mnesia:read(?CLUSTER_MFA, TnxId) of
            [] -> mnesia:abort(not_found);
            [#cluster_rpc_mfa{mfa = MFA, initiator = InitNode, created_at = CreatedAt}] ->
                #{tnx_id => TnxId, mfa => MFA, initiator => InitNode, created_at => CreatedAt}
        end
          end,
    transaction(Fun).

-spec reset() -> reset.
reset() -> gen_statem:call(?MODULE, reset).

-spec status() -> {'atomic', [map()]} | {'aborted', Reason :: term()}.
status() ->
    Fun = fun() ->
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
            end end, [], ?CLUSTER_COMMIT)
          end,
    transaction(Fun).

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

%% @private
init([Node]) ->
    {ok, _} = mnesia:subscribe({table, ?CLUSTER_MFA, simple}),
    {ok, ?CATCH_UP, Node, ?CATCH_UP_AFTER(0)}.

callback_mode() ->
    handle_event_function.

%% @private
format_status(Opt, [_PDict, StateName, Node]) ->
    #{state => StateName, node => Node, opt => Opt}.

%% @private
handle_event(state_timeout, catch_up_delay, _State, Node) ->
    catch_up(Node);

handle_event(info, {mnesia_table_event, {write, #cluster_rpc_mfa{} = MFARec, _AId}}, ?REALTIME, Node) ->
    handle_mfa_write_event(MFARec, Node);
handle_event(info, {mnesia_table_event, {write, #cluster_rpc_mfa{}, _ActivityId}}, ?CATCH_UP, _Node) ->
    {keep_state_and_data, [postpone, ?CATCH_UP_AFTER(0)]};

handle_event({call, From}, reset, _State, _Node) ->
    _ = ekka_mnesia:clear_table(?CLUSTER_COMMIT),
    _ = ekka_mnesia:clear_table(?CLUSTER_MFA),
    {keep_state_and_data, [{reply, From, ok}, ?CATCH_UP_AFTER(1)]};

handle_event({call, From}, {initiate, MFA}, ?REALTIME, Node) ->
    case transaction(fun() -> init_mfa(Node, MFA) end) of
        {atomic, {ok, TnxId}} ->
            {keep_state, Node, [{reply, From, {ok, TnxId}}]};
        {aborted, Reason} ->
            {keep_state, Node, [{reply, From, {error, Reason}}]}
    end;
handle_event({call, From}, {initiate, _MFA}, ?CATCH_UP, Node) ->
    case catch_up(Node) of
        {next_state, ?REALTIME, Node} ->
            {next_state, ?REALTIME, Node, [{postpone, true}]};
        _ ->
            Reason = "There are still transactions that have not been executed.",
            {keep_state, Node, [{reply, From, {error, Reason}}, ?CATCH_UP_AFTER(1)]}
    end;

handle_event(_EventType, _EventContent, ?CATCH_UP, _Node) ->
    {keep_state_and_data, [?CATCH_UP_AFTER(10)]};
handle_event(_EventType, _EventContent, _StateName, _Node) ->
    keep_state_and_data.

terminate(_Reason, _StateName, _Node) ->
    ok.

code_change(_OldVsn, StateName, Node, _Extra) ->
    {ok, StateName, Node}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
catch_up(Node) ->
    case get_next_mfa(Node) of
        {atomic, caught_up} -> {next_state, ?REALTIME, Node};
        {atomic, {still_lagging, NextId, MFA}} ->
            case apply_mfa(NextId, MFA) of
                ok ->
                    case transaction(fun() -> commit(Node, NextId) end) of
                        {atomic, ok} -> catch_up(Node);
                        _ -> {next_state, ?CATCH_UP, Node, [?CATCH_UP_AFTER(1)]}
                    end;
                _ -> {next_state, ?CATCH_UP, Node, [?CATCH_UP_AFTER(1)]}
            end;
        {aborted, _Reason} -> {next_state, ?CATCH_UP, Node, [?CATCH_UP_AFTER(1)]}
    end.

get_next_mfa(Node) ->
    Fun =
        fun() ->
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
            end
        end,
    transaction(Fun).

do_catch_up(ToTnxId, Node) ->
    case mnesia:wread({?CLUSTER_COMMIT, Node}) of
        [] ->
            commit(Node, ToTnxId),
            caught_up;
        [#cluster_rpc_commit{tnx_id = DoneTnxId}] when ToTnxId =:= DoneTnxId ->
            caught_up;
        [#cluster_rpc_commit{tnx_id = DoneTnxId}] when ToTnxId > DoneTnxId ->
            CurTnxId = DoneTnxId + 1,
            [#cluster_rpc_mfa{mfa = MFA}] = mnesia:read(?CLUSTER_MFA, CurTnxId),
            case apply_mfa(CurTnxId, MFA) of
                ok -> ok = commit(Node, CurTnxId);
                {error, Reason} -> mnesia:abort(Reason);
                Other -> mnesia:abort(Other)
            end;
        [#cluster_rpc_commit{tnx_id = DoneTnxId}]  ->
            Reason = lists:flatten(io_lib:format("~p catch up failed by DoneTnxId(~p) > ToTnxId(~p)",
                [Node, DoneTnxId, ToTnxId])),
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

handle_mfa_write_event(#cluster_rpc_mfa{tnx_id = TnxId, mfa = MFA}, Node) ->
    {atomic, DoneTnxId} = transaction(fun() -> get_done_id(Node, TnxId - 1) end),
    if DoneTnxId =:= TnxId - 1 ->
        case apply_mfa(TnxId, MFA) of
            ok ->
                case transaction(fun() -> commit(Node, TnxId) end) of
                    {atomic, ok} ->
                        {next_state, ?REALTIME, Node};
                    _ -> {next_state, ?CATCH_UP, Node, [?CATCH_UP_AFTER(1)]}
                end;
            _ -> {next_state, ?CATCH_UP, Node, [?CATCH_UP_AFTER(1)]}
        end;
        DoneTnxId >= TnxId -> %% It's means the initiator receive self event or other receive stale event.
            keep_state_and_data;
        true ->
            ?LOG(error, "LastAppliedID+1=/=EventId, maybe the mnesia event'order is messed up! restart process:~p",
                [{DoneTnxId, TnxId, MFA, Node}]),
            {stop, {"LastAppliedID+1=/=EventId", {DoneTnxId, TnxId, MFA, Node}}}
    end.

get_done_id(Node, Default) ->
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

transaction(Fun) ->
    ekka_mnesia:transaction(?COMMON_SHARD, Fun).

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
