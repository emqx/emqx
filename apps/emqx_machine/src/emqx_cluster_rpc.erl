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
-behaviour(gen_server).

%% API
-export([start_link/0, mnesia/1]).
-export([multicall/3, multicall/4, query/1, reset/0, status/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    handle_continue/2, code_change/3]).

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
-define(TIMEOUT, timer:minutes(1)).

%%%===================================================================
%%% API
%%%===================================================================
mnesia(boot) ->
    ok = ekka_mnesia:create_table(?CLUSTER_MFA, [
        {type, ordered_set},
        {rlog_shard, ?COMMON_SHARD},
        {disc_copies, [node()]},
        {record_name, cluster_rpc_mfa},
        {attributes, record_info(fields, cluster_rpc_mfa)}]),
    ok = ekka_mnesia:create_table(?CLUSTER_COMMIT, [
        {type, set},
        {rlog_shard, ?COMMON_SHARD},
        {disc_copies, [node()]},
        {record_name, cluster_rpc_commit},
        {attributes, record_info(fields, cluster_rpc_commit)}]);
mnesia(copy) ->
    ok = ekka_mnesia:copy_table(cluster_rpc_mfa, disc_copies),
    ok = ekka_mnesia:copy_table(cluster_rpc_commit, disc_copies).

start_link() ->
    RetryMs = application:get_env(emqx_machine, cluster_call_retry_interval, 1000),
    start_link(node(), ?MODULE, RetryMs).

start_link(Node, Name, RetryMs) ->
    gen_server:start_link({local, Name}, ?MODULE, [Node, RetryMs], []).

-spec multicall(Module, Function, Args) -> {ok, TnxId} | {error, Reason} when
    Module :: module(),
    Function :: atom(),
    Args :: [term()],
    TnxId :: pos_integer(),
    Reason :: string().
multicall(M, F, A) ->
    multicall(M, F, A, timer:minutes(2)).

-spec multicall(Module, Function, Args, Timeout) -> {ok, TnxId} |{error, Reason} when
    Module :: module(),
    Function :: atom(),
    Args :: [term()],
    TnxId :: pos_integer(),
    Timeout :: timeout(),
    Reason :: string().
multicall(M, F, A, Timeout) ->
    MFA = {initiate, {M, F, A}},
    case ekka_rlog:role() of
        core -> gen_server:call(?MODULE, MFA, Timeout);
        replicant ->
            %% the initiate transaction must happened on core node
            %% make sure MFA(in the transaction) and the transaction on the same node
            %% don't need rpc again inside transaction.
            case ekka_rlog_status:upstream_node(?COMMON_SHARD) of
                {ok, Node} -> gen_server:call({?MODULE, Node}, MFA, Timeout);
                disconnected -> {error, disconnected}
            end
    end.

-spec query(pos_integer()) -> {'atomic', map()} | {'aborted', Reason :: term()}.
query(TnxId) ->
    transaction(fun trans_query/1, [TnxId]).

-spec reset() -> reset.
reset() -> gen_server:call(?MODULE, reset).

-spec status() -> {'atomic', [map()]} | {'aborted', Reason :: term()}.
status() ->
    transaction(fun trans_status/0, []).

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

%% @private
init([Node, RetryMs]) ->
    {ok, _} = mnesia:subscribe({table, ?CLUSTER_MFA, simple}),
    {ok, #{node => Node, retry_interval => RetryMs}, {continue, ?CATCH_UP}}.

%% @private
handle_continue(?CATCH_UP, State) ->
    {noreply, State, catch_up(State)}.

handle_call(reset, _From, State) ->
    _ = ekka_mnesia:clear_table(?CLUSTER_COMMIT),
    _ = ekka_mnesia:clear_table(?CLUSTER_MFA),
    {reply, ok, State, {continue, ?CATCH_UP}};

handle_call({initiate, MFA}, _From, State = #{node := Node}) ->
    case transaction(fun init_mfa/2, [Node, MFA]) of
        {atomic, {ok, TnxId}} ->
            {reply, {ok, TnxId}, State, {continue, ?CATCH_UP}};
        {aborted, Reason} ->
            {reply, {error, Reason}, State, {continue, ?CATCH_UP}}
    end;
handle_call(_, _From, State) ->
    {reply, ok, State, catch_up(State)}.

handle_cast(_, State) ->
    {noreply, State, catch_up(State)}.

handle_info({mnesia_table_event, _}, State) ->
    {noreply, State, catch_up(State)};
handle_info(_, State) ->
    {noreply, State, catch_up(State)}.

terminate(_Reason, _Data) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
catch_up(#{node := Node, retry_interval := RetryMs} = State) ->
    case transaction(fun get_next_mfa/1, [Node]) of
        {atomic, caught_up} -> ?TIMEOUT;
        {atomic, {still_lagging, NextId, MFA}} ->
            case apply_mfa(NextId, MFA) of
                ok ->
                    case transaction(fun commit/2, [Node, NextId]) of
                        {atomic, ok} -> catch_up(State);
                        Error ->
                            ?SLOG(error, #{
                                msg => "mnesia write transaction failed",
                                node => Node,
                                nextId => NextId,
                                error => Error}),
                            RetryMs
                    end;
                _Error -> RetryMs
            end;
        {aborted, Reason} ->
            ?SLOG(error, #{
                msg => "get_next_mfa transaction failed",
                node => Node, error => Reason}),
            RetryMs
    end.

get_next_mfa(Node) ->
    NextId =
        case mnesia:wread({?CLUSTER_COMMIT, Node}) of
            [] ->
                LatestId = get_latest_id(),
                TnxId = max(LatestId - 1, 0),
                commit(Node, TnxId),
                ?SLOG(notice, #{
                    msg => "New node first catch up and start commit.",
                    node => Node, tnx_id => TnxId}),
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
        [#cluster_rpc_commit{tnx_id = LastAppliedId}] ->
            Reason = lists:flatten(io_lib:format("~p catch up failed by LastAppliedId(~p) > ToTnxId(~p)",
                [Node, LastAppliedId, ToTnxId])),
            ?SLOG(error, #{
                msg => "catch up failed!",
                last_applied_id => LastAppliedId,
                node => Node,
                to_tnx_id => ToTnxId
            }),
            {error, Reason}
    end.

commit(Node, TnxId) ->
    ok = mnesia:write(?CLUSTER_COMMIT, #cluster_rpc_commit{node = Node, tnx_id = TnxId}, write).

get_latest_id() ->
    case mnesia:last(?CLUSTER_MFA) of
        '$end_of_table' -> 0;
        Id -> Id
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
