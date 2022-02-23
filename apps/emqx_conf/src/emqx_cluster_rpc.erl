%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Note: multicall functions are statically checked by
%% `emqx_bapi_trans' and `emqx_bpapi_static_checks' modules. Don't
%% forget to update it when adding or removing them here:
-export([multicall/3, multicall/5, query/1, reset/0, status/0,
         skip_failed_commit/1, fast_forward_to_commit/2]).
-export([get_node_tnx_id/1, latest_tnx_id/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         handle_continue/2, code_change/3]).

-export_type([txn_id/0, succeed_num/0, multicall_return/1, multicall_return/0]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-boot_mnesia({mnesia, [boot]}).

-include_lib("emqx/include/logger.hrl").
-include("emqx_conf.hrl").

-define(CATCH_UP, catch_up).
-define(TIMEOUT, timer:minutes(1)).

-type txn_id() :: pos_integer().

-type succeed_num() :: pos_integer() | all.

-type multicall_return(Result) :: {ok, txn_id(), Result}
                                | {error, term()}
                                | {retry, txn_id(), Result, node()}.

-type multicall_return() :: multicall_return(_).

%%%===================================================================
%%% API
%%%===================================================================
mnesia(boot) ->
    ok = mria:create_table(?CLUSTER_MFA, [
        {type, ordered_set},
        {rlog_shard, ?CLUSTER_RPC_SHARD},
        {storage, disc_copies},
        {record_name, cluster_rpc_mfa},
        {attributes, record_info(fields, cluster_rpc_mfa)}]),
    ok = mria:create_table(?CLUSTER_COMMIT, [
        {type, set},
        {rlog_shard, ?CLUSTER_RPC_SHARD},
        {storage, disc_copies},
        {record_name, cluster_rpc_commit},
        {attributes, record_info(fields, cluster_rpc_commit)}]).

start_link() ->
    start_link(node(), ?MODULE, get_retry_ms()).

start_link(Node, Name, RetryMs) ->
    case gen_server:start_link({local, Name}, ?MODULE, [Node, RetryMs], []) of
        {ok, Pid} ->
            {ok, Pid};
        {error, {already_started, Pid}} ->
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc return {ok, TnxId, MFARes} the first MFA result when all MFA run ok.
%% return {error, MFARes} when the first MFA result is no ok or {ok, term()}.
%% return {retry, TnxId, MFARes, Nodes} when some Nodes failed and some Node ok.
-spec multicall(module(), atom(), list()) -> multicall_return().
multicall(M, F, A) ->
    multicall(M, F, A, all, timer:minutes(2)).

-spec multicall(module(), atom(), list(), succeed_num(), timeout()) -> multicall_return().
multicall(M, F, A, RequireNum, Timeout) when RequireNum =:= all orelse RequireNum >= 1 ->
    MFA = {initiate, {M, F, A}},
    Begin = erlang:monotonic_time(),
    InitRes =
        case mria_rlog:role() of
            core -> gen_server:call(?MODULE, MFA, Timeout);
            replicant ->
                %% the initiate transaction must happened on core node
                %% make sure MFA(in the transaction) and the transaction on the same node
                %% don't need rpc again inside transaction.
                case mria_status:upstream_node(?CLUSTER_RPC_SHARD) of
                    {ok, Node} -> gen_server:call({?MODULE, Node}, MFA, Timeout);
                    disconnected -> {error, disconnected}
                end
        end,
    End = erlang:monotonic_time(),
    MinDelay = erlang:convert_time_unit(End - Begin, native, millisecond) + 50,
    %% Fail after 3 attempts.
    RetryTimeout = ceil(3 * max(MinDelay, get_retry_ms())),
    OkOrFailed =
        case InitRes of
            {ok, _TnxId, _} when RequireNum =:= 1 ->
                ok;
            {ok, TnxId, _} when RequireNum =:= all ->
                wait_for_all_nodes_commit(TnxId, MinDelay, RetryTimeout);
            {ok, TnxId, _} when is_integer(RequireNum) ->
                wait_for_nodes_commit(RequireNum, TnxId, MinDelay, RetryTimeout);
            Error -> Error
        end,
    case OkOrFailed of
        ok -> InitRes;
        {error, Error0} -> {error, Error0};
        {retry, Node0} ->
            {ok, TnxId0, MFARes} = InitRes,
            {retry, TnxId0, MFARes, Node0}
    end.

-spec query(pos_integer()) -> {'atomic', map()} | {'aborted', Reason :: term()}.
query(TnxId) ->
    transaction(fun trans_query/1, [TnxId]).

-spec reset() -> reset.
reset() -> gen_server:call(?MODULE, reset).

-spec status() -> {'atomic', [map()]} | {'aborted', Reason :: term()}.
status() ->
    transaction(fun trans_status/0, []).

-spec latest_tnx_id() -> pos_integer().
latest_tnx_id() ->
    {atomic, TnxId} = transaction(fun get_latest_id/0, []),
    TnxId.

-spec get_node_tnx_id(node()) -> integer().
get_node_tnx_id(Node) ->
    case mnesia:wread({?CLUSTER_COMMIT, Node}) of
        [] -> -1;
        [#cluster_rpc_commit{tnx_id = TnxId}] -> TnxId
    end.

%% Regardless of what MFA is returned, consider it a success),
%% then move to the next tnxId.
%% if the next TnxId failed, need call the function again to skip.
-spec skip_failed_commit(node()) -> pos_integer().
skip_failed_commit(Node) ->
    gen_server:call({?MODULE, Node}, skip_failed_commit).

%% Regardless of what MFA is returned, consider it a success),
%% then skip the specified TnxId.
%% If CurrTnxId >= TnxId, nothing happened.
%% If CurrTnxId < TnxId, the CurrTnxId will skip to TnxId.
-spec fast_forward_to_commit(node(), pos_integer()) -> pos_integer().
fast_forward_to_commit(Node, ToTnxId) ->
    gen_server:call({?MODULE, Node}, {fast_forward_to_commit, ToTnxId}).
%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
init([Node, RetryMs]) ->
    _ = mria:wait_for_tables([?CLUSTER_MFA, ?CLUSTER_COMMIT]),
    {ok, _} = mnesia:subscribe({table, ?CLUSTER_MFA, simple}),
    State = #{node => Node, retry_interval => RetryMs},
    TnxId = emqx_app:get_init_tnx_id(),
    ok = maybe_init_tnx_id(Node, TnxId),
    {ok, State, {continue, ?CATCH_UP}}.

%% @private
handle_continue(?CATCH_UP, State) ->
    {noreply, State, catch_up(State)}.

handle_call(reset, _From, State) ->
    _ = mria:clear_table(?CLUSTER_COMMIT),
    _ = mria:clear_table(?CLUSTER_MFA),
    {reply, ok, State, {continue, ?CATCH_UP}};

handle_call({initiate, MFA}, _From, State = #{node := Node}) ->
    case transaction(fun init_mfa/2, [Node, MFA]) of
        {atomic, {ok, TnxId, Result}} ->
            {reply, {ok, TnxId, Result}, State, {continue, ?CATCH_UP}};
        {aborted, Reason} ->
            {reply, {error, Reason}, State, {continue, ?CATCH_UP}}
    end;
handle_call(skip_failed_commit, _From, State = #{node := Node}) ->
    Timeout = catch_up(State, true),
    {atomic, LatestId} = transaction(fun get_node_tnx_id/1, [Node]),
    {reply, LatestId, State, Timeout};
handle_call({fast_forward_to_commit, ToTnxId}, _From, State) ->
    NodeId = do_fast_forward_to_commit(ToTnxId, State),
    {reply, NodeId, State, catch_up(State)};
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
catch_up(State) -> catch_up(State, false).

catch_up(#{node := Node, retry_interval := RetryMs} = State, SkipResult) ->
    case transaction(fun read_next_mfa/1, [Node]) of
        {atomic, caught_up} -> ?TIMEOUT;
        {atomic, {still_lagging, NextId, MFA}} ->
            {Succeed, _} = apply_mfa(NextId, MFA),
            case Succeed orelse SkipResult of
                true ->
                    case transaction(fun commit/2, [Node, NextId]) of
                        {atomic, ok} -> catch_up(State, false);
                        Error ->
                            ?SLOG(error, #{
                                msg => "failed_to_commit_applied_call",
                                applied_id => NextId,
                                error => Error}),
                            RetryMs
                    end;
                false -> RetryMs
            end;
        {aborted, Reason} ->
            ?SLOG(error, #{msg => "read_next_mfa_transaction_failed", error => Reason}),
            RetryMs
    end.

read_next_mfa(Node) ->
    NextId =
        case mnesia:wread({?CLUSTER_COMMIT, Node}) of
            [] ->
                LatestId = get_latest_id(),
                TnxId = max(LatestId - 1, 0),
                commit(Node, TnxId),
                ?SLOG(notice, #{
                    msg => "new_node_first_catch_up_and_start_commit.",
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
                {true, _Result} -> ok = commit(Node, CurTnxId);
                {false, Error} -> mnesia:abort(Error)
            end;
        [#cluster_rpc_commit{tnx_id = LastAppliedId}] ->
            Reason = lists:flatten(
                io_lib:format("~p catch up failed by LastAppliedId(~p) > ToTnxId(~p)",
                [Node, LastAppliedId, ToTnxId])),
            ?SLOG(error, #{
                msg => "catch_up_failed!",
                last_applied_id => LastAppliedId,
                to_tnx_id => ToTnxId
            }),
            mnesia:abort(Reason)
    end.

commit(Node, TnxId) ->
    ok = mnesia:write(?CLUSTER_COMMIT, #cluster_rpc_commit{node = Node, tnx_id = TnxId}, write).

do_fast_forward_to_commit(ToTnxId, State = #{node := Node}) ->
    {atomic, NodeId} = transaction(fun get_node_tnx_id/1, [Node]),
    case NodeId >= ToTnxId of
        true -> NodeId;
        false ->
            {atomic, LatestId} = transaction(fun get_latest_id/0, []),
            case LatestId =< NodeId of
                true -> NodeId;
                false ->
                    catch_up(State, true),
                    do_fast_forward_to_commit(ToTnxId, State)
            end
    end.

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
    MFARec = #cluster_rpc_mfa{tnx_id = TnxId, mfa = MFA,
        initiator = Node, created_at = erlang:localtime()},
    ok = mnesia:write(?CLUSTER_MFA, MFARec, write),
    ok = commit(Node, TnxId),
    case apply_mfa(TnxId, MFA) of
        {true, Result} -> {ok, TnxId, Result};
        {false, Error} -> mnesia:abort(Error)
    end.

do_catch_up_in_one_trans(LatestId, Node) ->
    case do_catch_up(LatestId, Node) of
        caught_up -> ok;
        ok -> do_catch_up_in_one_trans(LatestId, Node)
    end.

transaction(Func, Args) ->
    mria:transaction(?CLUSTER_RPC_SHARD, Func, Args).

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

-define(TO_BIN(_B_), iolist_to_binary(io_lib:format("~p", [_B_]))).

apply_mfa(TnxId, {M, F, A}) ->
    Res =
        try erlang:apply(M, F, A)
        catch
            throw : Reason ->
                {error, #{reason => Reason}};
            Class : Reason : Stacktrace ->
                {error, #{exception => Class, reason => Reason, stacktrace => Stacktrace}}
        end,
    %% Do not log args as it might be sensitive information
    Meta = #{tnx_id => TnxId, entrypoint => format_mfa(M, F, length(A))},
    IsSuccess = is_success(Res),
    log_and_alarm(IsSuccess, Res, Meta),
    {IsSuccess, Res}.

format_mfa(M, F, A) ->
    iolist_to_binary([atom_to_list(M), ":", atom_to_list(F), "/", integer_to_list(A)]).

is_success(ok) -> true;
is_success({ok, _}) -> true;
is_success(_) -> false.

log_and_alarm(true, Res, Meta) ->
    ?SLOG(debug, Meta#{msg => "cluster_rpc_apply_ok", result => Res}),
    do_alarm(deactivate, Res, Meta);
log_and_alarm(false, Res, Meta) ->
    ?SLOG(error, Meta#{msg => "cluster_rpc_apply_failed", result => Res}),
    do_alarm(activate, Res, Meta).

do_alarm(Fun, Res, #{tnx_id := Id} = Meta) ->
    AlarmMsg = ["cluster_rpc_apply_failed=", integer_to_list(Id)],
    emqx_alarm:Fun(cluster_rpc_apply_failed, Meta#{result => ?TO_BIN(Res)}, AlarmMsg).

wait_for_all_nodes_commit(TnxId, Delay, Remain) ->
    case lagging_node(TnxId) of
        [_ | _] when Remain > 0 ->
            ok = timer:sleep(Delay),
            wait_for_all_nodes_commit(TnxId, Delay, Remain - Delay);
        [] -> ok;
        Nodes -> {retry, Nodes}
    end.

wait_for_nodes_commit(RequiredNum, TnxId, Delay, Remain) ->
    ok = timer:sleep(Delay),
    case length(synced_nodes(TnxId)) >= RequiredNum of
        true -> ok;
        false when Remain > 0 ->
            wait_for_nodes_commit(RequiredNum, TnxId, Delay, Remain - Delay);
        false ->
            case lagging_node(TnxId) of
                [] -> ok; %% All commit but The succeedNum > length(nodes()).
                Nodes -> {retry, Nodes}
            end
    end.

lagging_node(TnxId) ->
    {atomic, Nodes} = transaction(fun commit_status_trans/2, ['<', TnxId]),
    Nodes.

synced_nodes(TnxId) ->
    {atomic, Nodes} = transaction(fun commit_status_trans/2, ['>=', TnxId]),
    Nodes.

commit_status_trans(Operator, TnxId) ->
    MatchHead = #cluster_rpc_commit{tnx_id = '$1', node = '$2', _ = '_'},
    Guard = {Operator, '$1', TnxId},
    Result = '$2',
    mnesia:select(?CLUSTER_COMMIT, [{MatchHead, [Guard], [Result]}]).

get_retry_ms() ->
    emqx_conf:get(["node", "cluster_call", "retry_interval"], 1000).

maybe_init_tnx_id(_Node, TnxId)when TnxId < 0 -> ok;
maybe_init_tnx_id(Node, TnxId) ->
    {atomic, _} = transaction(fun init_node_tnx_id/2, [Node, TnxId]),
    ok.

init_node_tnx_id(Node, TnxId) ->
    case mnesia:read(?CLUSTER_COMMIT, Node) of
        [] -> commit(Node, TnxId);
        _ -> ok
    end.
