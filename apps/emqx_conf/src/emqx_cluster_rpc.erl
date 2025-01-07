%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-export([start_link/0, create_tables/0]).

%% Note: multicall functions are statically checked by
%% `emqx_bapi_trans' and `emqx_bpapi_static_checks' modules. Don't
%% forget to update it when adding or removing them here:
-export([
    multicall/3, multicall/5,
    query/1,
    reset/0,
    status/0,
    is_initiator/1,
    find_leader/0,
    skip_failed_commit/1,
    fast_forward_to_commit/2,
    on_mria_stop/1,
    force_leave_clean/1,
    wait_for_cluster_rpc/0,
    maybe_init_tnx_id/2,
    update_mfa/3
]).
-export([
    commit/2,
    commit_status_trans/2,
    get_cluster_tnx_id/0,
    get_current_tnx_id/0,
    get_node_tnx_id/1,
    init_mfa/2,
    force_sync_tnx_id/3,
    latest_tnx_id/0,
    make_initiate_call_req/3,
    read_next_mfa/1,
    trans_query/1,
    trans_status/0,
    on_leave_clean/1,
    on_leave_clean/0,
    get_commit_lag/0,
    get_commit_lag/1
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    handle_continue/2,
    code_change/3
]).

-export([get_tables_status/0]).

-export_type([tnx_id/0, succeed_num/0]).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("emqx_conf.hrl").

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).

-endif.

-define(INITIATE(MFA), {initiate, MFA}).
-define(CATCH_UP, catch_up).
-define(TIMEOUT, timer:minutes(1)).
-define(IS_STATUS(_A_), (_A_ =:= peers_lagging orelse _A_ =:= stopped_nodes)).

-type tnx_id() :: pos_integer().

-type succeed_num() :: pos_integer() | all.

-type multicall_return(Result) ::
    {ok, tnx_id(), Result}
    | {init_failure, term()}
    | {peers_lagging, tnx_id(), Result, [node()]}.

-type multicall_return() :: multicall_return(_).
-type init_call_req() :: ?INITIATE({module(), atom(), list()}).

%%%===================================================================
%%% API
%%%===================================================================

create_tables() ->
    ok = mria:create_table(?CLUSTER_MFA, [
        {type, ordered_set},
        {rlog_shard, ?CLUSTER_RPC_SHARD},
        {storage, disc_copies},
        {record_name, cluster_rpc_mfa},
        {attributes, record_info(fields, cluster_rpc_mfa)}
    ]),
    ok = mria:create_table(?CLUSTER_COMMIT, [
        {type, set},
        {rlog_shard, ?CLUSTER_RPC_SHARD},
        {storage, disc_copies},
        {record_name, cluster_rpc_commit},
        {attributes, record_info(fields, cluster_rpc_commit)}
    ]),
    [
        ?CLUSTER_MFA,
        ?CLUSTER_COMMIT
    ].

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

%% @doc Initiate a local call (or core node),
%% then async-ly replicate the call to peer nodes in the cluster.
%% The evaluation result of the provided MFA is returned,
%% the result is expected to be `ok | {ok, _}' to indicate success,
%% and `{error, _}' to indicate failure.
%%
%% The exception of the MFA evaluation is captured and translated
%% into an `{error, _}' tuple.
%% This call tries to wait for all peer nodes to be in-sync before
%% returning the result.
%%
%% In case of partial success, an `error' level log is emitted
%% but the initial local apply result is returned.
-spec multicall(module(), atom(), list()) -> term().
multicall(M, F, A) ->
    multicall(M, F, A, all, timer:minutes(2)).

-spec multicall(module(), atom(), list(), succeed_num(), timeout()) -> term().
multicall(M, F, A, RequiredSyncs, Timeout) when RequiredSyncs =:= all orelse RequiredSyncs >= 1 ->
    case do_multicall(M, F, A, RequiredSyncs, Timeout) of
        {ok, _TxnId, Result} ->
            Result;
        {init_failure, Error} ->
            Error;
        {Status, TnxId, Res, Nodes} when ?IS_STATUS(Status) ->
            %% The init MFA return ok, but some other nodes failed.
            ?SLOG(error, #{
                msg => "cluster_rpc_peers_lagging",
                status => Status,
                nodes => Nodes,
                tnx_id => TnxId
            }),
            Res
    end.

%% Return {ok, TnxId, MFARes} the first MFA result when all MFA run ok.
%% return {init_failure, Error} when the initial MFA result is no ok or {ok, term()}.
%% return {peers_lagging, TnxId, MFARes, Nodes} when some Nodes failed and some Node ok.
-spec do_multicall(module(), atom(), list(), succeed_num(), timeout()) -> multicall_return().
do_multicall(M, F, A, RequiredSyncs, Timeout) ->
    %% assert
    true = (RequiredSyncs =:= all orelse RequiredSyncs >= 1),
    Begin = erlang:monotonic_time(),
    InitReq = make_initiate_call_req(M, F, A),
    InitRes =
        case mria_rlog:role() of
            core ->
                gen_server:call(?MODULE, InitReq, Timeout);
            replicant ->
                %% the initiate transaction must happened on core node
                %% make sure MFA(in the transaction) and the transaction on the same node
                %% don't need rpc again inside transaction.
                case mria_status:upstream_node(?CLUSTER_RPC_SHARD) of
                    {ok, Node} -> gen_server:call({?MODULE, Node}, InitReq, Timeout);
                    disconnected -> {error, disconnected}
                end
        end,
    End = erlang:monotonic_time(),
    MinDelay = erlang:convert_time_unit(End - Begin, native, millisecond) + 50,
    %% Fail after 3 attempts.
    RetryTimeout = ceil(3 * max(MinDelay, get_retry_ms())),
    OkOrFailed =
        case InitRes of
            {ok, _TnxId, _} when RequiredSyncs =:= 1 ->
                ok;
            {ok, TnxId, _} when RequiredSyncs =:= all ->
                wait_for_all_nodes_commit(TnxId, MinDelay, RetryTimeout);
            {ok, TnxId, _} when is_integer(RequiredSyncs) ->
                wait_for_nodes_commit(RequiredSyncs, TnxId, MinDelay, RetryTimeout);
            Error ->
                Error
        end,
    case OkOrFailed of
        ok ->
            InitRes;
        {init_failure, Error0} ->
            {init_failure, Error0};
        {Status, Nodes} when ?IS_STATUS(Status) ->
            {ok, TnxId0, MFARes} = InitRes,
            {Status, TnxId0, MFARes, Nodes}
    end.

-spec query(pos_integer()) -> {'atomic', map()} | {'aborted', Reason :: term()}.
query(TnxId) ->
    transaction(fun ?MODULE:trans_query/1, [TnxId]).

-spec reset() -> ok.
reset() -> gen_server:call(?MODULE, reset).

-spec status() -> {'atomic', [map()]} | {'aborted', Reason :: term()}.
status() ->
    transaction(fun ?MODULE:trans_status/0, []).

is_initiator(Opts) ->
    ?KIND_INITIATE =:= maps:get(kind, Opts, ?KIND_INITIATE).

find_leader() ->
    {atomic, Status} = status(),
    case Status of
        [#{node := N} | _] ->
            N;
        [] ->
            %% running nodes already sort.
            [N | _] = emqx:running_nodes(),
            N
    end.

%% DO NOT delete this on_leave_clean/0, It's use when rpc before v560.
on_leave_clean() ->
    on_leave_clean(node()).

on_leave_clean(Node) ->
    mnesia:delete({?CLUSTER_COMMIT, Node}).

-spec latest_tnx_id() -> pos_integer().
latest_tnx_id() ->
    {atomic, TnxId} = transaction(fun ?MODULE:get_cluster_tnx_id/0, []),
    TnxId.

-spec make_initiate_call_req(module(), atom(), list()) -> init_call_req().
make_initiate_call_req(M, F, A) ->
    ?INITIATE({M, F, A}).

-spec get_node_tnx_id(node()) -> integer().
get_node_tnx_id(Node) ->
    case mnesia:wread({?CLUSTER_COMMIT, Node}) of
        [] -> ?DEFAULT_INIT_TXN_ID;
        [#cluster_rpc_commit{tnx_id = TnxId}] -> TnxId
    end.

%% @doc Return the commit lag of *this* node.
-spec get_commit_lag() -> #{my_id := pos_integer(), latest := pos_integer()}.
get_commit_lag() ->
    {atomic, Result} = transaction(fun ?MODULE:get_commit_lag/1, [node()]),
    Result.

get_commit_lag(Node) ->
    LatestId = get_cluster_tnx_id(),
    LatestNode =
        case mnesia:read(?CLUSTER_MFA, LatestId) of
            [#?CLUSTER_MFA{initiator = N}] -> N;
            _ -> undefined
        end,
    MyId = get_node_tnx_id(Node),
    #{my_id => MyId, latest => LatestId, latest_node => LatestNode}.

%% Checks whether the Mnesia tables used by this module are waiting to
%% be loaded and from where.
-spec get_tables_status() -> #{atom() => {waiting, [node()]} | {loaded, local | node()}}.
get_tables_status() ->
    maps:from_list([
        {Tab, do_get_tables_status(Tab)}
     || Tab <- [?CLUSTER_COMMIT, ?CLUSTER_MFA]
    ]).

do_get_tables_status(Tab) ->
    Props = mnesia:table_info(Tab, all),
    TabNodes = proplists:get_value(all_nodes, Props),
    KnownDown = mnesia_recover:get_mnesia_downs(),
    LocalNode = node(),
    %% load_node. Returns the name of the node that Mnesia loaded the table from.
    %% The structure of the returned value is unspecified, but can be useful for debugging purposes.
    LoadedFrom = proplists:get_value(load_node, Props),
    case LoadedFrom of
        unknown ->
            {waiting, TabNodes -- [LocalNode | KnownDown]};
        LocalNode ->
            {loaded, local};
        Node ->
            {loaded, Node}
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

%% It is necessary to clean this node commit record in the cluster
on_mria_stop(leave) ->
    gen_server:call(?MODULE, on_leave);
on_mria_stop(_) ->
    ok.

force_leave_clean(Node) ->
    case transaction(fun ?MODULE:on_leave_clean/1, [Node]) of
        {atomic, ok} -> ok;
        {aborted, Reason} -> {error, Reason}
    end.

wait_for_cluster_rpc() ->
    %% Workaround for https://github.com/emqx/mria/issues/94:
    Msg1 = #{msg => "wait_for_cluster_rpc_shard"},
    case mria_rlog:wait_for_shards([?CLUSTER_RPC_SHARD], 1500) of
        ok -> ?SLOG(info, Msg1#{result => ok});
        Error0 -> ?SLOG(error, Msg1#{result => Error0})
    end,
    Msg2 = #{msg => "wait_for_cluster_rpc_tables"},
    case mria:wait_for_tables([?CLUSTER_MFA, ?CLUSTER_COMMIT]) of
        ok -> ?SLOG(info, Msg2#{result => ok});
        Error1 -> ?SLOG(error, Msg2#{result => Error1})
    end,
    ok.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
init([Node, RetryMs]) ->
    register_mria_stop_cb(fun ?MODULE:on_mria_stop/1),
    {ok, _} = mnesia:subscribe({table, ?CLUSTER_MFA, simple}),
    State = #{node => Node, retry_interval => RetryMs, is_leaving => false},
    %% Now continue with the normal catch-up process
    %% That is: apply the missing transactions after the config
    %% was copied until now.
    {ok, State, {continue, {?CATCH_UP, init}}}.

%% @private
handle_continue({?CATCH_UP, init}, State) ->
    %% emqx app must be started before
    %% trying to catch up the rpc commit logs
    ok = wait_for_emqx_ready(),
    ok = wait_for_cluster_rpc(),
    {noreply, State, catch_up(State)};
handle_continue(?CATCH_UP, State) ->
    {noreply, State, catch_up(State)}.

handle_call(reset, _From, State) ->
    _ = mria:clear_table(?CLUSTER_COMMIT),
    _ = mria:clear_table(?CLUSTER_MFA),
    {reply, ok, State, {continue, ?CATCH_UP}};
handle_call(?INITIATE(MFA), _From, State) ->
    do_initiate(MFA, State, 1, #{});
handle_call(skip_failed_commit, _From, State = #{node := Node}) ->
    Timeout = catch_up(State, true),
    {atomic, LatestId} = transaction(fun ?MODULE:get_node_tnx_id/1, [Node]),
    {reply, LatestId, State, Timeout};
handle_call({fast_forward_to_commit, ToTnxId}, _From, State) ->
    NodeId = do_fast_forward_to_commit(ToTnxId, State),
    {reply, NodeId, State, catch_up(State)};
handle_call(on_leave, _From, State) ->
    {atomic, ok} = transaction(fun ?MODULE:on_leave_clean/1, [node()]),
    {reply, ok, State#{is_leaving := true}};
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

catch_up(#{node := Node, retry_interval := RetryMs, is_leaving := false} = State, SkipResult) ->
    case transaction(fun ?MODULE:read_next_mfa/1, [Node]) of
        {atomic, caught_up} ->
            ?tp(cluster_rpc_caught_up, #{}),
            ?TIMEOUT;
        {atomic, {still_lagging, NextId, MFA}} ->
            {Succeed, _} = apply_mfa(NextId, MFA, ?KIND_REPLICATE),
            case Succeed orelse SkipResult of
                true ->
                    case transaction(fun ?MODULE:commit/2, [Node, NextId]) of
                        {atomic, ok} ->
                            catch_up(State, false);
                        Error ->
                            ?SLOG(error, #{
                                msg => "failed_to_commit_applied_call",
                                applied_id => NextId,
                                error => Error
                            }),
                            RetryMs
                    end;
                false ->
                    RetryMs
            end;
        {aborted, Reason} ->
            ?SLOG(error, #{msg => "read_next_mfa_transaction_failed", error => Reason}),
            RetryMs
    end;
catch_up(#{is_leaving := true}, _SkipResult) ->
    ?SLOG(info, #{msg => "ignore_mfa_transactions", reason => "Node is in leaving"}),
    ?TIMEOUT.

read_next_mfa(Node) ->
    NextId =
        case mnesia:wread({?CLUSTER_COMMIT, Node}) of
            [] ->
                LatestId = get_cluster_tnx_id(),
                TnxId = max(LatestId - 1, 0),
                commit(Node, TnxId),
                ?SLOG(notice, #{
                    msg => "new_node_first_catch_up_and_start_commit.",
                    node => Node,
                    tnx_id => TnxId
                }),
                TnxId;
            [#cluster_rpc_commit{tnx_id = LastAppliedID}] ->
                OldestId = get_oldest_mfa_id(),
                max(LastAppliedID + 1, OldestId)
        end,
    case mnesia:read(?CLUSTER_MFA, NextId) of
        [] -> caught_up;
        [#cluster_rpc_mfa{mfa = MFA}] -> {still_lagging, NextId, MFA}
    end.

commit(Node, TnxId) ->
    ok = mnesia:write(?CLUSTER_COMMIT, #cluster_rpc_commit{node = Node, tnx_id = TnxId}, write).

do_fast_forward_to_commit(ToTnxId, State = #{node := Node}) ->
    {atomic, NodeId} = transaction(fun ?MODULE:get_node_tnx_id/1, [Node]),
    case NodeId >= ToTnxId of
        true ->
            NodeId;
        false ->
            case latest_tnx_id() =< NodeId of
                true ->
                    NodeId;
                false ->
                    catch_up(State, true),
                    do_fast_forward_to_commit(ToTnxId, State)
            end
    end.

get_cluster_tnx_id() ->
    case mnesia:last(?CLUSTER_MFA) of
        '$end_of_table' -> 0;
        Id -> Id
    end.

get_current_tnx_id() ->
    case mnesia:dirty_read(?CLUSTER_COMMIT, node()) of
        [] -> ?DEFAULT_INIT_TXN_ID;
        [#cluster_rpc_commit{tnx_id = TnxId}] -> TnxId
    end.

get_oldest_mfa_id() ->
    case mnesia:first(?CLUSTER_MFA) of
        '$end_of_table' -> 0;
        Id -> Id
    end.

do_initiate(_MFA, State, Count, Failure) when Count > 10 ->
    %% refuse to initiate cluster call from this node
    %% because it's likely that the caller is based on
    %% a stale view event we retry 10 time.
    Error = stale_view_of_cluster_msg(Failure, Count),
    {reply, {init_failure, Error}, State, {continue, ?CATCH_UP}};
do_initiate(MFA, State = #{node := Node}, Count, Failure0) ->
    case transaction(fun ?MODULE:init_mfa/2, [Node, MFA]) of
        {atomic, {ok, TnxId, Result}} ->
            {reply, {ok, TnxId, Result}, State, {continue, ?CATCH_UP}};
        {atomic, {retry, Failure1}} when Failure0 =:= Failure1 ->
            %% Useless retry, so we return early.
            Error = stale_view_of_cluster_msg(Failure0, Count),
            {reply, {init_failure, Error}, State, {continue, ?CATCH_UP}};
        {atomic, {retry, Failure1}} ->
            catch_up(State),
            do_initiate(MFA, State, Count + 1, Failure1);
        {aborted, Error} ->
            {reply, {init_failure, Error}, State, {continue, ?CATCH_UP}}
    end.

stale_view_of_cluster_msg(Meta, Count) ->
    Node = find_leader(),
    Reason = Meta#{
        msg => stale_view_of_cluster,
        retry_times => Count,
        suggestion => ?SUGGESTION(Node)
    },
    ?SLOG(warning, Reason),
    {error, Reason}.

%% The entry point of a config change transaction.
init_mfa(Node, MFA) ->
    mnesia:write_lock_table(?CLUSTER_MFA),
    LatestId = get_cluster_tnx_id(),
    MyTnxId = get_node_tnx_id(Node),
    case MyTnxId =:= LatestId of
        true ->
            TnxId = LatestId + 1,
            MFARec = #cluster_rpc_mfa{
                tnx_id = TnxId,
                mfa = MFA,
                initiator = Node,
                created_at = erlang:localtime()
            },
            ok = mnesia:write(?CLUSTER_MFA, MFARec, write),
            ok = commit(Node, TnxId),
            case apply_mfa(TnxId, MFA, ?KIND_INITIATE) of
                {true, Result} -> {ok, TnxId, Result};
                {false, Error} -> mnesia:abort(Error)
            end;
        false ->
            Meta = #{cluster_tnx_id => LatestId, node_tnx_id => MyTnxId},
            {retry, Meta}
    end.

force_sync_tnx_id(Node, MFA, NodeTnxId) ->
    mnesia:write_lock_table(?CLUSTER_MFA),
    case get_node_tnx_id(Node) of
        NodeTnxId ->
            TnxId = NodeTnxId + 1,
            MFARec = #cluster_rpc_mfa{
                tnx_id = TnxId,
                mfa = MFA,
                initiator = Node,
                created_at = erlang:localtime()
            },
            ok = mnesia:write(?CLUSTER_MFA, MFARec, write),
            lists:foreach(
                fun(N) ->
                    ok = emqx_cluster_rpc:commit(N, NodeTnxId)
                end,
                mria:running_nodes()
            );
        NewTnxId ->
            Fmt = "aborted_force_sync, tnx_id(~w) is not the latest(~w)",
            Reason = emqx_utils:format(Fmt, [NodeTnxId, NewTnxId]),
            mnesia:abort({error, Reason})
    end.

update_mfa(Node, MFA, LatestId) ->
    case transaction(fun ?MODULE:force_sync_tnx_id/3, [Node, MFA, LatestId]) of
        {atomic, ok} -> ok;
        {aborted, Error} -> Error
    end.

transaction(Func, Args) ->
    mria:transaction(?CLUSTER_RPC_SHARD, Func, Args).

trans_status() ->
    List = mnesia:foldl(
        fun(Rec, Acc) ->
            #cluster_rpc_commit{node = Node, tnx_id = TnxId} = Rec,
            case mnesia:read(?CLUSTER_MFA, TnxId) of
                [MFARec] ->
                    #cluster_rpc_mfa{mfa = MFA, initiator = InitNode, created_at = CreatedAt} =
                        MFARec,
                    [
                        #{
                            node => Node,
                            tnx_id => TnxId,
                            initiator => InitNode,
                            mfa => MFA,
                            created_at => CreatedAt
                        }
                        | Acc
                    ];
                [] ->
                    Acc
            end
        end,
        [],
        ?CLUSTER_COMMIT
    ),
    Cores = lists:sort(mria:cluster_nodes(cores)),
    RunningNodes = mria:running_nodes(),
    %% Make sure cores is ahead of replicants
    Replicants = lists:subtract(RunningNodes, Cores),
    Nodes = lists:append(Cores, Replicants),
    {NodeIndices, _} = lists:foldl(
        fun(N, {Acc, Seq}) ->
            {maps:put(N, Seq, Acc), Seq + 1}
        end,
        {#{}, 1},
        Nodes
    ),
    lists:sort(
        fun(A, B) ->
            compare_tnx_id_and_node(A, B, NodeIndices)
        end,
        List
    ).

compare_tnx_id_and_node(
    #{tnx_id := Id, node := NA},
    #{tnx_id := Id, node := NB},
    NodeIndices
    %% The smaller the seq, the higher the priority level.
) ->
    maps:get(NA, NodeIndices, undefined) < maps:get(NB, NodeIndices, undefined);
compare_tnx_id_and_node(#{tnx_id := IdA}, #{tnx_id := IdB}, _NodeIndices) ->
    IdA > IdB.

trans_query(TnxId) ->
    case mnesia:read(?CLUSTER_MFA, TnxId) of
        [] ->
            mnesia:abort(not_found);
        [#cluster_rpc_mfa{mfa = MFA, initiator = InitNode, created_at = CreatedAt}] ->
            #{tnx_id => TnxId, mfa => MFA, initiator => InitNode, created_at => CreatedAt}
    end.

-define(TO_BIN(_B_), iolist_to_binary(io_lib:format("~p", [_B_]))).

apply_mfa(TnxId, {M, F, A}, Kind) ->
    Res =
        try
            erlang:apply(M, F, A ++ [#{kind => Kind}])
        catch
            throw:Reason ->
                {error, #{reason => Reason}};
            Class:Reason:Stacktrace ->
                {error, #{exception => Class, reason => Reason, stacktrace => Stacktrace}}
        end,
    %% Do not log args as it might be sensitive information
    Meta = #{kind => Kind, tnx_id => TnxId, entrypoint => format_mfa(M, F, length(A))},
    IsSuccess = is_success(Res),
    log_and_alarm(IsSuccess, Res, Meta),
    {IsSuccess, Res}.

format_mfa(M, F, A) ->
    iolist_to_binary([atom_to_list(M), ":", atom_to_list(F), "/", integer_to_list(A)]).

is_success(ok) -> true;
is_success({ok, _}) -> true;
is_success(_) -> false.

log_and_alarm(IsSuccess, Res, #{kind := ?KIND_INITIATE} = Meta) ->
    %% no alarm or error log in case of failure at originating a new cluster-call
    %% because nothing is committed
    case IsSuccess of
        true ->
            ?SLOG(debug, Meta#{msg => "cluster_rpc_apply_result", result => emqx_utils:redact(Res)});
        false ->
            ?SLOG(warning, Meta#{
                msg => "cluster_rpc_apply_result", result => emqx_utils:redact(Res)
            })
    end;
log_and_alarm(true, Res, Meta) ->
    ?SLOG(debug, Meta#{msg => "cluster_rpc_apply_ok", result => emqx_utils:redact(Res)}),
    do_alarm(deactivate, Res, Meta);
log_and_alarm(false, Res, Meta) ->
    ?SLOG(error, Meta#{msg => "cluster_rpc_apply_failed", result => emqx_utils:redact(Res)}),
    do_alarm(activate, Res, Meta).

do_alarm(Fun, Res, #{tnx_id := Id} = Meta) ->
    AlarmMsg = ["cluster_rpc_apply_failed=", integer_to_list(Id)],
    emqx_alarm:Fun(cluster_rpc_apply_failed, Meta#{result => ?TO_BIN(Res)}, AlarmMsg).

wait_for_all_nodes_commit(TnxId, Delay, Remain) ->
    Lagging = lagging_nodes(TnxId),
    Stopped = Lagging -- mria:running_nodes(),
    case Lagging -- Stopped of
        [] when Stopped =:= [] ->
            ok;
        [] ->
            {stopped_nodes, Stopped};
        [_ | _] when Remain > 0 ->
            ok = timer:sleep(Delay),
            wait_for_all_nodes_commit(TnxId, Delay, Remain - Delay);
        [_ | _] ->
            {peers_lagging, Lagging}
    end.

wait_for_nodes_commit(RequiredSyncs, TnxId, Delay, Remain) ->
    ok = timer:sleep(Delay),
    case length(synced_nodes(TnxId)) >= RequiredSyncs of
        true ->
            ok;
        false when Remain > 0 ->
            wait_for_nodes_commit(RequiredSyncs, TnxId, Delay, Remain - Delay);
        false ->
            case lagging_nodes(TnxId) of
                [] ->
                    ok;
                Lagging ->
                    Stopped = Lagging -- mria:running_nodes(),
                    case Stopped of
                        [] -> {peers_lagging, Lagging};
                        _ -> {stopped_nodes, Stopped}
                    end
            end
    end.

lagging_nodes(TnxId) ->
    {atomic, Nodes} = transaction(fun ?MODULE:commit_status_trans/2, ['<', TnxId]),
    Nodes.

synced_nodes(TnxId) ->
    {atomic, Nodes} = transaction(fun ?MODULE:commit_status_trans/2, ['>=', TnxId]),
    Nodes.

commit_status_trans(Operator, TnxId) ->
    MatchHead = #cluster_rpc_commit{tnx_id = '$1', node = '$2', _ = '_'},
    Guard = {Operator, '$1', TnxId},
    Result = '$2',
    mnesia:select(?CLUSTER_COMMIT, [{MatchHead, [Guard], [Result]}]).

get_retry_ms() ->
    emqx_conf:get([node, cluster_call, retry_interval], timer:minutes(1)).

maybe_init_tnx_id(_Node, TnxId) when TnxId < 0 -> ok;
maybe_init_tnx_id(Node, TnxId) ->
    {atomic, _} = transaction(fun ?MODULE:commit/2, [Node, TnxId]),
    ok.

%% @private Cannot proceed until emqx app is ready.
%% Otherwise the committed transaction catch up may fail.
wait_for_emqx_ready() ->
    %% wait 10 seconds for emqx to start
    ok = do_wait_for_emqx_ready(10).

%% Wait for emqx app to be ready,
%% write a log message every 1 second
do_wait_for_emqx_ready(0) ->
    timeout;
do_wait_for_emqx_ready(N) ->
    %% check interval is 100ms
    %% makes the total wait time 1 second
    case do_wait_for_emqx_ready2(10) of
        ok ->
            ok;
        timeout ->
            ?SLOG(warning, #{msg => "still_waiting_for_emqx_app_to_be_ready"}),
            do_wait_for_emqx_ready(N - 1)
    end.

%% Wait for emqx app to be ready,
%% check interval is 100ms
do_wait_for_emqx_ready2(0) ->
    timeout;
do_wait_for_emqx_ready2(N) ->
    case emqx:is_running() of
        true ->
            ok;
        false ->
            timer:sleep(100),
            do_wait_for_emqx_ready2(N - 1)
    end.

register_mria_stop_cb(Callback) ->
    case mria_config:callback(stop) of
        undefined ->
            mria:register_callback(stop, Callback);
        {ok, Previous} ->
            mria:register_callback(
                stop,
                fun(Arg) ->
                    Callback(Arg),
                    case erlang:fun_info(Previous, arity) of
                        {arity, 0} ->
                            Previous();
                        {arity, 1} ->
                            Previous(Arg)
                    end
                end
            )
    end.
