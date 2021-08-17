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
-export([multicall/3, query/1, reset/0, status/0]).

-export([init/1, format_status/2, handle_event/4, terminate/3,
    code_change/4, callback_mode/0]).

%%-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
%%-endif.

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

-include("emqx.hrl").
-include("logger.hrl").
-define(CLUSTER_MFA, cluster_rpc_mfa).
-define(CLUSTER_CURSOR, cluster_rpc_cursor).

-rlog_shard({?COMMON_SHARD, ?CLUSTER_MFA}).
-rlog_shard({?COMMON_SHARD, ?CLUSTER_CURSOR}).

-define(CATCH_UP, catch_up).
-define(IDLE, idle).
-define(MFA_HISTORY_LEN, 100).
-define(DEL_MFA_AFTER(_Time1_), {timeout, _Time1_ * 1000, del_stale_mfa}).
-define(CATCH_UP_AFTER(_Time_), {state_timeout, _Time_ * 1000, catch_up_delay}).

-record(cluster_rpc_mfa, {tnx_id :: pos_integer(), mfa :: mfa(),
    created_at :: calendar:datetime(), initiator :: node()}).
-record(cluster_rpc_cursor, {node :: node(), tnx_id :: pos_integer()}).

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
    ok = ekka_mnesia:create_table(?CLUSTER_CURSOR, [
        {type, set},
        {disc_copies, [node()]},
        {local_content, true},
        {record_name, cluster_rpc_cursor},
        {attributes, record_info(fields, cluster_rpc_cursor)}]);
mnesia(copy) ->
    ok = ekka_mnesia:copy_table(cluster_rpc_mfa, disc_copies),
    ok = ekka_mnesia:copy_table(cluster_rpc_cursor, disc_copies).

start_link() ->
    gen_statem:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec multicall(Module, Function, Args) -> {ok, TnxId} |{error, Reason} when
    Module :: module(),
    Function :: atom(),
    Args :: [term()],
    TnxId :: pos_integer(),
    Reason :: term().
multicall(M, F, A) ->
    case ekka_rlog:core_nodes() of
        [] -> {error, "core_nodes is empty"};
        [Core | _] = CoreNodes ->
            case lists:member(node(), CoreNodes) of
                true -> gen_server:call(?MODULE, {commit, {M, F, A}}, 2 * 60 * 1000);
                false -> gen_server:call({?MODULE, Core}, {commit, {M, F, A}}, 2 * 60 * 1000)
            end
    end.

-spec query(pos_integer()) -> {'atomic', map()} | {'aborted', Reason :: term()}.
query(TnxId) ->
    Fun = fun() ->
        case mnesia:read(?CLUSTER_MFA, TnxId) of
            [] -> mnesia:abort("not_found");
            [#cluster_rpc_mfa{mfa = MFA, initiator = InitNode, created_at = CreatedAt}] ->
                #{mfa => MFA, initiator => InitNode, created_at => CreatedAt}
        end
          end,
    transaction(Fun).

-spec reset() -> reset.
reset() -> erlang:send(?MODULE, reset).

-spec status() -> {'atomic', [map()]} | {'aborted', Reason :: term()}.
status() ->
    Fun = fun() ->
        mnesia:foldl(fun(Rec, Acc) ->
            #cluster_rpc_cursor{node = Node, tnx_id = TnxId} = Rec,
            [MFARec] = mnesia:read(?CLUSTER_MFA, TnxId),
            #cluster_rpc_mfa{mfa = MFA, initiator = InitNode, created_at = CreatedAt} = MFARec,
            [#{
                node => Node,
                tnx_id => TnxId,
                initiator => InitNode,
                mfa => MFA,
                created_at => CreatedAt
            } | Acc]
                     end, [], ?CLUSTER_CURSOR)
          end,
    transaction(Fun).

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

%% @private
init([]) ->
    {ok, _Node} = mnesia:subscribe({table, ?CLUSTER_MFA, simple}),
    _ = emqx_misc:rand_seed(),
    {ok, ?CATCH_UP, {}, ?CATCH_UP_AFTER(0)}.

callback_mode() ->
    handle_event_function.

%% @private
format_status(Opt, [_PDict, StateName, Data]) ->
    #{state => StateName, data => Data, opt => Opt}.

%% @private
handle_event(state_timeout, catch_up_delay, _State, Data) ->
    catch_up(node(), Data);

handle_event(timeout, del_stale_mfa, ?IDLE, _Data) ->
    transaction(fun del_stale_mfa/0),
    {keep_state_and_data, [?CATCH_UP_AFTER(10 * 60)]};

handle_event(info, {mnesia_table_event, {write, MFARec, _ActivityId}}, ?IDLE, Data) ->
    handle_mfa_write_event(MFARec, Data);
handle_event(info, {mnesia_table_event, {write, _MFARec, _ActivityId}}, ?CATCH_UP, _Data) ->
    {keep_state_and_data, [?CATCH_UP_AFTER(1)]};

handle_event(info, reset, _State, _Data) ->
    _ = mnesia:clear_table(?CLUSTER_CURSOR),
    _ = mnesia:clear_table(?CLUSTER_MFA),
    {keep_state_and_data, [?CATCH_UP_AFTER(1)]};

handle_event({call, From}, {commit, MFA}, ?IDLE, Data) ->
    Node = node(),
    case transaction(fun() -> init_mfa(Node, MFA) end) of
        {atomic, {ok, TnxId}} ->
            {keep_state, Data, [{reply, From, {ok, TnxId}}, ?DEL_MFA_AFTER(5 * 60)]};
        {aborted, Reason} ->
            {keep_state, Data, [{reply, From, {error, Reason}}, ?DEL_MFA_AFTER(5 * 60)]}
    end;
handle_event({call, From}, {commit, _MFA}, ?CATCH_UP, Data) ->
    case catch_up(node(), Data) of
        {next_state, ?IDLE, Data, _Actions} ->
            {next_state, ?IDLE, Data, [{postpone, true}]};
        _ ->
            Reason = "There are still transactions that have not been executed.",
            {keep_state, Data, [{reply, From, {error, Reason}}, ?CATCH_UP_AFTER(1)]}
    end;

handle_event(_EventType, _EventContent, _StateName, _Data) ->
    {keep_state_and_data, [?DEL_MFA_AFTER(5 + rand:uniform(10))]}.

terminate(_Reason, _StateName, _Data) ->
    ok.

code_change(_OldVsn, StateName, Data, _Extra) ->
    {ok, StateName, Data}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
catch_up(Node, Data) ->
    case get_next_mfa(Node) of
        {atomic, caught_up} -> {next_state, ?IDLE, Data, [?DEL_MFA_AFTER(5 * 60)]};
        {atomic, {still_lagging, NextId, MFA}} ->
            case apply_mfa(MFA) of
                ok ->
                    Fun = fun() ->
                        Rec = #cluster_rpc_cursor{node = Node, tnx_id = NextId},
                        mnesia:write(?CLUSTER_CURSOR, Rec, write)
                          end,
                    case transaction(Fun) of
                        {atomic, ok} -> catch_up(Node, Data);
                        _ -> {next_state, ?CATCH_UP, Data, [?CATCH_UP_AFTER(1)]}
                    end;
                _ -> {next_state, ?CATCH_UP, Data, [?CATCH_UP_AFTER(1)]}
            end;
        {aborted, _Reason} -> {next_state, ?CATCH_UP, Data, [?CATCH_UP_AFTER(1)]}
    end.

get_next_mfa(Node) ->
    Fun =
        fun() ->
            case mnesia:read(?CLUSTER_CURSOR, Node) of
                [] ->
                    LatestId = get_latest_id(),
                    mnesia:write(?CLUSTER_CURSOR, #cluster_rpc_cursor{node = Node, tnx_id = LatestId}, write),
                    caught_up;
                [#cluster_rpc_cursor{tnx_id = DoneTnxId}] ->
                    case mnesia:read(?CLUSTER_MFA, DoneTnxId + 1) of
                        [] -> caught_up;
                        [#cluster_rpc_mfa{mfa = MFA}] -> {still_lagging, DoneTnxId + 1, MFA}
                    end
            end
        end,
    transaction(Fun).

do_catch_up(ToTnxId, Node) ->
    case mnesia:wread({?CLUSTER_CURSOR, Node}) of
        [] ->
            Rec = #cluster_rpc_cursor{tnx_id = ToTnxId, node = Node},
            mnesia:write(?CLUSTER_CURSOR, Rec, write),
            caught_up;
        [#cluster_rpc_cursor{tnx_id = DoneTnxId}] when ToTnxId =< DoneTnxId -> caught_up;
        [Rec = #cluster_rpc_cursor{tnx_id = DoneTnxId}] ->
            CurTnxId = DoneTnxId + 1,
            [#cluster_rpc_mfa{mfa = MFA}] = mnesia:read(?CLUSTER_MFA, CurTnxId),
            mnesia:write(?CLUSTER_CURSOR, Rec#cluster_rpc_cursor{tnx_id = CurTnxId}, write),
            apply_mfa(MFA)
    end.

get_latest_id() ->
    case mnesia:last(?CLUSTER_MFA) of
        '$end_of_table' -> 0;
        Id -> Id
    end.

handle_mfa_write_event(#cluster_rpc_mfa{tnx_id = TnxId, mfa = MFA}, Data) ->
    Node = node(),
    case transaction(fun() -> get_done_id(Node, TnxId - 1) end) of
        {atomic, DoneTnxId} when DoneTnxId =:= TnxId - 1 ->
            case apply_mfa(MFA) of
                ok ->
                    Trans = fun() ->
                        Rec = #cluster_rpc_cursor{tnx_id = TnxId, node = Node},
                        mnesia:write(?CLUSTER_CURSOR, Rec, write)
                            end,
                    case transaction(Trans) of
                        {atomic, ok} -> {next_state, ?IDLE, Data, [?DEL_MFA_AFTER(1 * 60)]};
                        _ -> {next_state, ?CATCH_UP, Data, [?CATCH_UP_AFTER(1)]}
                    end;
                _ -> {next_state, ?CATCH_UP, Data, [?CATCH_UP_AFTER(1)]}
            end;
        {atomic, _DoneTnxId} -> {next_state, ?CATCH_UP, Data, [?CATCH_UP_AFTER(0)]};
        _ -> {next_state, ?CATCH_UP, Data, [?CATCH_UP_AFTER(1)]}
    end.

get_done_id(Node, Default) ->
    case mnesia:wread({?CLUSTER_CURSOR, Node}) of
        [#cluster_rpc_cursor{tnx_id = TnxId}] -> TnxId;
        [] -> Default
    end.

init_mfa(Node, MFA) ->
    mnesia:write_lock_table(?CLUSTER_MFA),
    LatestId = get_latest_id(),
    ok = do_catch_up_in_one_trans(LatestId, Node),
    TnxId = LatestId + 1,
    mnesia:write(?CLUSTER_CURSOR, #cluster_rpc_cursor{node = Node, tnx_id = TnxId}, write),
    mnesia:write(?CLUSTER_MFA, #cluster_rpc_mfa{tnx_id = TnxId, mfa = MFA,
        initiator = Node, created_at = erlang:localtime()}, write),
    case apply_mfa(MFA) of
        ok -> {ok, TnxId};
        Other -> mnesia:abort(Other)
    end.

do_catch_up_in_one_trans(LatestId, Node) ->
    case do_catch_up(LatestId, Node) of
        caught_up -> ok;
        ok -> do_catch_up_in_one_trans(LatestId, Node);
        _ -> mnesia:abort("catch up failed")
    end.

%% @doc Keep the latest completed 100 records for querying and troubleshooting.
del_stale_mfa() ->
    DoneId =
        mnesia:foldl(fun(Rec, Min) -> min(Rec#cluster_rpc_cursor.tnx_id, Min) end,
            infinity, ?CLUSTER_CURSOR),
    delete_stale_mfa(mnesia:last(?CLUSTER_MFA), DoneId, ?MFA_HISTORY_LEN).

delete_stale_mfa('$end_of_table', _DoneId, _Count) -> ok;
delete_stale_mfa(CurrId, DoneId, Count) when CurrId > DoneId ->
    delete_stale_mfa(mnesia:prev(?CLUSTER_MFA, CurrId), DoneId, Count);
delete_stale_mfa(CurrId, DoneId, Count) when Count > 0 ->
    delete_stale_mfa(mnesia:prev(?CLUSTER_MFA, CurrId), DoneId, Count - 1);
delete_stale_mfa(CurrId, DoneId, Count) when Count =< 0 ->
    mnesia:delete(?CLUSTER_MFA, CurrId, write),
    delete_stale_mfa(mnesia:prev(?CLUSTER_MFA, CurrId), DoneId, Count - 1).

transaction(Fun) ->
    ekka_mnesia:transaction(?COMMON_SHARD, Fun).

apply_mfa({M, F, A}) ->
    ?LOG(warning, "Apply MFA: ~p~n", [{M, F, A}]),
    try erlang:apply(M, F, A)
    catch E:R -> {error, {E, R}}
    end.
