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
-module(emqx_cluster_rpc_handler).

-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").
-include("emqx_conf.hrl").

-export([start_link/0, start_link/2]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

start_link() ->
    MaxHistory = emqx_conf:get(["node", "cluster_call", "max_history"], 100),
    CleanupMs = emqx_conf:get(["node", "cluster_call", "cleanup_interval"], 5 * 60 * 1000),
    start_link(MaxHistory, CleanupMs).

start_link(MaxHistory, CleanupMs) ->
    State = #{max_history => MaxHistory, cleanup_ms => CleanupMs, timer => undefined},
    gen_server:start_link(?MODULE, [State], []).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

init([State]) ->
    erlang:process_flag(trap_exit, true),
    {ok, ensure_timer(State)}.

handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_msg", cast => Msg}),
    {noreply, State}.

handle_info({timeout, TRef, del_stale_mfa}, State = #{timer := TRef, max_history := MaxHistory}) ->
    case mria:transaction(?CLUSTER_RPC_SHARD, fun del_stale_mfa/1, [MaxHistory]) of
        {atomic, ok} -> ok;
        Error -> ?SLOG(error, #{msg => "del_stale_cluster_rpc_mfa_error", error => Error})
    end,
    {noreply, ensure_timer(State), hibernate};
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

terminate(_Reason, #{timer := TRef}) ->
    emqx_misc:cancel_timer(TRef).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
ensure_timer(State = #{cleanup_ms := Ms}) ->
    State#{timer := emqx_misc:start_timer(Ms, del_stale_mfa)}.

%% @doc Keep the latest completed 100 records for querying and troubleshooting.
del_stale_mfa(MaxHistory) ->
    DoneId =
        mnesia:foldl(
            fun(Rec, Min) -> min(Rec#cluster_rpc_commit.tnx_id, Min) end,
            infinity,
            ?CLUSTER_COMMIT
        ),
    delete_stale_mfa(mnesia:last(?CLUSTER_MFA), DoneId, MaxHistory).

delete_stale_mfa('$end_of_table', _DoneId, _Count) ->
    ok;
delete_stale_mfa(CurrId, DoneId, Count) when CurrId > DoneId ->
    delete_stale_mfa(mnesia:prev(?CLUSTER_MFA, CurrId), DoneId, Count);
delete_stale_mfa(CurrId, DoneId, Count) when Count > 0 ->
    delete_stale_mfa(mnesia:prev(?CLUSTER_MFA, CurrId), DoneId, Count - 1);
delete_stale_mfa(CurrId, DoneId, Count) when Count =< 0 ->
    mnesia:delete(?CLUSTER_MFA, CurrId, write),
    delete_stale_mfa(mnesia:prev(?CLUSTER_MFA, CurrId), DoneId, Count - 1).
