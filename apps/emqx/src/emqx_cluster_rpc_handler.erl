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
-module(emqx_cluster_rpc_handler).

-behaviour(gen_server).

-include("emqx.hrl").
-include("logger.hrl").
-include("emqx_cluster_rpc.hrl").

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-define(MFA_HISTORY_LEN, 100).

start_link() ->
    gen_server:start_link(?MODULE, [], []).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

init([]) ->
    _ = emqx_misc:rand_seed(),
    {ok, ensure_timer(#{timer => undefined})}.

handle_call(Req, _From, State) ->
    ?LOG(error, "unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "unexpected msg: ~p", [Msg]),
    {noreply, State}.

handle_info({timeout, TRef, del_stale_mfa}, State = #{timer := TRef}) ->
    case ekka_mnesia:transaction(?COMMON_SHARD, fun del_stale_mfa/0, []) of
        {atomic, ok} -> ok;
        Error -> ?LOG(error, "del_stale_cluster_rpc_mfa error:~p", [Error])
    end,
    {noreply, ensure_timer(State), hibernate};

handle_info(Info, State) ->
    ?LOG(error, "unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #{timer := TRef}) ->
    emqx_misc:cancel_timer(TRef).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

-ifdef(TEST).
ensure_timer(State) ->
    State#{timer := emqx_misc:start_timer(timer:seconds(1), del_stale_mfa)}.
-else.
ensure_timer(State) ->
    Ms = timer:minutes(5) + rand:uniform(5000),
    State#{timer := emqx_misc:start_timer(Ms, del_stale_mfa)}.
-endif.


%% @doc Keep the latest completed 100 records for querying and troubleshooting.
del_stale_mfa() ->
    DoneId =
        mnesia:foldl(fun(Rec, Min) -> min(Rec#cluster_rpc_commit.tnx_id, Min) end,
            infinity, ?CLUSTER_COMMIT),
    delete_stale_mfa(mnesia:last(?CLUSTER_MFA), DoneId, ?MFA_HISTORY_LEN).

delete_stale_mfa('$end_of_table', _DoneId, _Count) -> ok;
delete_stale_mfa(CurrId, DoneId, Count) when CurrId > DoneId ->
    delete_stale_mfa(mnesia:prev(?CLUSTER_MFA, CurrId), DoneId, Count);
delete_stale_mfa(CurrId, DoneId, Count) when Count > 0 ->
    delete_stale_mfa(mnesia:prev(?CLUSTER_MFA, CurrId), DoneId, Count - 1);
delete_stale_mfa(CurrId, DoneId, Count) when Count =< 0 ->
    mnesia:delete(?CLUSTER_MFA, CurrId, write),
    delete_stale_mfa(mnesia:prev(?CLUSTER_MFA, CurrId), DoneId, Count - 1).
