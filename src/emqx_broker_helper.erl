%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_broker_helper).

-behaviour(gen_server).

-compile({no_auto_import, [monitor/2]}).

-export([start_link/0]).
-export([monitor/2]).
-export([get_shared/2]).
-export([create_seq/1, reclaim_seq/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(HELPER, ?MODULE).
-define(SUBMON, emqx_submon).
-define(SUBSEQ, emqx_subseq).

-record(state, {pmon :: emqx_pmon:pmon()}).

-spec(start_link() -> emqx_types:startlink_ret()).
start_link() ->
    gen_server:start_link({local, ?HELPER}, ?MODULE, [], []).

-spec(monitor(pid(), emqx_types:subid()) -> ok).
monitor(SubPid, SubId) when is_pid(SubPid) ->
    case ets:lookup(?SUBMON, SubPid) of
        [] ->
            gen_server:cast(?HELPER, {monitor, SubPid, SubId});
        [{_, SubId}] ->
            ok;
        _Other ->
            error(subid_conflict)
    end.

-spec(get_shared(pid(), emqx_topic:topic()) -> non_neg_integer()).
get_shared(SubPid, Topic) ->
    case create_seq(Topic) of
        Seq when Seq =< 1024 -> 0;
        _Seq -> erlang:phash2(SubPid, ets:lookup_element(?SUBSEQ, shareds, 2))
    end.

-spec(create_seq(emqx_topic:topic()) -> emqx_sequence:seqid()).
create_seq(Topic) ->
    emqx_sequence:nextval(?SUBSEQ, Topic).

-spec(reclaim_seq(emqx_topic:topic()) -> emqx_sequence:seqid()).
reclaim_seq(Topic) ->
    emqx_sequence:reclaim(?SUBSEQ, Topic).

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init([]) ->
    %% SubSeq: Topic -> SeqId
    ok = emqx_sequence:create(?SUBSEQ),
    %% Shareds: CPU * 32
    true = ets:insert(?SUBSEQ, {shareds, emqx_vm:schedulers() * 32}),
    %% SubMon: SubPid -> SubId
    ok = emqx_tables:new(?SUBMON, [set, protected, {read_concurrency, true}]),
    %% Stats timer
    emqx_stats:update_interval(broker_stats, fun emqx_broker:stats_fun/0),
    {ok, #state{pmon = emqx_pmon:new()}, hibernate}.

handle_call(Req, _From, State) ->
    emqx_logger:error("[BrokerHelper] unexpected call: ~p", [Req]),
   {reply, ignored, State}.

handle_cast({monitor, SubPid, SubId}, State = #state{pmon = PMon}) ->
    true = ets:insert(?SUBMON, {SubPid, SubId}),
    {noreply, State#state{pmon = emqx_pmon:monitor(SubPid, PMon)}};

handle_cast(Msg, State) ->
    emqx_logger:error("[BrokerHelper] unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info({'DOWN', _MRef, process, SubPid, _Reason}, State = #state{pmon = PMon}) ->
    true = ets:delete(?SUBMON, SubPid),
    ok = emqx_pool:async_submit(fun emqx_broker:subscriber_down/1, [SubPid]),
    {noreply, State#state{pmon = emqx_pmon:erase(SubPid, PMon)}};

handle_info(Info, State) ->
    emqx_logger:error("[BrokerHelper] unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #state{}) ->
    _ = emqx_sequence:delete(?SUBSEQ),
    emqx_stats:cancel_update(broker_stats).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

