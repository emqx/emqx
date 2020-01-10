%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_broker_helper).

-behaviour(gen_server).

-include("logger.hrl").
-include("types.hrl").

-logger_header("[Broker Helper]").

-export([start_link/0]).

%% APIs
-export([ register_sub/2
        , lookup_subid/1
        , lookup_subpid/1
        , get_sub_shard/2
        , create_seq/1
        , reclaim_seq/1
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-define(HELPER, ?MODULE).
-define(SUBID, emqx_subid).
-define(SUBMON, emqx_submon).
-define(SUBSEQ, emqx_subseq).
-define(SHARD, 1024).

-define(BATCH_SIZE, 100000).

-spec(start_link() -> startlink_ret()).
start_link() ->
    gen_server:start_link({local, ?HELPER}, ?MODULE, [], []).

-spec(register_sub(pid(), emqx_types:subid()) -> ok).
register_sub(SubPid, SubId) when is_pid(SubPid) ->
    case ets:lookup(?SUBMON, SubPid) of
        [] ->
            gen_server:cast(?HELPER, {register_sub, SubPid, SubId});
        [{_, SubId}] ->
            ok;
        _Other ->
            error(subid_conflict)
    end.

-spec(lookup_subid(pid()) -> maybe(emqx_types:subid())).
lookup_subid(SubPid) when is_pid(SubPid) ->
    emqx_tables:lookup_value(?SUBMON, SubPid).

-spec(lookup_subpid(emqx_types:subid()) -> pid()).
lookup_subpid(SubId) ->
    emqx_tables:lookup_value(?SUBID, SubId).

-spec(get_sub_shard(pid(), emqx_topic:topic()) -> non_neg_integer()).
get_sub_shard(SubPid, Topic) ->
    case create_seq(Topic) of
        Seq when Seq =< ?SHARD -> 0;
        _ -> erlang:phash2(SubPid, shards_num()) + 1
    end.

-spec(shards_num() -> pos_integer()).
shards_num() ->
    %% Dynamic sharding later...
    ets:lookup_element(?HELPER, shards, 2).

-spec(create_seq(emqx_topic:topic()) -> emqx_sequence:seqid()).
create_seq(Topic) ->
    emqx_sequence:nextval(?SUBSEQ, Topic).

-spec(reclaim_seq(emqx_topic:topic()) -> emqx_sequence:seqid()).
reclaim_seq(Topic) ->
    emqx_sequence:reclaim(?SUBSEQ, Topic).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    %% Helper table
    ok = emqx_tables:new(?HELPER, [{read_concurrency, true}]),
    %% Shards: CPU * 32
    true = ets:insert(?HELPER, {shards, emqx_vm:schedulers() * 32}),
    %% SubSeq: Topic -> SeqId
    ok = emqx_sequence:create(?SUBSEQ),
    %% SubId: SubId -> SubPid
    ok = emqx_tables:new(?SUBID, [public, {read_concurrency, true}, {write_concurrency, true}]),
    %% SubMon: SubPid -> SubId
    ok = emqx_tables:new(?SUBMON, [public, {read_concurrency, true}, {write_concurrency, true}]),
    %% Stats timer
    ok = emqx_stats:update_interval(broker_stats, fun emqx_broker:stats_fun/0),
    {ok, #{pmon => emqx_pmon:new()}}.

handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast({register_sub, SubPid, SubId}, State = #{pmon := PMon}) ->
    true = (SubId =:= undefined) orelse ets:insert(?SUBID, {SubId, SubPid}),
    true = ets:insert(?SUBMON, {SubPid, SubId}),
    {noreply, State#{pmon := emqx_pmon:monitor(SubPid, PMon)}};

handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info({'DOWN', _MRef, process, SubPid, _Reason}, State = #{pmon := PMon}) ->
    SubPids = [SubPid | emqx_misc:drain_down(?BATCH_SIZE)],
    ok = emqx_pool:async_submit(
           fun lists:foreach/2, [fun clean_down/1, SubPids]),
    {_, PMon1} = emqx_pmon:erase_all(SubPids, PMon),
    {noreply, State#{pmon := PMon1}};

handle_info(Info, State) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    true = emqx_sequence:delete(?SUBSEQ),
    emqx_stats:cancel_update(broker_stats).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

clean_down(SubPid) ->
    case ets:lookup(?SUBMON, SubPid) of
        [{_, SubId}] ->
            true = ets:delete(?SUBMON, SubPid),
            true = (SubId =:= undefined)
                orelse ets:delete_object(?SUBID, {SubId, SubPid}),
            emqx_broker:subscriber_down(SubPid);
        [] -> ok
    end.

