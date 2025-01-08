%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include("emqx_router.hrl").
-include("emqx_shared_sub.hrl").
-include("logger.hrl").
-include("types.hrl").

-export([start_link/0]).

%% APIs
-export([
    register_sub/2,
    lookup_subid/1,
    lookup_subpid/1,
    get_sub_shard/2,
    create_seq/1,
    reclaim_seq/1
]).

%% Stats fun
-export([stats_fun/0]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
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

-spec start_link() -> startlink_ret().
start_link() ->
    gen_server:start_link({local, ?HELPER}, ?MODULE, [], []).

-spec register_sub(pid(), emqx_types:subid()) -> ok.
register_sub(SubPid, SubId) when is_pid(SubPid) ->
    case ets:lookup(?SUBMON, SubPid) of
        [] ->
            gen_server:cast(?HELPER, {register_sub, SubPid, SubId});
        [{_, SubId}] ->
            ok;
        _Other ->
            error(subid_conflict)
    end.

-spec lookup_subid(pid()) -> option(emqx_types:subid()).
lookup_subid(SubPid) when is_pid(SubPid) ->
    emqx_utils_ets:lookup_value(?SUBMON, SubPid).

-spec lookup_subpid(emqx_types:subid()) -> option(pid()).
lookup_subpid(SubId) ->
    emqx_utils_ets:lookup_value(?SUBID, SubId).

-spec get_sub_shard(pid(), emqx_types:topic()) -> non_neg_integer().
get_sub_shard(SubPid, Topic) ->
    case create_seq(Topic) of
        Seq when Seq =< ?SHARD -> 0;
        _ -> erlang:phash2(SubPid, shards_num()) + 1
    end.

-spec shards_num() -> pos_integer().
shards_num() ->
    %% Dynamic sharding later...
    ets:lookup_element(?HELPER, shards, 2).

-spec create_seq(emqx_types:topic()) -> emqx_sequence:seqid().
create_seq(Topic) ->
    emqx_sequence:nextval(?SUBSEQ, Topic).

-spec reclaim_seq(emqx_types:topic()) -> emqx_sequence:seqid().
reclaim_seq(Topic) ->
    emqx_sequence:reclaim(?SUBSEQ, Topic).

%%--------------------------------------------------------------------
%% Stats fun
%%--------------------------------------------------------------------

stats_fun() ->
    safe_update_stats(subscriber_val(), 'subscribers.count', 'subscribers.max'),
    safe_update_stats(subscription_count(), 'subscriptions.count', 'subscriptions.max'),
    safe_update_stats(
        durable_subscription_count(),
        'durable_subscriptions.count',
        'durable_subscriptions.max'
    ),
    safe_update_stats(table_size(?SUBOPTION), 'suboptions.count', 'suboptions.max').

safe_update_stats(undefined, _Stat, _MaxStat) ->
    ok;
safe_update_stats(Val, Stat, MaxStat) when is_integer(Val) ->
    emqx_stats:setstat(Stat, MaxStat, Val).

%% N.B.: subscriptions from durable sessions are not tied to any particular node.
%% Therefore, do not sum them with node-local subscriptions.
subscription_count() ->
    table_size(?SUBSCRIPTION).

durable_subscription_count() ->
    emqx_persistent_session_bookkeeper:get_subscription_count().

subscriber_val() ->
    sum_subscriber(table_size(?SUBSCRIBER), table_size(?SHARED_SUBSCRIBER)).

sum_subscriber(undefined, undefined) -> undefined;
sum_subscriber(undefined, V2) when is_integer(V2) -> V2;
sum_subscriber(V1, undefined) when is_integer(V1) -> V1;
sum_subscriber(V1, V2) when is_integer(V1), is_integer(V2) -> V1 + V2.

table_size(Tab) when is_atom(Tab) -> ets:info(Tab, size).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    %% Helper table
    ok = emqx_utils_ets:new(?HELPER, [{read_concurrency, true}]),
    %% Shards: CPU * 32
    true = ets:insert(?HELPER, {shards, emqx_vm:schedulers() * 32}),
    %% SubSeq: Topic -> SeqId
    ok = emqx_sequence:create(?SUBSEQ),
    %% SubId: SubId -> SubPid
    ok = emqx_utils_ets:new(?SUBID, [public, {read_concurrency, true}, {write_concurrency, true}]),
    %% SubMon: SubPid -> SubId
    ok = emqx_utils_ets:new(?SUBMON, [public, {read_concurrency, true}, {write_concurrency, true}]),
    %% Stats timer
    ok = emqx_stats:update_interval(broker_stats, fun ?MODULE:stats_fun/0),
    {ok, #{pmon => emqx_pmon:new()}}.

handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast({register_sub, SubPid, SubId}, State = #{pmon := PMon}) ->
    true = (SubId =:= undefined) orelse ets:insert(?SUBID, {SubId, SubPid}),
    true = ets:insert(?SUBMON, {SubPid, SubId}),
    {noreply, State#{pmon := emqx_pmon:monitor(SubPid, PMon)}};
handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, State}.

handle_info({'DOWN', _MRef, process, SubPid, _Reason}, State = #{pmon := PMon}) ->
    SubPids = [SubPid | emqx_utils:drain_down(?BATCH_SIZE)],
    ok = emqx_pool:async_submit(
        fun lists:foreach/2, [fun clean_down/1, SubPids]
    ),
    {_, PMon1} = emqx_pmon:erase_all(SubPids, PMon),
    {noreply, State#{pmon := PMon1}};
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
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
    try
        case ets:lookup(?SUBMON, SubPid) of
            [{_, SubId}] ->
                true = ets:delete(?SUBMON, SubPid),
                true =
                    (SubId =:= undefined) orelse
                        ets:delete_object(?SUBID, {SubId, SubPid}),
                emqx_broker:subscriber_down(SubPid);
            [] ->
                ok
        end
    catch
        error:badarg -> ok
    end.
