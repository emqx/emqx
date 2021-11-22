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

-module(emqx_slow_subs).

-behaviour(gen_server).

-include("include/emqx.hrl").
-include("include/logger.hrl").
-include("include/emqx_mqtt.hrl").
-include_lib("emqx_plugin_libs/include/emqx_slow_subs.hrl").

-logger_header("[SLOW SUBs]").

-export([ start_link/1, on_stats_update/4, clear_history/0
        , get_stats_interval/0
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-compile(nowarn_unused_type).

-type state() :: #{ config := proplist:proplist()
                  }.

-type message() :: #message{}.

-import(proplists, [get_value/2]).

-define(NOW, erlang:system_time(millisecond)).
-define(DEFAULT_STATS_INTEVAL, timer:seconds(60)).
-define(STATS_CONF_KEY, emqx_slow_subs_interval).

-type slow_subs_stats_env() :: #{ threshold := pos_integer()
                                , max_size := pos_integer()
                                }.

-ifdef(TEST).
-define(TOPK_ACCESS, public).
-else.
-define(TOPK_ACCESS, protected).
-endif.

%% erlang term order
%% number < atom < reference < fun < port < pid < tuple < list < bit string
%% ets ordered_set is ascending by term order

%%--------------------------------------------------------------------
%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------
%% @doc Start the st_statistics
-spec(start_link(Env :: list()) -> emqx_types:startlink_ret()).
start_link(Env) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Env], []).

-spec on_stats_update(emqx_slow_subs_stats:stats(),
                      slow_subs_stats_args(),
                      index(),
                      slow_subs_stats_env()) -> ok.

on_stats_update(#{ratio := Ratio, index := Index},
                _Args,
                Result,
                #{threshold := Threshold})
  when Ratio > Threshold ->
    try_delete_stats(Index),
    Result;

%% it may be better to track the ets's size and the first value
%% but the code will become more complicated
on_stats_update(#{ratio := Ratio, index := Index} = Stats,
                Args,
                Result,
                #{max_size := MaxSize}) ->

    try_delete_stats(Index),

    case ets:info(?TOPK_TAB, size) of
        Size when Size < MaxSize ->
            insert(Stats, Args);
        _ ->
            case ets:last(?TOPK_TAB) of
                Last when Last =< Ratio ->
                    Result;
                _ ->
                    %% here, we should insert first, and then delete the last one
                    %% example, if ets keys are x1 x2 x3, now the A and B want to insert
                    %% if we delete first, maybe will delete the x3 and x2, this is not what we expected
                    NewIndex = insert(Stats, Args),
                    ets:delete(ets:last(?TOPK_TAB)),
                    NewIndex
            end
    end.

clear_history() ->
    gen_server:call(?MODULE, ?FUNCTION_NAME).

get_stats_interval() ->
    persistent_term:get(?STATS_CONF_KEY, ?DEFAULT_STATS_INTEVAL).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------
init([Env]) ->
    erlang:process_flag(trap_exit, true),
    init_topk_tab(Env),
    Threshold = calc_threshold(Env),
    MaxSize = get_value(max_size, Env),
    Interval = get_value(interval, Env),
    update_stats_interval(Interval),
    load(Threshold, MaxSize),
    {ok, #{config => Env}}.

handle_call(clear_history, _, State) ->
    ets:delete_all_objects(?TOPK_TAB),
    {reply, ok, State};

handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info(Info, State) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _) ->
    unload(),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
init_topk_tab(_) ->
    ?TOPK_TAB = ets:new(?TOPK_TAB, [ ordered_set, ?TOPK_ACCESS, named_table
                                   , {keypos, #slow_sub.index},
                                     {write_concurrency, true}
                                   , {read_concurrency, true}
                                   ]).

-spec insert(emqx_slow_subs_stats:stats(), slow_subs_stats_args()) -> index().
insert(#{basis := Basis} = Stats, Args) ->
    ClientId = emqx_channel:info(clientid, Args),
    Index = ?MAKE_INDEX(Basis, ClientId),
    Session = emqx_channel:get_session(Args),
    MQueueLen = emqx_session:info(mqueue_len, Session),
    InFlight = emqx_session:info(inflight, Session),
    InFlightLen = emqx_inflight:size(InFlight),
    InFlightVals = emqx_inflight:values(InFlight),
    {Ack, Rec, Comp} = stats_wait(InFlightVals, 0, 0, 0),
    Record = #slow_sub{ index = Index
                      , last_stats = emqx_slow_subs_stats:to_summary(Stats)
                      , mqueue_len = MQueueLen
                      , inflight_len = InFlightLen
                      , wait_ack = Ack
                      , wait_rec = Rec
                      , wait_comp = Comp
                      },
    ets:insert(?TOPK_TAB, Record),
    Index.

load(Threshold, MaxSize) ->
    _ = emqx:hook('message.slow_subs_stats',
                  fun ?MODULE:on_stats_update/4,
                  [#{threshold => Threshold,
                     max_size => MaxSize}
                  ]),
    ok.

unload() ->
    emqx:unhook('message.slow_subs_stats', fun ?MODULE:on_stats_update/4).

try_delete_stats(undefined) ->
    true;
try_delete_stats(Key) ->
    ets:delete(?TOPK_TAB, Key).

stats_wait([{pubrel, _} | T], Ack, Rec, Comp) ->
    stats_wait(T, Ack, Rec, Comp + 1);

stats_wait([{Msg, _} | T], Ack, Rec, Comp) ->
    QOS = emqx_message:qos(Msg),
    case QOS of
        ?QOS_1 ->
            stats_wait(T, Ack + 1, Rec, Comp);
        _ ->
            stats_wait(T, Ack, Rec + 1, Comp)
    end;

stats_wait([], Ack, Rec, Comp) ->
    {Ack, Rec, Comp}.

update_stats_interval(Interval) ->
    persistent_term:put(?STATS_CONF_KEY, Interval).

%% use the basis point of the reciprocal of the congestion degree as the threshold
%% because topk is sorted in ascending order
%% so using the reciprocal can be more convenient for querying
calc_threshold(Conf) ->
    Congestion = get_value(congestion, Conf),
    erlang:floor(?BASE_FACTOR / (Congestion / 100)).
