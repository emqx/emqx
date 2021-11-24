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

-include_lib("include/emqx.hrl").
-include_lib("include/logger.hrl").
-include_lib("emqx_plugin_libs/include/emqx_slow_subs.hrl").

-logger_header("[SLOW Subs]").

-export([ start_link/1, on_stats_update/2, enable/0
        , disable/0, clear_history/0
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
                  , enable := boolean()
                  , last_tick_at := pos_integer()
                  }.

-type log() :: #{ rank := pos_integer()
                , clientid := emqx_types:clientid()
                , elapsed := float()
                , type := elapsed_type()
                }.

-type window_log() :: #{ last_tick_at := pos_integer()
                       , logs := [log()]
                       }.

-type message() :: #message{}.

-import(proplists, [get_value/2]).

-type stats_update_args() :: #{ clientid := emqx_types:clientid()
                              , elapsed := float()
                              , type := elapsed_type()
                              , last_insert_value := non_neg_integer()
                              , timestamp := pos_integer()
                              }.

-type stats_update_env() :: #{max_size := pos_integer()}.

-ifdef(TEST).
-define(TOPK_ACCESS, public).
-define(EXPIRE_CHECK_INTERVAL, timer:seconds(1)).
-else.
-define(TOPK_ACCESS, protected).
-define(EXPIRE_CHECK_INTERVAL, timer:seconds(10)).
-endif.

-define(NOW, erlang:system_time(millisecond)).
-define(NOTICE_TOPIC_NAME, "slow_subs").

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

-spec on_stats_update(stats_update_args(), stats_update_env()) -> true.
on_stats_update(#{clientid := ClientId,
                  elapsed := Elapsed,
                  type := Type,
                  last_insert_value := LIV,
                  timestamp := Ts},
                #{max_size := MaxSize}) ->

    Index = ?INDEX(Elapsed, ClientId),

    case ets:info(?TOPK_TAB, size) of
        Size when Size < MaxSize - 1, LIV =:= 0 ->
            %% if the size is enough and it is the first time this client try to insert, insert it directly
            ets:insert(?TOPK_TAB,
                       #top_k{index = Index, type = Type, timestamp = Ts});
        _Size ->
            case ets:first(?TOPK_TAB) of
                '$end_of_table' ->
                    %% if there are no elements, insert directly
                    ets:insert(?TOPK_TAB,
                               #top_k{index = Index, type = Type, timestamp = Ts});
                First ->
                    %% this client may already be in topk, try to delete
                    try_delete(First, LIV, ClientId),

                    case Elapsed =< First of
                        true -> true;
                        _ ->
                            %% update record, and remove the first on
                            ets:insert(?TOPK_TAB,
                                       #top_k{index = Index, type = Type, timestamp = Ts}),

                            ets:delete(ets:first(?TOPK_TAB))
                    end
            end
    end.

clear_history() ->
    gen_server:call(?MODULE, ?FUNCTION_NAME).

enable() ->
    gen_server:call(?MODULE, {enable, true}).

disable() ->
    gen_server:call(?MODULE, {enable, false}).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Conf]) ->
    erlang:process_flag(trap_exit, true),
    init_topk_tab(Conf),
    notice_tick(Conf),
    expire_tick(Conf),
    MaxSize = get_value(top_k_num, Conf),
    load(MaxSize),
    {ok, #{config => Conf,
           last_tick_at => ?NOW,
           enable => true}}.

handle_call({enable, Enable}, _From,
            #{config := Cfg, enable := IsEnable} = State) ->
    State2 = case Enable of
                 IsEnable ->
                     State;
                 true ->
                     MaxSize = get_value(max_topk_num, Cfg),
                     load(MaxSize),
                     State#{enable := true};
                 _ ->
                     unload(),
                     State#{enable := false}
             end,
    {reply, ok, State2};

handle_call(clear_history, _, State) ->
    ets:delete_all_objects(?TOPK_TAB),
    {reply, ok, State};

handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info(expire_tick, #{config := Cfg} = State) ->
    expire_tick(Cfg),
    Logs = ets:tab2list(?TOPK_TAB),
    do_clear(Cfg, Logs),
    {noreply, State};

handle_info(notice_tick, #{config := Cfg} = State) ->
    notice_tick(Cfg),
    Logs = ets:tab2list(?TOPK_TAB),
    do_notification(Logs, State),
    {noreply, State#{last_tick_at := ?NOW}};

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
expire_tick(_) ->
    erlang:send_after(?EXPIRE_CHECK_INTERVAL, self(), ?FUNCTION_NAME).

notice_tick(Cfg) ->
    case get_value(notice_interval, Cfg) of
        0 ->
            ok;
        Interval ->
            erlang:send_after(Interval, self(), ?FUNCTION_NAME)
    end.

init_topk_tab(_) ->
    ?TOPK_TAB = ets:new(?TOPK_TAB, [ ordered_set, ?TOPK_ACCESS, named_table
                                   , {keypos, #top_k.index}, {write_concurrency, true}
                                   , {read_concurrency, true}
                                   ]).

-spec do_notification(list(), state()) -> ok.
do_notification([], _) ->
    ok;

do_notification(Logs, #{last_tick_at := LastTickTime, config := Cfg}) ->
    start_publish(Logs, LastTickTime, Cfg),
    ok.

start_publish(Logs, TickTime, Cfg) ->
    emqx_pool:async_submit({fun do_publish/4, [Logs, erlang:length(Logs), TickTime, Cfg]}).

do_publish([], _, _, _) ->
    ok;

do_publish(Logs, Rank, TickTime, Cfg) ->
    BatchSize = get_value(notice_batch_size, Cfg),
    do_publish(Logs, BatchSize, Rank, TickTime, Cfg, []).

do_publish([Log | T], Size, Rank, TickTime, Cfg, Cache) when Size > 0 ->
    Cache2 = [convert_to_notice(Rank, Log) | Cache],
    do_publish(T, Size - 1, Rank - 1, TickTime, Cfg, Cache2);

do_publish(Logs, Size, Rank, TickTime, Cfg, Cache) when Size =:= 0 ->
    publish(TickTime, Cfg, Cache),
    do_publish(Logs, Rank, TickTime, Cfg);

do_publish([], _, _Rank, TickTime, Cfg, Cache) ->
    publish(TickTime, Cfg, Cache),
    ok.

convert_to_notice(Rank, #top_k{index = ?INDEX(Elapsed, ClientId),
                               type = Type,
                               timestamp = Ts}) ->
    #{rank => Rank,
      clientid => ClientId,
      elapsed => Elapsed,
      type => Type,
      timestamp => Ts}.

publish(TickTime, Cfg, Notices) ->
    WindowLog = #{last_tick_at => TickTime,
                  logs => lists:reverse(Notices)},
    Payload = emqx_json:encode(WindowLog),
    _ = emqx:publish(#message{ id = emqx_guid:gen()
                             , qos = get_value(notice_qos, Cfg)
                             , from = ?MODULE
                             , topic = emqx_topic:systop(?NOTICE_TOPIC_NAME)
                             , payload = Payload
                             , timestamp = ?NOW
                             }),
    ok.

load(MaxSize) ->
    _ = emqx:hook('message.slow_subs_stats',
                  fun ?MODULE:on_stats_update/2,
                  [#{max_size => MaxSize}]),
    ok.

unload() ->
    emqx:unhook('message.slow_subs_stats', fun ?MODULE:on_stats_update/2).

try_delete(First, LIV, _) when First > LIV ->
    true;

try_delete(_, LIV, ClientId) ->
    ets:delete(?INDEX(LIV, ClientId)).

do_clear(Cfg, Logs) ->
    Now = ?NOW,
    Interval = get_value(expire_interval, Cfg),
    Each = fun(#top_k{index = Index, timestamp = Ts}) ->
               case Now - Ts >= Interval of
                   true ->
                       ets:delete(?TOPK_TAB, Index);
                   _ ->
                       true
               end
           end,
    lists:foreach(Each, Logs).
