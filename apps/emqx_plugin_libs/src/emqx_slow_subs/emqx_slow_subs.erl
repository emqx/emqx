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
        , disable/0, clear_history/0, init_topk_tab/0
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
                , latency := non_neg_integer()
                , type := emqx_message_latency_stats:latency_type()
                }.

-type window_log() :: #{ last_tick_at := pos_integer()
                       , logs := [log()]
                       }.

-type message() :: #message{}.

-import(proplists, [get_value/2]).

-type stats_update_args() :: #{ clientid := emqx_types:clientid()
                              , latency := non_neg_integer()
                              , type := emqx_message_latency_stats:latency_type()
                              , last_insert_value := non_neg_integer()
                              , update_time := timer:time()
                              }.

-type stats_update_env() :: #{max_size := pos_integer()}.

-ifdef(TEST).
-define(EXPIRE_CHECK_INTERVAL, timer:seconds(1)).
-else.
-define(EXPIRE_CHECK_INTERVAL, timer:seconds(10)).
-endif.

-define(NOW, erlang:system_time(millisecond)).
-define(NOTICE_TOPIC_NAME, "slow_subs").
-define(DEF_CALL_TIMEOUT, timer:seconds(10)).

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

%% XXX NOTE:pay attention to the performance here
-spec on_stats_update(stats_update_args(), stats_update_env()) -> true.
on_stats_update(#{clientid := ClientId,
                  latency := Latency,
                  type := Type,
                  last_insert_value := LIV,
                  update_time := Ts},
                #{max_size := MaxSize}) ->

    LastIndex = ?INDEX(LIV, ClientId),
    Index = ?INDEX(Latency, ClientId),

    %% check whether the client is in the table
    case ets:lookup(?TOPK_TAB, LastIndex) of
        [#top_k{index = Index}] ->
            %% if last value == the new value, return
            true;
        [_] ->
            %% if Latency > minimum value, we should update it
            %% if Latency < minimum value, maybe it can replace the minimum value
            %% so alwyas update at here
            %% do we need check if Latency == minimum ???
            ets:insert(?TOPK_TAB,
                       #top_k{index = Index, type = Type, last_update_time = Ts}),
            ets:delete(?TOPK_TAB, LastIndex);
        [] ->
            %% try to insert
            try_insert_to_topk(MaxSize, Index, Latency, Type, Ts)
    end.

clear_history() ->
    gen_server:call(?MODULE, ?FUNCTION_NAME, ?DEF_CALL_TIMEOUT).

enable() ->
    gen_server:call(?MODULE, {enable, true}, ?DEF_CALL_TIMEOUT).

disable() ->
    gen_server:call(?MODULE, {enable, false}, ?DEF_CALL_TIMEOUT).

init_topk_tab() ->
    case ets:whereis(?TOPK_TAB) of
        undefined ->
            ?TOPK_TAB = ets:new(?TOPK_TAB,
                                [ ordered_set, public, named_table
                                , {keypos, #top_k.index}, {write_concurrency, true}
                                , {read_concurrency, true}
                                ]);
        _ ->
            ?TOPK_TAB
    end.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Conf]) ->
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
        0 -> ok;
        Interval ->
            erlang:send_after(Interval, self(), ?FUNCTION_NAME),
            ok
    end.

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

convert_to_notice(Rank, #top_k{index = ?INDEX(Latency, ClientId),
                               type = Type,
                               last_update_time = Ts}) ->
    #{rank => Rank,
      clientid => ClientId,
      latency => Latency,
      type => Type,
      timestamp => Ts}.

publish(TickTime, Cfg, Notices) ->
    WindowLog = #{last_tick_at => TickTime,
                  logs => lists:reverse(Notices)},
    Payload = emqx_json:encode(WindowLog),
    Msg = #message{ id = emqx_guid:gen()
                  , qos = get_value(notice_qos, Cfg)
                  , from = ?MODULE
                  , topic = emqx_topic:systop(?NOTICE_TOPIC_NAME)
                  , payload = Payload
                  , timestamp = ?NOW
                  },
    _ = emqx_broker:safe_publish(Msg),
    ok.

load(MaxSize) ->
    _ = emqx:hook('message.slow_subs_stats',
                  fun ?MODULE:on_stats_update/2,
                  [#{max_size => MaxSize}]),
    ok.

unload() ->
    emqx:unhook('message.slow_subs_stats', fun ?MODULE:on_stats_update/2).

do_clear(Cfg, Logs) ->
    Now = ?NOW,
    Interval = get_value(expire_interval, Cfg),
    Each = fun(#top_k{index = Index, last_update_time = Ts}) ->
                   case Now - Ts >= Interval of
                       true ->
                           ets:delete(?TOPK_TAB, Index);
                       _ ->
                           true
               end
           end,
    lists:foreach(Each, Logs).

try_insert_to_topk(MaxSize, Index, Latency, Type, Ts) ->
    case ets:info(?TOPK_TAB, size) of
        Size when Size < MaxSize ->
            %% if the size is under limit, insert it directly
            ets:insert(?TOPK_TAB,
                       #top_k{index = Index, type = Type, last_update_time = Ts});
        Size ->
            %% find the minimum value
            ?INDEX(Min, _) = First =
                case ets:first(?TOPK_TAB) of
                    ?INDEX(_, _) = I ->  I;
                    _ -> ?INDEX(Latency - 1, <<>>)
                end,

            case Latency =< Min of
                true -> true;
                _ ->
                    ets:insert(?TOPK_TAB,
                               #top_k{index = Index, type = Type, last_update_time = Ts}),

                    ets:delete(?TOPK_TAB, First)
            end
    end.
