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

-module(emqx_slow_subs).

-behaviour(gen_server).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx_slow_subs/include/emqx_slow_subs.hrl").

-export([ start_link/0, on_stats_update/2, update_settings/1
        , clear_history/0, init_topk_tab/0, post_config_update/5
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

-type state() :: #{ enable := boolean()
                  , last_tick_at := pos_integer()
                  , expire_timer := undefined | reference()
                  , notice_timer := undefined | reference()
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
-spec(start_link() -> emqx_types:startlink_ret()).
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

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
            %% if last value == the new value, update the type and last_update_time
            %% XXX for clients whose latency are stable for a long time, is it
            %% possible to reduce updates?
            ets:insert(?TOPK_TAB,
                       #top_k{index = Index, type = Type, last_update_time = Ts});
        [_] ->
            %% if Latency > minimum value, we should update it
            %% if Latency < minimum value, maybe it can replace the minimum value
            %% so always update at here
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

update_settings(Conf) ->
    emqx_conf:update([slow_subs], Conf, #{override_to => cluster}).

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

post_config_update(_KeyPath, _UpdateReq, NewConf, _OldConf, _AppEnvs) ->
    gen_server:call(?MODULE, {update_settings, NewConf}, ?DEF_CALL_TIMEOUT).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    emqx_conf:add_handler([slow_subs], ?MODULE),

    InitState = #{enable => false,
                  last_tick_at => 0,
                  expire_timer => undefined,
                  notice_timer => undefined
                 },

    Enable = emqx:get_config([slow_subs, enable]),
    {ok, check_enable(Enable, InitState)}.

handle_call({update_settings, #{enable := Enable} = Conf}, _From, State) ->
    emqx_config:put([slow_subs], Conf),
    State2 = check_enable(Enable, State),
    {reply, ok, State2};

handle_call(clear_history, _, State) ->
    ets:delete_all_objects(?TOPK_TAB),
    {reply, ok, State};

handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, State}.

handle_info(expire_tick, State) ->
    Logs = ets:tab2list(?TOPK_TAB),
    do_clear(Logs),
    State1 = start_timer(expire_timer, fun expire_tick/0, State),
    {noreply, State1};

handle_info(notice_tick, State) ->
    Logs = ets:tab2list(?TOPK_TAB),
    do_notification(Logs, State),
    State1 = start_timer(notice_timer, fun notice_tick/0, State),
    {noreply, State1#{last_tick_at := ?NOW}};

handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

terminate(_Reason, State) ->
    _ = unload(State),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
expire_tick() ->
    erlang:send_after(?EXPIRE_CHECK_INTERVAL, self(), ?FUNCTION_NAME).

notice_tick() ->
    case emqx:get_config([slow_subs, notice_interval]) of
        0 -> undefined;
        Interval ->
            erlang:send_after(Interval, self(), ?FUNCTION_NAME)
    end.

-spec do_notification(list(), state()) -> ok.
do_notification([], _) ->
    ok;

do_notification(Logs, #{last_tick_at := LastTickTime}) ->
    start_publish(Logs, LastTickTime),
    ok.

start_publish(Logs, TickTime) ->
    emqx_pool:async_submit({fun do_publish/3, [Logs, erlang:length(Logs), TickTime]}).

do_publish([], _, _) ->
    ok;

do_publish(Logs, Rank, TickTime) ->
    BatchSize = emqx:get_config([slow_subs, notice_batch_size]),
    do_publish(Logs, BatchSize, Rank, TickTime, []).

do_publish([Log | T], Size, Rank, TickTime, Cache) when Size > 0 ->
    Cache2 = [convert_to_notice(Rank, Log) | Cache],
    do_publish(T, Size - 1, Rank - 1, TickTime, Cache2);

do_publish(Logs, Size, Rank, TickTime, Cache) when Size =:= 0 ->
    publish(TickTime, Cache),
    do_publish(Logs, Rank, TickTime);

do_publish([], _, _Rank, TickTime, Cache) ->
    publish(TickTime, Cache),
    ok.

convert_to_notice(Rank, #top_k{index = ?INDEX(Latency, ClientId),
                               type = Type,
                               last_update_time = Ts}) ->
    #{rank => Rank,
      clientid => ClientId,
      latency => Latency,
      type => Type,
      timestamp => Ts}.

publish(TickTime, Notices) ->
    WindowLog = #{last_tick_at => TickTime,
                  logs => lists:reverse(Notices)},
    Payload = emqx_json:encode(WindowLog),
    Msg = #message{ id = emqx_guid:gen()
                  , qos = emqx:get_config([slow_subs, notice_qos])
                  , from = ?MODULE
                  , topic = emqx_topic:systop(?NOTICE_TOPIC_NAME)
                  , payload = Payload
                  , timestamp = ?NOW
                  },
    _ = emqx_broker:safe_publish(Msg),
    ok.

load(State) ->
    MaxSizeT = emqx:get_config([slow_subs, top_k_num]),
    MaxSize = erlang:min(MaxSizeT, ?MAX_TAB_SIZE),
    _ = emqx:hook('message.slow_subs_stats',
                  {?MODULE, on_stats_update, [#{max_size => MaxSize}]}
                 ),

    State1 = start_timer(notice_timer, fun notice_tick/0, State),
    State2 = start_timer(expire_timer, fun expire_tick/0, State1),
    State2#{enable := true, last_tick_at => ?NOW}.


unload(#{notice_timer := NoticeTimer, expire_timer := ExpireTimer} = State) ->
    emqx:unhook('message.slow_subs_stats', {?MODULE, on_stats_update}),
    State#{notice_timer := cancel_timer(NoticeTimer),
           expire_timer := cancel_timer(ExpireTimer)
          }.

do_clear(Logs) ->
    Now = ?NOW,
    Interval = emqx:get_config([slow_subs, expire_interval]),
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
        _Size ->
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

check_enable(Enable, #{enable := IsEnable} = State) ->
    update_threshold(),
    case Enable of
        IsEnable ->
            State;
        true ->
            load(State);
        _ ->
            unload(State)
    end.

update_threshold() ->
    Threshold = emqx:get_config([slow_subs, threshold]),
    emqx_message_latency_stats:update_threshold(Threshold),
    ok.

start_timer(Name, Fun, State) ->
    _ = cancel_timer(maps:get(Name, State)),
    State#{Name := Fun()}.

cancel_timer(TimerRef) when is_reference(TimerRef) ->
    _ = erlang:cancel_timer(TimerRef),
    undefined;
cancel_timer(_) ->
    undefined.
