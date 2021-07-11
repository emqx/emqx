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

-module(emqx_retainer).

-behaviour(gen_server).

-include("emqx_retainer.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-logger_header("[Retainer]").

-export([start_link/0]).

-export([unload/0
        ]).

-export([ on_session_subscribed/3
        , on_message_publish/1
        ]).

-export([ clean/1
        , update_config/1]).

%% for emqx_pool task func
-export([dispatch/2]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-record(state, {stats_fun, stats_timer, expiry_timer}).

-define(STATS_INTERVAL, timer:seconds(1)).
-define(DEF_STORAGE_TYPE, ram).
-define(DEF_MAX_RETAINED_MESSAGES, 0).
-define(DEF_MAX_PAYLOAD_SIZE, (1024 * 1024)).
-define(DEF_EXPIRY_INTERVAL, 0).
-define(DEF_ENABLE_VAL, false).

%% convenient to generate stats_timer/expiry_timer
-define(MAKE_TIMER(State, Timer, Interval, Msg),
        State#state{Timer = erlang:send_after(Interval, self(), Msg)}).

-rlog_shard({?RETAINER_SHARD, ?TAB}).

%%--------------------------------------------------------------------
%% Load/Unload
%%--------------------------------------------------------------------

load() ->
    _ = emqx:hook('session.subscribed', {?MODULE, on_session_subscribed, []}),
    _ = emqx:hook('message.publish', {?MODULE, on_message_publish, []}),
    ok.

unload() ->
    emqx:unhook('message.publish', {?MODULE, on_message_publish}),
    emqx:unhook('session.subscribed', {?MODULE, on_session_subscribed}).

on_session_subscribed(_, _, #{share := ShareName}) when ShareName =/= undefined ->
    ok;
on_session_subscribed(_, Topic, #{rh := Rh, is_new := IsNew}) ->
    case Rh =:= 0 orelse (Rh =:= 1 andalso IsNew) of
        true -> emqx_pool:async_submit(fun ?MODULE:dispatch/2, [self(), Topic]);
        _ -> ok
    end.

%% @private
dispatch(Pid, Topic) ->
    Msgs = case emqx_topic:wildcard(Topic) of
               false -> read_messages(Topic);
               true  -> match_messages(Topic)
           end,
    [Pid ! {deliver, Topic, Msg} || Msg  <- sort_retained(Msgs)].

%% RETAIN flag set to 1 and payload containing zero bytes
on_message_publish(Msg = #message{flags   = #{retain := true},
                                  topic   = Topic,
                                  payload = <<>>}) ->
    ekka_mnesia:dirty_delete(?TAB, topic2tokens(Topic)),
    {ok, Msg};

on_message_publish(Msg = #message{flags = #{retain := true}}) ->
    Msg1 = emqx_message:set_header(retained, true, Msg),
    store_retained(Msg1),
    {ok, Msg};
on_message_publish(Msg) ->
    {ok, Msg}.

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

%% @doc Start the retainer
-spec(start_link() -> emqx_types:startlink_ret()).
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec(clean(emqx_types:topic()) -> non_neg_integer()).
clean(Topic) when is_binary(Topic) ->
    case emqx_topic:wildcard(Topic) of
        true -> match_delete_messages(Topic);
        false ->
            Tokens = topic2tokens(Topic),
            Fun = fun() ->
                      case mnesia:read({?TAB, Tokens}) of
                          [] -> 0;
                          [_M] -> mnesia:delete({?TAB, Tokens}), 1
                      end
                  end,
            {atomic, N} = ekka_mnesia:transaction(?RETAINER_SHARD, Fun), N
    end.

%%--------------------------------------------------------------------
%% Update Config
%%--------------------------------------------------------------------
-spec update_config(hocon:config()) -> ok.
update_config(Conf) ->
    OldCfg = emqx_config:get([?APP]),
    emqx_config:put([?APP], Conf),
    check_enable_when_update(OldCfg).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    StorageType = emqx_config:get([?MODULE, storage_type], ?DEF_STORAGE_TYPE),
    ExpiryInterval = emqx_config:get([?MODULE, expiry_interval], ?DEF_EXPIRY_INTERVAL),
    Copies = case StorageType of
                 ram       -> ram_copies;
                 disc      -> disc_copies;
                 disc_only -> disc_only_copies
             end,
    StoreProps = [{ets, [compressed,
                         {read_concurrency, true},
                         {write_concurrency, true}]},
                  {dets, [{auto_save, 1000}]}],
    ok = ekka_mnesia:create_table(?TAB, [
                {type, set},
                {Copies, [node()]},
                {record_name, retained},
                {attributes, record_info(fields, retained)},
                {storage_properties, StoreProps}]),
    ok = ekka_mnesia:copy_table(?TAB, Copies),
    ok = ekka_rlog:wait_for_shards([?RETAINER_SHARD], infinity),
    case mnesia:table_info(?TAB, storage_type) of
        Copies -> ok;
        _Other ->
            {atomic, ok} = mnesia:change_table_copy_type(?TAB, node(), Copies),
            ok
    end,
    StatsFun = emqx_stats:statsfun('retained.count', 'retained.max'),
    State = ?MAKE_TIMER(#state{stats_fun = StatsFun}, stats_timer, ?STATS_INTERVAL, stats),
    check_enable_when_init(),
    {ok, start_expire_timer(ExpiryInterval, State)}.

start_expire_timer(0, State) ->
    State;
start_expire_timer(undefined, State) ->
    State;
start_expire_timer(Ms, State) ->
    ?MAKE_TIMER(State, expiry_timer, Ms, expire).

handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info(stats, State = #state{stats_fun = StatsFun}) ->
    StatsFun(retained_count()),
    {noreply, ?MAKE_TIMER(State, stats_timer, ?STATS_INTERVAL, stats), hibernate};

handle_info(expire, State) ->
    ok = expire_messages(),
    Interval = emqx_config:get([?MODULE, expiry_interval], ?DEF_EXPIRY_INTERVAL),
    {noreply, start_expire_timer(Interval, State), hibernate};

handle_info(Info, State) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #state{stats_timer = TRef1, expiry_timer = TRef2}) ->
    _ = erlang:cancel_timer(TRef1),
    _ = erlang:cancel_timer(TRef2),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
sort_retained([]) -> [];
sort_retained([Msg]) -> [Msg];
sort_retained(Msgs)  ->
    lists:sort(fun(#message{timestamp = Ts1}, #message{timestamp = Ts2}) ->
                       Ts1 =< Ts2 end,
               Msgs).

store_retained(Msg = #message{topic = Topic, payload = Payload}) ->
    case {is_table_full(), is_too_big(size(Payload))} of
        {false, false} ->
            ok = emqx_metrics:inc('messages.retained'),
            ekka_mnesia:dirty_write(?TAB, #retained{topic = topic2tokens(Topic),
                                                    msg = Msg,
                                                    expiry_time = get_expiry_time(Msg)});
        {true, false} ->
            {atomic, _} = ekka_mnesia:transaction(?RETAINER_SHARD,
                fun() ->
                        case mnesia:read(?TAB, Topic) of
                            [_] ->
                                mnesia:write(?TAB,
                                             #retained{topic = topic2tokens(Topic),
                                                       msg = Msg,
                                                       expiry_time = get_expiry_time(Msg)},
                                             write);
                            [] ->
                                ?LOG(error,
                                     "Cannot retain message(topic=~s) for table is full!", [Topic])
                    end
                end),
            ok;
        {true, _} ->
            ?LOG(error, "Cannot retain message(topic=~s) for table is full!", [Topic]);
        {_, true} ->
            ?LOG(error, "Cannot retain message(topic=~s, payload_size=~p) "
                        "for payload is too big!", [Topic, iolist_size(Payload)])
    end.

is_table_full() ->
    Limit = emqx_config:get([?MODULE, max_retained_messages], ?DEF_MAX_RETAINED_MESSAGES),
    Limit > 0 andalso (retained_count() > Limit).

is_too_big(Size) ->
    Limit = emqx_config:get([?MODULE, max_payload_size], ?DEF_MAX_PAYLOAD_SIZE),
    Limit > 0 andalso (Size > Limit).

get_expiry_time(#message{headers = #{properties := #{'Message-Expiry-Interval' := 0}}}) ->
    0;
get_expiry_time(#message{headers = #{properties := #{'Message-Expiry-Interval' := Interval}},
                         timestamp = Ts}) ->
    Ts + Interval * 1000;
get_expiry_time(#message{timestamp = Ts}) ->
    Interval = emqx_config:get([?MODULE, expiry_interval], ?DEF_EXPIRY_INTERVAL),
    case Interval of
        0 -> 0;
        _ -> Ts + Interval
    end.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

-spec(retained_count() -> non_neg_integer()).
retained_count() -> mnesia:table_info(?TAB, size).

topic2tokens(Topic) ->
    emqx_topic:words(Topic).

expire_messages() ->
    NowMs = erlang:system_time(millisecond),
    MsHd = #retained{topic = '$1', msg = '_', expiry_time = '$3'},
    Ms = [{MsHd, [{'=/=','$3',0}, {'<','$3',NowMs}], ['$1']}],
    {atomic, _} = ekka_mnesia:transaction(?RETAINER_SHARD,
        fun() ->
            Keys = mnesia:select(?TAB, Ms, write),
            lists:foreach(fun(Key) -> mnesia:delete({?TAB, Key}) end, Keys)
        end),
    ok.

-spec(read_messages(emqx_types:topic())
      -> [emqx_types:message()]).
read_messages(Topic) ->
    Tokens = topic2tokens(Topic),
    case mnesia:dirty_read(?TAB, Tokens) of
        [] -> [];
        [#retained{msg = Msg, expiry_time = Et}] ->
            case Et =:= 0 orelse Et >= erlang:system_time(millisecond) of
                true -> [Msg];
                false -> []
            end
    end.

-spec(match_messages(emqx_types:topic())
      -> [emqx_types:message()]).
match_messages(Filter) ->
    NowMs = erlang:system_time(millisecond),
    Cond = condition(emqx_topic:words(Filter)),
    MsHd = #retained{topic = Cond, msg = '$2', expiry_time = '$3'},
    Ms = [{MsHd, [{'=:=','$3',0}], ['$2']},
          {MsHd, [{'>','$3',NowMs}], ['$2']}],
    mnesia:dirty_select(?TAB, Ms).

-spec(match_delete_messages(emqx_types:topic())
      -> DeletedCnt :: non_neg_integer()).
match_delete_messages(Filter) ->
    Cond = condition(emqx_topic:words(Filter)),
    MsHd = #retained{topic = Cond, msg = '_', expiry_time = '_'},
    Ms = [{MsHd, [], ['$_']}],
    Rs = mnesia:dirty_select(?TAB, Ms),
    lists:foreach(fun(R) -> ekka_mnesia:dirty_delete_object(?TAB, R) end, Rs),
    length(Rs).

%% @private
condition(Ws) ->
    Ws1 = [case W =:= '+' of true -> '_'; _ -> W end || W <- Ws],
    case lists:last(Ws1) =:= '#' of
        false -> Ws1;
        _ -> (Ws1 -- ['#']) ++ '_'
    end.

-spec check_enable_when_init() -> ok.
check_enable_when_init() ->
    case emqx_config:get([?APP, enable], ?DEF_ENABLE_VAL) of
        true -> load();
        _  -> ok
    end.

-spec check_enable_when_update(hocon:config()) -> ok.
check_enable_when_update(OldCfg) ->
    OldVal = maps:get(enable, OldCfg, undefined),
    case emqx_config:get([?APP, enable], ?DEF_ENABLE_VAL) of
        OldVal ->
            ok;
        true ->
            load();
        _ ->
            unload()
    end.

