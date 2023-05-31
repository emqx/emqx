%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-logger_header("[Retainer]").

-export([start_link/1]).

-export([ load/1
        , unload/0
        ]).

-export([ on_session_subscribed/3
        , on_message_publish/2
        ]).

-export([clean/1, read_messages/1]).

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

%%--------------------------------------------------------------------
%% Load/Unload
%%--------------------------------------------------------------------

load(Env) ->
    _ = emqx:hook('session.subscribed', fun ?MODULE:on_session_subscribed/3, []),
    _ = emqx:hook('message.publish', fun ?MODULE:on_message_publish/2, [Env]),
    ok.

unload() ->
    emqx:unhook('message.publish', fun ?MODULE:on_message_publish/2),
    emqx:unhook('session.subscribed', fun ?MODULE:on_session_subscribed/3).

on_session_subscribed(_, _, #{share := ShareName}) when ShareName =/= undefined ->
    ok;
on_session_subscribed(_, Topic, #{rh := Rh, is_new := IsNew}) ->
    case Rh =:= 0 orelse (Rh =:= 1 andalso IsNew) of
        true -> emqx_pool:async_submit(?POOL, fun ?MODULE:dispatch/2, [self(), Topic]);
        _ -> ok
    end.

%% @private
dispatch(Pid, Topic) ->
    MsgsT = case emqx_topic:wildcard(Topic) of
                false -> read_messages(Topic);
                true  -> match_messages(Topic)
            end,
    Now = erlang:system_time(millisecond),
    Msgs = drop_banned_messages(MsgsT),
    [Pid ! {deliver, Topic, refresh_timestamp_expiry(Msg, Now)} || Msg  <- sort_retained(Msgs)].

%% RETAIN flag set to 1 and payload containing zero bytes
on_message_publish(Msg = #message{flags   = #{retain := true},
                                  topic   = Topic,
                                  payload = <<>>}, Env) ->
    mnesia:dirty_delete(?TAB, topic2tokens(Topic)),
    case stop_publish_clear_msg(Env) of
        true ->
            {ok, emqx_message:set_header(allow_publish, false, Msg)};
        _ ->
            {ok, Msg}
    end;

on_message_publish(Msg = #message{flags = #{retain := true}}, Env) ->
    Msg1 = emqx_message:set_header(retained, true, Msg),
    store_retained(Msg1, Env),
    {ok, Msg};
on_message_publish(Msg, _Env) ->
    {ok, Msg}.

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

%% @doc Start the retainer
-spec(start_link(Env :: list()) -> emqx_types:startlink_ret()).
start_link(Env) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Env], []).

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
            {atomic, N} = mnesia:transaction(Fun), N
    end.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Env]) ->
    Copies = case proplists:get_value(storage_type, Env, disc) of
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
    case mnesia:table_info(?TAB, storage_type) of
        Copies -> ok;
        _Other ->
            {atomic, ok} = mnesia:change_table_copy_type(?TAB, node(), Copies),
            ok
    end,
    StatsFun = emqx_stats:statsfun('retained.count', 'retained.max'),
    StatsTimer = erlang:send_after(timer:seconds(1), self(), stats),
    State = #state{stats_fun = StatsFun, stats_timer = StatsTimer},
    {ok, start_expire_timer(proplists:get_value(expiry_interval, Env, 0), State)}.

start_expire_timer(0, State) ->
    State;
start_expire_timer(undefined, State) ->
    State;
start_expire_timer(Ms, State) ->
    Timer = erlang:send_after(Ms, self(), {expire, Ms}),
    State#state{expiry_timer = Timer}.

handle_call(Req, _From, State) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?LOG(error, "Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info(stats, State = #state{stats_fun = StatsFun}) ->
    StatsTimer = erlang:send_after(timer:seconds(1), self(), stats),
    StatsFun(retained_count()),
    {noreply, State#state{stats_timer = StatsTimer}, hibernate};

handle_info({expire, Ms} = Expire, State) ->
    Timer = erlang:send_after(Ms, self(), Expire),
    ok = expire_messages(),
    {noreply, State#state{expiry_timer = Timer}, hibernate};

handle_info(Info, State) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #state{stats_timer = TRef1, expiry_timer = TRef2}) ->
    _ = timer:cancel(TRef1),
    _ = timer:cancel(TRef2),
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
                       Ts1 =< Ts2
               end, Msgs).

store_retained(Msg = #message{topic = Topic, payload = Payload}, Env) ->
    case {is_table_full(Env), is_too_big(size(Payload), Env)} of
        {false, false} ->
            ok = emqx_metrics:inc('messages.retained'),
            mnesia:dirty_write(?TAB, #retained{topic = topic2tokens(Topic),
                                               msg = Msg,
                                               expiry_time = get_expiry_time(Msg, Env)});
        {true, false} ->
            {atomic, _} = mnesia:transaction(
                fun() ->
                    case mnesia:read(?TAB, Topic) of
                        [_] ->
                            mnesia:write(?TAB,
                                         #retained{topic = topic2tokens(Topic),
                                                   msg = Msg,
                                                   expiry_time = get_expiry_time(Msg, Env)}, write);
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

stop_publish_clear_msg(Env) ->
    proplists:get_bool(stop_publish_clear_msg, Env).

is_table_full(Env) ->
    Limit = proplists:get_value(max_retained_messages, Env, 0),
    Limit > 0 andalso (retained_count() > Limit).

is_too_big(Size, Env) ->
    Limit = proplists:get_value(max_payload_size, Env, 0),
    Limit > 0 andalso (Size > Limit).

get_expiry_time(#message{headers = #{properties := #{'Message-Expiry-Interval' := 0}}}, _Env) ->
    0;
get_expiry_time(#message{headers = #{properties := #{'Message-Expiry-Interval' := Interval}},
                         timestamp = Ts}, _Env) ->
    Ts + Interval * 1000;
get_expiry_time(#message{timestamp = Ts}, Env) ->
    case proplists:get_value(expiry_interval, Env, 0) of
        0 -> 0;
        Interval -> Ts + Interval
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
    {atomic, _} = mnesia:transaction(
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
    lists:foreach(fun(R) -> mnesia:dirty_delete_object(?TAB, R) end, Rs),
    length(Rs).

%% @private
condition(Ws) ->
    Ws1 = [case W =:= '+' of true -> '_'; _ -> W end || W <- Ws],
    case lists:last(Ws1) =:= '#' of
        false -> Ws1;
        _ -> (Ws1 -- ['#']) ++ '_'
    end.

-spec(refresh_timestamp_expiry(emqx_types:message(), pos_integer()) -> emqx_types:message()).
refresh_timestamp_expiry(Msg = #message{headers =
                                            #{properties :=
                                                  #{'Message-Expiry-Interval' := Interval} = Props},
                                        timestamp = CreatedAt},
                         Now) ->
    Elapsed = max(0, Now - CreatedAt),
    Interval1 = max(1, Interval - (Elapsed div 1000)),
    emqx_message:set_header(properties,
                            Props#{'Message-Expiry-Interval' => Interval1},
                            Msg#message{timestamp = Now});

refresh_timestamp_expiry(Msg, Now) ->
    Msg#message{timestamp = Now}.

drop_banned_messages(Msgs) ->
    lists:filter(fun(Msg) ->
                         case emqx_banned:look_up({clientid, Msg#message.from}) of
                             [] ->
                                 true;
                             _ ->
                                 ?tp(
                                    notice,
                                    ignore_retained_message_deliver,
                                    #{
                                      reason => "client is banned",
                                      clientid => Msg#message.from
                                     }
                                   ),
                                 false
                         end
                 end, Msgs).
