%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
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

%% @doc MQTT retained message.
-module(emqttd_retainer).

-behaviour(gen_server).

-include("emqttd.hrl").

-include("emqttd_internal.hrl").

-include_lib("stdlib/include/ms_transform.hrl").

%% Mnesia Callbacks
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

%% API Function Exports
-export([retain/1, dispatch/2]).

%% API Function Exports
-export([start_link/0]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(retained_message, {topic, msg}).

-record(state, {stats_fun, expired_after, stats_timer, expire_timer}).

%%--------------------------------------------------------------------
%% Mnesia callbacks
%%--------------------------------------------------------------------

mnesia(boot) ->
    ok = emqttd_mnesia:create_table(retained_message, [
                {type, ordered_set},
                {disc_copies, [node()]},
                {record_name, retained_message},
                {attributes, record_info(fields, retained_message)},
                {storage_properties, [{ets, [compressed]},
                                      {dets, [{auto_save, 1000}]}]}]);

mnesia(copy) ->
    ok = emqttd_mnesia:copy_table(retained_message).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Start the retainer
-spec(start_link() -> {ok, pid()} | ignore | {error, any()}).
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Retain a message
-spec(retain(mqtt_message()) -> ok | ignore).
retain(#mqtt_message{retain = false}) -> ignore;

%% RETAIN flag set to 1 and payload containing zero bytes
retain(#mqtt_message{retain = true, topic = Topic, payload = <<>>}) ->
    delete_message(Topic);

retain(Msg = #mqtt_message{topic = Topic, retain = true, payload = Payload}) ->
    TabSize = retained_count(),
    case {TabSize < limit(table), size(Payload) < limit(payload)} of
        {true, true} ->
            retain_message(Msg),
            emqttd_metrics:set('messages/retained', retained_count());
       {false, _}->
            lager:error("Cannot retain message(topic=~s) for table is full!", [Topic]);
       {_, false}->
            lager:error("Cannot retain message(topic=~s, payload_size=~p)"
                            " for payload is too big!", [Topic, size(Payload)])
    end, ok.

limit(table)   -> env(max_message_num);
limit(payload) -> env(max_playload_size).

env(Key) ->
    case get({retained, Key}) of
        undefined ->
            Env = emqttd_conf:retained(),
            Val = proplists:get_value(Key, Env),
            put({retained, Key}, Val), Val;
        Val ->
            Val
    end.

%% @doc Deliver retained messages to the subscriber
-spec(dispatch(Topic :: binary(), CPid :: pid()) -> any()).
dispatch(Topic, CPid) when is_binary(Topic) ->
    Msgs = case emqttd_topic:wildcard(Topic) of
             false -> read_messages(Topic);
             true  -> match_messages(Topic)
           end,
    lists:foreach(fun(Msg) -> CPid ! {dispatch, Topic, Msg} end, lists:reverse(Msgs)).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    StatsFun = emqttd_stats:statsfun('retained/count', 'retained/max'),
    %% One second
    {ok, StatsTimer}  = timer:send_interval(timer:seconds(1), stats),
    State = #state{stats_fun = StatsFun, stats_timer = StatsTimer},
    {ok, init_expire_timer(env(expired_after), State)}.

init_expire_timer(0, State) ->
    State;
init_expire_timer(undefined, State) ->
    State;
init_expire_timer(Secs, State) ->
    {ok, Timer} = timer:send_interval(timer:seconds(Secs), expire),
    State#state{expired_after = Secs, expire_timer = Timer}.

handle_call(Req, _From, State) ->
    ?UNEXPECTED_REQ(Req, State).

handle_cast(Msg, State) ->
    ?UNEXPECTED_MSG(Msg, State).

handle_info(stats, State = #state{stats_fun = StatsFun}) ->
    StatsFun(retained_count()),
    {noreply, State, hibernate};

handle_info(expire, State = #state{expired_after = Never})
    when Never =:= 0 orelse Never =:= undefined ->
    {noreply, State, hibernate};

handle_info(expire, State = #state{expired_after = ExpiredAfter}) ->
    expire_messages(emqttd_time:now_to_secs() - ExpiredAfter),
    {noreply, State, hibernate};

handle_info(Info, State) ->
    ?UNEXPECTED_INFO(Info, State).

terminate(_Reason, _State = #state{stats_timer = TRef1, expire_timer = TRef2}) ->
    timer:cancel(TRef1),
    timer:cancel(TRef2).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

-spec(retain_message(mqtt_message()) -> ok).
retain_message(Msg = #mqtt_message{topic = Topic}) ->
    mnesia:dirty_write(#retained_message{topic = Topic, msg = Msg}).

-spec(read_messages(binary()) -> [mqtt_message()]).
read_messages(Topic) ->
    [Msg || #retained_message{msg = Msg} <- mnesia:dirty_read(retained_message, Topic)].

-spec(match_messages(binary()) -> [mqtt_message()]).
match_messages(Filter) ->
    %% TODO: optimize later...
    Fun = fun(#retained_message{topic = Name, msg = Msg}, Acc) ->
            case emqttd_topic:match(Name, Filter) of
                true -> [Msg|Acc];
                false -> Acc
            end
          end,
    mnesia:async_dirty(fun mnesia:foldl/3, [Fun, [], retained_message]).

-spec(delete_message(binary()) -> ok).
delete_message(Topic) ->
    mnesia:dirty_delete(retained_message, Topic).

-spec(expire_messages(pos_integer()) -> any()).
expire_messages(Time) when is_integer(Time) ->
    mnesia:transaction(
        fun() ->
            Match = ets:fun2ms(
                        fun(#retained_message{topic = Topic, msg = #mqtt_message{timestamp = {MegaSecs, Secs, _}}})
                            when Time > (MegaSecs * 1000000 + Secs) -> Topic
                        end),
            Topics = mnesia:select(retained_message, Match, write),
            lists:foreach(fun(<<"$SYS/", _/binary>>) -> ok; %% ignore $SYS/# messages
                             (Topic) -> mnesia:delete({retained_message, Topic})
                           end, Topics)
        end).

-spec(retained_count() -> non_neg_integer()).
retained_count() -> mnesia:table_info(retained_message, size).

