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

-module(emqttd_mod_retainer).

-behaviour(gen_server).

-behaviour(emqttd_gen_mod).

-include("emqttd.hrl").

-include("emqttd_internal.hrl").

-include_lib("stdlib/include/ms_transform.hrl").

%% gen_mod Callbacks
-export([load/1, unload/1]).

%% Hook Callbacks
-export([on_session_subscribed/4, on_message_publish/2]).

%% API Function Exports
-export([start_link/1]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(mqtt_retained, {topic, msg}).

-record(state, {stats_fun, expired_after, stats_timer, expire_timer}).

%%--------------------------------------------------------------------
%% Load/Unload
%%--------------------------------------------------------------------

load(Env) ->
    emqttd_mod_sup:start_child(spec(Env)),
    emqttd:hook('session.subscribed', fun ?MODULE:on_session_subscribed/4, [Env]),
    emqttd:hook('message.publish', fun ?MODULE:on_message_publish/2, [Env]).

on_session_subscribed(_ClientId, _Username, {Topic, _Opts}, _Env) ->
    SessPid = self(),
    Msgs = case emqttd_topic:wildcard(Topic) of
               false -> read_messages(Topic);
               true  -> match_messages(Topic)
           end,
    lists:foreach(fun(Msg) -> SessPid ! {dispatch, Topic, Msg} end, lists:reverse(Msgs)).

on_message_publish(Msg = #mqtt_message{retain = false}, _Env) ->
    {ok, Msg};

%% RETAIN flag set to 1 and payload containing zero bytes
on_message_publish(Msg = #mqtt_message{retain = true, topic = Topic, payload = <<>>}, _Env) ->
    mnesia:dirty_delete(mqtt_retained, Topic),
    {stop, Msg};

on_message_publish(Msg = #mqtt_message{topic = Topic, retain = true, payload = Payload}, Env) ->
    case {is_table_full(Env), is_too_big(size(Payload), Env)} of
        {false, false} ->
            mnesia:dirty_write(#mqtt_retained{topic = Topic, msg = Msg}),
            emqttd_metrics:set('messages/retained', retained_count());
       {true, _}->
            lager:error("Cannot retain message(topic=~s) for table is full!", [Topic]);
       {_, true}->
            lager:error("Cannot retain message(topic=~s, payload_size=~p)"
                            " for payload is too big!", [Topic, size(Payload)])
    end,
    {ok, Msg#mqtt_message{retain = false}}.

is_table_full(Env) ->
    Limit = proplists:get_value(max_message_num, Env, 0),
    Limit > 0 andalso (retained_count() > Limit).

is_too_big(Size, Env) ->
    Limit = proplists:get_value(max_payload_size, Env, 0),
    Limit > 0 andalso (Size > Limit).

unload(_Env) ->
    emqttd:unhook('session.subscribed', fun ?MODULE:on_session_subscribed/4),
    emqttd:unhook('message.publish', fun ?MODULE:on_message_publish/2),
    emqttd_mod_sup:stop_child(?MODULE).

spec(Env) ->
    {?MODULE, {?MODULE, start_link, [Env]}, permanent, 5000, worker, [?MODULE]}.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Start the retainer
-spec(start_link(Env :: list()) -> {ok, pid()} | ignore | {error, any()}).
start_link(Env) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Env], []).

%%--------------------------------------------------------------------
%% gen_server Callbacks
%%--------------------------------------------------------------------

init([Env]) ->
    Copy = case proplists:get_value(storage_type, Env, disc) of
               disc -> disc_copies;
               ram  -> ram_copies
           end,
    ok = emqttd_mnesia:create_table(mqtt_retained, [
                {type, ordered_set},
                {Copy, [node()]},
                {record_name, mqtt_retained},
                {attributes, record_info(fields, mqtt_retained)},
                {storage_properties, [{ets, [compressed]},
                                      {dets, [{auto_save, 1000}]}]}]),
    ok = emqttd_mnesia:copy_table(mqtt_retained),
    StatsFun = emqttd_stats:statsfun('retained/count', 'retained/max'),
    {ok, StatsTimer}  = timer:send_interval(timer:seconds(1), stats),
    State = #state{stats_fun = StatsFun, stats_timer = StatsTimer},
    {ok, init_expire_timer(proplists:get_value(expired_after, Env, 0), State)}.

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

-spec(read_messages(binary()) -> [mqtt_message()]).
read_messages(Topic) ->
    [Msg || #mqtt_retained{msg = Msg} <- mnesia:dirty_read(mqtt_retained, Topic)].

-spec(match_messages(binary()) -> [mqtt_message()]).
match_messages(Filter) ->
    %% TODO: optimize later...
    Fun = fun(#mqtt_retained{topic = Name, msg = Msg}, Acc) ->
            case emqttd_topic:match(Name, Filter) of
                true -> [Msg|Acc];
                false -> Acc
            end
          end,
    mnesia:async_dirty(fun mnesia:foldl/3, [Fun, [], mqtt_retained]).

-spec(expire_messages(pos_integer()) -> any()).
expire_messages(Time) when is_integer(Time) ->
    mnesia:transaction(
        fun() ->
            Match = ets:fun2ms(
                        fun(#mqtt_retained{topic = Topic, msg = #mqtt_message{timestamp = Ts}})
                            when Time > Ts -> Topic
                        end),
            Topics = mnesia:select(mqtt_retained, Match, write),
            lists:foreach(fun(<<"$SYS/", _/binary>>) -> ok; %% ignore $SYS/# messages
                             (Topic) -> mnesia:delete({mqtt_retained, Topic})
                           end, Topics)
        end).

-spec(retained_count() -> non_neg_integer()).
retained_count() -> mnesia:table_info(mqtt_retained, size).

