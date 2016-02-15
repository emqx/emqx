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

%% TODO: should match topic tree
%% @doc MQTT retained message storage.
-module(emqttd_retainer).

-behaviour(gen_server).

-include("emqttd.hrl").

-include("emqttd_internal.hrl").

-include_lib("stdlib/include/ms_transform.hrl").

%% Mnesia callbacks
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

%% API Function Exports
-export([retain/1, dispatch/2]).

%% API Function Exports
-export([start_link/0, expire/1]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(mqtt_retained, {topic, message}).

-record(state, {stats_fun, expired_after, stats_timer, expire_timer}).

%%--------------------------------------------------------------------
%% Mnesia callbacks
%%--------------------------------------------------------------------

mnesia(boot) ->
    ok = emqttd_mnesia:create_table(retained, [
                {type, ordered_set},
                {ram_copies, [node()]},
                {record_name, mqtt_retained},
                {attributes, record_info(fields, mqtt_retained)}]);
mnesia(copy) ->
    ok = emqttd_mnesia:copy_table(retained).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Start a retained server
-spec start_link() -> {ok, pid()} | ignore | {error, any()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Retain message
-spec retain(mqtt_message()) -> ok | ignore.
retain(#mqtt_message{retain = false}) -> ignore;

%% RETAIN flag set to 1 and payload containing zero bytes
retain(#mqtt_message{retain = true, topic = Topic, payload = <<>>}) ->
    mnesia:async_dirty(fun mnesia:delete/1, [{retained, Topic}]);

retain(Msg = #mqtt_message{topic = Topic, retain = true, payload = Payload}) ->
    TabSize = mnesia:table_info(retained, size),
    case {TabSize < limit(table), size(Payload) < limit(payload)} of
        {true, true} ->
            Retained = #mqtt_retained{topic = Topic, message = Msg},
            lager:debug("RETAIN ~s", [emqttd_message:format(Msg)]),
            mnesia:async_dirty(fun mnesia:write/3, [retained, Retained, write]),
            emqttd_metrics:set('messages/retained', mnesia:table_info(retained, size));
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
            Env = emqttd_broker:env(retained),
            Val = proplists:get_value(Key, Env),
            put({retained, Key}, Val), Val;
        Val ->
            Val
    end.

%% @doc Deliver retained messages to subscribed client
-spec dispatch(Topic, CPid) -> any() when
        Topic  :: binary(),
        CPid   :: pid().
dispatch(Topic, CPid) when is_binary(Topic) ->
    Msgs =
    case emqttd_topic:wildcard(Topic) of
        false ->
            [Msg || #mqtt_retained{message = Msg} <- mnesia:dirty_read(retained, Topic)];
        true ->
            Fun = fun(#mqtt_retained{topic = Name, message = Msg}, Acc) ->
                    case emqttd_topic:match(Name, Topic) of
                        true -> [Msg|Acc];
                        false -> Acc
                    end
            end,
            mnesia:async_dirty(fun mnesia:foldl/3, [Fun, [], retained])
    end,
    lists:foreach(fun(Msg) -> CPid ! {dispatch, Topic, Msg} end, lists:reverse(Msgs)).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    StatsFun = emqttd_stats:statsfun('retained/count', 'retained/max'),
    %% One second
    {ok, StatsTimer}  = timer:send_interval(timer:seconds(1), stats),
    %% Five minutes
    {ok, ExpireTimer} = timer:send_interval(timer:minutes(5), expire),
    {ok, #state{stats_fun     = StatsFun,
                expired_after = env(expired_after),
                stats_timer   = StatsTimer,
                expire_timer  = ExpireTimer}}.

handle_call(Req, _From, State) ->
    ?UNEXPECTED_REQ(Req, State).

handle_cast(Msg, State) ->
    ?UNEXPECTED_MSG(Msg, State).

handle_info(stats, State = #state{stats_fun = StatsFun}) ->
    StatsFun(mnesia:table_info(retained, size)),
    {noreply, State, hibernate};

handle_info(expire, State = #state{expired_after = Never})
    when Never =:= 0 orelse Never =:= undefined ->
    {noreply, State, hibernate};

handle_info(expire, State = #state{expired_after = ExpiredAfter}) ->
    expire(emqttd_time:now_to_secs() - ExpiredAfter),
    {noreply, State, hibernate};

handle_info(Info, State) ->
    ?UNEXPECTED_INFO(Info, State).

terminate(_Reason, _State = #state{stats_timer = TRef1, expire_timer = TRef2}) ->
    timer:cancel(TRef1),
    timer:cancel(TRef2).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

expire(Time) ->
    mnesia:async_dirty(
        fun() ->
            Match = ets:fun2ms(
                        fun(#mqtt_retained{topic = Topic, message = #mqtt_message{timestamp = {MegaSecs, Secs, _}}})
                            when Time > (MegaSecs * 1000000 + Secs) -> Topic
                        end),
            Topics = mnesia:select(retained, Match, write),
            lists:foreach(fun(Topic) -> mnesia:delete({retained, Topic}) end, Topics)
        end).

