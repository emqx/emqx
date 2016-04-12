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

%% API Function Exports
-export([retain/1, dispatch/2]).

%% API Function Exports
-export([start_link/0]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {stats_fun, expired_after, stats_timer, expire_timer}).

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
    emqttd_backend:delete_message(Topic);

retain(Msg = #mqtt_message{topic = Topic, retain = true, payload = Payload}) ->
    TabSize = emqttd_backend:retained_count(),
    case {TabSize < limit(table), size(Payload) < limit(payload)} of
        {true, true} ->
            emqttd_backend:retain_message(Msg),
            emqttd_metrics:set('messages/retained', emqttd_backend:retained_count());
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

%% @doc Deliver retained messages to the subscriber
-spec(dispatch(Topic :: binary(), CPid :: pid()) -> any()).
dispatch(Topic, CPid) when is_binary(Topic) ->
    Msgs = case emqttd_topic:wildcard(Topic) of
             false -> emqttd_backend:read_messages(Topic);
             true  -> emqttd_backend:match_messages(Topic)
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
    StatsFun(emqttd_backend:retained_count()),
    {noreply, State, hibernate};

handle_info(expire, State = #state{expired_after = Never})
    when Never =:= 0 orelse Never =:= undefined ->
    {noreply, State, hibernate};

handle_info(expire, State = #state{expired_after = ExpiredAfter}) ->
    emqttd_backend:expire_messages(emqttd_time:now_to_secs() - ExpiredAfter),
    {noreply, State, hibernate};

handle_info(Info, State) ->
    ?UNEXPECTED_INFO(Info, State).

terminate(_Reason, _State = #state{stats_timer = TRef1, expire_timer = TRef2}) ->
    timer:cancel(TRef1),
    timer:cancel(TRef2).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

