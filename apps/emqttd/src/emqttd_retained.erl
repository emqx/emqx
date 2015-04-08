%%%-----------------------------------------------------------------------------
%%% @Copyright (C) 2012-2015, Feng Lee <feng@emqtt.io>
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc
%%% emqttd retained messages.
%%% 
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_retained).

-author('feng@slimpp.io').

-include("emqttd.hrl").

-include("emqttd_topic.hrl").

-include("emqttd_packet.hrl").

%% API Function Exports
-export([retain/1, dispatch/2]).

%% @doc retain message.
-spec retain(mqtt_message()) -> ok | ignore.
retain(#mqtt_message{retain = false}) -> ignore;

%% RETAIN flag set to 1 and payload containing zero bytes
retain(#mqtt_message{retain = true, topic = Topic, payload = <<>>}) -> 
    mnesia:transaction(fun() -> mnesia:delete({mqtt_retained, Topic}) end);

retain(Msg = #mqtt_message{retain = true,
                           topic = Topic,
                           qos = Qos,
                           payload = Payload}) ->
    TabSize = mnesia:table_info(mqtt_retained, size),
    case {TabSize < limit(table), size(Payload) < limit(payload)} of
        {true, true} ->
            lager:debug("Retained: store message: ~p", [Msg]),
            mnesia:transaction(
                fun() -> 
                    mnesia:write(#mqtt_retained{topic = Topic,
                                                qos   = Qos,
                                                payload = Payload})
                end), 
            emqttd_metrics:set('messages/retained/count',
                               mnesia:table_info(mqtt_retained, size));
       {false, _}->
            lager:error("Retained: dropped message(topic=~s) for table is full!", [Topic]);
       {_, false}->
            lager:error("Retained: dropped message(topic=~s, payload=~p) for payload is too big!", [Topic, size(Payload)])
    end.

limit(table) ->
    proplists:get_value(max_message_num, env());
limit(payload) ->
    proplists:get_value(max_playload_size, env()).

env() -> 
    case get({env, retained}) of
        undefined ->
            {ok, Env} = application:get_env(emqttd, retained),
            put({env, retained}, Env), Env;
        Env -> 
            Env
    end.

%% @doc dispatch retained messages to subscribed client.
-spec dispatch(Topics, CPid) -> any() when
        Topics  :: list(binary()),
        CPid    :: pid().
dispatch(Topics, CPid) when is_pid(CPid) ->
    Msgs = lists:flatten([mnesia:dirty_read(mqtt_retained, Topic) || Topic <- match(Topics)]),
    lists:foreach(fun(Msg) -> CPid ! {dispatch, {self(), mqtt_msg(Msg)}} end, Msgs).

match(Topics) ->
    RetainedTopics = mnesia:dirty_all_keys(mqtt_retained),
    lists:flatten([match(Topic, RetainedTopics) || Topic <- Topics]).

match(Topic, RetainedTopics) ->
    case emqttd_topic:type(#topic{name=Topic}) of
        direct -> %% FIXME
            [Topic];
        wildcard ->
            [T || T <- RetainedTopics, emqttd_topic:match(T, Topic)]
    end.

mqtt_msg(#mqtt_retained{topic = Topic, qos = Qos, payload = Payload}) ->
    #mqtt_message{qos = Qos, retain = true, topic = Topic, payload = Payload}.

