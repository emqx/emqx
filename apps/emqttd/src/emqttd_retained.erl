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

-define(RETAINED_TABLE, message_retained).

%% API Function Exports
-export([retain/1, redeliver/2]).

%% @doc retain message.
-spec retain(mqtt_message()) -> ok | ignore.
retain(#mqtt_message{retain = false}) -> ignore;

%% RETAIN flag set to 1 and payload containing zero bytes
retain(#mqtt_message{retain = true, topic = Topic, payload = <<>>}) ->
    mnesia:async_dirty(fun mnesia:delete/1, [{?RETAINED_TABLE, Topic}]);

retain(Msg = #mqtt_message{retain = true,
                           topic = Topic,
                           qos = Qos,
                           payload = Payload}) ->
    TabSize = mnesia:table_info(?RETAINED_TABLE, size),
    case {TabSize < limit(table), size(Payload) < limit(payload)} of
        {true, true} ->
            lager:debug("Retained: store message: ~p", [Msg]),
            RetainedMsg = #message_retained{topic = Topic, qos = Qos, payload = Payload},
            mnesia:async_dirty(fun mnesia:write/1, [RetainedMsg]),
            emqttd_metrics:set('messages/retained/count',
                               mnesia:table_info(?RETAINED_TABLE, size));
       {false, _}->
            lager:error("Dropped retained message(topic=~s) for table is full!", [Topic]);
       {_, false}->
            lager:error("Dropped retained message(topic=~s, payload=~p) for payload is too big!", [Topic, size(Payload)])
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

%% @doc redeliver retained messages to subscribed client.
-spec redeliver(Topics, CPid) -> any() when
        Topics  :: list(binary()),
        CPid    :: pid().
redeliver(Topics, CPid) when is_pid(CPid) ->
    lists:foreach(fun(Topic) ->
        case emqtt_topic:wildcard(Topic) of
            false ->
                dispatch(CPid, mnesia:dirty_read(message_retained, Topic));
            true ->
                Fun = fun(Msg = #message_retained{topic = Name}, Acc) ->
                        case emqtt_topic:match(Name, Topic) of
                            true -> [Msg|Acc];
                            false -> Acc
                        end
                end,
                RetainedMsgs = mnesia:async_dirty(fun mnesia:foldl/3, [Fun, [], ?RETAINED_TABLE]),
                dispatch(CPid, lists:reverse(RetainedMsgs))
        end
    end, Topics).

dispatch(_CPid, []) ->
    ignore;
dispatch(CPid, RetainedMsgs) when is_list(RetainedMsgs) ->
    CPid ! {dispatch, {self(), [mqtt_msg(Msg) || Msg <- RetainedMsgs]}};
dispatch(CPid, RetainedMsg) when is_record(RetainedMsg, message_retained) ->
    CPid ! {dispatch, {self(), mqtt_msg(RetainedMsg)}}.

mqtt_msg(#message_retained{topic = Topic, qos = Qos, payload = Payload}) ->
    #mqtt_message{qos = Qos, retain = true, topic = Topic, payload = Payload}.

