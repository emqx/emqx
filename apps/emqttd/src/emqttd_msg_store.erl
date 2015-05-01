%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2015 eMQTT.IO, All Rights Reserved.
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
%%% MQTT retained message storage.
%%% 
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_msg_store).

-author("Feng Lee <feng@emqtt.io>").

-include_lib("emqtt/include/emqtt.hrl").

%% Mnesia callbacks
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

%% API Function Exports
-export([retain/1, redeliver/2]).

%%%=============================================================================
%%% Mnesia callbacks
%%%=============================================================================

mnesia(boot) ->
    ok = emqttd_mnesia:create_table(message, [
                {type, ordered_set},
                {ram_copies, [node()]},
                {record_name, mqtt_message},
                {attributes, record_info(fields, mqtt_message)}]);

mnesia(copy) ->
    ok = emqttd_mnesia:copy_table(message).

%%%=============================================================================
%%% API
%%%=============================================================================

%%%-----------------------------------------------------------------------------
%% @doc Retain message
%% @end
%%%-----------------------------------------------------------------------------
-spec retain(mqtt_message()) -> ok | ignore.
retain(#mqtt_message{retain = false}) -> ignore;

%% RETAIN flag set to 1 and payload containing zero bytes
retain(#mqtt_message{retain = true, topic = Topic, payload = <<>>}) ->
    mnesia:async_dirty(fun mnesia:delete/1, [{message, Topic}]);

retain(Msg = #mqtt_message{topic = Topic,
                           retain = true,
                           payload = Payload}) ->
    TabSize = mnesia:table_info(message, size),
    case {TabSize < limit(table), size(Payload) < limit(payload)} of
        {true, true} ->
            lager:debug("Retained ~s", [emqtt_message:format(Msg)]),
            mnesia:async_dirty(fun mnesia:write/3, [message, Msg, write]),
            emqttd_metrics:set('messages/retained/count',
                               mnesia:table_info(message, size));
       {false, _}->
            lager:error("Dropped retained message(topic=~s) for table is full!", [Topic]);
       {_, false}->
            lager:error("Dropped retained message(topic=~s, payload=~p) for payload is too big!", [Topic, size(Payload)])
    end, ok.

limit(table) ->
    proplists:get_value(max_message_num, env());
limit(payload) ->
    proplists:get_value(max_playload_size, env()).

env() -> 
    case get({env, retained}) of
        undefined ->
            Env = emqttd_broker:env(retained),
            put({env, retained}, Env), Env;
        Env -> 
            Env
    end.

%%%-----------------------------------------------------------------------------
%% @doc Redeliver retained messages to subscribed client
%% @end
%%%-----------------------------------------------------------------------------
-spec redeliver(Topic, CPid) -> any() when
        Topic  :: binary(),
        CPid   :: pid().
redeliver(Topic, CPid) when is_binary(Topic) andalso is_pid(CPid) ->
    case emqtt_topic:wildcard(Topic) of
        false ->
            dispatch(CPid, mnesia:dirty_read(message, Topic));
        true ->
            Fun = fun(Msg = #mqtt_message{topic = Name}, Acc) ->
                    case emqtt_topic:match(Name, Topic) of
                        true -> [Msg|Acc];
                        false -> Acc
                    end
            end,
            Msgs = mnesia:async_dirty(fun mnesia:foldl/3, [Fun, [], message]),
            dispatch(CPid, lists:reverse(Msgs))
    end.

dispatch(_CPid, []) ->
    ignore;
dispatch(CPid, Msgs) when is_list(Msgs) ->
    CPid ! {dispatch, {self(), [Msg || Msg <- Msgs]}};
dispatch(CPid, Msg) when is_record(Msg, mqtt_message) ->
    CPid ! {dispatch, {self(), Msg}}.

