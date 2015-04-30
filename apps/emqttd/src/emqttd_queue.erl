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
%%% emqttd simple queue.
%%%
%%% @end
%%%-----------------------------------------------------------------------------

%% TODO: this module should be removed...

-module(emqttd_queue).

-author("Feng Lee <feng@emqtt.io>").

-include_lib("emqtt/include/emqtt.hrl").

-export([new/1, new/2, in/3, all/1, clear/1]).

-define(DEFAULT_MAX_LEN, 1000).

-record(mqtt_queue_wrapper, {queue = queue:new(),
                             max_len = ?DEFAULT_MAX_LEN,
                             store_qos0 = false}). 

-type mqtt_queue() :: #mqtt_queue_wrapper{}.

%%------------------------------------------------------------------------------
%% @doc
%% New Queue.
%%
%% @end
%%------------------------------------------------------------------------------
-spec new(non_neg_integer()) -> mqtt_queue().
new(MaxLen) -> #mqtt_queue_wrapper{max_len = MaxLen}.

new(MaxLen, StoreQos0) -> #mqtt_queue_wrapper{max_len = MaxLen, store_qos0 = StoreQos0}.

%%------------------------------------------------------------------------------
%% @doc
%% Queue one message.
%%
%% @end
%%------------------------------------------------------------------------------
-spec in(binary(), mqtt_message(), mqtt_queue()) -> mqtt_queue().
in(ClientId, Message = #mqtt_message{qos = Qos}, 
    Wrapper = #mqtt_queue_wrapper{queue = Queue, max_len = MaxLen}) ->
    case queue:len(Queue) < MaxLen of
        true -> 
            Wrapper#mqtt_queue_wrapper{queue = queue:in(Message, Queue)};
        false -> % full
            if
                Qos =:= ?QOS_0 ->
                    lager:warning("Queue ~s drop qos0 message: ~p", [ClientId, Message]),
                    Wrapper;
                true ->
                    {{value, Msg}, Queue1} = queue:drop(Queue),
                    lager:warning("Queue ~s drop message: ~p", [ClientId, Msg]),
                    Wrapper#mqtt_queue_wrapper{queue = Queue1}
            end
    end.

%%------------------------------------------------------------------------------
%% @doc
%% Get all messages in queue.
%%
%% @end
%%------------------------------------------------------------------------------
-spec all(mqtt_queue()) -> list().
all(#mqtt_queue_wrapper { queue = Queue }) -> queue:to_list(Queue).

%%------------------------------------------------------------------------------
%% @doc
%% Clear queue.
%%
%% @end
%%------------------------------------------------------------------------------
-spec clear(mqtt_queue()) -> mqtt_queue().
clear(Queue) -> Queue#mqtt_queue_wrapper{queue = queue:new()}.

