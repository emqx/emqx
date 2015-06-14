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
%%% Inflight window of message queue. Wrap a list with len.
%%%
%%% @end
%%%-----------------------------------------------------------------------------

-module(emqttd_inflight).

-author("Feng Lee <feng@emqtt.io>").

-include_lib("emqtt/include/emqtt.hrl").

-export([new/2, is_full/1, len/1, in/2, ack/2]).

-define(MAX_SIZE, 100).

-record(inflight, {name, q = [], len = 0, size = ?MAX_SIZE}).

-type inflight() :: #inflight{}.

-export_type([inflight/0]).

new(Name, Max) ->
    #inflight{name = Name, size = Max}.

is_full(#inflight{size = 0}) ->
    false;
is_full(#inflight{len = Len, size = Size}) when Len < Size ->
    false;
is_full(_Inflight) ->
    true.

len(#inflight{len = Len}) ->
    Len.

in(_Msg, #inflight{len = Len, size = Size})
    when Len =:= Size -> {error, full};

in(Msg = #mqtt_message{msgid = MsgId}, Inflight = #inflight{q = Q, len = Len}) ->
    {ok, Inflight#inflight{q = [{MsgId, Msg}|Q], len = Len +1}}.
    
ack(MsgId, Inflight = #inflight{q = Q, len = Len}) ->
    case lists:keyfind(MsgId, 1, Q) of
        false ->
            lager:error("Inflight(~s) cannot find msgid: ~p", [MsgId]),
            Inflight;
        _Msg ->
            Inflight#inflight{q = lists:keydelete(MsgId, 1, Q), len = Len - 1}
    end.

