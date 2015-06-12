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

-module(emqttd_mqwin).

-author("Feng Lee <feng@emqtt.io>").

-export([new/2, len/1, in/2, ack/2]).

-define(WIN_SIZE, 100).

-record(mqwin, {name,
                w     = [], %% window list
                len   = 0,  %% current window len
                size  = ?WIN_SIZE}).

-type mqwin() :: #mqwin{}.

-export_type([mqwin/0]).

new(Name, Opts) ->
    WinSize = emqttd_opts:g(inflight_window, Opts, ?WIN_SIZE),
    #mqwin{name = Name, size = WinSize}.

len(#mqwin{len = Len}) ->
    Len.

in(_Msg, #mqwin{len = Len, size = Size})
    when Len =:= Size -> {error, full};

in(Msg, Win = #mqwin{w = W, len = Len}) ->
    {ok, Win#mqwin{w = [Msg|W], len = Len +1}}.
    
ack(MsgId, QWin = #mqwin{w = W, len = Len}) ->
    case lists:keyfind(MsgId, 2, W) of
        false ->
            lager:error("qwin(~s) cannot find msgid: ~p", [MsgId]), QWin;
        _Msg ->
            QWin#mqwin{w = lists:keydelete(MsgId, 2, W), len = Len - 1}
    end.


