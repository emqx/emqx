%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_keepalive).

%% APIs
-export([ start/3
        , check/1
        , cancel/1
        ]).

-record(keepalive, {statfun, statval, tsec, tmsg, tref, repeat = 0}).

-opaque(keepalive() :: #keepalive{}).

-export_type([keepalive/0]).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

%% @doc Start a keepalive
-spec(start(fun(), integer(), any()) -> {ok, keepalive()} | {error, term()}).
start(_, 0, _) ->
    {ok, #keepalive{}};
start(StatFun, TimeoutSec, TimeoutMsg) ->
    try StatFun() of
        {ok, StatVal} ->
            {ok, #keepalive{statfun = StatFun, statval = StatVal,
                            tsec = TimeoutSec, tmsg = TimeoutMsg,
                            tref = timer(TimeoutSec, TimeoutMsg)}};
        {error, Error} ->
            {error, Error}
    catch
        _Error:Reason ->
            {error, Reason}
    end.

%% @doc Check keepalive, called when timeout...
-spec(check(keepalive()) -> {ok, keepalive()} | {error, term()}).
check(KeepAlive = #keepalive{statfun = StatFun, statval = LastVal, repeat = Repeat}) ->
    try StatFun() of
        {ok, NewVal} ->
            if NewVal =/= LastVal ->
                    {ok, resume(KeepAlive#keepalive{statval = NewVal, repeat = 0})};
                Repeat < 1 ->
                    {ok, resume(KeepAlive#keepalive{statval = NewVal, repeat = Repeat + 1})};
                true ->
                    {error, timeout}
            end;
        {error, Error} ->
            {error, Error}
    catch
        _Error:Reason ->
            {error, Reason}
    end.

-spec(resume(keepalive()) -> keepalive()).
resume(KeepAlive = #keepalive{tsec = TimeoutSec, tmsg = TimeoutMsg}) ->
    KeepAlive#keepalive{tref = timer(TimeoutSec, TimeoutMsg)}.

%% @doc Cancel Keepalive
-spec(cancel(keepalive()) -> ok).
cancel(#keepalive{tref = TRef}) when is_reference(TRef) ->
    catch erlang:cancel_timer(TRef), ok;
cancel(_) ->
    ok.

timer(Secs, Msg) ->
    erlang:send_after(timer:seconds(Secs), self(), Msg).
