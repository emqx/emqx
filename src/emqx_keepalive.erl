%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([start/2, check/2, cancel/1]).

-record(keepalive, {tmsec, tmsg, tref}).

-type(keepalive() :: #keepalive{}).

-export_type([keepalive/0]).

-define(SWEET_SPOT, 50).    % 50ms

%% @doc Start a keepalive
-spec(start(integer(), any()) -> {ok, keepalive()}).
start(0, _) ->
    {ok, #keepalive{}};
start(TimeoutSec, TimeoutMsg) ->
    {ok, #keepalive{tmsec = TimeoutSec * 1000, tmsg = TimeoutMsg, tref = timer(TimeoutSec * 1000 + ?SWEET_SPOT, TimeoutMsg)}}.

%% @doc Check keepalive, called when timeout...
-spec(check(keepalive(), integer()) -> {ok, keepalive()} | {error, term()}).
check(KeepAlive = #keepalive{tmsec = TimeoutMs}, LastPacketTs) ->
    TimeDiff = erlang:system_time(millisecond) - LastPacketTs,
    case TimeDiff >= TimeoutMs of
        true ->
            {error, timeout};
        false ->
            {ok, resume(KeepAlive, TimeoutMs + ?SWEET_SPOT - TimeDiff)}
    end.

-spec(resume(keepalive(), integer()) -> keepalive()).
resume(KeepAlive = #keepalive{tmsg = TimeoutMsg}, TimeoutMs) ->
    KeepAlive#keepalive{tref = timer(TimeoutMs, TimeoutMsg)}.

%% @doc Cancel Keepalive
-spec(cancel(keepalive()) -> ok).
cancel(#keepalive{tref = TRef}) when is_reference(TRef) ->
    catch erlang:cancel_timer(TRef), ok;
cancel(_) ->
    ok.

timer(Millisecond, Msg) ->
    erlang:send_after(Millisecond, self(), Msg).

