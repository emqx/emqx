%%--------------------------------------------------------------------
%% Copyright (c) 2013-2017 EMQ Enterprise, Inc. (http://emqtt.io)
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

%% GC Utility functions.

-module(emqttd_gc).

-author("Feng Lee <feng@emqtt.io>").

-export([conn_max_gc_count/0, reset_conn_gc_count/2, maybe_force_gc/2,
         maybe_force_gc/3]).

-spec(conn_max_gc_count() -> integer()).
conn_max_gc_count() ->
    case emqttd:env(conn_force_gc_count) of
        {ok, I} when I > 0 -> I + rand:uniform(I);
        {ok, I} when I =< 0 -> undefined;
        undefined -> undefined
    end.

-spec(reset_conn_gc_count(pos_integer(), tuple()) -> tuple()).
reset_conn_gc_count(Pos, State) ->
    case element(Pos, State) of
        undefined -> State;
        _I        -> setelement(Pos, State, conn_max_gc_count())
    end.

maybe_force_gc(Pos, State) ->
    maybe_force_gc(Pos, State, fun() -> ok end).
maybe_force_gc(Pos, State, Cb) ->
    case element(Pos, State) of
        undefined     -> State;
        I when I =< 0 -> Cb(), garbage_collect(),
                         reset_conn_gc_count(Pos, State);
        I             -> setelement(Pos, State, I - 1)
    end.

