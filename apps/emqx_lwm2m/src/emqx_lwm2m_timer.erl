%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_lwm2m_timer).

-include("emqx_lwm2m.hrl").

-export([ cancel_timer/1
        , start_timer/2
        , refresh_timer/1
        , refresh_timer/2
        ]).

-record(timer_state, { interval
                     , tref
                     , message
                     }).

-define(LOG(Level, Format, Args),
    logger:Level("LWM2M-TIMER: " ++ Format, Args)).

cancel_timer(#timer_state{tref = TRef}) when is_reference(TRef) ->
    _ = erlang:cancel_timer(TRef), ok.

refresh_timer(State=#timer_state{interval = Interval, message = Msg}) ->
    cancel_timer(State), start_timer(Interval, Msg).
refresh_timer(NewInterval, State=#timer_state{message = Msg}) ->
    cancel_timer(State), start_timer(NewInterval, Msg).

%% start timer in seconds
start_timer(Interval, Msg) ->
    ?LOG(debug, "start_timer of ~p secs", [Interval]),
    TRef = erlang:send_after(timer:seconds(Interval), self(), Msg),
    #timer_state{interval = Interval, tref = TRef, message = Msg}.
