%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_sn_asleep_timer).

-export([ init/0
        , ensure/2
        ]).

-record(asleep_state, {
          %% Time internal (seconds)
          duration :: integer(),
          %% Timer reference
          tref :: reference() | undefined
         }).

-type(asleep_state() :: #asleep_state{}).

-export_type([asleep_state/0]).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec(init() -> asleep_state()).
init() ->
    #asleep_state{duration = 0}.

-spec(ensure(undefined | integer(), asleep_state()) -> asleep_state()).
ensure(undefined, State = #asleep_state{duration = Duration}) ->
    ensure(Duration, State);
ensure(Duration, State = #asleep_state{tref = TRef}) ->
    _ = cancel(TRef),
    State#asleep_state{duration = Duration, tref = start(Duration)}.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

-compile({inline, [start/1, cancel/1]}).

start(Duration) ->
    erlang:send_after(timer:seconds(Duration), self(), asleep_timeout).

cancel(undefined) -> ok;
cancel(TRef) when is_reference(TRef) ->
    erlang:cancel_timer(TRef).
