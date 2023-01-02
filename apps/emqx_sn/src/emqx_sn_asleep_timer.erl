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

-module(emqx_sn_asleep_timer).

-export([ init/0
        , ensure/2
        , cancel/1
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
ensure(Duration, State) ->
    cancel(State),
    State#asleep_state{duration = Duration, tref = start(Duration)}.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

-compile({inline, [start/1, cancel/1]}).

start(Duration) ->
    erlang:send_after(timer:seconds(Duration), self(), asleep_timeout).

cancel(#asleep_state{tref = Timer}) when is_reference(Timer) ->
    case erlang:cancel_timer(Timer) of
        false ->
            receive {timeout, Timer, _} -> ok after 0 -> ok end;
        _ -> ok
    end;
cancel(_) -> ok.