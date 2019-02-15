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

%% @doc TODO:
%% 1. Flapping Detection
%% 2. Conflict Detection?
-module(emqx_flapping).

-include("emqx.hrl").

-export([init_flapping/4,
         check_flapping/3]).

-spec init_flapping(pos_integer(),
                    pos_integer(),float(),
                    float()) -> flapping().
init_flapping(CheckTimes, TimeInterval,
              HighTreshold, LowTreshold) ->
    #flapping{state = stop,
              check_times = CheckTimes,
              time_interval = TimeInterval,
              high_treshold = HighTreshold,
              low_treshold = LowTreshold}.

-spec check_flapping(Fun::fun(), term(), flapping()) -> flapping().
check_flapping(CheckStateFun, StateChecked,
               FlappingRecord = #flapping{state = FlappingState,
                                          check_times = CheckTimes,
                                          time_interval = TimeInterval,
                                          high_treshold = HighTreshold,
                                          low_treshold = LowTreshold}) ->
    Weights = check_state_transition(CheckTimes, TimeInterval, CheckTimes,
                                     CheckStateFun, StateChecked, 0),
    case Weights/CheckTimes of
        NewFlappingState when NewFlappingState >= HighTreshold,
                              FlappingState =:= stop ->
            FlappingRecord#flapping{state = start};
        NewFlappingState when NewFlappingState =< LowTreshold,
                              FlappingState =:= start ->
            FlappingRecord#flapping{state = stop};
        _ ->
            FlappingRecord
    end.

-spec check_state_transition(non_neg_integer(), pos_integer(), pos_integer(), Fun::fun(), term(), number()) -> pos_integer().
check_state_transition(0, _TimeInterval, _Total, _CheckState, _State, Weight) ->
    Weight;
check_state_transition(Count, TimeInterval, Total, CheckState, State, Weight)
  when Count > 0->
    timer:sleep(TimeInterval),
    NewState = CheckState(),
    case State =:= NewState of
        false ->
            NewWeight = Weight + weight_transition(Total - Count + 1, Total),
            check_state_transition(Count - 1, TimeInterval, Total, CheckState, NewState, NewWeight);
        true ->
            check_state_transition(Count - 1, TimeInterval, Total, CheckState, State, Weight)
    end.

-spec weight_transition(pos_integer(), pos_integer()) -> float().
weight_transition(CheckTimes, TotalTimes) ->
    0.8 + 0.4/TotalTimes * CheckTimes.
