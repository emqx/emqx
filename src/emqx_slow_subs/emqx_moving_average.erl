%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @see https://en.wikipedia.org/wiki/Moving_average

-module(emqx_moving_average).

%% API
-export([new/0, new/1, new/2, update/2]).

-type type() :: cumulative
              | exponential.

-type ema() :: #{ type := exponential
                , average := 0 | float()
                , coefficient := float()
                }.

-type cma() :: #{ type := cumulative
                , average := 0 | float()
                , count := non_neg_integer()
                }.

-type moving_average() :: ema()
                        | cma().

-define(DEF_EMA_ARG, #{period => 10}).
-define(DEF_AVG_TYPE, exponential).

-export_type([type/0, moving_average/0, ema/0, cma/0]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
-spec new() -> moving_average().
new() ->
    new(?DEF_AVG_TYPE, #{}).

-spec new(type()) -> moving_average().
new(Type) ->
    new(Type, #{}).

-spec new(type(), Args :: map()) -> moving_average().
new(cumulative, _) ->
    #{ type => cumulative
     , average => 0
     , count => 0
     };

new(exponential, Arg) ->
    #{period := Period} = maps:merge(?DEF_EMA_ARG, Arg),
    #{ type => exponential
     , average => 0
       %% coefficient = 2/(N+1) is a common convention, see the wiki link for details
     , coefficient => 2 / (Period + 1)
     }.

-spec update(number(), moving_average()) -> moving_average().

update(Val, #{average := 0} = Avg) ->
    Avg#{average := Val};

update(Val, #{ type := cumulative
             , average := Average
             , count := Count} = CMA) ->
    NewCount = Count + 1,
    CMA#{average := (Count * Average + Val) / NewCount,
         count := NewCount};

update(Val, #{ type := exponential
             , average := Average
             , coefficient := Coefficient} = EMA) ->
    EMA#{average := Coefficient * Val + (1 - Coefficient) * Average}.

%%--------------------------------------------------------------------
%%  Internal functions
%%--------------------------------------------------------------------
