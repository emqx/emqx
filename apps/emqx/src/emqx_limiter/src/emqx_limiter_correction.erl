%%--------------------------------------------------------------------
%% Copyright (c) 2019-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_limiter_correction).

%% API
-export([add/2]).

-type correction_value() :: #{
    correction := emqx_limiter_decimal:zero_or_float(),
    any() => any()
}.

-export_type([correction_value/0]).

%%--------------------------------------------------------------------
%%% API
%%--------------------------------------------------------------------
-spec add(number(), correction_value()) -> {integer(), correction_value()}.
add(Inc, #{correction := Correction} = Data) ->
    FixedInc = Inc + Correction,
    IntInc = erlang:floor(FixedInc),
    {IntInc, Data#{correction := FixedInc - IntInc}}.
