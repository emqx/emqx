%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_ds_helper).

%% API:
-export([create_rr/1]).

%% internal exports:
-export([]).

-export_type([rr/0]).

%%================================================================================
%% Type declarations
%%================================================================================

-type item() :: {emqx_ds:stream_rank(), emqx_ds:stream()}.

-type rr() :: #{
    queue := #{term() => [{integer(), emqx_ds:stream()}]},
    active_ring := {[item()], [item()]}
}.

%%================================================================================
%% API funcions
%%================================================================================

-spec create_rr([item()]) -> rr().
create_rr(Streams) ->
    RR0 = #{latest_rank => #{}, active_ring => {[], []}},
    add_streams(RR0, Streams).

-spec add_streams(rr(), [item()]) -> rr().
add_streams(#{queue := Q0, active_ring := R0}, Streams) ->
    Q1 = lists:foldl(
        fun({{RankX, RankY}, Stream}, Acc) ->
            maps:update_with(RankX, fun(L) -> [{RankY, Stream} | L] end, Acc)
        end,
        Q0,
        Streams
    ),
    Q2 = maps:map(
        fun(_RankX, Streams1) ->
            lists:usort(Streams1)
        end,
        Q1
    ),
    #{queue => Q2, active_ring => R0}.

%%================================================================================
%% behavior callbacks
%%================================================================================

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================
