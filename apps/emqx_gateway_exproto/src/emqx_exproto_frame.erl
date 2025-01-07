%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
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

%% @doc The frame parser for ExProto
-module(emqx_exproto_frame).

-behaviour(emqx_gateway_frame).

-export([
    initial_parse_state/1,
    serialize_opts/0,
    parse/2,
    serialize_pkt/2,
    format/1,
    is_message/1,
    type/1
]).

initial_parse_state(_) ->
    #{}.

serialize_opts() ->
    #{}.

parse(Data, State) ->
    {ok, Data, <<>>, State}.

serialize_pkt(Data, _Opts) ->
    Data.

format(Data) ->
    io_lib:format("~p", [Data]).

is_message(_) -> true.

type(_) -> unknown.
