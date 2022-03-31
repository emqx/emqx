%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc The Gateway frame behavior
%%
%% This module does not export any functions at the moment.
%% It is only used to standardize the implement of emqx_foo_frame.erl
%% module if it integrated with emqx_gateway_conn module
%%
-module(emqx_gateway_frame).

-type parse_state() :: map().

-type frame() :: any().

-type parse_result() ::
    {ok, frame(), Rest :: binary(), NewState :: parse_state()}
    | {more, NewState :: parse_state()}.

-type serialize_options() :: map().

-export_type([
    parse_state/0,
    parse_result/0,
    serialize_options/0,
    frame/0
]).

%% Callbacks

%% @doc Initial the frame parser states
-callback initial_parse_state(map()) -> parse_state().

%% @doc
-callback serialize_opts() -> serialize_options().

%% @doc
-callback serialize_pkt(Frame :: any(), serialize_options()) -> iodata().

%% @doc
-callback parse(binary(), parse_state()) -> parse_result().

%% @doc
-callback format(Frame :: any()) -> string().

%% @doc
-callback type(Frame :: any()) -> atom().

%% @doc
-callback is_message(Frame :: any()) -> boolean().
