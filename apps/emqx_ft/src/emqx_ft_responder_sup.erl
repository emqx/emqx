%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ft_responder_sup).

-export([start_link/0]).
-export([start_child/3]).

-behaviour(supervisor).
-export([init/1]).

-define(SUPERVISOR, ?MODULE).

%%

-spec start_link() -> {ok, pid()}.
start_link() ->
    supervisor:start_link({local, ?SUPERVISOR}, ?MODULE, []).

start_child(Key, RespFun, Timeout) ->
    supervisor:start_child(?SUPERVISOR, [Key, RespFun, Timeout]).

-spec init(_) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init(_) ->
    Flags = #{
        strategy => simple_one_for_one,
        intensity => 100,
        period => 100
    },
    ChildSpec = #{
        id => responder,
        start => {emqx_ft_responder, start_link, []},
        restart => temporary
    },
    {ok, {Flags, [ChildSpec]}}.
