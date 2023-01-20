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

-module(emqx_ft_assembler_sup).

-export([start_link/1]).
-export([start_child/3]).

-behaviour(supervisor).
-export([init/1]).

-define(REF(ID), {via, gproc, {n, l, {?MODULE, ID}}}).

start_link(ID) ->
    supervisor:start_link(?REF(ID), ?MODULE, []).

start_child(ID, Storage, Transfer) ->
    Childspec = #{
        id => {Storage, Transfer},
        start => {emqx_ft_assembler, start_link, [Storage, Transfer]},
        restart => transient
    },
    supervisor:start_child(?REF(ID), Childspec).

init(_) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 100,
        period => 1000
    },
    {ok, SupFlags, []}.
