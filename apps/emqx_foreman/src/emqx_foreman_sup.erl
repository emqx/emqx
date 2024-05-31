%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_foreman_sup).

-behaviour(supervisor).

%% API
-export([
    start_link/0,
    start_child/1,
    delete_child/1
]).

%% `supervisor' API
-export([init/1]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_child(ChildSpec) ->
    supervisor:start_child(?MODULE, ChildSpec).

delete_child(ChildId) ->
    case supervisor:terminate_child(?MODULE, ChildId) of
        ok ->
            supervisor:delete_child(?MODULE, ChildId);
        Error ->
            Error
    end.

%%------------------------------------------------------------------------------
%% `supervisor' API
%%------------------------------------------------------------------------------

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 1
    },
    ChildSpecs = [],
    {ok, {SupFlags, ChildSpecs}}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------
