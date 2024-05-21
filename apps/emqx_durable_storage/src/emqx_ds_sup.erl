%%--------------------------------------------------------------------
%% Copyright (c) 2022-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_ds_sup).

-behaviour(supervisor).

%% API:
-export([start_link/0, attach_backend/2]).

%% behaviour callbacks:
-export([init/1]).

%%================================================================================
%% Type declarations
%%================================================================================

-define(SUP, ?MODULE).

%%================================================================================
%% API functions
%%================================================================================

-spec start_link() -> {ok, pid()}.
start_link() ->
    supervisor:start_link({local, ?SUP}, ?MODULE, top).

%% @doc Attach a child backend-specific supervisor to the top
%% application supervisor, if not yet present
-spec attach_backend(_BackendId, {module(), atom(), list()}) ->
    {ok, pid()} | {error, _}.
attach_backend(Backend, Start) ->
    Spec = #{
        id => Backend,
        start => Start,
        significant => false,
        shutdown => infinity,
        type => supervisor
    },
    case supervisor:start_child(?SUP, Spec) of
        {ok, Pid} ->
            {ok, Pid};
        {error, {already_started, Pid}} ->
            {ok, Pid};
        {error, Err} ->
            {error, Err}
    end.

%%================================================================================
%% behaviour callbacks
%%================================================================================

init(top) ->
    Children = [],
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 1
    },
    {ok, {SupFlags, Children}}.

%%================================================================================
%% Internal functions
%%================================================================================
