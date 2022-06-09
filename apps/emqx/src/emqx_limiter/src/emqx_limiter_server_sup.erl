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

-module(emqx_limiter_server_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, start/1, start/2, stop/1]).

%% Supervisor callbacks
-export([init/1]).

%%--------------------------------------------------------------------
%%  API functions
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%% @end
%%--------------------------------------------------------------------
-spec start_link() ->
    {ok, Pid :: pid()}
    | {error, {already_started, Pid :: pid()}}
    | {error, {shutdown, term()}}
    | {error, term()}
    | ignore.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start(emqx_limiter_schema:limiter_type()) -> _.
start(Type) ->
    Spec = make_child(Type),
    supervisor:start_child(?MODULE, Spec).

-spec start(emqx_limiter_schema:limiter_type(), hocons:config()) -> _.
start(Type, Cfg) ->
    Spec = make_child(Type, Cfg),
    supervisor:start_child(?MODULE, Spec).

stop(Type) ->
    Id = emqx_limiter_server:name(Type),
    _ = supervisor:terminate_child(?MODULE, Id),
    supervisor:delete_child(?MODULE, Id).

%%--------------------------------------------------------------------
%%  Supervisor callbacks
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart intensity, and child
%% specifications.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) ->
    {ok, {SupFlags :: supervisor:sup_flags(), [ChildSpec :: supervisor:child_spec()]}}
    | ignore.
init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 3600
    },

    {ok, {SupFlags, childs()}}.

%%--==================================================================
%%  Internal functions
%%--==================================================================
make_child(Type) ->
    Cfg = emqx:get_config([limiter, Type]),
    make_child(Type, Cfg).

make_child(Type, Cfg) ->
    Id = emqx_limiter_server:name(Type),
    #{
        id => Id,
        start => {emqx_limiter_server, start_link, [Type, Cfg]},
        restart => transient,
        shutdown => 5000,
        type => worker,
        modules => [emqx_limiter_server]
    }.

childs() ->
    [make_child(Type) || Type <- emqx_limiter_schema:types()].
