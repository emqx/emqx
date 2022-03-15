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

-module(emqx_session_router_worker_sup).

-behaviour(supervisor).

-export([start_link/1]).

-export([
    abort_worker/1,
    start_worker/2
]).

-export([init/1]).

start_link(SessionTab) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, SessionTab).

start_worker(SessionID, RemotePid) ->
    supervisor:start_child(?MODULE, [
        #{
            session_id => SessionID,
            remote_pid => RemotePid
        }
    ]).

abort_worker(Pid) ->
    supervisor:terminate_child(?MODULE, Pid).

%%--------------------------------------------------------------------
%% Supervisor callbacks
%%--------------------------------------------------------------------

init(SessionTab) ->
    %% Resume worker
    Worker = #{
        id => session_router_worker,
        start => {emqx_session_router_worker, start_link, [SessionTab]},
        restart => transient,
        shutdown => 2000,
        type => worker,
        modules => [emqx_session_router_worker]
    },
    Spec = #{
        strategy => simple_one_for_one,
        intensity => 1,
        period => 5
    },

    {ok, {Spec, [Worker]}}.
