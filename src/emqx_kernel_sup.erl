%%%===================================================================
%%% Copyright (c) 2013-2018 EMQ Inc. All rights reserved.
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%===================================================================

-module(emqx_kernel_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {{one_for_one, 10, 100},
          [child_spec(emqx_pool, supervisor),
           child_spec(emqx_alarm, worker),
           child_spec(emqx_hooks, worker),
           child_spec(emqx_stats, worker),
           child_spec(emqx_metrics, worker),
           child_spec(emqx_ctl, worker),
           child_spec(emqx_tracer, worker)]}}.

child_spec(M, worker) ->
    {M, {M, start_link, []}, permanent, 5000, worker, [M]};
child_spec(M, supervisor) ->
    {M, {M, start_link, []},
     permanent, infinity, supervisor, [M]}.

