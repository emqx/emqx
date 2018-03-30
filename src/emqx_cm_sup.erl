%%--------------------------------------------------------------------
%% Copyright © 2013-2018 EMQ Inc. All rights reserved.
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

-module(emqx_cm_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %% Create table
    lists:foreach(fun create_tab/1, [client, client_stats, client_attrs]),

    StatsFun = emqx_stats:statsfun('clients/count', 'clients/max'),
    
    CM = {emqx_cm, {emqx_cm, start_link, [StatsFun]},
          permanent, 5000, worker, [emqx_cm]},

    {ok, {{one_for_all, 10, 3600}, [CM]}}.

create_tab(Tab) ->
    emqx_tables:create(Tab, [public, ordered_set, named_table, {write_concurrency, true}]).

