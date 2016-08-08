%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
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

%% @doc PubSub Supervisor.
-module(emqttd_pubsub_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, pubsub_pool/0]).

%% Supervisor callbacks
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [emqttd_conf:pubsub()]).

pubsub_pool() ->
    hd([Pid || {pubsub_pool, Pid, _, _} <- supervisor:which_children(?MODULE)]).

init([Env]) ->
    {ok, PubSub} = emqttd:conf(pubsub_adapter),
    PubSubMFA = {PubSub, start_link, [Env]},
    PoolArgs = [pubsub, hash, pool_size(Env), PubSubMFA],
    PubSubPoolSup = emqttd_pool_sup:spec(pubsub_pool, PoolArgs),
    {ok, { {one_for_all, 10, 3600}, [PubSubPoolSup]} }.

pool_size(Env) ->
    Schedulers = erlang:system_info(schedulers),
    proplists:get_value(pool_size, Env, Schedulers).

