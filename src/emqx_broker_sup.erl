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

-module(emqx_broker_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(CONCURRENCY_OPTS, [{read_concurrency, true}, {write_concurrency, true}]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%--------------------------------------------------------------------
%% Supervisor callbacks
%%--------------------------------------------------------------------

init([]) ->
    %% Create the pubsub tables
    create_tabs(),

    %% Shared pubsub
    Shared = {shared_pubsub, {emqx_shared_pubsub, start_link, []},
              permanent, 5000, worker, [emqx_shared_pubsub]},

    %% Broker helper
    Helper = {broker_helper, {emqx_broker_helper, start_link, [stats_fun()]},
              permanent, 5000, worker, [emqx_broker_helper]},

    %% Broker pool
    PoolArgs = [broker, hash, emqx_sys:schedulers() * 2,
                {emqx_broker, start_link, []}],

    PoolSup = emqx_pool_sup:spec(broker_pool, PoolArgs),

    {ok, {{one_for_all, 0, 3600}, [Shared, Helper, PoolSup]}}.

%%--------------------------------------------------------------------
%% Create tables
%%--------------------------------------------------------------------

create_tabs() ->
    lists:foreach(fun create_tab/1, [subscription, subscriber, suboption]).

create_tab(suboption) ->
    %% Suboption: {Topic, Sub} -> [{qos, 1}]
    ensure_tab(suboption, [set | ?CONCURRENCY_OPTS]);

create_tab(subscriber) ->
    %% Subscriber: Topic -> Sub1, Sub2, Sub3, ..., SubN
    %% duplicate_bag: o(1) insert
    ensure_tab(subscriber, [duplicate_bag | ?CONCURRENCY_OPTS]);

create_tab(subscription) ->
    %% Subscription: Sub -> Topic1, Topic2, Topic3, ..., TopicN
    %% bag: o(n) insert
    ensure_tab(subscription, [bag | ?CONCURRENCY_OPTS]).

ensure_tab(Tab, Opts) ->
    case ets:info(Tab, name) of
        undefined ->
            ets:new(Tab, lists:usort([public, named_table | Opts]));
        Tab -> Tab
    end.

%%--------------------------------------------------------------------
%% Stats function
%%--------------------------------------------------------------------

stats_fun() ->
    fun() ->
        emqx_stats:setstat('subscribers/count', 'subscribers/max',
                           ets:info(subscriber, size)),
        emqx_stats:setstat('subscriptions/count', 'subscriptions/max',
                           ets:info(subscription, size))
    end.

