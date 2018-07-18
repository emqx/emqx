%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_broker_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(TAB_OPTS, [public, {read_concurrency, true}, {write_concurrency, true}]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%------------------------------------------------------------------------------
%% Supervisor callbacks
%%------------------------------------------------------------------------------

init([]) ->
    %% Create the pubsub tables
    ok = lists:foreach(fun create_tab/1, [subscription, subscriber, suboption]),

    %% Shared subscription
    SharedSub = {shared_sub, {emqx_shared_sub, start_link, []},
                 permanent, 5000, worker, [emqx_shared_sub]},

    %% Broker helper
    Helper = {broker_helper, {emqx_broker_helper, start_link, []},
              permanent, 5000, worker, [emqx_broker_helper]},

    %% Broker pool
    BrokerPool = emqx_pool_sup:spec(emqx_broker_pool,
                                    [broker, hash, emqx_vm:schedulers() * 2,
                                     {emqx_broker, start_link, []}]),

    {ok, {{one_for_all, 0, 1}, [SharedSub, Helper, BrokerPool]}}.

%%------------------------------------------------------------------------------
%% Create tables
%%------------------------------------------------------------------------------

create_tab(suboption) ->
    %% Suboption: {Topic, Sub} -> [{qos, 1}]
    emqx_tables:new(emqx_suboption, [set | ?TAB_OPTS]);

create_tab(subscriber) ->
    %% Subscriber: Topic -> Sub1, Sub2, Sub3, ..., SubN
    %% duplicate_bag: o(1) insert
    emqx_tables:new(emqx_subscriber, [duplicate_bag | ?TAB_OPTS]);

create_tab(subscription) ->
    %% Subscription: Sub -> Topic1, Topic2, Topic3, ..., TopicN
    %% bag: o(n) insert
    emqx_tables:new(emqx_subscription, [bag | ?TAB_OPTS]).

