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

-include("emqttd.hrl").

-define(HELPER, emqttd_pubsub_helper).

-define(CONCURRENCY_OPTS, [{read_concurrency, true}, {write_concurrency, true}]).

%% API
-export([start_link/0, pubsub_pool/0]).

%% Supervisor callbacks
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [emqttd_broker:env(pubsub)]).

pubsub_pool() ->
    hd([Pid|| {pubsub_pool, Pid, _, _} <- supervisor:which_children(?MODULE)]).

init([Env]) ->
    %% Create tabs
    create_tab(route), create_tab(reverse_route),

    %% PubSub Helper
    Helper = {helper, {?HELPER, start_link, [fun setstats/1]},
                permanent, infinity, worker, [?HELPER]},

    %% Router Pool Sup
    RouterMFA = {emqttd_router, start_link, [fun setstats/1, Env]},

    %% Pool_size / 2
    RouterSup = emqttd_pool_sup:spec(router_pool, [router, hash, router_pool(Env), RouterMFA]),

    %% PubSub Pool Sup
    PubSubMFA = {emqttd_pubsub, start_link, [fun setstats/1, Env]},
    PubSubSup = emqttd_pool_sup:spec(pubsub_pool, [pubsub, hash, pool_size(Env), PubSubMFA]),

    {ok, {{one_for_all, 10, 60}, [Helper, RouterSup, PubSubSup]}}.

create_tab(route) ->
    %% Route Table: Topic -> Pid1, Pid2, ..., PidN
    %% duplicate_bag: o(1) insert
    ensure_tab(route, [public, named_table, duplicate_bag | ?CONCURRENCY_OPTS]);

create_tab(reverse_route) ->
    %% Reverse Route Table: Pid -> Topic1, Topic2, ..., TopicN
    ensure_tab(reverse_route, [public, named_table, bag | ?CONCURRENCY_OPTS]).

ensure_tab(Tab, Opts) ->
    case ets:info(Tab, name) of
        undefined -> ets:new(Tab, Opts);
        _ -> ok
    end.

router_pool(Env) ->
    case pool_size(Env) div 2 of
        0 -> 1;
        I -> I
    end.

pool_size(Env) ->
    Schedulers = erlang:system_info(schedulers),
    proplists:get_value(pool_size, Env, Schedulers).

setstats(route) ->
    emqttd_stats:setstat('routes/count', ets:info(route, size));

setstats(reverse_route) ->
    emqttd_stats:setstat('routes/reverse', ets:info(reverse_route, size));

setstats(topic) ->
    emqttd_stats:setstats('topics/count', 'topics/max', mnesia:table_info(topic, size));

setstats(subscription) ->
    emqttd_stats:setstats('subscriptions/count', 'subscriptions/max',
                          mnesia:table_info(subscription, size)).

