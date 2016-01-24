%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2016 eMQTT.IO, All Rights Reserved.
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc PubSub Supervisor.
%%%
%%% @author Feng Lee <feng@emqtt.io>
%%%-----------------------------------------------------------------------------
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
    RouterSup = emqttd_pool_sup:spec(router_pool, [router, hash, pool_size(Env) div 2, RouterMFA]),

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

