%%%-----------------------------------------------------------------------------
%%% @Copyright (C) 2012-2015, Feng Lee <feng@emqtt.io>
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
%%% @doc
%%% emqttd pooler supervisor.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_pooler_sup).

-author('feng@emqtt.io').

-include("emqttd.hrl").

-behaviour(supervisor).

%% API
-export([start_link/0, start_link/1]).

%% Supervisor callbacks
-export([init/1]).

start_link() ->
    start_link(erlang:system_info(schedulers)).

start_link(PoolSize) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [PoolSize]).

init([PoolSize]) ->
    gproc_pool:new(pooler, random, [{size, PoolSize}]),
    Children = lists:map(
                 fun(I) ->
                         gproc_pool:add_worker(pooler, {pooler, I}, I),
                         {{emqttd_pooler, I},
                            {emqttd_pooler, start_link, [I]},
                                permanent, 5000, worker, [emqttd_pooler]}
                 end, lists:seq(1, PoolSize)),
    {ok, {{one_for_all, 10, 100}, Children}}.

