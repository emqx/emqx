%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2015 eMQTT.IO, All Rights Reserved.
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
%%% emqttd client manager supervisor.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_cm_sup).

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd.hrl").

-behaviour(supervisor).

%% API
-export([start_link/0, table/0]).

%% Supervisor callbacks
-export([init/1]).

-define(CLIENT_TAB, mqtt_client).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

table() -> ?CLIENT_TAB.

init([]) ->
    TabId = ets:new(?CLIENT_TAB, [set, named_table, public,
                                  {write_concurrency, true}]),
    Schedulers = erlang:system_info(schedulers),
    gproc_pool:new(cm_pool, hash, [{size, Schedulers}]),
    StatsFun = emqttd_stats:statsfun('clients/count', 'clients/max'),
    Children = lists:map(
                 fun(I) ->
                    Name = {emqttd_cm, I},
                    gproc_pool:add_worker(cm_pool, Name, I),
                    {Name, {emqttd_cm, start_link, [I, TabId, StatsFun]},
                        permanent, 10000, worker, [emqttd_cm]}
                 end, lists:seq(1, Schedulers)),
    {ok, {{one_for_all, 10, 100}, Children}}.


