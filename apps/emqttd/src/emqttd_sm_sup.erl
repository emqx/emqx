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
%%% emqttd session manager supervisor.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_sm_sup).

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd.hrl").

%% API
-export([start_link/0]).

-behaviour(supervisor).

%% Supervisor callbacks
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    ets:new(emqttd_sm:table(), [set, named_table, public,
                                {write_concurrency, true}]),
    Schedulers = erlang:system_info(schedulers),
    gproc_pool:new(emqttd_sm:pool(), hash, [{size, Schedulers}]),
    StatsFun = emqttd_stats:statsfun('sessions/count', 'sessions/max'),
    Children = lists:map(
                 fun(I) ->
                    Name = {emqttd_sm, I},
                    gproc_pool:add_worker(emqttd_sm:pool(), Name, I),
                    {Name, {emqttd_sm, start_link, [I, StatsFun]},
                                permanent, 10000, worker, [emqttd_sm]}
                 end, lists:seq(1, Schedulers)),
    {ok, {{one_for_all, 10, 100}, Children}}.


