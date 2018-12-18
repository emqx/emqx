%%% The MIT License (MIT)
%%%  
%%% Copyright (c) 2018 EMQ X
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
%%%
%%% 
%%% @author Feng Lee <feng@emqx.io>

-module(ecpool_worker_sup).

-behaviour(supervisor).

-export([start_link/3, init/1]).

start_link(Pool, Mod, Opts) when is_atom(Pool) ->
    supervisor:start_link(?MODULE, [Pool, Mod, Opts]).

init([Pool, Mod, Opts]) ->
    WorkerSpec = fun(Id) ->
        {{worker, Id}, {ecpool_worker, start_link, [Pool, Id, Mod, Opts]},
            transient, 5000, worker, [ecpool_worker, Mod]}
    end,
    Workers = [WorkerSpec(I) || I <- lists:seq(1, pool_size(Opts))],
    {ok, { {one_for_one, 10, 60}, Workers} }.

pool_size(Opts) ->
    Schedulers = erlang:system_info(schedulers),
    proplists:get_value(pool_size, Opts, Schedulers).

