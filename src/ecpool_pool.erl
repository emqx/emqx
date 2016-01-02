%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2015 eMQTT.IO, All Rights Reserved.
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
%%% @doc Wrap gproc_pool.
%%%
%%% @author Feng Lee <feng@emqtt.io>
%%%-----------------------------------------------------------------------------

-module(ecpool_pool).

-behaviour(gen_server).

-import(proplists, [get_value/3]).

%% API Function Exports
-export([start_link/2, info/1]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {name, size, type}).

%%%=============================================================================
%%% API
%%%=============================================================================

start_link(Pool, Opts) ->
    gen_server:start_link(?MODULE, [Pool, Opts], []).

info(Pid) ->
    gen_server:call(Pid, info).

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

init([Pool, Opts]) ->
    Schedulers = erlang:system_info(schedulers),
    PoolSize = get_value(pool_size, Opts, Schedulers),
    PoolType = get_value(pool_type, Opts, random),
    ensure_pool(ecpool:name(Pool), PoolType, [{size, PoolSize}]),
    lists:foreach(fun(I) ->
            ensure_pool_worker(ecpool:name(Pool), {Pool, I}, I)
        end, lists:seq(1, PoolSize)),
    {ok, #state{name = Pool, size = PoolSize, type = PoolType}}.

ensure_pool(Pool, Type, Opts) ->
    try gproc_pool:new(Pool, Type, Opts)
    catch
        error:exists -> ok
    end.

ensure_pool_worker(Pool, Name, Slot) ->
    try gproc_pool:add_worker(Pool, Name, Slot)
    catch
        error:exists -> ok
    end.

handle_call(info, _From, State = #state{name = Pool, size = Size, type = Type}) ->
    Info = [{pool_name, Pool}, {pool_size, Size},
            {pool_type, Type}, {workers, ecpool:workers(Pool)}],
    {reply, Info, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unexpected_req}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{name = Pool, size = Size}) ->
    lists:foreach(fun(I) ->
                gproc_pool:remove_worker(ecpool:name(Pool), {Pool, I})
        end, lists:seq(1, Size)),
    gproc_pool:delete(ecpool:name(Pool)).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

