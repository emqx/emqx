%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(ecpool_pool).

-behaviour(gen_server).

-import(proplists, [get_value/3]).

%% API Function Exports
-export([start_link/2]).

-export([info/1]).

%% gen_server Function Exports
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).

-record(state, {name, size, type}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec(start_link(ecpool:pool_name(), list(ecpool:option()))
      -> {ok, pid()} | {error, term()}).
start_link(Pool, Opts) ->
    gen_server:start_link(?MODULE, [Pool, Opts], []).

-spec(info(pid()) -> list()).
info(Pid) ->
    gen_server:call(Pid, info).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Pool, Opts]) ->
    Schedulers = erlang:system_info(schedulers),
    PoolSize = get_value(pool_size, Opts, Schedulers),
    PoolType = get_value(pool_type, Opts, random),
    ok = ensure_pool(ecpool:name(Pool), PoolType, [{size, PoolSize}]),
    ok = lists:foreach(
           fun(I) ->
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
    Workers = ecpool:workers(Pool),
    Info = [{pool_name, Pool},
            {pool_size, Size},
            {pool_type, Type},
            {workers, Workers}],
    {reply, Info, State};

handle_call(Req, _From, State) ->
    logger:error("[Pool] unexpected request: ~p", [Req]),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    logger:error("[Pool] unexpected msg: ~p", [Msg]),
    {noreply, State}.

handle_info(Info, State) ->
    logger:error("[Pool] unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #state{name = Pool, size = Size}) ->
    lists:foreach(
      fun(I) ->
              gproc_pool:remove_worker(ecpool:name(Pool), {Pool, I})
      end, lists:seq(1, Size)),
    gproc_pool:delete(ecpool:name(Pool)).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

