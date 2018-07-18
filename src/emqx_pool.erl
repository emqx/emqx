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

-module(emqx_pool).

-behaviour(gen_server).

-export([start_link/0, start_link/2]).
-export([submit/1, async_submit/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-record(state, {pool, id}).

-define(POOL, ?MODULE).

%% @doc Start pooler supervisor.
start_link() ->
    emqx_pool_sup:start_link(?POOL, random, {?MODULE, start_link, []}).

%% @doc Start pool
-spec(start_link(atom(), pos_integer()) -> {ok, pid()} | ignore | {error, term()}).
start_link(Pool, Id) ->
    gen_server:start_link({local, emqx_misc:proc_name(?MODULE, Id)}, ?MODULE, [Pool, Id], []).

%% @doc Submit work to the pool
-spec(submit(fun()) -> any()).
submit(Fun) ->
    gen_server:call(worker(), {submit, Fun}, infinity).

%% @doc Submit work to the pool asynchronously
-spec(async_submit(fun()) -> ok).
async_submit(Fun) ->
    gen_server:cast(worker(), {async_submit, Fun}).

worker() ->
    gproc_pool:pick_worker(pool).

%%-----------------------------------------------------------------------------
%% gen_server callbacks
%%-----------------------------------------------------------------------------

init([Pool, Id]) ->
    true = gproc_pool:connect_worker(Pool, {Pool, Id}),
    {ok, #state{pool = Pool, id = Id}}.

handle_call({submit, Fun}, _From, State) ->
    {reply, catch run(Fun), State};

handle_call(Req, _From, State) ->
    emqx_logger:error("[Pool] unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast({async_submit, Fun}, State) ->
    try run(Fun)
    catch _:Error:Stacktrace ->
        emqx_logger:error("[Pool] error: ~p, ~p", [Error, Stacktrace])
    end,
    {noreply, State};

handle_cast(Msg, State) ->
    emqx_logger:error("[Pool] unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info(Info, State) ->
    emqx_logger:error("[Pool] unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #state{pool = Pool, id = Id}) ->
    true = gproc_pool:disconnect_worker(Pool, {Pool, Id}).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%-----------------------------------------------------------------------------
%% Internal functions
%%-----------------------------------------------------------------------------

run({M, F, A}) ->
    erlang:apply(M, F, A);
run(Fun) when is_function(Fun) ->
    Fun().

