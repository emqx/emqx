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

-module(emqttd_pooler).

-behaviour(gen_server).

-include("emqttd_internal.hrl").

%% Start the pool supervisor
-export([start_link/0]).

%% API Exports 
-export([start_link/2, submit/1, async_submit/1]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {pool, id}).

%% @doc Start Pooler Supervisor.
start_link() ->
    emqttd_pool_sup:start_link(pooler, random, {?MODULE, start_link, []}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
-spec(start_link(atom(), pos_integer()) -> {ok, pid()} | ignore | {error, any()}).
start_link(Pool, Id) ->
    gen_server:start_link({local, ?PROC_NAME(?MODULE, Id)}, ?MODULE, [Pool, Id], []).

%% @doc Submit work to pooler
submit(Fun) -> gen_server:call(worker(), {submit, Fun}, infinity).

%% @doc Submit work to pooler asynchronously
async_submit(Fun) ->
    gen_server:cast(worker(), {async_submit, Fun}).

worker() ->
    gproc_pool:pick_worker(pooler).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Pool, Id]) ->
    ?GPROC_POOL(join, Pool, Id),
    {ok, #state{pool = Pool, id = Id}}.

handle_call({submit, Fun}, _From, State) ->
    {reply, run(Fun), State};

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast({async_submit, Fun}, State) ->
    try run(Fun)
    catch _:Error ->
        lager:error("Pooler Error: ~p, ~p", [Error, erlang:get_stacktrace()])
    end,
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{pool = Pool, id = Id}) ->
    ?GPROC_POOL(leave, Pool, Id), ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

run({M, F, A}) ->
    erlang:apply(M, F, A);
run(Fun) when is_function(Fun) ->
    Fun().

