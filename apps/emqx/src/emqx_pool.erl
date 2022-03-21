%%--------------------------------------------------------------------
%% Copyright (c) 2018-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_pool).

-behaviour(gen_server).

-include("logger.hrl").
-include("types.hrl").

%% APIs
-export([start_link/2]).

-export([
    submit/1,
    submit/2,
    async_submit/1,
    async_submit/2
]).

-ifdef(TEST).
-export([worker/0, flush_async_tasks/0]).
-endif.

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(POOL, ?MODULE).

-type task() :: fun() | mfa() | {fun(), Args :: list(any())}.

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

%% @doc Start pool.
-spec start_link(atom(), pos_integer()) -> startlink_ret().
start_link(Pool, Id) ->
    gen_server:start_link(
        {local, emqx_misc:proc_name(?MODULE, Id)},
        ?MODULE,
        [Pool, Id],
        [{hibernate_after, 1000}]
    ).

%% @doc Submit work to the pool.
-spec submit(task()) -> any().
submit(Task) ->
    call({submit, Task}).

-spec submit(fun(), list(any())) -> any().
submit(Fun, Args) ->
    call({submit, {Fun, Args}}).

%% @private
call(Req) ->
    gen_server:call(worker(), Req, infinity).

%% @doc Submit work to the pool asynchronously.
-spec async_submit(task()) -> ok.
async_submit(Task) ->
    cast({async_submit, Task}).

-spec async_submit(fun(), list(any())) -> ok.
async_submit(Fun, Args) ->
    cast({async_submit, {Fun, Args}}).

%% @private
cast(Msg) ->
    gen_server:cast(worker(), Msg).

%% @private
worker() ->
    gproc_pool:pick_worker(?POOL).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Pool, Id]) ->
    true = gproc_pool:connect_worker(Pool, {Pool, Id}),
    {ok, #{pool => Pool, id => Id}}.

handle_call({submit, Task}, _From, State) ->
    {reply, catch run(Task), State};
handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast({async_submit, Task}, State) ->
    try
        run(Task)
    catch
        Error:Reason:Stacktrace ->
            ?SLOG(error, #{
                msg => "async_submit_error",
                exception => Error,
                reason => Reason,
                stacktrace => Stacktrace
            })
    end,
    {noreply, State};
handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Msg}),
    {noreply, State}.

handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

terminate(_Reason, #{pool := Pool, id := Id}) ->
    gproc_pool:disconnect_worker(Pool, {Pool, Id}).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

run({M, F, A}) ->
    erlang:apply(M, F, A);
run({F, A}) when is_function(F), is_list(A) ->
    erlang:apply(F, A);
run(Fun) when is_function(Fun) ->
    Fun().

-ifdef(TEST).
%% This help function creates a large enough number of async tasks
%% to force flush the pool workers.
%% The number of tasks should be large enough to ensure all workers have
%% the chance to work on at least one of the tasks.
flush_async_tasks() ->
    Ref = make_ref(),
    Self = self(),
    L = lists:seq(1, 997),
    lists:foreach(fun(I) -> emqx_pool:async_submit(fun() -> Self ! {done, Ref, I} end, []) end, L),
    lists:foreach(
        fun(I) ->
            receive
                {done, Ref, I} -> ok
            end
        end,
        L
    ).
-endif.
