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

-module(ecpool).

-include("ecpool.hrl").

-export([ pool_spec/4
        , start_pool/3
        , start_sup_pool/3
        , stop_sup_pool/1
        , get_client/1
        , get_client/2
        , with_client/2
        , with_client/3
        , with_client/4
        , name/1
        , workers/1
        ]).

-export([set_reconnect_callback/2, add_reconnect_callback/2]).

-export_type([ pool_name/0
             , pool_type/0
             , option/0
             ]).

-type(pool_name() :: term()).

-type(pool_type() :: random | hash | round_robin).

-type(reconn_callback() :: {fun((pid()) -> term())}).

-type(option() :: {pool_size, pos_integer()}
                | {pool_type, pool_type()}
                | {auto_reconnect, false | pos_integer()}
                | {on_reconnect, reconn_callback()}
                | tuple()).

pool_spec(ChildId, Pool, Mod, Opts) ->
    #{id => ChildId,
      start => {?MODULE, start_pool, [Pool, Mod, Opts]},
      restart => permanent,
      shutdown => 5000,
      type => supervisor,
      modules => [ecpool_pool_sup]}.

%% @doc Start the pool sup.
-spec(start_pool(atom(), atom(), [option()]) -> {ok, pid()} | {error, term()}).
start_pool(Pool, Mod, Opts) when is_atom(Pool) ->
    ecpool_pool_sup:start_link(Pool, Mod, Opts).

%% @doc Start the pool supervised by ecpool_sup
start_sup_pool(Pool, Mod, Opts) when is_atom(Pool) ->
    ecpool_sup:start_pool(Pool, Mod, Opts).

%% @doc Start the pool supervised by ecpool_sup
stop_sup_pool(Pool) when is_atom(Pool) ->
    ecpool_sup:stop_pool(Pool).

%% @doc Get client/connection
-spec(get_client(atom()) -> pid()).
get_client(Pool) ->
    gproc_pool:pick_worker(name(Pool)).

%% @doc Get client/connection with hash key.
-spec(get_client(atom(), any()) -> pid()).
get_client(Pool, Key) ->
    gproc_pool:pick_worker(name(Pool), Key).

-spec(set_reconnect_callback(atom(), reconn_callback()) -> ok).
set_reconnect_callback(Pool, Callback) ->
    [ecpool_worker:set_reconnect_callback(Worker, Callback)
     || {_WorkerName, Worker} <- ecpool:workers(Pool)],
    ok.

-spec(add_reconnect_callback(atom(), reconn_callback()) -> ok).
add_reconnect_callback(Pool, Callback) ->
    [ecpool_worker:add_reconnect_callback(Worker, Callback)
     || {_WorkerName, Worker} <- ecpool:workers(Pool)],
    ok.

%% @doc Call the fun with client/connection
-spec(with_client(atom(), action()) -> any()).
with_client(Pool, Action) when is_atom(Pool) ->
    with_client(Pool, Action, direct).

-spec(with_client(atom(), action() | term(), action() | exec_mode()) -> any()).
with_client(Pool, Key, Action) when is_atom(Pool), is_function(Action) ->
    with_client(Pool, Key, Action, direct);

with_client(Pool, Action, Mode) when is_atom(Pool), is_function(Action) ->
    with_worker(gproc_pool:pick_worker(name(Pool)), Action, Mode).

-spec(with_client(atom(), any(), fun((Client :: pid()) -> term()), exec_mode()) -> any()).
with_client(Pool, Key, Action, Mode) when is_atom(Pool) ->
    with_worker(gproc_pool:pick_worker(name(Pool), Key), Action, Mode).

-spec with_worker(pid(), action(), exec_mode()) -> any().
with_worker(Worker, Action, direct) ->
    case ecpool_worker:client(Worker) of
        {ok, Client} -> Action(Client);
        {error, Reason} -> {error, Reason}
    end;
with_worker(Worker, Action, relay) ->
    ecpool_worker:exec(Worker, Action);
with_worker(Worker, Action, relay_async) ->
    ecpool_worker:exec_async(Worker, Action);
with_worker(Worker, Action, {relay_async, CallbackFun}) ->
    ecpool_worker:exec_async(Worker, Action, CallbackFun).

%% @doc Pool workers
workers(Pool) ->
    gproc_pool:active_workers(name(Pool)).

%% @doc ecpool name
name(Pool) -> {?MODULE, Pool}.

