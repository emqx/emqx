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
%%% @doc ecpool supervisor.
%%%
%%% @author Feng Lee <feng@emqtt.io>
%%%-----------------------------------------------------------------------------

-module(ecpool_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, start_pool/3, stop_pool/1, pools/0, pool/1]).

%% Supervisor callbacks
-export([init/1]).

%% @doc Start supervisor.
-spec start_link() -> {ok, pid()} | {error, any()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_pool(Pool, Mod, Opts) when is_atom(Pool) ->
    supervisor:start_child(?MODULE, pool_spec(Pool, Mod, Opts)).

-spec stop_pool(Pool :: atom()) -> ok | {error, any()}.
stop_pool(Pool) when is_atom(Pool) ->
    ChildId = child_id(Pool),
	case supervisor:terminate_child(?MODULE, ChildId) of
        ok ->
            supervisor:delete_child(?MODULE, ChildId);
        {error, Reason} ->
            {error, Reason}
	end.

%% @doc All Pools supervisored by ecpool_sup.
-spec pools() -> [{atom(), pid()}].
pools() ->
    [{Pool, Pid} || {{pool_sup, Pool}, Pid, supervisor, _}
                    <- supervisor:which_children(?MODULE)].

%% @doc Find a pool.
-spec pool(atom()) -> undefined | pid().
pool(Pool) when is_atom(Pool) ->
    ChildId = child_id(Pool),
    case [Pid || {Id, Pid, supervisor, _} <- supervisor:which_children(?MODULE), Id =:= ChildId] of
        [] -> undefined;
        L  -> hd(L)
    end.

%%%=============================================================================
%%% Supervisor callbacks
%%%=============================================================================

init([]) ->
    {ok, { {one_for_one, 10, 100}, []} }.

pool_spec(Pool, Mod, Opts) ->
    {child_id(Pool),
        {ecpool_pool_sup, start_link, [Pool, Mod, Opts]},
            transient, infinity, supervisor, [ecpool_pool_sup]}.

child_id(Pool) -> {pool_sup, Pool}.

