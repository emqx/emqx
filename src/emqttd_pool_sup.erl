%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>. All Rights Reserved.
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
%%% @doc Common Pool Supervisor
%%%
%%% @author Feng Lee <feng@emqtt.io>
%%%-----------------------------------------------------------------------------
-module(emqttd_pool_sup).

-behaviour(supervisor).

%% API
-export([spec/1, spec/2, start_link/3, start_link/4]).

%% Supervisor callbacks
-export([init/1]).

-spec spec(list()) -> supervisor:child_spec().
spec(Args) ->
    spec(pool_sup, Args).

-spec spec(any(), list()) -> supervisor:child_spec().
spec(ChildId, Args) ->
    {ChildId, {?MODULE, start_link, Args},
        transient, infinity, supervisor, [?MODULE]}.

-spec start_link(atom(), atom(), mfa()) -> {ok, pid()} | {error, any()}.
start_link(Pool, Type, MFA) ->
    Schedulers = erlang:system_info(schedulers),
    start_link(Pool, Type, Schedulers, MFA).

-spec start_link(atom(), atom(), pos_integer(), mfa()) -> {ok, pid()} | {error, any()}.
start_link(Pool, Type, Size, MFA) ->
    supervisor:start_link({local, sup_name(Pool)}, ?MODULE, [Pool, Type, Size, MFA]).

sup_name(Pool) when is_atom(Pool) ->
    list_to_atom(atom_to_list(Pool) ++ "_pool_sup").

init([Pool, Type, Size, {M, F, Args}]) ->
    ensure_pool(Pool, Type, [{size, Size}]),
    {ok, {{one_for_one, 10, 3600}, [
        begin
            ensure_pool_worker(Pool, {Pool, I}, I),
            {{M, I}, {M, F, [Pool, I | Args]},
                transient, 5000, worker, [M]}
        end || I <- lists:seq(1, Size)]}}.

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

