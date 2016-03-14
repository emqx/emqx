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

%% @doc Common Pool Supervisor
-module(emqttd_pool_sup).

-behaviour(supervisor).

%% API
-export([spec/1, spec/2, start_link/3, start_link/4]).

%% Supervisor callbacks
-export([init/1]).

-spec(spec(list()) -> supervisor:child_spec()).
spec(Args) ->
    spec(pool_sup, Args).

-spec(spec(any(), list()) -> supervisor:child_spec()).
spec(ChildId, Args) ->
    {ChildId, {?MODULE, start_link, Args},
        transient, infinity, supervisor, [?MODULE]}.

-spec(start_link(atom(), atom(), mfa()) -> {ok, pid()} | {error, any()}).
start_link(Pool, Type, MFA) ->
    Schedulers = erlang:system_info(schedulers),
    start_link(Pool, Type, Schedulers, MFA).

-spec(start_link(atom(), atom(), pos_integer(), mfa()) -> {ok, pid()} | {error, any()}).
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

