%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_router_utils).

-include("emqx.hrl").

-export([
    delete_trie_route/2,
    insert_trie_route/2,
    maybe_trans/3
]).

insert_trie_route(RouteTab, Route = #route{topic = Topic}) ->
    case mnesia:wread({RouteTab, Topic}) of
        [] -> emqx_trie:insert(Topic);
        _ -> ok
    end,
    mnesia:write(RouteTab, Route, sticky_write).

delete_trie_route(RouteTab, Route = #route{topic = Topic}) ->
    case mnesia:wread({RouteTab, Topic}) of
        [R] when R =:= Route ->
            %% Remove route and trie
            ok = mnesia:delete_object(RouteTab, Route, sticky_write),
            emqx_trie:delete(Topic);
        [_ | _] ->
            %% Remove route only
            mnesia:delete_object(RouteTab, Route, sticky_write);
        [] ->
            ok
    end.

%% @private
-spec maybe_trans(function(), list(any()), Shard :: atom()) -> ok | {error, term()}.
maybe_trans(Fun, Args, Shard) ->
    case emqx:get_config([broker, perf, route_lock_type]) of
        key ->
            trans(Fun, Args, Shard);
        global ->
            %% Assert:

            %% TODO: do something smarter than just crash
            mnesia = mria_rlog:backend(),
            lock_router(Shard),
            try
                mnesia:sync_dirty(Fun, Args)
            after
                unlock_router(Shard)
            end;
        tab ->
            trans(
                fun() ->
                    emqx_trie:lock_tables(),
                    apply(Fun, Args)
                end,
                [],
                Shard
            )
    end.

%% The created fun only terminates with explicit exception
-dialyzer({nowarn_function, [trans/3]}).

-spec trans(function(), list(any()), atom()) -> ok | {error, term()}.
trans(Fun, Args, Shard) ->
    {WPid, RefMon} =
        spawn_monitor(
            %% NOTE: this is under the assumption that crashes in Fun
            %% are caught by mnesia:transaction/2.
            %% Future changes should keep in mind that this process
            %% always exit with database write result.
            fun() ->
                Res =
                    case mria:transaction(Shard, Fun, Args) of
                        {atomic, Ok} -> Ok;
                        {aborted, Reason} -> {error, Reason}
                    end,
                exit({shutdown, Res})
            end
        ),
    %% Receive a 'shutdown' exit to pass result from the short-lived process.
    %% so the receive below can be receive-mark optimized by the compiler.
    %%
    %% If the result is sent as a regular message, we'll have to
    %% either demonitor (with flush which is essentially a 'receive' since
    %% the process is no longer alive after the result has been received),
    %% or use a plain 'receive' to drain the normal 'DOWN' message.
    %% However the compiler does not optimize this second 'receive'.
    receive
        {'DOWN', RefMon, process, WPid, Info} ->
            case Info of
                {shutdown, Result} -> Result;
                _ -> {error, {trans_crash, Info}}
            end
    end.

lock_router(Shard) ->
    %% if Retry is not 0, global:set_lock could sleep a random time up to 8s.
    %% Considering we have a limited number of brokers, it is safe to use sleep 1 ms.
    case global:set_lock({{?MODULE, Shard}, self()}, [node() | nodes()], 0) of
        false ->
            %% Force to sleep 1ms instead.
            timer:sleep(1),
            lock_router(Shard);
        true ->
            ok
    end.

unlock_router(Shard) ->
    global:del_lock({{?MODULE, Shard}, self()}).
