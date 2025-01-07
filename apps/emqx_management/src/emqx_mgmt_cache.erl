%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_mgmt_cache).

-behaviour(gen_server).

-define(SYS_MEMORY_KEY, sys_memory).
-define(EXPIRED_MS, 3000).
%% -100ms to early update cache
-define(REFRESH_MS, ?EXPIRED_MS - 100).
-define(DEFAULT_BAD_MEMORY, {0, 0}).

-export([start_link/0, get_sys_memory/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

get_sys_memory() ->
    case get_memory_from_cache() of
        {ok, CacheMem} ->
            erlang:send(?MODULE, refresh_sys_memory),
            CacheMem;
        stale ->
            get_sys_memory_sync()
    end.

get_sys_memory_sync() ->
    try
        gen_server:call(?MODULE, get_sys_memory, ?EXPIRED_MS)
    catch
        exit:{timeout, _} ->
            ?DEFAULT_BAD_MEMORY
    end.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    _ = ets:new(?MODULE, [set, named_table, public, {keypos, 1}]),
    {ok, #{latest_refresh => 0}}.

handle_call(get_sys_memory, _From, State) ->
    {Mem, NewState} = refresh_sys_memory(State),
    {reply, Mem, NewState};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(refresh_sys_memory, State) ->
    {_, NewState} = refresh_sys_memory(State),
    {noreply, NewState};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

refresh_sys_memory(State = #{latest_refresh := LatestRefresh}) ->
    Now = now_millisecond(),
    case Now - LatestRefresh >= ?REFRESH_MS of
        true ->
            do_refresh_sys_memory(Now, State);
        false ->
            case get_memory_from_cache() of
                stale -> do_refresh_sys_memory(Now, State);
                {ok, Mem} -> {Mem, State}
            end
    end.

do_refresh_sys_memory(RefreshAt, State) ->
    NewMem = load_ctl:get_sys_memory(),
    NewExpiredAt = now_millisecond() + ?EXPIRED_MS,
    ets:insert(?MODULE, {?SYS_MEMORY_KEY, {NewMem, NewExpiredAt}}),
    {NewMem, State#{latest_refresh => RefreshAt}}.

get_memory_from_cache() ->
    case ets:lookup(?MODULE, ?SYS_MEMORY_KEY) of
        [] ->
            stale;
        [{_, {Mem, ExpiredAt}}] ->
            case now_millisecond() < ExpiredAt of
                true -> {ok, Mem};
                false -> stale
            end
    end.

now_millisecond() ->
    erlang:system_time(millisecond).
