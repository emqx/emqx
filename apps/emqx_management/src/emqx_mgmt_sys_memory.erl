%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_mgmt_sys_memory).

-behaviour(gen_server).
-define(SYS_MEMORY_CACHE_KEY, ?MODULE).
-define(TIMEOUT, 2200).

-export([start_link/0, get_sys_memory/0, get_sys_memory/1]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

get_sys_memory() ->
    get_sys_memory(?TIMEOUT).

get_sys_memory(Timeout) ->
    try
        gen_server:call(?MODULE, get_sys_memory, Timeout)
    catch
        exit:{timeout, _} ->
            get_memory_from_cache()
    end.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    {ok, #{last_time => 0}}.

handle_call(get_sys_memory, _From, State = #{last_time := LastTime}) ->
    Now = erlang:system_time(millisecond),
    case Now - LastTime >= ?TIMEOUT of
        true ->
            Memory = load_ctl:get_sys_memory(),
            persistent_term:put(?SYS_MEMORY_CACHE_KEY, Memory),
            {reply, Memory, State#{last_time => Now}};
        false ->
            {reply, get_memory_from_cache(), State}
    end;
handle_call(_Request, _From, State = #{}) ->
    {reply, ok, State}.

handle_cast(_Request, State = #{}) ->
    {noreply, State}.

handle_info(_Info, State = #{}) ->
    {noreply, State}.

terminate(_Reason, _State = #{}) ->
    ok.

code_change(_OldVsn, State = #{}, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

get_memory_from_cache() ->
    persistent_term:get(?SYS_MEMORY_CACHE_KEY, {0, 0}).
