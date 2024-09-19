%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_resource_cache_cleaner).

-export([start_link/0]).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3,
    terminate/2
]).
-export([add/2]).

-define(SERVER, ?MODULE).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

add(ID, Pid) ->
    gen_server:call(?SERVER, {add, ID, Pid}, infinity).

init(_) ->
    process_flag(trap_exit, true),
    {ok, #{pmon => emqx_pmon:new()}}.

handle_call({add, ID, Pid}, _From, #{pmon := Pmon} = State) ->
    NewPmon = emqx_pmon:monitor(Pid, ID, Pmon),
    {reply, ok, State#{pmon => NewPmon}};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _MRef, process, Pid, _Reason}, #{pmon := Pmon} = State) ->
    NewPmon =
        case emqx_pmon:find(Pid, Pmon) of
            {ok, ID} ->
                maybe_erase_cache(Pid, ID),
                emqx_pmon:erase(Pid, Pmon);
            error ->
                Pmon
        end,
    {noreply, State#{pmon => NewPmon}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

maybe_erase_cache(DownManager, ID) ->
    case emqx_resource_cache:read_manager_pid(ID) =:= DownManager of
        true ->
            emqx_resource_cache:erase(ID);
        false ->
            %% already erased, or already replaced by another manager due to quick
            %% retart by supervisor
            ok
    end.
