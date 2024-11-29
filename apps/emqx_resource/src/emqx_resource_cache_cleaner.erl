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

-behaviour(gen_server).

-include_lib("snabbkaffe/include/trace.hrl").

%% API
-export([
    start_link/0,

    add_cache/2,
    add_dry_run/2
]).

%% `gen_server' API
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(SERVER, ?MODULE).

%% calls/casts/infos
-record(add_cache, {id :: emqx_resource:resource_id(), pid :: pid()}).
-record(add_dry_run, {id :: emqx_resource:resource_id(), pid :: pid()}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

add_cache(ID, Pid) ->
    gen_server:call(?SERVER, #add_cache{id = ID, pid = Pid}, infinity).

add_dry_run(ID, Pid) ->
    gen_server:cast(?SERVER, #add_dry_run{id = ID, pid = Pid}).

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

init(_) ->
    process_flag(trap_exit, true),
    State = #{
        cache_pmon => emqx_pmon:new(),
        dry_run_pmon => emqx_pmon:new()
    },
    {ok, State}.

handle_call(#add_cache{id = ID, pid = Pid}, _From, #{cache_pmon := Pmon} = State) ->
    NewPmon = emqx_pmon:monitor(Pid, ID, Pmon),
    {reply, ok, State#{cache_pmon := NewPmon}};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(#add_dry_run{id = ID, pid = Pid}, #{dry_run_pmon := Pmon0} = State0) ->
    Pmon = append_monitor(Pmon0, Pid, ID),
    State = State0#{dry_run_pmon := Pmon},
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _MRef, process, Pid, _Reason}, State0) ->
    State = handle_down(Pid, State0),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

handle_down(Pid, State0) ->
    #{
        cache_pmon := CachePmon,
        dry_run_pmon := DryrunPmon
    } = State0,
    case emqx_pmon:find(Pid, CachePmon) of
        {ok, ID} ->
            handle_down_cache(ID, Pid, State0);
        error ->
            case emqx_pmon:find(Pid, DryrunPmon) of
                {ok, IDs} ->
                    handle_down_dry_run(IDs, Pid, State0);
                error ->
                    State0
            end
    end.

handle_down_cache(ID, Pid, State0) ->
    #{cache_pmon := Pmon0} = State0,
    maybe_erase_cache(Pid, ID),
    Pmon = emqx_pmon:erase(Pid, Pmon0),
    State0#{cache_pmon := Pmon}.

handle_down_dry_run([ID | Rest], Pid, State0) ->
    #{dry_run_pmon := Pmon0} = State0,
    %% No need to wait here: since it's a dry run resource, it won't be recreated,
    %% assuming the ID is random enough.
    spawn(fun() ->
        _ = emqx_resource_manager:remove(ID),
        emqx_resource_manager_sup:delete_child(ID),
        ?tp("resource_cache_cleaner_deleted_child", #{id => ID})
    end),
    Pmon = emqx_pmon:erase(Pid, Pmon0),
    State = State0#{dry_run_pmon := Pmon},
    handle_down_dry_run(Rest, Pid, State);
handle_down_dry_run([], _Pid, State) ->
    State.

maybe_erase_cache(DownManager, ID) ->
    case emqx_resource_cache:read_manager_pid(ID) =:= DownManager of
        true ->
            emqx_resource_cache:erase(ID);
        false ->
            %% already erased, or already replaced by another manager due to quick
            %% restart by supervisor
            ok
    end.

append_monitor(Pmon0, Pid, Value) ->
    case emqx_pmon:find(Pid, Pmon0) of
        error ->
            emqx_pmon:monitor(Pid, [Value], Pmon0);
        {ok, Values} ->
            Pmon = emqx_pmon:demonitor(Pid, Pmon0),
            emqx_pmon:monitor(Pid, [Value | Values], Pmon)
    end.
