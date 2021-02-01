%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_rule_monitor).

-behavior(gen_server).

-include("rule_engine.hrl").
-include_lib("emqx/include/logger.hrl").
-logger_header("[Rule Monitor]").

-define(T_RETRY, 60000).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([ start_link/0
        , ensure_resource_started/1
        ]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    {ok, #{pids => #{}}}.

ensure_resource_started(ResId) ->
    gen_server:call(?MODULE, {create_restart_handler, resource, ResId}).

handle_call({create_restart_handler, Tag, Obj}, _From, State) ->
    Objects = maps:get(Tag, State, #{}),
    NewState = case maps:find(Obj, Objects) of
        error ->
            update_object(Tag, Obj,
                create_restart_handler(Tag, Obj), State);
        {ok, Pid} ->
            case is_process_alive(Pid) of
                true -> State;
                false ->
                    update_object(Tag, Obj,
                        create_restart_handler(Tag, Obj), State)
            end
    end,
    {reply, ok, NewState}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', _, process, Pid, Reason}, State = #{pids := Handlers}) ->
    case maps:take(Pid, Handlers) of
        {{Tag, Obj}, Handlers2} ->
            Objects = maps:get(Tag, State, #{}),
            {noreply, State#{Tag => maps:remove(Obj, Objects),
                             pids => Handlers2}};
        error ->
            ?LOG(error, "got unexpected proc down: ~p ~p", [Pid, Reason]),
            {noreply, State}
    end;

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

update_object(Tag, Obj, Pid, State) ->
    Objects = maps:get(Tag, State, #{}),
    Pids = maps:get(pids, State, #{}),
    State#{Tag => Objects#{Obj => Pid}, pids => Pids#{Pid => {Tag, Obj}}}.

create_restart_handler(Tag, Obj) ->
    ?LOG(info, "keep restarting ~p ~p", [Tag, Obj]),
    {Pid, _Ref} = spawn_monitor(fun() -> keep_restart(Tag, Obj, ?T_RETRY) end),
    Pid.

keep_restart(resource, ResId, Interval) ->
    case emqx_rule_registry:find_resource(ResId) of
        {ok, #resource{type = Type, config = Config}} ->
            {ok, #resource_type{on_create = {M, F}}} =
                emqx_rule_registry:find_resource_type(Type),
            try emqx_rule_engine:init_resource(M, F, ResId, Config)
            catch
                _:_ ->
                    timer:sleep(Interval),
                    keep_restart(resource, ResId, Interval)
            end;
        not_found ->
            ok
    end.
