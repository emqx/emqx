%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([ start_link/0
        , stop/0
        , async_refresh_resources_rules/0
        , ensure_resource_retrier/2
        , handler/3
        ]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:stop(?MODULE).

init([]) ->
    _ = erlang:process_flag(trap_exit, true),
    {ok, #{retryers => #{}}}.

async_refresh_resources_rules() ->
    gen_server:cast(?MODULE, {create_handler, refresh, resources_and_rules, #{}}).

ensure_resource_retrier(ResId, Interval) ->
    gen_server:cast(?MODULE, {create_handler, resource, ResId, Interval}).

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_cast({create_handler, Tag, Obj, Args}, State) ->
    Objects = maps:get(Tag, State, #{}),
    NewState = case maps:find(Obj, Objects) of
        error ->
            update_object(Tag, Obj,
                create_handler(Tag, Obj, Args), State);
        {ok, _Pid} ->
            State
    end,
    {noreply, NewState};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, Reason}, State = #{retryers := Retryers}) ->
    case maps:take(Pid, Retryers) of
        {{Tag, Obj}, Retryers2} ->
            Objects = maps:get(Tag, State, #{}),
            {noreply, State#{Tag => maps:remove(Obj, Objects),
                             retryers => Retryers2}};
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

update_object(Tag, Obj, Retryer, State) ->
    Objects = maps:get(Tag, State, #{}),
    Retryers = maps:get(retryers, State, #{}),
    State#{
        Tag => Objects#{Obj => Retryer},
        retryers => Retryers#{Retryer => {Tag, Obj}}
    }.

create_handler(Tag, Obj, Args) ->
    ?LOG(info, "create monitor handler for ~p ~p, args: ~p", [Tag, Obj, Args]),
    %% spawn a dedicated process to handle the task asynchronously
    spawn_link(?MODULE, handler, [Tag, Obj, Args]).

handler(resource, ResId, Interval) ->
    case emqx_rule_registry:find_resource(ResId) of
        {ok, #resource{type = Type, config = Config}} ->
            try
                {ok, #resource_type{on_create = {M, F}}} =
                    emqx_rule_registry:find_resource_type(Type),
                ok = emqx_rule_engine:init_resource(M, F, ResId, Config),
                refresh_and_enable_rules_of_resource(ResId)
            catch
                Err:Reason:ST ->
                    ?LOG(warning, "init_resource failed: ~p, ~0p",
                        [{Err, Reason}, ST]),
                    timer:sleep(Interval),
                    ?MODULE:handler(resource, ResId, Interval)
            end;
        not_found ->
            ok
    end;

handler(refresh, resources_and_rules, _) ->
    %% NOTE: the order matters.
    %% We should always refresh the resources first and then the rules.
    ok = emqx_rule_engine:refresh_resources(),
    ok = emqx_rule_engine:refresh_rules().

refresh_and_enable_rules_of_resource(ResId) ->
    lists:foreach(
        fun (#rule{id = Id, enabled = false, state = refresh_failed_at_bootup} = Rule) ->
                emqx_rule_engine:refresh_rule(Rule),
                emqx_rule_registry:add_rule(Rule#rule{enabled = true, state = normal}),
                ?LOG(info, "rule ~s is refreshed and re-enabled", [Id]);
            (_) -> ok
        end, emqx_rule_registry:find_rules_depends_on_resource(ResId)).
