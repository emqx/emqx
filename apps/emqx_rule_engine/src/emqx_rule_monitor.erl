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

-module(emqx_rule_monitor).

-behavior(gen_server).

-logger_header("[Rule Monitor]").
-include("rule_engine.hrl").

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([ start_link/0
        , stop/0
        , async_refresh_resources_rules/0
        , ensure_resource_retrier/1
        , ensure_rule_retrier/1
        , retry_loop/2
        , retry_loop/3
        ]).

-export([ put_resource_retry_interval/1
        , put_rule_retry_interval/1
        , get_resource_retry_interval/0
        , get_rule_retry_interval/0
        , erase_resource_retry_interval/0
        , erase_rule_retry_interval/0
        ]).

-define(T_RESOURCE_RETRY, 15000).
-define(T_RULE_RETRY, 20000).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
    gen_server:stop(?MODULE).

init([]) ->
    _ = erlang:process_flag(trap_exit, true),
    {ok, #{retryers => #{}}}.

put_resource_retry_interval(I) when is_integer(I) andalso I >= 10 ->
    _ = persistent_term:put({?MODULE, resource_restart_interval}, I),
    ok.
put_rule_retry_interval(I) when is_integer(I) andalso I >= 10 ->
    _ = persistent_term:put({?MODULE, rule_restart_interval}, I),
    ok.

erase_resource_retry_interval() ->
    _ = persistent_term:erase({?MODULE, resource_restart_interval}),
    ok.
erase_rule_retry_interval() ->
    _ = persistent_term:erase({?MODULE, rule_restart_interval}),
    ok.

get_resource_retry_interval() ->
    persistent_term:get({?MODULE, resource_restart_interval}, ?T_RESOURCE_RETRY).
get_rule_retry_interval() ->
    persistent_term:get({?MODULE, rule_restart_interval}, ?T_RULE_RETRY).

async_refresh_resources_rules() ->
    gen_server:cast(?MODULE, async_refresh).

ensure_resource_retrier(ResId) ->
    gen_server:cast(?MODULE, {create_restart_handler, resource, ResId}).

ensure_rule_retrier(RuleId) ->
    gen_server:cast(?MODULE, {create_restart_handler, rule, RuleId}).

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_cast(async_refresh, #{boot_refresh_pid := Pid} = State) when is_pid(Pid) ->
    %% the refresh task is already in progress, we discard the duplication
    {noreply, State};
handle_cast(async_refresh, State) ->
    Pid = spawn_link(fun do_async_refresh/0),
    {noreply, State#{boot_refresh_pid => Pid}};

handle_cast({create_restart_handler, Tag, Obj}, State) ->
    Objects = maps:get(Tag, State, #{}),
    NewState = case maps:find(Obj, Objects) of
        error ->
            update_object(Tag, Obj,
                create_restart_handler(Tag, Obj), State);
        {ok, _Pid} ->
            State
    end,
    {noreply, NewState};

handle_cast(_Msg, State) ->
    {noreply, State}.


handle_info({'EXIT', Pid, _Reason}, State = #{boot_refresh_pid := Pid}) ->
    {noreply, State#{boot_refresh_pid => undefined}};
handle_info({'EXIT', Pid, Reason}, State = #{retryers := Retryers}) ->
    %% We won't try to restart the 'retryers' event if the 'EXIT' Reason is not 'normal'.
    %% Instead we rely on the user to trigger a manual retry for the resources, and then enable
    %% the rules after resources are connected.
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

create_restart_handler(Tag, Obj) ->
    ?LOG(warning, "starting_a_retry_loop for ~p ~p", [Tag, Obj]),
    %% spawn a dedicated process to handle the restarting asynchronously
    spawn_link(?MODULE, retry_loop, [Tag, Obj]).

%% retry_loop/3 is to avoid crashes during relup
retry_loop(Tag, ResId, _Interval) ->
    retry_loop(Tag, ResId).

retry_loop(resource, ResId) ->
    timer:sleep(get_resource_retry_interval()),
    case emqx_rule_registry:find_resource(ResId) of
        {ok, #resource{type = Type, config = Config}} ->
            try
                {ok, #resource_type{on_create = {M, F}}} =
                    emqx_rule_registry:find_resource_type(Type),
                ok = emqx_rule_engine:init_resource(M, F, ResId, Config),
                refresh_and_enable_rules_of_resource(ResId)
            catch
                Err:Reason:Stacktrace ->
                    %% do not log stacktrace if it's a throw
                    LogContext =
                        case Err of
                            throw -> Reason;
                            _ -> {Reason, Stacktrace}
                        end,
                    ?LOG_SENSITIVE(warning, "init_resource_retry_failed ~p, ~0p", [ResId, LogContext]),
                    %% keep looping
                    ?MODULE:retry_loop(resource, ResId)
            end;
        not_found ->
            ok
    end;

retry_loop(rule, RuleId) ->
    timer:sleep(get_rule_retry_interval()),
    case emqx_rule_registry:get_rule(RuleId) of
        {ok, #rule{enabled = false, state = refresh_failed_at_bootup} = Rule} ->
            try
                emqx_rule_engine:refresh_rule(Rule),
                emqx_rule_registry:add_rule(Rule#rule{enabled = true, state = normal}),
                ?LOG(warning, "rule ~s has been refreshed and re-enabled", [RuleId])
            catch
                Err:Reason:ST ->
                    ?LOG(warning, "init_rule failed: ~p, ~0p",
                        [{Err, Reason}, ST]),
                    ?MODULE:retry_loop(rule, RuleId)
            end;
        {ok, #rule{enabled = false, state = State}} when State =/= refresh_failed_at_bootup ->
            ?LOG(warning, "rule ~s was disabled by the user, won't re-enable it", [RuleId]);
        _ ->
            ok
    end.

do_async_refresh() ->
    %% NOTE: the order matters.
    %% We should always refresh the resources first and then the rules.
    ok = emqx_rule_engine:refresh_resources(),
    ok = emqx_rule_engine:refresh_rules_when_boot().

refresh_and_enable_rules_of_resource(ResId) ->
    lists:foreach(
        fun (#rule{id = Id, enabled = false, state = refresh_failed_at_bootup} = Rule) ->
                emqx_rule_engine:refresh_rule(Rule),
                emqx_rule_registry:add_rule(Rule#rule{enabled = true, state = normal}),
                ?LOG(warning, "rule ~s is refreshed and re-enabled", [Id]);
            (_) -> ok
        end, emqx_rule_registry:find_rules_depends_on_resource(ResId)).
