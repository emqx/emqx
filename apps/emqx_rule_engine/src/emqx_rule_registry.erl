%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_rule_registry).

-behaviour(gen_server).

-include("rule_engine.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("stdlib/include/qlc.hrl").

-export([start_link/0]).

%% Rule Management
-export([ get_rules/0
        , get_rules_for_topic/1
        , get_rules_with_same_event/1
        , get_rules_ordered_by_ts/0
        , get_rule/1
        , add_rule/1
        , add_rules/1
        , remove_rule/1
        , remove_rules/1
        ]).

-export([ do_remove_rules/1
        , do_add_rules/1
        ]).

-export([ load_hooks_for_rules/1
        , unload_hooks_for_rule/1
        , add_metrics_for_rules/1
        , clear_metrics_for_rules/1
        ]).

%% gen_server Callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-define(REGISTRY, ?MODULE).

-define(T_CALL, 10000).

%%------------------------------------------------------------------------------
%% Start the registry
%%------------------------------------------------------------------------------

-spec(start_link() -> {ok, pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    gen_server:start_link({local, ?REGISTRY}, ?MODULE, [], []).

%%------------------------------------------------------------------------------
%% Rule Management
%%------------------------------------------------------------------------------

-spec(get_rules() -> [rule()]).
get_rules() ->
    get_all_records(?RULE_TAB).

get_rules_ordered_by_ts() ->
    lists:sort(fun(#{created_at := CreatedA}, #{created_at := CreatedB}) ->
            CreatedA =< CreatedB
        end, get_rules()).

-spec(get_rules_for_topic(Topic :: binary()) -> [rule()]).
get_rules_for_topic(Topic) ->
    [Rule || Rule = #{from := From} <- get_rules(),
             emqx_plugin_libs_rule:can_topic_match_oneof(Topic, From)].

-spec(get_rules_with_same_event(Topic :: binary()) -> [rule()]).
get_rules_with_same_event(Topic) ->
    EventName = emqx_rule_events:event_name(Topic),
    [Rule || Rule = #{from := From} <- get_rules(),
             lists:any(fun(T) -> is_of_event_name(EventName, T) end, From)].

is_of_event_name(EventName, Topic) ->
    EventName =:= emqx_rule_events:event_name(Topic).

-spec(get_rule(Id :: rule_id()) -> {ok, rule()} | not_found).
get_rule(Id) ->
    case ets:lookup(?RULE_TAB, Id) of
        [{Id, Rule}] -> {ok, Rule#{id => Id}};
        [] -> not_found
    end.

-spec(add_rule(rule()) -> ok).
add_rule(Rule) ->
    add_rules([Rule]).

-spec(add_rules([rule()]) -> ok).
add_rules(Rules) ->
    gen_server:call(?REGISTRY, {add_rules, Rules}, ?T_CALL).

-spec(remove_rule(rule() | rule_id()) -> ok).
remove_rule(RuleOrId) ->
    remove_rules([RuleOrId]).

-spec(remove_rules([rule()] | list(rule_id())) -> ok).
remove_rules(Rules) ->
    gen_server:call(?REGISTRY, {remove_rules, Rules}, ?T_CALL).

%% @private

do_add_rules([]) -> ok;
do_add_rules(Rules) ->
    load_hooks_for_rules(Rules),
    add_metrics_for_rules(Rules),
    ets:insert(?RULE_TAB, [{Id, maps:remove(id, R)} || #{id := Id} = R <- Rules]),
    ok.

%% @private
do_remove_rules([]) -> ok;
do_remove_rules(RuleIds = [Id|_]) when is_binary(Id) ->
    RuleRecs =
        lists:foldl(fun(RuleId, Acc) ->
            case get_rule(RuleId) of
                {ok, Rule} ->  [Rule|Acc];
                not_found -> Acc
            end
        end, [], RuleIds),
    remove_rules_unload_hooks(RuleRecs);
do_remove_rules(Rules = [Rule|_]) when is_map(Rule) ->
    remove_rules_unload_hooks(Rules).

remove_rules_unload_hooks(Rules) ->
    unload_hooks_for_rule(Rules),
    clear_metrics_for_rules(Rules),
    lists:foreach(fun(#{id := Id}) ->
            ets:delete(?RULE_TAB, Id)
        end, Rules).

load_hooks_for_rules(Rules) ->
    lists:foreach(fun(#{from := Topics}) ->
            lists:foreach(fun emqx_rule_events:load/1, Topics)
        end, Rules).

add_metrics_for_rules(Rules) ->
    lists:foreach(fun(#{id := Id}) ->
            ok = emqx_rule_metrics:create_rule_metrics(Id)
        end, Rules).

clear_metrics_for_rules(Rules) ->
    lists:foreach(fun(#{id := Id}) ->
            ok = emqx_rule_metrics:clear_rule_metrics(Id)
        end, Rules).

unload_hooks_for_rule(Rules) ->
    lists:foreach(fun(#{id := Id, from := Topics}) ->
        lists:foreach(fun(Topic) ->
            case get_rules_with_same_event(Topic) of
                [#{id := Id0}] when Id0 == Id -> %% we are now deleting the last rule
                    emqx_rule_events:unload(Topic);
                _ -> ok
            end
        end, Topics)
    end, Rules).

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init([]) ->
    _TableId = ets:new(?KV_TAB, [named_table, set, public, {write_concurrency, true},
                                 {read_concurrency, true}]),
    {ok, #{}}.

handle_call({add_rules, Rules}, _From, State) ->
    _ = emqx_plugin_libs_rule:cluster_call(?MODULE, do_add_rules, [Rules]),
    {reply, ok, State};

handle_call({remove_rules, Rules}, _From, State) ->
    _ = emqx_plugin_libs_rule:cluster_call(?MODULE, do_remove_rules, [Rules]),
    {reply, ok, State};

handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", request => Req}),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", request => Msg}),
    {noreply, State}.

handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", request => Info}),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%------------------------------------------------------------------------------
%% Private functions
%%------------------------------------------------------------------------------

get_all_records(Tab) ->
    [Rule#{id => Id} || {Id, Rule} <- ets:tab2list(Tab)].
