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

-module(emqx_rule_engine).

-behaviour(gen_server).
-behaviour(emqx_config_handler).

-include("rule_engine.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("stdlib/include/qlc.hrl").

-export([start_link/0]).

-export([ post_config_update/5
        , config_key_path/0
        ]).

%% Rule Management

-export([ load_rules/0
        ]).

-export([ create_rule/1
        , insert_rule/1
        , update_rule/1
        , delete_rule/1
        , get_rule/1
        ]).

-export([ get_rules/0
        , get_rules_for_topic/1
        , get_rules_with_same_event/1
        , get_rules_ordered_by_ts/0
        ]).

%% exported for cluster_call
-export([ do_delete_rule/1
        , do_insert_rule/1
        ]).

-export([ load_hooks_for_rule/1
        , unload_hooks_for_rule/1
        , add_metrics_for_rule/1
        , clear_metrics_for_rule/1
        ]).

%% gen_server Callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-define(RULE_ENGINE, ?MODULE).

-define(T_CALL, infinity).

config_key_path() ->
    [rule_engine, rules].

-spec(start_link() -> {ok, pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    gen_server:start_link({local, ?RULE_ENGINE}, ?MODULE, [], []).

%%------------------------------------------------------------------------------
%% The config handler for emqx_rule_engine
%%------------------------------------------------------------------------------
post_config_update(_, _Req, NewRules, OldRules, _AppEnvs) ->
    #{added := Added, removed := Removed, changed := Updated}
        = emqx_map_lib:diff_maps(NewRules, OldRules),
    maps_foreach(fun({Id, {_Old, New}}) ->
            {ok, _} = update_rule(New#{id => bin(Id)})
        end, Updated),
    maps_foreach(fun({Id, _Rule}) ->
            ok = delete_rule(bin(Id))
        end, Removed),
    maps_foreach(fun({Id, Rule}) ->
            {ok, _} = create_rule(Rule#{id => bin(Id)})
        end, Added),
    {ok, get_rules()}.

%%------------------------------------------------------------------------------
%% APIs for rules
%%------------------------------------------------------------------------------

-spec load_rules() -> ok.
load_rules() ->
    maps_foreach(fun({Id, Rule}) ->
            {ok, _} = create_rule(Rule#{id => bin(Id)})
        end, emqx_conf:get([rule_engine, rules], #{})).

-spec create_rule(map()) -> {ok, rule()} | {error, term()}.
create_rule(Params = #{id := RuleId}) when is_binary(RuleId) ->
    case get_rule(RuleId) of
        not_found -> do_create_rule(Params);
        {ok, _} -> {error, {already_exists, RuleId}}
    end.

-spec update_rule(map()) -> {ok, rule()} | {error, term()}.
update_rule(Params = #{id := RuleId}) when is_binary(RuleId) ->
    ok = delete_rule(RuleId),
    do_create_rule(Params).

-spec(delete_rule(RuleId :: rule_id()) -> ok).
delete_rule(RuleId) when is_binary(RuleId) ->
    gen_server:call(?RULE_ENGINE, {delete_rule, RuleId}, ?T_CALL).

-spec(insert_rule(Rule :: rule()) -> ok).
insert_rule(Rule) ->
    gen_server:call(?RULE_ENGINE, {insert_rule, Rule}, ?T_CALL).

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

load_hooks_for_rule(#{from := Topics}) ->
    lists:foreach(fun emqx_rule_events:load/1, Topics).

add_metrics_for_rule(#{id := Id}) ->
    ok = emqx_plugin_libs_metrics:create_metrics(rule_metrics, Id).

clear_metrics_for_rule(#{id := Id}) ->
    ok = emqx_plugin_libs_metrics:clear_metrics(rule_metrics, Id).

unload_hooks_for_rule(#{id := Id, from := Topics}) ->
    lists:foreach(fun(Topic) ->
        case get_rules_with_same_event(Topic) of
            [#{id := Id0}] when Id0 == Id -> %% we are now deleting the last rule
                emqx_rule_events:unload(Topic);
            _ -> ok
        end
    end, Topics).

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

init([]) ->
    _TableId = ets:new(?KV_TAB, [named_table, set, public, {write_concurrency, true},
                                 {read_concurrency, true}]),
    {ok, #{}}.

handle_call({insert_rule, Rule}, _From, State) ->
    _ = emqx_plugin_libs_rule:cluster_call(?MODULE, do_insert_rule, [Rule]),
    {reply, ok, State};

handle_call({delete_rule, Rule}, _From, State) ->
    _ = emqx_plugin_libs_rule:cluster_call(?MODULE, do_delete_rule, [Rule]),
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
%% Internal Functions
%%------------------------------------------------------------------------------

do_create_rule(Params = #{id := RuleId, sql := Sql, outputs := Outputs}) ->
    case emqx_rule_sqlparser:parse(Sql) of
        {ok, Select} ->
            Rule = #{
                id => RuleId,
                created_at => erlang:system_time(millisecond),
                enabled => maps:get(enabled, Params, true),
                sql => Sql,
                outputs => parse_outputs(Outputs),
                description => maps:get(description, Params, ""),
                %% -- calculated fields:
                from => emqx_rule_sqlparser:select_from(Select),
                is_foreach => emqx_rule_sqlparser:select_is_foreach(Select),
                fields => emqx_rule_sqlparser:select_fields(Select),
                doeach => emqx_rule_sqlparser:select_doeach(Select),
                incase => emqx_rule_sqlparser:select_incase(Select),
                conditions => emqx_rule_sqlparser:select_where(Select)
                %% -- calculated fields end
            },
            ok = insert_rule(Rule),
            {ok, Rule};
        {error, Reason} -> {error, Reason}
    end.

do_insert_rule(#{id := Id} = Rule) ->
    ok = load_hooks_for_rule(Rule),
    ok = add_metrics_for_rule(Rule),
    true = ets:insert(?RULE_TAB, {Id, maps:remove(id, Rule)}),
    ok.

do_delete_rule(RuleId) ->
    case get_rule(RuleId) of
        {ok, Rule} ->
            ok = unload_hooks_for_rule(Rule),
            ok = clear_metrics_for_rule(Rule),
            true = ets:delete(?RULE_TAB, RuleId),
            ok;
        not_found -> ok
    end.

parse_outputs(Outputs) ->
    [do_parse_output(Out) || Out <- Outputs].

do_parse_output(Output) when is_map(Output) ->
    emqx_rule_outputs:parse_output(Output);
do_parse_output(BridgeChannelId) when is_binary(BridgeChannelId) ->
    BridgeChannelId.

get_all_records(Tab) ->
    [Rule#{id => Id} || {Id, Rule} <- ets:tab2list(Tab)].

maps_foreach(Fun, Map) ->
    lists:foreach(Fun, maps:to_list(Map)).

bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
bin(B) when is_binary(B) -> B.
