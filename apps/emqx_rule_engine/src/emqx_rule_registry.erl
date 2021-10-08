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

-export([ load_hooks_for_rule/1
        , unload_hooks_for_rule/1
        ]).

%% for debug purposes
-export([dump/0]).

%% gen_server Callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

%% Mnesia bootstrap
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

-define(REGISTRY, ?MODULE).

-define(T_CALL, 10000).

%%------------------------------------------------------------------------------
%% Mnesia bootstrap
%%------------------------------------------------------------------------------

%% @doc Create or replicate tables.
-spec(mnesia(boot | copy) -> ok).
mnesia(boot) ->
    %% Optimize storage
    StoreProps = [{ets, [{read_concurrency, true}]}],
    %% Rule table
    ok = ekka_mnesia:create_table(?RULE_TAB, [
                {rlog_shard, ?RULE_ENGINE_SHARD},
                {disc_copies, [node()]},
                {record_name, rule},
                {attributes, record_info(fields, rule)},
                {storage_properties, StoreProps}]);

mnesia(copy) ->
    ok = ekka_mnesia:copy_table(?RULE_TAB, disc_copies).

dump() ->
    ?ULOG("Rules: ~p~n", [ets:tab2list(?RULE_TAB)]).

%%------------------------------------------------------------------------------
%% Start the registry
%%------------------------------------------------------------------------------

-spec(start_link() -> {ok, pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    gen_server:start_link({local, ?REGISTRY}, ?MODULE, [], []).

%%------------------------------------------------------------------------------
%% Rule Management
%%------------------------------------------------------------------------------

-spec(get_rules() -> list(emqx_rule_engine:rule())).
get_rules() ->
    get_all_records(?RULE_TAB).

get_rules_ordered_by_ts() ->
    F = fun() ->
        Query = qlc:q([E || E <- mnesia:table(?RULE_TAB)]),
        qlc:e(qlc:keysort(#rule.created_at, Query, [{order, ascending}]))
    end,
    {atomic, List} = ekka_mnesia:transaction(?RULE_ENGINE_SHARD, F),
    List.

-spec(get_rules_for_topic(Topic :: binary()) -> list(emqx_rule_engine:rule())).
get_rules_for_topic(Topic) ->
    [Rule || Rule = #rule{info = #{from := From}} <- get_rules(),
             emqx_plugin_libs_rule:can_topic_match_oneof(Topic, From)].

-spec(get_rules_with_same_event(Topic :: binary()) -> list(emqx_rule_engine:rule())).
get_rules_with_same_event(Topic) ->
    EventName = emqx_rule_events:event_name(Topic),
    [Rule || Rule = #rule{info = #{from := From}} <- get_rules(),
             lists:any(fun(T) -> is_of_event_name(EventName, T) end, From)].

is_of_event_name(EventName, Topic) ->
    EventName =:= emqx_rule_events:event_name(Topic).

-spec(get_rule(Id :: rule_id()) -> {ok, emqx_rule_engine:rule()} | not_found).
get_rule(Id) ->
    case mnesia:dirty_read(?RULE_TAB, Id) of
        [Rule] -> {ok, Rule};
        [] -> not_found
    end.

-spec(add_rule(emqx_rule_engine:rule()) -> ok).
add_rule(Rule) when is_record(Rule, rule) ->
    add_rules([Rule]).

-spec(add_rules(list(emqx_rule_engine:rule())) -> ok).
add_rules(Rules) ->
    gen_server:call(?REGISTRY, {add_rules, Rules}, ?T_CALL).

-spec(remove_rule(emqx_rule_engine:rule() | rule_id()) -> ok).
remove_rule(RuleOrId) ->
    remove_rules([RuleOrId]).

-spec(remove_rules(list(emqx_rule_engine:rule()) | list(rule_id())) -> ok).
remove_rules(Rules) ->
    gen_server:call(?REGISTRY, {remove_rules, Rules}, ?T_CALL).

%% @private

insert_rules([]) -> ok;
insert_rules(Rules) ->
    _ = emqx_plugin_libs_rule:cluster_call(?MODULE, load_hooks_for_rule, [Rules]),
    [mnesia:write(?RULE_TAB, Rule, write) ||Rule <- Rules].

%% @private
delete_rules([]) -> ok;
delete_rules(Rules = [R|_]) when is_binary(R) ->
    RuleRecs =
        lists:foldl(fun(RuleId, Acc) ->
            case get_rule(RuleId) of
                {ok, Rule} ->  [Rule|Acc];
                not_found -> Acc
            end
        end, [], Rules),
    delete_rules_unload_hooks(RuleRecs);
delete_rules(Rules = [Rule|_]) when is_record(Rule, rule) ->
    delete_rules_unload_hooks(Rules).

delete_rules_unload_hooks(Rules) ->
    _ =  emqx_plugin_libs_rule:cluster_call(?MODULE, unload_hooks_for_rule, [Rules]),
    [mnesia:delete_object(?RULE_TAB, Rule, write) ||Rule <- Rules].

load_hooks_for_rule(Rules) ->
    lists:foreach(fun(#rule{info = #{from := Topics}}) ->
            lists:foreach(fun emqx_rule_events:load/1, Topics)
        end, Rules).

unload_hooks_for_rule(Rules) ->
    lists:foreach(fun(#rule{id = Id, info = #{from := Topics}}) ->
        lists:foreach(fun(Topic) ->
            case get_rules_with_same_event(Topic) of
                [#rule{id = Id0}] when Id0 == Id -> %% we are now deleting the last rule
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
    trans(fun insert_rules/1, [Rules]),
    {reply, ok, State};

handle_call({remove_rules, Rules}, _From, State) ->
    trans(fun delete_rules/1, [Rules]),
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
    %mnesia:dirty_match_object(Tab, mnesia:table_info(Tab, wild_pattern)).
    %% Wrapping ets to a transaction to avoid reading inconsistent
    %% ( nest cluster_call transaction, no a r/o transaction)
    %% data during shard bootstrap
    {atomic, Ret} =
        ekka_mnesia:transaction(?RULE_ENGINE_SHARD,
                                   fun() ->
                                           ets:tab2list(Tab)
                                   end),
    Ret.

trans(Fun, Args) ->
    case ekka_mnesia:transaction(?RULE_ENGINE_SHARD, Fun, Args) of
        {atomic, Result} -> Result;
        {aborted, Reason} -> error(Reason)
    end.
