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

-module(emqx_acl_mnesia_db).

-include("emqx_auth_mnesia.hrl").
-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("stdlib/include/qlc.hrl").

%% ACL APIs
-export([ create_table/0
        , create_table2/0
        ]).

-export([ add_acl/4
        , lookup_acl/1
        , all_acls_export/0
        , all_acls/0
        , all_acls/1
        , remove_acl/2
        , merge_acl_records/3
        , login_acl_table/1
        , login_acl_table/2
        , is_migration_started/0
        ]).

-export([comparing/2]).

%%--------------------------------------------------------------------
%% ACL API
%%--------------------------------------------------------------------

%% @doc Create table `emqx_acl` of old format rules
-spec(create_table() -> ok).
create_table() ->
    ok = ekka_mnesia:create_table(?ACL_TABLE, [
        {type, bag},
        {disc_copies, [node()]},
        {attributes, record_info(fields, ?ACL_TABLE)},
        {storage_properties, [{ets, [{read_concurrency, true}]}]}]),
    ok = ekka_mnesia:copy_table(?ACL_TABLE, disc_copies).

%% @doc Create table `emqx_acl2` of new format rules
-spec(create_table2() -> ok).
create_table2() ->
    ok = ekka_mnesia:create_table(?ACL_TABLE2, [
            {type, ordered_set},
            {disc_copies, [node()]},
            {attributes, record_info(fields, ?ACL_TABLE2)},
            {storage_properties, [{ets, [{read_concurrency, true}]}]}]),
    ok = ekka_mnesia:copy_table(?ACL_TABLE2, disc_copies).

%% @doc Add Acls
-spec(add_acl(acl_target(), emqx_topic:topic(), legacy_action(), access()) ->
        ok | {error, any()}).
add_acl(Login, Topic, Action, Access) ->
    ret(mnesia:transaction(fun() ->
                              case is_migration_started() of
                                  true -> add_acl_new(Login, Topic, Action, Access);
                                  false -> add_acl_old(Login, Topic, Action, Access)
                              end
                           end)).

%% @doc Lookup acl by login
-spec(lookup_acl(acl_target()) -> list(acl_record())).
lookup_acl(undefined) -> [];
lookup_acl(Login) ->
    % After migration to ?ACL_TABLE2, ?ACL_TABLE never has any rules. This lookup should be removed later.
    MatchSpec = ets:fun2ms(fun(#?ACL_TABLE{filter = {Filter, _}} = Rec)
                                 when Filter =:= Login -> Rec
                           end),
    OldRecs = ets:select(?ACL_TABLE, MatchSpec),

    NewAcls = ets:lookup(?ACL_TABLE2, Login),
    MergedAcl = merge_acl_records(Login, OldRecs, NewAcls),
    lists:sort(fun comparing/2, acl_to_list(MergedAcl)).

%% @doc Remove ACL
-spec remove_acl(acl_target(), emqx_topic:topic()) -> ok | {error, any()}.
remove_acl(Login, Topic) ->
    ret(mnesia:transaction(fun() ->
                              mnesia:delete({?ACL_TABLE, {Login, Topic}}),
                              case mnesia:wread({?ACL_TABLE2, Login}) of
                                  [] -> ok;
                                  [#?ACL_TABLE2{rules = Rules} = Acl] ->
                                      case delete_topic_rules(Topic, Rules) of
                                          [] -> mnesia:delete({?ACL_TABLE2, Login});
                                          [_ | _] = RemainingRules ->
                                              mnesia:write(Acl#?ACL_TABLE2{rules = RemainingRules})
                                      end
                              end
                           end)).

%% @doc All ACL rules
-spec(all_acls() -> list(acl_record())).
all_acls() ->
    all_acls(username) ++
    all_acls(clientid) ++
    all_acls(all).

%% @doc All ACL rules of specified type
-spec(all_acls(acl_target_type()) -> list(acl_record())).
all_acls(AclTargetType) ->
    lists:sort(fun comparing/2, qlc:eval(login_acl_table(AclTargetType))).

%% @doc All ACL rules fetched transactionally
-spec(all_acls_export() -> list(acl_record())).
all_acls_export() ->
    AclTargetTypes = [username, clientid, all],
    MatchSpecNew = lists:flatmap(fun login_match_spec_new/1, AclTargetTypes),
    MatchSpecOld = lists:flatmap(fun login_match_spec_old/1, AclTargetTypes),

    {atomic, Records} = mnesia:transaction(
        fun() ->
            QH = acl_table(MatchSpecNew, MatchSpecOld, {#{}, #{}}, fun mnesia:table/2, fun lookup_mnesia/2),
            qlc:eval(QH)
        end),
    Records.

%% @doc QLC table of logins matching spec
-spec(login_acl_table(acl_target_type()) -> qlc:query_handle()).
login_acl_table(AclTargetType) ->
    login_acl_table(AclTargetType, {[], []}).

login_acl_table(AclTargetType, {Qs, Fuzzy}) ->
    ToMap = fun({Type, Symbol, Val}, Acc) -> Acc#{{Type, Symbol} => Val} end,
    Qs1 = lists:foldl(ToMap, #{}, Qs),
    Fuzzy1 = lists:foldl(ToMap, #{}, Fuzzy),
    MatchSpecNew = login_match_spec_new(AclTargetType, Qs1),
    MatchSpecOld = login_match_spec_old(AclTargetType, Qs1),
    acl_table(MatchSpecNew, MatchSpecOld, {Qs1, Fuzzy1}, fun ets:table/2, fun lookup_ets/2).

%% @doc Combine old `emqx_acl` ACL records with a new `emqx_acl2` ACL record for a given login
-spec(merge_acl_records(acl_target(), [#?ACL_TABLE{}], [#?ACL_TABLE2{}]) -> #?ACL_TABLE2{}).
merge_acl_records(Login, OldRecs, Acls) ->
    OldRules = old_recs_to_rules(OldRecs),
    NewRules = case Acls of
        [] -> [];
        [#?ACL_TABLE2{rules = Rules}] -> Rules
    end,
    #?ACL_TABLE2{who = Login, rules = merge_rules(NewRules, OldRules)}.

%% @doc Checks if background migration of ACL rules from `emqx_acl` to `emqx_acl2` format started.
%% Should be run in transaction
-spec(is_migration_started() -> boolean()).
is_migration_started() ->
    case mnesia:read({?ACL_TABLE, ?MIGRATION_MARK_KEY}) of
        [?MIGRATION_MARK_RECORD | _] -> true;
        [] -> false
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

add_acl_new(Login, Topic, Action, Access) ->
    Rule = {Access, Action, Topic, erlang:system_time(millisecond)},
    Rules = normalize_rule(Rule),
    OldAcl = mnesia:wread({?ACL_TABLE2, Login}),
    NewAcl = case OldAcl of
        [#?ACL_TABLE2{rules = OldRules} = Acl] ->
            Acl#?ACL_TABLE2{rules = merge_rules(Rules, OldRules)};
        [] ->
            #?ACL_TABLE2{who = Login, rules = Rules}
    end,
    mnesia:write(NewAcl).

add_acl_old(Login, Topic, Action, Access) ->
    Filter = {Login, Topic},
    Acl = #?ACL_TABLE{
             filter = Filter,
             action = Action,
             access = Access,
             created_at = erlang:system_time(millisecond)
            },
    OldRecords = mnesia:wread({?ACL_TABLE, Filter}),
    case Action of
        pubsub ->
            update_permission(pub, Acl, OldRecords),
            update_permission(sub, Acl, OldRecords);
        _ ->
            update_permission(Action, Acl, OldRecords)
    end.

old_recs_to_rules(OldRecs) ->
    lists:flatmap(fun old_rec_to_rules/1, OldRecs).

old_rec_to_rules(#?ACL_TABLE{filter = {_, Topic}, action = Action, access = Access, created_at = CreatedAt}) ->
    normalize_rule({Access, Action, Topic, CreatedAt}).

normalize_rule({Access, pubsub, Topic, CreatedAt}) ->
    [{Access, pub, Topic, CreatedAt}, {Access, sub, Topic, CreatedAt}];
normalize_rule({Access, Action, Topic, CreatedAt}) ->
    [{Access, Action, Topic, CreatedAt}].

merge_rules([], OldRules) -> OldRules;
merge_rules([NewRule | RestNewRules], OldRules) ->
    merge_rules(RestNewRules, merge_rule(NewRule, OldRules)).

merge_rule({_, Action, Topic, _ } = NewRule, OldRules) ->
    [NewRule | lists:filter(
        fun({_, OldAction, OldTopic, _}) ->
            {Action, Topic} =/= {OldAction, OldTopic}
        end, OldRules)].

acl_to_list(#?ACL_TABLE2{who = Login, rules = Rules}) ->
    [{Login, Topic, Action, Access, CreatedAt} || {Access, Action, Topic, CreatedAt} <- Rules].

delete_topic_rules(Topic, Rules) ->
    [Rule || {_, _, T, _} = Rule <- Rules, T =/= Topic].

comparing({_, _, _, _, CreatedAt} = Rec1,
          {_, _, _, _, CreatedAt} = Rec2) ->
        Rec1 >= Rec2;

comparing({_, _, _, _, CreatedAt1},
          {_, _, _, _, CreatedAt2}) ->
    CreatedAt1 >= CreatedAt2.

login_match_spec_old(Type) -> login_match_spec_old(Type, #{}).

login_match_spec_old(all, _) ->
    ets:fun2ms(fun(#?ACL_TABLE{filter = {all, _}} = Record) ->
                Record
               end);

login_match_spec_old(Type, Params) when (Type =:= username) orelse (Type =:= clientid) ->
    case maps:get({Type, '=:='}, Params, undefined) of
        undefined ->
            ets:fun2ms(fun(#?ACL_TABLE{filter = {{RType, _}, _}} = Rec) when RType =:= Type -> Rec end);
        Val ->
            ets:fun2ms(fun(#?ACL_TABLE{filter = {{RType, RVal}, _}} = Rec)
                when RType =:= Type andalso RVal =:= Val -> Rec end)
    end.

login_match_spec_new(Type) -> login_match_spec_new(Type, #{}).

login_match_spec_new(all, _) ->
    ets:fun2ms(fun(#?ACL_TABLE2{who = all} = Record) ->
                Record
               end);

login_match_spec_new(Type, Params) when (Type =:= username) orelse (Type =:= clientid) ->
    case maps:get({Type, '=:='}, Params, undefined) of
        undefined ->
            ets:fun2ms(fun(#?ACL_TABLE2{who = {RType, _}} = Rec) when RType =:= Type -> Rec end);
        Val ->
            ets:fun2ms(fun(#?ACL_TABLE2{who = {RType, RVal}} = Rec)
                when RType =:= Type andalso RVal =:= Val  -> Rec end)
    end.

acl_table(MatchSpecNew, MatchSpecOld, Params, TableFun, LookupFun) ->
    TraverseFun =
        fun() ->
           CursorNew =
               qlc:cursor(
                   TableFun(?ACL_TABLE2, [{traverse, {select, MatchSpecNew}}])),
           CursorOld =
               qlc:cursor(
                   TableFun(?ACL_TABLE, [{traverse, {select, MatchSpecOld}}])),
           traverse_new(CursorNew, CursorOld, Params, #{}, LookupFun)
        end,

    qlc:table(TraverseFun, []).


% These are traverse funs for qlc table created by `acl_table/4`.
% Traversing consumes memory: it collects logins present in `?ACL_TABLE` and
% at the same time having rules in `?ACL_TABLE2`.
% Such records appear if ACLs are inserted before migration started.
% After migration, number of such logins is zero, so traversing starts working in
% constant memory.

traverse_new(CursorNew, CursorOld, Params, FoundKeys, LookupFun) ->
    Acls = qlc:next_answers(CursorNew, 1),
    case Acls of
        [] ->
            qlc:delete_cursor(CursorNew),
            traverse_old(CursorOld, Params, FoundKeys);
        [#?ACL_TABLE2{who = Login, rules = Rules} = Acl] ->
            Keys = lists:usort([{Login, Topic} || {_, _, Topic, _} <- Rules]),
            OldRecs = lists:flatmap(fun(Key) -> LookupFun(?ACL_TABLE, Key) end, Keys),
            MergedAcl = merge_acl_records(Login, OldRecs, [Acl]),
            NewFoundKeys =
                lists:foldl(fun(#?ACL_TABLE{filter = Key}, Found) -> maps:put(Key, true, Found) end,
                            FoundKeys,
                            OldRecs),
            case acl_to_list(MergedAcl) of
                [] ->
                    traverse_new(CursorNew, CursorOld, Params, NewFoundKeys, LookupFun);
                List ->
                    filter_params(List, Params) ++
                    fun() -> traverse_new(CursorNew, CursorOld, Params, NewFoundKeys, LookupFun) end
            end
    end.

filter_params(List, {Qs, Fuzzy}) ->
    case maps:size(Qs) =:= 0 andalso maps:size(Fuzzy) =:= 0 of
        false ->
            Topic = maps:get({topic, '=:='}, Qs, undefined),
            Action = maps:get({action, '=:='}, Qs, undefined),
            Access = maps:get({access, '=:='}, Qs, undefined),
            lists:filter(fun({Target, Topic0, Action0, Access0, _CreatedAt}) ->
                CheckList = [{Topic, Topic0}, {Action, Action0}, {Access, Access0}],
                case lists:all(fun is_match/1, CheckList) of
                    true ->
                        case Target of
                            {Type, Login} ->
                                case maps:get({Type, 'like'}, Fuzzy, <<>>) of
                                    <<>> -> true;
                                    LikeSchema -> binary:match(Login, LikeSchema) =/= nomatch
                                end;
                            all -> true
                        end;
                    false -> false
                end
                         end, List);
        true -> List
    end.

is_match({Schema, Val}) ->
    Schema =:= undefined orelse Schema =:= Val.

traverse_old(CursorOld, Params, FoundKeys) ->
    OldAcls = qlc:next_answers(CursorOld),
    case OldAcls of
        [] ->
            qlc:delete_cursor(CursorOld),
            [];
        _ ->
            Records = [{Login, Topic, Action, Access, CreatedAt}
                || #?ACL_TABLE{filter = {Login, Topic}, action = LegacyAction, access = Access, created_at = CreatedAt} <- OldAcls,
                {_, Action, _, _} <- normalize_rule({Access, LegacyAction, Topic, CreatedAt}),
                not maps:is_key({Login, Topic}, FoundKeys)
            ],
            case Records of
                [] -> traverse_old(CursorOld, Params, FoundKeys);
                List ->
                    filter_params(List, Params)
                    ++ fun() -> traverse_old(CursorOld, Params, FoundKeys) end
            end
    end.

lookup_mnesia(Tab, Key) ->
    mnesia:read({Tab, Key}).

lookup_ets(Tab, Key) ->
    ets:lookup(Tab, Key).

update_permission(Action, Acl0, OldRecords) ->
    Acl = Acl0 #?ACL_TABLE{action = Action},
    maybe_delete_shadowed_records(Action, OldRecords),
    mnesia:write(Acl).

maybe_delete_shadowed_records(_, []) ->
    ok;
maybe_delete_shadowed_records(Action1, [Rec = #emqx_acl{action = Action2} | Rest]) ->
    if Action1 =:= Action2 ->
            ok = mnesia:delete_object(Rec);
       Action2 =:= pubsub ->
            %% Perform migration from the old data format on the
            %% fly. This is needed only for the enterprise version,
            %% delete this branch on 5.0
            mnesia:delete_object(Rec),
            mnesia:write(Rec#?ACL_TABLE{action = other_action(Action1)});
       true ->
            ok
    end,
    maybe_delete_shadowed_records(Action1, Rest).

other_action(pub) -> sub;
other_action(sub) -> pub.

ret({atomic, ok})     -> ok;
ret({aborted, Error}) -> {error, Error}.
