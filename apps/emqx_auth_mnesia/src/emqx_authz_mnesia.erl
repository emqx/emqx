%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_mnesia).

-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("emqx/include/logger.hrl").

-include_lib("emqx_auth/include/emqx_authz.hrl").

-define(ACL_SHARDED, emqx_acl_sharded).

%% To save some space, use an integer for label, 0 for 'all', {1, Username} and {2, ClientId}.
-define(ACL_TABLE_ALL, 0).
-define(ACL_TABLE_USERNAME, 1).
-define(ACL_TABLE_CLIENTID, 2).

-type username() :: {username, binary()}.
-type clientid() :: {clientid, binary()}.
-type who() :: username() | clientid() | all.

-type rule() :: {
    emqx_authz_rule:permission_resolution_precompile(),
    emqx_authz_rule:who_precompile(),
    emqx_authz_rule:action_precompile(),
    emqx_authz_rule:topic_precompile()
}.

-type legacy_rule() :: {
    emqx_authz_rule:permission_resolution_precompile(),
    emqx_authz_rule:action_precompile(),
    emqx_authz_rule:topic_precompile()
}.

-type rules() :: [rule() | legacy_rule()].

-record(emqx_acl, {
    who :: ?ACL_TABLE_ALL | {?ACL_TABLE_USERNAME, binary()} | {?ACL_TABLE_CLIENTID, binary()},
    rules :: rules()
}).

-behaviour(emqx_authz_source).
-behaviour(emqx_db_backup).

%% AuthZ Callbacks
-export([
    create/1,
    update/1,
    destroy/1,
    authorize/4
]).

%% Management API
-export([
    init_tables/0,
    store_rules/2,
    purge_rules/0,
    get_rules/1,
    delete_rules/1,
    list_clientid_rules/0,
    list_username_rules/0,
    record_count/0
]).

-export([backup_tables/0]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-spec create_tables() -> [mria:table()].
create_tables() ->
    ok = mria:create_table(?ACL_TABLE, [
        {type, ordered_set},
        {rlog_shard, ?ACL_SHARDED},
        {storage, disc_copies},
        {attributes, record_info(fields, ?ACL_TABLE)},
        {storage_properties, [{ets, [{read_concurrency, true}]}]}
    ]),
    [?ACL_TABLE].

%%--------------------------------------------------------------------
%% emqx_authz callbacks
%%--------------------------------------------------------------------

create(Source) -> Source.

update(Source) -> Source.

destroy(_Source) ->
    {atomic, ok} = mria:clear_table(?ACL_TABLE),
    ok.

authorize(
    #{
        username := Username,
        clientid := Clientid
    } = Client,
    PubSub,
    Topic,
    #{type := built_in_database}
) ->
    Rules =
        read_rules({?ACL_TABLE_CLIENTID, Clientid}) ++
            read_rules({?ACL_TABLE_USERNAME, Username}) ++
            read_rules(?ACL_TABLE_ALL),
    do_authorize(Client, PubSub, Topic, Rules).

%%--------------------------------------------------------------------
%% Data backup
%%--------------------------------------------------------------------

backup_tables() -> {<<"builtin_authz">>, [?ACL_TABLE]}.

%%--------------------------------------------------------------------
%% Management API
%%--------------------------------------------------------------------

%% Init
-spec init_tables() -> ok.
init_tables() ->
    ok = mria:wait_for_tables(create_tables()).

%% @doc Update authz rules
-spec store_rules(who(), rules()) -> ok.
store_rules({username, Username}, Rules) ->
    do_store_rules({?ACL_TABLE_USERNAME, Username}, normalize_rules(Rules));
store_rules({clientid, Clientid}, Rules) ->
    do_store_rules({?ACL_TABLE_CLIENTID, Clientid}, normalize_rules(Rules));
store_rules(all, Rules) ->
    do_store_rules(?ACL_TABLE_ALL, normalize_rules(Rules)).

%% @doc Clean all authz rules for (username & clientid & all)
-spec purge_rules() -> ok.
purge_rules() ->
    ok = lists:foreach(
        fun(Key) ->
            ok = mria:dirty_delete(?ACL_TABLE, Key)
        end,
        mnesia:dirty_all_keys(?ACL_TABLE)
    ).

%% @doc Get one record
-spec get_rules(who()) -> {ok, rules()} | not_found.
get_rules({username, Username}) ->
    do_get_rules({?ACL_TABLE_USERNAME, Username});
get_rules({clientid, Clientid}) ->
    do_get_rules({?ACL_TABLE_CLIENTID, Clientid});
get_rules(all) ->
    do_get_rules(?ACL_TABLE_ALL).

%% @doc Delete one record
-spec delete_rules(who()) -> ok.
delete_rules({username, Username}) ->
    mria:dirty_delete(?ACL_TABLE, {?ACL_TABLE_USERNAME, Username});
delete_rules({clientid, Clientid}) ->
    mria:dirty_delete(?ACL_TABLE, {?ACL_TABLE_CLIENTID, Clientid});
delete_rules(all) ->
    mria:dirty_delete(?ACL_TABLE, ?ACL_TABLE_ALL).

-spec list_username_rules() -> ets:match_spec().
list_username_rules() ->
    ets:fun2ms(
        fun(#emqx_acl{who = {?ACL_TABLE_USERNAME, Username}, rules = Rules}) ->
            [{username, Username}, {rules, Rules}]
        end
    ).

-spec list_clientid_rules() -> ets:match_spec().
list_clientid_rules() ->
    ets:fun2ms(
        fun(#emqx_acl{who = {?ACL_TABLE_CLIENTID, Clientid}, rules = Rules}) ->
            [{clientid, Clientid}, {rules, Rules}]
        end
    ).

-spec record_count() -> non_neg_integer().
record_count() ->
    mnesia:table_info(?ACL_TABLE, size).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

read_rules(Key) ->
    case mnesia:dirty_read(?ACL_TABLE, Key) of
        [] -> [];
        [#emqx_acl{rules = Rules}] when is_list(Rules) -> Rules;
        Other -> error({invalid_rules, Key, Other})
    end.

do_store_rules(Who, Rules) ->
    Record = #emqx_acl{who = Who, rules = Rules},
    mria:dirty_write(Record).

normalize_rules(Rules) ->
    lists:flatmap(fun normalize_rule/1, Rules).

normalize_rule(RuleRaw) ->
    case emqx_authz_rule_raw:parse_rule(RuleRaw) of
        %% For backward compatibility
        {ok, {Permission, Who, Action, TopicFilters}} ->
            [{Permission, Who, Action, TopicFilter} || TopicFilter <- TopicFilters];
        {error, Reason} ->
            error(Reason)
    end.

do_get_rules(Key) ->
    case mnesia:dirty_read(?ACL_TABLE, Key) of
        [#emqx_acl{rules = Rules}] -> {ok, Rules};
        [] -> not_found
    end.

do_authorize(_Client, _PubSub, _Topic, []) ->
    nomatch;
do_authorize(Client, PubSub, Topic, [Rule | Tail]) ->
    CompliledRule = compile_rule(Rule),
    case emqx_authz_rule:match(Client, PubSub, Topic, CompliledRule) of
        {matched, Permission} -> {matched, Permission};
        nomatch -> do_authorize(Client, PubSub, Topic, Tail)
    end.

compile_rule({Permission, Who, Action, TopicFilter}) ->
    emqx_authz_rule:compile(Permission, Who, Action, [TopicFilter]);
compile_rule({Permission, Action, TopicFilter}) ->
    emqx_authz_rule:compile(Permission, all, Action, [TopicFilter]).
