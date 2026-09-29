%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_mnesia).

-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("emqx/include/logger.hrl").

-include("emqx_auth_mnesia_internal.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_config.hrl").
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
    emqx_authz_rule:condition_precompile(),
    emqx_authz_rule:action_precompile(),
    emqx_authz_rule:topic_precompile()
}.

-type legacy_rule() :: {
    emqx_authz_rule:permission_resolution_precompile(),
    emqx_authz_rule:action_precompile(),
    emqx_authz_rule:topic_precompile()
}.

-type rules() :: [rule() | legacy_rule()].

-type table_who() ::
    ?ACL_TABLE_ALL | {?ACL_TABLE_USERNAME, binary()} | {?ACL_TABLE_CLIENTID, binary()}.

-record(?ACL_TABLE, {
    who :: table_who(),
    rules :: rules()
}).

-type maybe_namespace() :: emqx_config:maybe_namespace().

-behaviour(emqx_authz_source).
-behaviour(emqx_db_backup).

%% AuthZ Callbacks
-export([
    create/1,
    update/2,
    destroy/1,
    authorize/4
]).

%% Management API
-export([
    init_tables/0,
    store_rules/3,
    purge_rules/1,
    get_rules/2,
    delete_rules/2,
    list_clientid_rules/1,
    list_username_rules/1,
    record_count/1,
    record_count_per_namespace/0
]).

-export([
    backup_tables/0,
    namespace_backup_name/0,
    export_namespace_backup/1,
    import_namespace_backup/2
]).

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
    ok = mria:create_table(?AUTHZ_NS_TAB, [
        {type, ordered_set},
        {rlog_shard, ?ACL_SHARDED},
        {storage, disc_copies},
        {attributes, record_info(fields, ?AUTHZ_NS_TAB)},
        {storage_properties, [{ets, [{read_concurrency, true}]}]}
    ]),
    ok = emqx_utils_ets:new(?AUTHZ_NS_COUNT_TAB, [ordered_set, public]),
    [?ACL_TABLE, ?AUTHZ_NS_TAB].

%%--------------------------------------------------------------------
%% emqx_authz callbacks
%%--------------------------------------------------------------------

create(Source) -> Source.

update(_State, Source) -> create(Source).

destroy(_Source) ->
    {atomic, ok} = mria:clear_table(?ACL_TABLE),
    {atomic, ok} = mria:clear_table(?AUTHZ_NS_TAB),
    true = ets:delete_all_objects(?AUTHZ_NS_COUNT_TAB),
    ok.

authorize(
    #{
        username := Username,
        clientid := Clientid
    } = AuthzContext,
    PubSub,
    Topic,
    #{type := built_in_database}
) ->
    Namespace = get_namespace(AuthzContext),
    Rules = load_rules_for_authorize(Namespace, Clientid, Username),
    do_authorize(AuthzContext, PubSub, Topic, Rules).

%%--------------------------------------------------------------------
%% Data backup
%%--------------------------------------------------------------------

backup_tables() -> {<<"builtin_authz">>, [?ACL_TABLE, ?AUTHZ_NS_TAB]}.

namespace_backup_name() -> <<"builtin_authz">>.

%% Rules take the shape the REST API uses: `users', `clients' and `all'.
-spec export_namespace_backup(emqx_config:namespace()) -> map().
export_namespace_backup(Namespace) ->
    Records = mnesia:dirty_select(?AUTHZ_NS_TAB, [
        {#?AUTHZ_NS_TAB{who = ?AUTHZ_WHO_NS(Namespace, '_'), _ = '_'}, [], ['$_']}
    ]),
    lists:foldl(
        fun add_backup_rules/2,
        #{<<"version">> => 1, <<"users">> => [], <<"clients">> => [], <<"all">> => []},
        Records
    ).

%% Every rule list is validated as the REST API validates it before any is
%% stored. A list replaces the one stored for the same user or client.
-spec import_namespace_backup(emqx_config:namespace(), map()) -> ok | {error, term()}.
import_namespace_backup(Namespace, #{<<"version">> := 1} = Data) ->
    maybe
        {ok, Entries} ?= backup_entries(Data),
        import_backup_entries(Namespace, Entries)
    end;
import_namespace_backup(_Namespace, _Data) ->
    {error, unsupported_backup_format}.

%%--------------------------------------------------------------------
%% Management API
%%--------------------------------------------------------------------

%% Init
-spec init_tables() -> ok.
init_tables() ->
    ok = mria:wait_for_tables(create_tables()).

%% @doc Update authz rules
-spec store_rules(maybe_namespace(), who(), rules()) -> ok.
store_rules(Namespace, {username, Username}, Rules) ->
    do_store_rules(Namespace, {?ACL_TABLE_USERNAME, Username}, normalize_rules(Rules));
store_rules(Namespace, {clientid, Clientid}, Rules) ->
    do_store_rules(Namespace, {?ACL_TABLE_CLIENTID, Clientid}, normalize_rules(Rules));
store_rules(Namespace, all, Rules) ->
    do_store_rules(Namespace, ?ACL_TABLE_ALL, normalize_rules(Rules)).

%% @doc Clean all authz rules for (username & clientid & all)
-spec purge_rules(maybe_namespace()) -> ok.
purge_rules(?global_ns) ->
    ok = lists:foreach(
        fun(Key) ->
            ok = mria:dirty_delete(?ACL_TABLE, Key)
        end,
        mnesia:dirty_all_keys(?ACL_TABLE)
    );
purge_rules(Namespace) when is_binary(Namespace) ->
    ok = lists:foreach(
        fun
            (?AUTHZ_WHO_NS(Ns, _) = Key) when Ns == Namespace ->
                ok = do_delete_one_ns(Ns, Key);
            (_Key) ->
                ok
        end,
        mnesia:dirty_all_keys(?AUTHZ_NS_TAB)
    ).

%% @doc Get one record
-spec get_rules(maybe_namespace(), who()) -> {ok, rules()} | not_found.
get_rules(Namespace, {username, Username}) ->
    do_get_rules(Namespace, {?ACL_TABLE_USERNAME, Username});
get_rules(Namespace, {clientid, Clientid}) ->
    do_get_rules(Namespace, {?ACL_TABLE_CLIENTID, Clientid});
get_rules(Namespace, all) ->
    do_get_rules(Namespace, ?ACL_TABLE_ALL).

%% @doc Delete one record
-spec delete_rules(maybe_namespace(), who()) -> ok.
delete_rules(Namespace, {username, Username}) ->
    do_delete_one(Namespace, {?ACL_TABLE_USERNAME, Username});
delete_rules(Namespace, {clientid, Clientid}) ->
    do_delete_one(Namespace, {?ACL_TABLE_CLIENTID, Clientid});
delete_rules(Namespace, all) ->
    do_delete_one(Namespace, ?ACL_TABLE_ALL).

-spec list_username_rules(maybe_namespace()) -> ets:match_spec().
list_username_rules(?global_ns) ->
    ets:fun2ms(
        fun(#?ACL_TABLE{who = {?ACL_TABLE_USERNAME, Username}, rules = Rules}) ->
            [{username, Username}, {rules, Rules}]
        end
    );
list_username_rules(Namespace) when is_binary(Namespace) ->
    %% ets:fun2ms(
    %%     fun(#?ACL_NS_TABLE{who = ?WHO_NS(Namespace, {?ACL_TABLE_USERNAME, Username}), rules = Rules}) ->
    %%         [{username, Username}, {rules, Rules}]
    %%     end
    %% ).
    %% Manually constructing match spec to ensure key is at least partially bound to avoid
    %% full scan.
    [
        {
            #?AUTHZ_NS_TAB{
                who = ?AUTHZ_WHO_NS(Namespace, {?ACL_TABLE_USERNAME, '$1'}), rules = '$2', _ = '_'
            },
            [],
            [[{{username, '$1'}}, {{rules, '$2'}}]]
        }
    ].

-spec list_clientid_rules(maybe_namespace()) -> ets:match_spec().
list_clientid_rules(?global_ns) ->
    ets:fun2ms(
        fun(#?ACL_TABLE{who = {?ACL_TABLE_CLIENTID, Clientid}, rules = Rules}) ->
            [{clientid, Clientid}, {rules, Rules}]
        end
    );
list_clientid_rules(Namespace) when is_binary(Namespace) ->
    %% ets:fun2ms(
    %%     fun(#?ACL_NS_TABLE{who = ?WHO_NS(Ns, {?ACL_TABLE_CLIENTID, Clientid}), rules = Rules}) when
    %%         Ns == Namespace
    %%     ->
    %%         [{clientid, Clientid}, {rules, Rules}]
    %%     end
    %% ).
    %% Manually constructing match spec to ensure key is at least partially bound to avoid
    %% full scan.
    [
        {
            #?AUTHZ_NS_TAB{
                who = ?AUTHZ_WHO_NS(Namespace, {?ACL_TABLE_CLIENTID, '$1'}), rules = '$2', _ = '_'
            },
            [],
            [[{{clientid, '$1'}}, {{rules, '$2'}}]]
        }
    ].

-spec record_count(maybe_namespace()) -> non_neg_integer().
record_count(?global_ns) ->
    mnesia:table_info(?ACL_TABLE, size);
record_count(Namespace) when is_binary(Namespace) ->
    try
        ets:lookup_element(?AUTHZ_NS_COUNT_TAB, Namespace, 2, 0)
    catch
        error:badarg -> 0
    end.

-spec record_count_per_namespace() -> #{emqx_config:namespace() => non_neg_integer()}.
record_count_per_namespace() ->
    try
        maps:from_list(ets:tab2list(?AUTHZ_NS_COUNT_TAB))
    catch
        %% `emqx_auth_mnesia' is not running: the table does not exist.
        error:badarg -> #{}
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

load_rules_for_authorize(?global_ns, Clientid, Username) ->
    do_load_rules_for_authorize(?global_ns, Clientid, Username);
load_rules_for_authorize(Namespace, Clientid, Username) when is_binary(Namespace) ->
    maybe
        [] ?= do_load_rules_for_authorize(Namespace, Clientid, Username),
        do_load_rules_for_authorize(?global_ns, Clientid, Username)
    end.

do_load_rules_for_authorize(Namespace, Clientid, Username) ->
    read_rules(Namespace, {?ACL_TABLE_CLIENTID, Clientid}) ++
        read_rules(Namespace, {?ACL_TABLE_USERNAME, Username}) ++
        read_rules(Namespace, ?ACL_TABLE_ALL).

read_rules(Namespace, Key) ->
    case do_get_rules(Namespace, Key) of
        {ok, Rules} -> Rules;
        not_found -> []
    end.

do_store_rules(?global_ns, Who, Rules) ->
    Record = #?ACL_TABLE{who = Who, rules = Rules},
    mria:dirty_write(Record);
do_store_rules(Namespace, Who, Rules) when is_binary(Namespace) ->
    Key = ?AUTHZ_WHO_NS(Namespace, Who),
    Record = #?AUTHZ_NS_TAB{who = Key, rules = Rules},
    do_write_one_ns(Namespace, Key, Record).

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

do_get_rules(?global_ns, Key) ->
    case mnesia:dirty_read(?ACL_TABLE, Key) of
        [#?ACL_TABLE{rules = Rules}] -> {ok, Rules};
        [] -> not_found
    end;
do_get_rules(Namespace, Key) when is_binary(Namespace) ->
    case mnesia:dirty_read(?AUTHZ_NS_TAB, ?AUTHZ_WHO_NS(Namespace, Key)) of
        [#?AUTHZ_NS_TAB{rules = Rules}] -> {ok, Rules};
        [] -> not_found
    end.

do_authorize(_AuthzContext, _PubSub, _Topic, []) ->
    nomatch;
do_authorize(AuthzContext, PubSub, Topic, [Rule | Tail]) ->
    CompliledRule = compile_rule(Rule),
    case emqx_authz_rule:match(AuthzContext, PubSub, Topic, CompliledRule) of
        {matched, Permission} -> {matched, Permission};
        nomatch -> do_authorize(AuthzContext, PubSub, Topic, Tail)
    end.

compile_rule({Permission, Who, Action, TopicFilter}) ->
    emqx_authz_rule:compile(Permission, Who, Action, [TopicFilter]);
compile_rule({Permission, Action, TopicFilter}) ->
    emqx_authz_rule:compile(Permission, all, Action, [TopicFilter]).

do_delete_one(?global_ns, TableWho) ->
    mria:dirty_delete(?ACL_TABLE, TableWho);
do_delete_one(Namespace, TableWho) when is_binary(Namespace) ->
    Key = ?AUTHZ_WHO_NS(Namespace, TableWho),
    do_delete_one_ns(Namespace, Key).

do_delete_one_ns(Namespace, Key) when is_binary(Namespace) ->
    HasKey = ets:member(?AUTHZ_NS_TAB, Key),
    mria:dirty_delete(?AUTHZ_NS_TAB, Key),
    HasKey andalso dec_ns_rule_count(Namespace),
    ok.

do_write_one_ns(Namespace, Key, Record) when is_binary(Namespace) ->
    HasKey = ets:member(?AUTHZ_NS_TAB, Key),
    mria:dirty_write(Record),
    HasKey orelse inc_ns_rule_count(Namespace),
    ok.

get_namespace(#{client_attrs := #{?CLIENT_ATTR_NAME_TNS := Namespace}} = _ClientInfo) when
    is_binary(Namespace)
->
    Namespace;
get_namespace(_ClientInfo) ->
    ?global_ns.

add_backup_rules(#?AUTHZ_NS_TAB{who = ?AUTHZ_WHO_NS(_Namespace, Who), rules = Rules0}, Acc) ->
    Rules = [emqx_authz_rule_raw:format_rule(Rule) || Rule <- Rules0],
    case Who of
        ?ACL_TABLE_ALL ->
            Acc#{<<"all">> := Rules};
        {?ACL_TABLE_USERNAME, Username} ->
            #{<<"users">> := Users} = Acc,
            Acc#{<<"users">> := [#{<<"username">> => Username, <<"rules">> => Rules} | Users]};
        {?ACL_TABLE_CLIENTID, Clientid} ->
            #{<<"clients">> := Clients} = Acc,
            Acc#{<<"clients">> := [#{<<"clientid">> => Clientid, <<"rules">> => Rules} | Clients]}
    end.

backup_entries(#{<<"users">> := Users, <<"clients">> := Clients, <<"all">> := AllRules}) when
    is_list(Users), is_list(Clients), is_list(AllRules)
->
    maybe
        {ok, UserEntries} ?= who_backup_entries(<<"username">>, username, Users),
        {ok, ClientEntries} ?= who_backup_entries(<<"clientid">>, clientid, Clients),
        AllEntries = [{all, AllRules} || AllRules =/= []],
        {ok, AllEntries ++ UserEntries ++ ClientEntries}
    end;
backup_entries(_Data) ->
    {error, invalid_rules}.

who_backup_entries(Key, Type, Entries) ->
    emqx_utils:foldl_while(
        fun
            (#{Key := Id, <<"rules">> := Rules}, {ok, Acc}) when is_binary(Id), is_list(Rules) ->
                {cont, {ok, [{{Type, Id}, Rules} | Acc]}};
            (_Entry, _Acc) ->
                {halt, {error, invalid_rules}}
        end,
        {ok, []},
        Entries
    ).

import_backup_entries(_Namespace, []) ->
    ok;
import_backup_entries(Namespace, Entries) ->
    maybe
        {ok, MaxRules} ?= max_rules(),
        ok ?=
            emqx_utils:foldl_while(
                fun({Who, Rules}, ok) ->
                    case validate_backup_rules(Rules, MaxRules) of
                        ok -> {cont, ok};
                        {error, Reason} -> {halt, {error, #{who => Who, reason => Reason}}}
                    end
                end,
                ok,
                Entries
            ),
        lists:foreach(fun({Who, Rules}) -> ok = store_rules(Namespace, Who, Rules) end, Entries)
    end.

validate_backup_rules(Rules, MaxRules) when length(Rules) > MaxRules ->
    {error, too_many_rules};
validate_backup_rules(Rules, _MaxRules) ->
    emqx_utils:foldl_while(
        fun(Rule, ok) ->
            case validate_backup_rule(Rule) of
                {ok, _} -> {cont, ok};
                {error, _} = Error -> {halt, Error}
            end
        end,
        ok,
        Rules
    ).

%% A rule holds a single `topic', as in the REST API, so it is stored as one rule.
validate_backup_rule(#{<<"topic">> := Topic} = Rule) when is_binary(Topic) ->
    emqx_authz_rule_raw:parse_rule(Rule);
validate_backup_rule(Rule) when is_map(Rule) ->
    {error, #{reason => invalid_topic, value => Rule}};
validate_backup_rule(Rule) ->
    emqx_authz_rule_raw:parse_rule(Rule).

max_rules() ->
    Sources = emqx:get_config([authorization, sources], []),
    case [Source || #{type := built_in_database} = Source <- Sources] of
        [#{max_rules := MaxRules}] -> {ok, MaxRules};
        [] -> {error, built_in_database_source_not_found}
    end.

inc_ns_rule_count(Namespace) when is_binary(Namespace) ->
    _ = ets:update_counter(?AUTHZ_NS_COUNT_TAB, Namespace, {2, 1}, {Namespace, 0}),
    ok.

dec_ns_rule_count(Namespace) when is_binary(Namespace) ->
    _ = ets:update_counter(?AUTHZ_NS_COUNT_TAB, Namespace, {2, -1, 0, 0}, {Namespace, 0}),
    ok.
