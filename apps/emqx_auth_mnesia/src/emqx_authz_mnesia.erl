%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authz_mnesia).

-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("emqx/include/logger.hrl").

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

-type table_who() ::
    ?ACL_TABLE_ALL | {?ACL_TABLE_USERNAME, binary()} | {?ACL_TABLE_CLIENTID, binary()}.

-record(?ACL_TABLE, {
    who :: table_who(),
    rules :: rules()
}).

-define(WHO_NS(NS, WHO), {NS, WHO}).

-type maybe_namespace() :: emqx_config:maybe_namespace().

-record(?ACL_NS_TABLE, {
    %% ?WHO_NS(emqx_config:namespace(), table_who())
    who,
    %% rules()
    rules,
    extra = #{}
}).

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
    ok = mria:create_table(?ACL_NS_TABLE, [
        {type, ordered_set},
        {rlog_shard, ?ACL_SHARDED},
        {storage, disc_copies},
        {attributes, record_info(fields, ?ACL_NS_TABLE)},
        {storage_properties, [{ets, [{read_concurrency, true}]}]}
    ]),
    [?ACL_TABLE, ?ACL_NS_TABLE].

%%--------------------------------------------------------------------
%% emqx_authz callbacks
%%--------------------------------------------------------------------

create(Source) -> Source.

update(_State, Source) -> create(Source).

destroy(_Source) ->
    {atomic, ok} = mria:clear_table(?ACL_TABLE),
    {atomic, ok} = mria:clear_table(?ACL_NS_TABLE),
    ok.

authorize(
    #{
        username := Username,
        clientid := Clientid
    } = ClientInfo,
    PubSub,
    Topic,
    #{type := built_in_database}
) ->
    Namespace = get_namespace(ClientInfo),
    Rules =
        read_rules(Namespace, {?ACL_TABLE_CLIENTID, Clientid}) ++
            read_rules(Namespace, {?ACL_TABLE_USERNAME, Username}) ++
            read_rules(Namespace, ?ACL_TABLE_ALL),
    do_authorize(ClientInfo, PubSub, Topic, Rules).

%%--------------------------------------------------------------------
%% Data backup
%%--------------------------------------------------------------------

backup_tables() -> {<<"builtin_authz">>, [?ACL_TABLE, ?ACL_NS_TABLE]}.

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
            (?WHO_NS(Ns, _) = Key) when Ns == Namespace ->
                ok = mria:dirty_delete(?ACL_NS_TABLE, Key);
            (_Key) ->
                ok
        end,
        mnesia:dirty_all_keys(?ACL_NS_TABLE)
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
            #?ACL_NS_TABLE{
                who = ?WHO_NS(Namespace, {?ACL_TABLE_USERNAME, '$1'}), rules = '$2', _ = '_'
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
            #?ACL_NS_TABLE{
                who = ?WHO_NS(Namespace, {?ACL_TABLE_CLIENTID, '$1'}), rules = '$2', _ = '_'
            },
            [],
            [[{{clientid, '$1'}}, {{rules, '$2'}}]]
        }
    ].

-spec record_count(maybe_namespace()) -> non_neg_integer().
record_count(?global_ns) ->
    mnesia:table_info(?ACL_TABLE, size);
record_count(Namespace) when is_binary(Namespace) ->
    %% MS = ets:fun2ms(
    %%     fun(#?ACL_NS_TABLE{who = ?WHO_NS(Ns, _)}) when Ns == Namespace ->
    %%         true
    %%     end
    %% ),
    %% Manually constructing match spec to ensure key is at least partially bound to avoid
    %% full scan.
    MS = [
        {
            #?ACL_NS_TABLE{who = ?WHO_NS(Namespace, '_'), _ = '_'},
            [],
            [true]
        }
    ],
    ets:select_count(?ACL_NS_TABLE, MS).

-spec record_count_per_namespace() -> #{emqx_config:namespace() => non_neg_integer()}.
record_count_per_namespace() ->
    ets:foldl(
        fun(#?ACL_NS_TABLE{who = ?WHO_NS(Namespace, _)}, Acc) ->
            maps:update_with(Namespace, fun(N) -> N + 1 end, 1, Acc)
        end,
        #{},
        ?ACL_NS_TABLE
    ).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

read_rules(Namespace, Key) ->
    case do_get_rules(Namespace, Key) of
        {ok, Rules} -> Rules;
        not_found -> []
    end.

do_store_rules(?global_ns, Who, Rules) ->
    Record = #?ACL_TABLE{who = Who, rules = Rules},
    mria:dirty_write(Record);
do_store_rules(Namespace, Who, Rules) when is_binary(Namespace) ->
    Record = #?ACL_NS_TABLE{who = ?WHO_NS(Namespace, Who), rules = Rules},
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

do_get_rules(?global_ns, Key) ->
    case mnesia:dirty_read(?ACL_TABLE, Key) of
        [#?ACL_TABLE{rules = Rules}] -> {ok, Rules};
        [] -> not_found
    end;
do_get_rules(Namespace, Key) when is_binary(Namespace) ->
    case mnesia:dirty_read(?ACL_NS_TABLE, ?WHO_NS(Namespace, Key)) of
        [#?ACL_NS_TABLE{rules = Rules}] -> {ok, Rules};
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

do_delete_one(?global_ns, TableWho) ->
    mria:dirty_delete(?ACL_TABLE, TableWho);
do_delete_one(Namespace, TableWho) when is_binary(Namespace) ->
    mria:dirty_delete(?ACL_NS_TABLE, ?WHO_NS(Namespace, TableWho)).

get_namespace(#{client_attrs := #{?CLIENT_ATTR_NAME_TNS := Namespace}} = _ClientInfo) when
    is_binary(Namespace)
->
    Namespace;
get_namespace(_ClientInfo) ->
    ?global_ns.
