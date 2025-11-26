%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_scram_mnesia).

-include("emqx_auth_mnesia.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_config.hrl").
-include_lib("emqx_auth/include/emqx_authn.hrl").
-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("typerefl/include/types.hrl").

-behaviour(emqx_authn_provider).
-behaviour(emqx_db_backup).

-export([
    create/2,
    update/2,
    authenticate/2,
    destroy/1
]).

%% `emqx_authn_provider` API
-export([
    add_user/2,
    delete_user/3,
    update_user/4,
    lookup_user/3,
    list_users/2
]).

-export([
    run_fuzzy_filter/2,
    format_user_info/1
]).

-export([backup_tables/0]).

%% Internal exports (RPC)
-export([
    do_destroy/1,
    do_add_user/1,
    do_delete_user/3,
    do_update_user/4
]).

-define(TAB, ?MODULE).
-define(AUTHN_QSCHEMA, [
    {<<"like_user_id">>, binary},
    {<<"user_group">>, binary},
    {<<"is_superuser">>, atom}
]).

-type user_group() :: binary().
-type user_id() :: binary().

-export([init_tables/0]).

-record(user_info, {
    user_id,
    stored_key,
    server_key,
    salt,
    is_superuser
}).

-define(NS_TAB, emqx_authn_scram_mnesia_ns).
-define(NS_KEY(NS, GROUP, ID), {NS, GROUP, ID}).

-record(?NS_TAB, {
    user_id :: ?NS_KEY(emqx_config:namespace(), user_group(), user_id()),
    stored_key,
    server_key,
    salt,
    is_superuser,
    extra = #{} :: map()
}).
-reflect_type([user_group/0]).

%%------------------------------------------------------------------------------
%% Mnesia bootstrap
%%------------------------------------------------------------------------------

%% @doc Create or replicate tables.
-spec create_tables() -> [mria:table()].
create_tables() ->
    ok = mria:create_table(?TAB, [
        {rlog_shard, ?AUTHN_SHARD},
        {type, ordered_set},
        {storage, disc_copies},
        {record_name, user_info},
        {attributes, record_info(fields, user_info)},
        {storage_properties, [{ets, [{read_concurrency, true}]}]}
    ]),
    ok = mria:create_table(?NS_TAB, [
        {rlog_shard, ?AUTHN_SHARD},
        {type, ordered_set},
        {storage, disc_copies},
        {record_name, ?NS_TAB},
        {attributes, record_info(fields, ?NS_TAB)},
        {storage_properties, [{ets, [{read_concurrency, true}]}]}
    ]),
    [?TAB, ?NS_TAB].

-spec init_tables() -> ok.
init_tables() ->
    mria:wait_for_tables(create_tables()).

%%------------------------------------------------------------------------------
%% Data backup
%%------------------------------------------------------------------------------

backup_tables() -> {<<"builtin_authn">>, [?TAB, ?NS_TAB]}.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

create(
    AuthenticatorID,
    #{
        algorithm := Algorithm,
        iteration_count := IterationCount
    }
) ->
    State = #{
        user_group => AuthenticatorID,
        algorithm => Algorithm,
        iteration_count => IterationCount
    },
    {ok, State}.

update(Config, #{user_group := ID}) ->
    create(ID, Config).

authenticate(
    #{
        auth_method := AuthMethod,
        auth_data := AuthData,
        auth_cache := AuthCache
    } = Credential,
    State
) ->
    Namespace = get_namespace(Credential),
    RetrieveFun = fun(Username) ->
        retrieve(Namespace, Username, State)
    end,
    OnErrFun = fun(Msg, Reason) ->
        ?TRACE_AUTHN_PROVIDER(Msg, #{
            reason => Reason
        })
    end,
    emqx_utils_scram:authenticate(
        AuthMethod, AuthData, AuthCache, State, RetrieveFun, OnErrFun, [is_superuser]
    );
authenticate(_Credential, _State) ->
    ignore.

destroy(#{user_group := UserGroup}) ->
    trans(fun ?MODULE:do_destroy/1, [UserGroup]).

do_destroy(UserGroup) ->
    ok = lists:foreach(
        fun(UserInfo) ->
            mnesia:delete_object(?TAB, UserInfo, write)
        end,
        mnesia:select(?TAB, all_ns_group_match_spec(?global_ns, UserGroup), write)
    ),
    lists:foreach(
        fun(UserInfo) ->
            mnesia:delete_object(?NS_TAB, UserInfo, write)
        end,
        mnesia:select(?NS_TAB, all_ns_group_match_spec('_', UserGroup), write)
    ).

add_user(UserInfo, State) ->
    UserInfoRecord = user_info_record(UserInfo, State),
    trans(fun ?MODULE:do_add_user/1, [UserInfoRecord]).

do_add_user(UserInfoRecord) ->
    case do_lookup_by_rec_txn(UserInfoRecord) of
        [] ->
            ok = insert_user(UserInfoRecord),
            #{
                namespace := Namespace,
                user_id := UserId,
                is_superuser := IsSuperuser
            } = rec_to_map(UserInfoRecord),
            {ok, #{namespace => Namespace, user_id => UserId, is_superuser => IsSuperuser}};
        [_] ->
            {error, already_exist}
    end.

delete_user(Namespace, UserId, State) ->
    trans(fun ?MODULE:do_delete_user/3, [Namespace, UserId, State]).

do_delete_user(Namespace, UserId, #{user_group := UserGroup}) ->
    Table = table(Namespace),
    Key = key(Namespace, UserGroup, UserId),
    case mnesia:read(Table, Key, write) of
        [] ->
            {error, not_found};
        [_] ->
            mnesia:delete(Table, Key, write)
    end.

update_user(Namespace, UserId, User, State) ->
    FieldsToUpdate = fields_to_update(
        User,
        [
            keys_and_salt,
            is_superuser
        ],
        State
    ),
    trans(fun ?MODULE:do_update_user/4, [Namespace, UserId, FieldsToUpdate, State]).

do_update_user(
    Namespace,
    UserId,
    FieldsToUpdate,
    #{user_group := UserGroup} = _State
) ->
    Table = table(Namespace),
    Key = key(Namespace, UserGroup, UserId),
    case mnesia:read(Table, Key, write) of
        [] ->
            {error, not_found};
        [UserInfo0] ->
            UserInfo1 = update_user_record(UserInfo0, FieldsToUpdate),
            mnesia:write(Table, UserInfo1, write),
            {ok, format_user_info(UserInfo1)}
    end.

lookup_user(Namespace, UserId, #{user_group := UserGroup}) ->
    Table = table(Namespace),
    Key = key(Namespace, UserGroup, UserId),
    case mnesia:dirty_read(Table, Key) of
        [UserInfo] ->
            {ok, format_user_info(UserInfo)};
        [] ->
            {error, not_found}
    end.

list_users(QueryString0, #{user_group := UserGroup}) ->
    Namespace =
        case QueryString0 of
            #{<<"ns">> := Ns} -> Ns;
            _ -> ?global_ns
        end,
    NQueryString = QueryString0#{<<"user_group">> => UserGroup},
    Table = table(Namespace),
    emqx_mgmt_api:node_query(
        node(),
        Table,
        NQueryString,
        ?AUTHN_QSCHEMA,
        mk_qs2ms(Namespace),
        fun ?MODULE:format_user_info/1
    ).

%%--------------------------------------------------------------------
%% QueryString to MatchSpec

-spec mk_qs2ms(emqx_config:maybe_namespace()) ->
    fun((atom(), {list(), list()}) -> emqx_mgmt_api:match_spec_and_filter()).
mk_qs2ms(Namespace) ->
    fun(_Tab, {QString, FuzzyQString}) ->
        #{
            match_spec => ms_from_qstring(Namespace, QString),
            fuzzy_fun => fuzzy_filter_fun(FuzzyQString)
        }
    end.

%% Fuzzy username funcs
fuzzy_filter_fun([]) ->
    undefined;
fuzzy_filter_fun(Fuzzy) ->
    {fun ?MODULE:run_fuzzy_filter/2, [Fuzzy]}.

run_fuzzy_filter(_, []) ->
    true;
run_fuzzy_filter(
    E = #user_info{user_id = {_, UserId}},
    [{user_id, like, UserIdSubStr} | Fuzzy]
) ->
    binary:match(UserId, UserIdSubStr) /= nomatch andalso run_fuzzy_filter(E, Fuzzy);
run_fuzzy_filter(
    E = #?NS_TAB{user_id = ?NS_KEY(_, _, UserId)},
    [{user_id, like, UserIdSubStr} | Fuzzy]
) ->
    binary:match(UserId, UserIdSubStr) /= nomatch andalso run_fuzzy_filter(E, Fuzzy).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

user_info_record(
    #{
        user_id := UserId,
        password := Password
    } = UserInfo,
    #{user_group := UserGroup} = State
) ->
    Namespace = maps:get(namespace, UserInfo, ?global_ns),
    IsSuperuser = maps:get(is_superuser, UserInfo, false),
    user_info_record(Namespace, UserGroup, UserId, Password, IsSuperuser, State).

user_info_record(?global_ns, UserGroup, UserId, Password, IsSuperuser, State) ->
    {StoredKey, ServerKey, Salt} = sasl_auth_scram:generate_authentication_info(Password, State),
    #user_info{
        user_id = {UserGroup, UserId},
        stored_key = StoredKey,
        server_key = ServerKey,
        salt = Salt,
        is_superuser = IsSuperuser
    };
user_info_record(Namespace, UserGroup, UserId, Password, IsSuperuser, State) when
    is_binary(Namespace)
->
    {StoredKey, ServerKey, Salt} = sasl_auth_scram:generate_authentication_info(Password, State),
    #?NS_TAB{
        user_id = ?NS_KEY(Namespace, UserGroup, UserId),
        stored_key = StoredKey,
        server_key = ServerKey,
        salt = Salt,
        is_superuser = IsSuperuser
    }.

fields_to_update(
    #{password := Password} = UserInfo,
    [keys_and_salt | Rest],
    State
) ->
    {StoredKey, ServerKey, Salt} = sasl_auth_scram:generate_authentication_info(Password, State),
    [
        {keys_and_salt, {StoredKey, ServerKey, Salt}}
        | fields_to_update(UserInfo, Rest, State)
    ];
fields_to_update(#{is_superuser := IsSuperuser} = UserInfo, [is_superuser | Rest], State) ->
    [{is_superuser, IsSuperuser} | fields_to_update(UserInfo, Rest, State)];
fields_to_update(UserInfo, [_ | Rest], State) ->
    fields_to_update(UserInfo, Rest, State);
fields_to_update(_UserInfo, [], _State) ->
    [].

update_user_record(UserInfoRecord, []) ->
    UserInfoRecord;
update_user_record(#user_info{} = UserInfoRecord, [
    {keys_and_salt, {StoredKey, ServerKey, Salt}} | Rest
]) ->
    update_user_record(
        UserInfoRecord#user_info{
            stored_key = StoredKey,
            server_key = ServerKey,
            salt = Salt
        },
        Rest
    );
update_user_record(#?NS_TAB{} = UserInfoRecord, [
    {keys_and_salt, {StoredKey, ServerKey, Salt}} | Rest
]) ->
    update_user_record(
        UserInfoRecord#?NS_TAB{
            stored_key = StoredKey,
            server_key = ServerKey,
            salt = Salt
        },
        Rest
    );
update_user_record(#user_info{} = UserInfoRecord, [{is_superuser, IsSuperuser} | Rest]) ->
    update_user_record(UserInfoRecord#user_info{is_superuser = IsSuperuser}, Rest);
update_user_record(#?NS_TAB{} = UserInfoRecord, [{is_superuser, IsSuperuser} | Rest]) ->
    update_user_record(UserInfoRecord#?NS_TAB{is_superuser = IsSuperuser}, Rest).

retrieve(Namespace, UserId, #{user_group := UserGroup}) ->
    Table = table(Namespace),
    Key = key(Namespace, UserGroup, UserId),
    case mnesia:dirty_read(Table, Key) of
        [UserInfoRecord] ->
            UserInfoMap0 = rec_to_map(UserInfoRecord),
            UserInfoMap = maps:with([stored_key, server_key, salt, is_superuser], UserInfoMap0),
            {ok, UserInfoMap};
        [] ->
            {error, not_found}
    end.

%% TODO: Move to emqx_authn_utils.erl
trans(Fun, Args) ->
    case mria:transaction(?AUTHN_SHARD, Fun, Args) of
        {atomic, Res} -> Res;
        {aborted, {function_clause, Stack}} -> erlang:raise(error, function_clause, Stack);
        {aborted, Reason} -> {error, Reason}
    end.

format_user_info(#user_info{user_id = {_, UserId}, is_superuser = IsSuperuser}) ->
    #{user_id => UserId, is_superuser => IsSuperuser};
format_user_info(#?NS_TAB{user_id = ?NS_KEY(_, _, UserId), is_superuser = IsSuperuser}) ->
    #{user_id => UserId, is_superuser => IsSuperuser}.

ms_from_qstring(Namespace, QString) ->
    case lists:keytake(user_group, 1, QString) of
        {value, {user_group, '=:=', UserGroup}, QString2} ->
            group_match_spec(Namespace, UserGroup, QString2);
        _ ->
            []
    end.

all_ns_group_match_spec(Namespace, UserGroup) ->
    group_match_spec(Namespace, UserGroup, []).

group_match_spec(Namespace, UserGroup, QString) ->
    %% We manually construct match specs to ensure we have a partially bound key instead
    %% of using a guard that would result in a full scan.
    case lists:keyfind(is_superuser, 1, QString) of
        false ->
            MH = mk_group_match_head(Namespace, UserGroup, _PosValues = []),
            [{MH, [], ['$_']}];
        {is_superuser, '=:=', Value} ->
            PosValues = [mk_pos_value(Namespace, is_superuser, Value)],
            MH = mk_group_match_head(Namespace, UserGroup, PosValues),
            [{MH, [], ['$_']}]
    end.

mk_group_match_head(?global_ns, UserGroup, PosValues) ->
    erlang:make_tuple(
        record_info(size, user_info),
        '_',
        [{1, user_info}, {#user_info.user_id, {UserGroup, '_'}} | PosValues]
    );
mk_group_match_head(Namespace, UserGroup, PosValues) when is_binary(Namespace); Namespace == '_' ->
    erlang:make_tuple(
        record_info(size, ?NS_TAB),
        '_',
        [{1, ?NS_TAB}, {#?NS_TAB.user_id, ?NS_KEY(Namespace, UserGroup, '_')} | PosValues]
    ).

mk_pos_value(?global_ns, is_superuser, Value) ->
    {#user_info.is_superuser, Value};
mk_pos_value(Namespace, is_superuser, Value) when is_binary(Namespace); Namespace == '_' ->
    {#?NS_TAB.is_superuser, Value}.

insert_user(#user_info{} = UserInfoRecord) ->
    mnesia:write(?TAB, UserInfoRecord, write);
insert_user(#?NS_TAB{} = UserInfoRecord) ->
    mnesia:write(?NS_TAB, UserInfoRecord, write).

do_lookup_by_rec_txn(#user_info{user_id = Key}) ->
    mnesia:read(?TAB, Key, write);
do_lookup_by_rec_txn(#?NS_TAB{user_id = Key}) ->
    mnesia:read(?NS_TAB, Key, write).

rec_to_map(#user_info{} = Rec) ->
    #user_info{
        user_id = {UserGroup, UserId},
        stored_key = StoredKey,
        server_key = ServerKey,
        salt = Salt,
        is_superuser = IsSuperuser
    } = Rec,
    #{
        namespace => ?global_ns,
        user_id => UserId,
        user_group => UserGroup,
        stored_key => StoredKey,
        server_key => ServerKey,
        salt => Salt,
        is_superuser => IsSuperuser,
        extra => #{}
    };
rec_to_map(#?NS_TAB{} = Rec) ->
    #?NS_TAB{
        user_id = ?NS_KEY(Namespace, UserGroup, UserId),
        stored_key = StoredKey,
        server_key = ServerKey,
        salt = Salt,
        is_superuser = IsSuperuser,
        extra = Extra
    } = Rec,
    #{
        namespace => Namespace,
        user_id => UserId,
        user_group => UserGroup,
        stored_key => StoredKey,
        server_key => ServerKey,
        salt => Salt,
        is_superuser => IsSuperuser,
        extra => Extra
    }.

table(?global_ns) -> ?TAB;
table(Namespace) when is_binary(Namespace) -> ?NS_TAB.

key(?global_ns, UserGroup, UserId) ->
    {UserGroup, UserId};
key(Namespace, UserGroup, UserId) when is_binary(Namespace) ->
    ?NS_KEY(Namespace, UserGroup, UserId).

get_namespace(#{client_attrs := #{?CLIENT_ATTR_NAME_TNS := Namespace}} = _ClientInfo) when
    is_binary(Namespace)
->
    Namespace;
get_namespace(_ClientInfo) ->
    ?global_ns.
