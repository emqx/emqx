%%--------------------------------------------------------------------
%% Copyright (c) 2021-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_authn_mnesia).

-include("emqx_auth_mnesia.hrl").
-include("emqx_auth_mnesia_internal.hrl").
-include_lib("emqx_auth/include/emqx_authn.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_config.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

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
    import_users/2,
    add_user/2,
    delete_user/3,
    update_user/4,
    rotate_password/3,
    lookup_user/3,
    list_users/2
]).

-export([record_count/1, record_count_per_namespace/0]).

-export([purge_namespace/1]).

-export([
    run_fuzzy_filter/2,
    format_user_info/1
]).

%% Internal exports (Mnesia transactions)
-export([
    destroy_tx/1,
    add_user_tx/1,
    delete_user_tx/3,
    update_user_tx/4,
    rotate_password_tx/4
]).

%% RPC target
-export([
    need_use_ns_table_for_global/0
]).

-export([init_tables/0]).

-export([backup_tables/0]).

-ifdef(TEST).
-export([rec_to_map/1]).
-endif.

-export_type([user_group/0, user_id/0]).

-type user_group() :: atom() | binary().
-type user_id() :: binary().

-record(user_info, {
    user_id :: {user_group(), user_id()},
    password_hash :: binary(),
    salt :: binary(),
    is_superuser :: boolean()
}).

-define(TAB, ?MODULE).
-define(GLOBAL_NS_TABLE_BPAPI_VSN, 3).
-define(AUTHN_QSCHEMA, [
    {<<"like_user_id">>, binary},
    {<<"user_group">>, binary},
    {<<"is_superuser">>, atom}
]).
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
    ok = mria:create_table(?AUTHN_NS_TAB, [
        {rlog_shard, ?AUTHN_SHARD},
        {type, ordered_set},
        {storage, disc_copies},
        {record_name, ?AUTHN_NS_TAB},
        {attributes, record_info(fields, ?AUTHN_NS_TAB)},
        {storage_properties, [{ets, [{read_concurrency, true}]}]}
    ]),
    ok = emqx_utils_ets:new(?AUTHN_NS_COUNT_TAB, [ordered_set, public]),
    [?TAB, ?AUTHN_NS_TAB].

%% Init
-spec init_tables() -> ok.
init_tables() ->
    ok = mria:wait_for_tables(create_tables()).

%%------------------------------------------------------------------------------
%% Data backup
%%------------------------------------------------------------------------------

backup_tables() -> {<<"builtin_authn">>, [?TAB, ?AUTHN_NS_TAB]}.

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

%% RPC target
need_use_ns_table_for_global() -> true.

create(_AuthenticatorID, Config) ->
    create(Config).

create(
    #{
        user_id_type := Type,
        password_hash_algorithm := Algorithm,
        user_group := UserGroup,
        autogenerate_password := AutogeneratePassword
    } = Config
) ->
    ok = emqx_authn_password_hashing:init(Algorithm),
    State = #{
        user_group => UserGroup,
        user_id_type => Type,
        password_hash_algorithm => Algorithm,
        autogenerate_password => AutogeneratePassword
    },
    ok = bootstrap_user_from_file(Config, State),
    {ok, State}.

update(Config, _State) ->
    create(Config).

authenticate(#{auth_method := _}, _) ->
    ignore;
authenticate(#{password := undefined}, _) ->
    ignore;
authenticate(
    #{password := Password} = Credential,
    #{
        user_group := UserGroup,
        user_id_type := Type,
        password_hash_algorithm := Algorithm0
    }
) ->
    Namespace = get_namespace(Credential),
    UserId = get_user_identity(Credential, Type),
    case lookup_user_with_fallback(Namespace, UserGroup, UserId) of
        error ->
            ?TRACE_AUTHN_PROVIDER("user_not_found"),
            ignore;
        {ok, #{
            password_hash := PasswordHash,
            salt := Salt,
            is_superuser := IsSuperuser,
            extra := Extra
        }} ->
            Algorithm =
                case Extra of
                    #{algo := StoredAlgorithm} ->
                        emqx_authn_password_hashing:decode(StoredAlgorithm);
                    #{} ->
                        Algorithm0
                end,
            case
                emqx_authn_password_hashing:check_password(
                    Algorithm, Salt, PasswordHash, Password
                )
            of
                true ->
                    {ok, #{is_superuser => IsSuperuser}};
                false ->
                    {error, bad_username_or_password}
            end
    end.

%% fixme ns
destroy(#{user_group := UserGroup}) ->
    trans(fun ?MODULE:destroy_tx/1, [UserGroup]).

destroy_tx(UserGroup) ->
    ok = lists:foreach(
        fun(User) ->
            mnesia:delete_object(?TAB, User, write)
        end,
        mnesia:select(?TAB, legacy_group_match_spec(UserGroup), write)
    ),
    lists:foreach(
        fun(User) ->
            mnesia:delete_object(?AUTHN_NS_TAB, User, write)
        end,
        mnesia:select(?AUTHN_NS_TAB, all_ns_group_match_spec('_', UserGroup), write)
    ).

-doc """
Deletes all users belonging to the given namespace, across all user groups.

Users in the global namespace are never touched.
""".
-spec purge_namespace(emqx_config:namespace()) -> ok.
purge_namespace(Namespace) when is_binary(Namespace) ->
    ok = lists:foreach(
        fun(#?AUTHN_NS_TAB{user_id = Key}) ->
            ok = mria:dirty_delete(?AUTHN_NS_TAB, Key)
        end,
        mnesia:dirty_select(?AUTHN_NS_TAB, all_ns_group_match_spec(Namespace, '_'))
    ),
    _ = ets:delete(?AUTHN_NS_COUNT_TAB, Namespace),
    ok.

import_users(ImportSource, State) ->
    import_users(ImportSource, State, #{override => true}).

import_users({PasswordType, Filename, FileData}, State, Opts) ->
    Convertor = converter(PasswordType, State),
    try parse_import_users(Filename, FileData, Convertor) of
        {_NewUsersCnt, Users} ->
            case do_import_users(Users, Opts#{filename => Filename}) of
                {ok, Result} ->
                    {ok, Result};
                %% Do not log empty user entries.
                %% The default etc/auth-built-in-db.csv file contains an empty user entry.
                {error, empty_users} ->
                    {error, empty_users}
            end
    catch
        error:Reason:Stk ->
            ?SLOG(
                warning,
                #{
                    msg => "parse_authn_users_failed",
                    reason => Reason,
                    type => PasswordType,
                    filename => Filename,
                    stacktrace => Stk
                }
            ),
            {error, Reason}
    end.

do_import_users([], _Opts) ->
    {error, empty_users};
do_import_users(Users, Opts) ->
    FoldFn = fun(User, Acc0) ->
        Return = insert_user_tx(User, Opts),
        case Return of
            {Result, Ns, CountRecord} when Result =:= success; Result =:= override ->
                PerNs0 = maps:get(per_ns, Acc0),
                PerNs =
                    case CountRecord of
                        true -> inc_bucket_count(Ns, PerNs0);
                        false -> PerNs0
                    end,
                Acc1 = Acc0#{per_ns := PerNs},
                inc_bucket_count(Result, Acc1);
            _ ->
                inc_bucket_count(Return, Acc0)
        end
    end,
    Fun = fun() ->
        lists:foldl(
            FoldFn,
            #{success => 0, skipped => 0, override => 0, failed => 0, per_ns => #{}},
            Users
        )
    end,
    Res0 = #{per_ns := FinalPerNs} = trans(Fun),
    maps:foreach(fun inc_ns_rule_count/2, FinalPerNs),
    Res = maps:remove(per_ns, Res0),
    {ok, Res#{total => length(Users)}}.

inc_bucket_count(Bucket, Acc) ->
    N = maps:get(Bucket, Acc, 0),
    maps:put(Bucket, N + 1, Acc).

add_user(
    UserInfo,
    #{autogenerate_password := true}
) when is_map_key(password, UserInfo) ->
    {error, password_not_allowed};
add_user(
    UserInfo,
    #{autogenerate_password := true, password_hash_algorithm := Algorithm} = State
) ->
    Password = emqx_authn_password_hashing:gen_password(),
    UserInfoRecord = user_info_record(UserInfo#{password => Password}, State, Algorithm),
    add_user_record(UserInfoRecord, Password);
add_user(UserInfo, #{autogenerate_password := false}) when not is_map_key(password, UserInfo) ->
    {error, password_required};
add_user(UserInfo, State) ->
    Algorithm = maps:get(password_hash_algorithm, State),
    UserInfoRecord = user_info_record(UserInfo, State, Algorithm),
    add_user_record(UserInfoRecord, undefined).

add_user_record(UserInfoRecord, Password) ->
    Res = trans(fun ?MODULE:add_user_tx/1, [UserInfoRecord]),
    maybe
        {ok, #{namespace := Namespace}} ?= Res,
        true ?= is_namespaced_record(UserInfoRecord),
        inc_ns_rule_count(Namespace, 1)
    end,
    case Res of
        {ok, _} = Ok when Password =:= undefined -> Ok;
        {ok, User} -> {ok, User#{password => Password}};
        Error -> Error
    end.

add_user_tx(UserInfoRecord) ->
    case lookup_by_record_tx(UserInfoRecord) of
        [] ->
            case do_lookup_conflicting_global_txn(UserInfoRecord) of
                [] ->
                    ok = write_user_tx(UserInfoRecord),
                    #{
                        namespace := Namespace,
                        user_id := UserId,
                        is_superuser := IsSuperuser
                    } =
                        rec_to_map(UserInfoRecord),
                    {ok, #{
                        namespace => Namespace, user_id => UserId, is_superuser => IsSuperuser
                    }};
                [_] ->
                    {error, already_exist}
            end;
        [_] ->
            {error, already_exist}
    end.

delete_user(Namespace, UserId, State) ->
    Res = trans(fun ?MODULE:delete_user_tx/3, [Namespace, UserId, State]),
    case Res of
        {ok, true} ->
            dec_ns_rule_count(Namespace, 1),
            ok;
        {ok, false} ->
            ok;
        Error ->
            Error
    end.

delete_user_tx(Namespace, UserId, #{user_group := UserGroup}) ->
    NSKey = ?AUTHN_NS_KEY(Namespace, UserGroup, UserId),
    NSRecords = mnesia:read(?AUTHN_NS_TAB, NSKey, write),
    LegacyRecords = read_legacy_global_tx(Namespace, UserGroup, UserId),
    case NSRecords ++ LegacyRecords of
        [] ->
            {error, not_found};
        _ ->
            NSRecords =/= [] andalso mnesia:delete(?AUTHN_NS_TAB, NSKey, write),
            LegacyRecords =/= [] andalso delete_legacy_global_tx(Namespace, UserGroup, UserId),
            {ok, NSRecords =/= []}
    end.

update_user(_Namespace, _UserId, #{password := _}, #{autogenerate_password := true}) ->
    {error, password_not_allowed};
update_user(Namespace, UserId, UserInfo, State) ->
    FieldsToUpdate = fields_to_update(
        UserInfo,
        [
            algo,
            hash_and_salt,
            is_superuser
        ],
        State
    ),
    finish_user_update(
        Namespace,
        trans(fun ?MODULE:update_user_tx/4, [Namespace, UserId, FieldsToUpdate, State])
    ).

update_user_tx(
    Namespace,
    UserId,
    FieldsToUpdate,
    #{user_group := UserGroup, password_hash_algorithm := Algorithm}
) ->
    case read_user_for_update_tx(Namespace, UserGroup, UserId, Algorithm) of
        error ->
            {error, not_found};
        {ok, UserInfoRecord} ->
            NUserInfoRecord = update_user_record(UserInfoRecord, FieldsToUpdate),
            CountRecord = is_new_namespaced_record_tx(NUserInfoRecord),
            ok = write_user_tx(NUserInfoRecord),
            maybe_delete_legacy_global_tx(NUserInfoRecord, Namespace, UserGroup, UserId),
            #{user_id := UserId, is_superuser := IsSuperuser} = rec_to_map(NUserInfoRecord),
            {ok, #{namespace => Namespace, user_id => UserId, is_superuser => IsSuperuser},
                CountRecord}
    end.

rotate_password(_Namespace, _UserId, #{autogenerate_password := false}) ->
    {error, password_rotation_disabled};
rotate_password(Namespace, UserId, State) ->
    Password = emqx_authn_password_hashing:gen_password(),
    Res = finish_user_update(
        Namespace,
        trans(fun ?MODULE:rotate_password_tx/4, [Namespace, UserId, Password, State])
    ),
    case Res of
        {ok, User} -> {ok, User#{password => Password}};
        Error -> Error
    end.

rotate_password_tx(Namespace, UserId, Password, #{
    user_group := UserGroup, password_hash_algorithm := Algorithm
}) ->
    case read_user_for_update_tx(Namespace, UserGroup, UserId, Algorithm) of
        error ->
            {error, not_found};
        {ok, UserInfoRecord} ->
            {PasswordHash, Salt} = emqx_authn_password_hashing:hash(Algorithm, Password),
            NUserInfoRecord = update_user_record(UserInfoRecord, [
                {hash_and_salt, {PasswordHash, Salt}}, {algo, Algorithm}
            ]),
            CountRecord = is_new_namespaced_record_tx(NUserInfoRecord),
            ok = write_user_tx(NUserInfoRecord),
            maybe_delete_legacy_global_tx(NUserInfoRecord, Namespace, UserGroup, UserId),
            #{is_superuser := IsSuperuser} = rec_to_map(NUserInfoRecord),
            {ok, #{namespace => Namespace, user_id => UserId, is_superuser => IsSuperuser},
                CountRecord}
    end.

finish_user_update(Namespace, {ok, User, true}) ->
    inc_ns_rule_count(Namespace, 1),
    {ok, User};
finish_user_update(_Namespace, {ok, User, false}) ->
    {ok, User};
finish_user_update(_Namespace, Error) ->
    Error.

lookup_user(Namespace, UserId, #{user_group := UserGroup}) ->
    case do_lookup_user(Namespace, UserGroup, UserId) of
        {ok, UserInfo} -> {ok, public_user_info(UserInfo)};
        error -> {error, not_found}
    end.

list_users(QueryString0, #{user_group := UserGroup}) ->
    Namespace =
        case QueryString0 of
            #{<<"ns">> := Ns} -> Ns;
            _ -> ?global_ns
        end,
    case Namespace of
        ?global_ns ->
            QueryString = QueryString0#{<<"user_group">> => UserGroup},
            Result = emqx_mgmt_api:node_query_with_tabs(
                node(),
                [?TAB, ?AUTHN_NS_TAB],
                QueryString,
                ?AUTHN_QSCHEMA,
                mk_qs2ms(Namespace),
                fun ?MODULE:format_user_info/1
            ),
            fix_global_list_count(Result, QueryString);
        _ when is_binary(Namespace) ->
            QueryString = QueryString0#{<<"user_group">> => UserGroup},
            emqx_mgmt_api:node_query(
                node(),
                ?AUTHN_NS_TAB,
                QueryString,
                ?AUTHN_QSCHEMA,
                mk_qs2ms(Namespace),
                fun ?MODULE:format_user_info/1
            )
    end.

-spec record_count(emqx_config:maybe_namespace()) -> non_neg_integer().
record_count(?global_ns) ->
    mnesia:table_info(?TAB, size) + namespace_record_count(?global_ns);
record_count(Namespace) when is_binary(Namespace) ->
    namespace_record_count(Namespace).

namespace_record_count(Namespace) ->
    try
        ets:lookup_element(?AUTHN_NS_COUNT_TAB, Namespace, 2, 0)
    catch
        error:badarg -> 0
    end.

-spec record_count_per_namespace() -> #{emqx_config:namespace() => non_neg_integer()}.
record_count_per_namespace() ->
    try
        maps:from_list(ets:tab2list(?AUTHN_NS_COUNT_TAB))
    catch
        %% `emqx_auth_mnesia' is not running: the table does not exist.
        error:badarg -> #{}
    end.

%%--------------------------------------------------------------------
%% QueryString to MatchSpec

-spec mk_qs2ms(emqx_config:maybe_namespace()) ->
    fun((atom(), {list(), list()}) -> emqx_mgmt_api:match_spec_and_filter()).
mk_qs2ms(Namespace) ->
    fun(Tab, {QString, FuzzyQString}) ->
        #{
            match_spec => ms_from_qstring(Tab, Namespace, QString),
            fuzzy_fun => fuzzy_filter_fun(FuzzyQString)
        }
    end.

ms_from_qstring(Tab, Namespace, QString) ->
    case lists:keytake(user_group, 1, QString) of
        {value, {user_group, '=:=', UserGroup}, QString2} ->
            group_match_spec(Tab, Namespace, UserGroup, QString2);
        _ ->
            []
    end.

fuzzy_filter_fun([]) ->
    undefined;
fuzzy_filter_fun(Fuzzy) ->
    {fun ?MODULE:run_fuzzy_filter/2, [Fuzzy]}.

%% Fuzzy username funcs
run_fuzzy_filter(_, []) ->
    true;
run_fuzzy_filter(
    E = #user_info{user_id = {_, UserId}},
    [{user_id, like, UsernameSubStr} | Fuzzy]
) ->
    binary:match(UserId, UsernameSubStr) /= nomatch andalso run_fuzzy_filter(E, Fuzzy);
run_fuzzy_filter(
    E = #?AUTHN_NS_TAB{user_id = ?AUTHN_NS_KEY(_, _, UserId)},
    [{user_id, like, UsernameSubStr} | Fuzzy]
) ->
    binary:match(UserId, UsernameSubStr) /= nomatch andalso run_fuzzy_filter(E, Fuzzy).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

read_legacy_global_tx(?global_ns, UserGroup, UserId) ->
    mnesia:read(?TAB, {UserGroup, UserId}, write);
read_legacy_global_tx(_Namespace, _UserGroup, _UserId) ->
    [].

delete_legacy_global_tx(?global_ns, UserGroup, UserId) ->
    mnesia:delete(?TAB, {UserGroup, UserId}, write);
delete_legacy_global_tx(_Namespace, _UserGroup, _UserId) ->
    ok.

read_user_for_update_tx(?global_ns = Namespace, UserGroup, UserId, Algorithm) ->
    Key = ?AUTHN_NS_KEY(Namespace, UserGroup, UserId),
    case cluster_supports_global_ns_table() of
        false ->
            case read_legacy_global_tx(Namespace, UserGroup, UserId) of
                [#user_info{} = Rec] -> {ok, Rec};
                [] -> error
            end;
        true ->
            case read_legacy_global_tx(Namespace, UserGroup, UserId) of
                [#user_info{} = Rec] ->
                    {ok, legacy_record_to_ns(Rec, Algorithm)};
                [] ->
                    case mnesia:read(?AUTHN_NS_TAB, Key, write) of
                        [#?AUTHN_NS_TAB{} = Rec] ->
                            {ok, Rec};
                        [] ->
                            error
                    end
            end
    end;
read_user_for_update_tx(Namespace, UserGroup, UserId, _Algorithm) when is_binary(Namespace) ->
    Key = ?AUTHN_NS_KEY(Namespace, UserGroup, UserId),
    case mnesia:read(?AUTHN_NS_TAB, Key, write) of
        [#?AUTHN_NS_TAB{} = Rec] -> {ok, Rec};
        [] -> error
    end.

maybe_delete_legacy_global_tx(#?AUTHN_NS_TAB{}, Namespace, UserGroup, UserId) ->
    delete_legacy_global_tx(Namespace, UserGroup, UserId);
maybe_delete_legacy_global_tx(#user_info{}, _Namespace, _UserGroup, _UserId) ->
    ok.

legacy_record_to_ns(
    #user_info{
        user_id = {UserGroup, UserId},
        password_hash = PasswordHash,
        salt = Salt,
        is_superuser = IsSuperuser
    },
    Algorithm
) ->
    #?AUTHN_NS_TAB{
        user_id = ?AUTHN_NS_KEY(?global_ns, UserGroup, UserId),
        password_hash = PasswordHash,
        salt = Salt,
        is_superuser = IsSuperuser,
        extra = #{algo => emqx_authn_password_hashing:encode(Algorithm)}
    }.

user_exists(?global_ns, UserGroup, UserId) ->
    legacy_global_user_exists(UserGroup, UserId) orelse
        (cluster_supports_global_ns_table() andalso
            ns_user_exists(?global_ns, UserGroup, UserId));
user_exists(Namespace, UserGroup, UserId) when is_binary(Namespace) ->
    ns_user_exists(Namespace, UserGroup, UserId).

legacy_global_user_exists(UserGroup, UserId) ->
    ets:member(?TAB, {UserGroup, UserId}).

ns_user_exists(Namespace, UserGroup, UserId) ->
    ets:member(?AUTHN_NS_TAB, ?AUTHN_NS_KEY(Namespace, UserGroup, UserId)).

public_user_info(#{user_id := UserId, is_superuser := IsSuperuser}) ->
    #{user_id => UserId, is_superuser => IsSuperuser}.

-spec insert_user_tx(map(), map()) ->
    {success | override, emqx_config:maybe_namespace(), boolean()} | skipped | failed.
insert_user_tx(User, Opts) ->
    #{
        user_group := UserGroup,
        user_id := UserId,
        password_hash := PasswordHash,
        salt := Salt,
        is_superuser := IsSuperuser,
        password_hash_algorithm := StoredAlgorithm
    } = User,
    Namespace = maps:get(namespace, User, ?global_ns),
    case is_superuser_allowed(Namespace, IsSuperuser) of
        false ->
            ?SLOG(warning, #{
                msg => "import_superuser_in_namespace_not_allowed",
                namespace => Namespace,
                user_id => UserId,
                group_id => UserGroup,
                bootstrap_file => maps:get(filename, Opts)
            }),
            failed;
        true ->
            UserInfoRecord = imported_user_info_record(
                Namespace,
                UserGroup,
                UserId,
                PasswordHash,
                Salt,
                IsSuperuser,
                StoredAlgorithm
            ),
            insert_imported_user_tx(UserInfoRecord, Namespace, UserGroup, UserId, Opts)
    end.

insert_imported_user_tx(UserInfoRecord, Namespace, UserGroup, UserId, Opts) ->
    CountRecord = is_new_namespaced_record_tx(UserInfoRecord),
    LogF = fun(Msg) ->
        ?SLOG(warning, #{
            msg => Msg,
            namespace => Namespace,
            user_id => UserId,
            group_id => UserGroup,
            bootstrap_file => maps:get(filename, Opts)
        })
    end,
    case lookup_by_record_tx(UserInfoRecord) of
        [] ->
            case do_lookup_conflicting_global_txn(UserInfoRecord) of
                [] ->
                    ok = write_user_tx(UserInfoRecord),
                    {success, Namespace, CountRecord};
                [_] ->
                    LogF("import_namespaced_userid_conflicts_with_global_user"),
                    failed
            end;
        [UserInfoRecord] ->
            skipped;
        [_ExistingRecord] ->
            case maps:get(override, Opts, false) of
                true ->
                    ok = write_user_tx(UserInfoRecord),
                    maybe_delete_legacy_global_tx(
                        UserInfoRecord, Namespace, UserGroup, UserId
                    ),
                    LogF("override_an_exists_userid_into_authentication_database_ok"),
                    {override, Namespace, CountRecord};
                false ->
                    LogF("import_an_exists_userid_into_authentication_database_failed"),
                    failed
            end
    end.

%% MQTT users in a non-global namespace must never be superusers: explicit ACL
%% rules are enforced for tenant clients.  This mirrors the check done by the
%% per-user management API, covering the bulk import API and the bootstrap file.
is_superuser_allowed(?global_ns, _IsSuperuser) -> true;
is_superuser_allowed(_Namespace, false) -> true;
is_superuser_allowed(_Namespace, _IsSuperuser) -> false.

write_user_tx(#?AUTHN_NS_TAB{} = UserInfoRecord) ->
    mnesia:write(?AUTHN_NS_TAB, UserInfoRecord, write);
write_user_tx(#user_info{} = UserInfoRecord) ->
    mnesia:write(?TAB, UserInfoRecord, write).

user_info_record(
    #{
        user_id := UserId,
        password := Password
    } = UserInfo,
    #{
        password_hash_algorithm := _CurrentAlgorithm,
        user_group := UserGroup
    } = _State,
    Algorithm
) ->
    Namespace = maps:get(namespace, UserInfo, ?global_ns),
    IsSuperuser = maps:get(is_superuser, UserInfo, false),
    {PasswordHash, Salt} = emqx_authn_password_hashing:hash(Algorithm, Password),
    user_info_record(Namespace, UserGroup, UserId, PasswordHash, Salt, IsSuperuser, Algorithm).

user_info_record(?global_ns, UserGroup, UserId, PasswordHash, Salt, IsSuperuser, Algorithm) ->
    case cluster_supports_global_ns_table() of
        true ->
            namespaced_user_info_record(
                ?global_ns, UserGroup, UserId, PasswordHash, Salt, IsSuperuser, Algorithm
            );
        false ->
            #user_info{
                user_id = {UserGroup, UserId},
                password_hash = PasswordHash,
                salt = Salt,
                is_superuser = IsSuperuser
            }
    end;
user_info_record(Namespace, UserGroup, UserId, PasswordHash, Salt, IsSuperuser, Algorithm) ->
    namespaced_user_info_record(
        Namespace, UserGroup, UserId, PasswordHash, Salt, IsSuperuser, Algorithm
    ).

namespaced_user_info_record(
    Namespace, UserGroup, UserId, PasswordHash, Salt, IsSuperuser, Algorithm
) ->
    #?AUTHN_NS_TAB{
        user_id = ?AUTHN_NS_KEY(Namespace, UserGroup, UserId),
        password_hash = PasswordHash,
        salt = Salt,
        is_superuser = IsSuperuser,
        extra = #{algo => emqx_authn_password_hashing:encode(Algorithm)}
    }.

imported_user_info_record(
    ?global_ns, UserGroup, UserId, PasswordHash, Salt, IsSuperuser, StoredAlgorithm
) ->
    case cluster_supports_global_ns_table() of
        true ->
            #?AUTHN_NS_TAB{
                user_id = ?AUTHN_NS_KEY(?global_ns, UserGroup, UserId),
                password_hash = PasswordHash,
                salt = Salt,
                is_superuser = IsSuperuser,
                extra = #{algo => StoredAlgorithm}
            };
        false ->
            #user_info{
                user_id = {UserGroup, UserId},
                password_hash = PasswordHash,
                salt = Salt,
                is_superuser = IsSuperuser
            }
    end;
imported_user_info_record(
    Namespace, UserGroup, UserId, PasswordHash, Salt, IsSuperuser, StoredAlgorithm
) ->
    #?AUTHN_NS_TAB{
        user_id = ?AUTHN_NS_KEY(Namespace, UserGroup, UserId),
        password_hash = PasswordHash,
        salt = Salt,
        is_superuser = IsSuperuser,
        extra = #{algo => StoredAlgorithm}
    }.

fields_to_update(
    #{password := _} = UserInfo,
    [algo | Rest],
    #{password_hash_algorithm := Algorithm} = State
) ->
    [{algo, Algorithm} | fields_to_update(UserInfo, Rest, State)];
fields_to_update(
    #{password := Password} = UserInfo,
    [hash_and_salt | Rest],
    #{password_hash_algorithm := Algorithm} = State
) ->
    [
        {hash_and_salt, emqx_authn_password_hashing:hash(Algorithm, Password)}
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
update_user_record(#user_info{} = UserInfoRecord, [{hash_and_salt, {PasswordHash, Salt}} | Rest]) ->
    update_user_record(
        UserInfoRecord#user_info{password_hash = PasswordHash, salt = Salt}, Rest
    );
update_user_record(#user_info{} = UserInfoRecord, [{is_superuser, IsSuperuser} | Rest]) ->
    update_user_record(UserInfoRecord#user_info{is_superuser = IsSuperuser}, Rest);
update_user_record(#user_info{} = UserInfoRecord, [{algo, _Algorithm} | Rest]) ->
    update_user_record(UserInfoRecord, Rest);
update_user_record(#?AUTHN_NS_TAB{} = UserInfoRecord, [{hash_and_salt, {PasswordHash, Salt}} | Rest]) ->
    update_user_record(
        UserInfoRecord#?AUTHN_NS_TAB{password_hash = PasswordHash, salt = Salt}, Rest
    );
update_user_record(#?AUTHN_NS_TAB{} = UserInfoRecord, [{is_superuser, IsSuperuser} | Rest]) ->
    update_user_record(UserInfoRecord#?AUTHN_NS_TAB{is_superuser = IsSuperuser}, Rest);
update_user_record(#?AUTHN_NS_TAB{extra = Extra} = UserInfoRecord, [{algo, Algorithm} | Rest]) ->
    update_user_record(
        UserInfoRecord#?AUTHN_NS_TAB{
            extra = Extra#{algo => emqx_authn_password_hashing:encode(Algorithm)}
        },
        Rest
    ).

%% TODO: Support other type
get_user_identity(#{username := Username}, username) ->
    Username;
get_user_identity(#{clientid := ClientID}, clientid) ->
    ClientID;
get_user_identity(_, Type) ->
    {error, {bad_user_identity_type, Type}}.

trans(Fun, Args) ->
    case mria:transaction(?AUTHN_SHARD, Fun, Args) of
        {atomic, Res} -> Res;
        {aborted, Reason} -> {error, Reason}
    end.

trans(Fun) ->
    case mria:transaction(?AUTHN_SHARD, Fun) of
        {atomic, Res} -> Res;
        {aborted, Reason} -> {error, Reason}
    end.

to_binary(B) when is_binary(B) ->
    B;
to_binary(L) when is_list(L) ->
    iolist_to_binary(L).

format_user_info(#user_info{user_id = {_, UserId}, is_superuser = IsSuperuser}) ->
    #{user_id => UserId, is_superuser => IsSuperuser};
format_user_info(#?AUTHN_NS_TAB{user_id = ?AUTHN_NS_KEY(_, _, UserId), is_superuser = IsSuperuser}) ->
    #{user_id => UserId, is_superuser => IsSuperuser}.

all_ns_group_match_spec(Namespace, UserGroup) ->
    ns_group_match_spec(Namespace, UserGroup, []).

legacy_group_match_spec(UserGroup) ->
    [{legacy_match_head(UserGroup, '_', []), [], ['$_']}].

group_match_spec(?TAB, ?global_ns, UserGroup, QString) ->
    case lists:keyfind(is_superuser, 1, QString) of
        false ->
            legacy_group_match_spec(UserGroup);
        {is_superuser, '=:=', Value} ->
            [{legacy_match_head(UserGroup, '_', [{#user_info.is_superuser, Value}]), [], ['$_']}]
    end;
group_match_spec(?AUTHN_NS_TAB, Namespace, UserGroup, QString) ->
    ns_group_match_spec(Namespace, UserGroup, QString).

ns_group_match_spec(Namespace, UserGroup, QString) ->
    case lists:keyfind(is_superuser, 1, QString) of
        false ->
            [{ns_match_head(Namespace, UserGroup, '_', []), [], ['$_']}];
        {is_superuser, '=:=', Value} ->
            [
                {
                    ns_match_head(
                        Namespace,
                        UserGroup,
                        '_',
                        [{#?AUTHN_NS_TAB.is_superuser, Value}]
                    ),
                    [],
                    ['$_']
                }
            ]
    end.

legacy_match_head(UserGroup, UserId, PosValues) ->
    erlang:make_tuple(
        record_info(size, user_info),
        '_',
        [{1, user_info}, {#user_info.user_id, {UserGroup, UserId}} | PosValues]
    ).

ns_match_head(Namespace, UserGroup, UserId, PosValues) ->
    erlang:make_tuple(
        record_info(size, ?AUTHN_NS_TAB),
        '_',
        [
            {1, ?AUTHN_NS_TAB},
            {#?AUTHN_NS_TAB.user_id, ?AUTHN_NS_KEY(Namespace, UserGroup, UserId)}
            | PosValues
        ]
    ).

fix_global_list_count(#{meta := #{count := _} = Meta} = Result, QueryString) ->
    {_Count, ParsedQString} = emqx_mgmt_api:parse_qstring(QueryString, ?AUTHN_QSCHEMA),
    MsFun = mk_qs2ms(?global_ns),
    Count = lists:sum([
        count_matching_users(Tab, ParsedQString, MsFun)
     || Tab <- [?TAB, ?AUTHN_NS_TAB]
    ]),
    Result#{meta := Meta#{count := Count}};
fix_global_list_count(Result, _QueryString) ->
    Result.

count_matching_users(Tab, ParsedQString, MsFun) ->
    #{match_spec := MatchSpec, fuzzy_fun := undefined} = MsFun(Tab, ParsedQString),
    ets:select_count(Tab, [
        {MatchHead, Conditions, [true]}
     || {MatchHead, Conditions, _Return} <- MatchSpec
    ]).

%%--------------------------------------------------------------------
%% parse import file/data

parse_import_users(Filename, FileData, Convertor) ->
    UserStream = reader_fn(Filename, FileData),
    Users = emqx_utils_stream:consume(emqx_utils_stream:map(Convertor, UserStream)),
    NewUsersCount =
        lists:foldl(
            fun(
                #{
                    %% injected by converter fn
                    namespace := Namespace,
                    user_group := UserGroup,
                    user_id := UserId
                },
                Acc
            ) ->
                case user_exists(Namespace, UserGroup, UserId) of
                    true ->
                        Acc;
                    false ->
                        Acc + 1
                end
            end,
            0,
            Users
        ),
    {NewUsersCount, Users}.

reader_fn(prepared_user_list, List) when is_list(List) ->
    %% Example: [#{<<"user_id">> => <<>>, ...}]
    emqx_utils_stream:list(List);
reader_fn(Filename0, Data) ->
    case filename:extension(to_binary(Filename0)) of
        <<".json">> ->
            %% Example: data/user-credentials.json
            case emqx_utils_json:safe_decode(Data) of
                {ok, List} when is_list(List) ->
                    emqx_utils_stream:list(List);
                {ok, _} ->
                    error(unknown_file_format);
                {error, Reason} ->
                    error(Reason)
            end;
        <<".csv">> ->
            %% Example: etc/auth-built-in-db-bootstrap.csv
            emqx_utils_stream:csv(Data, #{nullable => true});
        <<>> ->
            error(unknown_file_format);
        Extension ->
            error({unsupported_file_format, Extension})
    end.

converter(PasswordType, State) ->
    fun(User) ->
        convert_user(User, PasswordType, State)
    end.

convert_user(
    User = #{<<"user_id">> := UserId},
    PasswordType,
    #{user_group := UserGroup, password_hash_algorithm := Algorithm}
) ->
    Namespace =
        case maps:get(<<"namespace">>, User, undefined) of
            undefined -> ?global_ns;
            null -> ?global_ns;
            Ns -> Ns
        end,
    {PasswordHash, Salt} = find_password_hash(PasswordType, User, Algorithm),
    #{
        namespace => Namespace,
        user_id => UserId,
        password_hash => PasswordHash,
        salt => Salt,
        is_superuser => is_superuser(User),
        user_group => UserGroup,
        password_hash_algorithm => emqx_authn_password_hashing:encode(Algorithm)
    };
convert_user(_, _, _) ->
    error(bad_format).

find_password_hash(hash, User = #{<<"password_hash">> := PasswordHash}, _) ->
    {PasswordHash, maps:get(<<"salt">>, User, <<>>)};
find_password_hash(plain, #{<<"password">> := Password}, Algorithm) ->
    emqx_authn_password_hashing:hash(Algorithm, Password);
find_password_hash(hash, _User, _) ->
    error("hash_import_requires_password_hash_field");
find_password_hash(plain, _User, _Algorithm) ->
    error("plain_import_requires_password_field");
find_password_hash(_, _, _) ->
    error(bad_format).

is_superuser(#{<<"is_superuser">> := <<"true">>}) -> true;
is_superuser(#{<<"is_superuser">> := true}) -> true;
is_superuser(_) -> false.

bootstrap_user_from_file(Config, State) ->
    case maps:get(bootstrap_file, Config, <<>>) of
        <<>> ->
            ok;
        Filename0 ->
            #{bootstrap_type := Type} = Config,
            IsDefault = (Filename0 =:= emqx_authn_mnesia_schema:default_bootstrap_file_path()),
            Filename = emqx_schema:naive_env_interpolation(Filename0),
            case file:read_file(Filename) of
                {ok, FileData} ->
                    _ = import_users({Type, Filename, FileData}, State, #{override => false}),
                    ok;
                {error, enoent} when IsDefault ->
                    ok;
                {error, Reason} ->
                    ?SLOG(warning, #{
                        msg => "bootstrap_authn_built_in_database_failed",
                        bootstrap_file => Filename,
                        bootstrap_type => Type,
                        reason => emqx_utils:explain_posix(Reason)
                    })
            end
    end.

lookup_user_with_fallback(?global_ns, UserGroup, UserId) ->
    do_lookup_user(?global_ns, UserGroup, UserId);
lookup_user_with_fallback(Namespace, UserGroup, UserId) when is_binary(Namespace) ->
    case do_lookup_user(Namespace, UserGroup, UserId) of
        {ok, _} = NamespacedUser ->
            case do_lookup_user(?global_ns, UserGroup, UserId) of
                error -> NamespacedUser;
                {ok, _} -> error
            end;
        error ->
            case is_namespace_empty(Namespace) of
                %% We only fall back to global if there are no records for the whole namespace.
                true -> do_lookup_user(?global_ns, UserGroup, UserId);
                false -> error
            end
    end.

do_lookup_user(?global_ns, UserGroup, UserId) ->
    case mnesia:dirty_read(?TAB, {UserGroup, UserId}) of
        [#user_info{} = Rec] ->
            {ok, rec_to_map(Rec)};
        [] ->
            case
                mnesia:dirty_read(
                    ?AUTHN_NS_TAB, ?AUTHN_NS_KEY(?global_ns, UserGroup, UserId)
                )
            of
                [] -> error;
                [#?AUTHN_NS_TAB{} = Rec] -> {ok, rec_to_map(Rec)}
            end
    end;
do_lookup_user(Namespace, UserGroup, UserId) when is_binary(Namespace) ->
    case mnesia:dirty_read(?AUTHN_NS_TAB, ?AUTHN_NS_KEY(Namespace, UserGroup, UserId)) of
        [] ->
            error;
        [#?AUTHN_NS_TAB{} = Rec] ->
            {ok, rec_to_map(Rec)}
    end.

lookup_by_record_tx(#?AUTHN_NS_TAB{user_id = ?AUTHN_NS_KEY(Namespace, UserGroup, UserId) = Key}) ->
    case read_legacy_global_tx(Namespace, UserGroup, UserId) of
        [] -> mnesia:read(?AUTHN_NS_TAB, Key, write);
        Records -> Records
    end;
lookup_by_record_tx(#user_info{user_id = Key}) ->
    mnesia:read(?TAB, Key, write).

cluster_supports_global_ns_table() ->
    emqx_bpapi:supported_version(emqx_authn) >= ?GLOBAL_NS_TABLE_BPAPI_VSN.

is_namespaced_record(#?AUTHN_NS_TAB{}) -> true;
is_namespaced_record(#user_info{}) -> false.

is_new_namespaced_record_tx(#?AUTHN_NS_TAB{user_id = Key}) ->
    mnesia:read(?AUTHN_NS_TAB, Key, write) =:= [];
is_new_namespaced_record_tx(#user_info{}) ->
    false.

do_lookup_conflicting_global_txn(#user_info{}) ->
    [];
do_lookup_conflicting_global_txn(#?AUTHN_NS_TAB{
    user_id = ?AUTHN_NS_KEY(_, UserGroup, UserId)
}) ->
    case mnesia:read(?TAB, {UserGroup, UserId}, write) of
        [] ->
            mnesia:read(
                ?AUTHN_NS_TAB,
                ?AUTHN_NS_KEY(?global_ns, UserGroup, UserId),
                write
            );
        Records ->
            Records
    end.

is_namespace_empty(Namespace) when is_binary(Namespace) ->
    %% `[]` is `<` than any (binary) user id or group
    %% `0` is `<` than any (atom) group (user group is an atom, despite what the original
    %% typespec said...)
    case mnesia:dirty_next(?AUTHN_NS_TAB, ?AUTHN_NS_KEY(Namespace, 0, [])) of
        ?AUTHN_NS_KEY(Namespace, _, _) ->
            false;
        _ ->
            true
    end.

rec_to_map(#user_info{} = Rec) ->
    #user_info{
        user_id = {UserGroup, UserId},
        password_hash = PasswordHash,
        salt = Salt,
        is_superuser = IsSuperuser
    } = Rec,
    #{
        namespace => ?global_ns,
        user_id => UserId,
        user_group => UserGroup,
        password_hash => PasswordHash,
        salt => Salt,
        is_superuser => IsSuperuser,
        extra => #{}
    };
rec_to_map(#?AUTHN_NS_TAB{} = Rec) ->
    #?AUTHN_NS_TAB{
        user_id = ?AUTHN_NS_KEY(Namespace, UserGroup, UserId),
        password_hash = PasswordHash,
        salt = Salt,
        is_superuser = IsSuperuser,
        extra = Extra
    } = Rec,
    #{
        namespace => Namespace,
        user_id => UserId,
        user_group => UserGroup,
        password_hash => PasswordHash,
        salt => Salt,
        is_superuser => IsSuperuser,
        extra => Extra
    }.

get_namespace(#{client_attrs := #{?CLIENT_ATTR_NAME_TNS := Namespace}} = _ClientInfo) when
    is_binary(Namespace)
->
    Namespace;
get_namespace(_ClientInfo) ->
    ?global_ns.

inc_ns_rule_count(Namespace, N) when Namespace =:= ?global_ns; is_binary(Namespace) ->
    _ = ets:update_counter(?AUTHN_NS_COUNT_TAB, Namespace, {2, N}, {Namespace, 0}),
    ok.

dec_ns_rule_count(Namespace, N) when Namespace =:= ?global_ns; is_binary(Namespace) ->
    _ = ets:update_counter(?AUTHN_NS_COUNT_TAB, Namespace, {2, -N, 0, 0}, {Namespace, 0}),
    ok.
