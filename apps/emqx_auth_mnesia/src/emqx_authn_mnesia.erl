%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    lookup_user/3,
    list_users/2
]).

-export([record_count/1, record_count_per_namespace/0]).

-export([
    run_fuzzy_filter/2,
    format_user_info/1
]).

%% Internal exports (RPC)
-export([
    do_destroy/1,
    do_add_user/1,
    do_delete_user/3,
    do_update_user/4
]).

-export([init_tables/0]).

-export([backup_tables/0]).

-ifdef(TEST).
-export([rec_to_map/1]).
-endif.

-export_type([user_group/0, user_id/0]).

-type user_group() :: binary().
-type user_id() :: binary().

-record(user_info, {
    user_id :: {user_group(), user_id()},
    password_hash :: binary(),
    salt :: binary(),
    is_superuser :: boolean()
}).

-define(TAB, ?MODULE).
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

create(_AuthenticatorID, Config) ->
    create(Config).

create(
    #{
        user_id_type := Type,
        password_hash_algorithm := Algorithm,
        user_group := UserGroup
    } = Config
) ->
    ok = emqx_authn_password_hashing:init(Algorithm),
    State = #{
        user_group => UserGroup,
        user_id_type => Type,
        password_hash_algorithm => Algorithm
    },
    ok = boostrap_user_from_file(Config, State),
    {ok, State}.

update(Config, _State) ->
    create(Config).

authenticate(#{auth_method := _}, _) ->
    ignore;
authenticate(#{password := undefined}, _) ->
    {error, bad_username_or_password};
authenticate(
    #{password := Password} = Credential,
    #{
        user_group := UserGroup,
        user_id_type := Type,
        password_hash_algorithm := Algorithm
    }
) ->
    Namespace = get_namespace(Credential),
    UserId = get_user_identity(Credential, Type),
    case do_lookup_user(Namespace, UserGroup, UserId) of
        error ->
            ?TRACE_AUTHN_PROVIDER("user_not_found"),
            ignore;
        {ok, #{password_hash := PasswordHash, salt := Salt, is_superuser := IsSuperuser}} ->
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
    trans(fun ?MODULE:do_destroy/1, [UserGroup]).

do_destroy(UserGroup) ->
    ok = lists:foreach(
        fun(User) ->
            mnesia:delete_object(?TAB, User, write)
        end,
        mnesia:select(?TAB, all_ns_group_match_spec(?global_ns, UserGroup), write)
    ),
    lists:foreach(
        fun(User) ->
            mnesia:delete_object(?AUTHN_NS_TAB, User, write)
        end,
        mnesia:select(?AUTHN_NS_TAB, all_ns_group_match_spec('_', UserGroup), write)
    ).

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
        Return = insert_user(User, Opts),
        case Return of
            {success, Ns} ->
                PerNs0 = maps:get(per_ns, Acc0),
                PerNs = inc_bucket_count(Ns, PerNs0),
                Acc1 = Acc0#{per_ns := PerNs},
                inc_bucket_count(success, Acc1);
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
    State
) ->
    UserInfoRecord = user_info_record(UserInfo, State),
    Res = trans(fun ?MODULE:do_add_user/1, [UserInfoRecord]),
    maybe
        {ok, #{namespace := Namespace}} ?= Res,
        inc_ns_rule_count(Namespace, 1)
    end,
    Res.

do_add_user(UserInfoRecord) ->
    case do_lookup_by_rec_txn(UserInfoRecord) of
        [] ->
            ok = insert_user(UserInfoRecord),
            #{
                namespace := Namespace,
                user_id := UserId,
                is_superuser := IsSuperuser
            } =
                rec_to_map(UserInfoRecord),
            {ok, #{namespace => Namespace, user_id => UserId, is_superuser => IsSuperuser}};
        [_] ->
            {error, already_exist}
    end.

delete_user(Namespace, UserId, State) ->
    Res = trans(fun ?MODULE:do_delete_user/3, [Namespace, UserId, State]),
    maybe
        ok ?= Res,
        dec_ns_rule_count(Namespace, 1)
    end,
    Res.

do_delete_user(Namespace, UserId, #{user_group := UserGroup}) ->
    Table = table(Namespace),
    Key = key(Namespace, UserGroup, UserId),
    case mnesia:read(Table, Key, write) of
        [] ->
            {error, not_found};
        [_] ->
            mnesia:delete(Table, Key, write)
    end.

update_user(Namespace, UserId, UserInfo, State) ->
    FieldsToUpdate = fields_to_update(
        UserInfo,
        [
            hash_and_salt,
            is_superuser
        ],
        State
    ),
    trans(fun ?MODULE:do_update_user/4, [Namespace, UserId, FieldsToUpdate, State]).

do_update_user(
    Namespace,
    UserId,
    FieldsToUpdate,
    #{
        user_group := UserGroup
    }
) ->
    Table = table(Namespace),
    Key = key(Namespace, UserGroup, UserId),
    case mnesia:read(Table, Key, write) of
        [] ->
            {error, not_found};
        [UserInfoRecord] ->
            NUserInfoRecord = update_user_record(UserInfoRecord, FieldsToUpdate),
            ok = insert_user(NUserInfoRecord),
            #{user_id := UserId, is_superuser := IsSuperuser} = rec_to_map(NUserInfoRecord),
            {ok, #{user_id => UserId, is_superuser => IsSuperuser}}
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
    QueryString = QueryString0#{<<"user_group">> => UserGroup},
    Table = table(Namespace),
    emqx_mgmt_api:node_query(
        node(),
        Table,
        QueryString,
        ?AUTHN_QSCHEMA,
        mk_qs2ms(Namespace),
        fun ?MODULE:format_user_info/1
    ).

-spec record_count(emqx_config:maybe_namespace()) -> non_neg_integer().
record_count(?global_ns) ->
    mnesia:table_info(?TAB, size);
record_count(Namespace) when is_binary(Namespace) ->
    try
        ets:lookup_element(?AUTHN_NS_COUNT_TAB, Namespace, 2, 0)
    catch
        error:badarg -> 0
    end.

-spec record_count_per_namespace() -> #{emqx_config:namespace() => non_neg_integer()}.
record_count_per_namespace() ->
    maps:from_list(ets:tab2list(?AUTHN_NS_COUNT_TAB)).

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

-spec insert_user(map(), map()) ->
    {success, emqx_config:maybe_namespace()} | skipped | override | failed.
insert_user(User, Opts) ->
    #{
        <<"user_group">> := UserGroup,
        <<"user_id">> := UserId,
        <<"password_hash">> := PasswordHash,
        <<"salt">> := Salt,
        <<"is_superuser">> := IsSuperuser
    } = User,
    Namespace = maps:get(<<"namespace">>, User, ?global_ns),
    UserInfoRecord = user_info_record(
        Namespace, UserGroup, UserId, PasswordHash, Salt, IsSuperuser
    ),
    case do_lookup_by_rec_txn(UserInfoRecord) of
        [] ->
            ok = insert_user(UserInfoRecord),
            {success, Namespace};
        [UserInfoRecord] ->
            skipped;
        [_] ->
            LogF = fun(Msg) ->
                ?SLOG(warning, #{
                    msg => Msg,
                    namespace => Namespace,
                    user_id => UserId,
                    group_id => UserGroup,
                    bootstrap_file => maps:get(filename, Opts)
                })
            end,
            case maps:get(override, Opts, false) of
                true ->
                    ok = insert_user(UserInfoRecord),
                    LogF("override_an_exists_userid_into_authentication_database_ok"),
                    override;
                false ->
                    LogF("import_an_exists_userid_into_authentication_database_failed"),
                    failed
            end
    end.

insert_user(#user_info{} = UserInfoRecord) ->
    mnesia:write(?TAB, UserInfoRecord, write);
insert_user(#?AUTHN_NS_TAB{} = UserInfoRecord) ->
    mnesia:write(?AUTHN_NS_TAB, UserInfoRecord, write).

user_info_record(?global_ns, UserGroup, UserId, PasswordHash, Salt, IsSuperuser) ->
    #user_info{
        user_id = {UserGroup, UserId},
        password_hash = PasswordHash,
        salt = Salt,
        is_superuser = IsSuperuser
    };
user_info_record(Namespace, UserGroup, UserId, PasswordHash, Salt, IsSuperuser) when
    is_binary(Namespace)
->
    #?AUTHN_NS_TAB{
        user_id = ?AUTHN_NS_KEY(Namespace, UserGroup, UserId),
        password_hash = PasswordHash,
        salt = Salt,
        is_superuser = IsSuperuser
    }.

user_info_record(
    #{
        user_id := UserId,
        password := Password
    } = UserInfo,
    #{
        password_hash_algorithm := Algorithm,
        user_group := UserGroup
    } = _State
) ->
    Namespace = maps:get(namespace, UserInfo, ?global_ns),
    IsSuperuser = maps:get(is_superuser, UserInfo, false),
    {PasswordHash, Salt} = emqx_authn_password_hashing:hash(Algorithm, Password),
    user_info_record(Namespace, UserGroup, UserId, PasswordHash, Salt, IsSuperuser).

fields_to_update(
    #{password := Password} = UserInfo,
    [hash_and_salt | Rest],
    #{password_hash_algorithm := Algorithm} = State
) ->
    [
        {hash_and_salt,
            emqx_authn_password_hashing:hash(
                Algorithm, Password
            )}
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
    update_user_record(UserInfoRecord#user_info{password_hash = PasswordHash, salt = Salt}, Rest);
update_user_record(#?AUTHN_NS_TAB{} = UserInfoRecord, [{hash_and_salt, {PasswordHash, Salt}} | Rest]) ->
    update_user_record(
        UserInfoRecord#?AUTHN_NS_TAB{password_hash = PasswordHash, salt = Salt}, Rest
    );
update_user_record(#user_info{} = UserInfoRecord, [{is_superuser, IsSuperuser} | Rest]) ->
    update_user_record(UserInfoRecord#user_info{is_superuser = IsSuperuser}, Rest);
update_user_record(#?AUTHN_NS_TAB{} = UserInfoRecord, [{is_superuser, IsSuperuser} | Rest]) ->
    update_user_record(UserInfoRecord#?AUTHN_NS_TAB{is_superuser = IsSuperuser}, Rest).

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
        record_info(size, ?AUTHN_NS_TAB),
        '_',
        [
            {1, ?AUTHN_NS_TAB},
            {#?AUTHN_NS_TAB.user_id, ?AUTHN_NS_KEY(Namespace, UserGroup, '_')}
            | PosValues
        ]
    ).

mk_pos_value(?global_ns, is_superuser, Value) ->
    {#user_info.is_superuser, Value};
mk_pos_value(Namespace, is_superuser, Value) when is_binary(Namespace); Namespace == '_' ->
    {#?AUTHN_NS_TAB.is_superuser, Value}.

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
                    <<"namespace">> := Namespace,
                    <<"user_group">> := UserGroup,
                    <<"user_id">> := UserId
                },
                Acc
            ) ->
                Table = table(Namespace),
                Key = key(Namespace, UserGroup, UserId),
                case ets:member(Table, Key) of
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
        <<"namespace">> => Namespace,
        <<"user_id">> => UserId,
        <<"password_hash">> => PasswordHash,
        <<"salt">> => Salt,
        <<"is_superuser">> => is_superuser(User),
        <<"user_group">> => UserGroup
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

boostrap_user_from_file(Config, State) ->
    case maps:get(bootstrap_file, Config, <<>>) of
        <<>> ->
            ok;
        Filename0 ->
            #{bootstrap_type := Type} = Config,
            Filename = emqx_schema:naive_env_interpolation(Filename0),
            case file:read_file(Filename) of
                {ok, FileData} ->
                    _ = import_users({Type, Filename, FileData}, State, #{override => false}),
                    ok;
                {error, Reason} ->
                    ?SLOG(warning, #{
                        msg => "boostrap_authn_built_in_database_failed",
                        boostrap_file => Filename,
                        boostrap_type => Type,
                        reason => emqx_utils:explain_posix(Reason)
                    })
            end
    end.

do_lookup_user(?global_ns, UserGroup, UserId) ->
    case mnesia:dirty_read(?TAB, {UserGroup, UserId}) of
        [] ->
            error;
        [#user_info{} = Rec] ->
            {ok, rec_to_map(Rec)}
    end;
do_lookup_user(Namespace, UserGroup, UserId) when is_binary(Namespace) ->
    case mnesia:dirty_read(?AUTHN_NS_TAB, ?AUTHN_NS_KEY(Namespace, UserGroup, UserId)) of
        [] ->
            error;
        [#?AUTHN_NS_TAB{} = Rec] ->
            {ok, rec_to_map(Rec)}
    end.

do_lookup_by_rec_txn(#user_info{user_id = Key}) ->
    mnesia:read(?TAB, Key, write);
do_lookup_by_rec_txn(#?AUTHN_NS_TAB{user_id = Key}) ->
    mnesia:read(?AUTHN_NS_TAB, Key, write).

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

table(?global_ns) -> ?TAB;
table(Namespace) when is_binary(Namespace) -> ?AUTHN_NS_TAB.

key(?global_ns, UserGroup, UserId) ->
    {UserGroup, UserId};
key(Namespace, UserGroup, UserId) when is_binary(Namespace) ->
    ?AUTHN_NS_KEY(Namespace, UserGroup, UserId).

get_namespace(#{client_attrs := #{?CLIENT_ATTR_NAME_TNS := Namespace}} = _ClientInfo) when
    is_binary(Namespace)
->
    Namespace;
get_namespace(_ClientInfo) ->
    ?global_ns.

inc_ns_rule_count(?global_ns, _N) ->
    ok;
inc_ns_rule_count(Namespace, N) when is_binary(Namespace) ->
    _ = ets:update_counter(?AUTHN_NS_COUNT_TAB, Namespace, {2, N}, {Namespace, 0}),
    ok.

dec_ns_rule_count(?global_ns, _N) ->
    ok;
dec_ns_rule_count(Namespace, N) when is_binary(Namespace) ->
    _ = ets:update_counter(?AUTHN_NS_COUNT_TAB, Namespace, {2, -N, 0, 0}, {Namespace, 0}),
    ok.
