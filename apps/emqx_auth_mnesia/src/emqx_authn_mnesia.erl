%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_authn_mnesia).

-include("emqx_auth_mnesia.hrl").
-include_lib("emqx_auth/include/emqx_authn.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-behaviour(emqx_authn_provider).
-behaviour(emqx_db_backup).

-export([
    create/2,
    update/2,
    authenticate/2,
    destroy/1
]).

-export([
    import_users/2,
    add_user/2,
    delete_user/2,
    update_user/3,
    lookup_user/2,
    list_users/2
]).

-export([
    qs2ms/2,
    run_fuzzy_filter/2,
    format_user_info/1,
    group_match_spec/1
]).

%% Internal exports (RPC)
-export([
    do_destroy/1,
    do_add_user/1,
    do_delete_user/2,
    do_update_user/3
]).

-export([init_tables/0]).

-export([backup_tables/0]).

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
    [?TAB].

%% Init
-spec init_tables() -> ok.
init_tables() ->
    ok = mria:wait_for_tables(create_tables()).

%%------------------------------------------------------------------------------
%% Data backup
%%------------------------------------------------------------------------------

backup_tables() -> {<<"builtin_authn">>, [?TAB]}.

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
    UserID = get_user_identity(Credential, Type),
    case mnesia:dirty_read(?TAB, {UserGroup, UserID}) of
        [] ->
            ?TRACE_AUTHN_PROVIDER("user_not_found"),
            ignore;
        [#user_info{password_hash = PasswordHash, salt = Salt, is_superuser = IsSuperuser}] ->
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

destroy(#{user_group := UserGroup}) ->
    trans(fun ?MODULE:do_destroy/1, [UserGroup]).

do_destroy(UserGroup) ->
    ok = lists:foreach(
        fun(User) ->
            mnesia:delete_object(?TAB, User, write)
        end,
        mnesia:select(?TAB, group_match_spec(UserGroup), write)
    ).

import_users(ImportSource, State) ->
    import_users(ImportSource, State, #{override => true}).

import_users({PasswordType, Filename, FileData}, State, Opts) ->
    Convertor = convertor(PasswordType, State),
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
    Fun = fun() ->
        lists:foldl(
            fun(User, Acc) ->
                Return = insert_user(User, Opts),
                N = maps:get(Return, Acc, 0),
                maps:put(Return, N + 1, Acc)
            end,
            #{success => 0, skipped => 0, override => 0, failed => 0},
            Users
        )
    end,
    Res = trans(Fun),
    {ok, Res#{total => length(Users)}}.

add_user(
    UserInfo,
    State
) ->
    UserInfoRecord = user_info_record(UserInfo, State),
    trans(fun ?MODULE:do_add_user/1, [UserInfoRecord]).

do_add_user(
    #user_info{
        user_id = {_UserGroup, UserID} = DBUserID,
        is_superuser = IsSuperuser
    } = UserInfoRecord
) ->
    case mnesia:read(?TAB, DBUserID, write) of
        [] ->
            ok = insert_user(UserInfoRecord),
            {ok, #{user_id => UserID, is_superuser => IsSuperuser}};
        [_] ->
            {error, already_exist}
    end.

delete_user(UserID, State) ->
    trans(fun ?MODULE:do_delete_user/2, [UserID, State]).

do_delete_user(UserID, #{user_group := UserGroup}) ->
    case mnesia:read(?TAB, {UserGroup, UserID}, write) of
        [] ->
            {error, not_found};
        [_] ->
            mnesia:delete(?TAB, {UserGroup, UserID}, write)
    end.

update_user(UserID, UserInfo, State) ->
    FieldsToUpdate = fields_to_update(
        UserInfo,
        [
            hash_and_salt,
            is_superuser
        ],
        State
    ),
    trans(fun ?MODULE:do_update_user/3, [UserID, FieldsToUpdate, State]).

do_update_user(
    UserID,
    FieldsToUpdate,
    #{
        user_group := UserGroup
    }
) ->
    case mnesia:read(?TAB, {UserGroup, UserID}, write) of
        [] ->
            {error, not_found};
        [#user_info{} = UserInfoRecord] ->
            NUserInfoRecord = update_user_record(UserInfoRecord, FieldsToUpdate),
            ok = insert_user(NUserInfoRecord),
            {ok, #{user_id => UserID, is_superuser => NUserInfoRecord#user_info.is_superuser}}
    end.

lookup_user(UserID, #{user_group := UserGroup}) ->
    case mnesia:dirty_read(?TAB, {UserGroup, UserID}) of
        [UserInfo] ->
            {ok, format_user_info(UserInfo)};
        [] ->
            {error, not_found}
    end.

list_users(QueryString, #{user_group := UserGroup}) ->
    NQueryString = QueryString#{<<"user_group">> => UserGroup},
    emqx_mgmt_api:node_query(
        node(),
        ?TAB,
        NQueryString,
        ?AUTHN_QSCHEMA,
        fun ?MODULE:qs2ms/2,
        fun ?MODULE:format_user_info/1
    ).

%%--------------------------------------------------------------------
%% QueryString to MatchSpec

-spec qs2ms(atom(), {list(), list()}) -> emqx_mgmt_api:match_spec_and_filter().
qs2ms(_Tab, {QString, FuzzyQString}) ->
    #{
        match_spec => ms_from_qstring(QString),
        fuzzy_fun => fuzzy_filter_fun(FuzzyQString)
    }.

%% Fuzzy username funcs
fuzzy_filter_fun([]) ->
    undefined;
fuzzy_filter_fun(Fuzzy) ->
    {fun ?MODULE:run_fuzzy_filter/2, [Fuzzy]}.

run_fuzzy_filter(_, []) ->
    true;
run_fuzzy_filter(
    E = #user_info{user_id = {_, UserID}},
    [{user_id, like, UsernameSubStr} | Fuzzy]
) ->
    binary:match(UserID, UsernameSubStr) /= nomatch andalso run_fuzzy_filter(E, Fuzzy).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

-spec insert_user(map(), map()) -> success | skipped | override | failed.
insert_user(User, Opts) ->
    #{
        <<"user_group">> := UserGroup,
        <<"user_id">> := UserID,
        <<"password_hash">> := PasswordHash,
        <<"salt">> := Salt,
        <<"is_superuser">> := IsSuperuser
    } = User,
    UserInfoRecord =
        #user_info{user_id = DBUserID} =
        user_info_record(UserGroup, UserID, PasswordHash, Salt, IsSuperuser),
    case mnesia:read(?TAB, DBUserID, write) of
        [] ->
            ok = insert_user(UserInfoRecord),
            success;
        [UserInfoRecord] ->
            skipped;
        [_] ->
            LogF = fun(Msg) ->
                ?SLOG(warning, #{
                    msg => Msg,
                    user_id => UserID,
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
    mnesia:write(?TAB, UserInfoRecord, write).

user_info_record(UserGroup, UserID, PasswordHash, Salt, IsSuperuser) ->
    #user_info{
        user_id = {UserGroup, UserID},
        password_hash = PasswordHash,
        salt = Salt,
        is_superuser = IsSuperuser
    }.

user_info_record(
    #{
        user_id := UserID,
        password := Password
    } = UserInfo,
    #{
        password_hash_algorithm := Algorithm,
        user_group := UserGroup
    } = _State
) ->
    IsSuperuser = maps:get(is_superuser, UserInfo, false),
    {PasswordHash, Salt} = emqx_authn_password_hashing:hash(Algorithm, Password),
    user_info_record(UserGroup, UserID, PasswordHash, Salt, IsSuperuser).

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
update_user_record(UserInfoRecord, [{hash_and_salt, {PasswordHash, Salt}} | Rest]) ->
    update_user_record(UserInfoRecord#user_info{password_hash = PasswordHash, salt = Salt}, Rest);
update_user_record(UserInfoRecord, [{is_superuser, IsSuperuser} | Rest]) ->
    update_user_record(UserInfoRecord#user_info{is_superuser = IsSuperuser}, Rest).

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

format_user_info(#user_info{user_id = {_, UserID}, is_superuser = IsSuperuser}) ->
    #{user_id => UserID, is_superuser => IsSuperuser}.

ms_from_qstring(QString) ->
    case lists:keytake(user_group, 1, QString) of
        {value, {user_group, '=:=', UserGroup}, QString2} ->
            group_match_spec(UserGroup, QString2);
        _ ->
            []
    end.

group_match_spec(UserGroup) ->
    group_match_spec(UserGroup, []).

group_match_spec(UserGroup, QString) ->
    case lists:keyfind(is_superuser, 1, QString) of
        false ->
            ets:fun2ms(fun(#user_info{user_id = {Group, _}} = User) when Group =:= UserGroup ->
                User
            end);
        {is_superuser, '=:=', Value} ->
            ets:fun2ms(fun(#user_info{user_id = {Group, _}, is_superuser = IsSuper} = User) when
                Group =:= UserGroup, IsSuper =:= Value
            ->
                User
            end)
    end.

%%--------------------------------------------------------------------
%% parse import file/data

parse_import_users(Filename, FileData, Convertor) ->
    UserStream = reader_fn(Filename, FileData),
    Users = emqx_utils_stream:consume(emqx_utils_stream:map(Convertor, UserStream)),
    NewUsersCount =
        lists:foldl(
            fun(
                #{
                    <<"user_group">> := UserGroup,
                    <<"user_id">> := UserID
                },
                Acc
            ) ->
                case ets:member(?TAB, {UserGroup, UserID}) of
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
            case emqx_utils_json:safe_decode(Data, [return_maps]) of
                {ok, List} when is_list(List) ->
                    emqx_utils_stream:list(List);
                {ok, _} ->
                    error(unknown_file_format);
                {error, Reason} ->
                    error(Reason)
            end;
        <<".csv">> ->
            %% Example: etc/auth-built-in-db-bootstrap.csv
            emqx_utils_stream:csv(Data);
        <<>> ->
            error(unknown_file_format);
        Extension ->
            error({unsupported_file_format, Extension})
    end.

convertor(PasswordType, State) ->
    fun(User) ->
        convert_user(User, PasswordType, State)
    end.

convert_user(
    User = #{<<"user_id">> := UserId},
    PasswordType,
    #{user_group := UserGroup, password_hash_algorithm := Algorithm}
) ->
    {PasswordHash, Salt} = find_password_hash(PasswordType, User, Algorithm),
    #{
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
        FileName0 ->
            #{bootstrap_type := Type} = Config,
            FileName = emqx_schema:naive_env_interpolation(FileName0),
            case file:read_file(FileName) of
                {ok, FileData} ->
                    _ = import_users({Type, FileName, FileData}, State, #{override => false}),
                    ok;
                {error, Reason} ->
                    ?SLOG(warning, #{
                        msg => "boostrap_authn_built_in_database_failed",
                        boostrap_file => FileName,
                        boostrap_type => Type,
                        reason => emqx_utils:explain_posix(Reason)
                    })
            end
    end.
